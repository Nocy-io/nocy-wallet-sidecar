use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::db::{
    get_block_headers_in_range, get_block_merkle_state_at_height, get_chain_head,
    get_merkle_ready_height, get_zswap_output_counts_in_range, insert_block_merkle_state_batch_tx,
    reset_merkle_ready_state_tx, update_merkle_ready_height_tx, BlockMerkleStateInsert,
};
use crate::error::AppError;
use crate::ledger_state_store::LedgerStateStore;
use crate::metrics::{
    record_merkle_not_ready, record_merkle_readiness_duration_ms,
    record_merkle_readiness_lag_blocks, record_merkle_readiness_stall,
    record_merkle_ready_height, set_merkle_readiness_stall_seconds,
};

const DEFAULT_BATCH_SIZE: i64 = 500;

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
pub struct MerkleReadinessHealth {
    last_ready_height: Arc<AtomicI64>,
    last_chain_head: Arc<AtomicI64>,
    last_advanced_ms: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy)]
pub struct MerkleReadinessStatus {
    pub stalled: bool,
    pub stall_duration: Duration,
    pub merkle_ready_height: i64,
    pub chain_head: i64,
}

impl MerkleReadinessHealth {
    pub fn new() -> Self {
        Self {
            last_ready_height: Arc::new(AtomicI64::new(-1)),
            last_chain_head: Arc::new(AtomicI64::new(-1)),
            last_advanced_ms: Arc::new(AtomicU64::new(now_millis())),
        }
    }

    pub fn record_progress(&self, merkle_ready_height: i64, chain_head: i64) {
        self.last_ready_height
            .store(merkle_ready_height, Ordering::Relaxed);
        self.last_chain_head.store(chain_head, Ordering::Relaxed);
        self.last_advanced_ms.store(now_millis(), Ordering::Relaxed);
    }

    pub fn record_no_progress(&self, merkle_ready_height: i64, chain_head: i64) {
        self.last_ready_height
            .store(merkle_ready_height, Ordering::Relaxed);
        self.last_chain_head.store(chain_head, Ordering::Relaxed);
    }

    pub fn status(&self, stall_threshold: Duration) -> MerkleReadinessStatus {
        let chain_head = self.last_chain_head.load(Ordering::Relaxed);
        let merkle_ready_height = self.last_ready_height.load(Ordering::Relaxed);
        let last_advanced_ms = self.last_advanced_ms.load(Ordering::Relaxed);
        let now_ms = now_millis();

        let stall_duration = if chain_head > merkle_ready_height && now_ms >= last_advanced_ms {
            Duration::from_millis(now_ms - last_advanced_ms)
        } else {
            Duration::from_secs(0)
        };

        let stalled = chain_head > merkle_ready_height && stall_duration >= stall_threshold;

        MerkleReadinessStatus {
            stalled,
            stall_duration,
            merkle_ready_height,
            chain_head,
        }
    }
}

#[derive(Debug)]
enum BatchOutcome {
    Advanced {
        processed: usize,
        new_ready_height: i64,
        chain_head: i64,
    },
    NoProgress {
        chain_head: i64,
        merkle_ready_height: i64,
        reason: &'static str,
    },
}

pub fn start_merkle_readiness_tracker(
    db_pool: PgPool,
    ledger_state_store: Arc<LedgerStateStore>,
    allow_sparse_blocks: bool,
    poll_interval: Duration,
    stall_threshold: Duration,
    health: Arc<MerkleReadinessHealth>,
) {
    tokio::spawn(async move {
        let mut last_chain_head: i64 = -1;
        let mut last_ready_height: i64 = -1;
        let mut last_stall_logged_at: Option<std::time::Instant> = None;
        loop {
            let started = std::time::Instant::now();
            match process_merkle_readiness_batch(
                &db_pool,
                &ledger_state_store,
                allow_sparse_blocks,
                DEFAULT_BATCH_SIZE,
            )
            .await
            {
                Ok(BatchOutcome::Advanced {
                    processed,
                    new_ready_height,
                    chain_head,
                }) => {
                    last_chain_head = chain_head;
                    last_ready_height = new_ready_height;
                    health.record_progress(new_ready_height, chain_head);
                    last_stall_logged_at = None;
                    set_merkle_readiness_stall_seconds(Duration::from_secs(0));
                    info!(
                        processed,
                        merkle_ready_height = new_ready_height,
                        chain_head,
                        "Merkle readiness advanced"
                    );
                    record_merkle_ready_height(new_ready_height);
                    record_merkle_readiness_lag_blocks(chain_head - new_ready_height);
                    record_merkle_readiness_duration_ms(started.elapsed());
                    continue;
                }
                Ok(BatchOutcome::NoProgress {
                    reason,
                    chain_head,
                    merkle_ready_height,
                }) => {
                    last_chain_head = chain_head;
                    last_ready_height = merkle_ready_height;
                    health.record_no_progress(merkle_ready_height, chain_head);
                    let status = health.status(stall_threshold);
                    set_merkle_readiness_stall_seconds(status.stall_duration);
                    if reason != "caught_up" {
                        info!(reason, chain_head, "Merkle readiness made no progress");
                    }
                    if reason == "snapshot_not_ready" {
                        record_merkle_not_ready();
                    }
                    record_merkle_ready_height(merkle_ready_height);
                    record_merkle_readiness_lag_blocks(chain_head - merkle_ready_height);
                    record_merkle_readiness_duration_ms(started.elapsed());
                    if status.stalled {
                        let should_log = last_stall_logged_at
                            .map(|logged| logged.elapsed() >= stall_threshold)
                            .unwrap_or(true);
                        if should_log {
                            record_merkle_readiness_stall();
                            warn!(
                                chain_head = status.chain_head,
                                merkle_ready_height = status.merkle_ready_height,
                                stall_seconds = status.stall_duration.as_secs(),
                                "Merkle readiness stalled beyond threshold"
                            );
                            last_stall_logged_at = Some(std::time::Instant::now());
                        }
                    }
                }
                Err(err) => {
                    error!(error = %err, "Merkle readiness batch failed");
                    record_merkle_readiness_duration_ms(started.elapsed());
                    if last_chain_head >= 0 {
                        health.record_no_progress(last_ready_height, last_chain_head);
                        let status = health.status(stall_threshold);
                        set_merkle_readiness_stall_seconds(status.stall_duration);
                        if status.stalled {
                            let should_log = last_stall_logged_at
                                .map(|logged| logged.elapsed() >= stall_threshold)
                                .unwrap_or(true);
                            if should_log {
                                record_merkle_readiness_stall();
                                warn!(
                                    chain_head = status.chain_head,
                                    merkle_ready_height = status.merkle_ready_height,
                                    stall_seconds = status.stall_duration.as_secs(),
                                    "Merkle readiness stalled beyond threshold"
                                );
                                last_stall_logged_at = Some(std::time::Instant::now());
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    });
}

async fn process_merkle_readiness_batch(
    pool: &PgPool,
    ledger_state_store: &LedgerStateStore,
    allow_sparse_blocks: bool,
    batch_size: i64,
) -> Result<BatchOutcome, AppError> {
    let current_ready_height = get_merkle_ready_height(pool).await?;
    let chain_head = get_chain_head(pool).await?;

    if chain_head <= current_ready_height {
        return Ok(BatchOutcome::NoProgress {
            chain_head,
            merkle_ready_height: current_ready_height,
            reason: "caught_up",
        });
    }

    let from_height = current_ready_height + 1;
    let to_height = (from_height + batch_size - 1).min(chain_head);

    let blocks = get_block_headers_in_range(pool, from_height, to_height).await?;
    if blocks.is_empty() {
        return Ok(BatchOutcome::NoProgress {
            chain_head,
            merkle_ready_height: current_ready_height,
            reason: "no_blocks",
        });
    }

    if !allow_sparse_blocks && blocks.first().map(|b| b.height) != Some(from_height) {
        warn!(
            expected = from_height,
            actual = blocks.first().map(|b| b.height),
            "Block gap detected before readiness batch; waiting for contiguous heights"
        );
        return Ok(BatchOutcome::NoProgress {
            chain_head,
            merkle_ready_height: current_ready_height,
            reason: "gap",
        });
    }

    let counts = get_zswap_output_counts_in_range(pool, from_height, to_height).await?;

    let (mut prev_end_index, mut prev_hash, mut prev_height) = if current_ready_height >= 0 {
        match get_block_merkle_state_at_height(pool, current_ready_height).await? {
            Some(row) => (
                row.end_index,
                row.block_hash,
                Some(current_ready_height),
            ),
            None => (0, None, None),
        }
    } else {
        (0, None, None)
    };

    let needs_snapshot = blocks.iter().any(|block| {
        counts
            .get(&block.height)
            .copied()
            .unwrap_or(0)
            > 0
    });

    let available_end_index = if needs_snapshot {
        match ledger_state_store.get_snapshot().await {
            Ok(Some(snapshot)) => snapshot.available_end_index().map(|v| v as i64),
            Ok(None) => None,
            Err(err) => {
                warn!(error = %err, "Failed to fetch ledger state snapshot");
                None
            }
        }
    } else {
        None
    };

    let mut expected_height = from_height;
    let mut rows_to_insert: Vec<BlockMerkleStateInsert> = Vec::new();
    let mut last_ready_height = current_ready_height;
    let mut stop_reason: Option<&'static str> = None;

    for block in blocks {
        if block.height != expected_height {
            if !allow_sparse_blocks {
                warn!(
                    expected = expected_height,
                    actual = block.height,
                    "Block gap detected during readiness batch; stopping at gap"
                );
                stop_reason = Some("gap");
                break;
            }

            info!(
                expected = expected_height,
                actual = block.height,
                "Block gap detected; allow_sparse_blocks enabled, continuing"
            );
            prev_hash = None;
            prev_height = None;
        }

        let is_contiguous = prev_height.map(|h| h + 1 == block.height).unwrap_or(false);
        if is_contiguous {
            if let Some(prev_hash_bytes) = prev_hash.as_ref() {
                if &block.parent_hash != prev_hash_bytes {
                    let safe_height = (block.height - 1).max(0);
                    let mut tx = pool.begin().await.map_err(|e| {
                        AppError::InternalError(format!(
                            "Failed to begin merkle readiness reset transaction: {}",
                            e
                        ))
                    })?;
                    reset_merkle_ready_state_tx(&mut tx, safe_height).await?;
                    tx.commit().await.map_err(|e| {
                        AppError::InternalError(format!(
                            "Failed to commit merkle readiness reset: {}",
                            e
                        ))
                    })?;
                    warn!(
                        height = block.height,
                        "Reorg detected during readiness batch; reset to safe height"
                    );
                    return Ok(BatchOutcome::NoProgress {
                        chain_head,
                        merkle_ready_height: safe_height,
                        reason: "reorg",
                    });
                }
            }
        }

        let output_count = counts.get(&block.height).copied().unwrap_or(0);
        let start_index = prev_end_index;
        let end_index = start_index + output_count;

        if output_count > 0 {
            let available_end_index = match available_end_index {
                Some(value) => value,
                None => {
                    stop_reason = Some("snapshot_not_ready");
                    break;
                }
            };

            if available_end_index < end_index - 1 {
                stop_reason = Some("snapshot_not_ready");
                break;
            }
        }

        rows_to_insert.push(BlockMerkleStateInsert {
            block_height: block.height,
            block_hash: Some(block.hash.clone()),
            start_index,
            end_index,
            output_count,
            protocol_version: block.protocol_version,
        });

        prev_end_index = end_index;
        prev_hash = Some(block.hash);
        prev_height = Some(block.height);
        expected_height = block.height + 1;
        last_ready_height = block.height;
    }

    if rows_to_insert.is_empty() {
        return Ok(BatchOutcome::NoProgress {
            chain_head,
            merkle_ready_height: current_ready_height,
            reason: stop_reason.unwrap_or("no_ready_blocks"),
        });
    }

    let mut tx = pool.begin().await.map_err(|e| {
        AppError::InternalError(format!(
            "Failed to begin merkle readiness update transaction: {}",
            e
        ))
    })?;
    insert_block_merkle_state_batch_tx(&mut tx, &rows_to_insert).await?;
    let updated = update_merkle_ready_height_tx(&mut tx, last_ready_height, current_ready_height)
        .await?;
    if !updated {
        tx.rollback().await.map_err(|e| {
            AppError::InternalError(format!(
                "Failed to rollback merkle readiness update: {}",
                e
            ))
        })?;
        return Ok(BatchOutcome::NoProgress {
            chain_head,
            merkle_ready_height: current_ready_height,
            reason: "concurrent_update",
        });
    }
    tx.commit().await.map_err(|e| {
        AppError::InternalError(format!(
            "Failed to commit merkle readiness update: {}",
            e
        ))
    })?;

    Ok(BatchOutcome::Advanced {
        processed: rows_to_insert.len(),
        new_ready_height: last_ready_height,
        chain_head,
    })
}
