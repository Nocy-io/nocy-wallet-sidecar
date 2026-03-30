//! SSE live stream endpoint for real-time feed updates
//!
//! Implements GET /v1/feed/subscribe with:
//! - Backlog mode: fetch existing blocks from fromHeight
//! - Live mode: stream new blocks as they arrive
//! - Heartbeats while waiting for walletReadyHeight to advance
//! - Backpressure handling for slow clients

use axum::{
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures::stream::Stream;
use midnight_ledger::events::{Event as LedgerEvent, EventDetails};
use midnight_serialize::tagged_deserialize;
use midnight_storage::DefaultDB;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, instrument, warn};

use crate::db::{
    begin_readonly_tx, compute_wallet_ready_height_tx, detect_identifiers_source,
    get_all_dust_events_tx, get_all_zswap_input_events_in_range_tx, get_block_hash_at_height_tx,
    get_block_merkle_state_in_range_tx, get_blocks_in_range_tx, get_chain_head_tx,
    get_contract_actions_for_txs_tx, get_dust_events_tx, get_relevant_transactions_tx,
    get_transaction_metadata_tx, get_unshielded_utxo_events_tx, get_upstream_session_id,
    get_zswap_events_for_txs_tx, get_merkle_ready_height_tx, IdentifiersSource,
};
use crate::error::AppError;
use crate::feed::{
    BlockBundle, ControlEvent, DustLedgerEventRaw, FeedItem, MerkleCollapsedUpdate,
    MerkleUpdatePosition, OrderedFeedItem, ShieldedRelevantTx, TransactionFees,
    TransactionMerkle, TransactionMeta, UnshieldedUtxoEvent, Watermarks, ZswapInputRaw,
};
use crate::ledger_state_store::{
    LedgerStateSnapshot, LedgerStateStore, LedgerStateStoreError, MerkleUpdateNotReady,
};
use crate::routes::session::{get_session_nullifier_streaming, NullifierStreaming, SessionState};
use crate::sequence::{parse_cursor, SequenceKey};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Session ID is a 32-byte SHA256 hash, hex-encoded (64 chars)
const SESSION_ID_BYTES: usize = 32;

/// Parse and validate a session ID (hex-encoded 32 bytes)
fn parse_session_id(session_id: &str) -> Result<Vec<u8>, crate::error::AppError> {
    let bytes = hex::decode(session_id)
        .map_err(|_| crate::error::AppError::BadRequest("session_id must be valid hex".into()))?;

    if bytes.len() != SESSION_ID_BYTES {
        return Err(crate::error::AppError::BadRequest(format!(
            "session_id must be {} bytes ({} hex chars), got {} bytes",
            SESSION_ID_BYTES,
            SESSION_ID_BYTES * 2,
            bytes.len()
        )));
    }

    Ok(bytes)
}

/// Query parameters for GET /v1/feed/subscribe
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeQuery {
    /// Session ID (hex-encoded 32 bytes)
    pub session_id: String,
    /// Starting block height (defaults to 0)
    #[serde(default)]
    pub from_height: Option<i64>,
    /// Starting merkle tree end index (exclusive). Enables inline merkle updates.
    #[serde(default)]
    pub from_merkle_index: Option<i64>,
    /// Starting sequence cursor (overrides from_height if provided)
    #[serde(default)]
    pub from_cursor: Option<String>,
    /// Whether to include global ZswapInput events for spend detection (defaults to true)
    /// Set to false if using 0-value change outputs for spend detection instead
    #[serde(default = "default_include_nullifiers")]
    pub include_nullifiers: bool,
    /// Whether to include transaction metadata items (defaults to true)
    #[serde(default = "default_include_tx_meta")]
    pub include_tx_meta: bool,
    /// Whether to include raw transaction bytes in transactionMeta (defaults to false)
    #[serde(default = "default_include_raw_tx")]
    pub include_raw_tx: bool,
    /// Whether to include transaction identifiers in transactionMeta (defaults to true)
    #[serde(default = "default_include_identifiers")]
    pub include_identifiers: bool,
    /// Whether to include ALL dust events (not just wallet-relevant) (defaults to false)
    /// Required for dust merkle tree sync because the SDK needs all events in sequence.
    /// When false, only wallet-relevant dust events are returned.
    #[serde(default = "default_include_all_dust")]
    pub include_all_dust: bool,
    /// Optional empty-block handling mode (defaults to "coalesce").
    #[serde(default)]
    pub empty_block_mode: Option<EmptyBlockMode>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum EmptyBlockMode {
    Coalesce,
    PerBlock,
}

fn default_include_nullifiers() -> bool {
    true
}

fn default_include_all_dust() -> bool {
    false
}

fn default_include_tx_meta() -> bool {
    true
}

fn default_include_raw_tx() -> bool {
    false
}

fn default_include_identifiers() -> bool {
    true
}

/// SSE event types
/// Uses adjacently tagged serde format: {"type": "...", "data": {...}}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "data")]
pub enum SseEvent {
    /// A block bundle with feed items
    Block(BlockBundle),
    /// Progress heartbeat while waiting for new data
    Heartbeat {
        watermarks: Watermarks,
        /// Current position being processed
        current_height: String,
    },
    /// Control event (reorg, reset, etc.)
    Control(ControlEvent),
}

/// Constants for SSE stream behavior
const BACKLOG_BATCH_SIZE: i64 = 50;
const CHANNEL_BUFFER_SIZE: usize = 32;
const PHASE_TX: i64 = 0;
const PHASE_LEDGER_EVENT: i64 = 1;
const PHASE_UNSHIELDED: i64 = 2;
const PHASE_POST_TX: i64 = 3;
const NOT_READY_EVENT_MIN_INTERVAL: Duration = Duration::from_secs(5);
const COALESCE_EMPTY_BLOCKS_MAX_RANGE: i64 = 1000;
const COALESCE_EMPTY_BLOCKS_MAX_DELAY: Duration = Duration::from_secs(5);
const MAX_SPARSE_GAP: i64 = 100_000;

fn decode_u128_value(bytes: &[u8]) -> u128 {
    if bytes.len() == 16 {
        u128::from_be_bytes(bytes.try_into().unwrap_or([0u8; 16]))
    } else {
        let mut padded = [0u8; 16];
        let len = bytes.len().min(16);
        padded[16 - len..].copy_from_slice(&bytes[..len]);
        u128::from_be_bytes(padded)
    }
}

fn decode_fee_string(bytes: &Option<Vec<u8>>) -> String {
    match bytes {
        Some(raw) if !raw.is_empty() => decode_u128_value(raw).to_string(),
        _ => "0".to_string(),
    }
}

fn decode_fee_option(bytes: &Option<Vec<u8>>) -> Option<String> {
    match bytes {
        Some(raw) if !raw.is_empty() => Some(decode_u128_value(raw).to_string()),
        Some(_) => Some("0".to_string()),
        None => None,
    }
}

/// GET /v1/feed/subscribe - SSE stream of feed updates
#[instrument(skip(state), fields(session_id = %query.session_id))]
pub async fn subscribe_feed(
    State(state): State<SessionState>,
    Query(query): Query<SubscribeQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Capture config values upfront for use in error path and main path
    let heartbeat_interval = state.heartbeat_interval;

    // Parse and validate session ID upfront
    let local_session_id_bytes = match parse_session_id(&query.session_id) {
        Ok(bytes) => bytes,
        Err(e) => {
            // For SSE, we can't return an error directly, so send an error event
            warn!(error = %e, "Invalid session_id");
            let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
            let _ = tx.send(Ok(Event::default().event("error").data(format!("{}", e)))).await;
            drop(tx);
            return Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::new().interval(heartbeat_interval));
        }
    };

    // Resolve the upstream wallet-indexer session ID for this local session.
    // The SSE stream needs it for walletReadyHeight tracking.
    let upstream_session_id_bytes = match get_upstream_session_id(&state.db_pool, &local_session_id_bytes).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(error = %e, "Invalid or unknown session_id");
            let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
            let _ = tx.send(Ok(Event::default().event("error").data(format!("{}", e)))).await;
            drop(tx);
            return Sse::new(ReceiverStream::new(rx))
                .keep_alive(KeepAlive::new().interval(heartbeat_interval));
        }
    };

    let from_merkle_index = query.from_merkle_index;

    let from_cursor = match query.from_cursor.as_deref() {
        Some(cursor) => match parse_cursor(cursor) {
            Ok(parsed) => Some(parsed),
            Err(message) => {
                warn!(error = %message, "Invalid from_cursor");
                let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
                let _ = tx
                    .send(Ok(Event::default().event("error").data(message)))
                    .await;
                drop(tx);
                return Sse::new(ReceiverStream::new(rx))
                    .keep_alive(KeepAlive::new().interval(heartbeat_interval));
            }
        },
        None => None,
    };
    let from_height = from_cursor
        .map(|cursor| cursor.block_height)
        .unwrap_or_else(|| query.from_height.unwrap_or(0))
        .max(0);

    if let Some(value) = from_merkle_index {
        if value < 0 && !(value == -1 && from_height == 0) {
            let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
            let _ = tx
                .send(Ok(Event::default().event("error").data(
                    "from_merkle_index must be >= 0 (or -1 when fromHeight == 0)".to_string(),
                )))
                .await;
            drop(tx);
            return Sse::new(ReceiverStream::new(rx))
                .keep_alive(KeepAlive::new().interval(heartbeat_interval));
        }
    }

    info!(
        from_height,
        include_all_dust = query.include_all_dust,
        include_nullifiers = query.include_nullifiers,
        include_tx_meta = query.include_tx_meta,
        local_session_id = %query.session_id,
        upstream_session_id = %hex::encode(&upstream_session_id_bytes),
        "Starting SSE subscription"
    );

    // Create a channel for sending events
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(CHANNEL_BUFFER_SIZE);

    // Track SSE client connection
    crate::metrics::sse_client_connected();

    // Capture values for the spawned task
    let include_nullifiers = query.include_nullifiers;
    let include_tx_meta = query.include_tx_meta;
    let include_raw_tx = query.include_raw_tx;
    let include_identifiers = query.include_identifiers;
    let include_all_dust = query.include_all_dust;
    let coalesce_empty_blocks = !matches!(query.empty_block_mode, Some(EmptyBlockMode::PerBlock));
    let local_session_id_str = query.session_id.clone();
    let from_merkle_index = match from_merkle_index {
        Some(-1) if from_height == 0 => Some(0),
        other => other,
    };

    // Spawn the stream producer task
    tokio::spawn(async move {
        if let Err(e) = run_stream(
            state,
            local_session_id_bytes,
            local_session_id_str,
            upstream_session_id_bytes,
            from_height,
            from_cursor,
            from_merkle_index,
            include_nullifiers,
            include_tx_meta,
            include_raw_tx,
            include_identifiers,
            include_all_dust,
            coalesce_empty_blocks,
            tx,
        )
        .await
        {
            warn!(error = %e, "SSE stream error");
        }
        // Track SSE client disconnection
        crate::metrics::sse_client_disconnected();
    });

    // Convert the receiver to a stream
    let stream = ReceiverStream::new(rx);

    Sse::new(stream).keep_alive(KeepAlive::new().interval(heartbeat_interval))
}

#[derive(Debug, Clone, Copy)]
struct PendingEmptyRange {
    start: i64,
    end: i64,
    merkle_end_index: i64,
}

impl PendingEmptyRange {
    fn len(&self) -> i64 {
        self.end - self.start + 1
    }
}

/// Internal stream producer that sends events through the channel
async fn run_stream(
    state: SessionState,
    local_session_id_bytes: Vec<u8>,
    local_session_id_str: String,
    upstream_session_id_bytes: Vec<u8>,
    mut current_height: i64,
    mut from_cursor: Option<SequenceKey>,
    from_merkle_index: Option<i64>,
    include_nullifiers: bool,
    include_tx_meta: bool,
    include_raw_tx: bool,
    include_identifiers: bool,
    include_all_dust: bool,
    coalesce_empty_blocks: bool,
    tx: mpsc::Sender<Result<Event, Infallible>>,
) -> Result<(), AppError> {
    let mut last_heartbeat = std::time::Instant::now();

    // Track merkle tree position for inline collapsed update injection
    // -1 means updates are disabled (no cursor provided)
    let inline_merkle_updates_enabled = from_merkle_index.is_some();
    let mut last_merkle_end_index: i64 = from_merkle_index.unwrap_or(-1);
    let mut last_not_ready_sent_at: Option<std::time::Instant> = None;
    let mut pending_empty_range: Option<PendingEmptyRange> = None;
    let mut pending_empty_range_started_at: Option<std::time::Instant> = None;

    if !inline_merkle_updates_enabled {
        let control = ControlEvent::inline_merkle_updates_disabled(
            "inline merkle updates disabled; client must fetch /v1/zswap/merkle-update",
        );
        let event = SseEvent::Control(control);
        let _ = send_event(&tx, "control", &event, state.sse_send_timeout).await;
    }

    // Determine nullifier streaming mode
    // Query param include_nullifiers=false takes precedence, else use session preference
    let nullifier_mode = if !include_nullifiers {
        NullifierStreaming::Disabled
    } else {
        get_session_nullifier_streaming(&state.db_pool, &local_session_id_str).await?
    };

    let identifiers_source = if include_tx_meta && include_identifiers {
        detect_identifiers_source(&state.db_pool).await?
    } else {
        IdentifiersSource::None
    };

    // Track the last block hash across batches for boundary reorg detection
    // On first batch, we'll fetch the previous block's hash if current_height > 0
    let mut last_block_hash: Option<String> = None;

    loop {
        // Check if the channel is closed (client disconnected)
        if tx.is_closed() {
            info!("Client disconnected, stopping stream");
            break;
        }

        let mut snapshot = begin_readonly_tx(&state.db_pool).await?;

        // Get current chain state
        let chain_head = get_chain_head_tx(&mut snapshot).await?;
        let wallet_ready_height =
            compute_wallet_ready_height_tx(&mut snapshot, &upstream_session_id_bytes, chain_head)
                .await?;
        let merkle_ready_height = get_merkle_ready_height_tx(&mut snapshot).await?;
        let combined_ready_height = wallet_ready_height
            .map(|wallet_height| wallet_height.min(merkle_ready_height).min(chain_head));

        // Note: finalized_height equals chain_head because the upstream chain-indexer
        // only processes finalized blocks (via subscribe_finalized() in subxt_node.rs).
        let watermarks = Watermarks {
            chain_head,
            wallet_ready_height,
            merkle_ready_height,
            combined_ready_height,
            finalized_height: Some(chain_head),
            allow_sparse_blocks: state.allow_sparse_blocks,
        };

        // Determine the ready height we can stream up to
        let ready_height = match combined_ready_height {
            Some(h) => h,
            None => {
                snapshot.commit().await.map_err(|e| {
                    AppError::InternalError(format!(
                        "Failed to commit subscribe snapshot (not ready): {}",
                        e
                    ))
                })?;
                if coalesce_empty_blocks && pending_empty_range.is_some() {
                    let mut sent_event = false;
                    if flush_pending_empty_range(
                        &mut pending_empty_range,
                        &mut pending_empty_range_started_at,
                        &tx,
                        state.sse_send_timeout,
                        &mut sent_event,
                        &mut last_heartbeat,
                        &mut current_height,
                    )
                    .await
                    .is_err()
                    {
                        return Ok(());
                    }
                }
                // Wallet not ready, send heartbeat and wait
                if last_heartbeat.elapsed() >= state.heartbeat_interval {
                    let event = SseEvent::Heartbeat {
                        watermarks: watermarks.clone(),
                        current_height: current_height.to_string(),
                    };
                    if send_event(&tx, "heartbeat", &event, state.sse_send_timeout).await.is_err() {
                        break;
                    }
                    last_heartbeat = std::time::Instant::now();
                }
                tokio::time::sleep(state.poll_interval).await;
                continue;
            }
        };

        // If we're caught up, wait for new blocks
        if current_height > ready_height {
            snapshot.commit().await.map_err(|e| {
                AppError::InternalError(format!(
                    "Failed to commit subscribe snapshot (caught up): {}",
                    e
                ))
            })?;
            if coalesce_empty_blocks && pending_empty_range.is_some() {
                let mut sent_event = false;
                if flush_pending_empty_range(
                    &mut pending_empty_range,
                    &mut pending_empty_range_started_at,
                    &tx,
                    state.sse_send_timeout,
                    &mut sent_event,
                    &mut last_heartbeat,
                    &mut current_height,
                )
                .await
                .is_err()
                {
                    return Ok(());
                }
            }
            if last_heartbeat.elapsed() >= state.heartbeat_interval {
                let event = SseEvent::Heartbeat {
                    watermarks: watermarks.clone(),
                    current_height: current_height.to_string(),
                };
                if send_event(&tx, "heartbeat", &event, state.sse_send_timeout).await.is_err() {
                    break;
                }
                last_heartbeat = std::time::Instant::now();
            }
            tokio::time::sleep(state.poll_interval).await;
            continue;
        }

        // Fetch a batch of blocks
        let to_height = (current_height + BACKLOG_BATCH_SIZE - 1).min(ready_height);
        let blocks_meta = get_blocks_in_range_tx(&mut snapshot, current_height, to_height).await?;

        // DIAGNOSTIC: Log batch processing
        tracing::debug!(
            current_height = current_height,
            to_height = to_height,
            ready_height = ready_height,
            blocks_fetched = blocks_meta.len(),
            "Processing block batch"
        );

        if blocks_meta.is_empty() {
            snapshot.commit().await.map_err(|e| {
                AppError::InternalError(format!(
                    "Failed to commit subscribe snapshot (empty batch): {}",
                    e
                ))
            })?;
            if coalesce_empty_blocks && pending_empty_range.is_some() {
                let mut sent_event = false;
                if flush_pending_empty_range(
                    &mut pending_empty_range,
                    &mut pending_empty_range_started_at,
                    &tx,
                    state.sse_send_timeout,
                    &mut sent_event,
                    &mut last_heartbeat,
                    &mut current_height,
                )
                .await
                .is_err()
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(state.poll_interval).await;
            continue;
        }

        // Validate contiguous blocks and check for reorgs (parent hash mismatch)
        let mut expected_height = current_height;

        // For boundary reorg detection: use tracked last_block_hash or fetch previous block
        // On first batch, fetch the previous block's hash if current_height > 0
        if last_block_hash.is_none() && current_height > 0 {
            last_block_hash =
                get_block_hash_at_height_tx(&mut snapshot, current_height - 1).await?;
        }
        let mut prev_hash: Option<&str> = last_block_hash.as_deref();
        let mut prev_height: Option<i64> =
            if current_height > 0 && last_block_hash.is_some() {
                Some(current_height - 1)
            } else {
                None
            };

        for block in &blocks_meta {
            // Check for height gaps
            if block.height != expected_height {
                if !state.allow_sparse_blocks {
                    let safe_restart_height = (expected_height - 1).max(0);

                    snapshot.commit().await.map_err(|e| {
                        AppError::InternalError(format!(
                            "Failed to commit subscribe snapshot (gap): {}",
                            e
                        ))
                    })?;

                    let control = ControlEvent::reset_required(
                        safe_restart_height,
                        format!(
                            "Block gap detected: expected height {}, got {}",
                            expected_height, block.height
                        ),
                    );
                    let event = SseEvent::Control(control);
                    if send_event(&tx, "control", &event, state.sse_send_timeout)
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    return Ok(());
                }

                let gap = block.height - expected_height;
                if gap > MAX_SPARSE_GAP {
                    let safe_restart_height = (expected_height - 1).max(0);

                    snapshot.commit().await.map_err(|e| {
                        AppError::InternalError(format!(
                            "Failed to commit subscribe snapshot (gap too large): {}",
                            e
                        ))
                    })?;

                    let control = ControlEvent::reset_required(
                        safe_restart_height,
                        format!(
                            "Block gap too large: expected height {}, got {} (gap {}, max {})",
                            expected_height, block.height, gap, MAX_SPARSE_GAP
                        ),
                    );
                    let event = SseEvent::Control(control);
                    if send_event(&tx, "control", &event, state.sse_send_timeout)
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    return Ok(());
                }

                info!(
                    expected = expected_height,
                    actual = block.height,
                    gap = gap,
                    max_gap = MAX_SPARSE_GAP,
                    "Block gap detected; allow_sparse_blocks enabled, continuing"
                );
                prev_hash = None;
                prev_height = None;
            }

            // Check for parent hash mismatch (reorg detection) only on contiguous blocks
            let is_contiguous = prev_height.map(|h| h + 1 == block.height).unwrap_or(false);
            if is_contiguous {
                if let Some(expected_parent) = prev_hash {
                    if block.parent_hash != expected_parent {
                        let safe_restart_height = (block.height - 1).max(0);

                        snapshot.commit().await.map_err(|e| {
                            AppError::InternalError(format!(
                                "Failed to commit subscribe snapshot (reorg): {}",
                                e
                            ))
                        })?;

                        let control = ControlEvent::reset_required(
                            safe_restart_height,
                            format!(
                                "Reorg detected at height {}: parent hash mismatch",
                                block.height
                            ),
                        );
                        let event = SseEvent::Control(control);
                        if send_event(&tx, "control", &event, state.sse_send_timeout)
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                        return Ok(());
                    }
                }
            }

            prev_hash = Some(&block.hash);
            prev_height = Some(block.height);
            expected_height = block.height + 1;
        }

        // Build and send block bundles (with inline merkle updates)
        let build_result = build_block_bundles_tx(
            &mut snapshot,
            &local_session_id_bytes,
            &upstream_session_id_bytes,
            &blocks_meta,
            nullifier_mode,
            include_tx_meta,
            include_raw_tx,
            include_identifiers,
            include_all_dust,
            identifiers_source,
            last_merkle_end_index,
            inline_merkle_updates_enabled,
            &state.ledger_state_store,
        )
        .await?;

        match build_result {
            BuildBlockBundlesResult::Ready {
                bundles,
                new_merkle_end_index,
                block_end_indices,
            } => {
                last_merkle_end_index = new_merkle_end_index;

                snapshot.commit().await.map_err(|e| {
                    AppError::InternalError(format!(
                        "Failed to commit subscribe snapshot (bundle build): {}",
                        e
                    ))
                })?;

                let mut sent_event = false;

                for mut bundle in bundles {
                    let height = bundle.meta.height;
                    if let Some(cursor) = from_cursor {
                        if height == cursor.block_height {
                            bundle.items.retain(|item| {
                                parse_cursor(&item.sequence)
                                    .map(|sequence| sequence > cursor)
                                    .unwrap_or(true)
                            });
                            from_cursor = None;
                        }
                    }
                    if bundle.items.is_empty() {
                        if coalesce_empty_blocks {
                            let merkle_end_index = block_end_indices
                                .get(&height)
                                .copied()
                                .unwrap_or(last_merkle_end_index);
                            match pending_empty_range.as_mut() {
                                Some(range) => {
                                    range.end = height;
                                    range.merkle_end_index = merkle_end_index;
                                }
                                None => {
                                    pending_empty_range = Some(PendingEmptyRange {
                                        start: height,
                                        end: height,
                                        merkle_end_index,
                                    });
                                    pending_empty_range_started_at = Some(std::time::Instant::now());
                                }
                            }
                            if pending_empty_range_started_at.is_none() {
                                pending_empty_range_started_at = Some(std::time::Instant::now());
                            }
                            if let Some(cursor) = from_cursor {
                                if cursor.block_height <= height {
                                    from_cursor = None;
                                }
                            }
                            if matches!(
                                pending_empty_range.as_ref(),
                                Some(range) if range.len() >= COALESCE_EMPTY_BLOCKS_MAX_RANGE
                            ) {
                                if flush_pending_empty_range(
                                    &mut pending_empty_range,
                                    &mut pending_empty_range_started_at,
                                    &tx,
                                    state.sse_send_timeout,
                                    &mut sent_event,
                                    &mut last_heartbeat,
                                    &mut current_height,
                                )
                                .await
                                .is_err()
                                {
                                    return Ok(());
                                }
                            }
                            if pending_empty_range_started_at
                                .map(|started| started.elapsed() >= COALESCE_EMPTY_BLOCKS_MAX_DELAY)
                                .unwrap_or(false)
                            {
                                if flush_pending_empty_range(
                                    &mut pending_empty_range,
                                    &mut pending_empty_range_started_at,
                                    &tx,
                                    state.sse_send_timeout,
                                    &mut sent_event,
                                    &mut last_heartbeat,
                                    &mut current_height,
                                )
                                .await
                                .is_err()
                                {
                                    return Ok(());
                                }
                            }
                            current_height = height + 1;
                            continue;
                        }
                        let merkle_end_index = block_end_indices
                            .get(&height)
                            .copied()
                            .unwrap_or(last_merkle_end_index);
                        let control = ControlEvent::merkle_progress(height, merkle_end_index);
                        let event = SseEvent::Control(control);
                        if send_event(&tx, "control", &event, state.sse_send_timeout)
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                        sent_event = true;
                        last_heartbeat = std::time::Instant::now();
                        current_height = height + 1;
                        continue;
                    }
                    if coalesce_empty_blocks && pending_empty_range.is_some() {
                        if flush_pending_empty_range(
                            &mut pending_empty_range,
                            &mut pending_empty_range_started_at,
                            &tx,
                            state.sse_send_timeout,
                            &mut sent_event,
                            &mut last_heartbeat,
                            &mut current_height,
                        )
                        .await
                        .is_err()
                        {
                            return Ok(());
                        }
                    }
                    let event = SseEvent::Block(bundle);
                    if send_event(&tx, "block", &event, state.sse_send_timeout)
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    sent_event = true;
                    last_heartbeat = std::time::Instant::now();
                    current_height = height + 1;
                }

                if sent_event {
                    last_heartbeat = std::time::Instant::now();
                }
            }
            BuildBlockBundlesResult::NotReady(info) => {
                snapshot.commit().await.map_err(|e| {
                    AppError::InternalError(format!(
                        "Failed to commit subscribe snapshot (not ready): {}",
                        e
                    ))
                })?;

                if coalesce_empty_blocks && pending_empty_range.is_some() {
                    let mut sent_event = false;
                    if flush_pending_empty_range(
                        &mut pending_empty_range,
                        &mut pending_empty_range_started_at,
                        &tx,
                        state.sse_send_timeout,
                        &mut sent_event,
                        &mut last_heartbeat,
                        &mut current_height,
                    )
                    .await
                    .is_err()
                    {
                        return Ok(());
                    }
                }

                let should_emit = last_not_ready_sent_at
                    .map(|t| t.elapsed() >= NOT_READY_EVENT_MIN_INTERVAL)
                    .unwrap_or(true);

                if should_emit {
                    let control = ControlEvent::not_ready(
                        build_not_ready_reason(&info),
                        info.required_from_index,
                        info.required_to_index,
                        info.available_end_index,
                        info.snapshot_block_height,
                        info.protocol_version,
                        Some(state.poll_interval.as_millis() as u64),
                    );
                    let event = SseEvent::Control(control);
                    if send_event(&tx, "control", &event, state.sse_send_timeout).await.is_err() {
                        return Ok(());
                    }
                    last_not_ready_sent_at = Some(std::time::Instant::now());
                }

                // CRITICAL FIX: Send heartbeat during NotReady state so clients can
                // advance their lastSyncedHeight cursor. Without this, the client's
                // cursor stays stuck at the last block with relevant transactions,
                // causing "Local sync behind wallet ready height" errors during spends.
                if last_heartbeat.elapsed() >= state.heartbeat_interval {
                    let event = SseEvent::Heartbeat {
                        watermarks: watermarks.clone(),
                        current_height: current_height.to_string(),
                    };
                    if send_event(&tx, "heartbeat", &event, state.sse_send_timeout)
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    last_heartbeat = std::time::Instant::now();
                }

                tokio::time::sleep(state.poll_interval).await;
                continue;
            }
            BuildBlockBundlesResult::DegradedMerkleSync {
                block_height,
                block_end_index,
                reason,
            } => {
                snapshot.commit().await.map_err(|e| {
                    AppError::InternalError(format!(
                        "Failed to commit subscribe snapshot (degraded merkle sync): {}",
                        e
                    ))
                })?;

                let control =
                    ControlEvent::degraded_merkle_sync(block_height, block_end_index, reason);
                let event = SseEvent::Control(control);
                let _ = send_event(&tx, "control", &event, state.sse_send_timeout).await;
                return Ok(());
            }
            BuildBlockBundlesResult::ResetRequired {
                safe_restart_height,
                reason,
            } => {
                snapshot.commit().await.map_err(|e| {
                    AppError::InternalError(format!(
                        "Failed to commit subscribe snapshot (reset required): {}",
                        e
                    ))
                })?;

                let control = ControlEvent::reset_required(safe_restart_height, reason);
                let event = SseEvent::Control(control);
                let _ = send_event(&tx, "control", &event, state.sse_send_timeout).await;
                return Ok(());
            }
        }

        // Update last_block_hash for next batch boundary check
        if let Some(last_block) = blocks_meta.last() {
            last_block_hash = Some(last_block.hash.clone());
        }

        // Heartbeat timer is updated when we actually send a block or heartbeat.
    }

    Ok(())
}

async fn flush_pending_empty_range(
    pending: &mut Option<PendingEmptyRange>,
    pending_started_at: &mut Option<std::time::Instant>,
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    timeout: Duration,
    sent_event: &mut bool,
    last_heartbeat: &mut std::time::Instant,
    current_height: &mut i64,
) -> Result<(), ()> {
    let Some(range) = pending.take() else {
        return Ok(());
    };
    *pending_started_at = None;

    let control = ControlEvent::merkle_progress(range.end, range.merkle_end_index);
    let event = SseEvent::Control(control);
    send_event(tx, "control", &event, timeout).await?;
    *sent_event = true;
    *last_heartbeat = std::time::Instant::now();
    *current_height = range.end + 1;
    Ok(())
}

/// Send an SSE event through the channel with timeout for slow clients
async fn send_event<T: Serialize>(
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    event_type: &str,
    data: &T,
    timeout: Duration,
) -> Result<(), ()> {
    let json = serde_json::to_string(data).map_err(|_| ())?;
    let event = Event::default().event(event_type).data(json);

    // Use timeout to handle slow clients - drop them if they can't keep up
    match tokio::time::timeout(timeout, tx.send(Ok(event))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => {
            warn!("Failed to send SSE event, client disconnected");
            Err(())
        }
        Err(_) => {
            warn!(
                timeout_secs = timeout.as_secs(),
                "SSE send timeout, dropping slow client"
            );
            Err(())
        }
    }
}

#[derive(Debug)]
enum BuildBlockBundlesResult {
    Ready {
        bundles: Vec<BlockBundle>,
        new_merkle_end_index: i64,
        block_end_indices: HashMap<i64, i64>,
    },
    NotReady(MerkleUpdateNotReady),
    DegradedMerkleSync {
        block_height: i64,
        block_end_index: i64,
        reason: String,
    },
    ResetRequired {
        safe_restart_height: i64,
        reason: String,
    },
}

/// Build block bundles for a range of blocks (reuses logic from feed.rs).
/// Returns NotReady if the ledger state cannot supply a required merkle update.
async fn build_block_bundles_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    local_session_id_bytes: &[u8],
    upstream_session_id_bytes: &[u8],
    blocks_meta: &[crate::feed::BlockMeta],
    nullifier_mode: NullifierStreaming,
    include_tx_meta: bool,
    include_raw_tx: bool,
    include_identifiers: bool,
    include_all_dust: bool,
    identifiers_source: IdentifiersSource,
    mut last_merkle_end_index: i64,
    inline_merkle_updates_enabled: bool,
    ledger_state_store: &Arc<LedgerStateStore>,
) -> Result<BuildBlockBundlesResult, AppError> {
    if blocks_meta.is_empty() {
        return Ok(BuildBlockBundlesResult::Ready {
            bundles: Vec::new(),
            new_merkle_end_index: last_merkle_end_index,
            block_end_indices: HashMap::new(),
        });
    }

    let from_height = blocks_meta.first().unwrap().height;
    let to_height = blocks_meta.last().unwrap().height;

    let block_merkle_states = get_block_merkle_state_in_range_tx(tx, from_height, to_height).await?;
    let mut block_merkle_by_height: HashMap<i64, crate::db::BlockMerkleStateRow> = HashMap::new();
    for state in block_merkle_states {
        block_merkle_by_height.insert(state.block_height, state);
    }
    let mut block_end_indices: HashMap<i64, i64> = HashMap::new();
    let mut last_end_index: Option<i64> = None;
    for block_meta in blocks_meta {
        let height = block_meta.height;
        let state = block_merkle_by_height.get(&height).ok_or_else(|| {
            AppError::InternalError(format!(
                "Missing block merkle state for height {}",
                height
            ))
        })?;
        let end_index = state.end_index;
        if let Some(last) = last_end_index {
            if end_index < last {
                // Non-monotonic merkle end index indicates a DB regression.
                // Signal reset to the client so they can resync properly instead
                // of silently clamping which would hide the issue and cause merkle desync.
                warn!(
                    height,
                    end_index,
                    last_end_index = last,
                    "Non-monotonic merkle end index detected; signaling reset"
                );
                // Safe restart is the block before the regression so client can resync
                let safe_restart_height = (height - 1).max(0);
                return Ok(BuildBlockBundlesResult::ResetRequired {
                    safe_restart_height,
                    reason: format!(
                        "Merkle end index regression detected at height {}: expected >= {}, got {}",
                        height, last, end_index
                    ),
                });
            }
        }
        block_end_indices.insert(height, end_index);
        last_end_index = Some(end_index);
    }

    // Fetch relevant transactions for this range
    let relevant_txs =
        get_relevant_transactions_tx(tx, upstream_session_id_bytes, from_height, to_height).await?;

    // DIAGNOSTIC: Log relevant transactions query results
    if !relevant_txs.is_empty() {
        let heights: Vec<i64> = relevant_txs.iter().map(|t| t.block_height).collect();
        tracing::info!(
            from_height = from_height,
            to_height = to_height,
            relevant_tx_count = relevant_txs.len(),
            relevant_heights = ?heights,
            upstream_session_id = hex::encode(upstream_session_id_bytes),
            "Found relevant transactions in range"
        );
    } else {
        tracing::debug!(
            from_height = from_height,
            to_height = to_height,
            upstream_session_id = hex::encode(upstream_session_id_bytes),
            "No relevant transactions in range"
        );
    }

    // Collect tx IDs for batch queries (shielded txs)
    let shielded_tx_ids: Vec<i64> = relevant_txs.iter().map(|tx| tx.tx_id).collect();

    // Fetch zswap events for relevant txs
    let zswap_events = get_zswap_events_for_txs_tx(tx, &shielded_tx_ids).await?;

    // Group zswap events by tx_id
    let mut zswap_by_tx: HashMap<i64, Vec<String>> = HashMap::new();
    let mut zswap_raw_by_tx: HashMap<i64, Vec<Vec<u8>>> = HashMap::new();
    for event in zswap_events.iter() {
        zswap_by_tx
            .entry(event.tx_id)
            .or_default()
            .push(hex::encode(&event.raw));
        zswap_raw_by_tx
            .entry(event.tx_id)
            .or_default()
            .push(event.raw.clone());
    }

    // Fetch dust events
    // When include_all_dust is true, fetch ALL dust events for proper merkle tree sync
    // (the SDK needs all events in sequence since there's no collapsed update mechanism for dust)
    let dust_events = if include_all_dust {
        let events = get_all_dust_events_tx(tx, from_height, to_height).await?;
        info!(
            from_height,
            to_height,
            dust_event_count = events.len(),
            include_all_dust = true,
            "Fetched ALL dust events for block range"
        );
        events
    } else {
        let events =
            get_dust_events_tx(tx, local_session_id_bytes, from_height, to_height).await?;
        info!(
            from_height,
            to_height,
            dust_event_count = events.len(),
            include_all_dust = false,
            "Fetched wallet-filtered dust events for block range"
        );
        events
    };

    // Fetch unshielded UTXO events
    let unshielded_events =
        get_unshielded_utxo_events_tx(tx, local_session_id_bytes, from_height, to_height).await?;

    // Collect ALL tx_ids that need contract actions (shielded + unshielded + dust)
    // This ensures ALL contract actions are fetched, including dust-only txs (e.g., contract updates)
    let mut all_tx_ids_for_actions: HashSet<i64> = shielded_tx_ids.iter().copied().collect();
    for event in &unshielded_events {
        all_tx_ids_for_actions.insert(event.tx_id);
    }
    for dust in &dust_events {
        all_tx_ids_for_actions.insert(dust.tx_id);
    }
    let all_tx_ids: Vec<i64> = all_tx_ids_for_actions.into_iter().collect();

    // Fetch contract actions for ALL relevant txs (shielded + unshielded + dust)
    let contract_actions = get_contract_actions_for_txs_tx(tx, &all_tx_ids).await?;

    // Group contract actions by tx_id
    let mut actions_by_tx: HashMap<i64, Vec<_>> = HashMap::new();
    for (tx_id, action) in contract_actions {
        actions_by_tx.entry(tx_id).or_default().push(action);
    }

    // Determine wallet-relevant tx_ids for transaction metadata
    let mut wallet_tx_ids: HashSet<i64> = HashSet::new();
    for tx in &relevant_txs {
        wallet_tx_ids.insert(tx.tx_id);
    }
    for dust in &dust_events {
        wallet_tx_ids.insert(dust.tx_id);
    }
    for event in &unshielded_events {
        wallet_tx_ids.insert(event.tx_id);
    }

    let tx_meta_rows = if include_tx_meta {
        let ids: Vec<i64> = wallet_tx_ids.iter().copied().collect();
        get_transaction_metadata_tx(
            tx,
            &ids,
            include_raw_tx,
            include_identifiers,
            identifiers_source,
        )
        .await?
    } else {
        Vec::new()
    };

    // Fetch ALL zswap input events for global nullifiers
    // Skip fetch entirely if disabled to avoid unnecessary DB load
    let global_zswap_inputs = match nullifier_mode {
        NullifierStreaming::Disabled => Vec::new(),
        _ => get_all_zswap_input_events_in_range_tx(tx, from_height, to_height).await?,
    };

    // Build block bundles
    let mut block_bundles: Vec<BlockBundle> = Vec::new();
    let mut ledger_snapshot: Option<LedgerStateSnapshot> = None;

    for block_meta in blocks_meta {
        let height = block_meta.height;
        let mut sortable_items: Vec<SortableFeedItem> = Vec::new();
        let block_merkle_state = block_merkle_by_height.get(&height).ok_or_else(|| {
            AppError::InternalError(format!(
                "Missing block merkle state for height {}",
                height
            ))
        })?;
        let mut validation_cursor = if last_merkle_end_index < 0 {
            0
        } else {
            last_merkle_end_index
        };

        // Add transaction metadata for wallet-relevant txs in this block
        if include_tx_meta {
            for meta in tx_meta_rows.iter().filter(|row| row.block_height == height) {
                let identifiers = meta.identifiers.as_ref().map(|items| {
                    items.iter().map(|item| hex::encode(item)).collect()
                });
                let merkle_root = meta
                    .merkle_tree_root
                    .as_ref()
                    .map(|root| hex::encode(root));
                let raw = meta.raw.as_ref().map(|bytes| hex::encode(bytes));

                let sequence = SequenceKey {
                    block_height: height,
                    tx_id: meta.tx_id,
                    phase: PHASE_TX,
                    ordinal: 0,
                };
                // Get contract actions for this transaction (for dust-only txs like contract updates)
                let tx_contract_actions = actions_by_tx
                    .get(&meta.tx_id)
                    .cloned()
                    .unwrap_or_default();
                sortable_items.push(SortableFeedItem::new(
                    sequence,
                    FeedItem::TransactionMeta(TransactionMeta {
                        tx_id: meta.tx_id,
                        tx_hash: hex::encode(&meta.tx_hash),
                        block_height: height,
                        protocol_version: meta.protocol_version,
                        transaction_result: meta.transaction_result.clone(),
                        identifiers,
                        fees: TransactionFees {
                            paid: decode_fee_option(&meta.paid_fees),
                            estimated: decode_fee_option(&meta.estimated_fees),
                        },
                        merkle: TransactionMerkle {
                            root: merkle_root,
                            start_index: meta.start_index,
                            end_index: meta.end_index,
                        },
                        raw,
                        contract_actions: tx_contract_actions,
                    }),
                ));
            }
        }

        // Add shielded relevant transactions for this block
        // Before each tx, check if we need to inject merkle collapsed updates
        for rtx in relevant_txs.iter().filter(|tx| tx.block_height == height) {
            let (tx_start_index, tx_end_index) = match (rtx.start_index, rtx.end_index) {
                (Some(start), Some(end)) => (start, end),
                _ => {
                    let derived = zswap_raw_by_tx
                        .get(&rtx.tx_id)
                        .and_then(|events| derive_merkle_indices_from_zswap_events(events));
                    let (derived_start, derived_end) = match derived {
                        Some(indices) => indices,
                        None => {
                            let reason = format!(
                                "Missing merkle indices for tx {} and unable to derive from zswap outputs",
                                rtx.tx_id
                            );
                            return Ok(BuildBlockBundlesResult::DegradedMerkleSync {
                                block_height: height,
                                block_end_index: block_merkle_state.end_index,
                                reason,
                            });
                        }
                    };
                    if let Some(start) = rtx.start_index {
                        if start != derived_start {
                            let reason = format!(
                                "Mismatched merkle start index for tx {} (expected {}, got {})",
                                rtx.tx_id, derived_start, start
                            );
                            return Ok(BuildBlockBundlesResult::DegradedMerkleSync {
                                block_height: height,
                                block_end_index: block_merkle_state.end_index,
                                reason,
                            });
                        }
                    }
                    if let Some(end) = rtx.end_index {
                        if end != derived_end {
                            let reason = format!(
                                "Mismatched merkle end index for tx {} (expected {}, got {})",
                                rtx.tx_id, derived_end, end
                            );
                            return Ok(BuildBlockBundlesResult::DegradedMerkleSync {
                                block_height: height,
                                block_end_index: block_merkle_state.end_index,
                                reason,
                            });
                        }
                    }
                    (derived_start, derived_end)
                }
            };

            if tx_end_index <= tx_start_index {
                // WORKAROUND: Skip transactions with empty merkle ranges instead of blocking sync.
                // This can happen when upstream indexer marks a tx as "relevant" but has no zswap events.
                // Log a warning and continue processing other transactions.
                tracing::warn!(
                    tx_id = rtx.tx_id,
                    block_height = height,
                    start_index = tx_start_index,
                    end_index = tx_end_index,
                    "Skipping transaction with invalid merkle indices (start >= end). \
                     This indicates upstream data inconsistency - tx marked as relevant but has no zswap events."
                );
                continue;
            }

            if tx_start_index < validation_cursor {
                let reason = format!(
                    "Non-monotonic merkle start index at height {} (tx {}, start={}, cursor={})",
                    height, rtx.tx_id, tx_start_index, validation_cursor
                );
                return Ok(BuildBlockBundlesResult::ResetRequired {
                    safe_restart_height: (height - 1).max(0),
                    reason,
                });
            }
            validation_cursor = tx_end_index;

            if inline_merkle_updates_enabled {
                // Check for merkle tree gap and inject collapsed update if needed
                // Gap exists if tx starts after our current merkle position
                // last_merkle_end_index of -1 means fresh sync (need update from 0)
                let need_update = if last_merkle_end_index < 0 {
                    tx_start_index > 0
                } else {
                    tx_start_index > last_merkle_end_index
                };

                if need_update {
                    let from_index = if last_merkle_end_index < 0 {
                        0
                    } else {
                        last_merkle_end_index
                    };
                    let to_index = tx_start_index;
                    if from_index < to_index {
                        let snapshot = match ledger_snapshot.as_ref() {
                            Some(snapshot) => snapshot,
                            None => {
                                let fetched = ledger_state_store.get_snapshot().await;
                                match fetched {
                                    Ok(Some(snapshot)) => {
                                        ledger_snapshot = Some(snapshot);
                                        ledger_snapshot.as_ref().unwrap()
                                    }
                                    Ok(None) => {
                                        let info = not_ready_info_from_snapshot(
                                            None,
                                            from_index as u64,
                                            to_index as u64,
                                        );
                                        return Ok(BuildBlockBundlesResult::NotReady(info));
                                    }
                                    Err(LedgerStateStoreError::Fetch(_))
                                    | Err(LedgerStateStoreError::Timeout(_)) => {
                                        let info = not_ready_info_from_snapshot(
                                            None,
                                            from_index as u64,
                                            to_index as u64,
                                        );
                                        return Ok(BuildBlockBundlesResult::NotReady(info));
                                    }
                                    Err(e) => return Err(AppError::InternalError(e.to_string())),
                                }
                            }
                        };

                        let update = match LedgerStateStore::compute_merkle_update(
                            snapshot,
                            from_index as u64,
                            to_index as u64,
                        ) {
                            Ok(update) => update,
                            Err(LedgerStateStoreError::NotReady(info)) => {
                                return Ok(BuildBlockBundlesResult::NotReady(info));
                            }
                            Err(e) => return Err(AppError::InternalError(e.to_string())),
                        };

                        let sequence = SequenceKey {
                            block_height: height,
                            tx_id: rtx.tx_id,
                            phase: PHASE_TX,
                            ordinal: -1,
                        };
                        sortable_items.push(SortableFeedItem::new(
                            sequence,
                            FeedItem::MerkleCollapsedUpdate(MerkleCollapsedUpdate {
                                position: MerkleUpdatePosition::BeforeTx,
                                start_index: update.start_index as i64,
                                end_index: update.end_index as i64,
                                update: hex::encode(update.update),
                                protocol_version: update.protocol_version,
                            }),
                        ));

                        info!(
                            from_index,
                            to_index,
                            tx_id = rtx.tx_id,
                            source = "ledger_state_store",
                            "Injected inline merkle collapsed update"
                        );
                        metrics::counter!("merkle_inline_updates_total").increment(1);
                    }
                }

                // Update merkle position to this transaction's end index
                last_merkle_end_index = tx_end_index;
            }

            let sequence = SequenceKey {
                block_height: height,
                tx_id: rtx.tx_id,
                phase: PHASE_TX,
                ordinal: 1,
            };
            sortable_items.push(SortableFeedItem::new(
                sequence,
                FeedItem::ShieldedRelevantTx(ShieldedRelevantTx {
                    tx_id: rtx.tx_id,
                    tx_hash: hex::encode(&rtx.tx_hash),
                    block_height: height,
                    zswap_events: zswap_by_tx.get(&rtx.tx_id).cloned().unwrap_or_default(),
                    contract_actions: actions_by_tx.get(&rtx.tx_id).cloned().unwrap_or_default(),
                    fees_paid: decode_fee_string(&rtx.paid_fees),
                    merkle_start_index: Some(tx_start_index),
                    merkle_end_index: Some(tx_end_index),
                }),
            ));
        }

        if inline_merkle_updates_enabled {
            let block_end_index = block_merkle_state.end_index;
            let from_index = if last_merkle_end_index < 0 {
                0
            } else {
                last_merkle_end_index
            };

            if block_end_index > from_index {
                let snapshot = match ledger_snapshot.as_ref() {
                    Some(snapshot) => snapshot,
                    None => {
                        let fetched = ledger_state_store.get_snapshot().await;
                        match fetched {
                            Ok(Some(snapshot)) => {
                                ledger_snapshot = Some(snapshot);
                                ledger_snapshot.as_ref().unwrap()
                            }
                            Ok(None) => {
                                let info = not_ready_info_from_snapshot(
                                    None,
                                    from_index as u64,
                                    block_end_index as u64,
                                );
                                return Ok(BuildBlockBundlesResult::NotReady(info));
                            }
                            Err(LedgerStateStoreError::Fetch(_))
                            | Err(LedgerStateStoreError::Timeout(_)) => {
                                let info = not_ready_info_from_snapshot(
                                    None,
                                    from_index as u64,
                                    block_end_index as u64,
                                );
                                return Ok(BuildBlockBundlesResult::NotReady(info));
                            }
                            Err(e) => return Err(AppError::InternalError(e.to_string())),
                        }
                    }
                };

                let update = match LedgerStateStore::compute_merkle_update(
                    snapshot,
                    from_index as u64,
                    block_end_index as u64,
                ) {
                    Ok(update) => update,
                    Err(LedgerStateStoreError::NotReady(info)) => {
                        return Ok(BuildBlockBundlesResult::NotReady(info));
                    }
                    Err(e) => return Err(AppError::InternalError(e.to_string())),
                };

                let sequence = SequenceKey {
                    block_height: height,
                    tx_id: i64::MAX,
                    phase: PHASE_POST_TX,
                    ordinal: i64::MAX,
                };
                sortable_items.push(SortableFeedItem::new(
                    sequence,
                    FeedItem::MerkleCollapsedUpdate(MerkleCollapsedUpdate {
                        position: MerkleUpdatePosition::AfterBlock,
                        start_index: update.start_index as i64,
                        end_index: update.end_index as i64,
                        update: hex::encode(update.update),
                        protocol_version: update.protocol_version,
                    }),
                ));

                last_merkle_end_index = block_end_index;
            }
        }

        // Add ZswapInputRaw for ALL zswap input events in this block (global spend detection)
        // Raw bytes are passed through - client parses with midnight-ledger SDK
        for event in global_zswap_inputs.iter().filter(|e| e.block_height == height) {
            let sequence = SequenceKey {
                block_height: height,
                tx_id: event.tx_id,
                phase: PHASE_LEDGER_EVENT,
                ordinal: event.ledger_event_id,
            };
            sortable_items.push(SortableFeedItem::new(
                sequence,
                FeedItem::ZswapInputRaw(ZswapInputRaw {
                    tx_id: event.tx_id,
                    tx_hash: hex::encode(&event.tx_hash),
                    ledger_event_id: event.ledger_event_id,
                    block_height: height,
                    raw: hex::encode(&event.raw),
                }),
            ));
        }

        // Add dust events for this block
        for dust in dust_events.iter().filter(|d| d.block_height == height) {
            let sequence = SequenceKey {
                block_height: height,
                tx_id: dust.tx_id,
                phase: PHASE_LEDGER_EVENT,
                ordinal: dust.ledger_event_id,
            };
            sortable_items.push(SortableFeedItem::new(
                sequence,
                FeedItem::DustLedgerEvent(DustLedgerEventRaw {
                    tx_id: dust.tx_id,
                    ledger_event_id: dust.ledger_event_id,
                    block_height: height,
                    raw: hex::encode(&dust.raw),
                }),
            ));
        }

        // Add unshielded UTXO events for this block
        for event in unshielded_events.iter().filter(|d| d.block_height == height) {
            let value = decode_u128_value(&event.value_bytes).to_string();
            let fees_paid = decode_fee_string(&event.paid_fees);
            let sequence = SequenceKey {
                block_height: height,
                tx_id: event.tx_id,
                phase: PHASE_UNSHIELDED,
                ordinal: event.utxo_id,
            };
            // Get ALL contract actions for this transaction (not just LIMIT 1!)
            let contract_actions = actions_by_tx
                .get(&event.tx_id)
                .cloned()
                .unwrap_or_default();
            sortable_items.push(SortableFeedItem::new(
                sequence,
                FeedItem::UnshieldedUtxo(UnshieldedUtxoEvent {
                    tx_id: event.tx_id,
                    tx_hash: hex::encode(&event.tx_hash),
                    block_height: height,
                    is_create: event.is_create,
                    address: hex::encode(&event.address),
                    intent_hash: hex::encode(&event.intent_hash),
                    output_index: event.output_index,
                    token_type: hex::encode(&event.token_type),
                    value,
                    initial_nonce: hex::encode(&event.initial_nonce),
                    registered_for_dust_generation: event.registered_for_dust_generation,
                    ctime: event.ctime,
                    fees_paid,
                    contract_actions,
                }),
            ));
        }

        // Sort items by global sequence ordering
        sortable_items.sort_by(|a, b| a.sequence.cmp(&b.sequence));

        // Validate event ordering (detect gaps in ledger_event_id sequences)
        validate_event_ordering(&sortable_items, height);

        let items: Vec<OrderedFeedItem> = sortable_items
            .into_iter()
            .map(|s| OrderedFeedItem {
                sequence: s.sequence.to_cursor(),
                item: s.item,
            })
            .collect();

        block_bundles.push(BlockBundle {
            meta: block_meta.clone(),
            items,
        });
    }

    Ok(BuildBlockBundlesResult::Ready {
        bundles: block_bundles,
        new_merkle_end_index: last_merkle_end_index,
        block_end_indices,
    })
}

fn derive_merkle_indices_from_zswap_events(raw_events: &[Vec<u8>]) -> Option<(i64, i64)> {
    let mut min_index: Option<u64> = None;
    let mut max_index: Option<u64> = None;

    for raw in raw_events {
        let event = match tagged_deserialize::<LedgerEvent<DefaultDB>>(&mut raw.as_slice()) {
            Ok(event) => event,
            Err(_) => continue,
        };
        if let EventDetails::ZswapOutput { mt_index, .. } = event.content {
            min_index = Some(min_index.map_or(mt_index, |min| min.min(mt_index)));
            max_index = Some(max_index.map_or(mt_index, |max| max.max(mt_index)));
        }
    }

    let min_index = min_index?;
    let max_index = max_index?;
    let end_index = max_index.checked_add(1)?;

    Some((min_index as i64, end_index as i64))
}

fn not_ready_info_from_snapshot(
    snapshot: Option<&LedgerStateSnapshot>,
    from_index: u64,
    to_index: u64,
) -> MerkleUpdateNotReady {
    MerkleUpdateNotReady {
        required_from_index: from_index,
        required_to_index: to_index,
        available_end_index: snapshot.and_then(|snap| snap.available_end_index()),
        snapshot_block_height: snapshot.map(|snap| snap.block_height),
        protocol_version: snapshot.map(|snap| snap.protocol_version),
    }
}

fn build_not_ready_reason(info: &MerkleUpdateNotReady) -> String {
    let available_end = info
        .available_end_index
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let snapshot_height = info
        .snapshot_block_height
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let protocol_version = info
        .protocol_version
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    format!(
        "MERKLE_UPDATE_NOT_READY: required {} to {} (exclusive), available_end_index={}, snapshot_height={}, protocol_version={}",
        info.required_from_index,
        info.required_to_index,
        available_end,
        snapshot_height,
        protocol_version
    )
}

/// A sortable feed item wrapper for ordering by sequence
struct SortableFeedItem {
    sequence: SequenceKey,
    item: FeedItem,
}

impl SortableFeedItem {
    fn new(sequence: SequenceKey, item: FeedItem) -> Self {
        Self { sequence, item }
    }
}

/// Validate event ordering within a block and detect gaps in ledger_event_id sequences.
/// This is an informational check - gaps are logged and recorded as metrics but don't block processing.
fn validate_event_ordering(items: &[SortableFeedItem], block_height: i64) {
    // Group real ledger_event_ids by tx_id (exclude synthetic values 0 and i64::MAX)
    let mut events_by_tx: HashMap<i64, Vec<i64>> = HashMap::new();

    for item in items {
        let (tx_id, ledger_event_id) = match &item.item {
            FeedItem::ZswapInputRaw(event) => (event.tx_id, event.ledger_event_id),
            FeedItem::DustLedgerEvent(event) => (event.tx_id, event.ledger_event_id),
            _ => continue,
        };
        if ledger_event_id > 0 {
            events_by_tx.entry(tx_id).or_default().push(ledger_event_id);
        }
    }

    // Check each transaction for gaps in ledger_event_id sequence
    for (tx_id, mut event_ids) in events_by_tx {
        if event_ids.len() < 2 {
            continue; // Can't have a gap with 0 or 1 events
        }

        event_ids.sort();

        // Check for gaps (each ID should be previous + 1)
        for window in event_ids.windows(2) {
            let prev = window[0];
            let curr = window[1];
            if curr != prev + 1 {
                tracing::warn!(
                    block_height,
                    tx_id,
                    expected_event_id = prev + 1,
                    actual_event_id = curr,
                    gap_size = curr - prev - 1,
                    "Detected gap in ledger_event_id sequence"
                );
                crate::metrics::record_event_ordering_anomaly("ledger_event_gap");
            }
        }
    }
}
