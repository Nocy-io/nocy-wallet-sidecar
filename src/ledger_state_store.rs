//! Ledger state store client for collapsed merkle updates.
//!
//! Reads the canonical ledger state from the NATS JetStream object store and
//! builds collapsed merkle updates for the SDK.

use async_nats::Client as NatsClient;
use async_nats::jetstream;
use async_nats::jetstream::object_store::{GetErrorKind, ObjectStore};
use midnight_ledger::structure::LedgerState;
use midnight_serialize::{tagged_deserialize, tagged_serialize};
use midnight_storage::DefaultDB;
use midnight_transient_crypto::merkle_tree::MerkleTreeCollapsedUpdate;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tracing::{debug, info};

const BUCKET_NAME: &str = "ledger_state_store";
const OBJECT_NAME: &str = "ledger_state";
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct LedgerStateSnapshot {
    pub ledger_state: Arc<LedgerState<DefaultDB>>,
    pub highest_zswap_state_index: Option<u64>,
    pub block_height: u32,
    pub protocol_version: u32,
}

impl LedgerStateSnapshot {
    pub fn available_end_index(&self) -> Option<u64> {
        self.highest_zswap_state_index.or_else(|| {
            let first_free = self.ledger_state.zswap.first_free;
            if first_free > 0 {
                Some(first_free - 1)
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct MerkleUpdateNotReady {
    pub required_from_index: u64,
    pub required_to_index: u64,
    pub available_end_index: Option<u64>,
    pub snapshot_block_height: Option<u32>,
    pub protocol_version: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct MerkleUpdateBytes {
    pub start_index: u64,
    pub end_index: u64,
    pub update: Vec<u8>,
    pub protocol_version: u32,
    pub snapshot_block_height: u32,
    pub available_end_index: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerStateStoreError {
    #[error("ledger state object not available")]
    NotFound,
    #[error("ledger state not ready for requested merkle update")]
    NotReady(MerkleUpdateNotReady),
    #[error("failed to fetch ledger state from NATS: {0}")]
    Fetch(String),
    #[error("failed to deserialize ledger state: {0}")]
    Deserialize(String),
    #[error("failed to build merkle update: {0}")]
    InvalidUpdate(String),
}

struct CachedSnapshot {
    snapshot: LedgerStateSnapshot,
    fetched_at: Instant,
}

#[derive(Clone)]
pub struct LedgerStateStore {
    nats_client: NatsClient,
    cache: Arc<RwLock<Option<CachedSnapshot>>>,
    cache_ttl: Duration,
    object_store: Arc<RwLock<Option<ObjectStore>>>,
}

impl LedgerStateStore {
    pub fn new(nats_client: NatsClient) -> Self {
        Self {
            nats_client,
            cache: Arc::new(RwLock::new(None)),
            cache_ttl: DEFAULT_CACHE_TTL,
            object_store: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_cache_ttl(nats_client: NatsClient, cache_ttl: Duration) -> Self {
        Self {
            nats_client,
            cache: Arc::new(RwLock::new(None)),
            cache_ttl,
            object_store: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn get_snapshot(&self) -> Result<Option<LedgerStateSnapshot>, LedgerStateStoreError> {
        if let Some(snapshot) = self.get_cached_snapshot().await {
            return Ok(Some(snapshot));
        }
        self.fetch_snapshot().await
    }

    pub fn compute_merkle_update(
        snapshot: &LedgerStateSnapshot,
        from_index: u64,
        to_index: u64,
    ) -> Result<MerkleUpdateBytes, LedgerStateStoreError> {
        if from_index >= to_index {
            return Err(LedgerStateStoreError::InvalidUpdate(format!(
                "from_index must be less than to_index ({} >= {})",
                from_index, to_index
            )));
        }

        let required_end_index = to_index - 1;
        let available_end_index = snapshot.available_end_index();

        if available_end_index
            .map(|available| available < required_end_index)
            .unwrap_or(true)
        {
            return Err(LedgerStateStoreError::NotReady(MerkleUpdateNotReady {
                required_from_index: from_index,
                required_to_index: to_index,
                available_end_index,
                snapshot_block_height: Some(snapshot.block_height),
                protocol_version: Some(snapshot.protocol_version),
            }));
        }

        let update = MerkleTreeCollapsedUpdate::new(
            &snapshot.ledger_state.zswap.coin_coms,
            from_index,
            required_end_index,
        )
        .map_err(|e| LedgerStateStoreError::InvalidUpdate(e.to_string()))?;

        let mut bytes = Vec::new();
        tagged_serialize(&update, &mut bytes)
            .map_err(|e| LedgerStateStoreError::InvalidUpdate(e.to_string()))?;

        Ok(MerkleUpdateBytes {
            start_index: from_index,
            end_index: required_end_index,
            update: bytes,
            protocol_version: snapshot.protocol_version,
            snapshot_block_height: snapshot.block_height,
            available_end_index,
        })
    }

    async fn get_cached_snapshot(&self) -> Option<LedgerStateSnapshot> {
        let cache = self.cache.read().await;
        if let Some(cached) = cache.as_ref() {
            if cached.fetched_at.elapsed() < self.cache_ttl {
                return Some(cached.snapshot.clone());
            }
        }
        None
    }

    async fn fetch_snapshot(&self) -> Result<Option<LedgerStateSnapshot>, LedgerStateStoreError> {
        let store = self.get_object_store().await?;
        let mut object = match store.get(OBJECT_NAME).await {
            Ok(object) => object,
            Err(err) if err.kind() == GetErrorKind::NotFound => {
                debug!("ledger_state object not found in NATS");
                return Ok(None);
            }
            Err(err) => {
                return Err(LedgerStateStoreError::Fetch(err.to_string()));
            }
        };

        let highest_raw = object
            .read_u64_le()
            .await
            .map_err(|e| LedgerStateStoreError::Fetch(e.to_string()))?;
        let highest_zswap_state_index = if highest_raw == u64::MAX {
            None
        } else {
            Some(highest_raw)
        };

        let block_height = object
            .read_u32_le()
            .await
            .map_err(|e| LedgerStateStoreError::Fetch(e.to_string()))?;
        let protocol_version = object
            .read_u32_le()
            .await
            .map_err(|e| LedgerStateStoreError::Fetch(e.to_string()))?;

        let mut bytes = Vec::with_capacity(object.info.size.saturating_sub(16));
        object
            .read_to_end(&mut bytes)
            .await
            .map_err(|e| LedgerStateStoreError::Fetch(e.to_string()))?;

        let ledger_state = tagged_deserialize::<LedgerState<DefaultDB>>(&mut bytes.as_slice())
            .map_err(|e| LedgerStateStoreError::Deserialize(e.to_string()))?;

        let snapshot = LedgerStateSnapshot {
            ledger_state: Arc::new(ledger_state),
            highest_zswap_state_index,
            block_height,
            protocol_version,
        };

        {
            let mut cache = self.cache.write().await;
            *cache = Some(CachedSnapshot {
                snapshot: snapshot.clone(),
                fetched_at: Instant::now(),
            });
        }

        info!(
            block_height = snapshot.block_height,
            highest_zswap_state_index = snapshot.highest_zswap_state_index,
            "Loaded ledger state snapshot from NATS"
        );

        Ok(Some(snapshot))
    }

    async fn get_object_store(&self) -> Result<ObjectStore, LedgerStateStoreError> {
        {
            let guard = self.object_store.read().await;
            if let Some(store) = guard.as_ref() {
                return Ok(store.clone());
            }
        }

        let jetstream = jetstream::new(self.nats_client.clone());
        let store = jetstream
            .get_object_store(BUCKET_NAME)
            .await
            .map_err(|e| LedgerStateStoreError::Fetch(e.to_string()))?;

        {
            let mut guard = self.object_store.write().await;
            *guard = Some(store.clone());
        }

        Ok(store)
    }

    pub fn not_ready_from_snapshot(
        snapshot: Option<&LedgerStateSnapshot>,
        from_index: u64,
        to_index: u64,
    ) -> LedgerStateStoreError {
        LedgerStateStoreError::NotReady(MerkleUpdateNotReady {
            required_from_index: from_index,
            required_to_index: to_index,
            available_end_index: snapshot.and_then(|snap| snap.available_end_index()),
            snapshot_block_height: snapshot.map(|snap| snap.block_height),
            protocol_version: snapshot.map(|snap| snap.protocol_version),
        })
    }

    pub fn not_ready_from_error(
        from_index: u64,
        to_index: u64,
        error: &LedgerStateStoreError,
    ) -> Option<MerkleUpdateNotReady> {
        match error {
            LedgerStateStoreError::NotReady(info) => Some(info.clone()),
            LedgerStateStoreError::NotFound => Some(MerkleUpdateNotReady {
                required_from_index: from_index,
                required_to_index: to_index,
                available_end_index: None,
                snapshot_block_height: None,
                protocol_version: None,
            }),
            _ => None,
        }
    }
}
