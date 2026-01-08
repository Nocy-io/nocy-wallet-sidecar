use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize, Serializer};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::instrument;

use crate::db::{
    begin_readonly_tx, compute_wallet_ready_height_tx, get_chain_head_tx,
    get_merkle_ready_height_tx, get_upstream_session_id_tx,
};
use crate::error::AppError;
use crate::feed::{Watermarks, API_VERSION};
use crate::routes::session::SessionState;

/// Serialize i64 as a decimal string for JavaScript compatibility
fn serialize_i64_as_string<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

/// Serialize Option<i64> as a decimal string for JavaScript compatibility
fn serialize_option_i64_as_string<S>(value: &Option<i64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(v) => serializer.serialize_some(&v.to_string()),
        None => serializer.serialize_none(),
    }
}

/// Query parameters for GET /v1/status
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusQuery {
    /// Session ID (hex-encoded 32 bytes)
    pub session_id: String,
}

/// Response for GET /v1/status
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    /// API version
    pub api_version: &'static str,
    /// Current watermarks
    pub watermarks: Watermarks,
    /// Ledger state snapshot status (used for collapsed merkle updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_snapshot: Option<LedgerSnapshotStatus>,
    /// Server timestamp (unix ms)
    pub server_time_ms: u64,
}

/// Ledger snapshot metadata for merkle update availability
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerSnapshotStatus {
    /// Block height of the snapshot
    #[serde(serialize_with = "serialize_i64_as_string")]
    pub block_height: i64,
    /// Highest available end index (inclusive), if known
    #[serde(serialize_with = "serialize_option_i64_as_string")]
    pub available_end_index: Option<i64>,
    /// Protocol version at snapshot height
    pub protocol_version: u32,
}

/// GET /v1/status - Fetch current chain + wallet watermarks
#[instrument(skip(state), fields(session_id = %query.session_id))]
pub async fn get_status(
    State(state): State<SessionState>,
    Query(query): Query<StatusQuery>,
) -> Result<Json<StatusResponse>, AppError> {
    let local_session_id_bytes = hex::decode(&query.session_id)
        .map_err(|_| AppError::BadRequest("session_id must be valid hex".into()))?;

    if local_session_id_bytes.len() != 32 {
        return Err(AppError::BadRequest(
            "session_id must be 32 bytes (64 hex chars)".into(),
        ));
    }

    let mut snapshot = begin_readonly_tx(&state.db_pool).await?;
    let upstream_session_id_bytes =
        get_upstream_session_id_tx(&mut snapshot, &local_session_id_bytes).await?;

    let chain_head = get_chain_head_tx(&mut snapshot).await?;
    let wallet_ready_height =
        compute_wallet_ready_height_tx(&mut snapshot, &upstream_session_id_bytes, chain_head)
            .await?;
    let merkle_ready_height = get_merkle_ready_height_tx(&mut snapshot).await?;
    let combined_ready_height = wallet_ready_height
        .map(|wallet_height| wallet_height.min(merkle_ready_height).min(chain_head));

    let watermarks = Watermarks {
        chain_head,
        wallet_ready_height,
        merkle_ready_height,
        combined_ready_height,
        finalized_height: Some(chain_head),
        allow_sparse_blocks: state.allow_sparse_blocks,
    };

    let ledger_snapshot = match state.ledger_state_store.get_snapshot().await {
        Ok(Some(snapshot)) => Some(LedgerSnapshotStatus {
            block_height: snapshot.block_height as i64,
            available_end_index: snapshot.available_end_index().map(|value| value as i64),
            protocol_version: snapshot.protocol_version,
        }),
        _ => None,
    };

    let server_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);

    Ok(Json(StatusResponse {
        api_version: API_VERSION,
        watermarks,
        ledger_snapshot,
        server_time_ms,
    }))
}
