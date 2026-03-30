//! Metrics instrumentation for the sidecar service
//!
//! Provides Prometheus-compatible metrics for:
//! - DB query latency
//! - Feed assembly time
//! - SSE client count

use metrics::{counter, gauge, histogram};
use std::time::{Duration, Instant};

/// Metric names as constants for consistency
pub mod names {
    pub const DB_QUERY_DURATION: &str = "db_query_duration_seconds";
    pub const FEED_ASSEMBLY_DURATION: &str = "feed_assembly_duration_seconds";
    pub const SSE_CLIENTS_ACTIVE: &str = "sse_clients_active";
    pub const FEED_BLOCKS_RETURNED: &str = "feed_blocks_returned_total";
    pub const REORG_DETECTED: &str = "reorg_detected_total";
    pub const UPSTREAM_REQUESTS: &str = "upstream_requests_total";
    pub const EVENT_ORDERING_ANOMALIES: &str = "event_ordering_anomalies_total";
    pub const MERKLE_READY_HEIGHT: &str = "merkle_ready_height";
    pub const MERKLE_READINESS_LAG_BLOCKS: &str = "merkle_readiness_lag_blocks";
    pub const MERKLE_READINESS_UPDATE_DURATION_MS: &str = "merkle_readiness_update_duration_ms";
    pub const MERKLE_NOT_READY_TOTAL: &str = "merkle_not_ready_total";
    pub const MERKLE_READINESS_STALL_SECONDS: &str = "merkle_readiness_stall_seconds";
    pub const MERKLE_READINESS_STALL_TOTAL: &str = "merkle_readiness_stall_total";
    pub const LEDGER_STATE_FETCH_TIMEOUT_TOTAL: &str = "ledger_state_fetch_timeout_total";
    pub const LEDGER_STATE_FETCH_ERROR_TOTAL: &str = "ledger_state_fetch_error_total";
}

/// Record a DB query duration
pub fn record_db_query(query_name: &'static str, duration: std::time::Duration) {
    histogram!(names::DB_QUERY_DURATION, "query" => query_name).record(duration.as_secs_f64());
}

/// Record feed assembly duration
pub fn record_feed_assembly(duration: std::time::Duration) {
    histogram!(names::FEED_ASSEMBLY_DURATION).record(duration.as_secs_f64());
}

/// Increment active SSE client count
pub fn sse_client_connected() {
    gauge!(names::SSE_CLIENTS_ACTIVE).increment(1.0);
}

/// Decrement active SSE client count
pub fn sse_client_disconnected() {
    gauge!(names::SSE_CLIENTS_ACTIVE).decrement(1.0);
}

/// Record blocks returned in a feed response
pub fn record_blocks_returned(count: u64) {
    counter!(names::FEED_BLOCKS_RETURNED).increment(count);
}

/// Record a reorg detection
pub fn record_reorg_detected(reorg_type: &'static str) {
    counter!(names::REORG_DETECTED, "type" => reorg_type).increment(1);
}

/// Record an upstream request
pub fn record_upstream_request(endpoint: &'static str, success: bool) {
    counter!(
        names::UPSTREAM_REQUESTS,
        "endpoint" => endpoint,
        "success" => if success { "true" } else { "false" }
    )
    .increment(1);
}

/// Record an event ordering anomaly (gap in ledger_event_id sequence)
pub fn record_event_ordering_anomaly(anomaly_type: &'static str) {
    counter!(
        names::EVENT_ORDERING_ANOMALIES,
        "type" => anomaly_type
    )
    .increment(1);
}

/// Record current merkle readiness height.
pub fn record_merkle_ready_height(height: i64) {
    gauge!(names::MERKLE_READY_HEIGHT).set(height as f64);
}

/// Record chain head lag in blocks for merkle readiness.
pub fn record_merkle_readiness_lag_blocks(lag_blocks: i64) {
    gauge!(names::MERKLE_READINESS_LAG_BLOCKS).set(lag_blocks as f64);
}

/// Record merkle readiness batch duration in milliseconds.
pub fn record_merkle_readiness_duration_ms(duration: std::time::Duration) {
    histogram!(names::MERKLE_READINESS_UPDATE_DURATION_MS)
        .record(duration.as_millis() as f64);
}

/// Record a not-ready outcome for merkle readiness.
pub fn record_merkle_not_ready() {
    counter!(names::MERKLE_NOT_READY_TOTAL).increment(1);
}

/// Record a merkle readiness stall event.
pub fn record_merkle_readiness_stall() {
    counter!(names::MERKLE_READINESS_STALL_TOTAL).increment(1);
}

/// Record current merkle readiness stall duration in seconds.
pub fn set_merkle_readiness_stall_seconds(duration: Duration) {
    gauge!(names::MERKLE_READINESS_STALL_SECONDS).set(duration.as_secs_f64());
}

/// Record a ledger state snapshot fetch timeout.
pub fn record_ledger_state_fetch_timeout() {
    counter!(names::LEDGER_STATE_FETCH_TIMEOUT_TOTAL).increment(1);
}

/// Record a ledger state snapshot fetch error.
pub fn record_ledger_state_fetch_error() {
    counter!(names::LEDGER_STATE_FETCH_ERROR_TOTAL).increment(1);
}

/// Helper struct for timing operations
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Start a new timer
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed duration
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }
}

/// Initialize the Prometheus metrics exporter
/// Returns a handle to the metrics endpoint
pub fn init_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .install_recorder()
        .expect("Failed to install Prometheus recorder")
}
