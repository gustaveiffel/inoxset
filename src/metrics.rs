//! Observability abstractions for the inoxset storage engine.
//!
//! This module defines the [`Metrics`] trait that decouples the engine from any
//! specific metrics back-end (Prometheus, StatsD, OpenTelemetry, etc.).  A
//! [`NullMetrics`] no-op implementation is provided so that callers who do not
//! care about metrics can avoid boilerplate.
//!
//! # Integration
//!
//! Pass a `Box<dyn Metrics>` (or `Arc<dyn Metrics>`) into the engine builder.
//! All methods receive enough context to emit labelled counters, histograms, or
//! gauges in whatever back-end the application uses.

use crate::error::InoxSetError;
use crate::types::{Period, PeriodState};

/// Observer interface for engine-level events.
///
/// Implement this trait to plug any metrics back-end into inoxset.  All methods
/// are infallible and should never block; implementations are expected to hand
/// off work to background channels rather than performing I/O inline.
///
/// The trait is `Send + Sync` so that a single implementation can be shared
/// across threads via `Arc<dyn Metrics>`.
pub trait Metrics: Send + Sync {
    /// Called after a bitmap part has been durably written.
    ///
    /// `event` is the logical event name; `period` identifies the time bucket;
    /// `bytes` is the on-disk size of the written part; `cardinality` is the
    /// number of set bits (distinct user IDs) in the bitmap.
    fn bitmap_written(&self, event: &str, period: &Period, bytes: u64, cardinality: u64);

    /// Called after a delta (tombstone) part has been written.
    ///
    /// `bits_removed` is the number of bits that will be masked out during the
    /// next compaction pass.
    fn delta_written(&self, event: &str, period: &Period, bits_removed: u64);

    /// Called when a write targets a period that has already closed (backfill).
    ///
    /// `lag_seconds` is the difference between the current wall-clock time and
    /// the nominal end of `period`, providing a measure of how stale the
    /// incoming data is.
    fn backfill_write(&self, event: &str, period: &Period, lag_seconds: u64);

    /// Called after a bitmap read completes.
    ///
    /// `data_parts` and `delta_parts` reflect how many part files were merged
    /// to produce the result; `duration_us` is the total read latency in
    /// microseconds.
    fn bitmap_read(
        &self,
        event: &str,
        period: &Period,
        data_parts: u32,
        delta_parts: u32,
        duration_us: u64,
    );

    /// Called after an automatic rollup step propagates a bitmap from `from` to
    /// `to`.
    fn rollup_performed(&self, event: &str, from: &Period, to: &Period);

    /// Called after the in-memory buffer (mempart) has been flushed to disk.
    ///
    /// `data_parts` and `delta_parts` are the totals flushed in this batch;
    /// `bytes` is the total bytes written.
    fn mempart_flushed(&self, data_parts: u32, delta_parts: u32, bytes: u64);

    /// Called when a compaction run completes successfully.
    ///
    /// `periods` is the number of time periods that were compacted;
    /// `parts_merged` is the total number of data parts merged together;
    /// `deltas_applied` is the number of delta parts whose bits were applied
    /// and then discarded; `bytes_reclaimed` is the total disk space freed.
    fn compaction_completed(
        &self,
        periods: u32,
        parts_merged: u32,
        deltas_applied: u32,
        bytes_reclaimed: u64,
    );

    /// Called whenever a period transitions between lifecycle states.
    ///
    /// `from` and `to` name the previous and next [`PeriodState`] respectively.
    fn period_state_changed(
        &self,
        event: &str,
        period: &Period,
        from: &PeriodState,
        to: &PeriodState,
    );

    /// Called when any engine-level error is produced, regardless of whether
    /// it is ultimately returned to the caller.
    fn error_occurred(&self, error: &InoxSetError);
}

/// A no-op implementation of [`Metrics`] that discards every observation.
///
/// Use `NullMetrics` when observability is not needed, or as a stand-in during
/// testing and development.
pub struct NullMetrics;

impl Metrics for NullMetrics {
    fn bitmap_written(&self, _: &str, _: &Period, _: u64, _: u64) {}
    fn delta_written(&self, _: &str, _: &Period, _: u64) {}
    fn backfill_write(&self, _: &str, _: &Period, _: u64) {}
    fn bitmap_read(&self, _: &str, _: &Period, _: u32, _: u32, _: u64) {}
    fn rollup_performed(&self, _: &str, _: &Period, _: &Period) {}
    fn mempart_flushed(&self, _: u32, _: u32, _: u64) {}
    fn compaction_completed(&self, _: u32, _: u32, _: u32, _: u64) {}
    fn period_state_changed(&self, _: &str, _: &Period, _: &PeriodState, _: &PeriodState) {}
    fn error_occurred(&self, _: &InoxSetError) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_metrics_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NullMetrics>();
    }

    #[test]
    fn null_metrics_callable() {
        let m = NullMetrics;
        m.bitmap_written("test", &Period::Static, 100, 50);
        m.mempart_flushed(3, 1, 1024);
    }
}
