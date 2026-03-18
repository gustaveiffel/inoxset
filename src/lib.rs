// inoxset — Roaring Bitmap storage engine with time-aware set algebra
//
// Library-first sync API. No async, no server.
// Embed via spawn_blocking in async runtimes.

pub mod builder;
pub mod catalog;
pub mod error;
pub mod mempart;
pub mod merge;
pub mod metrics;
pub mod part_store;
pub mod period;
pub mod rollup;
pub mod types;

use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, RwLock};

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, error::InoxSetError>;

/// The main storage engine handle.
///
/// Created via [`InoxSetBuilder`](builder::InoxSetBuilder). Provides methods
/// for writing, reading, and managing Roaring Bitmap data across time periods.
// Fields are populated now and will be consumed in future milestone tasks.
#[allow(dead_code)]
pub struct InoxSet {
    pub(crate) path: PathBuf,
    pub(crate) parts_root: PathBuf,
    pub(crate) catalog: catalog::Catalog,
    // Future: consider arc_swap::ArcSwap for lock-free reads if benchmarks show contention
    pub(crate) writer: RwLock<mempart::MemPart>,
    pub(crate) default_granularity: types::Granularity,
    pub(crate) default_rollup: types::Rollup,
    pub(crate) metrics: Arc<dyn metrics::Metrics>,
    pub(crate) flush_threshold: u64,
    pub(crate) read_only: bool,
    pub(crate) closed: AtomicBool,
    pub(crate) clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl InoxSet {
    /// Create a new builder for configuring and opening an InoxSet store.
    pub fn builder() -> builder::InoxSetBuilder {
        builder::InoxSetBuilder::new()
    }

    /// Returns the current Unix timestamp according to the configured clock.
    // Used by future milestone tasks; suppressed until then.
    #[allow(dead_code)]
    pub(crate) fn now_unix(&self) -> u64 {
        (self.clock)()
    }
}
