//! Builder for opening an [`InoxSet`](crate::InoxSet) store.
//!
//! [`InoxSetBuilder`] provides a fluent API for configuring and opening an
//! inoxset store.  All settings have sensible defaults so a minimal open
//! requires only a storage path.
//!
//! # Example
//!
//! ```no_run
//! use inoxset::builder::InoxSetBuilder;
//!
//! let store = InoxSetBuilder::new()
//!     .path("/var/lib/myapp/inoxset")
//!     .open()
//!     .unwrap();
//! ```

use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, RwLock};

use crate::catalog;
use crate::error::InoxSetError;
use crate::mempart::MemPart;
use crate::metrics::{Metrics, NullMetrics};
use crate::part_store;
use crate::types::{Granularity, IndexFreshness, Rollup};

/// Default mempart flush threshold: 16 MiB.
const DEFAULT_FLUSH_THRESHOLD: u64 = 16 * 1024 * 1024;

/// Default LMDB map size: platform-aware (64 MiB macOS, 256 MiB Linux).
/// macOS APFS doesn't support sparse files, so the full map_size is
/// allocated on disk.
fn default_map_size() -> usize {
    if cfg!(target_os = "macos") {
        64 * 1024 * 1024
    } else {
        256 * 1024 * 1024
    }
}

/// Fluent builder for opening an [`InoxSet`](crate::InoxSet) store.
///
/// Call [`InoxSetBuilder::new`] (or [`InoxSet::builder`](crate::InoxSet::builder)),
/// chain configuration methods, then call [`open`](InoxSetBuilder::open) to
/// create the store.
///
/// # Defaults
///
/// | Setting | Default |
/// |---|---|
/// | `default_granularity` | [`Granularity::Day`] |
/// | `default_rollup` | [`Rollup::None`] |
/// | `metrics` | [`NullMetrics`] (no-op) |
/// | `mempart_flush_threshold` | 16 MiB |
/// | `max_events` | 0 (unlimited) |
/// | `read_only` | `false` |
/// | `map_size` | 64 MiB (macOS) / 256 MiB (Linux) |
/// | `clock` | `SystemTime` UTC seconds |
pub struct InoxSetBuilder {
    path: Option<PathBuf>,
    default_granularity: Granularity,
    default_rollup: Rollup,
    metrics: Arc<dyn Metrics>,
    flush_threshold: u64,
    max_events: usize,
    read_only: bool,
    clock: Option<Box<dyn Fn() -> u64 + Send + Sync>>,
    index_freshness: IndexFreshness,
    map_size: usize,
}

impl InoxSetBuilder {
    /// Creates a new builder with all-default settings.
    pub fn new() -> Self {
        Self {
            path: None,
            default_granularity: Granularity::Day,
            default_rollup: Rollup::None,
            metrics: Arc::new(NullMetrics),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            max_events: 0,
            read_only: false,
            clock: None,
            index_freshness: IndexFreshness::Disabled,
            map_size: default_map_size(),
        }
    }

    /// Sets the directory where the store files are kept.
    ///
    /// This directory (and any missing parents) will be created on
    /// [`open`](Self::open) if it does not already exist.
    pub fn path(mut self, p: impl Into<PathBuf>) -> Self {
        self.path = Some(p.into());
        self
    }

    /// Sets the default granularity for events that do not specify their own.
    pub fn default_granularity(mut self, g: Granularity) -> Self {
        self.default_granularity = g;
        self
    }

    /// Sets the default rollup strategy for events that do not specify their own.
    pub fn default_rollup(mut self, r: Rollup) -> Self {
        self.default_rollup = r;
        self
    }

    /// Plugs in a custom metrics back-end.
    ///
    /// Defaults to [`NullMetrics`] when not set.
    pub fn metrics(mut self, m: Arc<dyn Metrics>) -> Self {
        self.metrics = m;
        self
    }

    /// Sets the in-memory buffer size (in bytes) at which the mempart is
    /// automatically flushed to disk.
    ///
    /// Defaults to 16 MiB.
    pub fn mempart_flush_threshold(mut self, bytes: u64) -> Self {
        self.flush_threshold = bytes;
        self
    }

    /// Sets the maximum number of events allowed in the store.
    ///
    /// When set to a non-zero value, auto-registration will fail with
    /// [`InoxSetError::Configuration`] once the limit is reached.
    /// Defaults to 0 (unlimited).
    pub fn max_events(mut self, max: usize) -> Self {
        self.max_events = max;
        self
    }

    /// Opens the store in read-only mode when `ro` is `true`.
    ///
    /// Mutating operations will return [`InoxSetError::ReadOnly`] on a
    /// read-only store.
    pub fn read_only(mut self, ro: bool) -> Self {
        self.read_only = ro;
        self
    }

    /// Overrides the clock used to produce Unix timestamps.
    ///
    /// The default clock reads `SystemTime::now()`.  This method is useful in
    /// tests where deterministic timestamps are required.
    pub fn clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Some(Box::new(clock));
        self
    }

    /// Sets the maximum size of the LMDB memory-mapped region.
    ///
    /// This controls the upper bound on the catalog database size.
    /// Defaults to 256 MiB.
    pub fn map_size(mut self, bytes: usize) -> Self {
        self.map_size = bytes;
        self
    }

    /// Sets the freshness strategy for the inverted index.
    ///
    /// The inverted index enables sub-microsecond reverse membership lookups
    /// via [`InoxSet::find_memberships`]. Defaults to [`IndexFreshness::Disabled`]
    /// (zero RAM overhead, falls back to bitmap scanning).
    ///
    /// When enabled, the index is built during [`open`](Self::open) so that
    /// reverse lookups are correct immediately after a reopen. Open latency
    /// grows with store size (hundreds of milliseconds on large stores);
    /// use [`IndexFreshness::Disabled`] if open latency matters more than
    /// reverse-lookup speed.
    pub fn index_freshness(mut self, freshness: IndexFreshness) -> Self {
        self.index_freshness = freshness;
        self
    }

    /// Opens (or creates) the store, returning an [`InoxSet`](crate::InoxSet).
    ///
    /// # Steps performed
    ///
    /// 1. Validate that a path was provided.
    /// 2. Create the store directory (and `parts/` sub-directory) if needed.
    /// 3. Open the catalog database (`catalog.mdb`).
    /// 4. Initialise an empty in-memory write buffer ([`MemPart`]).
    /// 5. Unless `read_only`, scan for orphan part files left by a previous
    ///    crashed process and delete them.
    /// 6. Construct and return the [`InoxSet`](crate::InoxSet) handle.
    ///
    /// # Errors
    ///
    /// Returns an error when no path has been set, the directory cannot be
    /// created, the catalog fails to open, or orphan cleanup encounters an
    /// I/O error.
    pub fn open(self) -> crate::Result<crate::InoxSet> {
        // 1. Require path.
        let path = self
            .path
            .ok_or_else(|| InoxSetError::Configuration("path is required".to_string()))?;

        // 2. Create store directory and parts/ sub-directory.
        let parts_root = path.join("parts");
        std::fs::create_dir_all(&parts_root).map_err(|e| InoxSetError::BitmapIo {
            path: parts_root.clone(),
            source: e,
        })?;

        // 3. Open catalog (LMDB environment stored in a sub-directory).
        let catalog_path = path.join("catalog.mdb");
        let catalog = catalog::Catalog::open_with_map_size(&catalog_path, self.map_size)?;

        // 4. Create empty MemPart.
        let mempart = MemPart::new();

        // 5. Unless read-only, scan for orphan part files and delete them.
        if !self.read_only {
            let known_ids: std::collections::HashSet<u64> =
                catalog.all_part_ids()?.into_iter().collect();
            let orphans = part_store::scan_orphans(&parts_root, &known_ids)?;
            for orphan in orphans {
                log::warn!("deleting orphan part file: {}", orphan.display());
                part_store::delete_part(&orphan)?;
            }
        }

        // 6. Build InoxSet.
        let clock: Box<dyn Fn() -> u64 + Send + Sync> = self.clock.unwrap_or_else(|| {
            Box::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            })
        });

        // 7. Build read index from catalog.
        let ridx = crate::read_index::ReadIndex::build(&catalog)?;

        // 8. Build InvertedStore based on configured freshness strategy.
        let inverted = match self.index_freshness {
            IndexFreshness::Disabled => crate::InvertedStore::None,
            IndexFreshness::OnFlush | IndexFreshness::OnCompact => {
                let idx = crate::inverted::InvertedIndex::empty();
                crate::InvertedStore::Frozen(arc_swap::ArcSwap::from_pointee(idx))
            }
            IndexFreshness::Immediate => {
                // Silently degrading to a disabled index would make every
                // find_memberships call scan bitmaps while the caller
                // believes they opted into the freshest index — fail loud
                // until Immediate is implemented.
                return Err(InoxSetError::Configuration(
                    "IndexFreshness::Immediate is not implemented yet; \
                     use OnFlush, OnCompact, or Disabled"
                        .to_string(),
                ));
            }
        };

        let store = crate::InoxSet {
            path,
            parts_root,
            catalog,
            writer: RwLock::new(mempart),
            ridx: arc_swap::ArcSwap::from_pointee(ridx),
            default_granularity: self.default_granularity,
            default_rollup: self.default_rollup,
            metrics: self.metrics,
            flush_threshold: self.flush_threshold,
            max_events: self.max_events,
            read_only: self.read_only,
            closed: AtomicBool::new(false),
            clock,
            inverted,
            index_freshness: self.index_freshness,
        };

        // 9. Populate the inverted index from the catalog. A reopened store
        // must serve find_memberships immediately: leaving the index empty
        // until the first flush silently returns no results for entities
        // that are present and queryable through every other path.
        if matches!(store.inverted, crate::InvertedStore::Frozen(_)) {
            store.rebuild_inverted_index()?;
        }

        Ok(store)
    }
}

impl Default for InoxSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn builder_creates_store() {
        let dir = TempDir::new().unwrap();
        let _store = InoxSetBuilder::new()
            .path(dir.path().join("data"))
            .open()
            .unwrap();
        assert!(dir.path().join("data/catalog.mdb").exists());
    }

    #[test]
    fn builder_default_config() {
        let dir = TempDir::new().unwrap();
        let store = InoxSetBuilder::new()
            .path(dir.path().join("data"))
            .open()
            .unwrap();
        assert_eq!(store.flush_threshold, 16 * 1024 * 1024);
        assert!(!store.read_only);
        assert_eq!(store.default_granularity, crate::types::Granularity::Day);
        assert_eq!(store.default_rollup, crate::types::Rollup::None);
    }

    #[test]
    fn builder_requires_path() {
        let result = InoxSetBuilder::new().open();
        assert!(result.is_err());
    }

    #[test]
    fn builder_custom_clock() {
        let dir = TempDir::new().unwrap();
        let store = InoxSetBuilder::new()
            .path(dir.path().join("data"))
            .clock(|| 1_234_567_890)
            .open()
            .unwrap();
        assert_eq!(store.now_unix(), 1_234_567_890);
    }
}
