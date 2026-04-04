// inoxset — Roaring Bitmap storage engine with time-aware set algebra
//
// Library-first sync API. No async, no server.
// Embed via spawn_blocking in async runtimes.

pub mod builder;
pub mod catalog;
pub mod dict;
pub mod error;
pub mod mempart;
pub mod merge;
pub mod metrics;
pub mod part_store;
pub mod period;
pub(crate) mod read_index;
pub mod rollup;
pub mod types;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;

use roaring::RoaringBitmap;

use crate::catalog::Catalog;
use crate::error::{validate_event_name, InoxSetError};
use crate::mempart::MemPartSnapshot;
use crate::period::catalog_key;
use crate::types::{
    CompactStats, EventConfig, Granularity, Health, Part, PartKind, Period, PeriodState, Rollup,
};

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, error::InoxSetError>;

/// The main storage engine handle.
///
/// Created via [`InoxSetBuilder`](builder::InoxSetBuilder). Provides methods
/// for writing, reading, and managing Roaring Bitmap data across time periods.
pub struct InoxSet {
    #[allow(dead_code)]
    pub(crate) path: PathBuf,
    pub(crate) parts_root: PathBuf,
    pub(crate) catalog: catalog::Catalog,
    pub(crate) writer: RwLock<mempart::MemPart>,
    /// Lock-free read index — caches catalog metadata to skip redb lookups.
    pub(crate) ridx: ArcSwap<read_index::ReadIndex>,
    pub(crate) default_granularity: types::Granularity,
    pub(crate) default_rollup: types::Rollup,
    pub(crate) metrics: Arc<dyn metrics::Metrics>,
    pub(crate) flush_threshold: u64,
    pub(crate) max_events: usize,
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
    pub(crate) fn now_unix(&self) -> u64 {
        (self.clock)()
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    /// Returns an error if the store has been closed.
    fn check_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(InoxSetError::Closed);
        }
        Ok(())
    }

    /// Rebuilds the in-memory read index from the catalog.
    ///
    /// Called after flush, compact, delete_period, drop_event — any operation
    /// that mutates the catalog's part/period metadata.
    fn rebuild_read_index(&self) -> Result<()> {
        let idx = read_index::ReadIndex::build(&self.catalog)?;
        self.ridx.store(Arc::new(idx));
        Ok(())
    }

    /// Returns an error if the store is read-only or closed.
    fn check_writable(&self) -> Result<()> {
        self.check_closed()?;
        if self.read_only {
            return Err(InoxSetError::ReadOnly);
        }
        Ok(())
    }

    /// Validates that `period`'s granularity matches the event's finest
    /// granularity.
    fn validate_granularity(event: &str, config: &EventConfig, period: &Period) -> Result<()> {
        if period.granularity() != config.finest_granularity {
            return Err(InoxSetError::GranularityMismatch {
                event: event.to_string(),
                period: *period,
                expected: config.finest_granularity,
            });
        }
        Ok(())
    }

    // ─── Task 12: Event Management ───────────────────────────────────────────

    /// Registers a new event with the given granularity and rollup strategy.
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::InvalidEventName`] if the name is invalid.
    /// Returns [`InoxSetError::EventAlreadyRegistered`] if the event already
    /// exists.
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    pub fn register_event(
        &self,
        name: &str,
        granularity: Granularity,
        rollup: Rollup,
    ) -> Result<()> {
        self.check_writable()?;
        validate_event_name(name)?;

        // Check for duplicate.
        if self.catalog.get_event(name)?.is_some() {
            return Err(InoxSetError::EventAlreadyRegistered(name.to_string()));
        }

        let config = EventConfig::new(name.to_string(), granularity, rollup);
        self.catalog.register_event(&config)?;
        Ok(())
    }

    /// Returns a list of all registered event configurations.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog I/O failure.
    pub fn list_events(&self) -> Result<Vec<EventConfig>> {
        self.check_closed()?;
        self.catalog.list_events()
    }

    /// Returns the list of periods that contain data for the given event.
    ///
    /// This includes periods with data parts, delta parts, or both in the
    /// catalog. In-memory (unflushed) periods are not included.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog I/O failure.
    pub fn list_periods(&self, event: &str) -> Result<Vec<Period>> {
        self.check_closed()?;
        validate_event_name(event)?;
        let keys = self.catalog.period_keys_for_event(event)?;
        // Catalog keys have the form "event/gran/period_key".
        // Extract the period_key (3rd segment) and parse it.
        let mut periods: Vec<Period> = keys
            .iter()
            .filter_map(|k| {
                let period_key = k.splitn(3, '/').nth(2)?;
                period::parse_period_key(period_key)
            })
            .collect();
        periods.sort();
        periods.dedup();
        Ok(periods)
    }

    /// Drops an event and all its associated data (periods, parts, deltas).
    ///
    /// On-disk part files referenced by the event are deleted after the catalog
    /// transaction commits.
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    /// Returns an error on catalog or file I/O failure.
    pub fn drop_event(&self, name: &str) -> Result<()> {
        self.check_writable()?;
        validate_event_name(name)?;

        // Remove from mempart.
        {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            mp.bitmaps.retain(|(ev, _), _| ev != name);
            mp.deltas.retain(|(ev, _), _| ev != name);
        }

        // Remove from catalog, getting parts back so we can delete files.
        let parts = self.catalog.delete_event_returning_parts(name)?;
        self.rebuild_read_index()?;
        for part in &parts {
            if part.file_path.exists() {
                let _ = part_store::delete_part(&part.file_path);
            }
        }
        Ok(())
    }

    /// Deletes a single period's data for the given event.
    ///
    /// Removes all data parts, delta parts, and period state from the catalog
    /// and deletes the corresponding files from disk. The event registration
    /// itself is preserved.
    ///
    /// Also clears any unflushed in-memory data for the period.
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    /// Returns an error on catalog or file I/O failure.
    pub fn delete_period(&self, event: &str, period: Period) -> Result<()> {
        self.check_writable()?;
        validate_event_name(event)?;

        // Clear from mempart.
        {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            mp.bitmaps.remove(&(event.to_string(), period));
            mp.deltas.remove(&(event.to_string(), period));
        }

        // Look up event config to build the catalog key.
        let config = match self.catalog.get_event(event)? {
            Some(c) => c,
            None => return Ok(()), // Event not registered → nothing to delete.
        };

        let cat_key = period::catalog_key(event, config.finest_granularity, &period);
        let parts = self.catalog.delete_period_returning_parts(&cat_key)?;
        self.rebuild_read_index()?;
        for part in &parts {
            if part.file_path.exists() {
                let _ = part_store::delete_part(&part.file_path);
            }
        }
        Ok(())
    }

    /// Retains only the periods matching the predicate, deleting the rest.
    ///
    /// Calls [`delete_period`](Self::delete_period) for each period where
    /// `keep` returns `false`. Returns the number of periods deleted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inoxset::InoxSet;
    /// # use inoxset::types::Period;
    /// # let store = InoxSet::builder().path("/tmp/ex").open().unwrap();
    /// // Keep only the last 90 days
    /// let cutoff = Period::Day(2026, 1, 1);
    /// let deleted = store.retain_periods("logins", |p| *p >= cutoff).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn retain_periods(&self, event: &str, keep: impl Fn(&Period) -> bool) -> Result<u32> {
        self.check_writable()?;
        let periods = self.list_periods(event)?;
        let mut deleted = 0u32;
        for period in periods {
            if !keep(&period) {
                self.delete_period(event, period)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Finds all `(event, period)` pairs where the given external ID is present.
    ///
    /// **Note:** This API is unstable and will be significantly optimized in a
    /// future release (arena storage, inverted index). The signature will remain
    /// the same but performance characteristics will change.
    ///
    /// Resolves the external ID through the dictionary, then checks membership
    /// across all events and periods. Returns an empty vec if the ID has never
    /// been assigned.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn find_memberships(&self, external_id: &str) -> Result<Vec<(String, Period)>> {
        self.check_closed()?;

        // Load the read index (lock-free via ArcSwap).
        let idx = self.ridx.load();

        // Phase 1: resolve dict IDs per event.
        let mut event_ids: Vec<(String, u32)> = Vec::new();
        for ev in &idx.event_names {
            if let Some(id) = dict::lookup(self.catalog.db(), ev, external_id)? {
                event_ids.push((ev.clone(), id));
            }
        }
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: scan the in-memory index — zero redb lookups.
        let mut memberships = Vec::new();
        let mut file_cache: std::collections::HashMap<PathBuf, Vec<u8>> =
            std::collections::HashMap::new();

        for (event_name, internal_id) in &event_ids {
            for ((ev, period), entry) in &idx.periods {
                if ev != event_name {
                    continue;
                }

                // Check data parts via serialized_contains.
                let mut found = false;
                for loc in &entry.data_parts {
                    let bytes = file_cache
                        .entry(loc.file_path.clone())
                        .or_insert_with(|| std::fs::read(&loc.file_path).unwrap_or_default());
                    if part_store::serialized_contains(bytes, *internal_id) {
                        found = true;
                        break;
                    }
                }
                if !found {
                    continue;
                }

                // Check tombstones.
                let mut tombstoned = false;
                for loc in &entry.delta_parts {
                    let bytes = file_cache
                        .entry(loc.file_path.clone())
                        .or_insert_with(|| std::fs::read(&loc.file_path).unwrap_or_default());
                    if part_store::serialized_contains(bytes, *internal_id) {
                        tombstoned = true;
                        break;
                    }
                }
                if tombstoned {
                    continue;
                }

                memberships.push((event_name.clone(), *period));
            }
        }

        // Phase 3: check mempart for unflushed data.
        {
            let mp = self.writer.read().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            for (event_name, internal_id) in &event_ids {
                for ((ev, period), bm) in &mp.bitmaps {
                    if ev == event_name && bm.contains(*internal_id) {
                        let deltad = mp
                            .get_delta(event_name, period)
                            .is_some_and(|d| d.contains(*internal_id));
                        if !deltad
                            && !memberships
                                .iter()
                                .any(|(e, p)| e == event_name && p == period)
                        {
                            memberships.push((event_name.clone(), *period));
                        }
                    }
                }
            }
        }

        Ok(memberships)
    }

    /// Checks whether an external ID is present in a specific event and period.
    ///
    /// **Note:** This API is unstable and will be significantly optimized in a
    /// future release (arena storage). The signature will remain the same.
    ///
    /// Uses zero-copy membership checks on serialized roaring bytes via
    /// [`part_store::mmap_contains`], avoiding full bitmap deserialization.
    /// Falls back to in-memory check for unflushed data.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn contains_id(&self, event: &str, period: Period, external_id: &str) -> Result<bool> {
        self.check_closed()?;
        let internal_id = match dict::lookup(self.catalog.db(), event, external_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        self.contains_bit(event, period, internal_id)
    }

    /// Checks whether a raw u32 value is present in the bitmap for an event
    /// and period, using zero-copy serialized checks on disk parts.
    fn contains_bit(&self, event: &str, period: Period, value: u32) -> Result<bool> {
        // Check mempart first (cheap, in-memory).
        {
            let mp = self.writer.read().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            if let Some(bm) = mp.get_bitmap(event, &period) {
                if bm.contains(value) {
                    // Check mempart delta — might have been removed.
                    if let Some(delta) = mp.get_delta(event, &period) {
                        if delta.contains(value) {
                            // Removed in mempart — still need to check disk.
                        } else {
                            return Ok(true);
                        }
                    } else {
                        return Ok(true);
                    }
                }
            }
            // Check if mempart delta removes it (even if not in mempart bitmap).
            if let Some(delta) = mp.get_delta(event, &period) {
                if delta.contains(value) {
                    // Value is tombstoned — even if on disk, it's deleted.
                    // But we need to know if it was on disk first to give
                    // correct result. A delta only matters if the value exists
                    // on disk. Check disk, then apply delta.
                }
            }
        }

        // Check disk parts via read index (zero redb lookups).
        let idx = self.ridx.load();
        let key = (event.to_string(), period);

        let entry = match idx.periods.get(&key) {
            Some(e) => e,
            None => return Ok(false),
        };

        let mut found_on_disk = false;
        for loc in &entry.data_parts {
            if part_store::mmap_contains(&loc.file_path, value)? {
                found_on_disk = true;
                break;
            }
        }

        if !found_on_disk {
            return Ok(false);
        }

        // Check disk deltas.
        for loc in &entry.delta_parts {
            if part_store::mmap_contains(&loc.file_path, value)? {
                return Ok(false); // tombstoned
            }
        }

        // Check mempart delta.
        {
            let mp = self.writer.read().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            if let Some(delta) = mp.get_delta(event, &period) {
                if delta.contains(value) {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Writes a set of external IDs for the given event and period.
    ///
    /// External IDs (strings) are transparently mapped to u32 values via
    /// the dictionary encoding layer. New IDs are auto-assigned a monotonic
    /// u32 on first encounter. The resulting bitmap is OR-accumulated with
    /// any existing data, just like [`put_bitmap`](Self::put_bitmap).
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    /// Returns an error on catalog or file I/O failure.
    pub fn put_ids(&self, event: &str, period: Period, external_ids: &[&str]) -> Result<()> {
        self.check_writable()?;
        if external_ids.is_empty() {
            return Ok(());
        }
        let internal_ids = dict::batch_assign_or_get(self.catalog.db(), event, external_ids)?;
        let mut bitmap = RoaringBitmap::new();
        for id in internal_ids {
            bitmap.insert(id);
        }
        self.put_bitmap(event, period, bitmap)
    }

    /// Reads the external IDs stored for the given event and period.
    ///
    /// Retrieves the merged bitmap via [`get`](Self::get), then resolves
    /// each u32 back to its external string ID through the dictionary.
    /// IDs that cannot be resolved are silently omitted.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn get_ids(&self, event: &str, period: Period) -> Result<Vec<String>> {
        let bitmap = self.get(event, period)?;
        let internal_ids: Vec<u32> = bitmap.iter().collect();
        let resolved = dict::batch_reverse_lookup(self.catalog.db(), event, &internal_ids)?;
        Ok(resolved.into_iter().flatten().collect())
    }

    /// Removes external IDs from the given event and period via delta tombstones.
    ///
    /// Looks up each external ID in the dictionary. IDs that have never been
    /// assigned are silently ignored. The resolved u32 values are passed to
    /// [`remove_bits`](Self::remove_bits).
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    /// Returns an error on catalog or file I/O failure.
    pub fn remove_ids(&self, event: &str, period: Period, external_ids: &[&str]) -> Result<()> {
        self.check_writable()?;
        if external_ids.is_empty() {
            return Ok(());
        }
        let mut bits: Vec<u32> = Vec::new();
        for ext_id in external_ids {
            if let Some(internal) = dict::lookup(self.catalog.db(), event, ext_id)? {
                bits.push(internal);
            }
        }
        if bits.is_empty() {
            return Ok(());
        }
        self.remove_bits(event, period, &bits)
    }

    /// Looks up an event by name, auto-registering it with defaults if it
    /// doesn't exist.
    ///
    /// This is used by the write path so that callers don't need to
    /// pre-register every event.
    pub(crate) fn ensure_event(&self, name: &str) -> Result<EventConfig> {
        validate_event_name(name)?;

        if let Some(config) = self.catalog.get_event(name)? {
            return Ok(config);
        }

        // Enforce max event limit before auto-registering.
        if self.max_events > 0 {
            let count = self.catalog.list_events()?.len();
            if count >= self.max_events {
                return Err(InoxSetError::Configuration(format!(
                    "max event limit reached ({}/{})",
                    count, self.max_events,
                )));
            }
        }

        // Auto-register with defaults.
        let config = EventConfig::new(
            name.to_string(),
            self.default_granularity,
            self.default_rollup,
        );
        self.catalog.register_event(&config)?;
        Ok(config)
    }

    // ─── Task 13: put_bitmap ─────────────────────────────────────────────────

    /// Writes a bitmap for the given event and period, OR-accumulating with
    /// any existing data.
    ///
    /// If the event is not registered, it is auto-registered with the store's
    /// default granularity and rollup. If rollup is [`Rollup::Auto`], the
    /// bitmap is propagated to coarser ancestor periods.
    ///
    /// When the in-memory buffer exceeds the configured flush threshold, an
    /// automatic flush is triggered.
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    /// Returns [`InoxSetError::GranularityMismatch`] if `period` does not
    /// match the event's configured granularity.
    pub fn put_bitmap(&self, event: &str, period: Period, bitmap: RoaringBitmap) -> Result<()> {
        self.check_writable()?;
        let config = self.ensure_event(event)?;

        // Static periods always pass; for time-based, validate granularity.
        if period != Period::Static {
            Self::validate_granularity(event, &config, &period)?;
        }

        // Backfill detection: if the period is closed, revert Compacted→Closed
        // so it can receive writes again.
        let now = self.now_unix();
        if period.is_closed(now) {
            let cat_key = catalog_key(event, config.finest_granularity, &period);
            if let Some(PeriodState::Compacted) = self.catalog.get_period_state(&cat_key)? {
                self.catalog
                    .set_period_state(&cat_key, PeriodState::Closed)?;
            }
        }

        // Write-lock mempart and OR the bitmap.
        let should_flush;
        {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            mp.or_bitmap(event, period, &bitmap);
            rollup::apply_rollup(&mut mp, &config, &period, &bitmap);
            should_flush = mp.size_bytes() >= self.flush_threshold;
        }

        // Auto-flush if over threshold.
        if should_flush {
            self.flush_internal()?;
        }

        Ok(())
    }

    // ─── Task 14: flush ──────────────────────────────────────────────────────

    /// Flushes the in-memory write buffer to durable storage.
    ///
    /// Takes a snapshot of the current mempart, writes each entry as an
    /// immutable part file, and commits the catalog metadata in a single
    /// atomic transaction.
    ///
    /// Returns immediately (no-op) if the buffer is empty.
    ///
    /// # Errors
    ///
    /// Returns an error on file or catalog I/O failure.
    pub fn flush(&self) -> Result<()> {
        self.check_writable()?;
        self.flush_internal()
    }

    /// Internal flush implementation, called from both `flush()` and
    /// auto-flush in `put_bitmap`.
    fn flush_internal(&self) -> Result<()> {
        let snapshot = {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            if mp.is_empty() {
                return Ok(());
            }
            mp.take_snapshot()
        };
        self.flush_snapshot(snapshot)
    }

    /// Persists a [`MemPartSnapshot`] to disk and updates the catalog.
    fn flush_snapshot(&self, snapshot: MemPartSnapshot) -> Result<()> {
        if snapshot.bitmaps.is_empty() && snapshot.deltas.is_empty() {
            return Ok(());
        }

        // Pre-resolve all event configs *before* opening the write transaction
        // so that auto-registration (which opens its own write txn) does not
        // deadlock against the flush transaction.
        let mut event_configs: std::collections::HashMap<String, EventConfig> =
            std::collections::HashMap::new();
        for (event, _period) in snapshot.bitmaps.keys() {
            if !event_configs.contains_key(event.as_str()) {
                event_configs.insert(event.clone(), self.ensure_event(event)?);
            }
        }
        for (event, _period) in snapshot.deltas.keys() {
            if !event_configs.contains_key(event.as_str()) {
                event_configs.insert(event.clone(), self.ensure_event(event)?);
            }
        }

        // Count total parts needed.
        let total_parts = snapshot.bitmaps.len() + snapshot.deltas.len();

        // Single atomic transaction for all catalog updates.
        let txn = self.catalog.db().begin_write()?;
        {
            let mut next_id_table = txn.open_table(catalog::NEXT_PART_ID)?;
            let mut parts_table = txn.open_table(catalog::PARTS)?;
            let mut pp_table = txn.open_table(catalog::PERIOD_PARTS)?;
            let mut pd_table = txn.open_table(catalog::PERIOD_DELTAS)?;
            let mut ps_table = txn.open_table(catalog::PERIOD_STATE)?;

            let ids = Catalog::txn_alloc_part_ids(&mut next_id_table, total_parts as u64)?;
            let mut id_iter = ids.into_iter();

            let now = self.now_unix();

            // Flush data bitmaps.
            let mut data_parts_written = 0u32;
            let mut total_bytes = 0u64;
            for ((event, period), bm_arc) in &snapshot.bitmaps {
                let part_id = id_iter
                    .next()
                    .ok_or_else(|| InoxSetError::CatalogCorrupted {
                        context: "ran out of allocated part IDs during flush".to_string(),
                    })?;

                let config = &event_configs[event.as_str()];
                let file_path = part_store::part_file_path(
                    &self.parts_root,
                    event,
                    config.finest_granularity,
                    period,
                    part_id,
                    PartKind::Data,
                );

                part_store::write_part(&file_path, bm_arc)?;

                let size_bytes = file_path
                    .metadata()
                    .map(|m| m.len())
                    .unwrap_or(bm_arc.serialized_size() as u64);

                let part = Part {
                    part_id,
                    kind: PartKind::Data,
                    event: event.clone(),
                    period: *period,
                    file_path,
                    size_bytes,
                    cardinality: bm_arc.len(),
                    created_at: now,
                    level: 0,
                };

                Catalog::txn_register_part(&mut parts_table, &part)?;

                let cat_key = catalog_key(event, period.granularity(), period);
                Catalog::txn_append_period_parts(&mut pp_table, &cat_key, &[part_id])?;

                // Ensure period state is at least Open.
                if Catalog::txn_get_period_state(&ps_table, &cat_key)?.is_none() {
                    Catalog::txn_set_period_state(&mut ps_table, &cat_key, PeriodState::Open)?;
                }

                data_parts_written += 1;
                total_bytes += size_bytes;
            }

            // Flush delta bitmaps.
            let mut delta_parts_written = 0u32;
            for ((event, period), delta_arc) in &snapshot.deltas {
                let part_id = id_iter
                    .next()
                    .ok_or_else(|| InoxSetError::CatalogCorrupted {
                        context: "ran out of allocated part IDs during flush".to_string(),
                    })?;

                let config = &event_configs[event.as_str()];
                let file_path = part_store::part_file_path(
                    &self.parts_root,
                    event,
                    config.finest_granularity,
                    period,
                    part_id,
                    PartKind::Delta,
                );

                part_store::write_part(&file_path, delta_arc)?;

                let size_bytes = file_path
                    .metadata()
                    .map(|m| m.len())
                    .unwrap_or(delta_arc.serialized_size() as u64);

                let part = Part {
                    part_id,
                    kind: PartKind::Delta,
                    event: event.clone(),
                    period: *period,
                    file_path,
                    size_bytes,
                    cardinality: delta_arc.len(),
                    created_at: now,
                    level: 0,
                };

                Catalog::txn_register_part(&mut parts_table, &part)?;

                let cat_key = catalog_key(event, period.granularity(), period);
                Catalog::txn_append_period_deltas(&mut pd_table, &cat_key, &[part_id])?;

                delta_parts_written += 1;
                total_bytes += size_bytes;
            }

            self.metrics
                .mempart_flushed(data_parts_written, delta_parts_written, total_bytes);
        }
        txn.commit()?;
        self.rebuild_read_index()?;
        Ok(())
    }

    // ─── Task 15: Read Path ──────────────────────────────────────────────────

    /// Retrieves the merged bitmap for an event and period.
    ///
    /// The result is the OR of all flushed data parts and the in-memory
    /// buffer, with all delta (tombstone) parts subtracted.
    ///
    /// Returns an empty bitmap if no data exists for the given event/period.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn get(&self, event: &str, period: Period) -> Result<RoaringBitmap> {
        self.check_closed()?;

        // Read from mempart (read-lock, then drop).
        let (mp_bitmap, mp_delta) = {
            let mp = self.writer.read().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            (mp.get_bitmap(event, &period), mp.get_delta(event, &period))
        };

        // Batched read transaction (Eng Decision 3).
        let cat_key = catalog_key(event, period.granularity(), &period);
        let txn = self.catalog.db().begin_read()?;
        let pp_table = txn.open_table(catalog::PERIOD_PARTS)?;
        let pd_table = txn.open_table(catalog::PERIOD_DELTAS)?;
        let parts_table = txn.open_table(catalog::PARTS)?;

        let data_part_ids = Catalog::txn_get_period_parts(&pp_table, &cat_key)?;
        let delta_part_ids = Catalog::txn_get_period_deltas(&pd_table, &cat_key)?;

        // OR all data parts from disk.
        let mut result = RoaringBitmap::new();
        for pid in &data_part_ids {
            if let Some(part) = Catalog::txn_get_part(&parts_table, *pid)? {
                let bm = part_store::mmap_read_part(&part.file_path).map_err(|e| match e {
                    InoxSetError::BitmapCorrupted { .. } => InoxSetError::BitmapCorrupted {
                        event: event.to_string(),
                        period,
                    },
                    other => other,
                })?;
                result |= bm;
            }
        }

        // OR mempart bitmap.
        if let Some(mp_bm) = mp_bitmap {
            result |= mp_bm.as_ref();
        }

        // Collect disk deltas.
        let mut all_deltas = RoaringBitmap::new();
        for pid in &delta_part_ids {
            if let Some(part) = Catalog::txn_get_part(&parts_table, *pid)? {
                let bm = part_store::mmap_read_part(&part.file_path).map_err(|e| match e {
                    InoxSetError::BitmapCorrupted { .. } => InoxSetError::BitmapCorrupted {
                        event: event.to_string(),
                        period,
                    },
                    other => other,
                })?;
                all_deltas |= bm;
            }
        }

        // OR mempart delta.
        if let Some(mp_d) = mp_delta {
            all_deltas |= mp_d.as_ref();
        }

        // AND-NOT deltas.
        if !all_deltas.is_empty() {
            result -= all_deltas;
        }

        self.metrics.bitmap_read(
            event,
            &period,
            data_part_ids.len() as u32,
            delta_part_ids.len() as u32,
            0,
        );

        Ok(result)
    }

    /// Retrieves bitmaps for a range of periods (inclusive).
    ///
    /// Returns a vector of `(Period, RoaringBitmap)` tuples for each period
    /// in the range that has at least one set bit. Empty periods are omitted.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn get_range(
        &self,
        event: &str,
        start: Period,
        end: Period,
    ) -> Result<Vec<(Period, RoaringBitmap)>> {
        self.check_closed()?;
        let periods = Self::enumerate_periods(start, end);
        // Guard against unbounded ranges that would degrade into a DoS.
        const MAX_RANGE_PERIODS: usize = 366 * 24; // ~1 year of hours
        if periods.len() > MAX_RANGE_PERIODS {
            return Err(InoxSetError::Configuration(format!(
                "get_range spans {} periods, limit is {}",
                periods.len(),
                MAX_RANGE_PERIODS,
            )));
        }
        let mut results = Vec::with_capacity(periods.len());
        for p in periods {
            let bm = self.get(event, p)?;
            if !bm.is_empty() {
                results.push((p, bm));
            }
        }
        Ok(results)
    }

    /// Returns the cardinality (number of set bits) for the given event and period.
    ///
    /// For compacted periods with a single data part and no in-memory overlay,
    /// this takes an O(1) fast path by reading the cached cardinality from the
    /// catalog instead of deserializing the full bitmap.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn cardinality(&self, event: &str, period: Period) -> Result<u64> {
        self.check_closed()?;

        // Fast path: compacted period with single part and no mempart overlay.
        let has_mempart_data;
        let has_mempart_delta;
        {
            let mp = self.writer.read().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            has_mempart_data = mp.get_bitmap(event, &period).is_some();
            has_mempart_delta = mp.get_delta(event, &period).is_some();
        }

        if !has_mempart_data && !has_mempart_delta {
            if let Some(config) = self.catalog.get_event(event)? {
                let cat_key = catalog_key(event, config.finest_granularity, &period);
                if let Some(state) = self.catalog.get_period_state(&cat_key)? {
                    if state == PeriodState::Compacted {
                        let data_ids = self.catalog.get_period_parts(&cat_key)?;
                        let delta_ids = self.catalog.get_period_deltas(&cat_key)?;
                        if data_ids.len() == 1 && delta_ids.is_empty() {
                            if let Some(part) = self.catalog.get_part(data_ids[0])? {
                                return Ok(part.cardinality);
                            }
                        }
                    }
                }
            }
        }

        // Slow path: full merge.
        Ok(self.get(event, period)?.len())
    }

    /// Returns `true` if the event has any data for the given period.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn exists(&self, event: &str, period: Period) -> Result<bool> {
        let bm = self.get(event, period)?;
        Ok(!bm.is_empty())
    }

    /// Enumerates all periods from `start` to `end` (inclusive).
    ///
    /// Only works for same-granularity start and end periods; returns an
    /// empty vector for mismatched granularities or Static periods.
    fn enumerate_periods(start: Period, end: Period) -> Vec<Period> {
        if start.granularity() != end.granularity() {
            return vec![];
        }
        if start == Period::Static || end == Period::Static {
            return vec![Period::Static];
        }

        let mut periods = Vec::new();
        let mut current = Some(start);
        while let Some(p) = current {
            periods.push(p);
            if p == end {
                break;
            }
            current = Self::next_period(p);
            // Safety valve: if next_period returns None, break.
            if current.is_none() {
                break;
            }
        }
        periods
    }

    /// Returns the next period at the same granularity, or `None` for Static.
    fn next_period(p: Period) -> Option<Period> {
        match p {
            Period::Static => None,
            Period::Hour(y, m, d, h) => {
                if h >= 23 {
                    let dim = crate::period::days_in_month(y as i32, m as u32);
                    if d >= dim as u8 {
                        if m >= 12 {
                            Some(Period::Hour(y + 1, 1, 1, 0))
                        } else {
                            Some(Period::Hour(y, m + 1, 1, 0))
                        }
                    } else {
                        Some(Period::Hour(y, m, d + 1, 0))
                    }
                } else {
                    Some(Period::Hour(y, m, d, h + 1))
                }
            }
            Period::Day(y, m, d) => {
                let dim = crate::period::days_in_month(y as i32, m as u32);
                if d >= dim as u8 {
                    if m >= 12 {
                        Some(Period::Day(y + 1, 1, 1))
                    } else {
                        Some(Period::Day(y, m + 1, 1))
                    }
                } else {
                    Some(Period::Day(y, m, d + 1))
                }
            }
            Period::Month(y, m) => {
                if m >= 12 {
                    Some(Period::Month(y + 1, 1))
                } else {
                    Some(Period::Month(y, m + 1))
                }
            }
            Period::Year(y) => Some(Period::Year(y + 1)),
        }
    }

    // ─── Task 16: remove_bits ────────────────────────────────────────────────

    /// Removes specific user IDs from the given event and period by writing
    /// a delta (tombstone) bitmap.
    ///
    /// The delta is OR-accumulated in the in-memory buffer and propagated
    /// through the rollup chain if the event uses [`Rollup::Auto`].
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    pub fn remove_bits(&self, event: &str, period: Period, user_ids: &[u32]) -> Result<()> {
        self.check_writable()?;
        let config = self.ensure_event(event)?;

        let mut delta = RoaringBitmap::new();
        for &id in user_ids {
            delta.insert(id);
        }

        {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            mp.or_delta(event, period, &delta);
            rollup::apply_rollup_delta(&mut mp, &config, &period, &delta);
        }

        Ok(())
    }

    // ─── Task 17: replace_bitmap, bulk_replace ───────────────────────────────

    /// Replaces the entire bitmap for the given event and period.
    ///
    /// This writes a new data part file, atomically updates the catalog to
    /// point to only the new part, and clears any pending deltas. Old part
    /// files are deleted after the catalog commit.
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    pub fn replace_bitmap(&self, event: &str, period: Period, bitmap: RoaringBitmap) -> Result<()> {
        self.check_writable()?;
        let config = self.ensure_event(event)?;

        // Clear mempart entries for this event/period.
        {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            mp.bitmaps.remove(&(event.to_string(), period));
            mp.deltas.remove(&(event.to_string(), period));
        }

        let cat_key = catalog_key(event, config.finest_granularity, &period);

        // Pre-collect old part IDs and file paths BEFORE the transaction,
        // because the txn will remove those catalog entries making them
        // unreachable afterwards (C3 fix).
        let old_data_ids = self.catalog.get_period_parts(&cat_key)?;
        let old_delta_ids = self.catalog.get_period_deltas(&cat_key)?;
        let mut old_file_paths = Vec::new();
        for &pid in old_data_ids.iter().chain(old_delta_ids.iter()) {
            if let Some(part) = self.catalog.get_part(pid)? {
                old_file_paths.push(part.file_path);
            }
        }

        // Write new part file.
        let now = self.now_unix();
        let txn = self.catalog.db().begin_write()?;
        let new_part_id;
        let new_file_path;
        {
            let mut next_id_table = txn.open_table(catalog::NEXT_PART_ID)?;
            let mut parts_table = txn.open_table(catalog::PARTS)?;
            let mut pp_table = txn.open_table(catalog::PERIOD_PARTS)?;
            let mut pd_table = txn.open_table(catalog::PERIOD_DELTAS)?;
            let mut ps_table = txn.open_table(catalog::PERIOD_STATE)?;

            let ids = Catalog::txn_alloc_part_ids(&mut next_id_table, 1)?;
            new_part_id = ids[0];

            new_file_path = part_store::part_file_path(
                &self.parts_root,
                event,
                config.finest_granularity,
                &period,
                new_part_id,
                PartKind::Data,
            );

            part_store::write_part(&new_file_path, &bitmap)?;

            let size_bytes = new_file_path
                .metadata()
                .map(|m| m.len())
                .unwrap_or(bitmap.serialized_size() as u64);

            let part = Part {
                part_id: new_part_id,
                kind: PartKind::Data,
                event: event.to_string(),
                period,
                file_path: new_file_path.clone(),
                size_bytes,
                cardinality: bitmap.len(),
                created_at: now,
                level: 0,
            };

            // Register new part.
            Catalog::txn_register_part(&mut parts_table, &part)?;

            // Set period parts to only the new part.
            Catalog::txn_set_period_parts(&mut pp_table, &cat_key, &[new_part_id])?;

            // Clear deltas.
            Catalog::txn_clear_period_deltas(&mut pd_table, &cat_key)?;

            // Ensure period state exists.
            if Catalog::txn_get_period_state(&ps_table, &cat_key)?.is_none() {
                Catalog::txn_set_period_state(&mut ps_table, &cat_key, PeriodState::Open)?;
            }

            // Remove old part entries.
            for &pid in old_data_ids.iter().chain(old_delta_ids.iter()) {
                parts_table.remove(pid)?;
            }
        }
        txn.commit()?;

        self.rebuild_read_index()?;

        // Delete old part files after commit using the pre-collected paths.
        for path in &old_file_paths {
            if path.exists() {
                let _ = part_store::delete_part(path);
            }
        }

        Ok(())
    }

    /// Atomically replaces bitmaps for multiple periods of the same event.
    ///
    /// Each entry in `entries` replaces the full bitmap for that period.
    /// All replacements share a single catalog transaction.
    ///
    /// # Errors
    ///
    /// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
    pub fn bulk_replace(&self, event: &str, entries: &[(Period, RoaringBitmap)]) -> Result<()> {
        self.check_writable()?;
        let config = self.ensure_event(event)?;

        // Collect old file paths for cleanup.
        let mut old_file_paths = Vec::new();

        // Clear mempart entries.
        {
            let mut mp = self.writer.write().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            for (period, _) in entries {
                mp.bitmaps.remove(&(event.to_string(), *period));
                mp.deltas.remove(&(event.to_string(), *period));
            }
        }

        // Collect old part info before the write txn.
        let mut old_parts_by_period: Vec<(String, Vec<u64>, Vec<u64>)> = Vec::new();
        for (period, _) in entries {
            let cat_key = catalog_key(event, config.finest_granularity, period);
            let old_data_ids = self.catalog.get_period_parts(&cat_key)?;
            let old_delta_ids = self.catalog.get_period_deltas(&cat_key)?;

            // Resolve file paths for old parts.
            for &pid in old_data_ids.iter().chain(old_delta_ids.iter()) {
                if let Some(part) = self.catalog.get_part(pid)? {
                    old_file_paths.push(part.file_path);
                }
            }

            old_parts_by_period.push((cat_key, old_data_ids, old_delta_ids));
        }

        let now = self.now_unix();
        let txn = self.catalog.db().begin_write()?;
        {
            let mut next_id_table = txn.open_table(catalog::NEXT_PART_ID)?;
            let mut parts_table = txn.open_table(catalog::PARTS)?;
            let mut pp_table = txn.open_table(catalog::PERIOD_PARTS)?;
            let mut pd_table = txn.open_table(catalog::PERIOD_DELTAS)?;
            let mut ps_table = txn.open_table(catalog::PERIOD_STATE)?;

            let ids = Catalog::txn_alloc_part_ids(&mut next_id_table, entries.len() as u64)?;

            for (i, (period, bitmap)) in entries.iter().enumerate() {
                let part_id = ids[i];
                let (ref cat_key, ref old_data_ids, ref old_delta_ids) = old_parts_by_period[i];

                let file_path = part_store::part_file_path(
                    &self.parts_root,
                    event,
                    config.finest_granularity,
                    period,
                    part_id,
                    PartKind::Data,
                );

                part_store::write_part(&file_path, bitmap)?;

                let size_bytes = file_path
                    .metadata()
                    .map(|m| m.len())
                    .unwrap_or(bitmap.serialized_size() as u64);

                let part = Part {
                    part_id,
                    kind: PartKind::Data,
                    event: event.to_string(),
                    period: *period,
                    file_path,
                    size_bytes,
                    cardinality: bitmap.len(),
                    created_at: now,
                    level: 0,
                };

                Catalog::txn_register_part(&mut parts_table, &part)?;
                Catalog::txn_set_period_parts(&mut pp_table, cat_key, &[part_id])?;
                Catalog::txn_clear_period_deltas(&mut pd_table, cat_key)?;

                if Catalog::txn_get_period_state(&ps_table, cat_key)?.is_none() {
                    Catalog::txn_set_period_state(&mut ps_table, cat_key, PeriodState::Open)?;
                }

                // Remove old part entries from parts table.
                for &pid in old_data_ids.iter().chain(old_delta_ids.iter()) {
                    parts_table.remove(pid)?;
                }
            }
        }
        txn.commit()?;
        self.rebuild_read_index()?;

        // Delete old files after commit.
        for path in &old_file_paths {
            if path.exists() {
                let _ = part_store::delete_part(path);
            }
        }

        Ok(())
    }

    // ─── Task 18: compact, health, close ─────────────────────────────────────

    /// Compacts all events by merging data parts and applying deltas.
    ///
    /// Returns statistics about the compaction run.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    ///
    /// # Platform notes
    ///
    /// Concurrent `get()` + `compact()` is safe on Unix: unlink semantics keep
    /// memory-mapped files valid until the last handle closes. On Windows,
    /// concurrent compact + read may fail if part files are memory-mapped.
    pub fn compact(&self) -> Result<CompactStats> {
        self.check_writable()?;
        let mut stats = CompactStats::default();
        let events = self.catalog.list_events()?;
        for ev in &events {
            let keys = self.catalog.period_keys_for_event(&ev.name)?;
            for key in &keys {
                self.compact_period(key, &mut stats)?;
            }
        }
        self.metrics.compaction_completed(
            stats.periods_compacted,
            stats.parts_merged,
            stats.deltas_applied,
            stats.bytes_reclaimed,
        );
        // Write compaction record to the compaction log (C2 fix).
        let now = self.now_unix();
        self.catalog.write_compaction_log(
            now,
            stats.periods_compacted,
            stats.parts_merged,
            stats.deltas_applied,
            stats.bytes_reclaimed,
        )?;
        Ok(stats)
    }

    /// Compacts all periods for a single event.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog or file I/O failure.
    pub fn compact_event(&self, event: &str) -> Result<CompactStats> {
        self.check_writable()?;
        let mut stats = CompactStats::default();
        let keys = self.catalog.period_keys_for_event(event)?;
        for key in &keys {
            self.compact_period(key, &mut stats)?;
        }
        self.metrics.compaction_completed(
            stats.periods_compacted,
            stats.parts_merged,
            stats.deltas_applied,
            stats.bytes_reclaimed,
        );
        Ok(stats)
    }

    /// Compacts a single period identified by its catalog key.
    fn compact_period(&self, cat_key: &str, stats: &mut CompactStats) -> Result<()> {
        let data_ids = self.catalog.get_period_parts(cat_key)?;
        let delta_ids = self.catalog.get_period_deltas(cat_key)?;

        if !merge::is_eligible(data_ids.len(), delta_ids.len()) {
            return Ok(());
        }

        // Resolve paths and collect old file info.
        let mut data_paths = Vec::new();
        let mut delta_paths = Vec::new();
        let mut old_parts: Vec<Part> = Vec::new();

        for &pid in &data_ids {
            if let Some(part) = self.catalog.get_part(pid)? {
                data_paths.push(part.file_path.clone());
                old_parts.push(part);
            }
        }
        for &pid in &delta_ids {
            if let Some(part) = self.catalog.get_part(pid)? {
                delta_paths.push(part.file_path.clone());
                old_parts.push(part);
            }
        }

        // Merge.
        let merged = merge::merge_parts(&data_paths, &delta_paths)?;

        // Determine event/period from the first old part.
        let representative = old_parts
            .first()
            .ok_or_else(|| InoxSetError::CatalogCorrupted {
                context: format!("compact_period: no parts found for {cat_key}"),
            })?;
        let event = &representative.event;
        let period = representative.period;

        // Resolve event config before opening the write transaction to avoid
        // deadlocking if auto-registration triggers its own write txn.
        let config = self.ensure_event(event)?;

        // Allocate new part ID and write merged file.
        let now = self.now_unix();
        let new_part_size: u64;
        let txn = self.catalog.db().begin_write()?;
        {
            let mut next_id_table = txn.open_table(catalog::NEXT_PART_ID)?;
            let mut parts_table = txn.open_table(catalog::PARTS)?;
            let mut pp_table = txn.open_table(catalog::PERIOD_PARTS)?;
            let mut pd_table = txn.open_table(catalog::PERIOD_DELTAS)?;
            let mut ps_table = txn.open_table(catalog::PERIOD_STATE)?;

            let ids = Catalog::txn_alloc_part_ids(&mut next_id_table, 1)?;
            let new_id = ids[0];

            let max_level = old_parts.iter().map(|p| p.level).max().unwrap_or(0);
            let file_path = part_store::part_file_path(
                &self.parts_root,
                event,
                config.finest_granularity,
                &period,
                new_id,
                PartKind::Data,
            );

            part_store::write_part(&file_path, &merged)?;

            let size_bytes = file_path
                .metadata()
                .map(|m| m.len())
                .unwrap_or(merged.serialized_size() as u64);
            new_part_size = size_bytes;

            let part = Part {
                part_id: new_id,
                kind: PartKind::Data,
                event: event.clone(),
                period,
                file_path,
                size_bytes,
                cardinality: merged.len(),
                created_at: now,
                level: max_level.saturating_add(1),
            };

            Catalog::txn_register_part(&mut parts_table, &part)?;
            Catalog::txn_set_period_parts(&mut pp_table, cat_key, &[new_id])?;
            Catalog::txn_clear_period_deltas(&mut pd_table, cat_key)?;

            // Update period state to Compacted.
            Catalog::txn_set_period_state(&mut ps_table, cat_key, PeriodState::Compacted)?;

            // Remove old part entries.
            for old in &old_parts {
                parts_table.remove(old.part_id)?;
            }
        }
        txn.commit()?;
        self.rebuild_read_index()?;

        // Update stats (subtract new merged part size for net reclamation).
        let old_bytes: u64 = old_parts.iter().map(|p| p.size_bytes).sum();
        let bytes_reclaimed = old_bytes.saturating_sub(new_part_size);
        stats.periods_compacted += 1;
        stats.parts_merged += data_ids.len() as u32;
        stats.deltas_applied += delta_ids.len() as u32;
        stats.bytes_reclaimed += bytes_reclaimed;

        // Delete old files after commit.
        for old in &old_parts {
            if old.file_path.exists() {
                let _ = part_store::delete_part(&old.file_path);
            }
        }

        Ok(())
    }

    /// Returns an operational health snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error on catalog I/O failure.
    pub fn health(&self) -> Result<Health> {
        self.check_closed()?;

        let (mempart_size_bytes, mempart_entries) = {
            let mp = self.writer.read().map_err(|e| {
                log::error!("RwLock poisoned: {e}");
                InoxSetError::Closed
            })?;
            (mp.size_bytes(), (mp.bitmaps.len() + mp.deltas.len()) as u32)
        };

        let events = self.catalog.list_events()?;
        let total_events = events.len() as u32;

        let mut total_data_parts = 0u64;
        let mut total_delta_parts = 0u64;
        let mut disk_usage_bytes = 0u64;
        let mut open_periods = 0u32;
        let mut closed_periods = 0u32;
        let mut compacted_periods = 0u32;
        let mut periods_needing_compaction = 0u32;

        // Single batched read transaction for all per-period lookups (I1 fix).
        let txn = self.catalog.db().begin_read()?;
        let pp_table = txn.open_table(catalog::PERIOD_PARTS)?;
        let pd_table = txn.open_table(catalog::PERIOD_DELTAS)?;
        let ps_table = txn.open_table(catalog::PERIOD_STATE)?;
        let parts_table = txn.open_table(catalog::PARTS)?;

        for ev in &events {
            let keys = self.catalog.period_keys_for_event(&ev.name)?;
            for key in &keys {
                let data_ids = Catalog::txn_get_period_parts(&pp_table, key)?;
                let delta_ids = Catalog::txn_get_period_deltas(&pd_table, key)?;

                total_data_parts += data_ids.len() as u64;
                total_delta_parts += delta_ids.len() as u64;

                if merge::is_eligible(data_ids.len(), delta_ids.len()) {
                    periods_needing_compaction += 1;
                }

                // Count disk usage.
                for &pid in data_ids.iter().chain(delta_ids.iter()) {
                    if let Some(part) = Catalog::txn_get_part(&parts_table, pid)? {
                        disk_usage_bytes += part.size_bytes;
                    }
                }

                // Count period states.
                if let Some(state) = Catalog::txn_get_period_state(&ps_table, key)? {
                    match state {
                        PeriodState::Open => open_periods += 1,
                        PeriodState::Closed => closed_periods += 1,
                        PeriodState::Compacted => compacted_periods += 1,
                        PeriodState::Dropped => {}
                    }
                }
            }
        }

        Ok(Health {
            catalog_ok: true,
            mempart_size_bytes,
            mempart_entries,
            total_events,
            total_data_parts,
            total_delta_parts,
            open_periods,
            closed_periods,
            compacted_periods,
            periods_needing_compaction,
            disk_usage_bytes,
        })
    }

    /// Flushes any buffered data and marks the store as closed.
    ///
    /// After calling `close`, all subsequent operations will return
    /// [`InoxSetError::Closed`].
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            // Already closed.
            return Ok(());
        }
        if !self.read_only {
            self.flush_internal()?;
        }
        Ok(())
    }
}

impl Drop for InoxSet {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::SeqCst) && !self.read_only {
            let _ = self.flush_internal();
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use tempfile::TempDir;

    fn test_store() -> (InoxSet, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .default_granularity(Granularity::Day)
            .default_rollup(Rollup::None)
            .clock(|| 1_773_500_000) // approx 2026-03-12
            .open()
            .unwrap();
        (store, dir)
    }

    fn test_store_with_rollup() -> (InoxSet, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .default_granularity(Granularity::Hour)
            .default_rollup(Rollup::Auto)
            .clock(|| 1_773_500_000)
            .open()
            .unwrap();
        (store, dir)
    }

    fn bitmap_with(ids: &[u32]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for &id in ids {
            bm.insert(id);
        }
        bm
    }

    // ─── Task 12: Event Management ───────────────────────────────────────────

    #[test]
    fn register_and_list_events() {
        let (store, _dir) = test_store();
        store
            .register_event("active", Granularity::Day, Rollup::None)
            .unwrap();
        store
            .register_event("purchase", Granularity::Hour, Rollup::Auto)
            .unwrap();
        let events = store.list_events().unwrap();
        assert_eq!(events.len(), 2);
        let names: Vec<&str> = events.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"active"));
        assert!(names.contains(&"purchase"));
    }

    #[test]
    fn register_duplicate_errors() {
        let (store, _dir) = test_store();
        store
            .register_event("active", Granularity::Day, Rollup::None)
            .unwrap();
        let result = store.register_event("active", Granularity::Day, Rollup::None);
        assert!(matches!(
            result,
            Err(InoxSetError::EventAlreadyRegistered(_))
        ));
    }

    #[test]
    fn drop_event_removes_all() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("active", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store.flush().unwrap();
        store.drop_event("active").unwrap();
        assert!(store.list_events().unwrap().is_empty());
        let bm = store.get("active", Period::Day(2026, 3, 11)).unwrap();
        assert!(bm.is_empty());
    }

    #[test]
    fn invalid_event_name_rejected() {
        let (store, _dir) = test_store();
        let result = store.register_event("foo bar", Granularity::Day, Rollup::None);
        assert!(matches!(result, Err(InoxSetError::InvalidEventName(_))));
    }

    // ─── Task 13: put_bitmap ─────────────────────────────────────────────────

    #[test]
    fn put_bitmap_basic() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("active", Period::Day(2026, 3, 11), bitmap_with(&[1, 2]))
            .unwrap();
        let bm = store.get("active", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(1));
        assert!(bm.contains(2));
    }

    #[test]
    fn put_bitmap_auto_registers() {
        let (store, _dir) = test_store();
        // "ev" is not pre-registered; put_bitmap should auto-register it.
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[42]))
            .unwrap();
        let events = store.list_events().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "ev");
    }

    #[test]
    fn put_bitmap_granularity_mismatch() {
        let (store, _dir) = test_store();
        store
            .register_event("hourly", Granularity::Hour, Rollup::None)
            .unwrap();
        // Trying to write a Day period to an Hour event should fail.
        let result = store.put_bitmap("hourly", Period::Day(2026, 3, 11), bitmap_with(&[1]));
        assert!(matches!(
            result,
            Err(InoxSetError::GranularityMismatch { .. })
        ));
    }

    #[test]
    fn put_bitmap_or_accumulates() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[2]))
            .unwrap();
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(1) && bm.contains(2));
    }

    #[test]
    fn put_bitmap_with_rollup() {
        let (store, _dir) = test_store_with_rollup();
        store
            .put_bitmap(
                "active",
                Period::Hour(2026, 3, 11, 14),
                bitmap_with(&[1, 2, 3]),
            )
            .unwrap();
        // Check that rollup populated ancestor periods.
        let day = store.get("active", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(day.len(), 3);
        let month = store.get("active", Period::Month(2026, 3)).unwrap();
        assert_eq!(month.len(), 3);
        let year = store.get("active", Period::Year(2026)).unwrap();
        assert_eq!(year.len(), 3);
    }

    #[test]
    fn put_bitmap_static() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .default_granularity(Granularity::None)
            .default_rollup(Rollup::None)
            .clock(|| 1_773_500_000)
            .open()
            .unwrap();
        store
            .put_bitmap("geo", Period::Static, bitmap_with(&[10, 20]))
            .unwrap();
        let bm = store.get("geo", Period::Static).unwrap();
        assert_eq!(bm.len(), 2);
    }

    // ─── Task 14: flush ──────────────────────────────────────────────────────

    #[test]
    fn flush_persists_mempart() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store.flush().unwrap();
        // Verify data is on disk by reading after flush.
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 3);
    }

    #[test]
    fn flush_empty_noop() {
        let (store, _dir) = test_store();
        // Flushing an empty mempart should not error.
        store.flush().unwrap();
    }

    #[test]
    fn flush_creates_period_state() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        store.flush().unwrap();
        let cat_key = catalog_key("ev", Granularity::Day, &Period::Day(2026, 3, 11));
        let state = store.catalog.get_period_state(&cat_key).unwrap();
        assert_eq!(state, Some(PeriodState::Open));
    }

    // ─── Task 15: Read Path ──────────────────────────────────────────────────

    #[test]
    fn get_after_flush() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store.flush().unwrap();
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(1) && bm.contains(2) && bm.contains(3));
    }

    #[test]
    fn get_merges_mempart_and_disk() {
        let (store, _dir) = test_store();
        // Write and flush some data.
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2]))
            .unwrap();
        store.flush().unwrap();
        // Write more data (in mempart).
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[3, 4]))
            .unwrap();
        // get() should merge disk + mempart.
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 4);
        for id in 1u32..=4 {
            assert!(bm.contains(id), "missing id {id}");
        }
    }

    #[test]
    fn get_nonexistent_returns_empty() {
        let (store, _dir) = test_store();
        let bm = store.get("nope", Period::Day(2026, 1, 1)).unwrap();
        assert!(bm.is_empty());
    }

    #[test]
    fn cardinality_matches_get() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        let card = store.cardinality("ev", Period::Day(2026, 3, 11)).unwrap();
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(card, bm.len());
    }

    #[test]
    fn exists_true_and_false() {
        let (store, _dir) = test_store();
        assert!(!store.exists("ev", Period::Day(2026, 3, 11)).unwrap());
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        assert!(store.exists("ev", Period::Day(2026, 3, 11)).unwrap());
    }

    #[test]
    fn get_range_returns_multiple() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 12), bitmap_with(&[2]))
            .unwrap();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 13), bitmap_with(&[3]))
            .unwrap();
        let range = store
            .get_range("ev", Period::Day(2026, 3, 11), Period::Day(2026, 3, 13))
            .unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, Period::Day(2026, 3, 11));
        assert_eq!(range[0].1.len(), 1);
        assert_eq!(range[2].0, Period::Day(2026, 3, 13));
        assert_eq!(range[2].1.len(), 1);
    }

    // ─── Task 16: remove_bits ────────────────────────────────────────────────

    #[test]
    fn remove_bits_basic() {
        let (store, _dir) = test_store();
        store
            .put_bitmap(
                "ev",
                Period::Day(2026, 3, 11),
                bitmap_with(&[1, 2, 3, 4, 5]),
            )
            .unwrap();
        store
            .remove_bits("ev", Period::Day(2026, 3, 11), &[2, 4])
            .unwrap();
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(1) && bm.contains(3) && bm.contains(5));
        assert!(!bm.contains(2) && !bm.contains(4));
    }

    #[test]
    fn remove_bits_propagates_rollup() {
        let (store, _dir) = test_store_with_rollup();
        store
            .put_bitmap("ev", Period::Hour(2026, 3, 11, 14), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store
            .remove_bits("ev", Period::Hour(2026, 3, 11, 14), &[2])
            .unwrap();
        // The delta should propagate to Day too.
        let day = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(day.len(), 2);
        assert!(!day.contains(2));
    }

    // ─── Task 17: replace_bitmap, bulk_replace ───────────────────────────────

    #[test]
    fn replace_bitmap_basic() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store.flush().unwrap();
        store
            .replace_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[10, 20]))
            .unwrap();
        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(10) && bm.contains(20));
        assert!(!bm.contains(1));
    }

    #[test]
    fn bulk_replace_atomic() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 12), bitmap_with(&[2]))
            .unwrap();
        store.flush().unwrap();

        store
            .bulk_replace(
                "ev",
                &[
                    (Period::Day(2026, 3, 11), bitmap_with(&[100])),
                    (Period::Day(2026, 3, 12), bitmap_with(&[200])),
                ],
            )
            .unwrap();

        let bm1 = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm1.len(), 1);
        assert!(bm1.contains(100));

        let bm2 = store.get("ev", Period::Day(2026, 3, 12)).unwrap();
        assert_eq!(bm2.len(), 1);
        assert!(bm2.contains(200));
    }

    // ─── Task 18: compact, health, close ─────────────────────────────────────

    #[test]
    fn compact_merges_parts() {
        let (store, _dir) = test_store();
        // Create two separate data parts by flushing twice.
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2]))
            .unwrap();
        store.flush().unwrap();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[3, 4]))
            .unwrap();
        store.flush().unwrap();

        let cat_key = catalog_key("ev", Granularity::Day, &Period::Day(2026, 3, 11));
        let parts_before = store.catalog.get_period_parts(&cat_key).unwrap();
        assert_eq!(
            parts_before.len(),
            2,
            "should have 2 data parts before compact"
        );

        let stats = store.compact().unwrap();
        assert!(stats.periods_compacted >= 1);
        assert!(stats.parts_merged >= 2);

        let parts_after = store.catalog.get_period_parts(&cat_key).unwrap();
        assert_eq!(
            parts_after.len(),
            1,
            "should have 1 data part after compact"
        );

        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 4);
    }

    #[test]
    fn compact_applies_deltas() {
        let (store, _dir) = test_store();
        store
            .put_bitmap(
                "ev",
                Period::Day(2026, 3, 11),
                bitmap_with(&[1, 2, 3, 4, 5]),
            )
            .unwrap();
        store.flush().unwrap();
        store
            .remove_bits("ev", Period::Day(2026, 3, 11), &[3, 5])
            .unwrap();
        store.flush().unwrap();

        let stats = store.compact().unwrap();
        assert!(stats.deltas_applied >= 1);

        let bm = store.get("ev", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(1) && bm.contains(2) && bm.contains(4));
        assert!(!bm.contains(3) && !bm.contains(5));
    }

    #[test]
    fn health_returns_stats() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        store.flush().unwrap();
        let h = store.health().unwrap();
        assert!(h.catalog_ok);
        assert!(h.total_events >= 1);
        assert!(h.total_data_parts >= 1);
    }

    #[test]
    fn close_flushes() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2]))
            .unwrap();
        store.close().unwrap();
        // After close, operations should fail.
        let result = store.put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[3]));
        assert!(matches!(result, Err(InoxSetError::Closed)));
    }

    // ─── Eng Decision 6: 7 mandatory tests ───────────────────────────────────

    #[test]
    fn auto_flush_on_threshold() {
        let dir = TempDir::new().unwrap();
        // Set flush threshold to 1 byte so any write triggers auto-flush.
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .default_granularity(Granularity::Day)
            .default_rollup(Rollup::None)
            .mempart_flush_threshold(1)
            .clock(|| 1_773_500_000)
            .open()
            .unwrap();

        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 12), bitmap_with(&[4, 5, 6]))
            .unwrap();

        // Auto-flush should have fired; mempart should be empty.
        let h = store.health().unwrap();
        assert_eq!(
            h.mempart_entries, 0,
            "mempart should be empty after auto-flush"
        );
        assert!(h.total_data_parts > 0, "data parts should exist on disk");
    }

    #[test]
    fn backfill_reverts_compacted_to_closed() {
        let (store, _dir) = test_store();
        let period = Period::Day(2025, 1, 1); // historical/closed period

        // Write + flush twice so compact has two parts to merge (is_eligible requires > 1).
        store
            .put_bitmap("ev", period, bitmap_with(&[1, 2]))
            .unwrap();
        store.flush().unwrap();
        store.put_bitmap("ev", period, bitmap_with(&[3])).unwrap();
        store.flush().unwrap();

        store.compact().unwrap();

        // Period is now Compacted.
        let cat_key = catalog_key("ev", Granularity::Day, &period);
        let state = store.catalog.get_period_state(&cat_key).unwrap();
        assert_eq!(state, Some(PeriodState::Compacted));

        // Writing to a compacted period (backfill) should revert state to Closed
        // and accept the write.
        store.put_bitmap("ev", period, bitmap_with(&[99])).unwrap();

        // The mempart now has data for this period.
        let bm = store.get("ev", period).unwrap();
        assert!(bm.contains(99), "backfill data should be readable");

        // Period state should be reverted to Closed.
        let state_after = store.catalog.get_period_state(&cat_key).unwrap();
        assert_eq!(state_after, Some(PeriodState::Closed));
    }

    #[test]
    fn cardinality_fast_path_compacted() {
        let (store, _dir) = test_store();
        let period = Period::Day(2025, 6, 1);

        // Write + flush twice so compact has two parts to merge (is_eligible requires > 1).
        store
            .put_bitmap("ev", period, bitmap_with(&[10, 20]))
            .unwrap();
        store.flush().unwrap();
        store.put_bitmap("ev", period, bitmap_with(&[30])).unwrap();
        store.flush().unwrap();

        store.compact().unwrap();

        // After compact: single part, Compacted state, no mempart overlay.
        let card = store.cardinality("ev", period).unwrap();
        assert_eq!(card, 3, "fast-path cardinality should return 3");
    }

    #[test]
    fn read_only_rejects_writes() {
        let dir = TempDir::new().unwrap();
        // First, create the store to initialize the database.
        {
            let store = InoxSet::builder()
                .path(dir.path().join("data"))
                .default_granularity(Granularity::Day)
                .default_rollup(Rollup::None)
                .clock(|| 1_773_500_000)
                .open()
                .unwrap();
            store
                .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]))
                .unwrap();
            store.close().unwrap();
        }
        // Reopen read-only.
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .read_only(true)
            .clock(|| 1_773_500_000)
            .open()
            .unwrap();
        let result = store.put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[2]));
        assert!(
            matches!(result, Err(InoxSetError::ReadOnly)),
            "read-only store should reject writes"
        );
    }

    #[test]
    fn closed_rejects_operations() {
        let (store, _dir) = test_store();
        store.close().unwrap();
        let result = store.put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1]));
        assert!(
            matches!(result, Err(InoxSetError::Closed)),
            "closed store should return Closed error"
        );
    }

    #[test]
    fn compact_event_only_compacts_target() {
        let (store, _dir) = test_store();

        // Write two flushes to event "a".
        store
            .put_bitmap("a", Period::Day(2026, 3, 11), bitmap_with(&[1]))
            .unwrap();
        store.flush().unwrap();
        store
            .put_bitmap("a", Period::Day(2026, 3, 11), bitmap_with(&[2]))
            .unwrap();
        store.flush().unwrap();

        // Write one flush to event "b".
        store
            .put_bitmap("b", Period::Day(2026, 3, 11), bitmap_with(&[10]))
            .unwrap();
        store.flush().unwrap();

        let cat_key_a = catalog_key("a", Granularity::Day, &Period::Day(2026, 3, 11));
        let cat_key_b = catalog_key("b", Granularity::Day, &Period::Day(2026, 3, 11));

        let parts_b_before = store.catalog.get_period_parts(&cat_key_b).unwrap().len();
        assert_eq!(
            parts_b_before, 1,
            "event b should have 1 part before compact"
        );

        // Compact only event "a".
        store.compact_event("a").unwrap();

        let parts_a_after = store.catalog.get_period_parts(&cat_key_a).unwrap().len();
        assert_eq!(
            parts_a_after, 1,
            "event a should have 1 merged part after compact"
        );

        let parts_b_after = store.catalog.get_period_parts(&cat_key_b).unwrap().len();
        assert_eq!(
            parts_b_after, parts_b_before,
            "event b should be unaffected by compact_event(a)"
        );
    }

    #[test]
    fn get_with_missing_part_file() {
        let (store, _dir) = test_store();
        store
            .put_bitmap("ev", Period::Day(2026, 3, 11), bitmap_with(&[1, 2, 3]))
            .unwrap();
        store.flush().unwrap();

        // Find and delete the part file on disk.
        let cat_key = catalog_key("ev", Granularity::Day, &Period::Day(2026, 3, 11));
        let part_ids = store.catalog.get_period_parts(&cat_key).unwrap();
        assert!(!part_ids.is_empty(), "should have at least one part");
        let part = store.catalog.get_part(part_ids[0]).unwrap().unwrap();
        std::fs::remove_file(&part.file_path).expect("delete part file");

        // get() should return an error, not panic.
        let result = store.get("ev", Period::Day(2026, 3, 11));
        assert!(
            result.is_err(),
            "get() should fail when part file is missing"
        );
    }

    #[test]
    fn list_periods_returns_flushed_periods() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        bm.insert(1);

        store
            .put_bitmap("clicks", Period::Day(2026, 3, 10), bm.clone())
            .unwrap();
        store
            .put_bitmap("clicks", Period::Day(2026, 3, 11), bm.clone())
            .unwrap();
        store
            .put_bitmap("clicks", Period::Day(2026, 3, 12), bm)
            .unwrap();
        store.flush().unwrap();

        let periods = store.list_periods("clicks").unwrap();
        assert_eq!(
            periods,
            vec![
                Period::Day(2026, 3, 10),
                Period::Day(2026, 3, 11),
                Period::Day(2026, 3, 12),
            ]
        );
    }

    #[test]
    fn delete_period_removes_data_and_files() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        bm.extend(0..100);

        store
            .put_bitmap("visits", Period::Day(2026, 3, 10), bm.clone())
            .unwrap();
        store
            .put_bitmap("visits", Period::Day(2026, 3, 11), bm.clone())
            .unwrap();
        store.flush().unwrap();

        // Delete one period.
        store
            .delete_period("visits", Period::Day(2026, 3, 10))
            .unwrap();

        // Deleted period returns empty bitmap.
        let got = store.get("visits", Period::Day(2026, 3, 10)).unwrap();
        assert!(got.is_empty());

        // Other period is intact.
        let got = store.get("visits", Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(got.len(), 100);

        // list_periods reflects the deletion.
        let periods = store.list_periods("visits").unwrap();
        assert_eq!(periods, vec![Period::Day(2026, 3, 11)]);
    }

    #[test]
    fn retain_periods_keeps_matching() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        bm.insert(1);

        for d in 1..=5 {
            store
                .put_bitmap("logins", Period::Day(2026, 3, d), bm.clone())
                .unwrap();
        }
        store.flush().unwrap();

        // Keep only days >= 4.
        let deleted = store
            .retain_periods("logins", |p| *p >= Period::Day(2026, 3, 4))
            .unwrap();
        assert_eq!(deleted, 3);

        let periods = store.list_periods("logins").unwrap();
        assert_eq!(
            periods,
            vec![Period::Day(2026, 3, 4), Period::Day(2026, 3, 5)]
        );
    }

    #[test]
    fn delete_period_clears_unflushed_mempart() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        bm.extend(0..50);

        // Write but don't flush.
        store
            .put_bitmap("events", Period::Day(2026, 4, 1), bm)
            .unwrap();

        // Delete the unflushed period.
        store
            .delete_period("events", Period::Day(2026, 4, 1))
            .unwrap();

        // Should be empty even without flush.
        let got = store.get("events", Period::Day(2026, 4, 1)).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn put_ids_get_ids_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        store
            .put_ids(
                "segment",
                Period::Day(2026, 3, 18),
                &["user-aaa", "user-bbb", "user-ccc"],
            )
            .unwrap();
        store.flush().unwrap();

        let ids = store.get_ids("segment", Period::Day(2026, 3, 18)).unwrap();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["user-aaa", "user-bbb", "user-ccc"]);
    }

    #[test]
    fn remove_ids_deletes_through_dictionary() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        store
            .put_ids(
                "seg",
                Period::Day(2026, 3, 18),
                &["alice", "bob", "charlie"],
            )
            .unwrap();

        store
            .remove_ids("seg", Period::Day(2026, 3, 18), &["bob"])
            .unwrap();

        let ids = store.get_ids("seg", Period::Day(2026, 3, 18)).unwrap();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["alice", "charlie"]);
    }

    #[test]
    fn remove_ids_unknown_id_is_noop() {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        store.put_ids("seg", Period::Static, &["x", "y"]).unwrap();

        store.remove_ids("seg", Period::Static, &["z"]).unwrap();

        let ids = store.get_ids("seg", Period::Static).unwrap();
        assert_eq!(ids.len(), 2);
    }
}
