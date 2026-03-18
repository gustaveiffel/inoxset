//! In-memory write buffer for the inoxset storage engine.
//!
//! [`MemPart`] accumulates bitmap data and delta (tombstone) bitmaps in memory
//! via OR-accumulation before they are flushed to immutable part files on disk.
//! It tracks approximate serialized-size so callers can decide when to trigger
//! a flush.
//!
//! Keys are `(event_name, Period)` tuples stored in flat [`HashMap`]s; this
//! keeps the implementation simple and avoids nested-map overhead.
//!
//! # Design notes
//!
//! - Bitmaps are stored behind [`Arc`] so that callers can cheaply retain a
//!   reference to a snapshot entry without copying the bitmap data.
//! - [`MemPart::take_snapshot`] atomically drains the buffer into a
//!   [`MemPartSnapshot`] and resets the buffer to empty. The snapshot can then
//!   be flushed to disk without holding a write lock on the live buffer.
//! - Size tracking uses [`RoaringBitmap::serialized_size`] as a proxy for
//!   heap usage. The value is approximate because `Arc` overhead and
//!   `HashMap` metadata are not included.

use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::types::Period;

/// In-memory write buffer accumulating bitmap and delta data by `(event, period)`.
///
/// Writes OR new bits into existing entries; there is no way to remove bits
/// from a `MemPart` — use delta bitmaps for that.
///
/// Call [`take_snapshot`](MemPart::take_snapshot) to drain the buffer for
/// flushing.
pub struct MemPart {
    /// Additive bitmap entries keyed by `(event_name, period)`.
    pub bitmaps: HashMap<(String, Period), Arc<RoaringBitmap>>,
    /// Tombstone / delete-delta entries keyed by `(event_name, period)`.
    pub deltas: HashMap<(String, Period), Arc<RoaringBitmap>>,
    /// Approximate in-memory size based on serialized bitmap sizes.
    size_bytes: u64,
}

/// Immutable snapshot of a [`MemPart`] taken at a point in time.
///
/// Produced by [`MemPart::take_snapshot`]. Intended to be handed off to a
/// background flush task while the live [`MemPart`] continues accepting writes.
pub struct MemPartSnapshot {
    /// Additive bitmap entries at the time of the snapshot.
    pub bitmaps: HashMap<(String, Period), Arc<RoaringBitmap>>,
    /// Delete-delta entries at the time of the snapshot.
    pub deltas: HashMap<(String, Period), Arc<RoaringBitmap>>,
    /// Approximate serialized size of all bitmaps in this snapshot, in bytes.
    pub size_bytes: u64,
}

impl MemPart {
    /// Creates an empty `MemPart`.
    pub fn new() -> Self {
        Self {
            bitmaps: HashMap::new(),
            deltas: HashMap::new(),
            size_bytes: 0,
        }
    }

    /// OR `bitmap` into the additive entry for `(event, period)`.
    ///
    /// If no entry exists yet, one is created from `bitmap`. The internal size
    /// counter is updated to reflect the change in serialized footprint.
    pub fn or_bitmap(&mut self, event: &str, period: Period, bitmap: &RoaringBitmap) {
        let key = (event.to_string(), period);
        let entry = self
            .bitmaps
            .entry(key)
            .or_insert_with(|| Arc::new(RoaringBitmap::new()));
        let existing = Arc::make_mut(entry);
        let old_size = existing.serialized_size();
        *existing |= bitmap;
        let new_size = existing.serialized_size();
        self.size_bytes = self
            .size_bytes
            .wrapping_add(new_size as u64)
            .wrapping_sub(old_size as u64);
    }

    /// OR `bitmap` into the delete-delta entry for `(event, period)`.
    ///
    /// Delta bitmaps record bits that have been deleted; they are applied
    /// (subtracted) during compaction to produce clean merged parts.
    pub fn or_delta(&mut self, event: &str, period: Period, bitmap: &RoaringBitmap) {
        let key = (event.to_string(), period);
        let entry = self
            .deltas
            .entry(key)
            .or_insert_with(|| Arc::new(RoaringBitmap::new()));
        let existing = Arc::make_mut(entry);
        let old_size = existing.serialized_size();
        *existing |= bitmap;
        let new_size = existing.serialized_size();
        self.size_bytes = self
            .size_bytes
            .wrapping_add(new_size as u64)
            .wrapping_sub(old_size as u64);
    }

    /// Returns an `Arc`-clone of the additive bitmap for `(event, period)`, or
    /// `None` if no data has been written for that key.
    ///
    /// Cloning an `Arc` is a cheap reference-count increment; no bitmap data is
    /// copied.
    pub fn get_bitmap(&self, event: &str, period: &Period) -> Option<Arc<RoaringBitmap>> {
        self.bitmaps.get(&(event.to_string(), *period)).cloned()
    }

    /// Returns an `Arc`-clone of the delete-delta bitmap for `(event, period)`,
    /// or `None` if no delta has been written for that key.
    pub fn get_delta(&self, event: &str, period: &Period) -> Option<Arc<RoaringBitmap>> {
        self.deltas.get(&(event.to_string(), *period)).cloned()
    }

    /// Drains the buffer into a [`MemPartSnapshot`] and resets `self` to empty.
    ///
    /// The snapshot owns all bitmap data; `self` is left with empty maps and a
    /// zeroed size counter. This allows a caller to flush the snapshot to disk
    /// while new writes accumulate in the now-empty buffer.
    pub fn take_snapshot(&mut self) -> MemPartSnapshot {
        let snap = MemPartSnapshot {
            bitmaps: std::mem::take(&mut self.bitmaps),
            deltas: std::mem::take(&mut self.deltas),
            size_bytes: self.size_bytes,
        };
        self.size_bytes = 0;
        snap
    }

    /// Returns the approximate serialized size of all buffered bitmaps in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns `true` if the buffer contains no bitmap or delta entries.
    pub fn is_empty(&self) -> bool {
        self.bitmaps.is_empty() && self.deltas.is_empty()
    }

    /// Returns `true` if any additive bitmap data exists for `(event, period)`.
    pub fn has_data(&self, event: &str, period: &Period) -> bool {
        self.bitmaps.contains_key(&(event.to_string(), *period))
    }
}

impl Default for MemPart {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    #[test]
    fn or_into_empty() {
        let mut mp = MemPart::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);
        mp.or_bitmap("active", Period::Hour(2026, 3, 11, 14), &bm);
        let got = mp
            .get_bitmap("active", &Period::Hour(2026, 3, 11, 14))
            .unwrap();
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn or_accumulates() {
        let mut mp = MemPart::new();
        let mut bm1 = RoaringBitmap::new();
        bm1.insert(1);
        mp.or_bitmap("active", Period::Day(2026, 3, 11), &bm1);

        let mut bm2 = RoaringBitmap::new();
        bm2.insert(2);
        mp.or_bitmap("active", Period::Day(2026, 3, 11), &bm2);

        let got = mp.get_bitmap("active", &Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(1));
        assert!(got.contains(2));
    }

    #[test]
    fn delta_accumulates() {
        let mut mp = MemPart::new();
        let mut d1 = RoaringBitmap::new();
        d1.insert(42);
        mp.or_delta("active", Period::Day(2026, 3, 11), &d1);
        let got = mp.get_delta("active", &Period::Day(2026, 3, 11)).unwrap();
        assert!(got.contains(42));
    }

    #[test]
    fn snapshot_and_clear() {
        let mut mp = MemPart::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        mp.or_bitmap("active", Period::Static, &bm);
        assert!(mp.size_bytes() > 0);

        let snap = mp.take_snapshot();
        assert!(mp.is_empty());
        assert_eq!(mp.size_bytes(), 0);
        assert!(!snap.bitmaps.is_empty());
    }

    #[test]
    fn get_missing_returns_none() {
        let mp = MemPart::new();
        assert!(mp.get_bitmap("nope", &Period::Static).is_none());
        assert!(mp.get_delta("nope", &Period::Static).is_none());
    }

    #[test]
    fn has_data_reflects_bitmaps_only() {
        let mut mp = MemPart::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(7);
        // Before writing, has_data should be false.
        assert!(!mp.has_data("ev", &Period::Day(2026, 1, 1)));
        mp.or_bitmap("ev", Period::Day(2026, 1, 1), &bm);
        assert!(mp.has_data("ev", &Period::Day(2026, 1, 1)));
        // Delta-only key should NOT make has_data true.
        mp.or_delta("ev2", Period::Day(2026, 1, 1), &bm);
        assert!(!mp.has_data("ev2", &Period::Day(2026, 1, 1)));
    }

    #[test]
    fn size_bytes_tracks_growth() {
        let mut mp = MemPart::new();
        assert_eq!(mp.size_bytes(), 0);
        let mut bm = RoaringBitmap::new();
        bm.insert(100);
        mp.or_bitmap("ev", Period::Static, &bm);
        let after_first = mp.size_bytes();
        assert!(after_first > 0, "size must grow after first insert");
        // OR-ing the same bitmap in should not shrink the size.
        mp.or_bitmap("ev", Period::Static, &bm);
        assert!(
            mp.size_bytes() >= after_first,
            "idempotent OR must not shrink size"
        );
    }

    #[test]
    fn default_is_empty() {
        let mp = MemPart::default();
        assert!(mp.is_empty());
        assert_eq!(mp.size_bytes(), 0);
    }

    #[test]
    fn snapshot_size_matches_pre_snapshot() {
        let mut mp = MemPart::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(5);
        mp.or_bitmap("ev", Period::Static, &bm);
        let expected = mp.size_bytes();
        let snap = mp.take_snapshot();
        assert_eq!(snap.size_bytes, expected);
    }
}
