//! Frozen inverted index mapping external IDs to (event, period) memberships.
//!
//! The index uses a two-level pre-filter before a binary search:
//! 1. Bloom filter (L1) — probabilistic, eliminates most non-members cheaply.
//! 2. [`RoaringBitmap`] over FxHash32 values (L2) — eliminates Bloom false positives.
//! 3. Binary search on FxHash64 over sorted [`IndexEntry`] list (exact lookup).

use roaring::RoaringBitmap;
use rustc_hash::FxHasher;
use std::hash::Hasher;

use crate::bloom::BloomFilter;
use crate::types::{IndexEntry, Membership, Period};

// ---------------------------------------------------------------------------
// Hash helpers
// ---------------------------------------------------------------------------

/// Compute the 64-bit FxHash of a string's bytes.
pub(crate) fn fx_hash64(s: &str) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(s.as_bytes());
    hasher.finish()
}

/// Compute the 32-bit FxHash of a string (lower 32 bits of [`fx_hash64`]).
pub(crate) fn fx_hash32(s: &str) -> u32 {
    fx_hash64(s) as u32
}

// ---------------------------------------------------------------------------
// Decode tables
// ---------------------------------------------------------------------------

/// Decode tables shared by all lookups on a frozen index.
pub(crate) struct InvertedMeta {
    /// Ordered list of event names; index equals the `event_id` stored in memberships.
    pub event_names: Vec<String>,
    /// Ordered list of time periods; index equals the `period_id` stored in memberships.
    pub periods: Vec<Period>,
}

// ---------------------------------------------------------------------------
// Frozen index
// ---------------------------------------------------------------------------

/// Frozen inverted index — keyed by FxHash64 of the external ID string.
///
/// Built via [`InvertedIndexBuilder`] and immutable after construction.
/// Lookups use a two-level pre-filter (Bloom + Roaring) before a binary
/// search over sorted [`IndexEntry`] rows.
pub(crate) struct InvertedIndex {
    /// Sorted by `id_hash`; used for binary search.
    entries: Vec<IndexEntry>,
    /// Flat-packed [`Membership`] array; sliced by `entry.offset .. entry.offset + entry.count`.
    memberships: Vec<Membership>,
    /// L1 pre-filter: Bloom filter over raw ID bytes.
    bloom: BloomFilter,
    /// L2 pre-filter: RoaringBitmap of FxHash32 values.
    known_hashes: RoaringBitmap,
    /// Decode tables for event names and periods.
    meta: InvertedMeta,
}

impl InvertedIndex {
    /// Returns `false` when `external_id` is **definitely absent** (Bloom says no).
    ///
    /// A `true` result means "probably present"; confirm with [`definitely_contains`](Self::definitely_contains).
    pub(crate) fn maybe_contains(&self, external_id: &str) -> bool {
        self.bloom.test(external_id.as_bytes())
    }

    /// Returns `true` when the FxHash32 of `external_id` is present in the Roaring bitmap.
    ///
    /// Eliminates Bloom false positives; still not a true membership check (hash collisions
    /// remain possible, though extremely unlikely in practice).
    pub(crate) fn definitely_contains(&self, external_id: &str) -> bool {
        self.known_hashes.contains(fx_hash32(external_id))
    }

    /// Returns all `(event_name, period)` pairs for `external_id`.
    ///
    /// Returns an empty `Vec` when the ID is not in the index.
    pub(crate) fn lookup(&self, external_id: &str) -> Vec<(String, Period)> {
        if !self.maybe_contains(external_id) {
            return Vec::new();
        }
        let h64 = fx_hash64(external_id);
        match self.entries.binary_search_by_key(&h64, |e| e.id_hash) {
            Err(_) => Vec::new(),
            Ok(idx) => {
                let entry = &self.entries[idx];
                let start = entry.offset as usize;
                let end = start + entry.count as usize;
                self.memberships[start..end]
                    .iter()
                    .filter_map(|m| {
                        let event_name = self.meta.event_names.get(m.event_id() as usize)?;
                        let period = self.meta.periods.get(m.period_id() as usize)?;
                        Some((event_name.clone(), *period))
                    })
                    .collect()
            }
        }
    }

    /// Returns `true` when `external_id` has an entry for the given `event` and `period`.
    pub(crate) fn contains(&self, external_id: &str, event: &str, period: &Period) -> bool {
        self.lookup(external_id)
            .iter()
            .any(|(e, p)| e == event && p == period)
    }

    /// Returns a reference to the decode tables.
    pub fn meta(&self) -> &InvertedMeta {
        &self.meta
    }

    /// Constructs an empty index (for use in [`Disabled`](crate::types::IndexFreshness::Disabled) mode).
    pub(crate) fn empty() -> Self {
        Self {
            entries: Vec::new(),
            memberships: Vec::new(),
            bloom: BloomFilter::with_capacity(1, 0.01),
            known_hashes: RoaringBitmap::new(),
            meta: InvertedMeta {
                event_names: Vec::new(),
                periods: Vec::new(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for a frozen [`InvertedIndex`].
///
/// Accumulate entries with [`add`](Self::add), then call [`build`](Self::build)
/// to produce the immutable index.
pub(crate) struct InvertedIndexBuilder {
    meta: InvertedMeta,
    /// Raw staging rows: `(h64, h32, ext_bytes, event_id, period_id)`.
    raw: Vec<(u64, u32, Vec<u8>, u16, u16)>,
}

impl InvertedIndexBuilder {
    /// Creates a new builder with the given event name and period decode tables.
    pub(crate) fn new(event_names: Vec<String>, periods: Vec<Period>) -> Self {
        Self {
            meta: InvertedMeta {
                event_names,
                periods,
            },
            raw: Vec::new(),
        }
    }

    /// Hashes `external_id` and stages an `(event_id, period_id)` row.
    pub(crate) fn add(&mut self, external_id: &str, event_id: u16, period_id: u16) {
        let h64 = fx_hash64(external_id);
        let h32 = fx_hash32(external_id);
        self.raw.push((
            h64,
            h32,
            external_id.as_bytes().to_vec(),
            event_id,
            period_id,
        ));
    }

    /// Consumes the builder and produces a frozen [`InvertedIndex`].
    ///
    /// Rows are sorted by `h64`; entries with the same `h64` are grouped into a
    /// single [`IndexEntry`] that points to a contiguous slice of the flat
    /// memberships array.
    pub(crate) fn build(mut self) -> InvertedIndex {
        // Sort by h64 so we can group by external ID and do binary search later.
        self.raw.sort_unstable_by_key(|r| r.0);

        let capacity = {
            // Count distinct h64 values for Bloom sizing.
            let mut prev = None::<u64>;
            self.raw
                .iter()
                .filter(|r| {
                    let is_new = prev != Some(r.0);
                    prev = Some(r.0);
                    is_new
                })
                .count()
        };

        let bloom_capacity = capacity.max(1);
        let mut bloom = BloomFilter::with_capacity(bloom_capacity, 0.01);
        let mut known_hashes = RoaringBitmap::new();
        let mut entries: Vec<IndexEntry> = Vec::with_capacity(capacity);
        let mut memberships: Vec<Membership> = Vec::with_capacity(self.raw.len());

        let mut i = 0;
        while i < self.raw.len() {
            let h64 = self.raw[i].0;
            let h32 = self.raw[i].1;
            let ext_bytes = &self.raw[i].2;

            // Insert into pre-filters using the raw bytes of the first row with this h64.
            bloom.insert(ext_bytes);
            known_hashes.insert(h32);

            let offset = memberships.len() as u32;
            let mut count = 0u32;

            // Group all rows sharing this h64.
            while i < self.raw.len() && self.raw[i].0 == h64 {
                let (_, _, _, event_id, period_id) = self.raw[i];
                memberships.push(Membership::inline(event_id, period_id));
                count += 1;
                i += 1;
            }

            entries.push(IndexEntry {
                id_hash: h64,
                offset,
                count,
            });
        }

        InvertedIndex {
            entries,
            memberships,
            bloom,
            known_hashes,
            meta: self.meta,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Period;

    fn make_test_index() -> InvertedIndex {
        let event_names = vec!["clicks".to_string(), "views".to_string()];
        let periods = vec![
            Period::Day(2026, 4, 1),
            Period::Day(2026, 4, 2),
            Period::Day(2026, 4, 3),
        ];
        let mut builder = InvertedIndexBuilder::new(event_names, periods);
        builder.add("alice", 0, 0);
        builder.add("alice", 0, 1);
        builder.add("alice", 1, 0);
        builder.add("bob", 0, 2);
        builder.add("charlie", 1, 1);
        builder.add("charlie", 1, 2);
        builder.build()
    }

    #[test]
    fn lookup_existing_entity() {
        let idx = make_test_index();
        let result = idx.lookup("alice");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn lookup_missing_entity() {
        let idx = make_test_index();
        assert!(idx.lookup("unknown").is_empty());
    }

    #[test]
    fn bloom_rejects_missing() {
        let idx = make_test_index();
        assert!(!idx.maybe_contains("unknown"));
        assert!(idx.maybe_contains("alice"));
    }

    #[test]
    fn roaring_rejects_bloom_fp() {
        let idx = make_test_index();
        assert!(idx.definitely_contains("alice"));
        assert!(!idx.definitely_contains("unknown"));
    }

    #[test]
    fn contains_entity_in_event_period() {
        let idx = make_test_index();
        assert!(idx.contains("alice", "clicks", &Period::Day(2026, 4, 1)));
        assert!(!idx.contains("alice", "clicks", &Period::Day(2026, 4, 3)));
        assert!(!idx.contains("unknown", "clicks", &Period::Day(2026, 4, 1)));
    }
}
