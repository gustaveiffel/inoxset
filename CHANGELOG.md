# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **`union_cardinality_many` / `intersect_cardinality_many`** — N-way cardinality over `(event, period)` keys with a single accumulator instead of N−1 pairwise intermediates; intersection short-circuits on empty.

### Fixed

- **Inverted index is populated at store open.** Previously a reopened store served an empty index until the first flush, so `find_memberships`/`contains_id` silently returned no results for entities that were present. Open latency grows with store size when the index is enabled; see `index_freshness` docs.
- **`contains_id` honors `delete_entity` immediately.** The staleness check resolved through the dictionary entry that `delete_entity` had just removed, so the frozen-index path kept answering `true` until flush+compact. Read-your-deletes now holds on every read path.

- **Re-insert after remove no longer loses data.** Delta (tombstone) parts are now applied in part-id order — a delta only erases bits from data parts flushed *before* it, and `put_bitmap` cancels pending mempart deltas for the bits it writes (including rollup ancestors). Previously, `put → remove → put` silently dropped the re-inserted bits at read, flush, and compaction time.
- **Inverted index Bloom filter false negatives on FxHash64 collision.** All external IDs in an h64 collision group are now inserted into the Bloom pre-filter; previously only the first one was, making the other IDs invisible to `find_memberships`/`contains_id`.
- **Corrupt part files now fail loudly.** `ReadIndex` construction (store open, post-flush/compaction rebuild) propagates part read errors instead of silently serving incomplete bitmaps.
- **`find_memberships` (index-disabled mode) now honors unflushed removes**, matching `get()` and the frozen-index path.
- **Period component validation.** `put_bitmap`, `remove_bits`, `replace_bitmap`, `bulk_replace`, and `get_range` reject out-of-range periods (e.g. `Day(2026, 13, 40)`, Feb 29 in a non-leap year) with the new `InoxSetError::InvalidPeriod`. Previously such periods wrote catalog keys that were silently skipped on reload.
- **`get_range` with reversed or invalid bounds no longer loops unboundedly** (release builds could hang and exhaust memory; debug builds panicked on year overflow).
- **`is_closed` for pre-1970 periods** now reports closed instead of wrapping to a far-future boundary.
- **Part file durability**: `write_part` now fsyncs the parent directory after the atomic rename, so a crash cannot leave the catalog referencing a vanished file.

### Changed

- `merge::merge_parts` takes `(part_id, path)` pairs instead of bare paths so deltas can be ordered against data parts.
- `contains_bit`/`find_memberships` answer flushed-state queries from the pre-merged bitmap cache instead of per-part `mmap_contains`/`serialized_contains` scans (same results, fewer syscalls, temporally correct).
- 12 new regression tests: re-insert after remove (same buffer, across flushes, after compaction, after reopen, with rollup), Bloom collision groups, range bounds, period validation, buffer size accounting.

## [0.1.0-alpha.4] — 2026-04-05

### Added

- **Export API** — `serialize_portable()` (CRoaring bytes), `export_u32_vec()`, `query_export()`, `query_serialize()`, `export_ids()`, `export_uuids()`.
- **Standalone Dictionary module** — `Dictionary` struct importable without full store. Methods: `get_or_assign`, `get_or_assign_batch`, `lookup`, `lookup_batch`, `contains`, `resolve`, `resolve_batch`, `delete`, `len`, `is_empty`. UUID batch support.
- ClickHouse integration validation: CRoaring compatibility test, segment sync, scale test.
- Production scale validation: 8M profiles, 50 segments, 90K QPS on 8 cores.
- QPS stress test with single-thread and multi-thread benchmarks.

### Fixed

- Dictionary doc: clarified store must be closed before opening standalone Dictionary on same path.
- `serialize_portable` doc: noted run containers not emitted by Rust serializer.
- `export_ids` doc: clarified lexicographic sort order.
- `delete` doc: noted u32 slot non-recycling.
- Empty bitmap edge case test for export API.

## [0.1.0-alpha.3] — 2026-04-05

### Added

- **SetExpr query engine** — composable set algebra with `And`, `Or`, `Diff` operators and short-circuit evaluation on empty operands.
- **`query()` and `query_cardinality()`** — evaluate set expressions declaratively. `query_cardinality` avoids bitmap allocation for leaf-pair And/Or.
- **`intersect_cardinality()` and `union_cardinality()`** — count intersection/union size without allocating an intermediate bitmap.
- **`cardinality_range()`** — time-series cardinality for a range of periods.
- **UUID support** via `uuid` feature flag (v4 + v7). New methods: `put_uuids`, `get_uuids`, `remove_uuids`, `delete_entity_uuid`, `find_memberships_uuid`, `contains_uuid`.

### Fixed

- `cardinality_range` now correctly includes unflushed mempart data.
- UUID canonical form documented (lowercase hyphenated).
- Added `# Errors` doc sections to all UUID API methods.

## [0.1.0-alpha.2] — 2026-04-05

### Changed

- **BREAKING:** Storage backend migrated from redb to LMDB (via heed). Existing stores must be recreated.
- Catalog stored in `catalog.mdb/` directory (was `catalog.redb` file).
- Platform-aware default LMDB map size: 64 MiB on macOS, 256 MiB on Linux.

### Added

- **`map_size` builder option** — configure LMDB memory-mapped region size.
- **`delete_entity()`** — GDPR erasure: remove entity from all segments + dictionary.
- **Inverted index** — opt-in via `IndexFreshness::OnFlush`. Bloom filter L1 + Roaring L2 + flat sorted array for sub-microsecond reverse membership lookups.
- **Bitmap cache** — pre-deserialized bitmaps in ReadIndex, served via `Arc<RoaringBitmap>`.
- **`find_memberships()` and `contains_id()`** — reverse lookup API.
- **`serialized_contains()`** — zero-copy membership check on serialized Roaring bytes.
- **Global dictionary** — one u32 per external_id across all events. Fixes cross-event bitmap intersection correctness.
- **Competitive benchmarks** vs Redis and bitmapist-server.

### Fixed

- Cross-event bitmap intersection now produces correct results (global dict).
- `contains_id` checks mempart deltas after frozen inverted index hit.
- `find_memberships` scans all mempart keys (not just indexed events).
- Inverted index rebuild subtracts disk delta parts.
- Dictionary u32 counter overflow returns error instead of wrapping.
- Batch reverse lookup uses single LMDB read transaction.

## [0.1.0-alpha.1] — 2026-03-25

### Added

- Initial release.
- Core engine: `put_bitmap`, `get`, `get_range`, `flush`, `compact`.
- Time-aware storage with hour/day/month/year granularity and opt-in rollup.
- Dictionary encoding: `put_ids`, `get_ids`, `remove_ids`.
- Surgical deletes via delta tombstones (`remove_bits`).
- `replace_bitmap` and `bulk_replace` for atomic period replacement.
- `list_events`, `list_periods`, `delete_period`, `retain_periods`.
- `cardinality`, `exists`, `health`.
- Property-based tests (proptest).
- Criterion benchmarks.
- LMDB (heed) catalog with ACID metadata.
- Roaring Bitmap compression.
- Memory-mapped part file reads.
- Atomic write-to-temp-then-rename for crash safety.
