# Performance Analysis

Detailed benchmark results, flamegraph analysis, and competitive comparison for inoxset.

**Hardware:** Apple M-series, single thread. All benchmarks use [Criterion](https://github.com/bheisler/criterion.rs).

---

## Summary

| Operation | Latency | Notes |
|-----------|---------|-------|
| `put_bitmap` (1K bits) | 1.9 µs | In-memory accumulation |
| `get` (compacted, 10K bits) | **228 ns** | Served from bitmap cache |
| `get` (compacted, 100K bits) | **420 ns** | Served from bitmap cache |
| `put_ids` (1K string IDs) | **115 µs** | Global dictionary (LMDB) + bitmap |
| `get_ids` (1K string IDs) | **147 µs** | Bitmap + reverse dict |
| `find_memberships` hit (600 checks) | **29 µs** | Inverted index, bloom+roaring+flat array |
| `find_memberships` miss | **30 ns** | Bloom filter L1 rejects |
| Intersection (10K ∩ 10K) | **784 ns** | Bitmap cache + Roaring AND |
| `contains_id` | ~50 ns | Inverted index path |
| `flush` (10ev × 100 periods) | 342 ms | Disk I/O bound |
| `compact` (50p × 5 parts) | 190 ms | Merge + rewrite |

---

## Disk Usage

inoxset uses a global dictionary (one u32 per external_id across all events) and Roaring Bitmap compression.

| Scenario | Total disk | Catalog (LMDB) | Parts (.roar) | Amplification |
|----------|-----------|----------------|---------------|--------------|
| 5 events × 7 days × 1K users | 301 KB | 232 KB | 69 KB | 153x |
| 20 events × 30 days × 10K users | 5.9 MB | 1.2 MB | 4.7 MB | 51x |
| 50 events × 30 days × 100K users | 31.5 MB | 8.0 MB | 23.5 MB | 27.5x |

Roaring compression: 1.3 bits/user at 100K scale (sequential IDs). The catalog stores dictionary mappings and part metadata in LMDB (B+tree, mmap, copy-on-write).

Profile disk usage: `cargo run --example profile_amplification --release`

---

## Reverse Membership Lookups

The most optimized path in inoxset. Use case: real-time ad bidding — "which segments is user X in?"

### Architecture

```
find_memberships("user-X")

  ┌──────────────────────────────┐
  │ 1. Bloom Filter (L1)         │  ~3 ns
  │    Fast probabilistic reject │──→ false? return [] (80%+ of traffic)
  └──────────┬───────────────────┘
             │ maybe exists
  ┌──────────▼───────────────────┐
  │ 2. Roaring Pre-Filter (L2)   │  ~12 ns
  │    Exact membership (h32)    │──→ false? return [] (catches bloom FPs)
  └──────────┬───────────────────┘
             │ definitely exists
  ┌──────────▼───────────────────┐
  │ 3. Flat Sorted Array (L3)    │  ~30 ns
  │    Binary search on FxHash64 │
  │    4-byte packed memberships │
  └──────────┬───────────────────┘
             │ decode
  ┌──────────▼───────────────────┐
  │ 4. Decode Memberships        │  ~50 ns
  │    event_id → name           │
  │    period_id → Period        │
  └──────────────────────────────┘
```

### Results

| Scenario | Disabled (default) | Inverted Index (OnFlush) | Improvement |
|----------|-------------------|--------------------------|-------------|
| Hit (600 segment×period checks) | 7.25 ms | **29 µs** | **250x** |
| Miss (unknown entity) | 17 µs | **30 ns** | **567x** |
| Per-membership check | 12 µs | **48 ns** | **250x** |

### Configuration

```rust
// Opt-in: enable inverted index
let store = InoxSet::builder()
    .path("data/store")
    .index_freshness(IndexFreshness::OnFlush)
    .open()?;
```

| Mode | Latency | Freshness | RAM overhead |
|------|---------|-----------|-------------|
| `Disabled` (default) | ~7 ms | N/A | 0 |
| `OnFlush` | **~29 µs** | At flush() | ~620 MB - 2.2 GB per shard |
| `OnCompact` | **~29 µs** | At compact() | ~620 MB - 2.2 GB per shard |

RAM depends on dataset: ~600 MB for 10M entities × 10 memberships, ~2.2 GB for 10M × 50.

---

## Competitive Comparison

Compared on the same machine, localhost. Redis 7.x, [bitmapist-server](https://github.com/Doist/bitmapist-server) v1.9.

### Write (1K IDs)

| | inoxset | Redis (pipeline) | bitmapist-server | Factor |
|---|---|---|---|---|
| Latency | **1.9 µs** | 1.19 ms | 2.60 ms | **630x vs Redis** |

### Read (10K bitmap)

| | inoxset | Redis (BITCOUNT) | bitmapist-server | Factor |
|---|---|---|---|---|
| Latency | **14 µs** | 86 µs | 18 µs | **6x vs Redis** |

### Intersection (10K ∩ 10K)

| | inoxset | Redis (BITOP AND) | bitmapist-server | Factor |
|---|---|---|---|---|
| Latency | **802 ns** | 87 µs | 21 µs | **108x vs Redis, 26x vs bitmapist** |

Bitmap cache eliminates deserialization: bitmaps are pre-loaded into RAM at flush time and served via `Arc<RoaringBitmap>`. The intersection itself (Roaring AND) takes ~300ns; the rest is two HashMap lookups + Arc clone.

### Reverse Membership (600 checks)

| | inoxset (inverted) | Redis (GETBIT pipeline) | bitmapist-server | Factor |
|---|---|---|---|---|
| Latency | **29 µs** | 721 µs | 1.54 ms | **25x vs Redis, 53x vs bitmapist** |

### Why inoxset is faster

- **Embedded**: no network round-trip (0 vs ~50-100 µs per request)
- **Memory-mapped reads**: OS page cache, zero-copy bitmap access
- **Roaring Bitmaps**: compressed set operations (AND/OR/XOR in microseconds)
- **Inverted index**: pre-computed reverse lookup with bloom + flat array (no disk I/O)

---

## Flamegraph Analysis

### Baseline (without inverted index)

**Profile**: `find_memberships` scanning 600 (event, period) combos.

Top bottlenecks:
1. **redb B-tree traversal** (~35%) — `Btree::get_helper`, `BranchAccessor::child_for_key`
2. **redb infrastructure** (~25%) — `PagedCachedFile::read`, `Mutex::lock`, `from_utf8`
3. **String allocations** (~15%) — `format!()` for catalog keys
4. **Memory operations** (~15%) — `memmove`, `memcmp`, `realloc`
5. **File I/O** (~5%) — `File::open_c`
6. **Bitmap operations** (~0%) — `serialized_contains` invisible

Key insight: `exists()` ≈ `get()` ≈ `contains_id()` in latency, proving that bitmap read/deserialization cost is negligible — the bottleneck was 100% catalog lookup overhead.

### Post inverted index

The inverted index hot path (bloom + binary search) is **invisible in the flamegraph** — too fast relative to setup/rebuild costs. redb operations appear only in the rebuild path (at flush time), not on the query path.

### Intersection analysis

Profiling the intersection path (get+get+AND) showed:
- Pure Roaring AND: 295ns (~1% of total)
- mmap + deserialize × 2: ~27µs (~93%)
- Memory allocations: ~1µs (~6%)

Fix: bitmap cache in ReadIndex pre-deserializes all bitmaps at flush time. `get()` serves from `Arc<RoaringBitmap>` cache — zero I/O, zero deserialization. Result: 27µs → 784ns (38x improvement).

---

## How to Reproduce

### Run benchmarks

```bash
# All benchmarks
cargo bench

# Specific groups
cargo bench --bench benchmarks -- "find_memberships"
cargo bench --bench benchmarks -- "find_memberships_inverted"

# Competitive comparison (requires Redis + bitmapist-server)
redis-server --daemonize yes
bitmapist-server -addr localhost:6380 -db /tmp/bitmapist.db &
cargo bench --bench comparison
```

### Run instrumented profiling

```bash
cargo run --example profile_find --release
cargo run --example profile_intersection --release
```

### Generate flamegraph (macOS, requires sudo for dtrace)

```bash
sudo cargo flamegraph --example profile_find -o flamegraph.svg
open flamegraph.svg  # view in browser
```

### Benchmark scenario

The standard benchmark scenario uses:
- 20 segments × 30 days = 600 (event, period) combos
- 10,000 unique entity IDs per combo
- Compacted (single part file per period)
- Both hit (entity in all 600 combos) and miss (unknown entity)

---

## Optimization History

| Phase | Change | find_memberships (600) | Intersection 10K∩10K | Commit |
|-------|--------|----------------------|---------------------|--------|
| Baseline | Bitmap scan, per-file reads | 7.25 ms | 30 µs | — |
| Phase 0 | ArcSwap ReadIndex (cache catalog) | 7.0 ms | 30 µs | `9e22d08` |
| Inverted Index | Bloom L1 + Roaring L2 + flat array | **29 µs** | 30 µs | `0a0fade` |
| Bitmap Cache | Pre-deserialized bitmaps in ReadIndex | **29 µs** | **784 ns** | `5377766` |
| Global Dict | One u32 per entity, 26x less disk, bitmap cache reads at 242ns | **27 µs** | **794 ns** | `40c523c` |
| **LMDB (heed)** | Replace redb with LMDB. 1.3x less disk, 2x faster dict writes | **25.6 µs** | **802 ns** | `8c06587` |

### What didn't work

- **`serialized_contains` on mmap'd bytes** — saved 0% because the bottleneck was redb lookups, not bitmap deserialization
- **File cache in `find_memberships`** — saved ~24% by caching `fs::read()` results, but still 600 file opens
- **Phase 0 ReadIndex alone** — saved only 26% because file I/O replaced redb as the bottleneck

### What worked

- **Inverted index** — eliminated the entire scan loop. One HashMap-style lookup replaces 600 file reads + 1200 redb queries.
- **Bloom filter L1** — rejects 80%+ of traffic in 3ns without touching any data structure
- **Flat sorted array** — binary search on u64 hashes with 4-byte packed memberships, single contiguous allocation
