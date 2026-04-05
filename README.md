# inoxset

Embeddable set engine for time-bucketed ID sets with set algebra.

inoxset stores compressed sets of IDs (Roaring Bitmaps) across time periods and provides set operations (union, intersection, difference), dictionary encoding for string IDs, and reverse membership lookups via an opt-in inverted index.

## Quick start

```rust
use inoxset::*;
use inoxset::types::Period;
use roaring::RoaringBitmap;

let store = InoxSet::builder()
    .path("data/my_store")
    .open()?;

// Write user IDs into time-bucketed sets
let mut active = RoaringBitmap::new();
active.extend(0..10_000);
store.put_bitmap("active", Period::Day(2026, 3, 18), active)?;

let mut premium = RoaringBitmap::new();
premium.extend(5_000..8_000);
store.put_bitmap("premium", Period::Static, premium)?;

store.flush()?;

// Query: active today AND premium
let active = store.get("active", Period::Day(2026, 3, 18))?;
let premium = store.get("premium", Period::Static)?;
let target = &active & &premium;
println!("{} users match", target.len()); // 3000
```

## Features

### Time-aware storage

Data is bucketed by hour, day, month, or year. Opt-in rollup propagates writes to coarser granularities.

```rust
store.register_event("logins", Granularity::Hour, Rollup::Auto)?;
store.put_bitmap("logins", Period::Hour(2026, 3, 18, 14), users)?;

let today = store.get("logins", Period::Day(2026, 3, 18))?;
let march = store.get("logins", Period::Month(2026, 3))?;
```

### Dictionary encoding

Map arbitrary string IDs (UUID, nanoid, any string) to u32 values transparently. The mapping is global: the same ID always maps to the same u32 across all events, so cross-event set operations produce correct results.

```rust
store.put_ids("premium", Period::Day(2026, 3, 18), &[
    "usr_9f3a2b-...",
    "usr_c81d4e-...",
])?;

let users: Vec<String> = store.get_ids("premium", Period::Day(2026, 3, 18))?;
```

With the `uuid` feature flag, typed UUID methods are available (v4 and v7):

```rust
// Cargo.toml: inoxset = { version = "0.1", features = ["uuid"] }
store.put_uuids("segment", Period::Day(2026, 4, 1), &[uuid_v7])?;
let uuids = store.get_uuids("segment", Period::Day(2026, 4, 1))?;
```

### Set expression engine

Compose queries declaratively instead of loading bitmaps manually.

```rust
use inoxset::types::SetExpr;

let expr = SetExpr::and(
    SetExpr::Ref { event: "active".into(), period: Period::Day(2026, 4, 1) },
    SetExpr::diff(
        SetExpr::Ref { event: "premium".into(), period: Period::Static },
        SetExpr::Ref { event: "churned".into(), period: Period::Static },
    ),
);

let result = store.query(&expr)?;
let count = store.query_cardinality(&expr)?; // zero-alloc for leaf pairs
```

### Reverse membership lookups

"Which segments is user X in?" — opt-in inverted index with bloom filter pre-filtering.

```rust
let store = InoxSet::builder()
    .path("data/my_store")
    .index_freshness(IndexFreshness::OnFlush)
    .open()?;

let segments = store.find_memberships("user-abc")?;
```

### GDPR erasure

Remove an entity from all segments, dictionary, and inverted index in one call.

```rust
let removed = store.delete_entity("usr_9f3a2b-...")?;
```

### Async runtimes

inoxset uses synchronous I/O. In async contexts, wrap calls with `spawn_blocking`:

```rust
let store = Arc::new(store);
let s = store.clone();
let bm = tokio::task::spawn_blocking(move || {
    s.get("active", Period::Day(2026, 3, 18))
}).await??;
```

`InoxSet` is `Send + Sync`.

## Architecture

```
                    ┌─────────────────────────────┐
  put_bitmap() ───▶ │  MemPart (in-memory buffer) │
                    └──────────┬──────────────────┘
                               │ flush()
                    ┌──────────▼─────────────────────┐
                    │  Immutable Part Files (.roar)  │
                    │  one file per flush per period │
                    └──────────┬─────────────────────┘
                               │ compact()
                    ┌──────────▼──────────────────┐
                    │  Merged Part (single file)  │
                    │  deltas applied, optimized  │
                    └──────────┬──────────────────┘
                               │
                    ┌──────────▼────────────────────┐
                    │  LMDB Catalog (ACID metadata) │
                    │  part index, period state     │
                    └───────────────────────────────┘
```

- **Roaring Bitmaps** — compressed set storage (millions of IDs in kilobytes)
- **LMDB** (via heed) — ACID metadata catalog, mmap'd, readers never block
- **Memory-mapped reads** — zero-copy bitmap access from disk
- **Bitmap cache** — pre-deserialized bitmaps in RAM, rebuilt on flush
- **Inverted index** (opt-in) — bloom L1 + Roaring L2 + flat sorted array for reverse lookups

## API

### Core operations

| Method | Description |
|--------|-------------|
| `put_bitmap` | OR a bitmap into a time period (auto-registers events) |
| `get` | Read the merged bitmap for an event + period |
| `get_range` | Read multiple periods at once |
| `flush` | Persist in-memory buffer to disk |
| `compact` | Merge parts and apply deletes |

### Counting

| Method | Description |
|--------|-------------|
| `cardinality` | Count distinct IDs (O(1) for compacted periods) |
| `intersect_cardinality` | Count intersection without allocating intermediate bitmap |
| `union_cardinality` | Count union without allocating intermediate bitmap |
| `cardinality_range` | Time-series cardinality for a period range |
| `exists` | Check if any data exists for an event + period |

### Dictionary (string/UUID → u32)

| Method | Description |
|--------|-------------|
| `put_ids` / `get_ids` | Write/read external string IDs |
| `put_uuids` / `get_uuids` | Write/read UUIDs (requires `uuid` feature) |
| `remove_ids` / `remove_uuids` | Delete by external ID |

### Set expressions

| Method | Description |
|--------|-------------|
| `query` | Evaluate a `SetExpr` and return the resulting bitmap |
| `query_cardinality` | Evaluate and return only the count (zero-alloc for leaf pairs) |

### Reverse lookups

| Method | Description |
|--------|-------------|
| `find_memberships` | Which (event, period) pairs contain this entity? |
| `contains_id` | Is this entity in a specific event + period? |

### Lifecycle

| Method | Description |
|--------|-------------|
| `delete_entity` | GDPR erasure across all segments + dictionary |
| `delete_period` | Remove a single period's data |
| `retain_periods` | Bulk TTL: keep only periods matching a predicate |
| `list_events` / `list_periods` | Discover stored data |
| `health` | Operational snapshot |

## Installation

```toml
[dependencies]
inoxset = "0.1.0-alpha.3"
roaring = "0.10"

# Optional: UUID support
inoxset = { version = "0.1.0-alpha.3", features = ["uuid"] }
```

**MSRV:** Rust 1.75+

## Benchmarks

See [PERFORMANCE.md](PERFORMANCE.md) for methodology, flamegraph analysis, and competitive comparison.

Summary (Apple M-series, single thread, Criterion):

| Operation | Latency |
|-----------|---------|
| `put_bitmap` (1K bits) | 1.9 µs |
| `get` (compacted, bitmap cache) | 230-440 ns |
| `put_ids` (1K string IDs) | 115 µs |
| `intersect_cardinality` (10K ∩ 10K) | 634 ns |
| `find_memberships` (600 checks, inverted) | 26 µs |
| `find_memberships` (miss, bloom reject) | 30 ns |
| Intersection (10K ∩ 10K, materialized) | 800 ns |
| `flush` (10 events × 100 periods) | 329 ms |

Comparison with Redis and [bitmapist-server](https://github.com/Doist/bitmapist-server) (localhost):

| Operation | inoxset | Redis | bitmapist-server |
|-----------|---------|-------|------------------|
| Write 1K IDs | 1.9 µs | 1.19 ms | 2.60 ms |
| Read 10K bitmap | 230 ns | 86 µs | 18 µs |
| Intersection 10K ∩ 10K | 800 ns | 87 µs | 21 µs |
| Reverse lookup (600 checks) | 26 µs | 721 µs | 1.54 ms |

The difference is primarily due to inoxset being embedded (no network round-trip). Redis and bitmapist-server are network services with TCP overhead per operation.

Run locally: `cargo bench`

## Inspirations and references

inoxset builds on ideas from several systems. We are grateful to their authors and communities.

### Storage and indexing

- **[LMDB](https://www.symas.com/mdb)** (Howard Chu / Symas) — memory-mapped B+tree with copy-on-write MVCC. Used as the catalog backend via [heed](https://github.com/meilisearch/heed).
- **[Roaring Bitmaps](https://roaringbitmap.org/)** (Daniel Lemire et al.) — compressed bitmap format. The [roaring](https://crates.io/crates/roaring) Rust crate provides the core set operations.
- **[tantivy](https://github.com/quickwit-oss/tantivy)** — composite file format, segment merging, mmap'd reads. Inspired the arena storage design direction.
- **[redb](https://github.com/cberner/redb)** — pure Rust embedded database. Used in alpha.1 before the LMDB migration.

### Architecture patterns

- **[Apache Druid](https://druid.apache.org/)** — immutable segments with bitmap indexes, dictionary encoding, smoosh file format.
- **[VictoriaMetrics](https://victoriametrics.com/)** — inverted index for label→series mapping, write-time index construction.
- **[ClickHouse](https://clickhouse.com/)** — MergeTree mark files, zone maps, granule-based skipping indexes.
- **[DuckDB](https://duckdb.org/)** — single-file columnar storage, zone maps, buffer pool vs mmap tradeoffs.

### Bitmap servers

- **[bitmapist-server](https://github.com/Doist/bitmapist-server)** (Doist) — Go roaring bitmap server. Used as a benchmark comparison target.
- **[Redis](https://redis.io/)** — native bitmap operations (SETBIT, GETBIT, BITOP). Used as a benchmark baseline.

### Concurrency

- **[LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)** — ring buffer mechanical sympathy. Informed the ArcSwap read index design.
- **[arc-swap](https://crates.io/crates/arc-swap)** — lock-free atomic swapping of Arc pointers.

### Research

- **[Chronicle Map](https://github.com/OpenHFT/Chronicle-Map)** — off-heap mmap'd concurrent map, sub-microsecond reads.
- **[MapDB](https://github.com/jankotek/mapdb)** — single-file mmap'd collections with page-based allocation.
- **[FeOxDB](https://github.com/mehrantsi/feoxdb)** — lock-free concurrent access, sub-microsecond reads from in-memory indexes.

## Further reading

### Probabilistic data structures

- **Bloom filters** — space-efficient probabilistic set membership test. Used as L1 pre-filter in the inverted index. See: [Bloom (1970)](https://dl.acm.org/doi/10.1145/362686.362692), [Kirsch & Mitzenmacher (2006)](https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf) for the double-hashing optimization used here.
- **HyperLogLog** — cardinality estimation with fixed memory. See: [Flajolet, Fusy, Gandouet & Meurisse (2007)](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). Not used in inoxset (exact counting via Roaring), but relevant for approximate analytics at extreme scale.
- **Theta Sketch** — set operations on cardinality sketches (union, intersection, difference). See: [Datasketches.apache.org](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html). Complementary to inoxset for approximate counting when exact bitmaps are too large.

### Compressed bitmap formats

- **Roaring Bitmaps** — hybrid container format (array, bitmap, run-length). See: [Chambi, Lemire et al. (2016)](https://arxiv.org/abs/1603.06549). The core data structure in inoxset.
- **Concise** — word-aligned hybrid compression. Predecessor to Roaring, used by early Apache Druid. See: [Colantonio & Di Pietro (2010)](https://arxiv.org/abs/1004.0403).
- **CRoaring** — C implementation of Roaring, portable serialization format. The Rust `roaring` crate uses this format, ensuring compatibility with Java, Go, C++, Python, and ClickHouse.

### Embedded storage engines

- **LMDB** — memory-mapped B+tree with copy-on-write MVCC. Single-writer, lock-free readers. See: [Chu (2011)](https://www.openldap.org/pub/hyc/mdm-paper.pdf). Used by inoxset for catalog storage.
- **LSM-tree** — log-structured merge-tree. Write-optimized, used by RocksDB, LevelDB, Fjall. See: [O'Neil et al. (1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf). Not used in inoxset (B+tree better for read-heavy workloads).
- **B-tree vs LSM trade-offs** — see: [Athanassoulis et al. (2019)](https://scholar.harvard.edu/files/stratos/files/dostoevskykv.pdf) for the RUM conjecture (Read, Update, Memory — pick two).

### Inverted indexes

- **Posting lists** — core structure for "term → document list" lookups. Used in inoxset's inverted index as "entity_hash → membership list". See: [Manning, Raghavan & Schütze (2008)](https://nlp.stanford.edu/IR-book/) Chapter 2.
- **VictoriaMetrics inverted index** — label→metricID posting lists for time-series. Inspired inoxset's write-time index construction. See: [VictoriaMetrics storage architecture](https://docs.victoriametrics.com/#storage).

## Status

Alpha. 211 tests (unit, integration, property-based). API is functional but may change before 1.0. Not yet battle-tested in production.

## License

MIT License ([LICENSE-MIT](LICENSE-MIT))
