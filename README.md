# inoxset

Embeddable set engine — power segmentation, analytics, and AI recall in real time.

inoxset stores compressed sets of IDs across time periods and lets you query them with set algebra (union, intersection, difference). Think "which users did X during period Y?" answered in microseconds.

## Use cases

- **Audience segmentation** — "active users last 7 days who are also premium"
- **Analytics pipelines** — pre-materialized cohorts for dashboards and reports
- **AI memory** — fast recall of entity sets for retrieval-augmented generation
- **GDPR compliance** — surgical bit-level deletes without rewriting history

## Quick start

```rust
use inoxset::*;
use roaring::RoaringBitmap;

// Open a store (creates the directory if needed)
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

// Flush to disk
store.flush()?;

// Query: active today AND premium
let active = store.get("active", Period::Day(2026, 3, 18))?;
let premium = store.get("premium", Period::Static)?;
let target = &active & &premium; // 5000..8000
println!("{} users match", target.len()); // 3000
```

## Features

**Time-aware storage** — data is bucketed by hour, day, month, or year. Query any granularity.

```rust
// Hourly writes roll up automatically to day/month/year
store.register_event("logins", Granularity::Hour, Rollup::Auto)?;
store.put_bitmap("logins", Period::Hour(2026, 3, 18, 14), users)?;

// Query at any level — rollup is pre-computed
let today = store.get("logins", Period::Day(2026, 3, 18))?;
let march = store.get("logins", Period::Month(2026, 3))?;
```

**Surgical deletes** — remove individual IDs without rewriting data. Deletes propagate through rollup levels.

```rust
// GDPR: remove user 42 from all hourly data for this day
store.remove_bits("logins", Period::Hour(2026, 3, 18, 14), &[42])?;

// Delta is applied at read time, materialized on compaction
store.compact()?;
```

**Compaction** — merge accumulated parts and apply deletes for optimal read performance.

```rust
let stats = store.compact()?;
println!(
    "compacted {} periods, freed {} bytes",
    stats.periods_compacted, stats.bytes_reclaimed
);
```

**Dictionary encoding** — store arbitrary string IDs (UUID, nanoid) in u32 Roaring Bitmaps. The mapping is automatic and persistent.

```rust
// Write with string IDs — dictionary assigns u32 internally
store.put_ids("premium", Period::Day(2026, 3, 18), &[
    "usr_9f3a2b-...",
    "usr_c81d4e-...",
])?;

// Read back the original string IDs
let users: Vec<String> = store.get_ids("premium", Period::Day(2026, 3, 18))?;

// GDPR delete by string ID
store.remove_ids("premium", Period::Day(2026, 3, 18), &["usr_9f3a2b-..."])?;
```

**Embeddable** — pure Rust, sync API, no runtime dependencies. Drop it into any application.

```rust
// In an async context, wrap with spawn_blocking
let result = tokio::task::spawn_blocking(move || {
    store.get("active", Period::Day(2026, 3, 18))
}).await??;
```

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
                    │  redb Catalog (ACID metadata) │
                    │  part index, period state     │
                    └───────────────────────────────┘
```

- **Roaring Bitmaps** for compressed set storage (millions of IDs in kilobytes)
- **redb** for ACID metadata catalog (crash-safe, zero-config)
- **Memory-mapped reads** for zero-copy bitmap access from disk
- **Append-only parts** — writes never mutate existing files

## API

| Method | Description |
|--------|-------------|
| `put_bitmap` | OR a bitmap into a time period (auto-registers events) |
| `get` | Read the merged bitmap for an event + period |
| `get_range` | Read multiple periods at once |
| `cardinality` | Count distinct IDs (O(1) for compacted periods) |
| `exists` | Check if any data exists for an event + period |
| `put_ids` | Write external string IDs (auto-mapped to u32 via dictionary) |
| `get_ids` | Read back external string IDs for an event + period |
| `remove_ids` | Delete external IDs via dictionary-resolved tombstones |
| `remove_bits` | Delete specific u32 IDs via delta tombstones |
| `replace_bitmap` | Atomically replace all data for a period |
| `bulk_replace` | Replace multiple periods in a single transaction |
| `flush` | Persist in-memory buffer to disk |
| `compact` | Merge parts and apply deletes |
| `list_periods` | Discover which periods contain data for an event |
| `health` | Operational health snapshot |

## Using with async runtimes

inoxset uses synchronous I/O (redb transactions, mmap page faults). In a tokio or async-std context, wrap calls with `spawn_blocking`:

```rust
let store = Arc::new(store);

// Read path
let s = store.clone();
let bm = tokio::task::spawn_blocking(move || {
    s.get("active", Period::Day(2026, 3, 18))
}).await??;

// Write path
let s = store.clone();
tokio::task::spawn_blocking(move || {
    s.put_bitmap("active", Period::Day(2026, 3, 18), bitmap)?;
    s.flush()
}).await??;
```

`InoxSet` is `Send + Sync` — safe to share via `Arc` across tasks. Reads acquire a shared lock and do not block other reads.

## Benchmarks

Measured with [Criterion](https://github.com/bheisler/criterion.rs) on Apple M-series, single thread. Median values.

| Operation | Scenario | Median |
|-----------|----------|--------|
| `put_bitmap` | 1K bits | **4.1 µs** |
| `put_bitmap` | 1K bits + rollup auto | **10.7 µs** |
| `get` | 1 part (mmap) | **29.6 µs** |
| `get` | 5 parts merged | **119.9 µs** |
| `get` | 20 parts merged | **468 µs** |
| `get` | compacted, 100K bits | **28.0 µs** |
| `flush` | 10 events x 100 periods | **321 ms** |
| `compact` | 50 periods x 5 parts | **231 ms** |

Baseline redb overhead: write txn commit ~2.6 ms, read txn open ~659 ns.

Run your own: `cargo bench`

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
inoxset = "0.1.0-alpha.1"
roaring = "0.10"
```

**MSRV:** Rust 1.75+

## Status

Alpha. The API is functional and tested (145 tests including property-based tests), but not yet battle-tested in production. Expect breaking changes before 1.0.

## License

MIT License ([LICENSE-MIT](LICENSE-MIT))