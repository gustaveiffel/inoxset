// benches/benchmarks.rs
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use inoxset::types::{Granularity, Period, Rollup};
use inoxset::InoxSet;
use redb::ReadableTable;
use roaring::RoaringBitmap;
use tempfile::TempDir;

fn bench_put_bitmap(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .mempart_flush_threshold(1024 * 1024 * 1024) // disable auto-flush
        .open()
        .unwrap();

    let mut bm = RoaringBitmap::new();
    for i in 0..1000 {
        bm.insert(i);
    }

    c.bench_function("put_bitmap/1K_bits", |b| {
        b.iter(|| {
            store
                .put_bitmap("bench", Period::Day(2026, 3, 11), black_box(bm.clone()))
                .unwrap();
        })
    });
}

fn bench_put_bitmap_with_rollup(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .mempart_flush_threshold(1024 * 1024 * 1024)
        .open()
        .unwrap();
    store
        .register_event("bench", Granularity::Hour, Rollup::Auto)
        .unwrap();

    let mut bm = RoaringBitmap::new();
    for i in 0..1000 {
        bm.insert(i);
    }

    c.bench_function("put_bitmap/1K_bits_rollup_auto", |b| {
        b.iter(|| {
            store
                .put_bitmap(
                    "bench",
                    Period::Hour(2026, 3, 11, 14),
                    black_box(bm.clone()),
                )
                .unwrap();
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    for n_parts in [1, 5, 20] {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        for i in 0..10_000 {
            bm.insert(i);
        }

        // Create n_parts via separate flushes
        for _ in 0..n_parts {
            store
                .put_bitmap("bench", Period::Day(2026, 3, 11), bm.clone())
                .unwrap();
            store.flush().unwrap();
        }

        group.bench_with_input(BenchmarkId::new("parts", n_parts), &n_parts, |b, _| {
            b.iter(|| {
                black_box(store.get("bench", Period::Day(2026, 3, 11)).unwrap());
            })
        });
    }
    group.finish();
}

fn bench_get_compacted(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    let mut bm = RoaringBitmap::new();
    for i in 0..100_000 {
        bm.insert(i);
    }
    store
        .put_bitmap("bench", Period::Day(2026, 3, 11), bm)
        .unwrap();
    store.flush().unwrap();
    store.compact().unwrap();

    c.bench_function("get/compacted_100K", |b| {
        b.iter(|| {
            black_box(store.get("bench", Period::Day(2026, 3, 11)).unwrap());
        })
    });
}

fn bench_flush(c: &mut Criterion) {
    c.bench_function("flush/10_events_100_periods", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let dir = TempDir::new().unwrap();
                let store = InoxSet::builder()
                    .path(dir.path().join("data"))
                    .mempart_flush_threshold(1024 * 1024 * 1024)
                    .open()
                    .unwrap();

                // Fill MemPart
                for e in 0..10 {
                    for d in 1..=10 {
                        let mut bm = RoaringBitmap::new();
                        for i in 0..100 {
                            bm.insert(i);
                        }
                        store
                            .put_bitmap(&format!("event_{e}"), Period::Day(2026, 3, d), bm)
                            .unwrap();
                    }
                }

                let start = std::time::Instant::now();
                store.flush().unwrap();
                total += start.elapsed();
            }
            total
        });
    });
}

fn bench_compact(c: &mut Criterion) {
    c.bench_function("compact/50_periods_5_parts_each", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let dir = TempDir::new().unwrap();
                let store = InoxSet::builder()
                    .path(dir.path().join("data"))
                    .open()
                    .unwrap();

                // Create 50 periods with 5 parts each
                for d in 1u8..=25 {
                    let mut bm = RoaringBitmap::new();
                    for i in 0..1000 {
                        bm.insert(i);
                    }
                    for _ in 0..5 {
                        store
                            .put_bitmap("x", Period::Day(2026, 3, d), bm.clone())
                            .unwrap();
                        store.flush().unwrap();
                    }
                }

                let start = std::time::Instant::now();
                store.compact().unwrap();
                total += start.elapsed();
            }
            total
        });
    });
}

fn bench_redb_txn_baseline(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let db = redb::Database::create(dir.path().join("bench.redb")).unwrap();
    let table_def: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("bench");

    // Init
    {
        let txn = db.begin_write().unwrap();
        {
            let _ = txn.open_table(table_def).unwrap();
        }
        txn.commit().unwrap();
    }

    c.bench_function("redb/write_txn_commit", |b| {
        b.iter(|| {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(table_def).unwrap();
                table.insert("key", black_box(b"value".as_slice())).unwrap();
            }
            txn.commit().unwrap();
        })
    });

    c.bench_function("redb/read_txn", |b| {
        b.iter(|| {
            let txn = db.begin_read().unwrap();
            let table = txn.open_table(table_def).unwrap();
            black_box(table.get("key").unwrap());
        })
    });
}

criterion_group!(
    benches,
    bench_put_bitmap,
    bench_put_bitmap_with_rollup,
    bench_get,
    bench_get_compacted,
    bench_flush,
    bench_compact,
    bench_redb_txn_baseline,
);
criterion_main!(benches);
