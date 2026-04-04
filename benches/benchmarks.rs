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

fn bench_put_ids(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .mempart_flush_threshold(1024 * 1024 * 1024)
        .open()
        .unwrap();

    // Pre-generate 1000 external IDs.
    let ids: Vec<String> = (0..1000).map(|i| format!("usr-{i:08}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

    c.bench_function("put_ids/1K_new_ids", |b| {
        // First call assigns, subsequent calls are lookups.
        b.iter(|| {
            store
                .put_ids("dict_bench", Period::Day(2026, 4, 1), black_box(&id_refs))
                .unwrap();
        })
    });
}

fn bench_get_ids(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    // Seed dictionary + bitmap.
    let ids: Vec<String> = (0..1000).map(|i| format!("usr-{i:08}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    store
        .put_ids("dict_bench", Period::Day(2026, 4, 1), &id_refs)
        .unwrap();
    store.flush().unwrap();

    c.bench_function("get_ids/1K_ids", |b| {
        b.iter(|| {
            black_box(
                store
                    .get_ids("dict_bench", Period::Day(2026, 4, 1))
                    .unwrap(),
            );
        })
    });
}

fn bench_dict_assign_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict/assign");

    for n in [100, 1000, 10_000] {
        let dir = TempDir::new().unwrap();
        let db = redb::Database::create(dir.path().join("dict.redb")).unwrap();
        inoxset::dict::ensure_tables(&db).unwrap();

        let ids: Vec<String> = (0..n).map(|i| format!("id-{i:08}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

        // First call seeds the dictionary.
        inoxset::dict::batch_assign_or_get(&db, &id_refs).unwrap();

        group.bench_with_input(BenchmarkId::new("warm", n), &n, |b, _| {
            b.iter(|| {
                black_box(inoxset::dict::batch_assign_or_get(&db, &id_refs).unwrap());
            })
        });
    }
    group.finish();
}

fn bench_contains_id(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    // Seed: 1 event, 1 period, 10K IDs (compacted).
    let ids: Vec<String> = (0..10_000).map(|i| format!("usr-{i:08}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    store
        .put_ids("segment", Period::Day(2026, 4, 1), &id_refs)
        .unwrap();
    store.flush().unwrap();
    store.compact().unwrap();

    c.bench_function("contains_id/1ev_1period_10K", |b| {
        b.iter(|| {
            black_box(
                store
                    .contains_id("segment", Period::Day(2026, 4, 1), "usr-00005000")
                    .unwrap(),
            );
        })
    });
}

fn bench_find_memberships(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_memberships");

    // Scenario: 5 segments × 7 days, 10K users each, compacted.
    {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let ids: Vec<String> = (0..10_000).map(|i| format!("usr-{i:08}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

        for seg in 0..5 {
            for d in 1..=7u8 {
                store
                    .put_ids(&format!("seg_{seg}"), Period::Day(2026, 4, d), &id_refs)
                    .unwrap();
            }
        }
        store.flush().unwrap();
        store.compact().unwrap();

        // User present in all 35 event×period combos.
        group.bench_function("5seg_7days_hit_all", |b| {
            b.iter(|| {
                black_box(store.find_memberships("usr-00005000").unwrap());
            })
        });

        // User not in dictionary at all.
        group.bench_function("5seg_7days_miss", |b| {
            b.iter(|| {
                black_box(store.find_memberships("unknown-user").unwrap());
            })
        });
    }

    // Scenario: 20 segments × 30 days, 10K users, compacted.
    {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let ids: Vec<String> = (0..10_000).map(|i| format!("usr-{i:08}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

        for seg in 0..20 {
            for d in 1..=30u8 {
                store
                    .put_ids(&format!("seg_{seg}"), Period::Day(2026, 4, d), &id_refs)
                    .unwrap();
            }
        }
        store.flush().unwrap();
        store.compact().unwrap();

        // 600 event×period combos to check.
        group.bench_function("20seg_30days_hit_all", |b| {
            b.iter(|| {
                black_box(store.find_memberships("usr-00005000").unwrap());
            })
        });
    }

    group.finish();
}

fn bench_find_memberships_inverted(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_memberships_inverted");

    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .index_freshness(inoxset::types::IndexFreshness::OnFlush)
        .open()
        .unwrap();

    let ids: Vec<String> = (0..10_000).map(|i| format!("usr-{i:08}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

    for seg in 0..20 {
        for d in 1..=30u8 {
            store
                .put_ids(&format!("seg_{seg}"), Period::Day(2026, 4, d), &id_refs)
                .unwrap();
        }
    }
    store.flush().unwrap();
    store.compact().unwrap();

    group.bench_function("hit_all_600", |b| {
        b.iter(|| {
            black_box(store.find_memberships("usr-00005000").unwrap());
        })
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            black_box(store.find_memberships("unknown-user").unwrap());
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_put_bitmap,
    bench_put_bitmap_with_rollup,
    bench_get,
    bench_get_compacted,
    bench_flush,
    bench_compact,
    bench_put_ids,
    bench_get_ids,
    bench_dict_assign_throughput,
    bench_contains_id,
    bench_find_memberships,
    bench_find_memberships_inverted,
    bench_redb_txn_baseline,
);
criterion_main!(benches);
