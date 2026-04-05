// benches/comparison.rs
//
// Competitive benchmarks: inoxset vs Redis vs bitmapist-server
// on equivalent bitmap operations.
//
// Prerequisites:
//   - Redis running on localhost:6379
//   - bitmapist-server running on localhost:6380
//
// Run: cargo bench --bench comparison

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use inoxset::types::Period;
use inoxset::InoxSet;
use roaring::RoaringBitmap;
use tempfile::TempDir;

fn redis_client(port: u16) -> Option<redis::Connection> {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).ok()?;
    client.get_connection().ok()
}

// ── Write: store 1K user IDs ─────────────────────────────────────────────────

fn bench_write_1k(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_1K_ids");

    // inoxset: put_bitmap with 1K bits
    {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .mempart_flush_threshold(1024 * 1024 * 1024)
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        for i in 0..1000 {
            bm.insert(i);
        }

        group.bench_function("inoxset/put_bitmap", |b| {
            b.iter(|| {
                store
                    .put_bitmap("bench", Period::Day(2026, 3, 11), black_box(bm.clone()))
                    .unwrap();
            })
        });
    }

    // Redis: SETBIT × 1000 (pipelined)
    if let Some(mut con) = redis_client(6379) {
        let _: () = redis::cmd("DEL")
            .arg("bench:2026-03-11")
            .query(&mut con)
            .unwrap();

        group.bench_function("redis/setbit_pipeline", |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for i in 0..1000u32 {
                    pipe.cmd("SETBIT")
                        .arg("bench:2026-03-11")
                        .arg(i)
                        .arg(1)
                        .ignore();
                }
                let _: () = pipe.query(&mut con).unwrap();
            })
        });
    }

    // bitmapist-server: SETBIT × 1000 (pipelined)
    if let Some(mut con) = redis_client(6380) {
        let _: () = redis::cmd("DEL")
            .arg("bench:2026-03-11")
            .query(&mut con)
            .unwrap_or(());

        group.bench_function("bitmapist/setbit_pipeline", |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for i in 0..1000u32 {
                    pipe.cmd("SETBIT")
                        .arg("bench:2026-03-11")
                        .arg(i)
                        .arg(1)
                        .ignore();
                }
                let _: () = pipe.query(&mut con).unwrap();
            })
        });
    }

    group.finish();
}

// ── Read: retrieve bitmap ────────────────────────────────────────────────────

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_bitmap");

    // inoxset: get (flushed, single part)
    {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm = RoaringBitmap::new();
        for i in 0..10_000 {
            bm.insert(i);
        }
        store
            .put_bitmap("bench", Period::Day(2026, 3, 11), bm)
            .unwrap();
        store.flush().unwrap();

        group.bench_function("inoxset/get_10K", |b| {
            b.iter(|| {
                black_box(store.get("bench", Period::Day(2026, 3, 11)).unwrap());
            })
        });
    }

    // Redis: BITCOUNT (10K bits set)
    if let Some(mut con) = redis_client(6379) {
        let _: () = redis::cmd("DEL")
            .arg("read_bench:2026-03-11")
            .query(&mut con)
            .unwrap();
        {
            let mut pipe = redis::pipe();
            for i in 0..10_000u32 {
                pipe.cmd("SETBIT")
                    .arg("read_bench:2026-03-11")
                    .arg(i)
                    .arg(1)
                    .ignore();
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("redis/bitcount_10K", |b| {
            b.iter(|| {
                let count: u64 = redis::cmd("BITCOUNT")
                    .arg("read_bench:2026-03-11")
                    .query(&mut con)
                    .unwrap();
                black_box(count);
            })
        });
    }

    // bitmapist-server: BITCOUNT (10K bits set)
    if let Some(mut con) = redis_client(6380) {
        {
            let mut pipe = redis::pipe();
            for i in 0..10_000u32 {
                pipe.cmd("SETBIT")
                    .arg("read_bench:2026-03-11")
                    .arg(i)
                    .arg(1)
                    .ignore();
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("bitmapist/bitcount_10K", |b| {
            b.iter(|| {
                let count: u64 = redis::cmd("BITCOUNT")
                    .arg("read_bench:2026-03-11")
                    .query(&mut con)
                    .unwrap();
                black_box(count);
            })
        });
    }

    group.finish();
}

// ── Intersection: AND two bitmaps ────────────────────────────────────────────

fn bench_intersection(c: &mut Criterion) {
    let mut group = c.benchmark_group("intersection");

    // inoxset: in-process & operator
    {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let mut bm_a = RoaringBitmap::new();
        let mut bm_b = RoaringBitmap::new();
        for i in 0..10_000 {
            bm_a.insert(i);
        }
        for i in 5_000..15_000 {
            bm_b.insert(i);
        }
        store
            .put_bitmap("a", Period::Day(2026, 3, 11), bm_a)
            .unwrap();
        store
            .put_bitmap("b", Period::Day(2026, 3, 11), bm_b)
            .unwrap();
        store.flush().unwrap();

        group.bench_function("inoxset/get_and_intersect", |b| {
            b.iter(|| {
                let a = store.get("a", Period::Day(2026, 3, 11)).unwrap();
                let b = store.get("b", Period::Day(2026, 3, 11)).unwrap();
                black_box(&a & &b);
            })
        });

        group.bench_function("inoxset/intersect_cardinality", |b| {
            b.iter(|| {
                black_box(
                    store
                        .intersect_cardinality(
                            "a",
                            Period::Day(2026, 3, 11),
                            "b",
                            Period::Day(2026, 3, 11),
                        )
                        .unwrap(),
                );
            })
        });
    }

    // Redis: BITOP AND
    if let Some(mut con) = redis_client(6379) {
        let _: () = redis::cmd("DEL")
            .arg("int_a")
            .arg("int_b")
            .arg("int_result")
            .query(&mut con)
            .unwrap();
        {
            let mut pipe = redis::pipe();
            for i in 0..10_000u32 {
                pipe.cmd("SETBIT").arg("int_a").arg(i).arg(1).ignore();
            }
            for i in 5_000..15_000u32 {
                pipe.cmd("SETBIT").arg("int_b").arg(i).arg(1).ignore();
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("redis/bitop_and", |b| {
            b.iter(|| {
                let _: i64 = redis::cmd("BITOP")
                    .arg("AND")
                    .arg("int_result")
                    .arg("int_a")
                    .arg("int_b")
                    .query(&mut con)
                    .unwrap();
            })
        });
    }

    // bitmapist-server: BITOP AND
    if let Some(mut con) = redis_client(6380) {
        {
            let mut pipe = redis::pipe();
            for i in 0..10_000u32 {
                pipe.cmd("SETBIT").arg("int_a").arg(i).arg(1).ignore();
            }
            for i in 5_000..15_000u32 {
                pipe.cmd("SETBIT").arg("int_b").arg(i).arg(1).ignore();
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("bitmapist/bitop_and", |b| {
            b.iter(|| {
                let _: i64 = redis::cmd("BITOP")
                    .arg("AND")
                    .arg("int_result")
                    .arg("int_a")
                    .arg("int_b")
                    .query(&mut con)
                    .unwrap();
            })
        });
    }

    group.finish();
}

// ── Membership: "is user X in segment Y on day Z?" ──────────────────────────

fn bench_membership(c: &mut Criterion) {
    let mut group = c.benchmark_group("membership");

    // ── Single check: 1 event, 1 period, 10K users ──

    // inoxset: contains_id
    {
        let dir = TempDir::new().unwrap();
        let store = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();

        let ids: Vec<String> = (0..10_000).map(|i| format!("usr-{i:08}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        store
            .put_ids("segment", Period::Day(2026, 4, 1), &id_refs)
            .unwrap();
        store.flush().unwrap();
        store.compact().unwrap();

        group.bench_function("inoxset/contains_id", |b| {
            b.iter(|| {
                black_box(
                    store
                        .contains_id("segment", Period::Day(2026, 4, 1), "usr-00005000")
                        .unwrap(),
                );
            })
        });
    }

    // Redis: GETBIT
    if let Some(mut con) = redis_client(6379) {
        let _: () = redis::cmd("DEL")
            .arg("membership:seg:2026-04-01")
            .query(&mut con)
            .unwrap();
        {
            let mut pipe = redis::pipe();
            for i in 0..10_000u32 {
                pipe.cmd("SETBIT")
                    .arg("membership:seg:2026-04-01")
                    .arg(i)
                    .arg(1)
                    .ignore();
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("redis/getbit", |b| {
            b.iter(|| {
                let v: u32 = redis::cmd("GETBIT")
                    .arg("membership:seg:2026-04-01")
                    .arg(5000)
                    .query(&mut con)
                    .unwrap();
                black_box(v);
            })
        });
    }

    // bitmapist-server: GETBIT
    if let Some(mut con) = redis_client(6380) {
        {
            let mut pipe = redis::pipe();
            for i in 0..10_000u32 {
                pipe.cmd("SETBIT")
                    .arg("membership:seg:2026-04-01")
                    .arg(i)
                    .arg(1)
                    .ignore();
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("bitmapist/getbit", |b| {
            b.iter(|| {
                let v: u32 = redis::cmd("GETBIT")
                    .arg("membership:seg:2026-04-01")
                    .arg(5000)
                    .query(&mut con)
                    .unwrap();
                black_box(v);
            })
        });
    }

    // ── Multi check: scan 20 segments × 30 days ──

    // inoxset: find_memberships
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

        group.bench_function("inoxset/find_memberships_600", |b| {
            b.iter(|| {
                black_box(store.find_memberships("usr-00005000").unwrap());
            })
        });
    }

    // Redis: GETBIT × 600 (pipelined)
    if let Some(mut con) = redis_client(6379) {
        // Seed 600 keys.
        for seg in 0..20 {
            let mut pipe = redis::pipe();
            for d in 1..=30u8 {
                let key = format!("find:seg_{seg}:2026-04-{d:02}");
                for i in 0..10_000u32 {
                    pipe.cmd("SETBIT").arg(&key).arg(i).arg(1).ignore();
                }
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("redis/getbit_pipeline_600", |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for seg in 0..20 {
                    for d in 1..=30u8 {
                        let key = format!("find:seg_{seg}:2026-04-{d:02}");
                        pipe.cmd("GETBIT").arg(key).arg(5000);
                    }
                }
                let results: Vec<u32> = pipe.query(&mut con).unwrap();
                black_box(results);
            })
        });
    }

    // bitmapist-server: GETBIT × 600 (pipelined)
    if let Some(mut con) = redis_client(6380) {
        for seg in 0..20 {
            let mut pipe = redis::pipe();
            for d in 1..=30u8 {
                let key = format!("find:seg_{seg}:2026-04-{d:02}");
                for i in 0..10_000u32 {
                    pipe.cmd("SETBIT").arg(&key).arg(i).arg(1).ignore();
                }
            }
            let _: () = pipe.query(&mut con).unwrap();
        }

        group.bench_function("bitmapist/getbit_pipeline_600", |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for seg in 0..20 {
                    for d in 1..=30u8 {
                        let key = format!("find:seg_{seg}:2026-04-{d:02}");
                        pipe.cmd("GETBIT").arg(key).arg(5000);
                    }
                }
                let results: Vec<u32> = pipe.query(&mut con).unwrap();
                black_box(results);
            })
        });
    }

    group.finish();
}

criterion_group!(
    comparison,
    bench_write_1k,
    bench_read,
    bench_intersection,
    bench_membership,
);
criterion_main!(comparison);
