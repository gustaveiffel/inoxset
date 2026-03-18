// tests/integration.rs
use inoxset::types::{Granularity, Period, Rollup};
use inoxset::InoxSet;
use roaring::RoaringBitmap;
use tempfile::TempDir;

fn store(dir: &TempDir) -> InoxSet {
    InoxSet::builder()
        .path(dir.path().join("data"))
        .default_granularity(Granularity::Day)
        .default_rollup(Rollup::None)
        .open()
        .unwrap()
}

fn store_with_rollup(dir: &TempDir) -> InoxSet {
    InoxSet::builder()
        .path(dir.path().join("data"))
        .default_granularity(Granularity::Hour)
        .default_rollup(Rollup::Auto)
        .open()
        .unwrap()
}

// --- Roundtrip ---

#[test]
fn put_flush_get_roundtrip() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let mut bm = RoaringBitmap::new();
    for i in 0..1000 {
        bm.insert(i);
    }
    s.put_bitmap("active", Period::Day(2026, 3, 11), bm.clone())
        .unwrap();
    s.flush().unwrap();

    let got = s.get("active", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(got, bm);
}

#[test]
fn put_get_without_flush_uses_mempart() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let mut bm = RoaringBitmap::new();
    bm.insert(42);
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();

    let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert!(got.contains(42));
}

// --- Static bitmaps ---

#[test]
fn static_bitmaps() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    s.register_event("premium", Granularity::None, Rollup::None)
        .unwrap();

    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    bm.insert(2);
    s.put_bitmap("premium", Period::Static, bm).unwrap();
    s.flush().unwrap();

    let got = s.get("premium", Period::Static).unwrap();
    assert_eq!(got.len(), 2);
}

// --- Rollup ---

#[test]
fn rollup_hour_to_day_month_year() {
    let dir = TempDir::new().unwrap();
    let s = store_with_rollup(&dir);
    s.register_event("active", Granularity::Hour, Rollup::Auto)
        .unwrap();

    let mut bm = RoaringBitmap::new();
    bm.insert(42);
    bm.insert(99);
    s.put_bitmap("active", Period::Hour(2026, 3, 11, 14), bm)
        .unwrap();
    s.flush().unwrap();

    let day = s.get("active", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(day.len(), 2);
    let month = s.get("active", Period::Month(2026, 3)).unwrap();
    assert_eq!(month.len(), 2);
    let year = s.get("active", Period::Year(2026)).unwrap();
    assert_eq!(year.len(), 2);
}

#[test]
fn rollup_none_no_propagation() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    s.register_event("x", Granularity::Hour, Rollup::None)
        .unwrap();

    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    s.put_bitmap("x", Period::Hour(2026, 3, 11, 14), bm)
        .unwrap();
    s.flush().unwrap();

    let day = s.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert!(day.is_empty());
}

#[test]
fn rollup_accumulates_multiple_hours() {
    let dir = TempDir::new().unwrap();
    let s = store_with_rollup(&dir);
    s.register_event("active", Granularity::Hour, Rollup::Auto)
        .unwrap();

    let mut bm1 = RoaringBitmap::new();
    bm1.insert(1);
    let mut bm2 = RoaringBitmap::new();
    bm2.insert(2);
    s.put_bitmap("active", Period::Hour(2026, 3, 11, 14), bm1)
        .unwrap();
    s.put_bitmap("active", Period::Hour(2026, 3, 11, 15), bm2)
        .unwrap();
    s.flush().unwrap();

    let day = s.get("active", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(day.len(), 2); // OR of both hours
}

// --- replace_bitmap ---

#[test]
fn replace_bitmap_overwrites() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut bm1 = RoaringBitmap::new();
    bm1.insert(1);
    bm1.insert(2);
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm1).unwrap();
    s.flush().unwrap();

    let mut bm2 = RoaringBitmap::new();
    bm2.insert(99);
    s.replace_bitmap("x", Period::Day(2026, 3, 11), bm2)
        .unwrap();

    let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(got.len(), 1);
    assert!(got.contains(99));
    assert!(!got.contains(1));
}

// --- bulk_replace ---

#[test]
fn bulk_replace_multiple_periods() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut b1 = RoaringBitmap::new();
    b1.insert(10);
    let mut b2 = RoaringBitmap::new();
    b2.insert(20);
    s.bulk_replace(
        "x",
        &[
            (Period::Day(2026, 3, 10), b1),
            (Period::Day(2026, 3, 11), b2),
        ],
    )
    .unwrap();

    assert_eq!(s.get("x", Period::Day(2026, 3, 10)).unwrap().len(), 1);
    assert_eq!(s.get("x", Period::Day(2026, 3, 11)).unwrap().len(), 1);
}

// --- remove_bits / delta parts ---

#[test]
fn remove_bits_applied_at_read() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    bm.insert(2);
    bm.insert(3);
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    s.flush().unwrap();

    s.remove_bits("x", Period::Day(2026, 3, 11), &[2]).unwrap();

    let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert!(!got.contains(2));
    assert_eq!(got.len(), 2);
}

#[test]
fn delta_rollup_propagation() {
    let dir = TempDir::new().unwrap();
    let s = store_with_rollup(&dir);
    s.register_event("active", Granularity::Hour, Rollup::Auto)
        .unwrap();

    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    bm.insert(2);
    s.put_bitmap("active", Period::Hour(2026, 3, 11, 14), bm)
        .unwrap();
    s.flush().unwrap();

    s.remove_bits("active", Period::Hour(2026, 3, 11, 14), &[1])
        .unwrap();

    let day = s.get("active", Period::Day(2026, 3, 11)).unwrap();
    assert!(!day.contains(1));
    assert!(day.contains(2));
}

#[test]
fn delta_compaction_materializes_deletes() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    bm.insert(2);
    bm.insert(3);
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    s.flush().unwrap();

    s.remove_bits("x", Period::Day(2026, 3, 11), &[2]).unwrap();
    s.flush().unwrap();

    s.compact().unwrap();

    let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(got.len(), 2);
    assert!(!got.contains(2));
}

// --- Crash recovery ---

#[test]
fn drop_auto_flushes_data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let s = store(&dir);
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
        // No explicit flush — Drop calls flush_internal by design.
    }

    let s2 = store(&dir);
    let got = s2.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert!(got.contains(42));
}

#[test]
fn data_survives_reopen_after_flush() {
    let dir = TempDir::new().unwrap();
    {
        let s = store(&dir);
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
        s.flush().unwrap();
    }

    let s2 = store(&dir);
    let got = s2.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(got.len(), 1);
    assert!(got.contains(42));
}

// --- Concurrent reads ---

#[test]
fn concurrent_reads_during_writes() {
    use std::sync::Arc;
    use std::thread;

    let dir = TempDir::new().unwrap();
    let s = Arc::new(store(&dir));

    // Pre-load some data
    let mut bm = RoaringBitmap::new();
    for i in 0..1000 {
        bm.insert(i);
    }
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    s.flush().unwrap();

    // Spawn readers
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let s = Arc::clone(&s);
            thread::spawn(move || {
                for _ in 0..100 {
                    let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
                    assert!(got.len() >= 1000);
                }
            })
        })
        .collect();

    // Writer continues
    for i in 1000..2000 {
        let mut bm = RoaringBitmap::new();
        bm.insert(i);
        s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    }

    for h in handles {
        h.join().unwrap();
    }
}

// --- Large bitmaps ---

#[test]
fn large_bitmap_roundtrip() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut bm = RoaringBitmap::new();
    for i in 0..100_000 {
        bm.insert(i);
    }
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    s.flush().unwrap();

    let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(got.len(), 100_000);
}

// --- Set operations (via roaring crate) ---

#[test]
fn set_operations_cross_event() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    s.register_event("premium", Granularity::None, Rollup::None)
        .unwrap();

    let mut active = RoaringBitmap::new();
    for i in 0..100 {
        active.insert(i);
    }
    s.put_bitmap("active", Period::Day(2026, 3, 11), active)
        .unwrap();

    let mut premium = RoaringBitmap::new();
    for i in 50..150 {
        premium.insert(i);
    }
    s.put_bitmap("premium", Period::Static, premium).unwrap();
    s.flush().unwrap();

    let a = s.get("active", Period::Day(2026, 3, 11)).unwrap();
    let p = s.get("premium", Period::Static).unwrap();

    let intersection = &a & &p;
    assert_eq!(intersection.len(), 50); // 50..100

    let union = &a | &p;
    assert_eq!(union.len(), 150); // 0..150

    let diff = &a - &p;
    assert_eq!(diff.len(), 50); // 0..50
}

// --- Backfill ---

#[test]
fn backfill_write_to_closed_period() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    // Use a past period that is definitely Closed
    let mut bm1 = RoaringBitmap::new();
    bm1.insert(1);
    s.put_bitmap("x", Period::Day(2020, 1, 1), bm1).unwrap();
    s.flush().unwrap();

    // Backfill write should succeed
    let mut bm2 = RoaringBitmap::new();
    bm2.insert(2);
    s.put_bitmap("x", Period::Day(2020, 1, 1), bm2).unwrap();
    s.flush().unwrap();

    let got = s.get("x", Period::Day(2020, 1, 1)).unwrap();
    assert_eq!(got.len(), 2);
}

#[test]
fn backfill_on_compacted_reverts_to_closed() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    // Create and compact a past period
    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    s.put_bitmap("x", Period::Day(2020, 1, 1), bm).unwrap();
    s.flush().unwrap();
    s.compact().unwrap();

    // Write to compacted period (backfill)
    let mut bm2 = RoaringBitmap::new();
    bm2.insert(2);
    s.put_bitmap("x", Period::Day(2020, 1, 1), bm2).unwrap();
    s.flush().unwrap();

    // Should have 2 parts now (compacted + new)
    let got = s.get("x", Period::Day(2020, 1, 1)).unwrap();
    assert_eq!(got.len(), 2);
}

// --- get_range ---

#[test]
fn get_range_multiple_days() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    for d in 10u8..=15 {
        let mut bm = RoaringBitmap::new();
        bm.insert(d as u32);
        s.put_bitmap("x", Period::Day(2026, 3, d), bm).unwrap();
    }
    s.flush().unwrap();

    let range = s
        .get_range("x", Period::Day(2026, 3, 11), Period::Day(2026, 3, 14))
        .unwrap();
    assert_eq!(range.len(), 4);
}

// --- exists ---

#[test]
fn exists_before_and_after_data() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    assert!(!s.exists("x", Period::Day(2026, 3, 11)).unwrap());

    let mut bm = RoaringBitmap::new();
    bm.insert(1);
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();

    assert!(s.exists("x", Period::Day(2026, 3, 11)).unwrap());
}

// --- cardinality ---

#[test]
fn cardinality_after_compact() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut bm = RoaringBitmap::new();
    for i in 0..500 {
        bm.insert(i);
    }
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    s.flush().unwrap();
    s.compact().unwrap();

    let card = s.cardinality("x", Period::Day(2026, 3, 11)).unwrap();
    assert_eq!(card, 500);
}

// --- Fix 3: Auto-registration on first put ---

#[test]
fn auto_registration_on_first_put() {
    let dir = TempDir::new().unwrap();
    let s = InoxSet::builder()
        .path(dir.path().join("data"))
        .default_granularity(Granularity::Day)
        .default_rollup(Rollup::Auto)
        .open()
        .unwrap();

    // No register_event call — auto-registers on first put
    let mut bm = RoaringBitmap::new();
    bm.insert(42);
    s.put_bitmap("new_event", Period::Day(2026, 3, 11), bm)
        .unwrap();

    let events = s.list_events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name, "new_event");
    assert_eq!(events[0].finest_granularity, Granularity::Day);
    assert_eq!(events[0].rollup, Rollup::Auto);
}

// --- Fix 4: Corrupt part file returns error ---

#[test]
fn corrupt_part_file_returns_error() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    let mut bm = RoaringBitmap::new();
    bm.insert(42);
    s.put_bitmap("x", Period::Day(2026, 3, 11), bm).unwrap();
    s.flush().unwrap();

    // Corrupt a part file by writing garbage
    let parts_dir = dir.path().join("data/parts/x/day");
    let part_files: Vec<_> = std::fs::read_dir(&parts_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension() == Some("roar".as_ref()))
        .collect();
    assert!(!part_files.is_empty());
    std::fs::write(part_files[0].path(), b"garbage data").unwrap();

    // get() should return BitmapCorrupted, not panic
    let result = s.get("x", Period::Day(2026, 3, 11));
    assert!(result.is_err());
}
