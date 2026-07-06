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

    // With bitmap cache: get() serves from cache (valid snapshot).
    // The corruption is only detected on reopen (rebuild cache from disk).
    let result = s.get("x", Period::Day(2026, 3, 11));
    assert!(
        result.is_ok(),
        "cached get() should succeed despite corrupt file"
    );

    // On reopen, the corrupt file is detected during cache build and open
    // fails loudly. Silently skipping the part would serve an empty bitmap
    // for a period that has data — undetectable data loss.
    drop(s);
    let result = InoxSet::builder().path(dir.path().join("data")).open();
    assert!(
        result.is_err(),
        "reopen must surface the corrupt part instead of serving empty data"
    );
}

// --- Dictionary Encoding Integration Tests ---

#[test]
fn dict_put_get_flush_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data");

    {
        let store = InoxSet::builder().path(&path).open().unwrap();
        store
            .put_ids(
                "audience",
                Period::Day(2026, 4, 1),
                &["usr-001", "usr-002", "usr-003"],
            )
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
    }

    {
        let store = InoxSet::builder().path(&path).open().unwrap();
        let ids = store.get_ids("audience", Period::Day(2026, 4, 1)).unwrap();
        let mut sorted = ids;
        sorted.sort();
        assert_eq!(sorted, vec!["usr-001", "usr-002", "usr-003"]);
    }
}

#[test]
fn dict_mixed_with_raw_bitmap() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    store
        .put_ids("ev", Period::Day(2026, 4, 1), &["a", "b"])
        .unwrap();

    let mut bm = RoaringBitmap::new();
    bm.insert(100);
    store.put_bitmap("ev", Period::Day(2026, 4, 1), bm).unwrap();

    let result = store.get("ev", Period::Day(2026, 4, 1)).unwrap();
    assert_eq!(result.len(), 3);

    let ids = store.get_ids("ev", Period::Day(2026, 4, 1)).unwrap();
    assert_eq!(ids.len(), 2);
}

#[test]
fn dict_compaction_preserves_dictionary() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    store
        .put_ids("seg", Period::Day(2026, 4, 1), &["x"])
        .unwrap();
    store.flush().unwrap();

    store
        .put_ids("seg", Period::Day(2026, 4, 1), &["y"])
        .unwrap();
    store.flush().unwrap();

    store.compact().unwrap();

    let ids = store.get_ids("seg", Period::Day(2026, 4, 1)).unwrap();
    let mut sorted = ids;
    sorted.sort();
    assert_eq!(sorted, vec!["x", "y"]);
}

// --- Inverted Index Integration Tests ---

#[test]
fn inverted_index_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data");
    {
        let store = InoxSet::builder()
            .path(&path)
            .index_freshness(inoxset::types::IndexFreshness::OnFlush)
            .open()
            .unwrap();
        store
            .put_ids("seg", Period::Day(2026, 4, 1), &["alice", "bob"])
            .unwrap();
        store.flush().unwrap();
        let m = store.find_memberships("alice").unwrap();
        assert_eq!(m.len(), 1);
        store.close().unwrap();
    }
    {
        let store = InoxSet::builder()
            .path(&path)
            .index_freshness(inoxset::types::IndexFreshness::OnFlush)
            .open()
            .unwrap();
        // Write a new period to trigger index rebuild
        store
            .put_ids("seg2", Period::Day(2026, 4, 2), &["alice"])
            .unwrap();
        store.flush().unwrap();
        // Verify the persisted data is still queryable via index
        let m = store.find_memberships("alice").unwrap();
        assert_eq!(m.len(), 2); // alice in seg (2026-4-1) and seg2 (2026-4-2)
        assert!(store.find_memberships("unknown").unwrap().is_empty());
    }
}

#[test]
fn inverted_index_disabled_fallback() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();
    store
        .put_ids("seg", Period::Day(2026, 4, 1), &["alice"])
        .unwrap();
    store.flush().unwrap();
    let m = store.find_memberships("alice").unwrap();
    assert_eq!(m.len(), 1);
}

#[test]
fn inverted_index_delete_period_rebuilds() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .index_freshness(inoxset::types::IndexFreshness::OnFlush)
        .open()
        .unwrap();
    store
        .put_ids("seg", Period::Day(2026, 4, 1), &["alice"])
        .unwrap();
    store
        .put_ids("seg", Period::Day(2026, 4, 2), &["alice"])
        .unwrap();
    store.flush().unwrap();
    assert_eq!(store.find_memberships("alice").unwrap().len(), 2);
    store.delete_period("seg", Period::Day(2026, 4, 1)).unwrap();
    assert_eq!(store.find_memberships("alice").unwrap().len(), 1);
}

#[test]
fn inverted_index_compaction_preserves() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .index_freshness(inoxset::types::IndexFreshness::OnFlush)
        .open()
        .unwrap();
    store
        .put_ids("seg", Period::Day(2026, 4, 1), &["alice"])
        .unwrap();
    store.flush().unwrap();
    store
        .put_ids("seg", Period::Day(2026, 4, 1), &["bob"])
        .unwrap();
    store.flush().unwrap();
    store.compact().unwrap();
    assert_eq!(store.find_memberships("alice").unwrap().len(), 1);
    assert_eq!(store.find_memberships("bob").unwrap().len(), 1);
}

// --- Cross-Event Intersection (global dict) ---

#[test]
fn cross_event_intersection_correct_with_global_dict() {
    // Global dictionary assigns the same u32 to the same external_id across
    // all events, so cross-event bitmap intersection produces correct results.
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    // "alice" visits pages; "alice" is also premium.
    store
        .put_ids("page_visitors", Period::Day(2026, 4, 1), &["alice", "bob"])
        .unwrap();
    store
        .put_ids("premium", Period::Static, &["charlie", "alice"])
        .unwrap();
    store.flush().unwrap();

    let visitors = store.get("page_visitors", Period::Day(2026, 4, 1)).unwrap();
    let premium = store.get("premium", Period::Static).unwrap();
    let intersection = &visitors & &premium;

    // With global dict: alice gets the SAME u32 in both events.
    // Bitmap intersection correctly returns only alice.
    assert_eq!(
        intersection.len(),
        1,
        "global dict: intersection should contain exactly alice"
    );
}

// --- Re-insert after remove (temporal delta semantics) ---

fn bm_of(ids: &[u32]) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    for &id in ids {
        bm.insert(id);
    }
    bm
}

#[test]
fn readd_after_remove_in_same_mempart() {
    // put → remove → put without any flush: the re-inserted bit must survive.
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let p = Period::Day(2026, 3, 11);

    s.put_bitmap("ev", p, bm_of(&[42])).unwrap();
    s.remove_bits("ev", p, &[42]).unwrap();
    s.put_bitmap("ev", p, bm_of(&[42])).unwrap();

    let got = s.get("ev", p).unwrap();
    assert!(got.contains(42), "re-inserted bit lost in mempart window");
}

#[test]
fn remove_after_readd_still_removes() {
    // put → remove → put → remove: the last remove wins.
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let p = Period::Day(2026, 3, 11);

    s.put_bitmap("ev", p, bm_of(&[42])).unwrap();
    s.remove_bits("ev", p, &[42]).unwrap();
    s.put_bitmap("ev", p, bm_of(&[42])).unwrap();
    s.remove_bits("ev", p, &[42]).unwrap();

    let got = s.get("ev", p).unwrap();
    assert!(!got.contains(42), "final remove must win");
}

#[test]
fn readd_after_remove_across_flushes() {
    // Three separate flushes: data{42} / delta{42} / data{42}.
    // The delta must only erase the first put, not the re-insert.
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let p = Period::Day(2026, 3, 11);

    s.put_bitmap("ev", p, bm_of(&[42, 7])).unwrap();
    s.flush().unwrap();
    s.remove_bits("ev", p, &[42]).unwrap();
    s.flush().unwrap();
    s.put_bitmap("ev", p, bm_of(&[42])).unwrap();

    // Before the final flush (mempart put over disk delta).
    let got = s.get("ev", p).unwrap();
    assert!(got.contains(42), "unflushed re-insert erased by disk delta");
    assert!(got.contains(7));

    // After the final flush (three parts on disk).
    s.flush().unwrap();
    let got = s.get("ev", p).unwrap();
    assert!(got.contains(42), "flushed re-insert erased by older delta");
    assert!(s.contains_batch("ev", p, &[42]).unwrap()[0]);

    // And after compaction.
    s.compact().unwrap();
    let got = s.get("ev", p).unwrap();
    assert!(got.contains(42), "compaction erased re-inserted bit");
    assert!(got.contains(7));
    assert_eq!(got.len(), 2);
}

#[test]
fn readd_after_remove_survives_reopen() {
    // The part-id ordering must also hold when the read index is rebuilt
    // from the catalog on reopen.
    let dir = TempDir::new().unwrap();
    let p = Period::Day(2026, 3, 11);
    {
        let s = store(&dir);
        s.put_bitmap("ev", p, bm_of(&[42])).unwrap();
        s.flush().unwrap();
        s.remove_bits("ev", p, &[42]).unwrap();
        s.flush().unwrap();
        s.put_bitmap("ev", p, bm_of(&[42])).unwrap();
        s.flush().unwrap();
        s.close().unwrap();
    }
    let s = store(&dir);
    let got = s.get("ev", p).unwrap();
    assert!(got.contains(42), "re-inserted bit lost after reopen");
}

#[test]
fn readd_after_remove_with_rollup() {
    // Remove propagates deltas to Day/Month/Year; a later put at another
    // hour of the same day must re-establish the bit at every ancestor.
    let dir = TempDir::new().unwrap();
    let s = store_with_rollup(&dir);

    s.put_bitmap("ev", Period::Hour(2026, 3, 11, 10), bm_of(&[42]))
        .unwrap();
    s.remove_bits("ev", Period::Hour(2026, 3, 11, 10), &[42])
        .unwrap();
    s.put_bitmap("ev", Period::Hour(2026, 3, 11, 14), bm_of(&[42]))
        .unwrap();

    // Hour 10 was removed and never re-put: still gone.
    assert!(!s
        .get("ev", Period::Hour(2026, 3, 11, 10))
        .unwrap()
        .contains(42));
    // Hour 14 and all rollup ancestors must contain the bit again.
    assert!(s
        .get("ev", Period::Hour(2026, 3, 11, 14))
        .unwrap()
        .contains(42));
    assert!(s.get("ev", Period::Day(2026, 3, 11)).unwrap().contains(42));
    assert!(s.get("ev", Period::Month(2026, 3)).unwrap().contains(42));
    assert!(s.get("ev", Period::Year(2026)).unwrap().contains(42));
}

#[test]
fn unflushed_remove_hides_membership_in_find_memberships() {
    // find_memberships must reflect unflushed removes, matching get().
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let p = Period::Day(2026, 3, 11);

    s.put_ids("ev", p, &["alice"]).unwrap();
    s.flush().unwrap();
    assert_eq!(s.find_memberships("alice").unwrap().len(), 1);

    s.remove_ids("ev", p, &["alice"]).unwrap();
    assert!(
        s.find_memberships("alice").unwrap().is_empty(),
        "unflushed remove must hide the membership"
    );
}

// --- get_range bounds hardening ---

#[test]
fn get_range_reversed_bounds_returns_empty() {
    // start > end must return empty instead of looping unboundedly.
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    s.put_bitmap("ev", Period::Day(2026, 3, 11), bm_of(&[1]))
        .unwrap();

    let got = s
        .get_range("ev", Period::Day(2026, 3, 12), Period::Day(2026, 3, 10))
        .unwrap();
    assert!(got.is_empty());
}

#[test]
fn get_range_invalid_bound_is_rejected() {
    // An unreachable end bound (Feb 30) must error instead of iterating
    // past it forever.
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    let result = s.get_range("ev", Period::Day(2026, 2, 1), Period::Day(2026, 2, 30));
    assert!(result.is_err());
}

#[test]
fn put_bitmap_rejects_invalid_period() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);
    assert!(s
        .put_bitmap("ev", Period::Day(2026, 13, 40), bm_of(&[1]))
        .is_err());
    assert!(s
        .put_bitmap("ev", Period::Day(2026, 2, 29), bm_of(&[1]))
        .is_err()); // 2026 is not a leap year
}

// --- Inverted index lifecycle: reopen and erasure staleness ---

fn store_with_index(dir: &TempDir) -> InoxSet {
    InoxSet::builder()
        .path(dir.path().join("data"))
        .default_granularity(Granularity::Day)
        .default_rollup(Rollup::None)
        .index_freshness(inoxset::IndexFreshness::OnFlush)
        .open()
        .unwrap()
}

#[test]
fn inverted_index_populated_on_reopen() {
    // Regression: after reopen the index started empty and find_memberships
    // silently returned no results until the first flush.
    let dir = TempDir::new().unwrap();
    let p = Period::Day(2026, 3, 11);
    {
        let s = store_with_index(&dir);
        s.put_ids("ev_a", p, &["alice", "bob"]).unwrap();
        s.put_ids("ev_b", p, &["alice"]).unwrap();
        s.flush().unwrap();
        assert_eq!(s.find_memberships("alice").unwrap().len(), 2);
        s.close().unwrap();
    }

    let s = store_with_index(&dir);
    let memberships = s.find_memberships("alice").unwrap();
    assert_eq!(
        memberships.len(),
        2,
        "reopened store must serve find_memberships without a prior flush"
    );
    assert!(s.contains_id("ev_a", p, "alice").unwrap());
    assert!(!s.contains_id("ev_a", p, "unknown").unwrap());
}

#[test]
fn delete_entity_read_your_deletes_with_frozen_index() {
    // Regression: between delete_entity and the next flush, contains_id kept
    // returning true because the tombstone check resolved through the
    // dictionary entry that delete_entity had just removed.
    let dir = TempDir::new().unwrap();
    let s = store_with_index(&dir);
    let p = Period::Day(2026, 3, 11);

    s.put_ids("ev_a", p, &["alice", "bob"]).unwrap();
    s.put_ids("ev_b", p, &["alice"]).unwrap();
    s.flush().unwrap();
    assert!(s.contains_id("ev_a", p, "alice").unwrap());

    let removed = s.delete_entity("alice").unwrap();
    assert_eq!(removed, 2);

    // Read-your-deletes: no read path may still affirm membership.
    assert!(
        !s.contains_id("ev_a", p, "alice").unwrap(),
        "contains_id must be false immediately after delete_entity"
    );
    assert!(!s.contains_id("ev_b", p, "alice").unwrap());
    assert!(s.find_memberships("alice").unwrap().is_empty());
    // Unrelated entities are untouched.
    assert!(s.contains_id("ev_a", p, "bob").unwrap());

    // And still false once the tombstones are durable.
    s.flush().unwrap();
    s.compact().unwrap();
    assert!(!s.contains_id("ev_a", p, "alice").unwrap());
}

// --- N-way cardinality ---

#[test]
fn union_cardinality_many_folds_buckets() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    s.put_bitmap("ev", Period::Day(2026, 3, 10), bm_of(&[1, 2]))
        .unwrap();
    s.put_bitmap("ev", Period::Day(2026, 3, 11), bm_of(&[2, 3]))
        .unwrap();
    s.put_bitmap("ev", Period::Day(2026, 3, 12), bm_of(&[3, 4]))
        .unwrap();
    s.flush().unwrap();

    let keys = [
        ("ev", Period::Day(2026, 3, 10)),
        ("ev", Period::Day(2026, 3, 11)),
        ("ev", Period::Day(2026, 3, 12)),
    ];
    assert_eq!(s.union_cardinality_many(&keys).unwrap(), 4); // {1,2,3,4}
    assert_eq!(s.union_cardinality_many(&keys[..1]).unwrap(), 2);
    assert_eq!(s.union_cardinality_many(&[]).unwrap(), 0);

    // Consistent with the pairwise API.
    assert_eq!(
        s.union_cardinality_many(&keys[..2]).unwrap(),
        s.union_cardinality("ev", Period::Day(2026, 3, 10), "ev", Period::Day(2026, 3, 11))
            .unwrap()
    );
}

#[test]
fn intersect_cardinality_many_short_circuits() {
    let dir = TempDir::new().unwrap();
    let s = store(&dir);

    s.put_bitmap("ev", Period::Day(2026, 3, 10), bm_of(&[1, 2, 3]))
        .unwrap();
    s.put_bitmap("ev", Period::Day(2026, 3, 11), bm_of(&[2, 3, 4]))
        .unwrap();
    s.put_bitmap("ev", Period::Day(2026, 3, 12), bm_of(&[3, 4, 5]))
        .unwrap();
    s.put_bitmap("ev", Period::Day(2026, 3, 13), bm_of(&[99]))
        .unwrap();
    s.flush().unwrap();

    let d = |day| ("ev", Period::Day(2026, 3, day));
    assert_eq!(s.intersect_cardinality_many(&[d(10), d(11), d(12)]).unwrap(), 1); // {3}
    assert_eq!(s.intersect_cardinality_many(&[d(10)]).unwrap(), 3);
    assert_eq!(s.intersect_cardinality_many(&[]).unwrap(), 0);
    // Disjoint bucket empties the running intersection (short-circuit path).
    assert_eq!(
        s.intersect_cardinality_many(&[d(10), d(13), d(11), d(12)]).unwrap(),
        0
    );
}
