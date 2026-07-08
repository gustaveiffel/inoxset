// tests/property.rs
use inoxset::types::{Granularity, Period, Rollup};
use inoxset::InoxSet;
use proptest::prelude::*;
use roaring::RoaringBitmap;
use tempfile::TempDir;

fn arb_bitmap(max_val: u32, max_len: usize) -> impl Strategy<Value = RoaringBitmap> {
    prop::collection::vec(0..max_val, 0..max_len).prop_map(|vals| {
        let mut bm = RoaringBitmap::new();
        for v in vals {
            bm.insert(v);
        }
        bm
    })
}

fn store(dir: &TempDir) -> InoxSet {
    InoxSet::builder()
        .path(dir.path().join("data"))
        .default_granularity(Granularity::Day)
        .default_rollup(Rollup::None)
        .open()
        .unwrap()
}

proptest! {
    /// put(bm); put(bm) ≡ put(bm)
    #[test]
    fn idempotent_put(bm in arb_bitmap(10000, 500)) {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);

        s.put_bitmap("x", Period::Day(2026, 3, 11), bm.clone()).unwrap();
        s.put_bitmap("x", Period::Day(2026, 3, 11), bm.clone()).unwrap();
        s.flush().unwrap();

        let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
        prop_assert_eq!(got, bm);
    }

    /// put(bm); remove_bits(bm); compact() → empty
    #[test]
    fn remove_inverse(bm in arb_bitmap(10000, 100)) {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);

        s.put_bitmap("x", Period::Day(2026, 3, 11), bm.clone()).unwrap();
        s.flush().unwrap();

        let ids: Vec<u32> = bm.iter().collect();
        s.remove_bits("x", Period::Day(2026, 3, 11), &ids).unwrap();
        s.flush().unwrap();
        s.compact().unwrap();

        let got = s.get("x", Period::Day(2026, 3, 11)).unwrap();
        prop_assert!(got.is_empty());
    }

    /// Compaction preserves data: get(before) == get(after)
    #[test]
    fn compaction_preserves_data(
        bm1 in arb_bitmap(10000, 200),
        bm2 in arb_bitmap(10000, 200),
    ) {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);

        s.put_bitmap("x", Period::Day(2026, 3, 11), bm1).unwrap();
        s.flush().unwrap();
        s.put_bitmap("x", Period::Day(2026, 3, 11), bm2).unwrap();
        s.flush().unwrap();

        let before = s.get("x", Period::Day(2026, 3, 11)).unwrap();
        s.compact().unwrap();
        let after = s.get("x", Period::Day(2026, 3, 11)).unwrap();

        prop_assert_eq!(before, after);
    }

    /// remove_bits(a); remove_bits(b) ≡ remove_bits(a ∪ b)
    #[test]
    fn delta_commutativity(
        data in arb_bitmap(10000, 300),
        del_a in arb_bitmap(10000, 50),
        del_b in arb_bitmap(10000, 50),
    ) {
        // Store 1: remove a then b
        let dir1 = TempDir::new().unwrap();
        let s1 = store(&dir1);
        s1.put_bitmap("x", Period::Day(2026, 3, 11), data.clone()).unwrap();
        s1.flush().unwrap();
        let ids_a: Vec<u32> = del_a.iter().collect();
        let ids_b: Vec<u32> = del_b.iter().collect();
        s1.remove_bits("x", Period::Day(2026, 3, 11), &ids_a).unwrap();
        s1.remove_bits("x", Period::Day(2026, 3, 11), &ids_b).unwrap();

        // Store 2: remove a ∪ b at once
        let dir2 = TempDir::new().unwrap();
        let s2 = store(&dir2);
        s2.put_bitmap("x", Period::Day(2026, 3, 11), data).unwrap();
        s2.flush().unwrap();
        let combined: Vec<u32> = (&del_a | &del_b).iter().collect();
        s2.remove_bits("x", Period::Day(2026, 3, 11), &combined).unwrap();

        let r1 = s1.get("x", Period::Day(2026, 3, 11)).unwrap();
        let r2 = s2.get("x", Period::Day(2026, 3, 11)).unwrap();
        prop_assert_eq!(r1, r2);
    }

    /// Rollup consistency: OR(all hourly children) == parent day (SPEC §18.3)
    #[test]
    fn rollup_consistency(
        bm1 in arb_bitmap(10000, 100),
        bm2 in arb_bitmap(10000, 100),
        bm3 in arb_bitmap(10000, 100),
    ) {
        let dir = TempDir::new().unwrap();
        let s = InoxSet::builder()
            .path(dir.path().join("data"))
            .open()
            .unwrap();
        s.register_event("active", Granularity::Hour, Rollup::Auto).unwrap();

        s.put_bitmap("active", Period::Hour(2026, 3, 11, 10), bm1.clone()).unwrap();
        s.put_bitmap("active", Period::Hour(2026, 3, 11, 11), bm2.clone()).unwrap();
        s.put_bitmap("active", Period::Hour(2026, 3, 11, 12), bm3.clone()).unwrap();
        s.flush().unwrap();

        let day = s.get("active", Period::Day(2026, 3, 11)).unwrap();
        let expected = &(&bm1 | &bm2) | &bm3;
        prop_assert_eq!(day, expected);
    }

    /// Compaction clears all delta parts
    #[test]
    fn compaction_clears_deltas(
        data in arb_bitmap(10000, 200),
        del in arb_bitmap(10000, 50),
    ) {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);

        s.put_bitmap("x", Period::Day(2026, 3, 11), data).unwrap();
        s.flush().unwrap();

        let ids: Vec<u32> = del.iter().collect();
        s.remove_bits("x", Period::Day(2026, 3, 11), &ids).unwrap();
        s.flush().unwrap();

        s.compact().unwrap();

        // After compaction, no delta parts should remain (Fix 5: use public API)
        let health = s.health().unwrap();
        prop_assert_eq!(health.total_delta_parts, 0);
    }
}

fn store_rollup_day(dir: &TempDir) -> InoxSet {
    InoxSet::builder()
        .path(dir.path().join("data"))
        .default_granularity(Granularity::Day)
        .default_rollup(Rollup::Auto)
        .open()
        .unwrap()
}

/// All periods of a Day range plus its rollup ancestors, for equivalence checks.
fn day_range_and_ancestors(start_day: u8, len: usize) -> (Vec<Period>, Vec<Period>) {
    let mut buckets = Vec::new();
    let (mut m, mut d) = (3u8, start_day);
    for _ in 0..len {
        buckets.push(Period::Day(2026, m, d));
        d += 1;
        if (m == 3 && d > 31) || (m == 4 && d > 30) {
            m += 1;
            d = 1;
        }
    }
    let ancestors = vec![
        Period::Month(2026, 3),
        Period::Month(2026, 4),
        Period::Year(2026),
    ];
    (buckets, ancestors)
}

proptest! {
    /// put_bitmap_range ≡ looped put_bitmap, for every bucket and ancestor,
    /// before and after flush.
    #[test]
    fn range_put_equiv_looped_put(
        bm in arb_bitmap(10_000, 200),
        start_day in 20u8..=28,
        len in 1usize..=14,
    ) {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let a = store_rollup_day(&dir_a);
        let b = store_rollup_day(&dir_b);
        let (buckets, ancestors) = day_range_and_ancestors(start_day, len);

        a.put_bitmap_range("ev", buckets[0], *buckets.last().unwrap(), bm.clone()).unwrap();
        for p in &buckets {
            b.put_bitmap("ev", *p, bm.clone()).unwrap();
        }

        for p in buckets.iter().chain(ancestors.iter()) {
            prop_assert_eq!(
                a.get("ev", *p).unwrap(),
                b.get("ev", *p).unwrap(),
                "pre-flush divergence at {:?}", p
            );
        }
        a.flush().unwrap();
        b.flush().unwrap();
        for p in buckets.iter().chain(ancestors.iter()) {
            prop_assert_eq!(
                a.get("ev", *p).unwrap(),
                b.get("ev", *p).unwrap(),
                "post-flush divergence at {:?}", p
            );
        }
    }

    /// remove_bits_range ≡ looped remove_bits after identical seeding.
    #[test]
    fn range_remove_equiv_looped_remove(
        bm in arb_bitmap(10_000, 200),
        removed in arb_bitmap(10_000, 100),
        start_day in 20u8..=28,
        len in 1usize..=14,
    ) {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let a = store_rollup_day(&dir_a);
        let b = store_rollup_day(&dir_b);
        let (buckets, ancestors) = day_range_and_ancestors(start_day, len);
        let (first, last) = (buckets[0], *buckets.last().unwrap());
        let ids: Vec<u32> = removed.iter().collect();

        a.put_bitmap_range("ev", first, last, bm.clone()).unwrap();
        b.put_bitmap_range("ev", first, last, bm.clone()).unwrap();
        a.flush().unwrap();
        b.flush().unwrap();

        if !ids.is_empty() {
            a.remove_bits_range("ev", first, last, &ids).unwrap();
            for p in &buckets {
                b.remove_bits("ev", *p, &ids).unwrap();
            }
        }

        for p in buckets.iter().chain(ancestors.iter()) {
            prop_assert_eq!(
                a.get("ev", *p).unwrap(),
                b.get("ev", *p).unwrap(),
                "divergence at {:?}", p
            );
        }
    }
}
