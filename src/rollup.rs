//! Rollup engine — OR-propagation of bitmaps from fine to coarser granularities.
//!
//! When a caller writes a bitmap at a fine granularity (e.g. [`Granularity::Hour`])
//! and the event's [`Rollup`] strategy is [`Rollup::Auto`], the rollup engine
//! propagates the bitmap upward through the ancestor chain so that every coarser
//! period also reflects the new data.
//!
//! # How it works
//!
//! Given an hourly write for `2026-03-11T14`, the rollup engine ORs the same
//! bitmap into:
//!
//! - `Day(2026, 3, 11)` — if [`Granularity::Day`] is in the rollup chain
//! - `Month(2026, 3)` — if [`Granularity::Month`] is in the rollup chain
//! - `Year(2026)` — if [`Granularity::Year`] is in the rollup chain
//!
//! The original period is **not** touched by these functions — the caller is
//! responsible for OR-ing into the source period before calling into this module.
//!
//! Delta rollup mirrors the same logic but operates on the [`MemPart`] delta
//! map, propagating tombstone bits to coarser periods so that deletes are
//! correctly reflected at every granularity level.

use roaring::RoaringBitmap;

use crate::mempart::MemPart;
use crate::types::{EventConfig, Period, Rollup};

/// OR-propagate `bitmap` into every coarser ancestor period recorded in
/// `config.rollup_chain`.
///
/// This function is a no-op when `config.rollup != Rollup::Auto`.  It does
/// **not** touch the original `period`; that write is the caller's
/// responsibility.
///
/// # Arguments
///
/// * `mempart` — the in-memory write buffer to mutate.
/// * `config` — event configuration, including the rollup strategy and chain.
/// * `period` — the source period whose ancestors should be updated.
/// * `bitmap` — the bitmap to OR into each ancestor.
///
/// # Example
///
/// ```rust
/// use roaring::RoaringBitmap;
/// use inoxset::mempart::MemPart;
/// use inoxset::rollup::apply_rollup;
/// use inoxset::types::{EventConfig, Granularity, Period, Rollup};
///
/// let config = EventConfig::new("active".into(), Granularity::Hour, Rollup::Auto);
/// let mut mp = MemPart::new();
/// let mut bm = RoaringBitmap::new();
/// bm.insert(42);
///
/// // First, OR into the source period (caller's responsibility).
/// mp.or_bitmap(&config.name, Period::Hour(2026, 3, 11, 14), &bm);
/// // Then propagate to coarser periods.
/// apply_rollup(&mut mp, &config, &Period::Hour(2026, 3, 11, 14), &bm);
///
/// assert!(mp.get_bitmap("active", &Period::Day(2026, 3, 11)).is_some());
/// assert!(mp.get_bitmap("active", &Period::Month(2026, 3)).is_some());
/// assert!(mp.get_bitmap("active", &Period::Year(2026)).is_some());
/// ```
pub fn apply_rollup(
    mempart: &mut MemPart,
    config: &EventConfig,
    period: &Period,
    bitmap: &RoaringBitmap,
) {
    if config.rollup != Rollup::Auto {
        return;
    }
    for ancestor in period.ancestors() {
        if config.rollup_chain.contains(&ancestor.granularity()) {
            mempart.or_bitmap(&config.name, ancestor, bitmap);
        }
    }
}

/// OR-propagate a delete-delta `bitmap` into every coarser ancestor period
/// recorded in `config.rollup_chain`.
///
/// This is the delta counterpart to [`apply_rollup`]: it propagates tombstone
/// bits through the ancestor chain so that coarser-granularity bitmaps also
/// reflect deletes during compaction.
///
/// This function is a no-op when `config.rollup != Rollup::Auto`.
///
/// # Arguments
///
/// * `mempart` — the in-memory write buffer to mutate.
/// * `config` — event configuration, including the rollup strategy and chain.
/// * `period` — the source period whose ancestors should receive the delta.
/// * `delta` — the tombstone bitmap to OR into each ancestor's delta map.
pub fn apply_rollup_delta(
    mempart: &mut MemPart,
    config: &EventConfig,
    period: &Period,
    delta: &RoaringBitmap,
) {
    if config.rollup != Rollup::Auto {
        return;
    }
    for ancestor in period.ancestors() {
        if config.rollup_chain.contains(&ancestor.granularity()) {
            mempart.or_delta(&config.name, ancestor, delta);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EventConfig, Granularity, Period, Rollup};
    use roaring::RoaringBitmap;

    fn bitmap_with(ids: &[u32]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for &id in ids {
            bm.insert(id);
        }
        bm
    }

    #[test]
    fn rollup_hour_propagates_to_day_month_year() {
        let config = EventConfig::new("active".into(), Granularity::Hour, Rollup::Auto);
        let mut mp = MemPart::new();
        let bm = bitmap_with(&[1, 2, 3]);
        let period = Period::Hour(2026, 3, 11, 14);

        // Simulate the caller OR-ing into the source period first.
        mp.or_bitmap(&config.name, period, &bm);
        apply_rollup(&mut mp, &config, &period, &bm);

        // Hour itself: 3 bits.
        let hour = mp
            .get_bitmap("active", &Period::Hour(2026, 3, 11, 14))
            .unwrap();
        assert_eq!(hour.len(), 3);

        // Day must have been populated.
        let day = mp.get_bitmap("active", &Period::Day(2026, 3, 11)).unwrap();
        assert_eq!(day.len(), 3);
        assert!(day.contains(1) && day.contains(2) && day.contains(3));

        // Month must have been populated.
        let month = mp.get_bitmap("active", &Period::Month(2026, 3)).unwrap();
        assert_eq!(month.len(), 3);

        // Year must have been populated.
        let year = mp.get_bitmap("active", &Period::Year(2026)).unwrap();
        assert_eq!(year.len(), 3);
    }

    #[test]
    fn rollup_none_does_nothing() {
        let config = EventConfig::new("active".into(), Granularity::Hour, Rollup::None);
        let mut mp = MemPart::new();
        let bm = bitmap_with(&[42]);
        let period = Period::Hour(2026, 3, 11, 14);

        mp.or_bitmap(&config.name, period, &bm);
        apply_rollup(&mut mp, &config, &period, &bm);

        // No ancestor periods should have been written.
        assert!(mp.get_bitmap("active", &Period::Day(2026, 3, 11)).is_none());
        assert!(mp.get_bitmap("active", &Period::Month(2026, 3)).is_none());
        assert!(mp.get_bitmap("active", &Period::Year(2026)).is_none());
    }

    #[test]
    fn rollup_static_does_nothing() {
        // Static granularity forces Rollup::None internally.
        let config = EventConfig::new("geo".into(), Granularity::None, Rollup::Auto);
        let mut mp = MemPart::new();
        let bm = bitmap_with(&[7]);

        mp.or_bitmap(&config.name, Period::Static, &bm);
        apply_rollup(&mut mp, &config, &Period::Static, &bm);

        // Static has no ancestors; rollup chain is also empty.
        // Only the static entry itself exists.
        assert!(mp.get_bitmap("geo", &Period::Static).is_some());
        // Nothing else should exist.
        assert_eq!(mp.bitmaps.len(), 1);
    }

    #[test]
    fn rollup_day_propagates_to_month_year() {
        let config = EventConfig::new("purchase".into(), Granularity::Day, Rollup::Auto);
        let mut mp = MemPart::new();
        let bm = bitmap_with(&[10, 20]);
        let period = Period::Day(2026, 3, 11);

        mp.or_bitmap(&config.name, period, &bm);
        apply_rollup(&mut mp, &config, &period, &bm);

        // Day should exist (written by caller).
        let day = mp
            .get_bitmap("purchase", &Period::Day(2026, 3, 11))
            .unwrap();
        assert_eq!(day.len(), 2);

        // Month should be populated by rollup.
        let month = mp.get_bitmap("purchase", &Period::Month(2026, 3)).unwrap();
        assert_eq!(month.len(), 2);

        // Year should be populated by rollup.
        let year = mp.get_bitmap("purchase", &Period::Year(2026)).unwrap();
        assert_eq!(year.len(), 2);

        // Hour should NOT exist — Day has no hour ancestor.
        assert!(mp
            .get_bitmap("purchase", &Period::Hour(2026, 3, 11, 0))
            .is_none());
    }

    #[test]
    fn rollup_delta_propagates() {
        let config = EventConfig::new("active".into(), Granularity::Hour, Rollup::Auto);
        let mut mp = MemPart::new();
        let delta = bitmap_with(&[99, 100]);
        let period = Period::Hour(2026, 3, 11, 8);

        // Simulate the caller writing the source-period delta first.
        mp.or_delta(&config.name, period, &delta);
        apply_rollup_delta(&mut mp, &config, &period, &delta);

        // The delta should propagate to Day, Month, Year.
        let day_delta = mp.get_delta("active", &Period::Day(2026, 3, 11)).unwrap();
        assert!(day_delta.contains(99) && day_delta.contains(100));

        let month_delta = mp.get_delta("active", &Period::Month(2026, 3)).unwrap();
        assert_eq!(month_delta.len(), 2);

        let year_delta = mp.get_delta("active", &Period::Year(2026)).unwrap();
        assert_eq!(year_delta.len(), 2);
    }

    #[test]
    fn rollup_accumulates_across_multiple_hours() {
        let config = EventConfig::new("ev".into(), Granularity::Hour, Rollup::Auto);
        let mut mp = MemPart::new();

        // Write hour 0: user 1
        let bm0 = bitmap_with(&[1]);
        mp.or_bitmap(&config.name, Period::Hour(2026, 1, 1, 0), &bm0);
        apply_rollup(&mut mp, &config, &Period::Hour(2026, 1, 1, 0), &bm0);

        // Write hour 1: user 2
        let bm1 = bitmap_with(&[2]);
        mp.or_bitmap(&config.name, Period::Hour(2026, 1, 1, 1), &bm1);
        apply_rollup(&mut mp, &config, &Period::Hour(2026, 1, 1, 1), &bm1);

        // Day should have both users.
        let day = mp.get_bitmap("ev", &Period::Day(2026, 1, 1)).unwrap();
        assert!(day.contains(1) && day.contains(2));
        assert_eq!(day.len(), 2);
    }
}
