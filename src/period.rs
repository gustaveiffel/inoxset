//! Period methods: key formatting, granularity mapping, parent traversal, and
//! closed-period detection.
//!
//! This module extends [`Period`] with the domain logic needed by every other
//! module in the crate. It also provides the [`catalog_key`] helper and the
//! [`PeriodAncestors`] iterator.

use crate::types::{Granularity, Period};

impl Period {
    /// Returns the string key for this period, used in file paths and catalog
    /// keys.
    ///
    /// | Variant | Format | Example |
    /// |---------|--------|---------|
    /// | `Static` | `"_static"` | `"_static"` |
    /// | `Hour(y, m, d, h)` | `"YYYY-MM-DDTHH"` | `"2026-03-11T14"` |
    /// | `Day(y, m, d)` | `"YYYY-MM-DD"` | `"2026-03-11"` |
    /// | `Month(y, m)` | `"YYYY-MM"` | `"2026-03"` |
    /// | `Year(y)` | `"YYYY"` | `"2026"` |
    pub fn key(&self) -> String {
        match self {
            Period::Static => "_static".to_string(),
            Period::Hour(y, m, d, h) => format!("{y:04}-{m:02}-{d:02}T{h:02}"),
            Period::Day(y, m, d) => format!("{y:04}-{m:02}-{d:02}"),
            Period::Month(y, m) => format!("{y:04}-{m:02}"),
            Period::Year(y) => format!("{y:04}"),
        }
    }

    /// Returns the [`Granularity`] associated with this period variant.
    pub fn granularity(&self) -> Granularity {
        match self {
            Period::Static => Granularity::None,
            Period::Hour(..) => Granularity::Hour,
            Period::Day(..) => Granularity::Day,
            Period::Month(..) => Granularity::Month,
            Period::Year(..) => Granularity::Year,
        }
    }

    /// Returns the immediate parent period (one level coarser), or `None` for
    /// [`Period::Year`] and [`Period::Static`].
    ///
    /// ```text
    /// Hour → Day → Month → Year → None
    /// Static → None
    /// ```
    pub fn parent(&self) -> Option<Period> {
        match *self {
            Period::Static | Period::Year(_) => None,
            Period::Hour(y, m, d, _) => Some(Period::Day(y, m, d)),
            Period::Day(y, m, _) => Some(Period::Month(y, m)),
            Period::Month(y, _) => Some(Period::Year(y)),
        }
    }

    /// Returns an iterator over all ancestor periods from immediate parent up
    /// to [`Period::Year`].
    ///
    /// [`Period::Static`] and [`Period::Year`] produce empty iterators.
    pub fn ancestors(&self) -> PeriodAncestors {
        PeriodAncestors {
            current: self.parent(),
        }
    }

    /// Returns `true` if this period's time window has fully elapsed relative
    /// to `now_unix` (seconds since the Unix epoch).
    ///
    /// [`Period::Static`] is **never** closed — it holds time-independent data
    /// that has no expiry.
    pub fn is_closed(&self, now_unix: u64) -> bool {
        match self.close_boundary_unix() {
            Some(boundary) => now_unix >= boundary,
            None => false,
        }
    }

    /// Returns the Unix timestamp (seconds) at which this period becomes
    /// closed, i.e. the first instant *after* the period ends.
    ///
    /// Returns `None` for [`Period::Static`], which never closes.
    fn close_boundary_unix(&self) -> Option<u64> {
        match *self {
            Period::Static => None,
            Period::Hour(y, m, d, h) => {
                Some(datetime_to_unix(y as i32, m as i32, d as i32, h as i32 + 1))
            }
            Period::Day(y, m, d) => Some(datetime_to_unix(y as i32, m as i32, d as i32 + 1, 0)),
            Period::Month(y, m) => {
                let (ny, nm) = if m >= 12 {
                    (y as i32 + 1, 1)
                } else {
                    (y as i32, m as i32 + 1)
                };
                Some(datetime_to_unix(ny, nm, 1, 0))
            }
            Period::Year(y) => Some(datetime_to_unix(y as i32 + 1, 1, 1, 0)),
        }
    }
}

/// Iterator over ancestor [`Period`]s, from immediate parent up to
/// [`Period::Year`].
///
/// Produced by [`Period::ancestors`].
pub struct PeriodAncestors {
    current: Option<Period>,
}

impl Iterator for PeriodAncestors {
    type Item = Period;

    fn next(&mut self) -> Option<Period> {
        let p = self.current?;
        self.current = p.parent();
        Some(p)
    }
}

/// Builds a compound catalog key of the form `"event/gran_dir/period_key"`.
///
/// This key is used as the row identifier in the redb catalog tables.
///
/// # Example
///
/// ```rust
/// use inoxset::period::catalog_key;
/// use inoxset::types::{Granularity, Period};
///
/// let key = catalog_key("active", Granularity::Hour, &Period::Hour(2026, 3, 11, 14));
/// assert_eq!(key, "active/hour/2026-03-11T14");
/// ```
pub fn catalog_key(event: &str, gran: Granularity, period: &Period) -> String {
    format!("{}/{}/{}", event, gran.dir_name(), period.key())
}

/// Returns the number of days in the given month of the given year, correctly
/// accounting for leap years.
///
/// Used internally for date arithmetic and exposed crate-wide so that
/// other modules can share this implementation without duplication.
pub(crate) fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Converts calendar components to a Unix timestamp (seconds since 1970-01-01
/// UTC). Handles overflow in `h`, `d`, and `m` through normalization.
fn datetime_to_unix(y: i32, m: i32, d: i32, h: i32) -> u64 {
    let (y, m, d, h) = normalize(y, m, d, h);
    let days = days_from_civil(y, m as u32, d as u32);
    (days as u64).wrapping_mul(86400) + h as u64 * 3600
}

/// Normalizes potentially out-of-range hour, day, and month components into
/// valid calendar values, carrying over into larger units as needed.
fn normalize(mut y: i32, mut m: i32, mut d: i32, mut h: i32) -> (i32, i32, i32, i32) {
    // Normalize hours → days
    if h >= 24 {
        d += h / 24;
        h %= 24;
    }
    // Normalize months first (needed to know days-in-month correctly)
    loop {
        if m > 12 {
            m -= 12;
            y += 1;
            continue;
        }
        break;
    }
    // Normalize days → months
    loop {
        let dim = days_in_month(y, m as u32) as i32;
        if d <= dim {
            break;
        }
        d -= dim;
        m += 1;
        if m > 12 {
            m -= 12;
            y += 1;
        }
    }
    (y, m, d, h)
}

/// Converts a proleptic Gregorian date to days since 1970-01-01 using
/// Howard Hinnant's civil calendar algorithm.
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let (y, m, d) = (y as i64, m as i64, d as i64);
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Granularity;

    #[test]
    fn key_static() {
        assert_eq!(Period::Static.key(), "_static");
    }

    #[test]
    fn key_hour() {
        assert_eq!(Period::Hour(2026, 3, 11, 14).key(), "2026-03-11T14");
    }

    #[test]
    fn key_day() {
        assert_eq!(Period::Day(2026, 3, 11).key(), "2026-03-11");
    }

    #[test]
    fn key_month() {
        assert_eq!(Period::Month(2026, 3).key(), "2026-03");
    }

    #[test]
    fn key_year() {
        assert_eq!(Period::Year(2026).key(), "2026");
    }

    #[test]
    fn granularity_mapping() {
        assert_eq!(Period::Static.granularity(), Granularity::None);
        assert_eq!(
            Period::Hour(2026, 3, 11, 14).granularity(),
            Granularity::Hour
        );
        assert_eq!(Period::Day(2026, 3, 11).granularity(), Granularity::Day);
        assert_eq!(Period::Month(2026, 3).granularity(), Granularity::Month);
        assert_eq!(Period::Year(2026).granularity(), Granularity::Year);
    }

    #[test]
    fn parent_chain() {
        assert_eq!(
            Period::Hour(2026, 3, 11, 14).parent(),
            Some(Period::Day(2026, 3, 11))
        );
        assert_eq!(
            Period::Day(2026, 3, 11).parent(),
            Some(Period::Month(2026, 3))
        );
        assert_eq!(Period::Month(2026, 3).parent(), Some(Period::Year(2026)));
        assert_eq!(Period::Year(2026).parent(), None);
        assert_eq!(Period::Static.parent(), None);
    }

    #[test]
    fn ancestors_hour() {
        let a: Vec<Period> = Period::Hour(2026, 3, 11, 14).ancestors().collect();
        assert_eq!(
            a,
            vec![
                Period::Day(2026, 3, 11),
                Period::Month(2026, 3),
                Period::Year(2026)
            ]
        );
    }

    #[test]
    fn ancestors_static_empty() {
        let a: Vec<Period> = Period::Static.ancestors().collect();
        assert!(a.is_empty());
    }

    #[test]
    fn catalog_key_format() {
        let key = catalog_key("active", Granularity::Hour, &Period::Hour(2026, 3, 11, 14));
        assert_eq!(key, "active/hour/2026-03-11T14");
    }

    #[test]
    fn is_closed_static_never() {
        assert!(!Period::Static.is_closed(u64::MAX));
    }

    #[test]
    fn is_closed_future_period() {
        let now_2026 = 1_773_500_000; // approx 2026-03-12
        assert!(!Period::Hour(2099, 1, 1, 0).is_closed(now_2026));
    }

    #[test]
    fn is_closed_past_hour() {
        let now_2026 = 1_773_500_000;
        assert!(Period::Hour(2020, 1, 1, 0).is_closed(now_2026));
    }

    #[test]
    fn is_closed_past_day() {
        let now_2026 = 1_773_500_000;
        assert!(Period::Day(2020, 1, 1).is_closed(now_2026));
    }

    #[test]
    fn is_closed_month_boundary() {
        let now_2026 = 1_773_500_000;
        assert!(Period::Month(2025, 12).is_closed(now_2026));
    }

    #[test]
    fn is_closed_year_boundary() {
        let now_2026 = 1_773_500_000;
        assert!(Period::Year(2025).is_closed(now_2026));
        assert!(!Period::Year(2026).is_closed(now_2026));
    }
}
