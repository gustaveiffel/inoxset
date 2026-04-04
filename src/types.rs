//! Core enums and types for the inoxset storage engine.
//!
//! This module defines the foundational data model used by every other module
//! in the crate: time granularity, time periods, rollup strategy, part metadata,
//! and operational health snapshots.

use std::path::PathBuf;

/// Time granularity for period-based storage.
///
/// Variants are ordered from coarsest (`None`) to finest (`Year` is actually
/// the coarsest time granularity, `Hour` the finest temporal one). The derived
/// `PartialOrd`/`Ord` implementation uses declaration order, so `None < Hour < Day
/// < Month < Year`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Granularity {
    /// Static set with no time dimension. Data is stored once and never rolled up.
    None,
    /// Hourly time bucket.
    Hour,
    /// Daily time bucket.
    Day,
    /// Monthly time bucket.
    Month,
    /// Yearly time bucket.
    Year,
}

impl Granularity {
    /// Returns a stable byte representation suitable for on-disk encoding.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Hour => 1,
            Self::Day => 2,
            Self::Month => 3,
            Self::Year => 4,
        }
    }

    /// Converts a byte previously produced by [`as_u8`](Self::as_u8) back to a
    /// `Granularity`. Returns `None` for unknown values.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Hour),
            2 => Some(Self::Day),
            3 => Some(Self::Month),
            4 => Some(Self::Year),
            _ => None,
        }
    }

    /// Returns the directory-name segment used in the flat disk layout.
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
            Self::Year => "year",
        }
    }
}

/// A time period identifying a storage bucket.
///
/// Each variant encodes the fields needed to locate data on disk.  `Static` is
/// used for time-independent sets (e.g. geo lookups).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Period {
    /// Time-independent set; stored once with no rollup.
    Static,
    /// Hourly bucket: `(year, month, day, hour)`.
    Hour(u16, u8, u8, u8),
    /// Daily bucket: `(year, month, day)`.
    Day(u16, u8, u8),
    /// Monthly bucket: `(year, month)`.
    Month(u16, u8),
    /// Yearly bucket: `(year)`.
    Year(u16),
}

/// Rollup strategy for an event.
///
/// When set to [`Auto`](Self::Auto), inoxset propagates bitmaps from the finest
/// granularity up to `Year` on every write.  [`None`](Self::None) stores data
/// only at the finest granularity configured for the event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rollup {
    /// OR-propagate from fine to coarse granularity on write.
    Auto,
    /// Store only at the finest configured granularity; no coarse-grain copies.
    None,
}

/// Whether a part file contains additive data or tombstone deltas.
///
/// `Data` parts hold positive (union) bitmaps; `Delta` parts hold bit-masks
/// that are subtracted during compaction to implement deletes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartKind {
    /// Additive bitmap data.
    Data,
    /// Tombstone / delete delta.
    Delta,
}

/// Lifecycle state of a time period.
///
/// The state machine is: `Open` → `Closed` → `Compacted` → `Dropped`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodState {
    /// Actively accepting writes.
    Open,
    /// No longer accepting writes; eligible for compaction.
    Closed,
    /// Parts have been merged and deltas applied; read-optimised.
    Compacted,
    /// All data has been evicted; the period is logically gone.
    Dropped,
}

impl PeriodState {
    /// Returns a stable byte representation suitable for on-disk / catalog encoding.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Open => 0,
            Self::Closed => 1,
            Self::Compacted => 2,
            Self::Dropped => 3,
        }
    }

    /// Converts a byte previously produced by [`as_u8`](Self::as_u8) back to a
    /// `PeriodState`. Returns `None` for unknown values.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Open),
            1 => Some(Self::Closed),
            2 => Some(Self::Compacted),
            3 => Some(Self::Dropped),
            _ => None,
        }
    }
}

/// Configuration for a registered event type.
///
/// Constructed via [`EventConfig::new`]; the constructor enforces the invariant
/// that `Granularity::None` always implies `Rollup::None` and an empty
/// `rollup_chain`.
#[derive(Debug, Clone)]
pub struct EventConfig {
    /// Logical name of the event (e.g. `"active"`, `"purchase"`).
    pub name: String,
    /// The finest time granularity at which data is written.
    pub finest_granularity: Granularity,
    /// The effective rollup strategy (may differ from the requested one when
    /// `finest_granularity` is [`Granularity::None`]).
    pub rollup: Rollup,
    /// Ordered list of granularities that participate in rollup, from finest to
    /// coarsest.  Empty when rollup is [`Rollup::None`] or granularity is
    /// [`Granularity::None`].
    pub rollup_chain: Vec<Granularity>,
}

impl EventConfig {
    /// Creates a new `EventConfig`, enforcing consistency between granularity and
    /// rollup strategy.
    ///
    /// If `finest_granularity` is [`Granularity::None`] the rollup is forced to
    /// [`Rollup::None`] regardless of the `rollup` argument.
    pub fn new(name: String, finest_granularity: Granularity, rollup: Rollup) -> Self {
        // Static sets cannot be rolled up.
        let rollup = if finest_granularity == Granularity::None {
            Rollup::None
        } else {
            rollup
        };

        let rollup_chain = if rollup == Rollup::Auto {
            match finest_granularity {
                Granularity::None => vec![],
                Granularity::Hour => vec![
                    Granularity::Hour,
                    Granularity::Day,
                    Granularity::Month,
                    Granularity::Year,
                ],
                Granularity::Day => {
                    vec![Granularity::Day, Granularity::Month, Granularity::Year]
                }
                Granularity::Month => vec![Granularity::Month, Granularity::Year],
                Granularity::Year => vec![Granularity::Year],
            }
        } else {
            vec![]
        };

        Self {
            name,
            finest_granularity,
            rollup,
            rollup_chain,
        }
    }
}

/// Metadata for an immutable part file on disk.
///
/// Parts are the atomic storage units written by the mempart flush and read
/// during queries and compaction.
#[derive(Debug, Clone)]
pub struct Part {
    /// Monotonically increasing identifier assigned by the catalog.
    pub part_id: u64,
    /// Whether this part holds positive data or tombstone deltas.
    pub kind: PartKind,
    /// The event this part belongs to.
    pub event: String,
    /// The time period this part covers.
    pub period: Period,
    /// Absolute path to the part file on disk.
    pub file_path: PathBuf,
    /// On-disk size in bytes.
    pub size_bytes: u64,
    /// Number of distinct user IDs (set bits) in the bitmap.
    pub cardinality: u64,
    /// Unix timestamp (seconds) when the part was written.
    pub created_at: u64,
    /// Compaction level; `0` = freshly flushed, higher = more compacted.
    pub level: u8,
}

/// Operational health snapshot returned by the engine's health-check method.
///
/// All fields are point-in-time observations and may race with concurrent
/// writes in a live system.
#[derive(Debug, Clone)]
pub struct Health {
    /// `true` if the catalog database opened and responded successfully.
    pub catalog_ok: bool,
    /// Current in-memory (mempart) buffer size in bytes.
    pub mempart_size_bytes: u64,
    /// Number of entries currently held in the mempart buffer.
    pub mempart_entries: u32,
    /// Total number of registered event types.
    pub total_events: u32,
    /// Total number of data part files tracked by the catalog.
    pub total_data_parts: u64,
    /// Total number of delta part files tracked by the catalog.
    pub total_delta_parts: u64,
    /// Number of periods in the `Open` state.
    pub open_periods: u32,
    /// Number of periods in the `Closed` state.
    pub closed_periods: u32,
    /// Number of periods in the `Compacted` state.
    pub compacted_periods: u32,
    /// Number of periods that have enough parts to warrant compaction.
    pub periods_needing_compaction: u32,
    /// Total disk usage across all part files in bytes.
    pub disk_usage_bytes: u64,
}

/// Statistics returned after a compaction run.
#[derive(Debug, Clone, Default)]
pub struct CompactStats {
    /// Number of time periods that were compacted.
    pub periods_compacted: u32,
    /// Number of individual part files that were merged together.
    pub parts_merged: u32,
    /// Number of delta (tombstone) parts whose bits were applied and discarded.
    pub deltas_applied: u32,
    /// Total bytes freed from disk after compaction.
    pub bytes_reclaimed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn granularity_ordering() {
        assert!(Granularity::None < Granularity::Hour);
        assert!(Granularity::Hour < Granularity::Day);
        assert!(Granularity::Day < Granularity::Month);
        assert!(Granularity::Month < Granularity::Year);
    }

    #[test]
    fn period_equality_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Period::Hour(2026, 3, 11, 14));
        assert!(set.contains(&Period::Hour(2026, 3, 11, 14)));
        assert!(!set.contains(&Period::Hour(2026, 3, 11, 15)));
        assert_eq!(Period::Static, Period::Static);
        assert_ne!(Period::Static, Period::Day(2026, 3, 11));
    }

    #[test]
    fn event_config_rollup_chain_hour_auto() {
        let ec = EventConfig::new("active".into(), Granularity::Hour, Rollup::Auto);
        assert_eq!(
            ec.rollup_chain,
            vec![
                Granularity::Hour,
                Granularity::Day,
                Granularity::Month,
                Granularity::Year
            ]
        );
    }

    #[test]
    fn event_config_static_forces_no_rollup() {
        let ec = EventConfig::new("geo".into(), Granularity::None, Rollup::Auto);
        assert_eq!(ec.rollup, Rollup::None);
        assert!(ec.rollup_chain.is_empty());
    }

    #[test]
    fn event_config_day_auto() {
        let ec = EventConfig::new("x".into(), Granularity::Day, Rollup::Auto);
        assert_eq!(
            ec.rollup_chain,
            vec![Granularity::Day, Granularity::Month, Granularity::Year]
        );
    }

    #[test]
    fn event_config_month_auto() {
        let ec = EventConfig::new("x".into(), Granularity::Month, Rollup::Auto);
        assert_eq!(ec.rollup_chain, vec![Granularity::Month, Granularity::Year]);
    }

    #[test]
    fn event_config_year_auto() {
        let ec = EventConfig::new("x".into(), Granularity::Year, Rollup::Auto);
        assert_eq!(ec.rollup_chain, vec![Granularity::Year]);
    }

    #[test]
    fn event_config_none_rollup() {
        let ec = EventConfig::new("x".into(), Granularity::Hour, Rollup::None);
        assert!(ec.rollup_chain.is_empty());
    }

    #[test]
    fn granularity_u8_roundtrip() {
        for g in [
            Granularity::None,
            Granularity::Hour,
            Granularity::Day,
            Granularity::Month,
            Granularity::Year,
        ] {
            assert_eq!(Granularity::from_u8(g.as_u8()), Some(g));
        }
    }

    #[test]
    fn period_state_u8_roundtrip() {
        for s in [
            PeriodState::Open,
            PeriodState::Closed,
            PeriodState::Compacted,
            PeriodState::Dropped,
        ] {
            assert_eq!(PeriodState::from_u8(s.as_u8()), Some(s));
        }
    }
}
