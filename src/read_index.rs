//! In-memory read index for zero-redb-lookup queries.
//!
//! The [`ReadIndex`] caches catalog metadata in a `HashMap` so that hot-path
//! operations like [`find_memberships`](crate::InoxSet::find_memberships) and
//! [`contains_bit`](crate::InoxSet::contains_bit) can skip redb B-tree
//! traversals entirely.
//!
//! The index is built at startup from the catalog and rebuilt atomically on
//! every flush or compaction via [`ArcSwap`](arc_swap::ArcSwap).

use std::collections::HashMap;
use std::path::PathBuf;

use crate::catalog::Catalog;
use crate::period::parse_period_key;
use crate::types::Period;

/// Cached part location — file path for a single part.
#[derive(Debug, Clone)]
pub(crate) struct PartLoc {
    pub file_path: PathBuf,
}

/// Per-period cached metadata.
#[derive(Debug, Clone, Default)]
pub(crate) struct PeriodEntry {
    pub data_parts: Vec<PartLoc>,
    pub delta_parts: Vec<PartLoc>,
}

/// In-memory read index, replacing per-query redb lookups.
///
/// Built once at startup, then swapped atomically on flush/compact.
/// All reads go through `Arc<ReadIndex>` loaded from `ArcSwap` — lock-free.
#[derive(Debug, Default)]
pub(crate) struct ReadIndex {
    /// `(event_name, period)` → cached part locations.
    pub periods: HashMap<(String, Period), PeriodEntry>,

    /// Event names (cached to avoid redb list_events).
    pub event_names: Vec<String>,
}

impl ReadIndex {
    /// Builds a fresh [`ReadIndex`] from the catalog.
    ///
    /// Reads all events, period keys, part IDs, and part metadata in a
    /// minimal number of redb transactions, then constructs the in-memory
    /// index.
    pub fn build(catalog: &Catalog) -> crate::Result<Self> {
        let events = catalog.list_events()?;
        let event_names: Vec<String> = events.iter().map(|e| e.name.clone()).collect();

        let mut periods: HashMap<(String, Period), PeriodEntry> = HashMap::new();

        for ev in &events {
            let keys = catalog.period_keys_for_event(&ev.name)?;

            for cat_key in &keys {
                // Parse period from catalog key "event/gran/period_key".
                let period = match cat_key.splitn(3, '/').nth(2) {
                    Some(pk) => match parse_period_key(pk) {
                        Some(p) => p,
                        None => continue,
                    },
                    None => continue,
                };

                let mut entry = PeriodEntry::default();

                // Resolve data part file paths.
                let data_ids = catalog.get_period_parts(cat_key)?;
                for pid in data_ids {
                    if let Some(part) = catalog.get_part(pid)? {
                        entry.data_parts.push(PartLoc {
                            file_path: part.file_path,
                        });
                    }
                }

                // Resolve delta part file paths.
                let delta_ids = catalog.get_period_deltas(cat_key)?;
                for pid in delta_ids {
                    if let Some(part) = catalog.get_part(pid)? {
                        entry.delta_parts.push(PartLoc {
                            file_path: part.file_path,
                        });
                    }
                }

                periods.insert((ev.name.clone(), period), entry);
            }
        }

        Ok(Self {
            periods,
            event_names,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn build_empty_catalog() {
        let dir = TempDir::new().unwrap();
        let cat = Catalog::open(dir.path().join("catalog.redb")).unwrap();
        let idx = ReadIndex::build(&cat).unwrap();
        assert!(idx.periods.is_empty());
        assert!(idx.event_names.is_empty());
    }
}
