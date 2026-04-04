//! Dictionary encoding module: bidirectional mapping between external string IDs
//! and compact internal `u32` values.
//!
//! This module uses a **global** dictionary — every external string ID maps to
//! exactly one `u32` across all event types.  There is no per-event namespace;
//! the same external ID always resolves to the same internal integer regardless
//! of which event it is used with.
//!
//! Counters and mappings are stored in a `redb::Database` that the caller
//! supplies directly — this module does **not** go through the
//! [`crate::catalog::Catalog`] struct.
//!
//! # Tables
//!
//! | Constant | Key | Value |
//! |---|---|---|
//! | [`DICT_FWD`] | `external_id` (str) | `u32` internal ID |
//! | [`DICT_REV`] | `u32` internal ID | external ID string |
//! | [`NEXT_DICT_ID`] | `()` (unit) | next available `u32` counter |

use redb::{Database, ReadableTable, TableDefinition};

// ─── Table definitions ────────────────────────────────────────────────────────

/// Forward mapping table: `external_id` → internal `u32`.
pub(crate) const DICT_FWD: TableDefinition<&str, u32> = TableDefinition::new("dict_fwd_v2");

/// Reverse mapping table: internal `u32` → external ID string.
pub(crate) const DICT_REV: TableDefinition<u32, &str> = TableDefinition::new("dict_rev_v2");

/// Global monotonic counter table: `()` → next available `u32`.
pub(crate) const NEXT_DICT_ID: TableDefinition<(), u32> = TableDefinition::new("next_dict_id_v2");

// ─── Helper: read a single forward entry ─────────────────────────────────────

/// Performs a single read-only lookup in the forward table.
///
/// Returns `None` if the key is absent.  Materialises the value before the
/// transaction and table are dropped, avoiding `AccessGuard` lifetime issues.
fn read_fwd(db: &Database, external_id: &str) -> crate::Result<Option<u32>> {
    let rtxn = db.begin_read()?;
    let fwd = rtxn.open_table(DICT_FWD)?;
    let v = fwd.get(external_id)?.map(|g| g.value());
    Ok(v)
}

// ─── Core functions ───────────────────────────────────────────────────────────

/// Ensures that the three dictionary tables exist in `db`.
///
/// This should be called once during database initialisation (e.g. inside a
/// write transaction that also creates other catalog tables).
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn ensure_tables(db: &Database) -> crate::Result<()> {
    let txn = db.begin_write()?;
    {
        txn.open_table(DICT_FWD)?;
        txn.open_table(DICT_REV)?;
        txn.open_table(NEXT_DICT_ID)?;
    }
    txn.commit()?;
    Ok(())
}

/// Returns the internal `u32` for `external_id`, assigning a new one if it has
/// never been seen before.
///
/// The implementation uses a **read-first optimisation**: if the mapping already
/// exists it is returned without acquiring a write lock.  If it is absent a
/// write transaction is opened and the entry is double-checked before inserting,
/// so two concurrent callers cannot assign duplicate IDs.
///
/// IDs are assigned sequentially from a single global counter, starting at `0`.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn assign_or_get(db: &Database, external_id: &str) -> crate::Result<u32> {
    // Phase 1: optimistic read — avoids a write lock on the common path.
    if let Some(id) = read_fwd(db, external_id)? {
        return Ok(id);
    }

    // Phase 2: write transaction with double-check.
    let wtxn = db.begin_write()?;
    let id = {
        let mut fwd = wtxn.open_table(DICT_FWD)?;
        let mut rev = wtxn.open_table(DICT_REV)?;
        let mut ctr = wtxn.open_table(NEXT_DICT_ID)?;

        // Double-check: another writer may have raced us.
        let existing = fwd.get(external_id)?.map(|g| g.value());
        if let Some(id) = existing {
            id
        } else {
            let next = ctr.get(())?.map(|g| g.value()).unwrap_or(0u32);
            fwd.insert(external_id, next)?;
            rev.insert(next, external_id)?;
            ctr.insert((), next + 1)?;
            next
        }
    };
    wtxn.commit()?;
    Ok(id)
}

/// Returns the internal `u32` for `external_id`, or `None` if the external ID
/// has not been registered.
///
/// This is a read-only operation and never modifies the database.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn lookup(db: &Database, external_id: &str) -> crate::Result<Option<u32>> {
    read_fwd(db, external_id)
}

/// Returns the external string ID for `internal_id`, or `None` if the internal
/// ID is not present in the reverse table.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn reverse_lookup(db: &Database, internal_id: u32) -> crate::Result<Option<String>> {
    let rtxn = db.begin_read()?;
    let rev = rtxn.open_table(DICT_REV)?;
    let v = rev.get(internal_id)?.map(|g| g.value().to_owned());
    Ok(v)
}

/// Removes the `external_id` entry from both the forward and reverse tables.
///
/// Returns the internal `u32` that was associated with `external_id`, or `None`
/// if the ID was not registered.  The global counter is **not** decremented;
/// freed IDs are not reused.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn delete(db: &Database, external_id: &str) -> crate::Result<Option<u32>> {
    let wtxn = db.begin_write()?;
    let removed = {
        let mut fwd = wtxn.open_table(DICT_FWD)?;
        let mut rev = wtxn.open_table(DICT_REV)?;

        let existing = fwd.remove(external_id)?.map(|g| g.value());
        if let Some(id) = existing {
            rev.remove(id)?;
            Some(id)
        } else {
            None
        }
    };
    wtxn.commit()?;
    Ok(removed)
}

// ─── Batch operations ─────────────────────────────────────────────────────────

/// Assigns or retrieves internal `u32` IDs for every element of `external_ids`.
///
/// Returns a `Vec<u32>` in the **same order** as the input slice.
///
/// The implementation uses a **two-phase strategy**:
///
/// 1. A single read transaction resolves all IDs that already exist.
/// 2. A single write transaction (opened only when needed) handles the unknown
///    subset, double-checking each entry before insertion.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn batch_assign_or_get(db: &Database, external_ids: &[&str]) -> crate::Result<Vec<u32>> {
    let mut result: Vec<Option<u32>> = vec![None; external_ids.len()];

    // Phase 1: read-only pass — materialize all values before dropping the txn.
    {
        let rtxn = db.begin_read()?;
        let fwd = rtxn.open_table(DICT_FWD)?;
        for (i, &ext) in external_ids.iter().enumerate() {
            result[i] = fwd.get(ext)?.map(|g| g.value());
        }
    }

    // Check if all were resolved.
    let needs_write = result.iter().any(|r| r.is_none());
    if !needs_write {
        return Ok(result.into_iter().flatten().collect());
    }

    // Phase 2: write transaction for unknowns, with double-check.
    let wtxn = db.begin_write()?;
    {
        let mut fwd = wtxn.open_table(DICT_FWD)?;
        let mut rev = wtxn.open_table(DICT_REV)?;
        let mut ctr = wtxn.open_table(NEXT_DICT_ID)?;

        let mut next = ctr.get(())?.map(|g| g.value()).unwrap_or(0u32);

        for (i, &ext) in external_ids.iter().enumerate() {
            if result[i].is_some() {
                continue;
            }

            // Double-check: another writer may have inserted while we waited.
            let existing = fwd.get(ext)?.map(|g| g.value());
            let id = if let Some(existing_id) = existing {
                existing_id
            } else {
                fwd.insert(ext, next)?;
                rev.insert(next, ext)?;
                let assigned = next;
                next += 1;
                assigned
            };
            result[i] = Some(id);
        }

        ctr.insert((), next)?;
    }
    wtxn.commit()?;

    Ok(result.into_iter().flatten().collect())
}

/// Resolves a batch of internal `u32` IDs to their external string
/// representations.
///
/// Returns a `Vec<Option<String>>` in the **same order** as `internal_ids`.
/// An entry is `None` when the corresponding internal ID is not present in the
/// reverse table.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn batch_reverse_lookup(
    db: &Database,
    internal_ids: &[u32],
) -> crate::Result<Vec<Option<String>>> {
    let rtxn = db.begin_read()?;
    let rev = rtxn.open_table(DICT_REV)?;

    let mut result = Vec::with_capacity(internal_ids.len());
    for &id in internal_ids {
        let entry = rev.get(id)?.map(|g| g.value().to_owned());
        result.push(entry);
    }
    Ok(result)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_db() -> (Database, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path().join("dict_test.redb")).unwrap();
        ensure_tables(&db).unwrap();
        (db, dir)
    }

    #[test]
    fn assign_or_get_global() {
        let (db, _dir) = test_db();
        let id0 = assign_or_get(&db, "user-abc-123").unwrap();
        let id1 = assign_or_get(&db, "user-def-456").unwrap();
        let id0_again = assign_or_get(&db, "user-abc-123").unwrap();
        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id0_again, 0);
    }

    #[test]
    fn same_id_regardless_of_usage_context() {
        let (db, _dir) = test_db();
        // The same external string must resolve to the same u32 regardless of
        // which event it is logically associated with at the call site.
        let id_clicks = assign_or_get(&db, "user-1").unwrap();
        let id_views = assign_or_get(&db, "user-1").unwrap();
        assert_eq!(id_clicks, id_views);
        assert_eq!(id_clicks, 0);
    }

    #[test]
    fn batch_assign_or_get_global() {
        let (db, _dir) = test_db();
        let ids = batch_assign_or_get(&db, &["aaa", "bbb", "ccc"]).unwrap();
        assert_eq!(ids, vec![0, 1, 2]);
        let ids2 = batch_assign_or_get(&db, &["bbb", "ddd", "aaa"]).unwrap();
        assert_eq!(ids2, vec![1, 3, 0]);
    }

    #[test]
    fn batch_reverse_lookup_global() {
        let (db, _dir) = test_db();
        batch_assign_or_get(&db, &["alice", "bob", "charlie"]).unwrap();
        let names = batch_reverse_lookup(&db, &[2, 0, 1]).unwrap();
        assert_eq!(
            names,
            vec![
                Some("charlie".to_string()),
                Some("alice".to_string()),
                Some("bob".to_string()),
            ]
        );
        let missing = batch_reverse_lookup(&db, &[99]).unwrap();
        assert_eq!(missing, vec![None]);
    }

    #[test]
    fn delete_removes_both_directions() {
        let (db, _dir) = test_db();
        let id = assign_or_get(&db, "to-delete").unwrap();
        assert_eq!(id, 0);

        // Verify forward and reverse both exist.
        assert_eq!(lookup(&db, "to-delete").unwrap(), Some(0));
        assert_eq!(
            reverse_lookup(&db, 0).unwrap(),
            Some("to-delete".to_string())
        );

        // Delete and confirm both directions are gone.
        let removed = delete(&db, "to-delete").unwrap();
        assert_eq!(removed, Some(0));
        assert_eq!(lookup(&db, "to-delete").unwrap(), None);
        assert_eq!(reverse_lookup(&db, 0).unwrap(), None);

        // Deleting a non-existent entry returns None.
        assert_eq!(delete(&db, "never-existed").unwrap(), None);
    }
}
