//! Dictionary encoding module: bidirectional mapping between external string IDs
//! and compact internal `u32` values.
//!
//! Each event has its own independent ID space, so the same external string can
//! map to different internal integers under different event names.  Counters and
//! mappings are stored in a `redb::Database` that the caller supplies directly —
//! this module does **not** go through the [`crate::catalog::Catalog`] struct.
//!
//! # Tables
//!
//! | Constant | Key | Value |
//! |---|---|---|
//! | [`DICT_FWD`] | `"event\0external_id"` | `u32` internal ID |
//! | [`DICT_REV`] | `"event\0{id:010}"` | external ID string |
//! | [`NEXT_DICT_ID`] | event name | next available `u32` counter |

use redb::{Database, ReadableTable, TableDefinition};

// ─── Table definitions ────────────────────────────────────────────────────────

/// Forward mapping table: `"event\0external_id"` → internal `u32`.
pub(crate) const DICT_FWD: TableDefinition<&str, u32> = TableDefinition::new("dict_fwd");

/// Reverse mapping table: `"event\0{id:010}"` → external ID string.
pub(crate) const DICT_REV: TableDefinition<&str, &str> = TableDefinition::new("dict_rev");

/// Per-event monotonic counter table: event name → next available `u32`.
pub(crate) const NEXT_DICT_ID: TableDefinition<&str, u32> = TableDefinition::new("next_dict_id");

// ─── Key helpers ─────────────────────────────────────────────────────────────

/// Builds a forward-lookup key: `"event\0external_id"`.
///
/// The null byte acts as a separator that cannot appear in valid event names,
/// so there is no ambiguity between the two parts.
fn fwd_key(event: &str, external_id: &str) -> String {
    format!("{event}\0{external_id}")
}

/// Builds a reverse-lookup key: `"event\0{id:010}"`.
///
/// The internal ID is zero-padded to ten digits so that lexicographic order
/// matches numeric order, enabling range scans in the future.
fn rev_key(event: &str, internal_id: u32) -> String {
    format!("{event}\0{internal_id:010}")
}

// ─── Helper: read a single forward entry ─────────────────────────────────────

/// Performs a single read-only lookup in the forward table.
///
/// Returns `None` if the key is absent.  Materialises the value before the
/// transaction and table are dropped, avoiding `AccessGuard` lifetime issues.
fn read_fwd(db: &Database, fk: &str) -> crate::Result<Option<u32>> {
    let rtxn = db.begin_read()?;
    let fwd = rtxn.open_table(DICT_FWD)?;
    let v = fwd.get(fk)?.map(|g| g.value());
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

/// Returns the internal `u32` for `external_id` under `event`, assigning a new
/// one if it has never been seen before.
///
/// The implementation uses a **read-first optimisation**: if the mapping already
/// exists we return it without acquiring a write lock.  If it is absent we open
/// a write transaction and double-check before inserting, so two concurrent
/// callers cannot assign duplicate IDs.
///
/// IDs are assigned sequentially starting at `0`, incremented per event.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn assign_or_get(db: &Database, event: &str, external_id: &str) -> crate::Result<u32> {
    let fk = fwd_key(event, external_id);

    // Phase 1: optimistic read — avoids a write lock on the common path.
    if let Some(id) = read_fwd(db, &fk)? {
        return Ok(id);
    }

    // Phase 2: write transaction with double-check.
    let wtxn = db.begin_write()?;
    let id = {
        let mut fwd = wtxn.open_table(DICT_FWD)?;
        let mut rev = wtxn.open_table(DICT_REV)?;
        let mut ctr = wtxn.open_table(NEXT_DICT_ID)?;

        // Double-check: another writer may have raced us.
        let existing = fwd.get(fk.as_str())?.map(|g| g.value());
        if let Some(id) = existing {
            id
        } else {
            let next = ctr.get(event)?.map(|g| g.value()).unwrap_or(0u32);
            let rk = rev_key(event, next);
            fwd.insert(fk.as_str(), next)?;
            rev.insert(rk.as_str(), external_id)?;
            ctr.insert(event, next + 1)?;
            next
        }
    };
    wtxn.commit()?;
    Ok(id)
}

/// Returns the internal `u32` for `external_id` under `event`, or `None` if
/// the external ID has not been registered.
///
/// This is a read-only operation and never modifies the database.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn lookup(db: &Database, event: &str, external_id: &str) -> crate::Result<Option<u32>> {
    let fk = fwd_key(event, external_id);
    read_fwd(db, &fk)
}

/// Returns the external string ID for `internal_id` under `event`, or `None`
/// if the internal ID is not present in the reverse table.
///
/// # Errors
///
/// Returns an error on any redb I/O failure.
pub fn reverse_lookup(
    db: &Database,
    event: &str,
    internal_id: u32,
) -> crate::Result<Option<String>> {
    let rk = rev_key(event, internal_id);
    let rtxn = db.begin_read()?;
    let rev = rtxn.open_table(DICT_REV)?;
    let v = rev.get(rk.as_str())?.map(|g| g.value().to_owned());
    Ok(v)
}

// ─── Batch operations ─────────────────────────────────────────────────────────

/// Assigns or retrieves internal `u32` IDs for every element of `external_ids`
/// under `event`.
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
pub fn batch_assign_or_get(
    db: &Database,
    event: &str,
    external_ids: &[&str],
) -> crate::Result<Vec<u32>> {
    let mut result: Vec<Option<u32>> = vec![None; external_ids.len()];

    // Phase 1: read-only pass — materialize all values before dropping the txn.
    {
        let rtxn = db.begin_read()?;
        let fwd = rtxn.open_table(DICT_FWD)?;
        for (i, &ext) in external_ids.iter().enumerate() {
            let fk = fwd_key(event, ext);
            result[i] = fwd.get(fk.as_str())?.map(|g| g.value());
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

        let mut next = ctr.get(event)?.map(|g| g.value()).unwrap_or(0u32);

        for (i, &ext) in external_ids.iter().enumerate() {
            if result[i].is_some() {
                continue;
            }
            let fk = fwd_key(event, ext);

            // Double-check: another writer may have inserted while we waited.
            let existing = fwd.get(fk.as_str())?.map(|g| g.value());
            let id = if let Some(existing_id) = existing {
                existing_id
            } else {
                let rk = rev_key(event, next);
                fwd.insert(fk.as_str(), next)?;
                rev.insert(rk.as_str(), ext)?;
                let assigned = next;
                next += 1;
                assigned
            };
            result[i] = Some(id);
        }

        ctr.insert(event, next)?;
    }
    wtxn.commit()?;

    Ok(result.into_iter().flatten().collect())
}

/// Resolves a batch of internal `u32` IDs to their external string
/// representations under `event`.
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
    event: &str,
    internal_ids: &[u32],
) -> crate::Result<Vec<Option<String>>> {
    let rtxn = db.begin_read()?;
    let rev = rtxn.open_table(DICT_REV)?;

    let mut result = Vec::with_capacity(internal_ids.len());
    for &id in internal_ids {
        let rk = rev_key(event, id);
        let entry = rev.get(rk.as_str())?.map(|g| g.value().to_owned());
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
    fn assign_or_get_monotonic() {
        let (db, _dir) = test_db();
        let id1 = assign_or_get(&db, "segment", "user-abc-123").unwrap();
        let id2 = assign_or_get(&db, "segment", "user-def-456").unwrap();
        let id1_again = assign_or_get(&db, "segment", "user-abc-123").unwrap();
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1_again, 0);
    }

    #[test]
    fn batch_assign_or_get_returns_vec() {
        let (db, _dir) = test_db();
        let ids = batch_assign_or_get(&db, "segment", &["aaa", "bbb", "ccc"]).unwrap();
        assert_eq!(ids, vec![0, 1, 2]);
        let ids2 = batch_assign_or_get(&db, "segment", &["bbb", "ddd", "aaa"]).unwrap();
        assert_eq!(ids2, vec![1, 3, 0]);
    }

    #[test]
    fn event_scoping_independent() {
        let (db, _dir) = test_db();
        let id_a = assign_or_get(&db, "clicks", "user-1").unwrap();
        let id_b = assign_or_get(&db, "views", "user-1").unwrap();
        assert_eq!(id_a, 0);
        assert_eq!(id_b, 0);
    }

    #[test]
    fn batch_reverse_lookup_resolves() {
        let (db, _dir) = test_db();
        batch_assign_or_get(&db, "ev", &["alice", "bob", "charlie"]).unwrap();
        let names = batch_reverse_lookup(&db, "ev", &[2, 0, 1]).unwrap();
        assert_eq!(
            names,
            vec![
                Some("charlie".to_string()),
                Some("alice".to_string()),
                Some("bob".to_string()),
            ]
        );
        let names2 = batch_reverse_lookup(&db, "ev", &[99]).unwrap();
        assert_eq!(names2, vec![None]);
    }
}
