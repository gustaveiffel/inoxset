//! Dictionary encoding module: bidirectional mapping between external string IDs
//! and compact internal `u32` values.
//!
//! This module uses a **global** dictionary — every external string ID maps to
//! exactly one `u32` across all event types.  There is no per-event namespace;
//! the same external ID always resolves to the same internal integer regardless
//! of which event it is used with.
//!
//! Counters and mappings are stored in the LMDB environment shared with the
//! [`crate::catalog::Catalog`].  The three named databases (`dict_fwd_v2`,
//! `dict_rev_v2`, `next_dict_id_v2`) are opened by the catalog at startup and
//! passed to these functions.
//!
//! # Databases
//!
//! | Catalog field | Key | Value |
//! |---|---|---|
//! | `dict_fwd` | `external_id` (Str) | `u32` internal ID (U64) |
//! | `dict_rev` | `u32` internal ID (U64) | external ID string (Str) |
//! | `dict_next_id` | `"_"` (Str) | next available `u32` counter (U64) |

use crate::catalog::Catalog;

/// Fixed key used for the singleton next-ID counter.
const SINGLETON_KEY: &str = "_";

// ─── Helper: read a single forward entry ─────────────────────────────────────

/// Performs a single read-only lookup in the forward table.
///
/// Returns `None` if the key is absent.
fn read_fwd(cat: &Catalog, external_id: &str) -> crate::Result<Option<u32>> {
    let rtxn = cat.env().read_txn()?;
    let v = cat.dict_fwd.get(&rtxn, external_id)?.map(|v| v as u32);
    Ok(v)
}

// ─── Core functions ───────────────────────────────────────────────────────────

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
/// Returns an error on any LMDB I/O failure.
pub fn assign_or_get(cat: &Catalog, external_id: &str) -> crate::Result<u32> {
    // Phase 1: optimistic read — avoids a write lock on the common path.
    if let Some(id) = read_fwd(cat, external_id)? {
        return Ok(id);
    }

    // Phase 2: write transaction with double-check.
    let mut wtxn = cat.env().write_txn()?;

    // Double-check: another writer may have raced us.
    if let Some(id) = cat.dict_fwd.get(&wtxn, external_id)? {
        return Ok(id as u32);
    }

    let next = cat.dict_next_id.get(&wtxn, SINGLETON_KEY)?.unwrap_or(0u64);
    let next_u32 = u32::try_from(next).map_err(|_| {
        crate::error::InoxSetError::Configuration(
            "global dictionary ID space exhausted (u32::MAX)".into(),
        )
    })?;
    let next_val = next.checked_add(1).ok_or_else(|| {
        crate::error::InoxSetError::Configuration(
            "global dictionary ID space exhausted (u32::MAX)".into(),
        )
    })?;

    cat.dict_fwd.put(&mut wtxn, external_id, &next)?;
    cat.dict_rev.put(&mut wtxn, &next, external_id)?;
    cat.dict_next_id.put(&mut wtxn, SINGLETON_KEY, &next_val)?;
    wtxn.commit()?;
    Ok(next_u32)
}

/// Returns the internal `u32` for `external_id`, or `None` if the external ID
/// has not been registered.
///
/// This is a read-only operation and never modifies the database.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
pub fn lookup(cat: &Catalog, external_id: &str) -> crate::Result<Option<u32>> {
    read_fwd(cat, external_id)
}

/// Returns the external string ID for `internal_id`, or `None` if the internal
/// ID is not present in the reverse table.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
pub fn reverse_lookup(cat: &Catalog, internal_id: u32) -> crate::Result<Option<String>> {
    let rtxn = cat.env().read_txn()?;
    let v = cat
        .dict_rev
        .get(&rtxn, &(internal_id as u64))?
        .map(|s| s.to_owned());
    Ok(v)
}

/// Reverse-lookups a batch of internal IDs in a single read transaction.
///
/// More efficient than calling [`reverse_lookup`] in a loop — opens one
/// transaction for all lookups instead of one per ID.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
pub fn batch_reverse_lookup_u32(
    cat: &Catalog,
    internal_ids: impl Iterator<Item = u32>,
) -> crate::Result<Vec<(u32, String)>> {
    let rtxn = cat.env().read_txn()?;
    let mut results = Vec::new();
    for id in internal_ids {
        if let Some(s) = cat.dict_rev.get(&rtxn, &(id as u64))? {
            results.push((id, s.to_owned()));
        }
    }
    Ok(results)
}

/// Removes the `external_id` entry from both the forward and reverse tables.
///
/// Returns the internal `u32` that was associated with `external_id`, or `None`
/// if the ID was not registered.  The global counter is **not** decremented;
/// freed IDs are not reused.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
pub fn delete(cat: &Catalog, external_id: &str) -> crate::Result<Option<u32>> {
    let mut wtxn = cat.env().write_txn()?;

    let existing = cat.dict_fwd.get(&wtxn, external_id)?.map(|v| v as u32);
    if let Some(id) = existing {
        cat.dict_fwd.delete(&mut wtxn, external_id)?;
        cat.dict_rev.delete(&mut wtxn, &(id as u64))?;
        wtxn.commit()?;
        Ok(Some(id))
    } else {
        Ok(None)
    }
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
/// Returns an error on any LMDB I/O failure.
pub fn batch_assign_or_get(cat: &Catalog, external_ids: &[&str]) -> crate::Result<Vec<u32>> {
    let mut result: Vec<Option<u32>> = vec![None; external_ids.len()];

    // Phase 1: read-only pass.
    {
        let rtxn = cat.env().read_txn()?;
        for (i, &ext) in external_ids.iter().enumerate() {
            result[i] = cat.dict_fwd.get(&rtxn, ext)?.map(|v| v as u32);
        }
    }

    // Check if all were resolved.
    let needs_write = result.iter().any(|r| r.is_none());
    if !needs_write {
        return Ok(result.into_iter().flatten().collect());
    }

    // Phase 2: write transaction for unknowns, with double-check.
    let mut wtxn = cat.env().write_txn()?;

    let mut next = cat.dict_next_id.get(&wtxn, SINGLETON_KEY)?.unwrap_or(0u64);

    for (i, &ext) in external_ids.iter().enumerate() {
        if result[i].is_some() {
            continue;
        }

        // Double-check: another writer may have inserted while we waited.
        let existing = cat.dict_fwd.get(&wtxn, ext)?.map(|v| v as u32);
        let id = if let Some(existing_id) = existing {
            existing_id
        } else {
            let assigned = u32::try_from(next).map_err(|_| {
                crate::error::InoxSetError::Configuration(
                    "global dictionary ID space exhausted (u32::MAX)".into(),
                )
            })?;
            cat.dict_fwd.put(&mut wtxn, ext, &next)?;
            cat.dict_rev.put(&mut wtxn, &next, ext)?;
            let a = assigned;
            next = next.checked_add(1).ok_or_else(|| {
                crate::error::InoxSetError::Configuration(
                    "global dictionary ID space exhausted (u32::MAX)".into(),
                )
            })?;
            a
        };
        result[i] = Some(id);
    }

    cat.dict_next_id.put(&mut wtxn, SINGLETON_KEY, &next)?;
    wtxn.commit()?;

    Ok(result.into_iter().flatten().collect())
}

/// Resolves external IDs to internal `u32`s in **one** read transaction,
/// without assigning IDs to unknown entries.
///
/// Returns a `Vec<Option<u32>>` in the **same order** as the input slice;
/// an entry is `None` when the external ID has never been assigned.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
pub fn batch_lookup(cat: &Catalog, external_ids: &[&str]) -> crate::Result<Vec<Option<u32>>> {
    let rtxn = cat.env().read_txn()?;
    let mut result = Vec::with_capacity(external_ids.len());
    for &ext in external_ids {
        result.push(cat.dict_fwd.get(&rtxn, ext)?.map(|v| v as u32));
    }
    Ok(result)
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
/// Returns an error on any LMDB I/O failure.
pub fn batch_reverse_lookup(
    cat: &Catalog,
    internal_ids: &[u32],
) -> crate::Result<Vec<Option<String>>> {
    let rtxn = cat.env().read_txn()?;
    let mut result = Vec::with_capacity(internal_ids.len());
    for &id in internal_ids {
        let entry = cat.dict_rev.get(&rtxn, &(id as u64))?.map(|s| s.to_owned());
        result.push(entry);
    }
    Ok(result)
}

// ─── UUID helpers ─────────────────────────────────────────────────────────────

#[cfg(feature = "uuid")]
use uuid::Uuid;

/// Assigns or retrieves a global `u32` for a UUID.
///
/// The UUID is stored using its canonical lowercase hyphenated string
/// representation (36 bytes, e.g. `"550e8400-e29b-41d4-a716-446655440000"`).
/// If the same UUID is also inserted via [`assign_or_get`] with a differently
/// cased string, they will be treated as distinct entries. Always use this
/// method for UUID-typed IDs.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
#[cfg(feature = "uuid")]
pub fn assign_or_get_uuid(cat: &Catalog, id: &Uuid) -> crate::Result<u32> {
    assign_or_get(cat, &id.to_string())
}

/// Read-only UUID lookup.
///
/// Returns the internal `u32` for `id`, or `None` if the UUID has not been
/// registered.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
#[cfg(feature = "uuid")]
pub fn lookup_uuid(cat: &Catalog, id: &Uuid) -> crate::Result<Option<u32>> {
    lookup(cat, &id.to_string())
}

/// Reverse lookup: `u32` → [`Uuid`].
///
/// Returns `None` if the internal ID is not present in the reverse table or
/// the stored string cannot be parsed as a UUID.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
#[cfg(feature = "uuid")]
pub fn reverse_lookup_uuid(cat: &Catalog, internal_id: u32) -> crate::Result<Option<Uuid>> {
    match reverse_lookup(cat, internal_id)? {
        Some(s) => Ok(s.parse::<Uuid>().ok()),
        None => Ok(None),
    }
}

/// Deletes a UUID entity from the dictionary.
///
/// Returns the internal `u32` that was associated with `id`, or `None` if
/// the UUID was not registered.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
#[cfg(feature = "uuid")]
pub fn delete_uuid(cat: &Catalog, id: &Uuid) -> crate::Result<Option<u32>> {
    delete(cat, &id.to_string())
}

/// Batch assign or retrieve `u32`s for UUIDs.
///
/// Returns a `Vec<u32>` in the **same order** as the input slice.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
#[cfg(feature = "uuid")]
pub fn batch_assign_or_get_uuids(cat: &Catalog, ids: &[Uuid]) -> crate::Result<Vec<u32>> {
    let strings: Vec<String> = ids.iter().map(|u| u.to_string()).collect();
    let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
    batch_assign_or_get(cat, &refs)
}

/// Batch reverse lookup for UUIDs.
///
/// Returns a `Vec<Option<Uuid>>` in the **same order** as `internal_ids`.
/// An entry is `None` when the internal ID is absent or the stored value
/// cannot be parsed as a UUID.
///
/// # Errors
///
/// Returns an error on any LMDB I/O failure.
#[cfg(feature = "uuid")]
pub fn batch_reverse_lookup_uuids(
    cat: &Catalog,
    internal_ids: &[u32],
) -> crate::Result<Vec<Option<Uuid>>> {
    let strings = batch_reverse_lookup(cat, internal_ids)?;
    Ok(strings
        .into_iter()
        .map(|opt| opt.and_then(|s| s.parse::<Uuid>().ok()))
        .collect())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_catalog() -> (Catalog, TempDir) {
        let dir = TempDir::new().unwrap();
        let cat = Catalog::open(dir.path().join("dict_test.mdb")).unwrap();
        (cat, dir)
    }

    #[test]
    fn assign_or_get_global() {
        let (cat, _dir) = test_catalog();
        let id0 = assign_or_get(&cat, "user-abc-123").unwrap();
        let id1 = assign_or_get(&cat, "user-def-456").unwrap();
        let id0_again = assign_or_get(&cat, "user-abc-123").unwrap();
        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id0_again, 0);
    }

    #[test]
    fn same_id_regardless_of_usage_context() {
        let (cat, _dir) = test_catalog();
        let id_clicks = assign_or_get(&cat, "user-1").unwrap();
        let id_views = assign_or_get(&cat, "user-1").unwrap();
        assert_eq!(id_clicks, id_views);
        assert_eq!(id_clicks, 0);
    }

    #[test]
    fn batch_assign_or_get_global() {
        let (cat, _dir) = test_catalog();
        let ids = batch_assign_or_get(&cat, &["aaa", "bbb", "ccc"]).unwrap();
        assert_eq!(ids, vec![0, 1, 2]);
        let ids2 = batch_assign_or_get(&cat, &["bbb", "ddd", "aaa"]).unwrap();
        assert_eq!(ids2, vec![1, 3, 0]);
    }

    #[test]
    fn batch_reverse_lookup_global() {
        let (cat, _dir) = test_catalog();
        batch_assign_or_get(&cat, &["alice", "bob", "charlie"]).unwrap();
        let names = batch_reverse_lookup(&cat, &[2, 0, 1]).unwrap();
        assert_eq!(
            names,
            vec![
                Some("charlie".to_string()),
                Some("alice".to_string()),
                Some("bob".to_string()),
            ]
        );
        let missing = batch_reverse_lookup(&cat, &[99]).unwrap();
        assert_eq!(missing, vec![None]);
    }

    #[test]
    fn delete_removes_both_directions() {
        let (cat, _dir) = test_catalog();
        let id = assign_or_get(&cat, "to-delete").unwrap();
        assert_eq!(id, 0);

        assert_eq!(lookup(&cat, "to-delete").unwrap(), Some(0));
        assert_eq!(
            reverse_lookup(&cat, 0).unwrap(),
            Some("to-delete".to_string())
        );

        let removed = delete(&cat, "to-delete").unwrap();
        assert_eq!(removed, Some(0));
        assert_eq!(lookup(&cat, "to-delete").unwrap(), None);
        assert_eq!(reverse_lookup(&cat, 0).unwrap(), None);

        assert_eq!(delete(&cat, "never-existed").unwrap(), None);
    }
}

#[cfg(all(test, feature = "uuid"))]
mod uuid_tests {
    use super::*;
    use uuid::Uuid;

    fn test_catalog() -> (Catalog, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let cat = Catalog::open(dir.path().join("catalog.mdb")).unwrap();
        (cat, dir)
    }

    #[test]
    fn uuid_assign_and_lookup() {
        let (cat, _dir) = test_catalog();
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let u32_id = assign_or_get_uuid(&cat, &id).unwrap();
        let again = assign_or_get_uuid(&cat, &id).unwrap();
        assert_eq!(u32_id, again);

        let looked = lookup_uuid(&cat, &id).unwrap();
        assert_eq!(looked, Some(u32_id));

        let rev = reverse_lookup_uuid(&cat, u32_id).unwrap();
        assert_eq!(rev, Some(id));
    }

    #[test]
    fn uuid_batch_roundtrip() {
        let (cat, _dir) = test_catalog();

        let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let u32s = batch_assign_or_get_uuids(&cat, &ids).unwrap();
        assert_eq!(u32s.len(), 5);

        let reversed = batch_reverse_lookup_uuids(&cat, &u32s).unwrap();
        for (i, rev) in reversed.iter().enumerate() {
            assert_eq!(rev.as_ref(), Some(&ids[i]));
        }
    }

    #[test]
    fn uuid_delete() {
        let (cat, _dir) = test_catalog();
        let id = Uuid::new_v4();

        assign_or_get_uuid(&cat, &id).unwrap();
        assert!(lookup_uuid(&cat, &id).unwrap().is_some());

        delete_uuid(&cat, &id).unwrap();
        assert!(lookup_uuid(&cat, &id).unwrap().is_none());
    }

    #[test]
    fn uuid_v7_works() {
        let (cat, _dir) = test_catalog();
        let id = Uuid::now_v7();

        let u32_id = assign_or_get_uuid(&cat, &id).unwrap();
        let again = assign_or_get_uuid(&cat, &id).unwrap();
        assert_eq!(u32_id, again);

        let rev = reverse_lookup_uuid(&cat, u32_id).unwrap();
        assert_eq!(rev, Some(id));
    }
}
