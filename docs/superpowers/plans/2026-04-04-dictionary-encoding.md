# Dictionary Encoding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bidirectional dictionary layer that maps arbitrary external IDs (UUID, nanoid, string) to monotonic u32 values, enabling RoaringBitmap storage for entities with non-u32 identifiers.

**Architecture:** Two new redb tables per store (`DICT_FWD` and `DICT_REV`) hold the bidirectional mapping, scoped by event name. A `NEXT_DICT_ID` counter per event provides monotonic u32 allocation. New `put_ids` / `get_ids` / `remove_ids` API methods translate external IDs through the dictionary before delegating to existing bitmap operations. The dictionary is append-only — IDs are never reassigned.

**Tech Stack:** Rust, redb (existing), RoaringBitmap (existing). No new dependencies.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/dict.rs` | **Create** | Dictionary module: redb table definitions, encode/decode, lookup/assign, reverse lookup, batch operations |
| `src/lib.rs` | **Modify** | Add `put_ids`, `get_ids`, `remove_ids` public API methods; add `pub mod dict;` |
| `src/catalog.rs` | **Modify** | Open `DICT_FWD`, `DICT_REV`, `NEXT_DICT_ID` tables during catalog init to ensure they exist |
| `src/types.rs` | **Modify** | Add `DictStats` struct for health reporting |
| `tests/integration.rs` | **Modify** | Add dictionary integration tests |

---

### Task 1: Dictionary Module — Table Definitions and Core Lookup

**Files:**
- Create: `src/dict.rs`
- Modify: `src/lib.rs` (add `pub mod dict;`)

- [ ] **Step 1: Write failing test — assign_or_get returns monotonic u32**

```rust
// src/dict.rs at bottom
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_db() -> (Database, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        // Ensure tables exist.
        let txn = db.begin_write().unwrap();
        txn.open_table(DICT_FWD).unwrap();
        txn.open_table(DICT_REV).unwrap();
        txn.open_table(NEXT_DICT_ID).unwrap();
        txn.commit().unwrap();
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
        assert_eq!(id1_again, 0); // idempotent
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib dict::tests::assign_or_get_monotonic -v`
Expected: FAIL — module `dict` does not exist.

- [ ] **Step 3: Implement dictionary module skeleton**

```rust
// src/dict.rs
//! Dictionary encoding: bidirectional mapping between external string IDs
//! and internal `u32` values suitable for RoaringBitmap storage.
//!
//! # Tables
//!
//! | Constant | Key | Value |
//! |---|---|---|
//! | [`DICT_FWD`] | `"event\0external_id"` | `u32` |
//! | [`DICT_REV`] | `(event, u32)` — encoded as `"event\0{id:010}"` | external_id string |
//! | [`NEXT_DICT_ID`] | event name | next `u32` to allocate |

use redb::{Database, ReadableTable, TableDefinition};

use crate::error::InoxSetError;

/// Forward dictionary: `"event\0external_id"` → `u32`.
pub(crate) const DICT_FWD: TableDefinition<&str, u32> = TableDefinition::new("dict_fwd");

/// Reverse dictionary: `"event\0{id:010}"` → external_id.
pub(crate) const DICT_REV: TableDefinition<&str, &str> = TableDefinition::new("dict_rev");

/// Per-event monotonic u32 counter: event name → next available `u32`.
pub(crate) const NEXT_DICT_ID: TableDefinition<&str, u32> = TableDefinition::new("next_dict_id");

/// Builds the forward lookup key: `"event\0external_id"`.
fn fwd_key(event: &str, external_id: &str) -> String {
    format!("{}\0{}", event, external_id)
}

/// Builds the reverse lookup key: `"event\0{id:010}"`.
fn rev_key(event: &str, internal_id: u32) -> String {
    format!("{}\0{:010}", event, internal_id)
}

/// Looks up or assigns a u32 for `external_id` within `event`.
///
/// If the external ID already exists, returns the existing u32.
/// Otherwise, allocates the next monotonic u32 and stores the
/// bidirectional mapping.
///
/// # Errors
///
/// Returns an error on redb I/O failure.
pub fn assign_or_get(db: &Database, event: &str, external_id: &str) -> crate::Result<u32> {
    // Fast path: read-only check.
    {
        let rtxn = db.begin_read()?;
        let table = rtxn.open_table(DICT_FWD)?;
        let key = fwd_key(event, external_id);
        if let Some(guard) = table.get(key.as_str())? {
            return Ok(guard.value());
        }
    }

    // Slow path: allocate under write txn.
    let txn = db.begin_write()?;
    let id;
    {
        let mut fwd = txn.open_table(DICT_FWD)?;
        let key = fwd_key(event, external_id);

        // Double-check under write lock (another thread may have inserted).
        if let Some(guard) = fwd.get(key.as_str())? {
            // No commit needed — just read.
            return Ok(guard.value());
        }

        let mut counter = txn.open_table(NEXT_DICT_ID)?;
        id = counter
            .get(event)?
            .map(|g| g.value())
            .unwrap_or(0);
        counter.insert(event, id + 1)?;

        fwd.insert(key.as_str(), id)?;

        let mut rev = txn.open_table(DICT_REV)?;
        let rk = rev_key(event, id);
        rev.insert(rk.as_str(), external_id)?;
    }
    txn.commit()?;
    Ok(id)
}

/// Looks up the u32 for an external ID without allocating.
///
/// Returns `None` if the external ID has never been assigned.
///
/// # Errors
///
/// Returns an error on redb I/O failure.
pub fn lookup(db: &Database, event: &str, external_id: &str) -> crate::Result<Option<u32>> {
    let rtxn = db.begin_read()?;
    let table = rtxn.open_table(DICT_FWD)?;
    let key = fwd_key(event, external_id);
    Ok(table.get(key.as_str())?.map(|g| g.value()))
}

/// Resolves a u32 back to its external ID.
///
/// Returns `None` if the internal ID has never been assigned.
///
/// # Errors
///
/// Returns an error on redb I/O failure.
pub fn reverse_lookup(db: &Database, event: &str, internal_id: u32) -> crate::Result<Option<String>> {
    let rtxn = db.begin_read()?;
    let table = rtxn.open_table(DICT_REV)?;
    let key = rev_key(event, internal_id);
    Ok(table.get(key.as_str())?.map(|g| g.value().to_string()))
}
```

Add `pub mod dict;` to `src/lib.rs` after the other module declarations.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib dict::tests::assign_or_get_monotonic -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(dict): dictionary module with assign_or_get, lookup, reverse_lookup
```

---

### Task 2: Batch Operations and Event Scoping

**Files:**
- Modify: `src/dict.rs`

- [ ] **Step 1: Write failing test — batch_assign_or_get**

```rust
#[test]
fn batch_assign_or_get_returns_vec() {
    let (db, _dir) = test_db();
    let ids = batch_assign_or_get(&db, "segment", &["aaa", "bbb", "ccc"]).unwrap();
    assert_eq!(ids, vec![0, 1, 2]);

    // Idempotent with mixed known/unknown.
    let ids2 = batch_assign_or_get(&db, "segment", &["bbb", "ddd", "aaa"]).unwrap();
    assert_eq!(ids2, vec![1, 3, 0]);
}

#[test]
fn event_scoping_independent() {
    let (db, _dir) = test_db();
    let id_a = assign_or_get(&db, "clicks", "user-1").unwrap();
    let id_b = assign_or_get(&db, "views", "user-1").unwrap();
    // Same external ID, different events → both start at 0.
    assert_eq!(id_a, 0);
    assert_eq!(id_b, 0);
}

#[test]
fn batch_reverse_lookup_resolves() {
    let (db, _dir) = test_db();
    batch_assign_or_get(&db, "ev", &["alice", "bob", "charlie"]).unwrap();
    let names = batch_reverse_lookup(&db, "ev", &[2, 0, 1]).unwrap();
    assert_eq!(names, vec![
        Some("charlie".to_string()),
        Some("alice".to_string()),
        Some("bob".to_string()),
    ]);
    // Unknown ID.
    let names2 = batch_reverse_lookup(&db, "ev", &[99]).unwrap();
    assert_eq!(names2, vec![None]);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib dict::tests -v`
Expected: FAIL — `batch_assign_or_get` and `batch_reverse_lookup` not defined.

- [ ] **Step 3: Implement batch operations**

```rust
/// Assigns or retrieves u32 IDs for a batch of external IDs.
///
/// Uses a single write transaction for all new assignments.
/// Returns a `Vec<u32>` in the same order as `external_ids`.
///
/// # Errors
///
/// Returns an error on redb I/O failure.
pub fn batch_assign_or_get(
    db: &Database,
    event: &str,
    external_ids: &[&str],
) -> crate::Result<Vec<u32>> {
    let mut results = Vec::with_capacity(external_ids.len());
    let mut to_assign: Vec<(usize, &str)> = Vec::new();

    // Phase 1: read-only pass to resolve known IDs.
    {
        let rtxn = db.begin_read()?;
        let table = rtxn.open_table(DICT_FWD)?;
        for (i, ext_id) in external_ids.iter().enumerate() {
            let key = fwd_key(event, ext_id);
            if let Some(guard) = table.get(key.as_str())? {
                results.push(guard.value());
            } else {
                results.push(u32::MAX); // placeholder
                to_assign.push((i, ext_id));
            }
        }
    }

    if to_assign.is_empty() {
        return Ok(results);
    }

    // Phase 2: write txn for new assignments.
    let txn = db.begin_write()?;
    {
        let mut fwd = txn.open_table(DICT_FWD)?;
        let mut rev = txn.open_table(DICT_REV)?;
        let mut counter = txn.open_table(NEXT_DICT_ID)?;

        let mut next_id = counter
            .get(event)?
            .map(|g| g.value())
            .unwrap_or(0);

        for (idx, ext_id) in &to_assign {
            let key = fwd_key(event, ext_id);
            // Double-check (concurrent writer may have assigned).
            if let Some(guard) = fwd.get(key.as_str())? {
                results[*idx] = guard.value();
            } else {
                let id = next_id;
                next_id += 1;
                fwd.insert(key.as_str(), id)?;
                let rk = rev_key(event, id);
                rev.insert(rk.as_str(), *ext_id)?;
                results[*idx] = id;
            }
        }

        counter.insert(event, next_id)?;
    }
    txn.commit()?;
    Ok(results)
}

/// Resolves a batch of internal u32 IDs back to their external IDs.
///
/// Returns `Vec<Option<String>>` in the same order as `internal_ids`.
/// Returns `None` for IDs that have never been assigned.
///
/// # Errors
///
/// Returns an error on redb I/O failure.
pub fn batch_reverse_lookup(
    db: &Database,
    event: &str,
    internal_ids: &[u32],
) -> crate::Result<Vec<Option<String>>> {
    let rtxn = db.begin_read()?;
    let table = rtxn.open_table(DICT_REV)?;
    let mut results = Vec::with_capacity(internal_ids.len());
    for &id in internal_ids {
        let key = rev_key(event, id);
        match table.get(key.as_str())? {
            Some(guard) => results.push(Some(guard.value().to_string())),
            None => results.push(None),
        }
    }
    Ok(results)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib dict::tests -v`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Commit**

```
feat(dict): batch assign/lookup and event-scoped ID spaces
```

---

### Task 3: Catalog Integration — Ensure Dictionary Tables Exist

**Files:**
- Modify: `src/catalog.rs`

- [ ] **Step 1: Write failing test — catalog open creates dict tables**

```rust
// In catalog::tests
#[test]
fn catalog_creates_dict_tables() {
    let (cat, _dir) = test_catalog();
    // Verify the dict tables are accessible by performing a read.
    let rtxn = cat.db().begin_read().unwrap();
    let _fwd = rtxn.open_table(crate::dict::DICT_FWD).unwrap();
    let _rev = rtxn.open_table(crate::dict::DICT_REV).unwrap();
    let _next = rtxn.open_table(crate::dict::NEXT_DICT_ID).unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib catalog::tests::catalog_creates_dict_tables -v`
Expected: FAIL — tables not found.

- [ ] **Step 3: Add dict table creation to Catalog::open**

In `src/catalog.rs`, inside the `Catalog::open` method where existing tables are created in the initial write transaction, add:

```rust
txn.open_table(crate::dict::DICT_FWD)?;
txn.open_table(crate::dict::DICT_REV)?;
txn.open_table(crate::dict::NEXT_DICT_ID)?;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib catalog::tests::catalog_creates_dict_tables -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(dict): ensure dictionary tables exist on catalog open
```

---

### Task 4: Public API — `put_ids` and `get_ids`

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing test — put_ids and get_ids roundtrip**

```rust
// In lib.rs tests
#[test]
fn put_ids_get_ids_roundtrip() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    store
        .put_ids("segment", Period::Day(2026, 3, 18), &["user-aaa", "user-bbb", "user-ccc"])
        .unwrap();
    store.flush().unwrap();

    let ids = store
        .get_ids("segment", Period::Day(2026, 3, 18))
        .unwrap();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["user-aaa", "user-bbb", "user-ccc"]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib tests::put_ids_get_ids_roundtrip -v`
Expected: FAIL — `put_ids` not defined.

- [ ] **Step 3: Implement `put_ids`**

```rust
/// Writes a set of external IDs for the given event and period.
///
/// External IDs (strings) are transparently mapped to u32 values via
/// the dictionary encoding layer. New IDs are auto-assigned a monotonic
/// u32 on first encounter. The resulting bitmap is OR-accumulated with
/// any existing data, just like [`put_bitmap`](Self::put_bitmap).
///
/// # Errors
///
/// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
/// Returns an error on catalog or file I/O failure.
pub fn put_ids(
    &self,
    event: &str,
    period: Period,
    external_ids: &[&str],
) -> Result<()> {
    self.check_writable()?;
    if external_ids.is_empty() {
        return Ok(());
    }

    let internal_ids = dict::batch_assign_or_get(self.catalog.db(), event, external_ids)?;

    let mut bitmap = RoaringBitmap::new();
    for id in internal_ids {
        bitmap.insert(id);
    }

    self.put_bitmap(event, period, bitmap)
}
```

- [ ] **Step 4: Implement `get_ids`**

```rust
/// Reads the external IDs stored for the given event and period.
///
/// Retrieves the merged bitmap via [`get`](Self::get), then resolves
/// each u32 back to its external string ID through the dictionary.
/// IDs that cannot be resolved (should not happen in normal operation)
/// are silently omitted.
///
/// # Errors
///
/// Returns an error on catalog or file I/O failure.
pub fn get_ids(&self, event: &str, period: Period) -> Result<Vec<String>> {
    let bitmap = self.get(event, period)?;
    let internal_ids: Vec<u32> = bitmap.iter().collect();
    let resolved = dict::batch_reverse_lookup(self.catalog.db(), event, &internal_ids)?;
    Ok(resolved.into_iter().flatten().collect())
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib tests::put_ids_get_ids_roundtrip -v`
Expected: PASS

- [ ] **Step 6: Commit**

```
feat: put_ids and get_ids — string ID API backed by dictionary encoding
```

---

### Task 5: Public API — `remove_ids`

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing test — remove_ids deletes via dictionary**

```rust
#[test]
fn remove_ids_deletes_through_dictionary() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    store
        .put_ids("seg", Period::Day(2026, 3, 18), &["alice", "bob", "charlie"])
        .unwrap();

    store
        .remove_ids("seg", Period::Day(2026, 3, 18), &["bob"])
        .unwrap();

    let ids = store.get_ids("seg", Period::Day(2026, 3, 18)).unwrap();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["alice", "charlie"]);
}

#[test]
fn remove_ids_unknown_id_is_noop() {
    let dir = TempDir::new().unwrap();
    let store = InoxSet::builder()
        .path(dir.path().join("data"))
        .open()
        .unwrap();

    store
        .put_ids("seg", Period::Static, &["x", "y"])
        .unwrap();

    // "z" was never put — should not error.
    store
        .remove_ids("seg", Period::Static, &["z"])
        .unwrap();

    let ids = store.get_ids("seg", Period::Static).unwrap();
    assert_eq!(ids.len(), 2);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib tests::remove_ids -v`
Expected: FAIL — `remove_ids` not defined.

- [ ] **Step 3: Implement `remove_ids`**

```rust
/// Removes external IDs from the given event and period via delta tombstones.
///
/// Looks up each external ID in the dictionary. IDs that have never been
/// assigned are silently ignored. The resolved u32 values are passed to
/// [`remove_bits`](Self::remove_bits).
///
/// # Errors
///
/// Returns [`InoxSetError::ReadOnly`] if the store is read-only.
/// Returns an error on catalog or file I/O failure.
pub fn remove_ids(
    &self,
    event: &str,
    period: Period,
    external_ids: &[&str],
) -> Result<()> {
    self.check_writable()?;
    if external_ids.is_empty() {
        return Ok(());
    }

    let mut bits: Vec<u32> = Vec::new();
    for ext_id in external_ids {
        if let Some(internal) = dict::lookup(self.catalog.db(), event, ext_id)? {
            bits.push(internal);
        }
    }

    if bits.is_empty() {
        return Ok(());
    }

    self.remove_bits(event, period, &bits)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib tests::remove_ids -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: remove_ids — string-based delete through dictionary encoding
```

---

### Task 6: Integration Tests

**Files:**
- Modify: `tests/integration.rs`

- [ ] **Step 1: Write integration tests**

```rust
#[test]
fn dict_put_get_flush_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data");

    // Write and flush.
    {
        let store = InoxSet::builder().path(&path).open().unwrap();
        store
            .put_ids("audience", Period::Day(2026, 4, 1), &["usr-001", "usr-002", "usr-003"])
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
    }

    // Reopen and verify dictionary survives.
    {
        let store = InoxSet::builder().path(&path).open().unwrap();
        let ids = store
            .get_ids("audience", Period::Day(2026, 4, 1))
            .unwrap();
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

    // Use put_ids for some data.
    store
        .put_ids("ev", Period::Day(2026, 4, 1), &["a", "b"])
        .unwrap();

    // Use raw put_bitmap with known u32 IDs.
    let mut bm = roaring::RoaringBitmap::new();
    bm.insert(100); // raw u32, not in dictionary
    store.put_bitmap("ev", Period::Day(2026, 4, 1), bm).unwrap();

    // get returns all 3 bits (dict IDs 0,1 + raw 100).
    let result = store.get("ev", Period::Day(2026, 4, 1)).unwrap();
    assert_eq!(result.len(), 3);

    // get_ids only resolves the 2 dictionary entries.
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
```

- [ ] **Step 2: Run integration tests**

Run: `cargo test --test integration dict -v`
Expected: PASS (3 tests)

- [ ] **Step 3: Commit**

```
test: dictionary encoding integration tests — reopen, mixed mode, compaction
```

---

### Task 7: Documentation — Update README API Table

**Files:**
- Modify: `PUBLIC_README.md`

- [ ] **Step 1: Add dict API methods to API table**

Add to the API table in `PUBLIC_README.md`:

```markdown
| `put_ids` | Write external string IDs (auto-mapped to u32 via dictionary) |
| `get_ids` | Read back external string IDs for an event + period |
| `remove_ids` | Delete external IDs via dictionary-resolved tombstones |
```

- [ ] **Step 2: Add a "Dictionary Encoding" section to Features**

After the "Compaction" section:

```markdown
**Dictionary encoding** — store arbitrary string IDs (UUID, nanoid) in u32 Roaring Bitmaps. The mapping is automatic and persistent.

\```rust
// Write with string IDs — dictionary assigns u32 internally
store.put_ids("premium", Period::Day(2026, 3, 18), &[
    "usr_9f3a2b-...",
    "usr_c81d4e-...",
])?;

// Read back the original string IDs
let users: Vec<String> = store.get_ids("premium", Period::Day(2026, 3, 18))?;

// GDPR delete by string ID
store.remove_ids("premium", Period::Day(2026, 3, 18), &["usr_9f3a2b-..."])?;
\```
```

- [ ] **Step 3: Commit**

```
docs: add dictionary encoding to README — API table and feature section
```

---

## Self-Review Checklist

1. **Spec coverage:** All items from the memory doc (`project_dmp_dictionary.md`) are covered — `DICT_FWD`, `DICT_REV`, `put_ids`, `get_ids`, event-scoped mapping, bidirectional lookup.
2. **Placeholder scan:** No TBD/TODO/placeholders. All code blocks are complete.
3. **Type consistency:** `assign_or_get`, `batch_assign_or_get`, `lookup`, `reverse_lookup`, `batch_reverse_lookup` — names consistent throughout. `fwd_key`/`rev_key` helpers used in all operations. Public API: `put_ids`/`get_ids`/`remove_ids` consistent naming.
4. **Edge cases covered:** Unknown ID in `remove_ids` (noop), mixed dict + raw bitmap, dictionary survives reopen + compaction, event scoping (independent ID spaces).
