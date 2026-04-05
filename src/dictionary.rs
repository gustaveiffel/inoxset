//! Standalone dictionary for external ID → u32 mapping.
//!
//! The [`Dictionary`] can be opened independently from an [`InoxSet`](crate::InoxSet)
//! store, enabling external consumers (e.g., Kafka ingestion workers) to assign
//! `person_int_id` values without depending on the full store.
//!
//! The dictionary shares the same LMDB environment and tables as the
//! [`InoxSet`](crate::InoxSet) catalog. **Important:** the store must be
//! closed/dropped before opening a standalone `Dictionary` on the same path
//! (LMDB does not allow two environments on the same path in one process).
//!
//! # Example
//!
//! ```no_run
//! use inoxset::dictionary::Dictionary;
//!
//! let dict = Dictionary::open("data/my_store/catalog.mdb").unwrap();
//!
//! let id = dict.get_or_assign("user-abc").unwrap();
//! let same_id = dict.get_or_assign("user-abc").unwrap();
//! assert_eq!(id, same_id);
//!
//! let resolved = dict.resolve(id).unwrap();
//! assert_eq!(resolved.as_deref(), Some("user-abc"));
//! ```

use crate::catalog::Catalog;
use crate::dict;

/// Standalone dictionary for external ID ↔ u32 mapping.
///
/// Thread-safe: backed by LMDB with lock-free readers.
/// Can be shared across threads via `Arc<Dictionary>`.
pub struct Dictionary {
    catalog: Catalog,
}

impl Dictionary {
    /// Opens (or creates) a dictionary at the given LMDB path.
    ///
    /// If an [`InoxSet`](crate::InoxSet) store already exists at this path,
    /// the dictionary shares the same LMDB environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB environment cannot be opened.
    pub fn open(path: impl AsRef<std::path::Path>) -> crate::Result<Self> {
        let catalog = Catalog::open(path)?;
        Ok(Self { catalog })
    }

    /// Opens with an explicit LMDB map size.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB environment cannot be opened.
    pub fn open_with_map_size(
        path: impl AsRef<std::path::Path>,
        map_size: usize,
    ) -> crate::Result<Self> {
        let catalog = Catalog::open_with_map_size(path, map_size)?;
        Ok(Self { catalog })
    }

    /// Assigns or retrieves a u32 for the given external ID.
    ///
    /// Idempotent: the same external ID always returns the same u32.
    /// New IDs are assigned sequentially starting from 0.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn get_or_assign(&self, external_id: &str) -> crate::Result<u32> {
        dict::assign_or_get(&self.catalog, external_id)
    }

    /// Assigns or retrieves u32s for a batch of external IDs.
    ///
    /// Returns a `Vec<u32>` in the same order as the input.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn get_or_assign_batch(&self, external_ids: &[&str]) -> crate::Result<Vec<u32>> {
        dict::batch_assign_or_get(&self.catalog, external_ids)
    }

    /// Looks up the u32 for an external ID without assigning.
    ///
    /// Returns `None` if the ID has never been assigned.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn lookup(&self, external_id: &str) -> crate::Result<Option<u32>> {
        dict::lookup(&self.catalog, external_id)
    }

    /// Checks if an external ID exists in the dictionary.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn contains(&self, external_id: &str) -> crate::Result<bool> {
        Ok(dict::lookup(&self.catalog, external_id)?.is_some())
    }

    /// Looks up u32s for a batch of external IDs without assigning.
    ///
    /// Returns `Vec<Option<u32>>` in the same order as the input.
    /// More efficient than calling [`lookup`](Self::lookup) in a loop
    /// (single LMDB read transaction for all lookups).
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn lookup_batch(&self, external_ids: &[&str]) -> crate::Result<Vec<Option<u32>>> {
        let rtxn = self.catalog.env().read_txn()?;
        let mut results = Vec::with_capacity(external_ids.len());
        for &ext in external_ids {
            let v = self.catalog.dict_fwd.get(&rtxn, ext)?.map(|id| id as u32);
            results.push(v);
        }
        Ok(results)
    }

    /// Resolves a u32 back to its external ID string.
    ///
    /// Returns `None` if the u32 has never been assigned.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn resolve(&self, internal_id: u32) -> crate::Result<Option<String>> {
        dict::reverse_lookup(&self.catalog, internal_id)
    }

    /// Resolves a batch of u32s back to their external ID strings.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn resolve_batch(&self, internal_ids: &[u32]) -> crate::Result<Vec<Option<String>>> {
        dict::batch_reverse_lookup(&self.catalog, internal_ids)
    }

    /// Deletes an external ID from both forward and reverse mappings.
    ///
    /// The u32 slot is not recycled; the ID space is consumed monotonically.
    ///
    /// Returns the u32 that was assigned, or `None` if the ID was not found.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn delete(&self, external_id: &str) -> crate::Result<Option<u32>> {
        dict::delete(&self.catalog, external_id)
    }

    /// Returns the number of entries in the dictionary.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn len(&self) -> crate::Result<u64> {
        let rtxn = self.catalog.env().read_txn()?;
        let count = self.catalog.dict_fwd.len(&rtxn)?;
        Ok(count)
    }

    /// Returns `true` if the dictionary is empty.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn is_empty(&self) -> crate::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// UUID support: assigns or retrieves a u32 for a UUID.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    #[cfg(feature = "uuid")]
    pub fn get_or_assign_uuid(&self, id: &uuid::Uuid) -> crate::Result<u32> {
        dict::assign_or_get_uuid(&self.catalog, id)
    }

    /// UUID support: resolves a u32 back to a UUID.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    #[cfg(feature = "uuid")]
    pub fn resolve_uuid(&self, internal_id: u32) -> crate::Result<Option<uuid::Uuid>> {
        dict::reverse_lookup_uuid(&self.catalog, internal_id)
    }

    /// UUID support: assigns or retrieves u32s for a batch of UUIDs.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    #[cfg(feature = "uuid")]
    pub fn get_or_assign_uuid_batch(&self, ids: &[uuid::Uuid]) -> crate::Result<Vec<u32>> {
        dict::batch_assign_or_get_uuids(&self.catalog, ids)
    }

    /// UUID support: resolves a batch of u32s back to UUIDs.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    #[cfg(feature = "uuid")]
    pub fn resolve_uuid_batch(
        &self,
        internal_ids: &[u32],
    ) -> crate::Result<Vec<Option<uuid::Uuid>>> {
        dict::batch_reverse_lookup_uuids(&self.catalog, internal_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn standalone_roundtrip() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let id1 = dict.get_or_assign("alice").unwrap();
        let id2 = dict.get_or_assign("bob").unwrap();
        let id1_again = dict.get_or_assign("alice").unwrap();
        assert_eq!(id1, id1_again);
        assert_ne!(id1, id2);

        assert_eq!(dict.resolve(id1).unwrap().as_deref(), Some("alice"));
        assert_eq!(dict.lookup("bob").unwrap(), Some(id2));
        assert_eq!(dict.lookup("unknown").unwrap(), None);
    }

    #[test]
    fn standalone_batch() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let ids = dict.get_or_assign_batch(&["x", "y", "z"]).unwrap();
        assert_eq!(ids.len(), 3);

        let resolved = dict.resolve_batch(&ids).unwrap();
        assert_eq!(resolved[0].as_deref(), Some("x"));
        assert_eq!(resolved[1].as_deref(), Some("y"));
        assert_eq!(resolved[2].as_deref(), Some("z"));
    }

    #[test]
    fn standalone_len_and_delete() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        assert!(dict.is_empty().unwrap());
        dict.get_or_assign("a").unwrap();
        dict.get_or_assign("b").unwrap();
        assert_eq!(dict.len().unwrap(), 2);

        dict.delete("a").unwrap();
        assert_eq!(dict.len().unwrap(), 1);
        assert!(dict.lookup("a").unwrap().is_none());
    }

    #[test]
    fn shared_with_store() {
        // Dictionary opened after store close shares the ID space.
        let dir = TempDir::new().unwrap();
        let store_path = dir.path().join("data");

        {
            let store = crate::InoxSet::builder().path(&store_path).open().unwrap();
            store
                .put_ids("seg", crate::types::Period::Day(2026, 4, 1), &["alice"])
                .unwrap();
            store.flush().unwrap();
            store.close().unwrap();
        }

        // Store is closed. Open dictionary on the same catalog path.
        let dict = Dictionary::open(store_path.join("catalog.mdb")).unwrap();
        let alice_id = dict.lookup("alice").unwrap();
        assert!(alice_id.is_some());

        let _bob_id = dict.get_or_assign("bob").unwrap();
    }

    #[test]
    fn idempotent_assign() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        // Same ID assigned 1000 times → always same u32.
        let first = dict.get_or_assign("usr-stable").unwrap();
        for _ in 0..1000 {
            assert_eq!(dict.get_or_assign("usr-stable").unwrap(), first);
        }
    }

    #[test]
    fn monotonic_ids() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let mut ids = Vec::new();
        for i in 0..100 {
            ids.push(dict.get_or_assign(&format!("usr-{i}")).unwrap());
        }
        // IDs are sequential starting from 0.
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*id, i as u32);
        }
    }

    #[test]
    fn delete_then_lookup_returns_none() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let id = dict.get_or_assign("to-delete").unwrap();
        assert_eq!(dict.resolve(id).unwrap().as_deref(), Some("to-delete"));

        let deleted = dict.delete("to-delete").unwrap();
        assert_eq!(deleted, Some(id));

        assert!(dict.lookup("to-delete").unwrap().is_none());
        assert!(dict.resolve(id).unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        assert_eq!(dict.delete("ghost").unwrap(), None);
    }

    #[test]
    fn reopen_persists() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dict.mdb");

        let id = {
            let dict = Dictionary::open(&path).unwrap();
            dict.get_or_assign("persistent").unwrap()
        };

        // Reopen — data survives.
        let dict = Dictionary::open(&path).unwrap();
        assert_eq!(dict.lookup("persistent").unwrap(), Some(id));
        assert_eq!(dict.len().unwrap(), 1);
    }

    #[test]
    fn empty_string_id_rejected() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        // LMDB rejects empty keys (BadValSize).
        assert!(dict.get_or_assign("").is_err());
    }

    #[test]
    fn unicode_ids() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let id_fr = dict.get_or_assign("utilisateur-éàü").unwrap();
        let id_jp = dict.get_or_assign("ユーザー").unwrap();
        let id_emoji = dict.get_or_assign("👤-user-🎯").unwrap();

        assert_ne!(id_fr, id_jp);
        assert_ne!(id_jp, id_emoji);

        assert_eq!(
            dict.resolve(id_fr).unwrap().as_deref(),
            Some("utilisateur-éàü")
        );
        assert_eq!(dict.resolve(id_jp).unwrap().as_deref(), Some("ユーザー"));
    }

    #[test]
    fn contains_check() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        assert!(!dict.contains("nope").unwrap());
        dict.get_or_assign("exists").unwrap();
        assert!(dict.contains("exists").unwrap());
    }

    #[test]
    fn lookup_batch_returns_options() {
        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        dict.get_or_assign("a").unwrap();
        dict.get_or_assign("c").unwrap();

        let results = dict.lookup_batch(&["a", "b", "c"]).unwrap();
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn uuid_batch_roundtrip() {
        use uuid::Uuid;

        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let u32s = dict.get_or_assign_uuid_batch(&ids).unwrap();
        assert_eq!(u32s.len(), 5);

        let resolved = dict.resolve_uuid_batch(&u32s).unwrap();
        for (i, r) in resolved.iter().enumerate() {
            assert_eq!(r.as_ref(), Some(&ids[i]));
        }
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn uuid_roundtrip() {
        use uuid::Uuid;

        let dir = TempDir::new().unwrap();
        let dict = Dictionary::open(dir.path().join("dict.mdb")).unwrap();

        let id = Uuid::new_v4();
        let u32_id = dict.get_or_assign_uuid(&id).unwrap();
        let resolved = dict.resolve_uuid(u32_id).unwrap();
        assert_eq!(resolved, Some(id));
    }
}
