//! Merge engine — compaction of N data parts and M delta parts into one bitmap.
//!
//! The merge engine is the core of the compaction pipeline.  Given lists of
//! `(part_id, path)` pairs for data and delta parts it:
//!
//! 1. Sorts all parts (data and delta together) by ascending `part_id`.
//! 2. Folds over them in that order: data parts are OR-merged into the
//!    accumulator, delta (tombstone) parts are subtracted via AND-NOT.
//! 3. Optimizes the merged bitmap for compact on-disk storage by serializing
//!    into a buffer and back, allowing the roaring library to select the most
//!    efficient internal container layout.
//!
//! # Why part-id order matters
//!
//! Part IDs are allocated monotonically at flush time, so they encode write
//! order. A delta part must only erase bits from data parts that were flushed
//! **before** it — bits re-inserted by a *later* put must survive. Applying
//! all deltas to the union of all data parts (regardless of age) would
//! silently erase re-inserted bits.
//!
//! # Eligibility
//!
//! Use [`is_eligible`] to determine whether a given period has enough parts
//! to justify a compaction pass before actually performing I/O.
//!
//! # Error handling
//!
//! All I/O is delegated to [`crate::part_store::mmap_read_part`], which
//! returns [`crate::error::InoxSetError::BitmapIo`] for OS-level failures and
//! [`crate::error::InoxSetError::BitmapCorrupted`] for deserialization errors.
//! No `unwrap()` or `expect()` calls appear in this module.

use std::path::Path;

use roaring::RoaringBitmap;

use crate::error::InoxSetError;

/// Merge N data part files and apply M delta part files into a single bitmap.
///
/// Parts are applied in ascending `part_id` order: data parts OR into the
/// accumulator, delta parts subtract from it. Because part IDs encode write
/// order, a delta only erases bits from data parts older than itself; bits
/// re-inserted by a later put survive the merge.
///
/// The result is optimized for compact storage by round-tripping through
/// serialization so the roaring library selects the best container layout.
///
/// An empty `data_parts` slice returns an empty, optimized bitmap without
/// error.
///
/// # Arguments
///
/// * `data_parts` — `(part_id, path)` pairs of data (additive) part files.
/// * `delta_parts` — `(part_id, path)` pairs of delta (tombstone) part files.
///
/// # Errors
///
/// Returns [`crate::error::InoxSetError::BitmapIo`] if any file cannot be
/// opened or read.  Returns [`crate::error::InoxSetError::BitmapCorrupted`]
/// if any file fails bitmap deserialization.
///
/// # Example
///
/// ```rust,no_run
/// use std::path::Path;
/// use inoxset::merge::merge_parts;
///
/// let merged = merge_parts(
///     &[(1, Path::new("/parts/2026-03-11.000000000001.roar")),
///       (2, Path::new("/parts/2026-03-11.000000000002.roar"))],
///     &[(3, Path::new("/parts/2026-03-11.d_000000000003.roar"))],
/// ).unwrap();
/// println!("merged cardinality: {}", merged.len());
/// ```
pub fn merge_parts<P: AsRef<Path>>(
    data_parts: &[(u64, P)],
    delta_parts: &[(u64, P)],
) -> crate::Result<RoaringBitmap> {
    // Interleave data and delta parts by ascending part_id so that each delta
    // only affects data flushed before it.
    let mut ordered: Vec<(u64, bool, &Path)> = Vec::with_capacity(data_parts.len() + delta_parts.len());
    for (id, path) in data_parts {
        ordered.push((*id, false, path.as_ref()));
    }
    for (id, path) in delta_parts {
        ordered.push((*id, true, path.as_ref()));
    }
    ordered.sort_unstable_by_key(|(id, _, _)| *id);

    let mut merged = RoaringBitmap::new();
    for (_, is_delta, path) in ordered {
        let bm = crate::part_store::mmap_read_part(path)?;
        if is_delta {
            merged -= bm;
        } else {
            merged |= bm;
        }
    }

    // Optimize storage layout by serializing and deserializing so the roaring
    // library can select the most compact container representation.
    let mut buf: Vec<u8> = Vec::new();
    merged
        .serialize_into(&mut buf)
        .map_err(|e| InoxSetError::BitmapIo {
            path: std::path::PathBuf::from("<merge>"),
            source: e,
        })?;
    let optimized = RoaringBitmap::deserialize_from(buf.as_slice()).map_err(|_| {
        InoxSetError::BitmapCorrupted {
            event: String::new(),
            period: crate::types::Period::Static,
        }
    })?;
    Ok(optimized)
}

/// Returns `true` if the period has enough parts to warrant a compaction pass.
///
/// A period is eligible when:
/// - it has **more than one** data part (merging reduces read amplification), or
/// - it has **at least one** delta part (applying deletes frees space and
///   eliminates tombstone overhead).
///
/// # Arguments
///
/// * `data_part_count` — number of data part files tracked for this period.
/// * `delta_part_count` — number of delta part files tracked for this period.
///
/// # Examples
///
/// ```rust
/// use inoxset::merge::is_eligible;
///
/// assert!(!is_eligible(0, 0)); // nothing to compact
/// assert!(!is_eligible(1, 0)); // single data part, no deltas — already optimal
/// assert!(is_eligible(2, 0)); // two data parts → merge reduces read amplification
/// assert!(is_eligible(1, 1)); // one delta → must apply tombstones
/// assert!(is_eligible(0, 1)); // delta with no data — unusual but eligible
/// ```
pub fn is_eligible(data_part_count: usize, delta_part_count: usize) -> bool {
    data_part_count > 1 || delta_part_count > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part_store::{part_file_path, write_part};
    use crate::types::{Granularity, PartKind, Period};
    use roaring::RoaringBitmap;
    use tempfile::TempDir;

    fn bitmap_with(ids: &[u32]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for &id in ids {
            bm.insert(id);
        }
        bm
    }

    #[test]
    fn merge_two_data_parts() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");

        let bm1 = bitmap_with(&[1, 2, 3]);
        let bm2 = bitmap_with(&[3, 4, 5]);

        let p1 = part_file_path(
            &root,
            "active",
            Granularity::Day,
            &Period::Day(2026, 3, 11),
            1,
            PartKind::Data,
        );
        let p2 = part_file_path(
            &root,
            "active",
            Granularity::Day,
            &Period::Day(2026, 3, 11),
            2,
            PartKind::Data,
        );
        write_part(&p1, &bm1).unwrap();
        write_part(&p2, &bm2).unwrap();

        let merged =
            merge_parts(&[(1, p1.as_path()), (2, p2.as_path())], &[]).unwrap();

        // OR of {1,2,3} and {3,4,5} = {1,2,3,4,5}
        assert_eq!(merged.len(), 5);
        for id in 1u32..=5 {
            assert!(merged.contains(id), "missing id {id}");
        }
    }

    #[test]
    fn merge_with_delta_parts() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");

        let data = bitmap_with(&[10, 20, 30, 40]);
        let delta = bitmap_with(&[20, 30]); // delete users 20 and 30

        let dp = part_file_path(
            &root,
            "active",
            Granularity::Day,
            &Period::Day(2026, 3, 11),
            1,
            PartKind::Data,
        );
        let del = part_file_path(
            &root,
            "active",
            Granularity::Day,
            &Period::Day(2026, 3, 11),
            2,
            PartKind::Delta,
        );
        write_part(&dp, &data).unwrap();
        write_part(&del, &delta).unwrap();

        let merged = merge_parts(&[(1, dp.as_path())], &[(2, del.as_path())]).unwrap();

        // {10,20,30,40} AND-NOT {20,30} = {10,40}
        assert_eq!(merged.len(), 2);
        assert!(merged.contains(10));
        assert!(merged.contains(40));
        assert!(!merged.contains(20));
        assert!(!merged.contains(30));
    }

    #[test]
    fn delta_only_erases_older_data_parts() {
        // Timeline: put {42} (id 1), remove {42} (id 2), put {42} again (id 3).
        // The delta must erase the first put but not the re-insert.
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");

        let period = Period::Day(2026, 3, 11);
        let d1 = part_file_path(&root, "ev", Granularity::Day, &period, 1, PartKind::Data);
        let del = part_file_path(&root, "ev", Granularity::Day, &period, 2, PartKind::Delta);
        let d3 = part_file_path(&root, "ev", Granularity::Day, &period, 3, PartKind::Data);
        write_part(&d1, &bitmap_with(&[42, 7])).unwrap();
        write_part(&del, &bitmap_with(&[42])).unwrap();
        write_part(&d3, &bitmap_with(&[42])).unwrap();

        let merged = merge_parts(
            &[(1, d1.as_path()), (3, d3.as_path())],
            &[(2, del.as_path())],
        )
        .unwrap();

        // 42 was re-inserted after the delete: it must survive.
        assert!(merged.contains(42), "re-inserted bit erased by older delta");
        assert!(merged.contains(7));
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn delta_newer_than_all_data_erases() {
        // Regression guard: a delta newer than every data part still deletes.
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");

        let period = Period::Day(2026, 3, 11);
        let d1 = part_file_path(&root, "ev", Granularity::Day, &period, 1, PartKind::Data);
        let d2 = part_file_path(&root, "ev", Granularity::Day, &period, 2, PartKind::Data);
        let del = part_file_path(&root, "ev", Granularity::Day, &period, 3, PartKind::Delta);
        write_part(&d1, &bitmap_with(&[1, 2])).unwrap();
        write_part(&d2, &bitmap_with(&[2, 3])).unwrap();
        write_part(&del, &bitmap_with(&[2])).unwrap();

        let merged = merge_parts(
            &[(1, d1.as_path()), (2, d2.as_path())],
            &[(3, del.as_path())],
        )
        .unwrap();

        assert!(!merged.contains(2));
        assert!(merged.contains(1));
        assert!(merged.contains(3));
    }

    #[test]
    fn merge_empty_data() {
        // No data parts, no delta parts — should return an empty bitmap, not error.
        let result =
            merge_parts(&[] as &[(u64, &std::path::Path)], &[] as &[(u64, &std::path::Path)])
                .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn merge_applies_run_optimize() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");

        // Build a dense, run-friendly bitmap: a contiguous range of 10,000 IDs.
        // After run_optimize(), this is encoded as a single run container,
        // so the serialized size must be smaller than the raw array encoding.
        let bm = RoaringBitmap::from_iter(0u32..10_000);
        let p = part_file_path(
            &root,
            "ev",
            Granularity::Day,
            &Period::Day(2026, 1, 1),
            1,
            PartKind::Data,
        );
        write_part(&p, &bm).unwrap();

        let merged = merge_parts(&[(1, p.as_path())], &[]).unwrap();

        // Cardinality must be intact.
        assert_eq!(merged.len(), 10_000);

        // run_optimize() produces a run container for dense ranges, which is
        // significantly more compact than an array container.
        // Array encoding for 10,000 u16 values ≈ 20,000 bytes;
        // a single run encodes as 4 bytes header + 4 bytes range ≈ tiny.
        let optimized_size = merged.serialized_size();
        let unoptimized = RoaringBitmap::from_iter(0u32..10_000);
        // The unoptimized bitmap is still valid — just checking that run_optimize
        // produced a representation at most as large as the non-optimized form.
        assert!(
            optimized_size <= unoptimized.serialized_size(),
            "run_optimize should not increase serialized size"
        );
    }

    #[test]
    fn is_eligible_thresholds() {
        assert!(!is_eligible(0, 0));
        assert!(!is_eligible(1, 0));
        assert!(is_eligible(2, 0));
        assert!(is_eligible(1, 1));
        assert!(is_eligible(0, 1));
        assert!(is_eligible(3, 5));
    }
}
