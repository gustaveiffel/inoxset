//! Part Store — low-level bitmap file I/O for immutable part files.
//!
//! This module handles the complete lifecycle of on-disk part files:
//! path derivation, atomic writes, standard and memory-mapped reads,
//! deletion, and orphan detection.
//!
//! # Disk layout
//!
//! ```text
//! parts/
//!   {event}/
//!     {granularity}/          ← e.g. "hour", "day"
//!       {period_key}.{part_id:012}.roar          ← Data part
//!       {period_key}.d_{part_id:012}.roar        ← Delta part
//! ```
//!
//! Files are written atomically: the bitmap is serialised into an in-memory
//! buffer, written to disk, and then `sync_all()` is called before returning
//! to the caller.

use std::{
    collections::HashSet,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use roaring::RoaringBitmap;

use crate::{
    error::InoxSetError,
    types::{Granularity, Part, PartKind, Period},
    Result,
};

/// Derives the canonical file-system path for a part file.
///
/// # Arguments
///
/// * `parts_root` — root directory that contains all part sub-directories
///   (usually `<store_dir>/parts`).
/// * `event` — logical event name (e.g. `"active"`).
/// * `gran` — time granularity of the period (e.g. [`Granularity::Hour`]).
/// * `period` — the time period the part covers.
/// * `part_id` — the catalog-assigned monotonic identifier for the part.
/// * `kind` — whether the part is a [`PartKind::Data`] or
///   [`PartKind::Delta`] file.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
/// use inoxset::part_store::part_file_path;
/// use inoxset::types::{Granularity, Period, PartKind};
///
/// let root = PathBuf::from("/data/parts");
/// let path = part_file_path(&root, "active", Granularity::Hour,
///     &Period::Hour(2026, 3, 11, 14), 1, PartKind::Data);
/// assert_eq!(path.file_name().unwrap().to_str().unwrap(),
///     "2026-03-11T14.000000000001.roar");
/// ```
pub fn part_file_path(
    parts_root: &Path,
    event: &str,
    gran: Granularity,
    period: &Period,
    part_id: u64,
    kind: PartKind,
) -> PathBuf {
    let dir = parts_root.join(event).join(gran.dir_name());
    let period_key = period.key();
    let filename = match kind {
        PartKind::Data => format!("{}.{:012}.roar", period_key, part_id),
        PartKind::Delta => format!("{}.d_{:012}.roar", period_key, part_id),
    };
    dir.join(filename)
}

/// Writes a [`RoaringBitmap`] to an immutable part file atomically.
///
/// The bitmap is serialised into an in-memory buffer, the parent directories
/// are created if they do not exist, the file is written, and `sync_all()` is
/// called before returning to guarantee durability.
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if directory creation, file creation,
/// bitmap serialisation, or the `sync_all()` call fails.
pub fn write_part(path: &Path, bitmap: &RoaringBitmap) -> Result<()> {
    // Serialise into a Vec<u8> first so that a failed serialise does not
    // leave a partial file on disk.
    let mut buf: Vec<u8> = Vec::new();
    bitmap
        .serialize_into(&mut buf)
        .map_err(|e| InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        })?;

    // Ensure the parent directory hierarchy exists.
    let parent = path.parent().ok_or_else(|| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "no parent directory"),
    })?;
    fs::create_dir_all(parent).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    // Write to a temporary file, sync, then atomically rename into place.
    // This prevents partial files from being visible on crash.
    let tmp_path = path.with_extension("roar.tmp");

    let mut file = fs::File::create(&tmp_path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    file.write_all(&buf).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        }
    })?;

    // Flush OS buffers to durable storage before rename.
    file.sync_all().map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        }
    })?;

    // Atomic rename: on Unix this is guaranteed atomic by POSIX.
    fs::rename(&tmp_path, path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    Ok(())
}

/// Reads a [`RoaringBitmap`] from a part file using a standard buffered read.
///
/// This is the default read path. For large sets where OS page-cache reuse is
/// important, prefer [`mmap_read_part`].
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if the file cannot be opened or read.
/// Returns [`InoxSetError::BitmapCorrupted`] if deserialization fails.
///
/// *Note:* The `event` field of `BitmapCorrupted` is left as an empty string
/// and `period` as [`Period::Static`] because this function does not carry
/// that context; callers should wrap or re-map the error with the correct
/// event/period values where needed.
pub fn read_part(path: &Path) -> Result<RoaringBitmap> {
    let bytes = fs::read(path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    RoaringBitmap::deserialize_from(bytes.as_slice()).map_err(|_| InoxSetError::BitmapCorrupted {
        event: String::new(),
        period: Period::Static,
    })
}

/// Reads a [`RoaringBitmap`] from a part file via a memory-mapped region.
///
/// Memory-mapping allows the OS page cache to be shared across multiple
/// readers of the same file without duplicating bytes in user-space memory.
/// This is the preferred read path during query fanout over many parts.
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if the file cannot be opened or mapped.
/// Returns [`InoxSetError::BitmapCorrupted`] if deserialization fails.
///
/// *Note:* Like [`read_part`], the `event` and `period` fields of
/// `BitmapCorrupted` are set to placeholder values; callers are expected to
/// re-map with correct context.
pub fn mmap_read_part(path: &Path) -> Result<RoaringBitmap> {
    let file = fs::File::open(path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    // SAFETY: Part files are immutable once written — no writer will mutate
    // the file's content after the initial `write_part` + `sync_all` call.
    // The mapping is read-only, so there is no risk of aliased mutable
    // references. Concurrent writes to the same path during an active mapping
    // would violate this invariant, but the engine's part lifecycle
    // (write-once, delete-only-after-compaction) prevents that.
    let mmap = unsafe {
        memmap2::Mmap::map(&file).map_err(|e| InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        })?
    };

    RoaringBitmap::deserialize_from(mmap.as_ref()).map_err(|_| InoxSetError::BitmapCorrupted {
        event: String::new(),
        period: Period::Static,
    })
}

/// Checks membership of a single `u32` value directly on serialized Roaring
/// Bitmap bytes, **without deserializing** the full bitmap into memory.
///
/// This performs a binary search on the container descriptive header, then
/// a targeted check within the matched container (array binary search or
/// bitmap bit test). The cost is O(log C + log N) where C is the number of
/// containers and N is the container cardinality — typically under 200ns.
///
/// Returns `false` for empty or malformed data (no panic, no error).
pub fn serialized_contains(data: &[u8], value: u32) -> bool {
    let hi = (value >> 16) as u16;
    let lo = (value & 0xFFFF) as u16;

    // Minimum valid size: 4 bytes cookie + 4 bytes container count or cookie.
    if data.len() < 4 {
        return false;
    }

    let cookie = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

    const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
    const SERIAL_COOKIE: u16 = 12347;
    // The roaring crate always writes offset headers regardless of container
    // count. The spec says >= 4, but the Rust crate includes them always.
    const NO_OFFSET_THRESHOLD: usize = 1;

    let (nb_containers, has_runs, header_start) = if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
        if data.len() < 8 {
            return false;
        }
        let n = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        (n, false, 8)
    } else if (cookie & 0xFFFF) as u16 == SERIAL_COOKIE {
        let n = ((cookie >> 16) + 1) as usize;
        (n, true, 4)
    } else {
        return false;
    };

    if nb_containers == 0 {
        return false;
    }

    // Skip run bitmap if present: ceil(nb_containers / 8) bytes.
    let run_bitmap_start = header_start;
    let run_bitmap_bytes = if has_runs {
        nb_containers.div_ceil(8)
    } else {
        0
    };
    let desc_start = header_start + run_bitmap_bytes;

    // Descriptive header: 4 bytes per container (key_hi: u16, card_minus_1: u16).
    let desc_end = desc_start + nb_containers * 4;
    if data.len() < desc_end {
        return false;
    }

    // Binary search for the container with key == hi.
    let container_idx = {
        let mut lo_idx = 0usize;
        let mut hi_idx = nb_containers;
        let mut found = None;
        while lo_idx < hi_idx {
            let mid = lo_idx + (hi_idx - lo_idx) / 2;
            let off = desc_start + mid * 4;
            let key = u16::from_le_bytes([data[off], data[off + 1]]);
            match key.cmp(&hi) {
                std::cmp::Ordering::Equal => {
                    found = Some(mid);
                    break;
                }
                std::cmp::Ordering::Less => lo_idx = mid + 1,
                std::cmp::Ordering::Greater => hi_idx = mid,
            }
        }
        match found {
            Some(idx) => idx,
            None => return false,
        }
    };

    // Read cardinality - 1 for this container.
    let desc_off = desc_start + container_idx * 4;
    let card_minus_1 = u16::from_le_bytes([data[desc_off + 2], data[desc_off + 3]]) as usize;
    let cardinality = card_minus_1 + 1;

    // Determine if this is a run container.
    let is_run = has_runs && {
        let byte_idx = run_bitmap_start + container_idx / 8;
        let bit_idx = container_idx % 8;
        data.get(byte_idx).is_some_and(|b| b & (1 << bit_idx) != 0)
    };

    // Compute offset to the container's data.
    let container_data_offset = if nb_containers >= NO_OFFSET_THRESHOLD {
        // Offset header present after descriptive header.
        let offset_header_start = desc_end;
        let offset_off = offset_header_start + container_idx * 4;
        if data.len() < offset_off + 4 {
            return false;
        }
        u32::from_le_bytes([
            data[offset_off],
            data[offset_off + 1],
            data[offset_off + 2],
            data[offset_off + 3],
        ]) as usize
    } else {
        // No offset header — compute sequentially.
        let mut offset = desc_end; // after descriptive header (no offset header)
        for i in 0..container_idx {
            let d = desc_start + i * 4;
            let c = u16::from_le_bytes([data[d + 2], data[d + 3]]) as usize + 1;
            let is_run_i = has_runs && {
                let bi = run_bitmap_start + i / 8;
                let bit = i % 8;
                data.get(bi).is_some_and(|b| b & (1 << bit) != 0)
            };
            if is_run_i {
                // Run container: first u16 is number of runs, then pairs.
                if data.len() < offset + 2 {
                    return false;
                }
                let n_runs = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
                offset += 2 + n_runs * 4;
            } else if c <= 4096 {
                offset += c * 2; // array container
            } else {
                offset += 8192; // bitmap container
            }
        }
        offset
    };

    if is_run {
        // Run container: u16 n_runs, then n_runs × (start: u16, length: u16).
        if data.len() < container_data_offset + 2 {
            return false;
        }
        let n_runs =
            u16::from_le_bytes([data[container_data_offset], data[container_data_offset + 1]])
                as usize;
        let runs_start = container_data_offset + 2;
        if data.len() < runs_start + n_runs * 4 {
            return false;
        }
        // Binary search runs for lo.
        let mut lo_idx = 0usize;
        let mut hi_idx = n_runs;
        while lo_idx < hi_idx {
            let mid = lo_idx + (hi_idx - lo_idx) / 2;
            let off = runs_start + mid * 4;
            let start = u16::from_le_bytes([data[off], data[off + 1]]);
            let length = u16::from_le_bytes([data[off + 2], data[off + 3]]);
            if lo < start {
                hi_idx = mid;
            } else if lo > start + length {
                lo_idx = mid + 1;
            } else {
                return true;
            }
        }
        false
    } else if cardinality <= 4096 {
        // Array container: sorted u16 values, binary search.
        let arr_start = container_data_offset;
        if data.len() < arr_start + cardinality * 2 {
            return false;
        }
        let mut lo_idx = 0usize;
        let mut hi_idx = cardinality;
        while lo_idx < hi_idx {
            let mid = lo_idx + (hi_idx - lo_idx) / 2;
            let off = arr_start + mid * 2;
            let val = u16::from_le_bytes([data[off], data[off + 1]]);
            match val.cmp(&lo) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => lo_idx = mid + 1,
                std::cmp::Ordering::Greater => hi_idx = mid,
            }
        }
        false
    } else {
        // Bitmap container: 8192 bytes = 65536 bits.
        let bm_start = container_data_offset;
        if data.len() < bm_start + 8192 {
            return false;
        }
        let byte_idx = bm_start + (lo as usize) / 8;
        let bit_idx = (lo as usize) % 8;
        data[byte_idx] & (1 << bit_idx) != 0
    }
}

/// Checks membership of a `u32` value in a part file without deserializing
/// the bitmap.
///
/// Memory-maps the file and calls [`serialized_contains`] directly on the
/// mapped bytes. This is ~100x faster than [`mmap_read_part`] followed by
/// `bitmap.contains()` for single-value lookups.
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if the file cannot be opened or mapped.
/// Checks membership of a `u32` value in a part file without deserializing
/// the bitmap.
///
/// For files ≤ 64 KiB, reads into a stack-friendly buffer to avoid mmap
/// syscall overhead. For larger files, uses memory-mapping.
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if the file cannot be opened or read.
pub fn mmap_contains(path: &Path, value: u32) -> Result<bool> {
    let meta = fs::metadata(path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    if meta.len() <= 65_536 {
        // Small file: direct read is faster than mmap (no kernel mapping overhead).
        let bytes = fs::read(path).map_err(|e| InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        })?;
        return Ok(serialized_contains(&bytes, value));
    }

    // Large file: mmap for OS page cache sharing.
    let file = fs::File::open(path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })?;

    // SAFETY: same invariants as mmap_read_part — immutable part files.
    let mmap = unsafe {
        memmap2::Mmap::map(&file).map_err(|e| InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        })?
    };

    Ok(serialized_contains(&mmap, value))
}

/// Removes a part file from disk.
///
/// This is a thin wrapper around [`std::fs::remove_file`] that maps the error
/// to [`InoxSetError::BitmapIo`] with the offending path attached.
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if the file cannot be removed (e.g. it
/// does not exist, or a permissions error occurs).
pub fn delete_part(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|e| InoxSetError::BitmapIo {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Scans `parts_root` recursively for `.roar` files whose parsed `part_id` is
/// not present in `known_ids`, returning the paths of those orphaned files.
///
/// An orphan is a `.roar` file on disk that is not tracked by the catalog.
/// This can happen after a crash between a file write and the catalog commit,
/// or after manual intervention.  The returned paths can be passed to
/// [`delete_part`] to reclaim disk space.
///
/// Files whose names do not conform to the expected naming convention are
/// silently skipped (they may be temporary files, `.gitkeep`, etc.).
///
/// # Errors
///
/// Returns [`InoxSetError::BitmapIo`] if directory traversal fails.
pub fn scan_orphans(parts_root: &Path, known_ids: &HashSet<u64>) -> Result<Vec<PathBuf>> {
    let mut orphans = Vec::new();
    scan_dir_for_orphans(parts_root, known_ids, &mut orphans)?;
    Ok(orphans)
}

/// Recursive helper for [`scan_orphans`].
fn scan_dir_for_orphans(
    dir: &Path,
    known_ids: &HashSet<u64>,
    orphans: &mut Vec<PathBuf>,
) -> Result<()> {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(InoxSetError::BitmapIo {
                path: dir.to_path_buf(),
                source: e,
            })
        }
    };

    for entry in read_dir {
        let entry = entry.map_err(|e| InoxSetError::BitmapIo {
            path: dir.to_path_buf(),
            source: e,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| InoxSetError::BitmapIo {
            path: path.clone(),
            source: e,
        })?;

        if file_type.is_dir() {
            scan_dir_for_orphans(&path, known_ids, orphans)?;
        } else if file_type.is_file() {
            // Only inspect files with the `.roar` extension.
            if path.extension().and_then(|e| e.to_str()) != Some("roar") {
                continue;
            }
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(part_id) = extract_part_id_from_filename(filename) {
                    if !known_ids.contains(&part_id) {
                        orphans.push(path);
                    }
                }
                // Files that don't parse are silently skipped.
            }
        }
    }

    Ok(())
}

/// Parses the `part_id` from a part filename.
///
/// Recognised formats:
/// - Data part: `{period_key}.{part_id:012}.roar`
/// - Delta part: `{period_key}.d_{part_id:012}.roar`
///
/// Returns `None` if `filename` does not match either pattern.
///
/// # Examples
///
/// ```rust
/// use inoxset::part_store::extract_part_id_from_filename;
///
/// assert_eq!(extract_part_id_from_filename("2026-03-11T14.000000000001.roar"), Some(1));
/// assert_eq!(extract_part_id_from_filename("2026-03-11T14.d_000000000009.roar"), Some(9));
/// assert_eq!(extract_part_id_from_filename("not_a_part_file.txt"), None);
/// ```
pub fn extract_part_id_from_filename(filename: &str) -> Option<u64> {
    // Strip the `.roar` extension.
    let stem = filename.strip_suffix(".roar")?;

    // The part_id is always the last dot-separated segment (possibly prefixed
    // with "d_" for delta parts).
    let last_segment = stem.rsplit('.').next()?;

    // Handle delta prefix: "d_{part_id:012}"
    let id_str = if let Some(s) = last_segment.strip_prefix("d_") {
        s
    } else {
        last_segment
    };

    id_str.parse::<u64>().ok()
}

// Suppress the unused-import warning for `Part` — it is part of the public
// surface re-exported from this module for callers that store part metadata
// alongside their paths.
const _: fn() = || {
    let _: Option<Part> = None;
};

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use tempfile::TempDir;

    #[test]
    fn write_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        bm.insert(1337);
        let path = part_file_path(
            &root,
            "active",
            Granularity::Hour,
            &Period::Hour(2026, 3, 11, 14),
            1,
            PartKind::Data,
        );
        write_part(&path, &bm).unwrap();
        let loaded = read_part(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains(42));
        assert!(loaded.contains(1337));
    }

    #[test]
    fn mmap_read() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");
        let mut bm = RoaringBitmap::new();
        for i in 0..10000 {
            bm.insert(i);
        }
        let path = part_file_path(
            &root,
            "active",
            Granularity::Day,
            &Period::Day(2026, 3, 11),
            5,
            PartKind::Data,
        );
        write_part(&path, &bm).unwrap();
        let loaded = mmap_read_part(&path).unwrap();
        assert_eq!(loaded.len(), 10000);
    }

    #[test]
    fn delta_file_path_format() {
        let root = std::path::PathBuf::from("/data/parts");
        let path = part_file_path(
            &root,
            "active",
            Granularity::Hour,
            &Period::Hour(2026, 3, 11, 14),
            9,
            PartKind::Delta,
        );
        let name = path.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("2026-03-11T14.d_"));
        assert!(name.ends_with(".roar"));
    }

    #[test]
    fn data_file_path_format() {
        let root = std::path::PathBuf::from("/data/parts");
        let path = part_file_path(
            &root,
            "active",
            Granularity::Hour,
            &Period::Hour(2026, 3, 11, 14),
            1,
            PartKind::Data,
        );
        let name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, "2026-03-11T14.000000000001.roar");
    }

    #[test]
    fn orphan_scan() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        let p1 = part_file_path(
            &root,
            "active",
            Granularity::Hour,
            &Period::Hour(2026, 3, 11, 14),
            1,
            PartKind::Data,
        );
        let p2 = part_file_path(
            &root,
            "active",
            Granularity::Hour,
            &Period::Hour(2026, 3, 11, 14),
            2,
            PartKind::Data,
        );
        write_part(&p1, &bm).unwrap();
        write_part(&p2, &bm).unwrap();
        let known: HashSet<u64> = [1].into_iter().collect();
        let orphans = scan_orphans(&root, &known).unwrap();
        assert_eq!(orphans.len(), 1);
        assert!(orphans[0].to_string_lossy().contains("000000000002"));
    }

    #[test]
    fn extract_part_id_data() {
        assert_eq!(
            extract_part_id_from_filename("2026-03-11T14.000000000001.roar"),
            Some(1)
        );
    }

    #[test]
    fn extract_part_id_delta() {
        assert_eq!(
            extract_part_id_from_filename("2026-03-11T14.d_000000000009.roar"),
            Some(9)
        );
    }

    #[test]
    fn extract_part_id_invalid() {
        assert_eq!(extract_part_id_from_filename("not_a_part_file.txt"), None);
        assert_eq!(extract_part_id_from_filename("no_extension"), None);
    }

    #[test]
    fn delete_part_removes_file() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");
        let mut bm = RoaringBitmap::new();
        bm.insert(99);
        let path = part_file_path(
            &root,
            "ev",
            Granularity::Day,
            &Period::Day(2026, 1, 1),
            7,
            PartKind::Data,
        );
        write_part(&path, &bm).unwrap();
        assert!(path.exists());
        delete_part(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn scan_orphans_empty_root() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("parts");
        // Directory doesn't exist yet — scan should return an empty list, not error.
        let known: HashSet<u64> = HashSet::new();
        let orphans = scan_orphans(&root, &known).unwrap();
        assert!(orphans.is_empty());
    }

    #[test]
    fn serialized_contains_array_container() {
        // Small bitmap → array container.
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        bm.insert(1000);
        bm.insert(9999);
        let mut buf = Vec::new();
        bm.serialize_into(&mut buf).unwrap();

        assert!(serialized_contains(&buf, 42));
        assert!(serialized_contains(&buf, 1000));
        assert!(serialized_contains(&buf, 9999));
        assert!(!serialized_contains(&buf, 0));
        assert!(!serialized_contains(&buf, 43));
        assert!(!serialized_contains(&buf, 10000));
    }

    #[test]
    fn serialized_contains_bitmap_container() {
        // Large bitmap in one container → bitmap container (>4096 values).
        let mut bm = RoaringBitmap::new();
        for i in 0..10_000u32 {
            bm.insert(i);
        }
        let mut buf = Vec::new();
        bm.serialize_into(&mut buf).unwrap();

        assert!(serialized_contains(&buf, 0));
        assert!(serialized_contains(&buf, 5000));
        assert!(serialized_contains(&buf, 9999));
        assert!(!serialized_contains(&buf, 10000));
        assert!(!serialized_contains(&buf, u32::MAX));
    }

    #[test]
    fn serialized_contains_multiple_containers() {
        // Values in different high-16-bit buckets → multiple containers.
        let mut bm = RoaringBitmap::new();
        bm.insert(5); // container 0
        bm.insert(65_536 + 10); // container 1
        bm.insert(131_072 + 3); // container 2
        let mut buf = Vec::new();
        bm.serialize_into(&mut buf).unwrap();

        assert!(serialized_contains(&buf, 5));
        assert!(serialized_contains(&buf, 65_546));
        assert!(serialized_contains(&buf, 131_075));
        assert!(!serialized_contains(&buf, 6));
        assert!(!serialized_contains(&buf, 65_535));
        assert!(!serialized_contains(&buf, 65_537));
    }

    #[test]
    fn serialized_contains_empty_and_malformed() {
        assert!(!serialized_contains(&[], 0));
        assert!(!serialized_contains(&[0, 0], 0));

        // Valid empty bitmap.
        let bm = RoaringBitmap::new();
        let mut buf = Vec::new();
        bm.serialize_into(&mut buf).unwrap();
        assert!(!serialized_contains(&buf, 0));
    }

    #[test]
    fn mmap_contains_matches_deserialize() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.roar");

        let mut bm = RoaringBitmap::new();
        for i in (0..100_000u32).step_by(7) {
            bm.insert(i);
        }
        write_part(&path, &bm).unwrap();

        // Spot-check: mmap_contains agrees with deserialized contains.
        for v in [0, 7, 14, 100, 999, 7777, 99_995, 99_996, 99_999] {
            assert_eq!(
                mmap_contains(&path, v).unwrap(),
                bm.contains(v),
                "mismatch for value {v}"
            );
        }
    }
}
