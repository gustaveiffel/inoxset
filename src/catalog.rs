//! Catalog module: LMDB-backed persistent metadata store for inoxset.
//!
//! The catalog stores event configurations, part metadata, period–part
//! associations, delta-part lists, and period lifecycle state.  Every
//! serialized value is prefixed with a version byte (`1`) so that future
//! format changes can be detected at startup.
//!
//! # Databases (LMDB named databases)
//!
//! | Field | Key | Value |
//! |---|---|---|
//! | `events` | event name (`Str`) | serialized [`EventConfig`] (`Bytes`) |
//! | `parts` | `part_id` (`U64`) | serialized [`Part`] (`Bytes`) |
//! | `period_parts` | `"event/gran/period_key"` (`Str`) | packed `Vec<u64>` (`Bytes`) |
//! | `period_deltas` | `"event/gran/period_key"` (`Str`) | packed `Vec<u64>` (`Bytes`) |
//! | `period_state` | `"event/gran/period_key"` (`Str`) | `u8` as 1-byte slice (`Bytes`) |
//! | `compaction_log` | timestamp (`U64`) | raw bytes (`Bytes`) |
//! | `next_part_id` | fixed key `"_"` (`Str`) | next available `u64` (`U64`) |

use std::collections::HashSet;
use std::path::Path;

use heed::types::{Bytes, Str, U64};
use heed::{Database, Env};

use crate::error::InoxSetError;
use crate::types::{EventConfig, Granularity, Part, PartKind, Period, PeriodState, Rollup};

/// Fixed key used for singleton entries (next_part_id, next_dict_id).
const SINGLETON_KEY: &str = "_";

// ─── Serialization helpers ────────────────────────────────────────────────────

/// Serializes an [`EventConfig`] to bytes.
///
/// Layout: `[version=1][granularity u8][rollup u8]`
pub(crate) fn serialize_event_config(ec: &EventConfig) -> Vec<u8> {
    vec![
        1u8,                           // version
        ec.finest_granularity.as_u8(), // granularity
        match ec.rollup {
            // rollup
            Rollup::Auto => 0,
            Rollup::None => 1,
        },
    ]
}

/// Deserializes an [`EventConfig`] from bytes previously written by
/// [`serialize_event_config`].
///
/// # Errors
///
/// Returns [`InoxSetError::CatalogCorrupted`] if `data` is too short, if the
/// version byte is not `1`, or if either enum byte is unrecognised.
pub(crate) fn deserialize_event_config(name: &str, data: &[u8]) -> crate::Result<EventConfig> {
    if data.len() < 3 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!(
                "event '{}': expected at least 3 bytes, got {}",
                name,
                data.len()
            ),
        });
    }
    let version = data[0];
    if version != 1 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!(
                "event '{}': unsupported version byte {} (expected 1)",
                name, version
            ),
        });
    }
    let gran = Granularity::from_u8(data[1]).ok_or_else(|| InoxSetError::CatalogCorrupted {
        context: format!("event '{}': unknown granularity byte {}", name, data[1]),
    })?;
    let rollup = match data[2] {
        0 => Rollup::Auto,
        1 => Rollup::None,
        other => {
            return Err(InoxSetError::CatalogCorrupted {
                context: format!("event '{}': unknown rollup byte {}", name, other),
            })
        }
    };
    Ok(EventConfig::new(name.to_string(), gran, rollup))
}

/// Serializes a [`Period`] into `buf`.
///
/// Layout: `[tag][year u16 LE][month u8][day u8][hour u8]` — trailing fields
/// are omitted when the period variant does not need them.
pub(crate) fn serialize_period_into(buf: &mut Vec<u8>, period: &Period) {
    match *period {
        Period::Static => {
            buf.push(0u8);
        }
        Period::Hour(y, m, d, h) => {
            buf.push(1u8);
            buf.extend_from_slice(&y.to_le_bytes());
            buf.push(m);
            buf.push(d);
            buf.push(h);
        }
        Period::Day(y, m, d) => {
            buf.push(2u8);
            buf.extend_from_slice(&y.to_le_bytes());
            buf.push(m);
            buf.push(d);
        }
        Period::Month(y, m) => {
            buf.push(3u8);
            buf.extend_from_slice(&y.to_le_bytes());
            buf.push(m);
        }
        Period::Year(y) => {
            buf.push(4u8);
            buf.extend_from_slice(&y.to_le_bytes());
        }
    }
}

/// Deserializes a [`Period`] from `data` starting at `offset`.
///
/// Returns `(period, bytes_consumed)`.
///
/// # Errors
///
/// Returns [`InoxSetError::CatalogCorrupted`] on truncated data or unknown tag.
pub(crate) fn deserialize_period(
    ctx: &str,
    data: &[u8],
    offset: usize,
) -> crate::Result<(Period, usize)> {
    if offset >= data.len() {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!(
                "{ctx}: period tag missing (offset {offset}, len {})",
                data.len()
            ),
        });
    }
    let tag = data[offset];
    match tag {
        0 => Ok((Period::Static, 1)),
        1 => {
            // year(2) + month(1) + day(1) + hour(1) = 5
            let need = offset + 1 + 5;
            if data.len() < need {
                return Err(InoxSetError::CatalogCorrupted {
                    context: format!(
                        "{ctx}: truncated Hour period (need {need}, have {})",
                        data.len()
                    ),
                });
            }
            let y = u16::from_le_bytes([data[offset + 1], data[offset + 2]]);
            let m = data[offset + 3];
            let d = data[offset + 4];
            let h = data[offset + 5];
            Ok((Period::Hour(y, m, d, h), 6))
        }
        2 => {
            // year(2) + month(1) + day(1) = 4
            let need = offset + 1 + 4;
            if data.len() < need {
                return Err(InoxSetError::CatalogCorrupted {
                    context: format!(
                        "{ctx}: truncated Day period (need {need}, have {})",
                        data.len()
                    ),
                });
            }
            let y = u16::from_le_bytes([data[offset + 1], data[offset + 2]]);
            let m = data[offset + 3];
            let d = data[offset + 4];
            Ok((Period::Day(y, m, d), 5))
        }
        3 => {
            // year(2) + month(1) = 3
            let need = offset + 1 + 3;
            if data.len() < need {
                return Err(InoxSetError::CatalogCorrupted {
                    context: format!(
                        "{ctx}: truncated Month period (need {need}, have {})",
                        data.len()
                    ),
                });
            }
            let y = u16::from_le_bytes([data[offset + 1], data[offset + 2]]);
            let m = data[offset + 3];
            Ok((Period::Month(y, m), 4))
        }
        4 => {
            // year(2) = 2
            let need = offset + 1 + 2;
            if data.len() < need {
                return Err(InoxSetError::CatalogCorrupted {
                    context: format!(
                        "{ctx}: truncated Year period (need {need}, have {})",
                        data.len()
                    ),
                });
            }
            let y = u16::from_le_bytes([data[offset + 1], data[offset + 2]]);
            Ok((Period::Year(y), 3))
        }
        other => Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: unknown period tag {other}"),
        }),
    }
}

/// Serializes a `Vec<u64>` as a version byte followed by tightly-packed
/// little-endian 8-byte words.
///
/// Layout: `[version=1][id0 u64 LE][id1 u64 LE]…`
pub(crate) fn serialize_u64_vec(ids: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + ids.len() * 8);
    buf.push(1u8); // version
    for id in ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    buf
}

/// Deserializes a `Vec<u64>` from bytes previously written by
/// [`serialize_u64_vec`].
///
/// # Errors
///
/// Returns [`InoxSetError::CatalogCorrupted`] if `data` is too short, if the
/// version byte is not `1`, or if the remaining payload length is not a
/// multiple of 8.
pub(crate) fn deserialize_u64_vec(ctx: &str, data: &[u8]) -> crate::Result<Vec<u64>> {
    if data.is_empty() {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: u64 list missing version byte"),
        });
    }
    let version = data[0];
    if version != 1 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: u64 list unsupported version byte {version} (expected 1)"),
        });
    }
    let payload = &data[1..];
    if payload.len() % 8 != 0 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!(
                "{ctx}: u64 list payload length {} is not a multiple of 8",
                payload.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(payload.len() / 8);
    for chunk in payload.chunks_exact(8) {
        // Safety: chunks_exact(8) guarantees exactly 8 bytes.
        let arr: [u8; 8] = chunk
            .try_into()
            .map_err(|_| InoxSetError::CatalogCorrupted {
                context: format!("{ctx}: chunk conversion failed"),
            })?;
        out.push(u64::from_le_bytes(arr));
    }
    Ok(out)
}

/// Serializes a [`Part`] to bytes.
///
/// Layout:
/// ```text
/// [version=1]
/// [part_id u64 LE]
/// [kind u8]
/// [size_bytes u64 LE]
/// [cardinality u64 LE]
/// [created_at u64 LE]
/// [level u8]
/// [period bytes — variable]
/// [event name: u16 LE length + UTF-8 bytes]
/// [file path: u16 LE length + UTF-8 bytes]
/// ```
///
/// # Errors
///
/// Returns [`InoxSetError::CatalogCorrupted`] if the event name or file path
/// exceeds 65535 bytes (the maximum representable in the u16 length prefix).
pub(crate) fn serialize_part(part: &Part) -> crate::Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.push(1u8); // version
    buf.extend_from_slice(&part.part_id.to_le_bytes());
    buf.push(match part.kind {
        PartKind::Data => 0,
        PartKind::Delta => 1,
    });
    buf.extend_from_slice(&part.size_bytes.to_le_bytes());
    buf.extend_from_slice(&part.cardinality.to_le_bytes());
    buf.extend_from_slice(&part.created_at.to_le_bytes());
    buf.push(part.level);
    serialize_period_into(&mut buf, &part.period);

    // event name
    let event_bytes = part.event.as_bytes();
    let event_len =
        u16::try_from(event_bytes.len()).map_err(|_| InoxSetError::CatalogCorrupted {
            context: format!(
                "part_id={}: event name length {} exceeds u16::MAX",
                part.part_id,
                event_bytes.len()
            ),
        })?;
    buf.extend_from_slice(&event_len.to_le_bytes());
    buf.extend_from_slice(event_bytes);

    // file path
    let path_str = part.file_path.to_string_lossy();
    let path_bytes = path_str.as_bytes();
    let path_len = u16::try_from(path_bytes.len()).map_err(|_| InoxSetError::CatalogCorrupted {
        context: format!(
            "part_id={}: file path length {} exceeds u16::MAX",
            part.part_id,
            path_bytes.len()
        ),
    })?;
    buf.extend_from_slice(&path_len.to_le_bytes());
    buf.extend_from_slice(path_bytes);

    Ok(buf)
}

/// Deserializes a [`Part`] from bytes previously written by [`serialize_part`].
///
/// # Errors
///
/// Returns [`InoxSetError::CatalogCorrupted`] on short data, bad version byte,
/// or any other format violation.
pub(crate) fn deserialize_part(ctx: &str, data: &[u8]) -> crate::Result<Part> {
    // Minimum: 1 (ver) + 8 (part_id) + 1 (kind) + 8 + 8 + 8 (stats) + 1 (level) = 35
    // plus at least 1 byte for period tag + 2+2 for two empty length-prefixed strings
    let min_len = 35 + 1 + 4;
    if data.len() < min_len {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!(
                "{ctx}: part data too short (need at least {min_len}, got {})",
                data.len()
            ),
        });
    }

    let version = data[0];
    if version != 1 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: unsupported part version byte {version} (expected 1)"),
        });
    }

    let mut pos = 1usize;

    // part_id
    if data.len() < pos + 8 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated part_id"),
        });
    }
    let part_id = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
        InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: part_id slice conversion failed"),
        }
    })?);
    pos += 8;

    // kind
    if data.len() < pos + 1 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated kind"),
        });
    }
    let kind = match data[pos] {
        0 => PartKind::Data,
        1 => PartKind::Delta,
        other => {
            return Err(InoxSetError::CatalogCorrupted {
                context: format!("{ctx}: unknown part kind byte {other}"),
            })
        }
    };
    pos += 1;

    // size_bytes
    if data.len() < pos + 8 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated size_bytes"),
        });
    }
    let size_bytes = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
        InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: size_bytes slice conversion failed"),
        }
    })?);
    pos += 8;

    // cardinality
    if data.len() < pos + 8 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated cardinality"),
        });
    }
    let cardinality = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
        InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: cardinality slice conversion failed"),
        }
    })?);
    pos += 8;

    // created_at
    if data.len() < pos + 8 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated created_at"),
        });
    }
    let created_at = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
        InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: created_at slice conversion failed"),
        }
    })?);
    pos += 8;

    // level
    if data.len() < pos + 1 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated level"),
        });
    }
    let level = data[pos];
    pos += 1;

    // period
    let (period, period_len) = deserialize_period(ctx, data, pos)?;
    pos += period_len;

    // event name
    if data.len() < pos + 2 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated event name length"),
        });
    }
    let event_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;
    if data.len() < pos + event_len {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated event name (need {event_len} bytes)"),
        });
    }
    let event = std::str::from_utf8(&data[pos..pos + event_len])
        .map_err(|_| InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: event name is not valid UTF-8"),
        })?
        .to_string();
    pos += event_len;

    // file path
    if data.len() < pos + 2 {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated file path length"),
        });
    }
    let path_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;
    if data.len() < pos + path_len {
        return Err(InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: truncated file path (need {path_len} bytes)"),
        });
    }
    let file_path = std::str::from_utf8(&data[pos..pos + path_len]).map_err(|_| {
        InoxSetError::CatalogCorrupted {
            context: format!("{ctx}: file path is not valid UTF-8"),
        }
    })?;

    Ok(Part {
        part_id,
        kind,
        event,
        period,
        file_path: file_path.into(),
        size_bytes,
        cardinality,
        created_at,
        level,
    })
}

// ─── Catalog ─────────────────────────────────────────────────────────────────

/// Returns the platform-aware default LMDB map size.
///
/// macOS (APFS) does not support sparse files, so the mmap pre-allocates the
/// full map_size on disk. We use a smaller default (64 MiB) to avoid wasting
/// disk on dev machines. Linux supports sparse files, so 256 MiB is fine.
fn default_map_size() -> usize {
    if cfg!(target_os = "macos") {
        64 * 1024 * 1024 // 64 MiB
    } else {
        256 * 1024 * 1024 // 256 MiB
    }
}

/// Persistent metadata catalog backed by LMDB via the [`heed`] crate.
///
/// One `Catalog` instance is intended to live for the lifetime of an open
/// store.  All operations open a fresh transaction, perform their work, and
/// commit — there is no long-lived transaction state.
///
/// Each named LMDB database handle is opened once at construction time and
/// stored on the struct; transactions simply borrow them.
///
/// # Thread safety
///
/// `heed::Env` is `Send + Sync`; the `Catalog` inherits those bounds and
/// can be shared across threads (e.g. via `Arc<Catalog>`).
pub struct Catalog {
    env: Env,
    pub(crate) events: Database<Str, Bytes>,
    pub(crate) parts: Database<U64<heed::byteorder::NativeEndian>, Bytes>,
    pub(crate) period_parts: Database<Str, Bytes>,
    pub(crate) period_deltas: Database<Str, Bytes>,
    pub(crate) period_state: Database<Str, Bytes>,
    pub(crate) compaction_log: Database<U64<heed::byteorder::NativeEndian>, Bytes>,
    pub(crate) next_part_id: Database<Str, U64<heed::byteorder::NativeEndian>>,
    // Dictionary databases (shared environment).
    pub(crate) dict_fwd: Database<Str, U64<heed::byteorder::NativeEndian>>,
    pub(crate) dict_rev: Database<U64<heed::byteorder::NativeEndian>, Str>,
    pub(crate) dict_next_id: Database<Str, U64<heed::byteorder::NativeEndian>>,
}

impl Catalog {
    /// Opens (or creates) the catalog LMDB environment at `path`.
    ///
    /// All named databases are created on first open.  The `map_size` parameter
    /// controls the maximum size of the memory-mapped region (default 256 MiB).
    ///
    /// # Safety
    ///
    /// `heed::Env::open` is `unsafe` because the underlying LMDB memory-maps
    /// the data file.  The caller must ensure that no external process modifies
    /// the data file while this environment is open — the same constraint that
    /// applies to our bitmap mmap usage in `part_store`.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment cannot be opened or any named
    /// database cannot be created.
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        Self::open_with_map_size(path, default_map_size())
    }

    /// Opens the catalog with an explicit LMDB map size.
    ///
    /// See [`open`](Self::open) for details.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment cannot be opened or any named
    /// database cannot be created.
    pub fn open_with_map_size(path: impl AsRef<Path>, map_size: usize) -> crate::Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(|e| InoxSetError::BitmapIo {
            path: path.to_path_buf(),
            source: e,
        })?;

        // Safety: we guarantee the LMDB data file is not modified externally
        // while this environment is open (same constraint as part_store mmap).
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(map_size)
                .max_dbs(12)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let events = env.create_database(&mut wtxn, Some("events"))?;
        let parts = env.create_database(&mut wtxn, Some("parts"))?;
        let period_parts = env.create_database(&mut wtxn, Some("period_parts"))?;
        let period_deltas = env.create_database(&mut wtxn, Some("period_deltas"))?;
        let period_state = env.create_database(&mut wtxn, Some("period_state"))?;
        let compaction_log = env.create_database(&mut wtxn, Some("compaction_log"))?;
        let next_part_id = env.create_database(&mut wtxn, Some("next_part_id"))?;
        let dict_fwd = env.create_database(&mut wtxn, Some("dict_fwd_v2"))?;
        let dict_rev = env.create_database(&mut wtxn, Some("dict_rev_v2"))?;
        let dict_next_id = env.create_database(&mut wtxn, Some("next_dict_id_v2"))?;
        wtxn.commit()?;

        Ok(Self {
            env,
            events,
            parts,
            period_parts,
            period_deltas,
            period_state,
            compaction_log,
            next_part_id,
            dict_fwd,
            dict_rev,
            dict_next_id,
        })
    }

    // ─── Events ───────────────────────────────────────────────────────────────

    /// Registers a new event configuration in the catalog.
    ///
    /// Overwrites any existing entry with the same name.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn register_event(&self, config: &EventConfig) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        let bytes = serialize_event_config(config);
        self.events.put(&mut txn, config.name.as_str(), &bytes)?;
        txn.commit()?;
        Ok(())
    }

    /// Returns the [`EventConfig`] for `name`, or `None` if not registered.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn get_event(&self, name: &str) -> crate::Result<Option<EventConfig>> {
        let txn = self.env.read_txn()?;
        match self.events.get(&txn, name)? {
            None => Ok(None),
            Some(data) => {
                let ec = deserialize_event_config(name, data)?;
                Ok(Some(ec))
            }
        }
    }

    /// Returns all registered [`EventConfig`]s.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn list_events(&self) -> crate::Result<Vec<EventConfig>> {
        let txn = self.env.read_txn()?;
        let mut out = Vec::new();
        for result in self.events.iter(&txn)? {
            let (key, value) = result?;
            let ec = deserialize_event_config(key, value)?;
            out.push(ec);
        }
        Ok(out)
    }

    /// Deletes an event and all associated period and part metadata in a single
    /// atomic transaction.
    ///
    /// Removes entries from `events`, `period_parts`, `period_deltas`,
    /// `period_state`, and `parts`.  Returns the list of `part_id`s that were
    /// referenced, so the caller can delete the corresponding on-disk files.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn delete_event(&self, name: &str) -> crate::Result<Vec<u64>> {
        let parts = self.delete_event_returning_parts(name)?;
        Ok(parts.into_iter().map(|p| p.part_id).collect())
    }

    /// Deletes an event and all associated period and part metadata in a single
    /// atomic transaction, returning the full [`Part`] structs that were
    /// removed.
    ///
    /// Parts are read from the catalog before removal so that the returned
    /// structs are complete.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn delete_event_returning_parts(&self, name: &str) -> crate::Result<Vec<Part>> {
        let mut txn = self.env.write_txn()?;
        let mut parts: Vec<Part> = Vec::new();

        self.events.delete(&mut txn, name)?;

        let prefix = format!("{}/", name);

        // Collect part IDs from period_parts, propagating I/O errors.
        let pp_keys: Vec<String> = {
            let mut keys = Vec::new();
            for result in self.period_parts.iter(&txn)? {
                let (k, _) = result?;
                if k.starts_with(&prefix) {
                    keys.push(k.to_string());
                }
            }
            keys
        };
        let mut part_ids: Vec<u64> = Vec::new();
        for key in &pp_keys {
            if let Some(data) = self.period_parts.get(&txn, key.as_str())? {
                let ids = deserialize_u64_vec(key, data)?;
                part_ids.extend(ids);
            }
            self.period_parts.delete(&mut txn, key.as_str())?;
        }

        // Delete period_deltas entries, propagating I/O errors.
        let pd_keys: Vec<String> = {
            let mut keys = Vec::new();
            for result in self.period_deltas.iter(&txn)? {
                let (k, _) = result?;
                if k.starts_with(&prefix) {
                    keys.push(k.to_string());
                }
            }
            keys
        };
        for key in &pd_keys {
            self.period_deltas.delete(&mut txn, key.as_str())?;
        }

        // Delete period_state entries, propagating I/O errors.
        let ps_keys: Vec<String> = {
            let mut keys = Vec::new();
            for result in self.period_state.iter(&txn)? {
                let (k, _) = result?;
                if k.starts_with(&prefix) {
                    keys.push(k.to_string());
                }
            }
            keys
        };
        for key in &ps_keys {
            self.period_state.delete(&mut txn, key.as_str())?;
        }

        // Read then remove each Part entry so we can return the structs.
        for pid in part_ids {
            let ctx = format!("part_id={pid}");
            if let Some(data) = self.parts.get(&txn, &pid)? {
                let part = deserialize_part(&ctx, data)?;
                parts.push(part);
            }
            self.parts.delete(&mut txn, &pid)?;
        }

        txn.commit()?;
        Ok(parts)
    }

    /// Deletes a single period's data and delta parts from the catalog in a
    /// single atomic transaction, returning the [`Part`] structs that were
    /// removed.
    ///
    /// Removes entries from `period_parts`, `period_deltas`, `period_state`,
    /// and `parts` for the given catalog key. The event registration itself
    /// is **not** removed.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn delete_period_returning_parts(&self, cat_key: &str) -> crate::Result<Vec<Part>> {
        let mut txn = self.env.write_txn()?;
        let mut parts: Vec<Part> = Vec::new();

        // Collect data part IDs.
        let mut part_ids: Vec<u64> = Vec::new();
        if let Some(data) = self.period_parts.get(&txn, cat_key)? {
            let ids = deserialize_u64_vec(cat_key, data)?;
            part_ids.extend(ids);
        }
        self.period_parts.delete(&mut txn, cat_key)?;

        // Collect delta part IDs.
        if let Some(data) = self.period_deltas.get(&txn, cat_key)? {
            let ids = deserialize_u64_vec(cat_key, data)?;
            part_ids.extend(ids);
        }
        self.period_deltas.delete(&mut txn, cat_key)?;

        // Remove period state.
        self.period_state.delete(&mut txn, cat_key)?;

        // Read then remove each Part entry.
        for pid in part_ids {
            let ctx = format!("part_id={pid}");
            if let Some(data) = self.parts.get(&txn, &pid)? {
                let part = deserialize_part(&ctx, data)?;
                parts.push(part);
            }
            self.parts.delete(&mut txn, &pid)?;
        }

        txn.commit()?;
        Ok(parts)
    }

    // ─── Part IDs ─────────────────────────────────────────────────────────────

    /// Allocates the next monotonically-increasing part ID and persists the
    /// updated counter.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn next_part_id(&self) -> crate::Result<u64> {
        let mut txn = self.env.write_txn()?;
        let ids = Self::txn_alloc_part_ids(&self.next_part_id, &mut txn, 1)?;
        let id = ids
            .into_iter()
            .next()
            .ok_or_else(|| InoxSetError::CatalogCorrupted {
                context: "txn_alloc_part_ids(1) returned empty vec".to_string(),
            })?;
        txn.commit()?;
        Ok(id)
    }

    // ─── Parts ────────────────────────────────────────────────────────────────

    /// Persists a [`Part`] record in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn register_part(&self, part: &Part) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        Self::txn_register_part(&self.parts, &mut txn, part)?;
        txn.commit()?;
        Ok(())
    }

    /// Returns the [`Part`] with the given `part_id`, or `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn get_part(&self, part_id: u64) -> crate::Result<Option<Part>> {
        let txn = self.env.read_txn()?;
        Self::txn_get_part(&self.parts, &txn, part_id)
    }

    /// Returns all `part_id`s currently tracked in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn all_part_ids(&self) -> crate::Result<Vec<u64>> {
        let txn = self.env.read_txn()?;
        let mut ids = Vec::new();
        for result in self.parts.iter(&txn)? {
            let (key, _) = result?;
            ids.push(key);
        }
        Ok(ids)
    }

    // ─── Period Parts ─────────────────────────────────────────────────────────

    /// Returns the list of `part_id`s associated with `key`.
    ///
    /// Returns an empty `Vec` when the key is not present.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn get_period_parts(&self, key: &str) -> crate::Result<Vec<u64>> {
        let txn = self.env.read_txn()?;
        Self::txn_get_period_parts(&self.period_parts, &txn, key)
    }

    /// Appends `ids` to the period-parts list for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn append_period_parts(&self, key: &str, ids: &[u64]) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        Self::txn_append_period_parts(&self.period_parts, &mut txn, key, ids)?;
        txn.commit()?;
        Ok(())
    }

    /// Replaces the period-parts list for `key` with `ids`.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn set_period_parts(&self, key: &str, ids: &[u64]) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        Self::txn_set_period_parts(&self.period_parts, &mut txn, key, ids)?;
        txn.commit()?;
        Ok(())
    }

    // ─── Period Deltas ────────────────────────────────────────────────────────

    /// Returns the list of delta `part_id`s for `key`.
    ///
    /// Returns an empty `Vec` when the key is not present.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn get_period_deltas(&self, key: &str) -> crate::Result<Vec<u64>> {
        let txn = self.env.read_txn()?;
        Self::txn_get_period_deltas(&self.period_deltas, &txn, key)
    }

    /// Appends `ids` to the period-deltas list for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn append_period_deltas(&self, key: &str, ids: &[u64]) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        Self::txn_append_period_deltas(&self.period_deltas, &mut txn, key, ids)?;
        txn.commit()?;
        Ok(())
    }

    /// Removes the period-deltas entry for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn clear_period_deltas(&self, key: &str) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        Self::txn_clear_period_deltas(&self.period_deltas, &mut txn, key)?;
        txn.commit()?;
        Ok(())
    }

    // ─── Period State ─────────────────────────────────────────────────────────

    /// Returns the [`PeriodState`] for `key`, or `None` if not yet recorded.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn get_period_state(&self, key: &str) -> crate::Result<Option<PeriodState>> {
        let txn = self.env.read_txn()?;
        Self::txn_get_period_state(&self.period_state, &txn, key)
    }

    /// Sets the [`PeriodState`] for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn set_period_state(&self, key: &str, state: PeriodState) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        Self::txn_set_period_state(&self.period_state, &mut txn, key, state)?;
        txn.commit()?;
        Ok(())
    }

    // ─── Environment access ──────────────────────────────────────────────────

    /// Returns a reference to the underlying [`Env`].
    ///
    /// This is provided so that callers (e.g. the flush or compaction layer)
    /// can open their own transactions and call the `txn_*` static helpers
    /// without going through individual `Catalog` methods.
    pub fn env(&self) -> &Env {
        &self.env
    }

    // ─── Transaction helpers ──────────────────────────────────────────────────

    /// Allocates `count` consecutive part IDs within an open write transaction.
    ///
    /// The counter is initialised to `1` on first use.  Returned IDs are
    /// contiguous and monotonically increasing.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn txn_alloc_part_ids(
        db: &Database<Str, U64<heed::byteorder::NativeEndian>>,
        txn: &mut heed::RwTxn,
        count: u64,
    ) -> crate::Result<Vec<u64>> {
        let start = db.get(txn, SINGLETON_KEY)?.unwrap_or(1u64);
        let end = start
            .checked_add(count)
            .ok_or_else(|| InoxSetError::CatalogCorrupted {
                context: format!("part ID counter overflow: start={start}, count={count}"),
            })?;
        db.put(txn, SINGLETON_KEY, &end)?;
        Ok((start..end).collect())
    }

    /// Inserts a [`Part`] record using the given database and write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn txn_register_part(
        db: &Database<U64<heed::byteorder::NativeEndian>, Bytes>,
        txn: &mut heed::RwTxn,
        part: &Part,
    ) -> crate::Result<()> {
        let bytes = serialize_part(part)?;
        db.put(txn, &part.part_id, &bytes)?;
        Ok(())
    }

    /// Appends `ids` to the period-parts list using the given database and
    /// write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn txn_append_period_parts(
        db: &Database<Str, Bytes>,
        txn: &mut heed::RwTxn,
        key: &str,
        ids: &[u64],
    ) -> crate::Result<()> {
        let mut existing = match db.get(txn, key)? {
            Some(data) => deserialize_u64_vec(key, data)?,
            None => Vec::new(),
        };
        existing.extend_from_slice(ids);
        let bytes = serialize_u64_vec(&existing);
        db.put(txn, key, &bytes)?;
        Ok(())
    }

    /// Replaces the period-parts list using the given database and write
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn txn_set_period_parts(
        db: &Database<Str, Bytes>,
        txn: &mut heed::RwTxn,
        key: &str,
        ids: &[u64],
    ) -> crate::Result<()> {
        let bytes = serialize_u64_vec(ids);
        db.put(txn, key, &bytes)?;
        Ok(())
    }

    /// Appends `ids` to the period-deltas list using the given database and
    /// write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn txn_append_period_deltas(
        db: &Database<Str, Bytes>,
        txn: &mut heed::RwTxn,
        key: &str,
        ids: &[u64],
    ) -> crate::Result<()> {
        let mut existing = match db.get(txn, key)? {
            Some(data) => deserialize_u64_vec(key, data)?,
            None => Vec::new(),
        };
        existing.extend_from_slice(ids);
        let bytes = serialize_u64_vec(&existing);
        db.put(txn, key, &bytes)?;
        Ok(())
    }

    /// Removes the period-deltas entry for `key` using the given database and
    /// write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn txn_clear_period_deltas(
        db: &Database<Str, Bytes>,
        txn: &mut heed::RwTxn,
        key: &str,
    ) -> crate::Result<()> {
        db.delete(txn, key)?;
        Ok(())
    }

    /// Reads the [`PeriodState`] for `key` from a period-state database using
    /// any readable transaction.
    ///
    /// Returns `None` when `key` is absent.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or an unknown state byte.
    pub fn txn_get_period_state(
        db: &Database<Str, Bytes>,
        txn: &heed::RoTxn,
        key: &str,
    ) -> crate::Result<Option<PeriodState>> {
        match db.get(txn, key)? {
            None => Ok(None),
            Some(data) => {
                if data.is_empty() {
                    return Err(InoxSetError::CatalogCorrupted {
                        context: format!("period_state '{}': empty value", key),
                    });
                }
                let byte = data[0];
                PeriodState::from_u8(byte)
                    .map(Some)
                    .ok_or_else(|| InoxSetError::CatalogCorrupted {
                        context: format!("period_state '{}': unknown state byte {}", key, byte),
                    })
            }
        }
    }

    /// Sets the [`PeriodState`] for `key` using the given database and write
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn txn_set_period_state(
        db: &Database<Str, Bytes>,
        txn: &mut heed::RwTxn,
        key: &str,
        state: PeriodState,
    ) -> crate::Result<()> {
        db.put(txn, key, &[state.as_u8()])?;
        Ok(())
    }

    /// Reads the `part_id` list for `key` from a period-parts database using
    /// any readable transaction.
    ///
    /// Returns an empty `Vec` when `key` is absent.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn txn_get_period_parts(
        db: &Database<Str, Bytes>,
        txn: &heed::RoTxn,
        key: &str,
    ) -> crate::Result<Vec<u64>> {
        match db.get(txn, key)? {
            None => Ok(Vec::new()),
            Some(data) => deserialize_u64_vec(key, data),
        }
    }

    /// Reads the delta `part_id` list for `key` from a period-deltas database
    /// using any readable transaction.
    ///
    /// Returns an empty `Vec` when `key` is absent.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn txn_get_period_deltas(
        db: &Database<Str, Bytes>,
        txn: &heed::RoTxn,
        key: &str,
    ) -> crate::Result<Vec<u64>> {
        match db.get(txn, key)? {
            None => Ok(Vec::new()),
            Some(data) => deserialize_u64_vec(key, data),
        }
    }

    /// Reads a [`Part`] by `pid` from a parts database using any readable
    /// transaction.
    ///
    /// Returns `None` when `pid` is not present.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure or data corruption.
    pub fn txn_get_part(
        db: &Database<U64<heed::byteorder::NativeEndian>, Bytes>,
        txn: &heed::RoTxn,
        pid: u64,
    ) -> crate::Result<Option<Part>> {
        match db.get(txn, &pid)? {
            None => Ok(None),
            Some(data) => {
                let ctx = format!("part_id={pid}");
                deserialize_part(&ctx, data).map(Some)
            }
        }
    }

    // ─── Compaction log ───────────────────────────────────────────────────────

    /// Appends a compaction record to the compaction log database.
    ///
    /// The key is the Unix timestamp supplied by the caller. The value is a
    /// version-1 binary encoding:
    ///
    /// ```text
    /// [version=1 u8]
    /// [timestamp u64 LE]
    /// [periods_compacted u32 LE]
    /// [parts_merged u32 LE]
    /// [deltas_applied u32 LE]
    /// [bytes_reclaimed u64 LE]
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn write_compaction_log(
        &self,
        timestamp: u64,
        periods_compacted: u32,
        parts_merged: u32,
        deltas_applied: u32,
        bytes_reclaimed: u64,
    ) -> crate::Result<()> {
        let mut buf = Vec::with_capacity(1 + 8 + 4 + 4 + 4 + 8);
        buf.push(1u8);
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.extend_from_slice(&periods_compacted.to_le_bytes());
        buf.extend_from_slice(&parts_merged.to_le_bytes());
        buf.extend_from_slice(&deltas_applied.to_le_bytes());
        buf.extend_from_slice(&bytes_reclaimed.to_le_bytes());

        let mut txn = self.env.write_txn()?;
        self.compaction_log.put(&mut txn, &timestamp, &buf)?;
        txn.commit()?;
        Ok(())
    }

    // ─── Event scanning ───────────────────────────────────────────────────────

    /// Returns a deduplicated list of all period keys (`"event/gran/period"`)
    /// that reference the given event, drawn from both `period_parts` and
    /// `period_deltas` databases.
    ///
    /// Uses a [`HashSet`] internally to guarantee O(n) deduplication.
    ///
    /// # Errors
    ///
    /// Returns an error on LMDB I/O failure.
    pub fn period_keys_for_event(&self, event: &str) -> crate::Result<Vec<String>> {
        let prefix = format!("{}/", event);
        let txn = self.env.read_txn()?;

        let mut seen: HashSet<String> = HashSet::new();

        for result in self.period_parts.iter(&txn)? {
            let (k, _) = result?;
            if k.starts_with(&prefix) {
                seen.insert(k.to_string());
            }
        }

        for result in self.period_deltas.iter(&txn)? {
            let (k, _) = result?;
            if k.starts_with(&prefix) {
                seen.insert(k.to_string());
            }
        }

        Ok(seen.into_iter().collect())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use tempfile::TempDir;

    fn test_catalog() -> (Catalog, TempDir) {
        let dir = TempDir::new().unwrap();
        let cat = Catalog::open(dir.path().join("catalog.mdb")).unwrap();
        (cat, dir)
    }

    #[test]
    fn register_and_get_event() {
        let (cat, _dir) = test_catalog();
        let ec = EventConfig::new("active".into(), Granularity::Hour, Rollup::Auto);
        cat.register_event(&ec).unwrap();
        let got = cat.get_event("active").unwrap().unwrap();
        assert_eq!(got.name, "active");
        assert_eq!(got.finest_granularity, Granularity::Hour);
        assert_eq!(got.rollup, Rollup::Auto);
    }

    #[test]
    fn list_events_empty() {
        let (cat, _dir) = test_catalog();
        assert!(cat.list_events().unwrap().is_empty());
    }

    #[test]
    fn next_part_id_monotonic() {
        let (cat, _dir) = test_catalog();
        let id1 = cat.next_part_id().unwrap();
        let id2 = cat.next_part_id().unwrap();
        assert!(id2 > id1);
    }

    #[test]
    fn period_parts_crud() {
        let (cat, _dir) = test_catalog();
        let key = "active/hour/2026-03-11T14";
        cat.append_period_parts(key, &[1, 2]).unwrap();
        let parts = cat.get_period_parts(key).unwrap();
        assert_eq!(parts, vec![1, 2]);
        cat.append_period_parts(key, &[3]).unwrap();
        let parts = cat.get_period_parts(key).unwrap();
        assert_eq!(parts, vec![1, 2, 3]);
    }

    #[test]
    fn period_state_default_and_set() {
        let (cat, _dir) = test_catalog();
        let key = "active/hour/2026-03-11T14";
        assert_eq!(cat.get_period_state(key).unwrap(), None);
        cat.set_period_state(key, PeriodState::Open).unwrap();
        assert_eq!(cat.get_period_state(key).unwrap(), Some(PeriodState::Open));
    }

    #[test]
    fn register_part_and_get() {
        let (cat, _dir) = test_catalog();
        let part = Part {
            part_id: 1,
            kind: PartKind::Data,
            event: "active".into(),
            period: Period::Hour(2026, 3, 11, 14),
            file_path: "active/hour/2026-03-11T14.000000000001.roar".into(),
            size_bytes: 1024,
            cardinality: 500,
            created_at: 1000,
            level: 0,
        };
        cat.register_part(&part).unwrap();
        let got = cat.get_part(1).unwrap().unwrap();
        assert_eq!(got.part_id, 1);
        assert_eq!(got.event, "active");
        assert_eq!(got.size_bytes, 1024);
    }

    #[test]
    fn delete_event_cleans_up() {
        let (cat, _dir) = test_catalog();
        let ec = EventConfig::new("active".into(), Granularity::Hour, Rollup::Auto);
        cat.register_event(&ec).unwrap();
        cat.set_period_state("active/hour/2026-03-11T14", PeriodState::Open)
            .unwrap();
        cat.append_period_parts("active/hour/2026-03-11T14", &[1])
            .unwrap();
        let part = Part {
            part_id: 1,
            kind: PartKind::Data,
            event: "active".into(),
            period: Period::Hour(2026, 3, 11, 14),
            file_path: "active/hour/2026-03-11T14.000000000001.roar".into(),
            size_bytes: 512,
            cardinality: 100,
            created_at: 1000,
            level: 0,
        };
        cat.register_part(&part).unwrap();

        let deleted_part_ids = cat.delete_event("active").unwrap();
        assert_eq!(deleted_part_ids, vec![1]);
        assert!(cat.get_event("active").unwrap().is_none());
    }

    #[test]
    fn version_byte_corruption_detected() {
        let result = deserialize_event_config("test", &[99, 1, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn short_data_corruption_detected() {
        let result = deserialize_event_config("test", &[1]);
        assert!(result.is_err());
    }

    #[test]
    fn catalog_creates_dict_databases() {
        let (cat, _dir) = test_catalog();
        // Verify dict databases are accessible via a read transaction.
        let rtxn = cat.env().read_txn().unwrap();
        assert!(cat.dict_fwd.get(&rtxn, "nonexistent").unwrap().is_none());
        assert!(cat.dict_rev.get(&rtxn, &999u64).unwrap().is_none());
        assert!(cat.dict_next_id.get(&rtxn, "_").unwrap().is_none());
    }
}
