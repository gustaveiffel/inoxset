//! Catalog module: redb-backed persistent metadata store for inoxset.
//!
//! The catalog stores event configurations, part metadata, period–part
//! associations, delta-part lists, and period lifecycle state.  Every
//! serialized value is prefixed with a version byte (`1`) so that future
//! format changes can be detected at startup.
//!
//! # Tables
//!
//! | Constant | Key | Value |
//! |---|---|---|
//! | [`EVENTS`] | event name | serialized [`EventConfig`] |
//! | [`PARTS`] | `part_id` | serialized [`Part`] |
//! | [`PERIOD_PARTS`] | `"event/gran/period_key"` | packed `Vec<u64>` |
//! | [`PERIOD_DELTAS`] | `"event/gran/period_key"` | packed `Vec<u64>` |
//! | [`PERIOD_STATE`] | `"event/gran/period_key"` | `u8` |
//! | [`COMPACTION_LOG`] | timestamp | raw bytes |
//! | [`NEXT_PART_ID`] | `()` | `u64` |

use std::collections::HashSet;
use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use crate::error::InoxSetError;
use crate::types::{EventConfig, Granularity, Part, PartKind, Period, PeriodState, Rollup};

// ─── Table definitions ────────────────────────────────────────────────────────

/// Event configuration table: event name → serialized [`EventConfig`].
pub(crate) const EVENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("events");

/// Part metadata table: `part_id` → serialized [`Part`].
pub(crate) const PARTS: TableDefinition<u64, &[u8]> = TableDefinition::new("parts");

/// Period-to-part-list table: `"event/gran/period_key"` → packed `Vec<u64>`.
pub(crate) const PERIOD_PARTS: TableDefinition<&str, &[u8]> = TableDefinition::new("period_parts");

/// Period-to-delta-list table: `"event/gran/period_key"` → packed `Vec<u64>`.
pub(crate) const PERIOD_DELTAS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("period_deltas");

/// Period lifecycle state table: `"event/gran/period_key"` → `u8`.
pub(crate) const PERIOD_STATE: TableDefinition<&str, u8> = TableDefinition::new("period_state");

/// Compaction log table: Unix timestamp → raw bytes.
pub(crate) const COMPACTION_LOG: TableDefinition<u64, &[u8]> =
    TableDefinition::new("compaction_log");

/// Monotonically-increasing part-ID counter: `()` → next available `u64`.
pub(crate) const NEXT_PART_ID: TableDefinition<(), u64> = TableDefinition::new("next_part_id");

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

/// Persistent metadata catalog backed by a [`redb`] embedded database.
///
/// One `Catalog` instance is intended to live for the lifetime of an open
/// store.  All operations open a fresh transaction, perform their work, and
/// commit — there is no long-lived transaction state.
///
/// # Thread safety
///
/// `redb::Database` is `Send + Sync`; the `Catalog` inherits those bounds and
/// can be shared across threads (e.g. via `Arc<Catalog>`).
pub struct Catalog {
    db: Database,
}

impl Catalog {
    /// Opens (or creates) the catalog database at `path`.
    ///
    /// All required tables are created on first open.
    ///
    /// # Errors
    ///
    /// Returns an error if the database file cannot be opened or any table
    /// cannot be created.
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let db = Database::create(path.as_ref()).map_err(redb::Error::from)?;
        // Ensure all tables exist.
        let write_txn = db.begin_write()?;
        {
            write_txn.open_table(EVENTS)?;
            write_txn.open_table(PARTS)?;
            write_txn.open_table(PERIOD_PARTS)?;
            write_txn.open_table(PERIOD_DELTAS)?;
            write_txn.open_table(PERIOD_STATE)?;
            write_txn.open_table(COMPACTION_LOG)?;
            write_txn.open_table(NEXT_PART_ID)?;
        }
        write_txn.commit()?;
        Ok(Self { db })
    }

    // ─── Events ───────────────────────────────────────────────────────────────

    /// Registers a new event configuration in the catalog.
    ///
    /// Overwrites any existing entry with the same name.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn register_event(&self, config: &EventConfig) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(EVENTS)?;
            let bytes = serialize_event_config(config);
            table.insert(config.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Returns the [`EventConfig`] for `name`, or `None` if not registered.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn get_event(&self, name: &str) -> crate::Result<Option<EventConfig>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(EVENTS)?;
        let result = match table.get(name)? {
            None => Ok(None),
            Some(guard) => {
                let ec = deserialize_event_config(name, guard.value())?;
                Ok(Some(ec))
            }
        };
        result
    }

    /// Returns all registered [`EventConfig`]s.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn list_events(&self) -> crate::Result<Vec<EventConfig>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(EVENTS)?;
        let mut out = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let ec = deserialize_event_config(key.value(), value.value())?;
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
    /// Returns an error on redb I/O failure or data corruption.
    pub fn delete_event(&self, name: &str) -> crate::Result<Vec<u64>> {
        let txn = self.db.begin_write()?;
        let mut part_ids: Vec<u64> = Vec::new();
        {
            let mut events_table = txn.open_table(EVENTS)?;
            events_table.remove(name)?;

            let mut pp_table = txn.open_table(PERIOD_PARTS)?;
            let mut pd_table = txn.open_table(PERIOD_DELTAS)?;
            let mut ps_table = txn.open_table(PERIOD_STATE)?;
            let mut parts_table = txn.open_table(PARTS)?;

            let prefix = format!("{}/", name);

            // Collect keys to delete from period_parts, propagating I/O errors.
            let mut pp_keys: Vec<String> = Vec::new();
            for item in pp_table.iter()? {
                let (k, _) = item?;
                let key = k.value().to_string();
                if key.starts_with(&prefix) {
                    pp_keys.push(key);
                }
            }

            for key in &pp_keys {
                if let Some(guard) = pp_table.remove(key.as_str())? {
                    let ids = deserialize_u64_vec(key, guard.value())?;
                    part_ids.extend(ids);
                }
            }

            // Collect and delete keys from period_deltas, propagating I/O errors.
            let mut pd_keys: Vec<String> = Vec::new();
            for item in pd_table.iter()? {
                let (k, _) = item?;
                let key = k.value().to_string();
                if key.starts_with(&prefix) {
                    pd_keys.push(key);
                }
            }
            for key in &pd_keys {
                pd_table.remove(key.as_str())?;
            }

            // Collect and delete keys from period_state, propagating I/O errors.
            let mut ps_keys: Vec<String> = Vec::new();
            for item in ps_table.iter()? {
                let (k, _) = item?;
                let key = k.value().to_string();
                if key.starts_with(&prefix) {
                    ps_keys.push(key);
                }
            }
            for key in &ps_keys {
                ps_table.remove(key.as_str())?;
            }

            // Remove the PARTS entries for every part_id collected above.
            for &pid in &part_ids {
                parts_table.remove(pid)?;
            }
        }
        txn.commit()?;
        Ok(part_ids)
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
    /// Returns an error on redb I/O failure or data corruption.
    pub fn delete_event_returning_parts(&self, name: &str) -> crate::Result<Vec<Part>> {
        let txn = self.db.begin_write()?;
        let mut parts: Vec<Part> = Vec::new();
        {
            let mut events_table = txn.open_table(EVENTS)?;
            events_table.remove(name)?;

            let mut pp_table = txn.open_table(PERIOD_PARTS)?;
            let mut pd_table = txn.open_table(PERIOD_DELTAS)?;
            let mut ps_table = txn.open_table(PERIOD_STATE)?;
            let mut parts_table = txn.open_table(PARTS)?;

            let prefix = format!("{}/", name);

            // Collect part IDs from period_parts, propagating I/O errors.
            let mut pp_keys: Vec<String> = Vec::new();
            for item in pp_table.iter()? {
                let (k, _) = item?;
                let key = k.value().to_string();
                if key.starts_with(&prefix) {
                    pp_keys.push(key);
                }
            }
            let mut part_ids: Vec<u64> = Vec::new();
            for key in &pp_keys {
                if let Some(guard) = pp_table.remove(key.as_str())? {
                    let ids = deserialize_u64_vec(key, guard.value())?;
                    part_ids.extend(ids);
                }
            }

            // Delete period_deltas entries, propagating I/O errors.
            let mut pd_keys: Vec<String> = Vec::new();
            for item in pd_table.iter()? {
                let (k, _) = item?;
                let key = k.value().to_string();
                if key.starts_with(&prefix) {
                    pd_keys.push(key);
                }
            }
            for key in &pd_keys {
                pd_table.remove(key.as_str())?;
            }

            // Delete period_state entries, propagating I/O errors.
            let mut ps_keys: Vec<String> = Vec::new();
            for item in ps_table.iter()? {
                let (k, _) = item?;
                let key = k.value().to_string();
                if key.starts_with(&prefix) {
                    ps_keys.push(key);
                }
            }
            for key in &ps_keys {
                ps_table.remove(key.as_str())?;
            }

            // Read then remove each Part entry so we can return the structs.
            for pid in part_ids {
                let ctx = format!("part_id={pid}");
                if let Some(guard) = parts_table.remove(pid)? {
                    let part = deserialize_part(&ctx, guard.value())?;
                    parts.push(part);
                }
            }
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
    /// Returns an error on redb I/O failure.
    pub fn next_part_id(&self) -> crate::Result<u64> {
        let txn = self.db.begin_write()?;
        let id;
        {
            let mut table = txn.open_table(NEXT_PART_ID)?;
            id = Self::txn_alloc_part_ids(&mut table, 1)?
                .into_iter()
                .next()
                .ok_or_else(|| InoxSetError::CatalogCorrupted {
                    context: "txn_alloc_part_ids(1) returned empty vec".to_string(),
                })?;
        }
        txn.commit()?;
        Ok(id)
    }

    // ─── Parts ────────────────────────────────────────────────────────────────

    /// Persists a [`Part`] record in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn register_part(&self, part: &Part) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PARTS)?;
            Self::txn_register_part(&mut table, part)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Returns the [`Part`] with the given `part_id`, or `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn get_part(&self, part_id: u64) -> crate::Result<Option<Part>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PARTS)?;
        Self::txn_get_part(&table, part_id)
    }

    /// Returns all `part_id`s currently tracked in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn all_part_ids(&self) -> crate::Result<Vec<u64>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PARTS)?;
        let mut ids = Vec::new();
        for item in table.iter()? {
            let (key, _) = item?;
            ids.push(key.value());
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
    /// Returns an error on redb I/O failure or data corruption.
    pub fn get_period_parts(&self, key: &str) -> crate::Result<Vec<u64>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PERIOD_PARTS)?;
        Self::txn_get_period_parts(&table, key)
    }

    /// Appends `ids` to the period-parts list for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn append_period_parts(&self, key: &str, ids: &[u64]) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PERIOD_PARTS)?;
            Self::txn_append_period_parts(&mut table, key, ids)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Replaces the period-parts list for `key` with `ids`.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn set_period_parts(&self, key: &str, ids: &[u64]) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PERIOD_PARTS)?;
            Self::txn_set_period_parts(&mut table, key, ids)?;
        }
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
    /// Returns an error on redb I/O failure or data corruption.
    pub fn get_period_deltas(&self, key: &str) -> crate::Result<Vec<u64>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PERIOD_DELTAS)?;
        Self::txn_get_period_deltas(&table, key)
    }

    /// Appends `ids` to the period-deltas list for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn append_period_deltas(&self, key: &str, ids: &[u64]) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PERIOD_DELTAS)?;
            Self::txn_append_period_deltas(&mut table, key, ids)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Removes the period-deltas entry for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn clear_period_deltas(&self, key: &str) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PERIOD_DELTAS)?;
            Self::txn_clear_period_deltas(&mut table, key)?;
        }
        txn.commit()?;
        Ok(())
    }

    // ─── Period State ─────────────────────────────────────────────────────────

    /// Returns the [`PeriodState`] for `key`, or `None` if not yet recorded.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn get_period_state(&self, key: &str) -> crate::Result<Option<PeriodState>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PERIOD_STATE)?;
        Self::txn_get_period_state(&table, key)
    }

    /// Sets the [`PeriodState`] for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn set_period_state(&self, key: &str, state: PeriodState) -> crate::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PERIOD_STATE)?;
            Self::txn_set_period_state(&mut table, key, state)?;
        }
        txn.commit()?;
        Ok(())
    }

    // ─── Database access ──────────────────────────────────────────────────────

    /// Returns a reference to the underlying [`Database`].
    ///
    /// This is provided so that callers (e.g. the flush or compaction layer)
    /// can open their own transactions and call the `txn_*` static helpers
    /// without going through individual `Catalog` methods.
    pub fn db(&self) -> &Database {
        &self.db
    }

    // ─── Transaction helpers ──────────────────────────────────────────────────

    /// Allocates `count` consecutive part IDs within an open write transaction.
    ///
    /// The counter is initialised to `1` on first use.  Returned IDs are
    /// contiguous and monotonically increasing.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn txn_alloc_part_ids(
        table: &mut redb::Table<(), u64>,
        count: u64,
    ) -> crate::Result<Vec<u64>> {
        let start = match table.get(())? {
            Some(guard) => guard.value(),
            None => 1u64,
        };
        let end = start
            .checked_add(count)
            .ok_or_else(|| InoxSetError::CatalogCorrupted {
                context: format!("part ID counter overflow: start={start}, count={count}"),
            })?;
        table.insert((), end)?;
        Ok((start..end).collect())
    }

    /// Inserts a [`Part`] record into an open `parts` write table.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn txn_register_part(
        table: &mut redb::Table<u64, &[u8]>,
        part: &Part,
    ) -> crate::Result<()> {
        let bytes = serialize_part(part)?;
        table.insert(part.part_id, bytes.as_slice())?;
        Ok(())
    }

    /// Appends `ids` to the period-parts list in an open write table.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn txn_append_period_parts(
        table: &mut redb::Table<&str, &[u8]>,
        key: &str,
        ids: &[u64],
    ) -> crate::Result<()> {
        let mut existing = match table.get(key)? {
            Some(guard) => deserialize_u64_vec(key, guard.value())?,
            None => Vec::new(),
        };
        existing.extend_from_slice(ids);
        let bytes = serialize_u64_vec(&existing);
        table.insert(key, bytes.as_slice())?;
        Ok(())
    }

    /// Replaces the period-parts list in an open write table.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn txn_set_period_parts(
        table: &mut redb::Table<&str, &[u8]>,
        key: &str,
        ids: &[u64],
    ) -> crate::Result<()> {
        let bytes = serialize_u64_vec(ids);
        table.insert(key, bytes.as_slice())?;
        Ok(())
    }

    /// Appends `ids` to the period-deltas list in an open write table.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn txn_append_period_deltas(
        table: &mut redb::Table<&str, &[u8]>,
        key: &str,
        ids: &[u64],
    ) -> crate::Result<()> {
        let mut existing = match table.get(key)? {
            Some(guard) => deserialize_u64_vec(key, guard.value())?,
            None => Vec::new(),
        };
        existing.extend_from_slice(ids);
        let bytes = serialize_u64_vec(&existing);
        table.insert(key, bytes.as_slice())?;
        Ok(())
    }

    /// Removes the period-deltas entry for `key` in an open write table.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn txn_clear_period_deltas(
        table: &mut redb::Table<&str, &[u8]>,
        key: &str,
    ) -> crate::Result<()> {
        table.remove(key)?;
        Ok(())
    }

    /// Reads the [`PeriodState`] for `key` from any readable period-state
    /// table (both write and read-only tables are accepted).
    ///
    /// Returns `None` when `key` is absent.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or an unknown state byte.
    pub fn txn_get_period_state<T: ReadableTable<&'static str, u8>>(
        table: &T,
        key: &str,
    ) -> crate::Result<Option<PeriodState>> {
        match table.get(key)? {
            None => Ok(None),
            Some(guard) => {
                let byte = guard.value();
                PeriodState::from_u8(byte)
                    .map(Some)
                    .ok_or_else(|| InoxSetError::CatalogCorrupted {
                        context: format!("period_state '{}': unknown state byte {}", key, byte),
                    })
            }
        }
    }

    /// Sets the [`PeriodState`] for `key` in an open write table.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn txn_set_period_state(
        table: &mut redb::Table<&str, u8>,
        key: &str,
        state: PeriodState,
    ) -> crate::Result<()> {
        table.insert(key, state.as_u8())?;
        Ok(())
    }

    /// Reads the `part_id` list for `key` from an open **read-only** table.
    ///
    /// Returns an empty `Vec` when `key` is absent.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn txn_get_period_parts(
        table: &redb::ReadOnlyTable<&str, &[u8]>,
        key: &str,
    ) -> crate::Result<Vec<u64>> {
        match table.get(key)? {
            None => Ok(Vec::new()),
            Some(guard) => deserialize_u64_vec(key, guard.value()),
        }
    }

    /// Reads the delta `part_id` list for `key` from an open **read-only**
    /// table.
    ///
    /// Returns an empty `Vec` when `key` is absent.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn txn_get_period_deltas(
        table: &redb::ReadOnlyTable<&str, &[u8]>,
        key: &str,
    ) -> crate::Result<Vec<u64>> {
        match table.get(key)? {
            None => Ok(Vec::new()),
            Some(guard) => deserialize_u64_vec(key, guard.value()),
        }
    }

    /// Reads a [`Part`] by `pid` from an open **read-only** parts table.
    ///
    /// Returns `None` when `pid` is not present.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure or data corruption.
    pub fn txn_get_part(
        table: &redb::ReadOnlyTable<u64, &[u8]>,
        pid: u64,
    ) -> crate::Result<Option<Part>> {
        match table.get(pid)? {
            None => Ok(None),
            Some(guard) => {
                let ctx = format!("part_id={pid}");
                deserialize_part(&ctx, guard.value()).map(Some)
            }
        }
    }

    // ─── Compaction log ───────────────────────────────────────────────────────

    /// Appends a compaction record to the [`COMPACTION_LOG`] table.
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
    /// Returns an error on redb I/O failure.
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

        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(COMPACTION_LOG)?;
            table.insert(timestamp, buf.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    // ─── Event scanning ───────────────────────────────────────────────────────

    /// Returns a deduplicated list of all period keys (`"event/gran/period"`)
    /// that reference the given event, drawn from both `period_parts` and
    /// `period_deltas` tables.
    ///
    /// Uses a [`HashSet`] internally to guarantee O(n) deduplication.
    ///
    /// # Errors
    ///
    /// Returns an error on redb I/O failure.
    pub fn period_keys_for_event(&self, event: &str) -> crate::Result<Vec<String>> {
        let prefix = format!("{}/", event);
        let txn = self.db.begin_read()?;

        let mut seen: HashSet<String> = HashSet::new();

        {
            let pp_table = txn.open_table(PERIOD_PARTS)?;
            for item in pp_table.iter()? {
                let (key, _) = item?;
                let k = key.value();
                if k.starts_with(&prefix) {
                    seen.insert(k.to_string());
                }
            }
        }

        {
            let pd_table = txn.open_table(PERIOD_DELTAS)?;
            for item in pd_table.iter()? {
                let (key, _) = item?;
                let k = key.value();
                if k.starts_with(&prefix) {
                    seen.insert(k.to_string());
                }
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
        let cat = Catalog::open(dir.path().join("catalog.redb")).unwrap();
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

        let deleted_part_ids = cat.delete_event("active").unwrap();
        assert_eq!(deleted_part_ids, vec![1]);
        assert!(cat.get_event("active").unwrap().is_none());
    }

    #[test]
    fn version_byte_corruption_detected() {
        // Verify that deserialize_event_config rejects data with wrong version
        let result = deserialize_event_config("test", &[99, 1, 0]); // version 99
        assert!(result.is_err());
    }

    #[test]
    fn short_data_corruption_detected() {
        // Verify that deserialize_event_config rejects too-short data
        let result = deserialize_event_config("test", &[1]); // only 1 byte
        assert!(result.is_err());
    }
}
