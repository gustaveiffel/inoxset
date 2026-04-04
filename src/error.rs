//! Error types for the inoxset storage engine.
//!
//! All fallible public API functions return [`crate::Result<T>`], which is an
//! alias for `std::result::Result<T, InoxSetError>`.  Individual error
//! variants carry just enough context for callers to diagnose and, where
//! appropriate, recover from failures.

use std::path::PathBuf;

use crate::types::{Granularity, Period};

/// The top-level error type for all inoxset operations.
///
/// Variants cover catalog I/O, bitmap file I/O, event lifecycle, and
/// configuration mismatches.  Catalog-layer errors from `redb` are wrapped
/// via `#[from]` so the `?` operator works transparently in catalog code.
#[derive(Debug, thiserror::Error)]
pub enum InoxSetError {
    /// Wraps a generic [`redb::Error`] returned by the catalog layer.
    #[error("catalog error: {0}")]
    Catalog(#[from] redb::Error),

    /// Wraps a [`redb::TransactionError`] from catalog transaction management.
    #[error("catalog transaction error: {0}")]
    CatalogTransaction(#[from] redb::TransactionError),

    /// Wraps a [`redb::TableError`] from catalog table access.
    #[error("catalog table error: {0}")]
    CatalogTable(#[from] redb::TableError),

    /// Wraps a [`redb::StorageError`] from low-level catalog storage.
    #[error("catalog storage error: {0}")]
    CatalogStorage(#[from] redb::StorageError),

    /// Wraps a [`redb::CommitError`] from catalog transaction commit.
    #[error("catalog commit error: {0}")]
    CatalogCommit(#[from] redb::CommitError),

    /// Bitmap file could not be read or written.
    ///
    /// `path` is the absolute path of the affected file; `source` is the
    /// underlying [`std::io::Error`].
    #[error("bitmap file I/O: {path}: {source}")]
    BitmapIo {
        /// Path of the bitmap file that triggered the error.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// The [`Period`] supplied for a write or query does not match the
    /// [`Granularity`] configured for the event.
    #[error("period {period:?} does not match granularity {expected:?} for event {event}")]
    GranularityMismatch {
        /// Name of the event whose granularity was violated.
        event: String,
        /// The period that was supplied.
        period: Period,
        /// The granularity that was expected.
        expected: Granularity,
    },

    /// A bitmap file exists but its contents could not be deserialized.
    #[error("bitmap deserialization failed for {event} at {period:?}")]
    BitmapCorrupted {
        /// Name of the event whose bitmap is corrupted.
        event: String,
        /// The period whose bitmap file is corrupted.
        period: Period,
    },

    /// The event name contains characters that are not in `[a-zA-Z0-9_:.-]`
    /// or the name is empty.
    #[error("invalid event name: {0} (allowed: [a-zA-Z0-9_:.\\-])")]
    InvalidEventName(String),

    /// An attempt was made to register an event name that is already present
    /// in the catalog.
    #[error("event already registered: {0}")]
    EventAlreadyRegistered(String),

    /// The store was opened in read-only mode but a mutating operation was
    /// attempted.
    #[error("store is read-only")]
    ReadOnly,

    /// The store has been closed and cannot accept further operations.
    #[error("store is closed")]
    Closed,

    /// A builder or API call was made with an invalid or missing configuration
    /// value.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// The catalog database contains data that cannot be interpreted, likely
    /// indicating version skew or on-disk corruption.
    #[error("catalog data corrupted: {context}")]
    CatalogCorrupted {
        /// Human-readable description of what was found and why it is invalid.
        context: String,
    },
}

/// Validate that `name` is a legal event name.
///
/// A valid event name is non-empty and consists solely of characters from
/// `[a-zA-Z0-9_:.-]`.
///
/// # Errors
///
/// Returns [`InoxSetError::InvalidEventName`] when `name` is empty or
/// contains a disallowed character.
pub fn validate_event_name(name: &str) -> crate::Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':' || b == b'.' || b == b'-')
    {
        return Err(InoxSetError::InvalidEventName(name.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = InoxSetError::GranularityMismatch {
            event: "active".into(),
            period: Period::Static,
            expected: Granularity::Hour,
        };
        assert!(e.to_string().contains("active"));
    }

    #[test]
    fn validate_event_name_ok() {
        assert!(validate_event_name("active").is_ok());
        assert!(validate_event_name("dmp:segment_42").is_ok());
        assert!(validate_event_name("geo.h3_abc").is_ok());
        assert!(validate_event_name("ab-test").is_ok());
    }

    #[test]
    fn validate_event_name_reject() {
        assert!(validate_event_name("").is_err());
        assert!(validate_event_name("foo bar").is_err());
        assert!(validate_event_name("foo/bar").is_err());
        assert!(validate_event_name("foo\0bar").is_err());
        // Path traversal: "." and ".." must be rejected
        assert!(validate_event_name(".").is_err());
        assert!(validate_event_name("..").is_err());
    }

    #[test]
    fn catalog_corrupted_display() {
        let e = InoxSetError::CatalogCorrupted {
            context: "bad version byte".into(),
        };
        assert!(e.to_string().contains("bad version byte"));
    }
}
