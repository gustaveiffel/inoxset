// inoxset — Roaring Bitmap storage engine with time-aware set algebra
//
// Library-first sync API. No async, no server.
// Embed via spawn_blocking in async runtimes.

pub mod types;
pub mod error;
pub mod period;
pub mod catalog;
pub mod mempart;
pub mod part_store;
pub mod rollup;
pub mod merge;
pub mod metrics;
pub mod builder;
