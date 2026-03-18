// inoxset — Roaring Bitmap storage engine with time-aware set algebra
//
// Library-first sync API. No async, no server.
// Embed via spawn_blocking in async runtimes.

pub mod builder;
pub mod catalog;
pub mod error;
pub mod mempart;
pub mod merge;
pub mod metrics;
pub mod part_store;
pub mod period;
pub mod rollup;
pub mod types;
