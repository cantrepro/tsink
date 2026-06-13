//! Storage-engine entrypoints, owner map, and refactor guardrails.
//!
//! The engine is split into three layers so contributors can change one concern
//! without re-reading the full ingest, persistence, and query stack on every
//! edit:
//! - Foundation libraries: `chunk`, `compactor`, `encoder`, `query`,
//!   `segment`, `series`, `wal`, and the smaller format/utilities they build
//!   on.
//! - Shared shell/state: `storage_engine` / `engine.rs`, `construction`,
//!   `core_impl`, `state`, `visibility`, `runtime`, `metadata_lookup`,
//!   `shard_routing`, `metrics`, `observability`, and `process_lock`.
//! - Owner modules: `bootstrap`, `ingest` + `ingest_pipeline`, `write_buffer`,
//!   `lifecycle`, `maintenance` + `tiering` + `registry_catalog`,
//!   `query_exec` + `query_read`, `deletion`, and `rollups`.
//!
//! Placement rules:
//! - Keep new product logic in the owning subsystem; `engine.rs` and
//!   `core_impl.rs` are the integration shell, not the default overflow area.
//! - When an owner already delegates to child modules such as `bootstrap/*`,
//!   `ingest_pipeline/*`, `lifecycle/*`, `maintenance/*`, `query_exec/*`,
//!   `query_read/*`, `tiering/*`, or `visibility/*`, extend the owning child
//!   instead of growing the parent orchestration file.

pub(crate) mod binio;
/// Immutable in-memory chunk layout and chunk builder primitives.
pub mod chunk;
/// Immutable segment compaction and rewrite helpers.
pub mod compactor;
pub(crate) mod durability;
/// Chunk encoding and compression codecs.
pub mod encoder;
/// Filesystem utilities used by durable engine paths.
pub mod fs_utils;
/// Persisted index and postings lookup helpers.
pub mod index;
/// Query planning helpers shared across storage and server callers.
pub mod query;
/// Immutable on-disk segment format and segment loading.
pub mod segment;
/// Series registry, postings, and label/metric dictionaries.
pub mod series;
#[path = "engine.rs"]
/// `ChunkStorage` and the shared engine state graph.
pub mod storage_engine;
pub(crate) mod tombstone;
/// Write-ahead log framing, append, and replay.
pub mod wal;

pub use storage_engine as engine;

pub const STORAGE_FORMAT_VERSION: u16 = 2;
pub const DEFAULT_CHUNK_POINTS: usize = crate::storage::DEFAULT_CHUNK_POINTS;

pub(crate) use storage_engine::{build_storage, restore_storage_from_snapshot};
