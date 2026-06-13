//! Segment/chunk storage engine module tree.

pub mod chunk;
pub mod compactor;
pub mod encoder;
pub mod index;
pub mod query;
pub mod segment;
pub mod series_registry;
#[path = "engine.rs"]
pub mod storage_engine;
pub mod wal;

pub use storage_engine as engine;

pub const STORAGE_FORMAT_VERSION: u16 = 1;
pub const DEFAULT_CHUNK_POINTS: usize = 2048;

pub(crate) use storage_engine::{build_storage, restore_storage_from_snapshot};
