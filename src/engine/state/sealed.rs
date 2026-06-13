use super::super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::engine::storage_engine) struct SealedChunkKey {
    pub(in crate::engine::storage_engine) min_ts: i64,
    pub(in crate::engine::storage_engine) max_ts: i64,
    pub(in crate::engine::storage_engine) point_count: u16,
    pub(in crate::engine::storage_engine) sequence: u64,
}

impl SealedChunkKey {
    pub(in crate::engine::storage_engine) fn from_chunk(chunk: &Chunk, sequence: u64) -> Self {
        Self {
            min_ts: chunk.header.min_ts,
            max_ts: chunk.header.max_ts,
            point_count: chunk.header.point_count,
            sequence,
        }
    }

    pub(in crate::engine::storage_engine) fn upper_bound_for_min_ts(min_ts_exclusive: i64) -> Self {
        Self {
            min_ts: min_ts_exclusive,
            max_ts: i64::MIN,
            point_count: 0,
            sequence: 0,
        }
    }
}
