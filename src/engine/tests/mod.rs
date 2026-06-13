use std::collections::HashMap;
use std::time::Duration;

use tempfile::TempDir;

use super::bootstrap::merge_loaded_segment_indexes;
use super::{
    ChunkStorage, ChunkStorageOptions, BLOB_LANE_ROOT, DEFAULT_ADMISSION_POLL_INTERVAL,
    DEFAULT_COMPACTION_INTERVAL, NUMERIC_LANE_ROOT, WAL_DIR_NAME,
};
use crate::engine::chunk::{
    Chunk, ChunkHeader, ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane,
};
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    load_segment_indexes, load_segments_for_level, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::SeriesRegistry;
use crate::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
use crate::wal::{WalReplayMode, WalSyncMode};
use crate::{
    DataPoint, Label, Row, SeriesMatcher, SeriesSelection, Storage, StorageBuilder,
    TimestampPrecision, TsinkError, Value,
};

mod admission_control;
mod capacity;
mod failure_modes;
mod ingest_concurrency;
mod ingest_core;
mod ingest_failures;
mod persistence_background;
mod persistence_recovery;
mod persistence_segments;
mod reliability;
mod retention_policy;
mod series_selection;

fn make_persisted_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
    assert!(!points.is_empty());
    let chunk_points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();
    let encoded = Encoder::encode_chunk_points(&chunk_points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            point_count: chunk_points.len() as u16,
            min_ts: chunk_points.first().unwrap().ts,
            max_ts: chunk_points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points: chunk_points,
        encoded_payload: encoded.payload,
    }
}
