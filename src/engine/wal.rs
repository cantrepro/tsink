use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex, MutexGuard};
use tracing::warn;

use crate::engine::binio::{
    append_i64, append_u16, append_u32, append_u64, append_u8, checksum32, read_bytes, read_i64,
    read_u16, read_u32, read_u32_at, read_u64, read_u64_at, read_u8, write_u32_at, write_u64_at,
};
use crate::engine::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};
use crate::engine::encoder::{EncodedChunk, Encoder};
use crate::engine::segment::WalHighWatermark;
use crate::engine::series::SeriesId;
use crate::wal::{WalReplayMode, WalSyncMode};
use crate::{Label, Result, TsinkError, Value};

mod codec;
mod logical;
mod replay;
mod segments;
mod series_index;
#[cfg(test)]
mod tests;

#[cfg(test)]
use codec::encode_series_definition;
use codec::{
    decode_samples_series_ids, decode_series_definition, merge_encoded_payload,
    split_encoded_payload,
};
pub(in crate::engine) use logical::LogicalWalWrite;
#[cfg(test)]
use replay::{replay_from_path, replay_from_path_with_mode};
pub use replay::{CommittedWalReplayStream, WalReplayStream};
#[cfg(test)]
use segments::{collect_wal_segment_files, scan_last_seq, segment_path};
use series_index::{CachedSeriesDefinitionFrame, CachedSeriesDefinitionIndex};

const WAL_FILE_NAME: &str = "wal.log";
const WAL_SEGMENT_FILE_PREFIX: &str = "wal-";
const WAL_SEGMENT_FILE_SUFFIX: &str = ".log";
const WAL_PUBLISHED_HIGHWATER_FILE_NAME: &str = "wal.published";
const WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME: &str = "wal.published.tmp";
const DEFAULT_WAL_BUFFER_SIZE: usize = 4096;
const DEFAULT_WAL_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const FRAME_MAGIC: [u8; 4] = *b"TSFR";
const FRAME_HEADER_LEN: usize = 24;
const MAX_FRAME_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const PUBLISHED_HIGHWATER_MAGIC: [u8; 4] = *b"TSHW";
const PUBLISHED_HIGHWATER_RECORD_LEN: usize = 24;

const FRAME_TYPE_SERIES_DEF: u8 = 1;
const FRAME_TYPE_SAMPLES: u8 = 2;

#[cfg(test)]
type WalAppendSyncHook = dyn Fn() -> Result<()> + Send + Sync + 'static;
#[cfg(test)]
type WalCachedSeriesDefinitionRebuildHook = dyn Fn() + Send + Sync + 'static;

#[derive(Debug, Clone)]
pub struct SeriesDefinitionFrame {
    pub series_id: SeriesId,
    pub metric: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone)]
pub struct SamplesBatchFrame {
    pub series_id: SeriesId,
    pub lane: ValueLane,
    pub ts_codec: TimestampCodecId,
    pub value_codec: ValueCodecId,
    pub point_count: u16,
    pub base_ts: i64,
    pub ts_payload: Vec<u8>,
    pub value_payload: Vec<u8>,
}

impl SamplesBatchFrame {
    pub fn from_points(
        series_id: SeriesId,
        lane: ValueLane,
        points: &[ChunkPoint],
    ) -> Result<Self> {
        let encoded = Encoder::encode_chunk_points(points, lane)?;
        let (ts_payload, value_payload) = split_encoded_payload(&encoded.payload)?;

        let base_ts = points.first().map(|point| point.ts).ok_or_else(|| {
            TsinkError::InvalidConfiguration("cannot WAL-encode empty batch".to_string())
        })?;

        let point_count = u16::try_from(points.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("WAL batch exceeds u16 point count".to_string())
        })?;

        Ok(Self {
            series_id,
            lane,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
            point_count,
            base_ts,
            ts_payload,
            value_payload,
        })
    }

    pub fn from_timestamp_value_refs(
        series_id: SeriesId,
        lane: ValueLane,
        points: &[(i64, &Value)],
    ) -> Result<Self> {
        let encoded = Encoder::encode_timestamp_value_refs(points, lane)?;
        let (ts_payload, value_payload) = split_encoded_payload(&encoded.payload)?;

        let base_ts = points.first().map(|point| point.0).ok_or_else(|| {
            TsinkError::InvalidConfiguration("cannot WAL-encode empty batch".to_string())
        })?;

        let point_count = u16::try_from(points.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("WAL batch exceeds u16 point count".to_string())
        })?;

        Ok(Self {
            series_id,
            lane,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
            point_count,
            base_ts,
            ts_payload,
            value_payload,
        })
    }

    pub fn decode_points(&self) -> Result<Vec<ChunkPoint>> {
        let payload = merge_encoded_payload(&self.ts_payload, &self.value_payload);
        let encoded = EncodedChunk {
            lane: self.lane,
            ts_codec: self.ts_codec,
            value_codec: self.value_codec,
            point_count: self.point_count as usize,
            payload,
        };

        let points = Encoder::decode_chunk_points(&encoded)?;
        if points.first().map(|point| point.ts) != Some(self.base_ts) {
            return Err(TsinkError::DataCorruption(
                "WAL batch base_ts does not match decoded timestamps".to_string(),
            ));
        }

        Ok(points)
    }
}

#[derive(Debug, Clone)]
pub enum ReplayFrame {
    SeriesDefinition(SeriesDefinitionFrame),
    Samples(Vec<SamplesBatchFrame>),
}

#[derive(Debug, Clone)]
pub struct CommittedWalWriteFrame {
    pub series_definitions: Vec<SeriesDefinitionFrame>,
    pub sample_batches: Vec<SamplesBatchFrame>,
    pub highwater: WalHighWatermark,
}

pub struct FramedWal {
    dir: PathBuf,
    path: Mutex<PathBuf>,
    published_highwater_path: PathBuf,
    published_highwater_tmp_path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    active_segment: AtomicU64,
    active_segment_size_bytes: AtomicU64,
    next_seq: AtomicU64,
    total_size_bytes: AtomicU64,
    segment_count: AtomicU64,
    cached_series_definition_index: Mutex<CachedSeriesDefinitionIndex>,
    cached_series_definition_index_ready: Condvar,
    last_appended_highwater: Mutex<WalHighWatermark>,
    last_published_highwater: Mutex<WalHighWatermark>,
    last_durable_highwater: Mutex<WalHighWatermark>,
    configured_replay_mode: Mutex<WalReplayMode>,
    sync_mode: WalSyncMode,
    last_sync: Mutex<Instant>,
    segment_max_bytes: u64,
    #[cfg(test)]
    append_sync_hook: Mutex<Option<Arc<WalAppendSyncHook>>>,
    #[cfg(test)]
    cached_series_definition_rebuild_hook: Mutex<Option<Arc<WalCachedSeriesDefinitionRebuildHook>>>,
}
