use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
pub(in crate::engine) use segments::validate_legacy_wal_identity;
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
/// Format-level safety ceiling for one decoded replay batch. Live write limits remain optional for
/// compatibility, but a compressed constant-RLE frame must never expand without a finite bound.
pub(in crate::engine) const MAX_WAL_REPLAY_DECODED_BATCH_BYTES: usize = 256 * 1024 * 1024;
const PUBLISHED_HIGHWATER_MAGIC: [u8; 4] = *b"TSHW";
const PUBLISHED_HIGHWATER_RECORD_LEN: usize = 24;

const FRAME_TYPE_SERIES_DEF: u8 = 1;
const FRAME_TYPE_SAMPLES: u8 = 2;

#[cfg(test)]
type WalAppendSyncHook = dyn Fn() -> Result<()> + Send + Sync + 'static;
#[cfg(test)]
type WalPublishedHighwaterPostRenameHook = dyn Fn() -> Result<()> + Send + Sync + 'static;
#[cfg(test)]
type WalCachedSeriesDefinitionRebuildHook = dyn Fn() + Send + Sync + 'static;
#[cfg(test)]
type WalDurabilityFailpointHook =
    dyn Fn(WalDurabilityFailpoint) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine) enum WalDurabilityFailpoint {
    SeriesDefinitionAppend,
    SamplesAppend,
    Flush,
    ResetAfterTruncate,
}

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
    pub(in crate::engine) fn modeled_decoded_points_peak_bytes(&self) -> Result<usize> {
        Encoder::modeled_decoded_chunk_peak_bytes(
            self.lane,
            self.value_codec,
            usize::from(self.point_count),
            &self.value_payload,
        )
    }

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

const WAL_INSPECTION_ALLOCATION_SLACK_BYTES: usize = 64;

/// Conservatively models the peak retained heap required to decode one inspector WAL payload.
///
/// The structural walk validates every encoded length and codec identifier without allocating.
pub(crate) fn modeled_frame_payload_for_inspection(
    frame_type: u8,
    payload: &[u8],
) -> Result<usize> {
    match frame_type {
        FRAME_TYPE_SERIES_DEF => {
            let mut pos = 0usize;
            let _series_id = read_u64(payload, &mut pos)?;
            let metric_len = usize::from(read_u16(payload, &mut pos)?);
            let metric = read_bytes(payload, &mut pos, metric_len)?;
            std::str::from_utf8(metric).map_err(|err| {
                TsinkError::DataCorruption(format!(
                    "series-definition metric is not valid UTF-8: {err}"
                ))
            })?;
            let label_count = usize::from(read_u16(payload, &mut pos)?);
            let mut string_bytes = metric_len;
            for _ in 0..label_count {
                let name_len = usize::from(read_u16(payload, &mut pos)?);
                let name = read_bytes(payload, &mut pos, name_len)?;
                std::str::from_utf8(name).map_err(|err| {
                    TsinkError::DataCorruption(format!(
                        "series-definition label name is not valid UTF-8: {err}"
                    ))
                })?;
                let value_len = usize::from(read_u16(payload, &mut pos)?);
                let value = read_bytes(payload, &mut pos, value_len)?;
                std::str::from_utf8(value).map_err(|err| {
                    TsinkError::DataCorruption(format!(
                        "series-definition label value is not valid UTF-8: {err}"
                    ))
                })?;
                string_bytes = string_bytes
                    .checked_add(name_len)
                    .and_then(|bytes| bytes.checked_add(value_len))
                    .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            }
            if pos != payload.len() {
                return Err(TsinkError::DataCorruption(
                    "series-definition payload has trailing bytes".to_string(),
                ));
            }
            let label_storage = label_count
                .checked_mul(std::mem::size_of::<Label>())
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            let string_allocations = label_count
                .checked_mul(2)
                .and_then(|count| count.checked_add(1))
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            let allocator_slack = string_allocations
                .checked_add(1)
                .and_then(|count| count.checked_mul(WAL_INSPECTION_ALLOCATION_SLACK_BYTES))
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            payload
                .len()
                .checked_add(std::mem::size_of::<SeriesDefinitionFrame>())
                .and_then(|bytes| bytes.checked_add(label_storage))
                .and_then(|bytes| bytes.checked_add(string_bytes))
                .and_then(|bytes| bytes.checked_add(allocator_slack))
                .ok_or(TsinkError::WriteBatchSizeOverflow)
        }
        FRAME_TYPE_SAMPLES => {
            let mut pos = 0usize;
            let batch_count = usize::from(read_u16(payload, &mut pos)?);
            let mut cloned_payload_bytes = 0usize;
            let mut largest_merged_payload = 0usize;
            let mut largest_decoded_points = 0usize;
            for _ in 0..batch_count {
                let _series_id = read_u64(payload, &mut pos)?;
                let lane = match read_u8(payload, &mut pos)? {
                    0 => ValueLane::Numeric,
                    1 => ValueLane::Blob,
                    raw => {
                        return Err(TsinkError::DataCorruption(format!(
                            "invalid value lane {raw} in WAL"
                        )))
                    }
                };
                let _ts_codec = match read_u8(payload, &mut pos)? {
                    1 => TimestampCodecId::FixedStepRle,
                    2 => TimestampCodecId::DeltaOfDeltaBitpack,
                    3 => TimestampCodecId::DeltaVarint,
                    raw => {
                        return Err(TsinkError::DataCorruption(format!(
                            "invalid timestamp codec id {raw} in WAL"
                        )))
                    }
                };
                let value_codec = match read_u8(payload, &mut pos)? {
                    1 => ValueCodecId::GorillaXorF64,
                    2 => ValueCodecId::ZigZagDeltaBitpackI64,
                    3 => ValueCodecId::DeltaBitpackU64,
                    4 => ValueCodecId::ConstantRle,
                    5 => ValueCodecId::BoolBitpack,
                    6 => ValueCodecId::BytesDeltaBlock,
                    raw => {
                        return Err(TsinkError::DataCorruption(format!(
                            "invalid value codec id {raw} in WAL"
                        )))
                    }
                };
                if read_u8(payload, &mut pos)? != 0 {
                    return Err(TsinkError::DataCorruption(
                        "samples WAL payload reserved byte is nonzero".to_string(),
                    ));
                }
                let point_count = usize::from(read_u16(payload, &mut pos)?);
                if point_count == 0 {
                    return Err(TsinkError::DataCorruption(
                        "samples WAL payload contains an empty batch".to_string(),
                    ));
                }
                let _base_ts = read_i64(payload, &mut pos)?;
                let ts_len = usize::try_from(read_u32(payload, &mut pos)?)
                    .map_err(|_| TsinkError::WriteBatchSizeOverflow)?;
                let value_len = usize::try_from(read_u32(payload, &mut pos)?)
                    .map_err(|_| TsinkError::WriteBatchSizeOverflow)?;
                let _ts_payload = read_bytes(payload, &mut pos, ts_len)?;
                let value_payload = read_bytes(payload, &mut pos, value_len)?;
                let batch_payload = ts_len
                    .checked_add(value_len)
                    .ok_or(TsinkError::WriteBatchSizeOverflow)?;
                cloned_payload_bytes = cloned_payload_bytes
                    .checked_add(batch_payload)
                    .ok_or(TsinkError::WriteBatchSizeOverflow)?;
                largest_merged_payload = largest_merged_payload.max(
                    batch_payload
                        .checked_add(8)
                        .ok_or(TsinkError::WriteBatchSizeOverflow)?,
                );
                let decoded_points = Encoder::modeled_decoded_chunk_peak_bytes(
                    lane,
                    value_codec,
                    point_count,
                    value_payload,
                )?
                .checked_add(
                    point_count
                        .checked_mul(4)
                        .and_then(|count| count.checked_mul(WAL_INSPECTION_ALLOCATION_SLACK_BYTES))
                        .ok_or(TsinkError::WriteBatchSizeOverflow)?,
                )
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
                largest_decoded_points = largest_decoded_points.max(decoded_points);
            }
            if pos != payload.len() {
                return Err(TsinkError::DataCorruption(
                    "samples payload has trailing bytes".to_string(),
                ));
            }
            let batch_structures = batch_count
                .checked_mul(std::mem::size_of::<SamplesBatchFrame>())
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            let allocations = batch_count
                .checked_mul(2)
                .and_then(|count| count.checked_add(4))
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            let allocator_slack = allocations
                .checked_mul(WAL_INSPECTION_ALLOCATION_SLACK_BYTES)
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            payload
                .len()
                .checked_add(batch_structures)
                .and_then(|bytes| bytes.checked_add(cloned_payload_bytes))
                .and_then(|bytes| bytes.checked_add(largest_merged_payload))
                .and_then(|bytes| bytes.checked_add(largest_decoded_points))
                .and_then(|bytes| bytes.checked_add(allocator_slack))
                .ok_or(TsinkError::WriteBatchSizeOverflow)
        }
        _ => Err(TsinkError::DataCorruption(format!(
            "unknown WAL frame type {frame_type}"
        ))),
    }
}

/// Strictly validates one already checksum-verified WAL payload for the read-only inspector.
///
/// `Ok(false)` means the caller's explicit decoded-memory limit was too small; malformed payloads
/// return a corruption error. This helper performs no filesystem operations.
pub(crate) fn validate_frame_payload_for_inspection(
    frame_type: u8,
    payload: &[u8],
    max_decoded_bytes: usize,
) -> Result<bool> {
    let required = modeled_frame_payload_for_inspection(frame_type, payload)?;
    if required > max_decoded_bytes {
        return Ok(false);
    }
    match frame_type {
        FRAME_TYPE_SERIES_DEF => {
            decode_series_definition(payload)?;
            Ok(true)
        }
        FRAME_TYPE_SAMPLES => {
            let batches = codec::decode_samples_payload(payload)?;
            for batch in &batches {
                batch.decode_points()?;
            }
            Ok(true)
        }
        _ => Err(TsinkError::DataCorruption(format!(
            "unknown WAL frame type {frame_type}"
        ))),
    }
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
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    #[cfg(test)]
    append_sync_hook: Mutex<Option<Arc<WalAppendSyncHook>>>,
    #[cfg(test)]
    published_highwater_post_rename_hook: Mutex<Option<Arc<WalPublishedHighwaterPostRenameHook>>>,
    #[cfg(test)]
    cached_series_definition_rebuild_hook: Mutex<Option<Arc<WalCachedSeriesDefinitionRebuildHook>>>,
    #[cfg(test)]
    durability_failpoint_hook: Mutex<Option<Arc<WalDurabilityFailpointHook>>>,
}

#[cfg(test)]
impl FramedWal {
    pub(in crate::engine) fn set_durability_failpoint_hook<F>(&self, hook: F)
    where
        F: Fn(WalDurabilityFailpoint) -> Result<()> + Send + Sync + 'static,
    {
        *self.durability_failpoint_hook.lock() = Some(Arc::new(hook));
    }

    pub(in crate::engine) fn clear_durability_failpoint_hook(&self) {
        *self.durability_failpoint_hook.lock() = None;
    }

    fn invoke_durability_failpoint(&self, point: WalDurabilityFailpoint) -> Result<()> {
        match self.durability_failpoint_hook.lock().clone() {
            Some(hook) => hook(point),
            None => Ok(()),
        }
    }
}
