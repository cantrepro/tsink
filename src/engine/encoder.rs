use crate::{DataPoint, NativeHistogram, Result, TsinkError, Value};

use super::binio::{read_array, read_bytes, read_i64, read_u32, read_u64};
use super::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};
use super::series::SeriesValueFamily;

#[path = "encoder/timestamps.rs"]
mod timestamps;
#[path = "encoder/values.rs"]
mod values;

use self::timestamps::{
    build_timestamp_search_index, candidate_anchor_block_for_ceiling, choose_best_timestamp_codec,
    decode_delta_of_delta_timestamp_block, decode_delta_varint_timestamp_block,
    decode_fixed_step_timestamp_block, decode_timestamps, split_chunk_payload,
};
use self::values::{
    choose_best_value_codec, decode_values, decode_values_in_index_range, infer_value_family,
};
#[cfg(test)]
use self::values::{decode_values_f64_xor, decode_values_f64_xor_range, encode_values_f64_xor};

#[derive(Debug, Clone)]
pub struct EncodedChunk {
    pub lane: ValueLane,
    pub ts_codec: TimestampCodecId,
    pub value_codec: ValueCodecId,
    pub point_count: usize,
    pub payload: Vec<u8>,
}

const TIMESTAMP_SEARCH_BLOCK_POINTS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimestampSearchIndex {
    FixedStep { first_ts: i64, step: i64 },
    DeltaVarint { anchors: Vec<DeltaVarintAnchor> },
    DeltaOfDelta { anchors: Vec<DeltaOfDeltaAnchor> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaVarintAnchor {
    point_idx: usize,
    timestamp: i64,
    payload_offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaOfDeltaAnchor {
    point_idx: usize,
    timestamp: i64,
    prev_delta: i64,
    payload_offset: usize,
}

impl TimestampSearchIndex {
    pub(crate) fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_sub(match self {
                Self::FixedStep { .. } => 0,
                Self::DeltaVarint { .. } => std::mem::size_of::<Vec<DeltaVarintAnchor>>(),
                Self::DeltaOfDelta { .. } => std::mem::size_of::<Vec<DeltaOfDeltaAnchor>>(),
            })
            .saturating_add(match self {
                Self::FixedStep { .. } => 0,
                Self::DeltaVarint { anchors } => anchors
                    .capacity()
                    .saturating_mul(std::mem::size_of::<DeltaVarintAnchor>()),
                Self::DeltaOfDelta { anchors } => anchors
                    .capacity()
                    .saturating_mul(std::mem::size_of::<DeltaOfDeltaAnchor>()),
            })
    }

    pub(crate) fn candidate_block_for_ceiling(
        &self,
        point_count: usize,
        ceiling_inclusive: i64,
    ) -> Option<usize> {
        if point_count == 0 {
            return None;
        }

        match self {
            Self::FixedStep { first_ts, step } => {
                if *first_ts > ceiling_inclusive {
                    return None;
                }
                let latest_idx = if *step <= 0 {
                    point_count.saturating_sub(1)
                } else {
                    let max_steps = ceiling_inclusive
                        .saturating_sub(*first_ts)
                        .checked_div(*step)
                        .unwrap_or(0);
                    usize::try_from(max_steps)
                        .unwrap_or(usize::MAX)
                        .min(point_count.saturating_sub(1))
                };
                Some(latest_idx / TIMESTAMP_SEARCH_BLOCK_POINTS)
            }
            Self::DeltaVarint { anchors } => candidate_anchor_block_for_ceiling(
                anchors.iter().map(|anchor| anchor.timestamp),
                ceiling_inclusive,
            ),
            Self::DeltaOfDelta { anchors } => candidate_anchor_block_for_ceiling(
                anchors.iter().map(|anchor| anchor.timestamp),
                ceiling_inclusive,
            ),
        }
    }

    pub(crate) fn decode_block(
        &self,
        point_count: usize,
        ts_payload: &[u8],
        block_idx: usize,
    ) -> Result<Vec<i64>> {
        match self {
            Self::FixedStep { first_ts, step } => {
                decode_fixed_step_timestamp_block(*first_ts, *step, point_count, block_idx)
            }
            Self::DeltaVarint { anchors } => {
                decode_delta_varint_timestamp_block(ts_payload, point_count, anchors, block_idx)
            }
            Self::DeltaOfDelta { anchors } => {
                decode_delta_of_delta_timestamp_block(ts_payload, point_count, anchors, block_idx)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueFamily {
    F64,
    I64,
    U64,
    Bool,
    Blob,
    Histogram,
}

trait EncodablePoint {
    fn ts(&self) -> i64;
    fn value(&self) -> &Value;
}

impl EncodablePoint for DataPoint {
    fn ts(&self) -> i64 {
        self.timestamp
    }

    fn value(&self) -> &Value {
        &self.value
    }
}

impl EncodablePoint for ChunkPoint {
    fn ts(&self) -> i64 {
        self.ts
    }

    fn value(&self) -> &Value {
        &self.value
    }
}

pub struct Encoder;

impl Encoder {
    pub fn choose_lane(points: &[DataPoint]) -> ValueLane {
        if points.iter().any(|point| {
            matches!(
                point.value,
                Value::Bytes(_) | Value::String(_) | Value::Histogram(_)
            )
        }) {
            ValueLane::Blob
        } else {
            ValueLane::Numeric
        }
    }

    pub fn choose_codecs(points: &[DataPoint]) -> Result<(TimestampCodecId, ValueCodecId)> {
        let lane = Self::choose_lane(points);
        Self::choose_codecs_for_points(points, lane)
    }

    pub fn encode(points: &[DataPoint]) -> Result<EncodedChunk> {
        if points.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot encode empty chunk".to_string(),
            ));
        }

        let lane = Self::choose_lane(points);
        Self::encode_points(points, lane)
    }

    pub fn decode(encoded: &EncodedChunk) -> Result<Vec<DataPoint>> {
        Self::decode_chunk_points(encoded).map(|points| {
            points
                .into_iter()
                .map(|point| DataPoint::new(point.ts, point.value))
                .collect()
        })
    }

    pub fn encode_chunk_points(points: &[ChunkPoint], lane: ValueLane) -> Result<EncodedChunk> {
        Self::encode_points(points, lane)
    }

    pub fn encode_timestamp_value_refs(
        points: &[(i64, &Value)],
        lane: ValueLane,
    ) -> Result<EncodedChunk> {
        #[derive(Clone, Copy)]
        struct TimestampValueRef<'a> {
            ts: i64,
            value: &'a Value,
        }

        impl EncodablePoint for TimestampValueRef<'_> {
            fn ts(&self) -> i64 {
                self.ts
            }

            fn value(&self) -> &Value {
                self.value
            }
        }

        let refs = points
            .iter()
            .map(|(ts, value)| TimestampValueRef { ts: *ts, value })
            .collect::<Vec<_>>();
        Self::encode_points(&refs, lane)
    }

    pub fn validate_chunk_points(points: &[ChunkPoint], lane: ValueLane) -> Result<()> {
        infer_value_family(points, lane)?;
        Ok(())
    }

    pub(crate) fn infer_series_value_family(
        points: &[ChunkPoint],
        lane: ValueLane,
    ) -> Result<SeriesValueFamily> {
        Ok(match infer_value_family(points, lane)? {
            ValueFamily::F64 => SeriesValueFamily::F64,
            ValueFamily::I64 => SeriesValueFamily::I64,
            ValueFamily::U64 => SeriesValueFamily::U64,
            ValueFamily::Bool => SeriesValueFamily::Bool,
            ValueFamily::Blob => SeriesValueFamily::Blob,
            ValueFamily::Histogram => SeriesValueFamily::Histogram,
        })
    }

    fn choose_codecs_for_points<P: EncodablePoint>(
        points: &[P],
        lane: ValueLane,
    ) -> Result<(TimestampCodecId, ValueCodecId)> {
        if points.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot encode empty chunk".to_string(),
            ));
        }

        let (ts_codec, _) = choose_best_timestamp_codec(points)?;
        let (value_codec, _) = choose_best_value_codec(points, lane)?;
        Ok((ts_codec, value_codec))
    }

    fn encode_points<P: EncodablePoint>(points: &[P], lane: ValueLane) -> Result<EncodedChunk> {
        if points.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot encode empty chunk".to_string(),
            ));
        }

        let (ts_codec, ts_payload) = choose_best_timestamp_codec(points)?;
        let (value_codec, value_payload) = choose_best_value_codec(points, lane)?;

        let mut payload = Vec::with_capacity(8 + ts_payload.len() + value_payload.len());
        payload.extend_from_slice(&(ts_payload.len() as u32).to_le_bytes());
        payload.extend_from_slice(&ts_payload);
        payload.extend_from_slice(&(value_payload.len() as u32).to_le_bytes());
        payload.extend_from_slice(&value_payload);

        Ok(EncodedChunk {
            lane,
            ts_codec,
            value_codec,
            point_count: points.len(),
            payload,
        })
    }

    pub fn decode_chunk_points(encoded: &EncodedChunk) -> Result<Vec<ChunkPoint>> {
        Self::decode_chunk_points_from_payload(
            encoded.lane,
            encoded.ts_codec,
            encoded.value_codec,
            encoded.point_count,
            &encoded.payload,
        )
    }

    pub fn decode_chunk_points_from_payload(
        lane: ValueLane,
        ts_codec: TimestampCodecId,
        value_codec: ValueCodecId,
        point_count: usize,
        payload: &[u8],
    ) -> Result<Vec<ChunkPoint>> {
        if point_count == 0 {
            return Ok(Vec::new());
        }

        let mut pos = 0usize;
        let ts_len = read_u32(payload, &mut pos)? as usize;
        let ts_payload = read_bytes(payload, &mut pos, ts_len)?;
        let value_len = read_u32(payload, &mut pos)? as usize;
        let value_payload = read_bytes(payload, &mut pos, value_len)?;

        if pos != payload.len() {
            return Err(TsinkError::DataCorruption(
                "encoded chunk payload has trailing bytes".to_string(),
            ));
        }

        let timestamps = decode_timestamps(ts_codec, ts_payload, point_count)?;
        let values = decode_values(value_codec, lane, value_payload, point_count)?;

        if timestamps.len() != values.len() {
            return Err(TsinkError::DataCorruption(
                "timestamp/value count mismatch after decode".to_string(),
            ));
        }

        Ok(timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, value)| ChunkPoint { ts, value })
            .collect())
    }

    pub fn decode_timestamps_from_payload(
        ts_codec: TimestampCodecId,
        point_count: usize,
        payload: &[u8],
    ) -> Result<Vec<i64>> {
        if point_count == 0 {
            return Ok(Vec::new());
        }

        let mut pos = 0usize;
        let ts_len = read_u32(payload, &mut pos)? as usize;
        let ts_payload = read_bytes(payload, &mut pos, ts_len)?;
        let value_len = read_u32(payload, &mut pos)? as usize;
        let _value_payload = read_bytes(payload, &mut pos, value_len)?;

        if pos != payload.len() {
            return Err(TsinkError::DataCorruption(
                "encoded chunk payload has trailing bytes".to_string(),
            ));
        }

        decode_timestamps(ts_codec, ts_payload, point_count)
    }

    pub(crate) fn timestamp_payload_from_chunk_payload(payload: &[u8]) -> Result<&[u8]> {
        let (ts_payload, _value_payload) = split_chunk_payload(payload)?;
        Ok(ts_payload)
    }

    pub(crate) fn build_timestamp_search_index_from_payload(
        ts_codec: TimestampCodecId,
        point_count: usize,
        payload: &[u8],
    ) -> Result<TimestampSearchIndex> {
        let ts_payload = Self::timestamp_payload_from_chunk_payload(payload)?;
        build_timestamp_search_index(ts_codec, point_count, ts_payload)
    }

    pub fn decode_chunk_points_from_payload_in_range(
        lane: ValueLane,
        ts_codec: TimestampCodecId,
        value_codec: ValueCodecId,
        point_count: usize,
        payload: &[u8],
        start: i64,
        end: i64,
    ) -> Result<Vec<ChunkPoint>> {
        if point_count == 0 || start >= end {
            return Ok(Vec::new());
        }

        let mut pos = 0usize;
        let ts_len = read_u32(payload, &mut pos)? as usize;
        let ts_payload = read_bytes(payload, &mut pos, ts_len)?;
        let value_len = read_u32(payload, &mut pos)? as usize;
        let value_payload = read_bytes(payload, &mut pos, value_len)?;

        if pos != payload.len() {
            return Err(TsinkError::DataCorruption(
                "encoded chunk payload has trailing bytes".to_string(),
            ));
        }

        let timestamps = decode_timestamps(ts_codec, ts_payload, point_count)?;
        let first = timestamps.partition_point(|ts| *ts < start);
        let last = timestamps.partition_point(|ts| *ts < end);

        if first >= last {
            return Ok(Vec::new());
        }

        let values = decode_values_in_index_range(
            value_codec,
            lane,
            value_payload,
            point_count,
            first,
            last,
        )?;

        if values.len() != last.saturating_sub(first) {
            return Err(TsinkError::DataCorruption(
                "timestamp/value count mismatch after range decode".to_string(),
            ));
        }

        Ok(timestamps[first..last]
            .iter()
            .copied()
            .zip(values)
            .map(|(ts, value)| ChunkPoint { ts, value })
            .collect())
    }
}

fn choose_smallest<T>(mut candidates: Vec<(T, Vec<u8>)>) -> Result<(T, Vec<u8>)> {
    if candidates.is_empty() {
        return Err(TsinkError::Codec(
            "no codec candidates produced for chunk".to_string(),
        ));
    }

    candidates.sort_by_key(|(_, payload)| payload.len());
    Ok(candidates.swap_remove(0))
}

fn encode_uvarint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_uvarint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut x = 0u64;
    let mut shift = 0u32;

    while shift <= 63 {
        let byte = *bytes.get(*pos).ok_or_else(|| {
            TsinkError::DataCorruption("uvarint is truncated at end of payload".to_string())
        })?;
        *pos += 1;

        if byte < 0x80 {
            if shift == 63 && byte > 1 {
                return Err(TsinkError::DataCorruption(
                    "uvarint overflow while decoding".to_string(),
                ));
            }
            return Ok(x | ((byte as u64) << shift));
        }

        x |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
    }

    Err(TsinkError::DataCorruption(
        "uvarint overflow while decoding".to_string(),
    ))
}

fn encode_svarint(value: i64, out: &mut Vec<u8>) {
    let zigzag = ((value as u64) << 1) ^ ((value >> 63) as u64);
    encode_uvarint(zigzag, out);
}

fn decode_svarint(bytes: &[u8], pos: &mut usize) -> Result<i64> {
    let zigzag = decode_uvarint(bytes, pos)?;
    Ok(((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64)))
}

#[cfg(test)]
#[path = "encoder/tests.rs"]
mod tests;
