use crate::{DataPoint, Result, TsinkError, Value};

use super::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};

#[derive(Debug, Clone)]
pub struct EncodedChunk {
    pub lane: ValueLane,
    pub ts_codec: TimestampCodecId,
    pub value_codec: ValueCodecId,
    pub point_count: usize,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueFamily {
    F64,
    I64,
    U64,
    Bool,
    Blob,
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
        if points
            .iter()
            .any(|point| matches!(point.value, Value::Bytes(_) | Value::String(_)))
        {
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

    pub fn validate_chunk_points(points: &[ChunkPoint], lane: ValueLane) -> Result<()> {
        infer_value_family(points, lane)?;
        Ok(())
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

fn choose_best_timestamp_codec<P: EncodablePoint>(
    points: &[P],
) -> Result<(TimestampCodecId, Vec<u8>)> {
    let mut candidates = Vec::with_capacity(3);

    if let Some(payload) = encode_timestamps_fixed_step_rle(points)? {
        candidates.push((TimestampCodecId::FixedStepRle, payload));
    }

    // Delta-of-delta can overflow when successive deltas swing across i64 extremes.
    // Keep evaluating other codecs instead of aborting selection immediately.
    if let Ok(payload) = encode_timestamps_delta_of_delta(points) {
        candidates.push((TimestampCodecId::DeltaOfDeltaBitpack, payload));
    }
    candidates.push((
        TimestampCodecId::DeltaVarint,
        encode_timestamps_delta_varint(points)?,
    ));

    choose_smallest(candidates)
}

fn choose_best_value_codec<P: EncodablePoint>(
    points: &[P],
    lane: ValueLane,
) -> Result<(ValueCodecId, Vec<u8>)> {
    let family = infer_value_family(points, lane)?;
    let mut candidates = Vec::new();

    if let Some(payload) = encode_values_constant_rle(points) {
        candidates.push((ValueCodecId::ConstantRle, payload));
    }

    match family {
        ValueFamily::F64 => {
            candidates.push((ValueCodecId::GorillaXorF64, encode_values_f64_xor(points)?));
        }
        ValueFamily::I64 => {
            candidates.push((
                ValueCodecId::ZigZagDeltaBitpackI64,
                encode_values_i64_delta(points)?,
            ));
        }
        ValueFamily::U64 => {
            candidates.push((
                ValueCodecId::DeltaBitpackU64,
                encode_values_u64_delta(points)?,
            ));
        }
        ValueFamily::Bool => {
            candidates.push((
                ValueCodecId::BoolBitpack,
                encode_values_bool_bitpack(points)?,
            ));
        }
        ValueFamily::Blob => {
            candidates.push((
                ValueCodecId::BytesDeltaBlock,
                encode_values_blob_delta_block(points)?,
            ));
        }
    }

    choose_smallest(candidates)
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

fn infer_value_family<P: EncodablePoint>(points: &[P], lane: ValueLane) -> Result<ValueFamily> {
    let Some(first) = points.first() else {
        return Err(TsinkError::Codec(
            "cannot infer value family from empty chunk".to_string(),
        ));
    };

    let first_family = match (first.value(), lane) {
        (Value::F64(_), ValueLane::Numeric) => ValueFamily::F64,
        (Value::I64(_), ValueLane::Numeric) => ValueFamily::I64,
        (Value::U64(_), ValueLane::Numeric) => ValueFamily::U64,
        (Value::Bool(_), ValueLane::Numeric) => ValueFamily::Bool,
        (Value::Bytes(_) | Value::String(_), ValueLane::Blob) => ValueFamily::Blob,
        (value, ValueLane::Numeric) => {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "numeric lane value".to_string(),
                actual: value.kind().to_string(),
            });
        }
        (value, ValueLane::Blob) => {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "blob lane value".to_string(),
                actual: value.kind().to_string(),
            });
        }
    };

    for point in points.iter().skip(1) {
        let ok = matches!(
            (point.value(), first_family),
            (Value::F64(_), ValueFamily::F64)
                | (Value::I64(_), ValueFamily::I64)
                | (Value::U64(_), ValueFamily::U64)
                | (Value::Bool(_), ValueFamily::Bool)
                | (Value::Bytes(_), ValueFamily::Blob)
                | (Value::String(_), ValueFamily::Blob)
        );

        if !ok {
            return Err(TsinkError::ValueTypeMismatch {
                expected: match first_family {
                    ValueFamily::F64 => "f64",
                    ValueFamily::I64 => "i64",
                    ValueFamily::U64 => "u64",
                    ValueFamily::Bool => "bool",
                    ValueFamily::Blob => "bytes/string",
                }
                .to_string(),
                actual: point.value().kind().to_string(),
            });
        }
    }

    Ok(first_family)
}

fn encode_timestamps_fixed_step_rle<P: EncodablePoint>(points: &[P]) -> Result<Option<Vec<u8>>> {
    let Some(first) = points.first() else {
        return Ok(None);
    };
    let first_ts = first.ts();
    let step = if points.len() > 1 {
        points[1].ts().checked_sub(points[0].ts()).ok_or_else(|| {
            TsinkError::Codec(
                "timestamp delta overflow while encoding fixed-step timestamps".to_string(),
            )
        })?
    } else {
        0
    };

    let mut is_fixed_step = true;
    for window in points.windows(2) {
        let delta = window[1].ts().checked_sub(window[0].ts()).ok_or_else(|| {
            TsinkError::Codec(
                "timestamp delta overflow while encoding fixed-step timestamps".to_string(),
            )
        })?;
        if delta != step {
            is_fixed_step = false;
            break;
        }
    }

    if is_fixed_step {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&first_ts.to_le_bytes());
        out.extend_from_slice(&step.to_le_bytes());
        Ok(Some(out))
    } else {
        Ok(None)
    }
}

fn encode_timestamps_delta_of_delta<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&points[0].ts().to_le_bytes());

    if points.len() == 1 {
        return Ok(out);
    }

    let mut prev_delta = points[1].ts().checked_sub(points[0].ts()).ok_or_else(|| {
        TsinkError::Codec(
            "timestamp delta overflow while encoding delta-of-delta timestamps".to_string(),
        )
    })?;
    out.extend_from_slice(&prev_delta.to_le_bytes());

    for window in points.windows(2).skip(1) {
        let delta = window[1].ts().checked_sub(window[0].ts()).ok_or_else(|| {
            TsinkError::Codec(
                "timestamp delta overflow while encoding delta-of-delta timestamps".to_string(),
            )
        })?;
        let dod = delta.checked_sub(prev_delta).ok_or_else(|| {
            TsinkError::Codec(
                "timestamp delta-of-delta overflow while encoding timestamps".to_string(),
            )
        })?;
        encode_svarint(dod, &mut out);
        prev_delta = delta;
    }

    Ok(out)
}

fn encode_timestamps_delta_varint<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&points[0].ts().to_le_bytes());

    for window in points.windows(2) {
        let delta = window[1].ts().checked_sub(window[0].ts()).ok_or_else(|| {
            TsinkError::Codec(
                "timestamp delta overflow while encoding delta-varint timestamps".to_string(),
            )
        })?;
        encode_svarint(delta, &mut out);
    }

    Ok(out)
}

fn decode_timestamps(
    codec: TimestampCodecId,
    payload: &[u8],
    point_count: usize,
) -> Result<Vec<i64>> {
    match codec {
        TimestampCodecId::FixedStepRle => decode_timestamps_fixed_step_rle(payload, point_count),
        TimestampCodecId::DeltaOfDeltaBitpack => {
            decode_timestamps_delta_of_delta(payload, point_count)
        }
        TimestampCodecId::DeltaVarint => decode_timestamps_delta_varint(payload, point_count),
    }
}

fn decode_timestamps_fixed_step_rle(payload: &[u8], point_count: usize) -> Result<Vec<i64>> {
    if payload.len() != 16 {
        return Err(TsinkError::DataCorruption(
            "fixed-step payload must be exactly 16 bytes".to_string(),
        ));
    }

    let first_ts = i64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    let step = i64::from_le_bytes(payload[8..16].try_into().unwrap_or([0; 8]));

    let mut out = Vec::with_capacity(point_count);
    for i in 0..point_count {
        let idx = i64::try_from(i).map_err(|_| {
            TsinkError::DataCorruption("fixed-step payload index exceeds i64 range".to_string())
        })?;
        let offset = step.checked_mul(idx).ok_or_else(|| {
            TsinkError::DataCorruption("fixed-step timestamp overflow while decoding".to_string())
        })?;
        let ts = first_ts.checked_add(offset).ok_or_else(|| {
            TsinkError::DataCorruption("fixed-step timestamp overflow while decoding".to_string())
        })?;
        out.push(ts);
    }
    Ok(out)
}

fn decode_timestamps_delta_of_delta(payload: &[u8], point_count: usize) -> Result<Vec<i64>> {
    if point_count == 0 {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "delta-of-delta payload missing first timestamp".to_string(),
        ));
    }

    let mut pos = 0usize;
    let first_ts = read_i64(payload, &mut pos)?;
    let mut out = Vec::with_capacity(point_count);
    out.push(first_ts);

    if point_count == 1 {
        return Ok(out);
    }

    if payload.len().saturating_sub(pos) < 8 {
        return Err(TsinkError::DataCorruption(
            "delta-of-delta payload missing first delta".to_string(),
        ));
    }

    let mut prev_delta = read_i64(payload, &mut pos)?;
    let second = first_ts.checked_add(prev_delta).ok_or_else(|| {
        TsinkError::DataCorruption("delta-of-delta timestamp overflow while decoding".to_string())
    })?;
    out.push(second);

    while out.len() < point_count {
        let dod = decode_svarint(payload, &mut pos)?;
        let delta = prev_delta.checked_add(dod).ok_or_else(|| {
            TsinkError::DataCorruption("delta-of-delta delta overflow while decoding".to_string())
        })?;
        let next = out
            .last()
            .copied()
            .unwrap_or(first_ts)
            .checked_add(delta)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "delta-of-delta timestamp overflow while decoding".to_string(),
                )
            })?;
        out.push(next);
        prev_delta = delta;
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "delta-of-delta payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn decode_timestamps_delta_varint(payload: &[u8], point_count: usize) -> Result<Vec<i64>> {
    if point_count == 0 {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "delta-varint payload missing first timestamp".to_string(),
        ));
    }

    let mut pos = 0usize;
    let first_ts = read_i64(payload, &mut pos)?;
    let mut out = Vec::with_capacity(point_count);
    out.push(first_ts);

    while out.len() < point_count {
        let delta = decode_svarint(payload, &mut pos)?;
        let next = out
            .last()
            .copied()
            .unwrap_or(first_ts)
            .checked_add(delta)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "delta-varint timestamp overflow while decoding".to_string(),
                )
            })?;
        out.push(next);
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "delta-varint payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn encode_values_constant_rle<P: EncodablePoint>(points: &[P]) -> Option<Vec<u8>> {
    let first = points.first()?.value();
    if points.iter().all(|point| point.value() == first) {
        encode_single_value(first).ok()
    } else {
        None
    }
}

fn encode_values_f64_xor<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(points.len().saturating_mul(8));
    let mut prev = match points.first() {
        Some(point) => match point.value() {
            Value::F64(v) => v.to_bits(),
            value => {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: "f64".to_string(),
                    actual: value.kind().to_string(),
                });
            }
        },
        None => return Ok(out),
    };

    out.extend_from_slice(&prev.to_le_bytes());

    if points.len() <= 1 {
        return Ok(out);
    }

    let mut writer = BitWriter::with_capacity(points.len().saturating_mul(2));
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;
    let mut has_prev_window = false;

    for point in points.iter().skip(1) {
        let Value::F64(v) = point.value() else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "f64".to_string(),
                actual: point.value().kind().to_string(),
            });
        };

        let bits = v.to_bits();
        let xor = bits ^ prev;

        if xor == 0 {
            writer.write_bit(false);
            prev = bits;
            continue;
        }

        writer.write_bit(true);
        let leading = (xor.leading_zeros() as u8).min(31);
        let trailing = xor.trailing_zeros() as u8;
        let can_reuse = has_prev_window && leading >= prev_leading && trailing >= prev_trailing;

        if can_reuse {
            writer.write_bit(false);
            let meaningful = 64u8.saturating_sub(prev_leading + prev_trailing);
            writer.write_bits(xor >> prev_trailing, meaningful);
        } else {
            writer.write_bit(true);
            let meaningful = 64u8.saturating_sub(leading + trailing);
            writer.write_bits(u64::from(leading), 5);
            let encoded_len = if meaningful == 64 { 0 } else { meaningful };
            writer.write_bits(u64::from(encoded_len), 6);
            writer.write_bits(xor >> trailing, meaningful);
            prev_leading = leading;
            prev_trailing = trailing;
            has_prev_window = true;
        }

        prev = bits;
    }

    out.extend_from_slice(&writer.finish());

    Ok(out)
}

fn encode_values_i64_delta<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let first = match points.first() {
        Some(point) => match point.value() {
            Value::I64(v) => *v,
            value => {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: "i64".to_string(),
                    actual: value.kind().to_string(),
                });
            }
        },
        None => return Ok(out),
    };

    out.extend_from_slice(&first.to_le_bytes());
    let mut prev = first;

    for point in points.iter().skip(1) {
        let Value::I64(v) = point.value() else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "i64".to_string(),
                actual: point.value().kind().to_string(),
            });
        };

        let delta = v.checked_sub(prev).ok_or_else(|| {
            TsinkError::Codec("i64 delta overflow while encoding values".to_string())
        })?;
        encode_svarint(delta, &mut out);
        prev = *v;
    }

    Ok(out)
}

fn encode_values_u64_delta<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let first = match points.first() {
        Some(point) => match point.value() {
            Value::U64(v) => *v,
            value => {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: "u64".to_string(),
                    actual: value.kind().to_string(),
                });
            }
        },
        None => return Ok(out),
    };

    out.extend_from_slice(&first.to_le_bytes());
    let mut prev = first;

    for point in points.iter().skip(1) {
        let Value::U64(v) = point.value() else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "u64".to_string(),
                actual: point.value().kind().to_string(),
            });
        };

        // Encode deltas in wrapping u64 space so every valid transition is representable.
        let delta = v.wrapping_sub(prev) as i64;
        encode_svarint(delta, &mut out);
        prev = *v;
    }

    Ok(out)
}

fn encode_values_bool_bitpack<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = vec![0u8; points.len().div_ceil(8)];

    for (idx, point) in points.iter().enumerate() {
        let Value::Bool(v) = point.value() else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "bool".to_string(),
                actual: point.value().kind().to_string(),
            });
        };

        if *v {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            out[byte_idx] |= 1 << bit_idx;
        }
    }

    Ok(out)
}

fn encode_values_blob_delta_block<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    for point in points {
        match point.value() {
            Value::Bytes(bytes) => {
                out.push(5);
                encode_uvarint(bytes.len() as u64, &mut out);
                out.extend_from_slice(bytes);
            }
            Value::String(text) => {
                out.push(6);
                let bytes = text.as_bytes();
                encode_uvarint(bytes.len() as u64, &mut out);
                out.extend_from_slice(bytes);
            }
            other => {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: "bytes|string".to_string(),
                    actual: other.kind().to_string(),
                });
            }
        }
    }

    Ok(out)
}

fn decode_values(
    codec: ValueCodecId,
    lane: ValueLane,
    payload: &[u8],
    point_count: usize,
) -> Result<Vec<Value>> {
    match codec {
        ValueCodecId::ConstantRle => decode_values_constant_rle(payload, point_count),
        ValueCodecId::GorillaXorF64 => decode_values_f64_xor(payload, point_count),
        ValueCodecId::ZigZagDeltaBitpackI64 => decode_values_i64_delta(payload, point_count),
        ValueCodecId::DeltaBitpackU64 => decode_values_u64_delta(payload, point_count),
        ValueCodecId::BoolBitpack => decode_values_bool_bitpack(payload, point_count),
        ValueCodecId::BytesDeltaBlock => decode_values_blob_delta_block(payload, point_count, lane),
    }
}

fn decode_values_in_index_range(
    codec: ValueCodecId,
    lane: ValueLane,
    payload: &[u8],
    point_count: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    if point_count == 0 || start_idx >= end_idx {
        return Ok(Vec::new());
    }
    if end_idx > point_count {
        return Err(TsinkError::DataCorruption(format!(
            "value index range [{start_idx}, {end_idx}) exceeds point count {point_count}",
        )));
    }

    match codec {
        ValueCodecId::ConstantRle => decode_values_constant_rle_range(payload, start_idx, end_idx),
        ValueCodecId::GorillaXorF64 => {
            decode_values_f64_xor_range(payload, point_count, start_idx, end_idx)
        }
        ValueCodecId::ZigZagDeltaBitpackI64 => {
            decode_values_i64_delta_range(payload, point_count, start_idx, end_idx)
        }
        ValueCodecId::DeltaBitpackU64 => {
            decode_values_u64_delta_range(payload, point_count, start_idx, end_idx)
        }
        ValueCodecId::BoolBitpack => {
            decode_values_bool_bitpack_range(payload, point_count, start_idx, end_idx)
        }
        ValueCodecId::BytesDeltaBlock => {
            decode_values_blob_delta_block_range(payload, point_count, lane, start_idx, end_idx)
        }
    }
}

fn decode_values_constant_rle(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    let (value, used) = decode_single_value(payload)?;
    if used != payload.len() {
        return Err(TsinkError::DataCorruption(
            "constant-rle payload has trailing bytes".to_string(),
        ));
    }

    Ok(std::iter::repeat_n(value, point_count).collect())
}

fn decode_values_constant_rle_range(
    payload: &[u8],
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    let (value, used) = decode_single_value(payload)?;
    if used != payload.len() {
        return Err(TsinkError::DataCorruption(
            "constant-rle payload has trailing bytes".to_string(),
        ));
    }

    Ok(std::iter::repeat_n(value, end_idx.saturating_sub(start_idx)).collect())
}

fn decode_values_f64_xor(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    decode_values_f64_xor_in_range(payload, point_count, 0, point_count)
}

fn decode_values_f64_xor_range(
    payload: &[u8],
    point_count: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    decode_values_f64_xor_in_range(payload, point_count, start_idx, end_idx)
}

fn decode_values_f64_xor_in_range(
    payload: &[u8],
    point_count: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    if point_count == 0 || start_idx >= end_idx {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "f64 xor payload missing first value".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(end_idx.saturating_sub(start_idx));
    let mut pos = 0usize;
    let mut prev = read_u64(payload, &mut pos)?;
    if start_idx == 0 {
        out.push(Value::F64(f64::from_bits(prev)));
    }

    if point_count == 1 {
        if payload.len() != pos {
            return Err(TsinkError::DataCorruption(
                "f64 xor payload has trailing bytes".to_string(),
            ));
        }
        return Ok(out);
    }

    let mut reader = BitReader::new(&payload[pos..]);
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;
    let mut has_prev_window = false;

    for idx in 1..end_idx {
        let bits = if !reader.read_bit()? {
            prev
        } else {
            let xor = if !reader.read_bit()? {
                if !has_prev_window {
                    return Err(TsinkError::DataCorruption(
                        "f64 xor payload reused window before initialization".to_string(),
                    ));
                }
                let meaningful = 64u8.saturating_sub(prev_leading + prev_trailing);
                let significant = reader.read_bits(meaningful)?;
                significant << prev_trailing
            } else {
                let leading = reader.read_bits(5)? as u8;
                let encoded_len = reader.read_bits(6)? as u8;
                let meaningful = if encoded_len == 0 { 64 } else { encoded_len };
                if leading + meaningful > 64 {
                    return Err(TsinkError::DataCorruption(
                        "f64 xor payload has invalid significant-bit window".to_string(),
                    ));
                }
                let trailing = 64u8.saturating_sub(leading + meaningful);
                let significant = reader.read_bits(meaningful)?;
                prev_leading = leading;
                prev_trailing = trailing;
                has_prev_window = true;
                significant << trailing
            };
            prev ^ xor
        };

        if idx >= start_idx {
            out.push(Value::F64(f64::from_bits(bits)));
        }
        prev = bits;
    }

    if end_idx == point_count {
        reader.ensure_terminated()?;
    }

    Ok(out)
}

fn decode_values_i64_delta(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    if point_count == 0 {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "i64 delta payload missing first value".to_string(),
        ));
    }

    let mut pos = 0usize;
    let first = read_i64(payload, &mut pos)?;

    let mut out = Vec::with_capacity(point_count);
    out.push(Value::I64(first));
    let mut prev = first;

    while out.len() < point_count {
        let delta = decode_svarint(payload, &mut pos)?;
        let next = prev.checked_add(delta).ok_or_else(|| {
            TsinkError::DataCorruption("i64 delta overflow while decoding values".to_string())
        })?;
        out.push(Value::I64(next));
        prev = next;
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "i64 delta payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn decode_values_i64_delta_range(
    payload: &[u8],
    point_count: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    if point_count == 0 || start_idx >= end_idx {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "i64 delta payload missing first value".to_string(),
        ));
    }

    let mut pos = 0usize;
    let mut prev = read_i64(payload, &mut pos)?;

    let mut out = Vec::with_capacity(end_idx.saturating_sub(start_idx));
    if start_idx == 0 {
        out.push(Value::I64(prev));
    }

    for idx in 1..end_idx {
        let delta = decode_svarint(payload, &mut pos)?;
        let next = prev.checked_add(delta).ok_or_else(|| {
            TsinkError::DataCorruption("i64 delta overflow while decoding values".to_string())
        })?;
        if idx >= start_idx {
            out.push(Value::I64(next));
        }
        prev = next;
    }

    if end_idx == point_count && pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "i64 delta payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn decode_values_u64_delta(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    if point_count == 0 {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "u64 delta payload missing first value".to_string(),
        ));
    }

    let mut pos = 0usize;
    let first = read_u64(payload, &mut pos)?;

    let mut out = Vec::with_capacity(point_count);
    out.push(Value::U64(first));
    let mut prev = first;

    while out.len() < point_count {
        let delta = decode_svarint(payload, &mut pos)?;
        let next = prev.wrapping_add(delta as u64);

        out.push(Value::U64(next));
        prev = next;
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "u64 delta payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn decode_values_u64_delta_range(
    payload: &[u8],
    point_count: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    if point_count == 0 || start_idx >= end_idx {
        return Ok(Vec::new());
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "u64 delta payload missing first value".to_string(),
        ));
    }

    let mut pos = 0usize;
    let mut prev = read_u64(payload, &mut pos)?;

    let mut out = Vec::with_capacity(end_idx.saturating_sub(start_idx));
    if start_idx == 0 {
        out.push(Value::U64(prev));
    }

    for idx in 1..end_idx {
        let delta = decode_svarint(payload, &mut pos)?;
        let next = prev.wrapping_add(delta as u64);
        if idx >= start_idx {
            out.push(Value::U64(next));
        }
        prev = next;
    }

    if end_idx == point_count && pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "u64 delta payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn decode_values_bool_bitpack(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    let expected_len = point_count.div_ceil(8);
    if payload.len() != expected_len {
        return Err(TsinkError::DataCorruption(format!(
            "bool bitpack payload length mismatch: expected {expected_len}, got {}",
            payload.len()
        )));
    }

    let mut out = Vec::with_capacity(point_count);
    for idx in 0..point_count {
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        let value = (payload[byte_idx] >> bit_idx) & 1 == 1;
        out.push(Value::Bool(value));
    }

    Ok(out)
}

fn decode_values_bool_bitpack_range(
    payload: &[u8],
    point_count: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    let expected_len = point_count.div_ceil(8);
    if payload.len() != expected_len {
        return Err(TsinkError::DataCorruption(format!(
            "bool bitpack payload length mismatch: expected {expected_len}, got {}",
            payload.len()
        )));
    }

    let mut out = Vec::with_capacity(end_idx.saturating_sub(start_idx));
    for idx in start_idx..end_idx {
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        let value = (payload[byte_idx] >> bit_idx) & 1 == 1;
        out.push(Value::Bool(value));
    }

    Ok(out)
}

fn decode_values_blob_delta_block(
    payload: &[u8],
    point_count: usize,
    lane: ValueLane,
) -> Result<Vec<Value>> {
    if lane != ValueLane::Blob {
        return Err(TsinkError::ValueTypeMismatch {
            expected: "blob lane".to_string(),
            actual: "numeric lane".to_string(),
        });
    }

    let mut pos = 0usize;
    let mut out = Vec::with_capacity(point_count);

    while out.len() < point_count {
        let tag = *payload.get(pos).ok_or_else(|| {
            TsinkError::DataCorruption("blob payload truncated while reading tag".to_string())
        })?;
        pos += 1;

        let len = decode_uvarint(payload, &mut pos)? as usize;
        let bytes = read_bytes(payload, &mut pos, len)?;

        let value = match tag {
            5 => Value::Bytes(bytes.to_vec()),
            6 => Value::String(String::from_utf8(bytes.to_vec())?),
            other => {
                return Err(TsinkError::DataCorruption(format!(
                    "unknown blob value tag {other}"
                )));
            }
        };

        out.push(value);
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "blob payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn decode_values_blob_delta_block_range(
    payload: &[u8],
    point_count: usize,
    lane: ValueLane,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<Value>> {
    if lane != ValueLane::Blob {
        return Err(TsinkError::ValueTypeMismatch {
            expected: "blob lane".to_string(),
            actual: "numeric lane".to_string(),
        });
    }

    let mut pos = 0usize;
    let mut out = Vec::with_capacity(end_idx.saturating_sub(start_idx));
    for idx in 0..end_idx.min(point_count) {
        let tag = *payload.get(pos).ok_or_else(|| {
            TsinkError::DataCorruption("blob payload truncated while reading tag".to_string())
        })?;
        pos += 1;

        let len = decode_uvarint(payload, &mut pos)? as usize;
        let bytes = read_bytes(payload, &mut pos, len)?;

        if idx < start_idx {
            match tag {
                5 => {}
                6 => {
                    std::str::from_utf8(bytes).map_err(|err| {
                        TsinkError::DataCorruption(format!(
                            "blob string payload is not valid UTF-8: {err}"
                        ))
                    })?;
                }
                _ => {
                    return Err(TsinkError::DataCorruption(format!(
                        "unknown blob value tag {tag}"
                    )));
                }
            }
            continue;
        }

        let value = match tag {
            5 => Value::Bytes(bytes.to_vec()),
            6 => Value::String(String::from_utf8(bytes.to_vec())?),
            other => {
                return Err(TsinkError::DataCorruption(format!(
                    "unknown blob value tag {other}"
                )));
            }
        };
        out.push(value);
    }

    if end_idx == point_count && pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "blob payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn encode_single_value(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    match value {
        Value::F64(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::I64(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::U64(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Bool(v) => {
            out.push(4);
            out.push(u8::from(*v));
        }
        Value::Bytes(bytes) => {
            out.push(5);
            encode_uvarint(bytes.len() as u64, &mut out);
            out.extend_from_slice(bytes);
        }
        Value::String(text) => {
            out.push(6);
            let bytes = text.as_bytes();
            encode_uvarint(bytes.len() as u64, &mut out);
            out.extend_from_slice(bytes);
        }
    }

    Ok(out)
}

fn decode_single_value(bytes: &[u8]) -> Result<(Value, usize)> {
    if bytes.is_empty() {
        return Err(TsinkError::DataCorruption(
            "constant-rle payload is empty".to_string(),
        ));
    }

    let tag = bytes[0];
    let mut pos = 1usize;

    let value = match tag {
        1 => {
            let value = f64::from_le_bytes(read_array8(bytes, &mut pos)?);
            Value::F64(value)
        }
        2 => {
            let value = i64::from_le_bytes(read_array8(bytes, &mut pos)?);
            Value::I64(value)
        }
        3 => {
            let value = u64::from_le_bytes(read_array8(bytes, &mut pos)?);
            Value::U64(value)
        }
        4 => {
            let raw = *bytes.get(pos).ok_or_else(|| {
                TsinkError::DataCorruption("constant bool payload is truncated".to_string())
            })?;
            pos += 1;
            Value::Bool(raw != 0)
        }
        5 => {
            let len = decode_uvarint(bytes, &mut pos)? as usize;
            let payload = read_bytes(bytes, &mut pos, len)?;
            Value::Bytes(payload.to_vec())
        }
        6 => {
            let len = decode_uvarint(bytes, &mut pos)? as usize;
            let payload = read_bytes(bytes, &mut pos, len)?;
            Value::String(String::from_utf8(payload.to_vec())?)
        }
        _ => {
            return Err(TsinkError::DataCorruption(format!(
                "unknown constant value tag {tag}"
            )));
        }
    };

    Ok((value, pos))
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

fn read_array8(bytes: &[u8], pos: &mut usize) -> Result<[u8; 8]> {
    let data = read_bytes(bytes, pos, 8)?;
    let mut out = [0u8; 8];
    out.copy_from_slice(data);
    Ok(out)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    let raw = read_array4(bytes, pos)?;
    Ok(u32::from_le_bytes(raw))
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> Result<i64> {
    let raw = read_array8(bytes, pos)?;
    Ok(i64::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let raw = read_array8(bytes, pos)?;
    Ok(u64::from_le_bytes(raw))
}

fn read_array4(bytes: &[u8], pos: &mut usize) -> Result<[u8; 4]> {
    let data = read_bytes(bytes, pos, 4)?;
    let mut out = [0u8; 4];
    out.copy_from_slice(data);
    Ok(out)
}

fn read_bytes<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = pos.saturating_add(len);
    if end > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "payload truncated: need {} bytes, have {}",
            len,
            bytes.len().saturating_sub(*pos)
        )));
    }

    let slice = &bytes[*pos..end];
    *pos = end;
    Ok(slice)
}

struct BitWriter {
    bytes: Vec<u8>,
    bit_in_byte: u8,
}

impl BitWriter {
    fn with_capacity(bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bytes),
            bit_in_byte: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if self.bit_in_byte == 0 {
            self.bytes.push(0);
        }
        if bit {
            let byte_idx = self.bytes.len().saturating_sub(1);
            self.bytes[byte_idx] |= 1 << (7 - self.bit_in_byte);
        }
        self.bit_in_byte = (self.bit_in_byte + 1) % 8;
    }

    fn write_bits(&mut self, bits: u64, bit_count: u8) {
        let bit_count = usize::from(bit_count);
        for shift in (0..bit_count).rev() {
            self.write_bit(((bits >> shift) & 1) != 0);
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct BitReader<'a> {
    bytes: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, bit_pos: 0 }
    }

    fn read_bit(&mut self) -> Result<bool> {
        if self.bit_pos >= self.bytes.len().saturating_mul(8) {
            return Err(TsinkError::DataCorruption(
                "f64 xor payload is truncated while reading bits".to_string(),
            ));
        }

        let byte_idx = self.bit_pos / 8;
        let bit_in_byte = self.bit_pos % 8;
        self.bit_pos += 1;

        Ok(((self.bytes[byte_idx] >> (7 - bit_in_byte)) & 1) != 0)
    }

    fn read_bits(&mut self, bit_count: u8) -> Result<u64> {
        let mut out = 0u64;
        for _ in 0..bit_count {
            out <<= 1;
            if self.read_bit()? {
                out |= 1;
            }
        }
        Ok(out)
    }

    fn ensure_terminated(&self) -> Result<()> {
        let consumed_bytes = self.bit_pos.div_ceil(8);
        if consumed_bytes != self.bytes.len() {
            return Err(TsinkError::DataCorruption(
                "f64 xor payload has trailing bytes".to_string(),
            ));
        }

        for bit in self.bit_pos..self.bytes.len().saturating_mul(8) {
            let byte_idx = bit / 8;
            let bit_in_byte = bit % 8;
            if ((self.bytes[byte_idx] >> (7 - bit_in_byte)) & 1) != 0 {
                return Err(TsinkError::DataCorruption(
                    "f64 xor payload has non-zero trailing padding bits".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_values_f64_xor, decode_values_f64_xor_range, encode_values_f64_xor, Encoder,
    };
    use crate::engine::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};
    use crate::{DataPoint, TsinkError, Value};

    fn chunk_points(ts: &[i64], values: Vec<Value>) -> Vec<ChunkPoint> {
        ts.iter()
            .copied()
            .zip(values)
            .map(|(ts, value)| ChunkPoint { ts, value })
            .collect()
    }

    #[test]
    fn chooses_fixed_step_for_regular_timestamps() {
        let timestamps = (0..64).map(|idx| 1_000 + idx * 10).collect::<Vec<_>>();
        let values = (0..64)
            .map(|idx| Value::I64(idx as i64))
            .collect::<Vec<_>>();
        let points = chunk_points(&timestamps, values);

        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        assert_eq!(encoded.ts_codec, TimestampCodecId::FixedStepRle);
    }

    #[test]
    fn chooses_constant_codec_for_constant_values() {
        let points = chunk_points(
            &[1, 2, 3, 4],
            vec![Value::I64(7), Value::I64(7), Value::I64(7), Value::I64(7)],
        );

        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        assert_eq!(encoded.value_codec, ValueCodecId::ConstantRle);
    }

    #[test]
    fn roundtrip_f64_values() {
        let points = vec![
            DataPoint::new(1, 1.25),
            DataPoint::new(3, 1.75),
            DataPoint::new(5, 2.5),
            DataPoint::new(8, -0.25),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn gorilla_f64_bitpacking_reduces_payload_for_smooth_series() {
        let mut bits = 1_000.0f64.to_bits();
        let values = (0..256)
            .map(|idx| {
                if idx > 0 {
                    bits = bits.wrapping_add(1);
                }
                Value::F64(f64::from_bits(bits))
            })
            .collect::<Vec<_>>();
        let timestamps = (0..256).map(|idx| idx as i64).collect::<Vec<_>>();
        let points = chunk_points(&timestamps, values);

        let payload = encode_values_f64_xor(&points).unwrap();
        assert!(
            payload.len() < points.len().saturating_mul(8),
            "expected Gorilla bitpacking to beat raw 8-byte/value XOR, got {} bytes for {} values",
            payload.len(),
            points.len()
        );

        let decoded = decode_values_f64_xor(&payload, points.len()).unwrap();
        let expected = points
            .iter()
            .map(|point| point.value.clone())
            .collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn gorilla_f64_range_decode_matches_full_decode_slice() {
        let values = (0..128)
            .map(|idx| Value::F64(1_000.0 + (idx as f64) * 0.25))
            .collect::<Vec<_>>();
        let timestamps = (0..128).map(|idx| idx as i64).collect::<Vec<_>>();
        let points = chunk_points(&timestamps, values);

        let payload = encode_values_f64_xor(&points).unwrap();
        let full = decode_values_f64_xor(&payload, points.len()).unwrap();
        let start = 11usize;
        let end = 97usize;
        let ranged = decode_values_f64_xor_range(&payload, points.len(), start, end).unwrap();

        assert_eq!(ranged, full[start..end].to_vec());
    }

    #[test]
    fn roundtrip_i64_values() {
        let points = vec![
            DataPoint::new(10, Value::I64(-5)),
            DataPoint::new(11, Value::I64(-4)),
            DataPoint::new(17, Value::I64(2)),
            DataPoint::new(18, Value::I64(2)),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn roundtrip_u64_values() {
        let points = vec![
            DataPoint::new(100, Value::U64(1000)),
            DataPoint::new(102, Value::U64(900)),
            DataPoint::new(103, Value::U64(1900)),
            DataPoint::new(108, Value::U64(1900)),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn roundtrip_u64_values_with_large_deltas() {
        let points = vec![
            DataPoint::new(1, Value::U64(0)),
            DataPoint::new(2, Value::U64(u64::MAX)),
            DataPoint::new(3, Value::U64(0)),
            DataPoint::new(4, Value::U64(1u64 << 63)),
            DataPoint::new(5, Value::U64(0)),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        assert_eq!(encoded.value_codec, ValueCodecId::DeltaBitpackU64);

        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn roundtrip_bool_values() {
        let points = vec![
            DataPoint::new(1, Value::Bool(true)),
            DataPoint::new(2, Value::Bool(false)),
            DataPoint::new(3, Value::Bool(true)),
            DataPoint::new(4, Value::Bool(false)),
            DataPoint::new(5, Value::Bool(true)),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn roundtrip_blob_values() {
        let points = vec![
            DataPoint::new(1, Value::Bytes(b"abc".to_vec())),
            DataPoint::new(2, Value::String("xyz".to_string())),
            DataPoint::new(3, Value::Bytes(vec![0, 1, 2, 3])),
            DataPoint::new(4, Value::String("longer-payload".to_string())),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        assert_eq!(encoded.value_codec, ValueCodecId::BytesDeltaBlock);

        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn rejects_timestamp_delta_overflow() {
        let points = vec![
            DataPoint::new(i64::MIN, Value::I64(1)),
            DataPoint::new(i64::MAX, Value::I64(2)),
        ];

        let err = Encoder::encode(&points).unwrap_err();
        assert!(matches!(err, TsinkError::Codec(_)));
    }

    #[test]
    fn falls_back_to_delta_varint_when_delta_of_delta_overflows() {
        let points = vec![
            DataPoint::new(0, Value::I64(1)),
            DataPoint::new(i64::MAX, Value::I64(2)),
            DataPoint::new(-1, Value::I64(3)),
        ];

        let encoded = Encoder::encode(&points).unwrap();
        assert_eq!(encoded.ts_codec, TimestampCodecId::DeltaVarint);

        let decoded = Encoder::decode(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn rejects_i64_delta_overflow() {
        let points = vec![
            DataPoint::new(1, Value::I64(i64::MIN)),
            DataPoint::new(2, Value::I64(i64::MAX)),
        ];

        let err = Encoder::encode(&points).unwrap_err();
        assert!(matches!(err, TsinkError::Codec(_)));
    }
}
