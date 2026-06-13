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
        let chunk_points = points
            .iter()
            .map(|point| ChunkPoint {
                ts: point.timestamp,
                value: point.value.clone(),
            })
            .collect::<Vec<_>>();

        let lane = Self::choose_lane(points);
        let encoded = Self::encode_chunk_points(&chunk_points, lane)?;
        Ok((encoded.ts_codec, encoded.value_codec))
    }

    pub fn encode(points: &[DataPoint]) -> Result<EncodedChunk> {
        if points.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot encode empty chunk".to_string(),
            ));
        }

        let lane = Self::choose_lane(points);
        let chunk_points = points
            .iter()
            .map(|point| ChunkPoint {
                ts: point.timestamp,
                value: point.value.clone(),
            })
            .collect::<Vec<_>>();

        Self::encode_chunk_points(&chunk_points, lane)
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

    pub fn validate_chunk_points(points: &[ChunkPoint], lane: ValueLane) -> Result<()> {
        infer_value_family(points, lane)?;
        Ok(())
    }

    pub fn decode_chunk_points(encoded: &EncodedChunk) -> Result<Vec<ChunkPoint>> {
        if encoded.point_count == 0 {
            return Ok(Vec::new());
        }

        let mut pos = 0usize;
        let ts_len = read_u32(&encoded.payload, &mut pos)? as usize;
        let ts_payload = read_bytes(&encoded.payload, &mut pos, ts_len)?;
        let value_len = read_u32(&encoded.payload, &mut pos)? as usize;
        let value_payload = read_bytes(&encoded.payload, &mut pos, value_len)?;

        if pos != encoded.payload.len() {
            return Err(TsinkError::DataCorruption(
                "encoded chunk payload has trailing bytes".to_string(),
            ));
        }

        let timestamps = decode_timestamps(encoded.ts_codec, ts_payload, encoded.point_count)?;
        let values = decode_values(
            encoded.value_codec,
            encoded.lane,
            value_payload,
            encoded.point_count,
        )?;

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
}

fn choose_best_timestamp_codec(points: &[ChunkPoint]) -> Result<(TimestampCodecId, Vec<u8>)> {
    let mut candidates = Vec::with_capacity(3);

    if let Some(payload) = encode_timestamps_fixed_step_rle(points) {
        candidates.push((TimestampCodecId::FixedStepRle, payload));
    }

    candidates.push((
        TimestampCodecId::DeltaOfDeltaBitpack,
        encode_timestamps_delta_of_delta(points),
    ));
    candidates.push((
        TimestampCodecId::DeltaVarint,
        encode_timestamps_delta_varint(points),
    ));

    choose_smallest(candidates)
}

fn choose_best_value_codec(
    points: &[ChunkPoint],
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

fn infer_value_family(points: &[ChunkPoint], lane: ValueLane) -> Result<ValueFamily> {
    let Some(first) = points.first() else {
        return Err(TsinkError::Codec(
            "cannot infer value family from empty chunk".to_string(),
        ));
    };

    let first_family = match (&first.value, lane) {
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
            (&point.value, first_family),
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
                actual: point.value.kind().to_string(),
            });
        }
    }

    Ok(first_family)
}

fn encode_timestamps_fixed_step_rle(points: &[ChunkPoint]) -> Option<Vec<u8>> {
    let first_ts = points.first()?.ts;
    let step = if points.len() > 1 {
        points[1].ts.saturating_sub(points[0].ts)
    } else {
        0
    };

    if points
        .windows(2)
        .all(|window| window[1].ts.saturating_sub(window[0].ts) == step)
    {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&first_ts.to_le_bytes());
        out.extend_from_slice(&step.to_le_bytes());
        Some(out)
    } else {
        None
    }
}

fn encode_timestamps_delta_of_delta(points: &[ChunkPoint]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&points[0].ts.to_le_bytes());

    if points.len() == 1 {
        return out;
    }

    let mut prev_delta = points[1].ts.saturating_sub(points[0].ts);
    out.extend_from_slice(&prev_delta.to_le_bytes());

    for window in points.windows(2).skip(1) {
        let delta = window[1].ts.saturating_sub(window[0].ts);
        let dod = delta.saturating_sub(prev_delta);
        encode_svarint(dod, &mut out);
        prev_delta = delta;
    }

    out
}

fn encode_timestamps_delta_varint(points: &[ChunkPoint]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&points[0].ts.to_le_bytes());

    for window in points.windows(2) {
        let delta = window[1].ts.saturating_sub(window[0].ts);
        encode_svarint(delta, &mut out);
    }

    out
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
        out.push(first_ts.saturating_add(step.saturating_mul(i as i64)));
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
    out.push(first_ts.saturating_add(prev_delta));

    while out.len() < point_count {
        let dod = decode_svarint(payload, &mut pos)?;
        let delta = prev_delta.saturating_add(dod);
        let next = out
            .last()
            .copied()
            .unwrap_or(first_ts)
            .saturating_add(delta);
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
            .saturating_add(delta);
        out.push(next);
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "delta-varint payload has trailing bytes".to_string(),
        ));
    }

    Ok(out)
}

fn encode_values_constant_rle(points: &[ChunkPoint]) -> Option<Vec<u8>> {
    let first = points.first()?.value.clone();
    if points.iter().all(|point| point.value == first) {
        encode_single_value(&first).ok()
    } else {
        None
    }
}

fn encode_values_f64_xor(points: &[ChunkPoint]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(points.len() * 8);
    let mut prev = match points.first() {
        Some(ChunkPoint {
            value: Value::F64(v),
            ..
        }) => v.to_bits(),
        Some(point) => {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "f64".to_string(),
                actual: point.value.kind().to_string(),
            });
        }
        None => return Ok(out),
    };

    out.extend_from_slice(&prev.to_le_bytes());

    for point in points.iter().skip(1) {
        let Value::F64(v) = &point.value else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "f64".to_string(),
                actual: point.value.kind().to_string(),
            });
        };

        let bits = v.to_bits();
        let xor = bits ^ prev;
        out.extend_from_slice(&xor.to_le_bytes());
        prev = bits;
    }

    Ok(out)
}

fn encode_values_i64_delta(points: &[ChunkPoint]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let first = match points.first() {
        Some(ChunkPoint {
            value: Value::I64(v),
            ..
        }) => *v,
        Some(point) => {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "i64".to_string(),
                actual: point.value.kind().to_string(),
            });
        }
        None => return Ok(out),
    };

    out.extend_from_slice(&first.to_le_bytes());
    let mut prev = first;

    for point in points.iter().skip(1) {
        let Value::I64(v) = point.value else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "i64".to_string(),
                actual: point.value.kind().to_string(),
            });
        };

        let delta = v.saturating_sub(prev);
        encode_svarint(delta, &mut out);
        prev = v;
    }

    Ok(out)
}

fn encode_values_u64_delta(points: &[ChunkPoint]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let first = match points.first() {
        Some(ChunkPoint {
            value: Value::U64(v),
            ..
        }) => *v,
        Some(point) => {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "u64".to_string(),
                actual: point.value.kind().to_string(),
            });
        }
        None => return Ok(out),
    };

    out.extend_from_slice(&first.to_le_bytes());
    let mut prev = first;

    for point in points.iter().skip(1) {
        let Value::U64(v) = point.value else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "u64".to_string(),
                actual: point.value.kind().to_string(),
            });
        };

        let delta_i128 = (v as i128) - (prev as i128);
        let delta = i64::try_from(delta_i128).map_err(|_| {
            TsinkError::Codec("u64 delta exceeds i64 range for delta codec".to_string())
        })?;

        encode_svarint(delta, &mut out);
        prev = v;
    }

    Ok(out)
}

fn encode_values_bool_bitpack(points: &[ChunkPoint]) -> Result<Vec<u8>> {
    let mut out = vec![0u8; points.len().div_ceil(8)];

    for (idx, point) in points.iter().enumerate() {
        let Value::Bool(v) = point.value else {
            return Err(TsinkError::ValueTypeMismatch {
                expected: "bool".to_string(),
                actual: point.value.kind().to_string(),
            });
        };

        if v {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            out[byte_idx] |= 1 << bit_idx;
        }
    }

    Ok(out)
}

fn encode_values_blob_delta_block(points: &[ChunkPoint]) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    for point in points {
        match &point.value {
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

fn decode_values_constant_rle(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    let (value, used) = decode_single_value(payload)?;
    if used != payload.len() {
        return Err(TsinkError::DataCorruption(
            "constant-rle payload has trailing bytes".to_string(),
        ));
    }

    Ok(std::iter::repeat_n(value, point_count).collect())
}

fn decode_values_f64_xor(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    if point_count == 0 {
        return Ok(Vec::new());
    }

    let expected_len = point_count.saturating_mul(8);
    if payload.len() != expected_len {
        return Err(TsinkError::DataCorruption(format!(
            "f64 xor payload length mismatch: expected {expected_len}, got {}",
            payload.len()
        )));
    }

    let mut out = Vec::with_capacity(point_count);
    let mut prev = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    out.push(Value::F64(f64::from_bits(prev)));

    for idx in 1..point_count {
        let start = idx * 8;
        let end = start + 8;
        let xor = u64::from_le_bytes(payload[start..end].try_into().unwrap_or([0; 8]));
        let bits = prev ^ xor;
        out.push(Value::F64(f64::from_bits(bits)));
        prev = bits;
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
        let next = prev.saturating_add(delta);
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
        let next_i128 = (prev as i128) + (delta as i128);
        let next = u64::try_from(next_i128).map_err(|_| {
            TsinkError::DataCorruption("u64 delta decode produced negative value".to_string())
        })?;

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

#[cfg(test)]
mod tests {
    use super::Encoder;
    use crate::engine::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};
    use crate::{DataPoint, Value};

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
}
