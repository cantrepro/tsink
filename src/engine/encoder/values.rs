use super::*;

pub(super) fn choose_best_value_codec<P: EncodablePoint>(
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
        ValueFamily::Blob | ValueFamily::Histogram => {
            candidates.push((
                ValueCodecId::BytesDeltaBlock,
                encode_values_blob_delta_block(points)?,
            ));
        }
    }

    choose_smallest(candidates)
}

pub(super) fn infer_value_family<P: EncodablePoint>(
    points: &[P],
    lane: ValueLane,
) -> Result<ValueFamily> {
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
        (Value::Histogram(_), ValueLane::Blob) => ValueFamily::Histogram,
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
                | (Value::Histogram(_), ValueFamily::Histogram)
        );

        if !ok {
            return Err(TsinkError::ValueTypeMismatch {
                expected: match first_family {
                    ValueFamily::F64 => "f64",
                    ValueFamily::I64 => "i64",
                    ValueFamily::U64 => "u64",
                    ValueFamily::Bool => "bool",
                    ValueFamily::Blob => "bytes/string",
                    ValueFamily::Histogram => "histogram",
                }
                .to_string(),
                actual: point.value().kind().to_string(),
            });
        }
    }

    Ok(first_family)
}

fn encode_values_constant_rle<P: EncodablePoint>(points: &[P]) -> Option<Vec<u8>> {
    let first = points.first()?.value();
    if points.iter().all(|point| point.value() == first) {
        encode_single_value(first).ok()
    } else {
        None
    }
}

pub(super) fn encode_values_f64_xor<P: EncodablePoint>(points: &[P]) -> Result<Vec<u8>> {
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
            Value::Histogram(histogram) => {
                out.push(7);
                let bytes = encode_histogram_payload(histogram)?;
                encode_uvarint(bytes.len() as u64, &mut out);
                out.extend_from_slice(&bytes);
            }
            other => {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: "bytes|string|histogram".to_string(),
                    actual: other.kind().to_string(),
                });
            }
        }
    }

    Ok(out)
}

pub(super) fn decode_values(
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

pub(super) fn decode_values_in_index_range(
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

pub(super) fn decode_values_f64_xor(payload: &[u8], point_count: usize) -> Result<Vec<Value>> {
    decode_values_f64_xor_in_range(payload, point_count, 0, point_count)
}

pub(super) fn decode_values_f64_xor_range(
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
            7 => Value::from(decode_histogram_payload(bytes)?),
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
                7 => {}
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
            7 => Value::from(decode_histogram_payload(bytes)?),
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
        Value::Histogram(histogram) => {
            out.push(7);
            let bytes = encode_histogram_payload(histogram)?;
            encode_uvarint(bytes.len() as u64, &mut out);
            out.extend_from_slice(&bytes);
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
            let value = f64::from_le_bytes(read_array::<8>(bytes, &mut pos)?);
            Value::F64(value)
        }
        2 => {
            let value = i64::from_le_bytes(read_array::<8>(bytes, &mut pos)?);
            Value::I64(value)
        }
        3 => {
            let value = u64::from_le_bytes(read_array::<8>(bytes, &mut pos)?);
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
        7 => {
            let len = decode_uvarint(bytes, &mut pos)? as usize;
            let payload = read_bytes(bytes, &mut pos, len)?;
            Value::from(decode_histogram_payload(payload)?)
        }
        _ => {
            return Err(TsinkError::DataCorruption(format!(
                "unknown constant value tag {tag}"
            )));
        }
    };

    Ok((value, pos))
}

fn encode_histogram_payload(histogram: &NativeHistogram) -> Result<Vec<u8>> {
    Ok(bincode::serialize(histogram)?)
}

fn decode_histogram_payload(bytes: &[u8]) -> Result<NativeHistogram> {
    Ok(bincode::deserialize(bytes)?)
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
