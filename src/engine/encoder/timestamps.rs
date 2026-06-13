use super::*;

pub(super) fn choose_best_timestamp_codec<P: EncodablePoint>(
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

pub(super) fn decode_timestamps(
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

pub(super) fn split_chunk_payload(payload: &[u8]) -> Result<(&[u8], &[u8])> {
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

    Ok((ts_payload, value_payload))
}

pub(super) fn build_timestamp_search_index(
    codec: TimestampCodecId,
    point_count: usize,
    ts_payload: &[u8],
) -> Result<TimestampSearchIndex> {
    match codec {
        TimestampCodecId::FixedStepRle => build_fixed_step_timestamp_search_index(ts_payload),
        TimestampCodecId::DeltaVarint => {
            build_delta_varint_timestamp_search_index(ts_payload, point_count)
        }
        TimestampCodecId::DeltaOfDeltaBitpack => {
            build_delta_of_delta_timestamp_search_index(ts_payload, point_count)
        }
    }
}

fn build_fixed_step_timestamp_search_index(payload: &[u8]) -> Result<TimestampSearchIndex> {
    if payload.len() != 16 {
        return Err(TsinkError::DataCorruption(
            "fixed-step payload must be exactly 16 bytes".to_string(),
        ));
    }

    let mut pos = 0usize;
    let first_ts = read_i64(payload, &mut pos)?;
    let step = read_i64(payload, &mut pos)?;
    Ok(TimestampSearchIndex::FixedStep { first_ts, step })
}

fn build_delta_varint_timestamp_search_index(
    payload: &[u8],
    point_count: usize,
) -> Result<TimestampSearchIndex> {
    if point_count == 0 {
        return Ok(TimestampSearchIndex::DeltaVarint {
            anchors: Vec::new(),
        });
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "delta-varint payload missing first timestamp".to_string(),
        ));
    }

    let mut pos = 0usize;
    let mut timestamp = read_i64(payload, &mut pos)?;
    let mut anchors = vec![DeltaVarintAnchor {
        point_idx: 0,
        timestamp,
        payload_offset: pos,
    }];

    for point_idx in 1..point_count {
        let delta = decode_svarint(payload, &mut pos)?;
        timestamp = timestamp.checked_add(delta).ok_or_else(|| {
            TsinkError::DataCorruption("delta-varint timestamp overflow while indexing".to_string())
        })?;
        if point_idx % TIMESTAMP_SEARCH_BLOCK_POINTS == 0 {
            anchors.push(DeltaVarintAnchor {
                point_idx,
                timestamp,
                payload_offset: pos,
            });
        }
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "delta-varint payload has trailing bytes".to_string(),
        ));
    }

    Ok(TimestampSearchIndex::DeltaVarint { anchors })
}

fn build_delta_of_delta_timestamp_search_index(
    payload: &[u8],
    point_count: usize,
) -> Result<TimestampSearchIndex> {
    if point_count == 0 {
        return Ok(TimestampSearchIndex::DeltaOfDelta {
            anchors: Vec::new(),
        });
    }
    if payload.len() < 8 {
        return Err(TsinkError::DataCorruption(
            "delta-of-delta payload missing first timestamp".to_string(),
        ));
    }

    let mut pos = 0usize;
    let first_ts = read_i64(payload, &mut pos)?;
    let mut anchors = vec![DeltaOfDeltaAnchor {
        point_idx: 0,
        timestamp: first_ts,
        prev_delta: 0,
        payload_offset: pos,
    }];

    if point_count == 1 {
        if pos != payload.len() {
            return Err(TsinkError::DataCorruption(
                "delta-of-delta payload has trailing bytes".to_string(),
            ));
        }
        return Ok(TimestampSearchIndex::DeltaOfDelta { anchors });
    }

    if payload.len().saturating_sub(pos) < 8 {
        return Err(TsinkError::DataCorruption(
            "delta-of-delta payload missing first delta".to_string(),
        ));
    }

    let mut point_idx = 1usize;
    let mut prev_delta = read_i64(payload, &mut pos)?;
    let mut timestamp = first_ts.checked_add(prev_delta).ok_or_else(|| {
        TsinkError::DataCorruption("delta-of-delta timestamp overflow while indexing".to_string())
    })?;

    loop {
        if point_idx.is_multiple_of(TIMESTAMP_SEARCH_BLOCK_POINTS) {
            anchors.push(DeltaOfDeltaAnchor {
                point_idx,
                timestamp,
                prev_delta,
                payload_offset: pos,
            });
        }
        if point_idx + 1 >= point_count {
            break;
        }

        let dod = decode_svarint(payload, &mut pos)?;
        let delta = prev_delta.checked_add(dod).ok_or_else(|| {
            TsinkError::DataCorruption("delta-of-delta delta overflow while indexing".to_string())
        })?;
        timestamp = timestamp.checked_add(delta).ok_or_else(|| {
            TsinkError::DataCorruption(
                "delta-of-delta timestamp overflow while indexing".to_string(),
            )
        })?;
        prev_delta = delta;
        point_idx = point_idx.saturating_add(1);
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "delta-of-delta payload has trailing bytes".to_string(),
        ));
    }

    Ok(TimestampSearchIndex::DeltaOfDelta { anchors })
}

pub(super) fn candidate_anchor_block_for_ceiling<I>(
    timestamps: I,
    ceiling_inclusive: i64,
) -> Option<usize>
where
    I: IntoIterator<Item = i64>,
{
    let mut candidate = None;
    for (idx, timestamp) in timestamps.into_iter().enumerate() {
        if timestamp > ceiling_inclusive {
            break;
        }
        candidate = Some(idx);
    }
    candidate
}

fn timestamp_block_bounds(point_count: usize, block_idx: usize) -> Result<(usize, usize)> {
    let start_idx = block_idx.saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS);
    if start_idx >= point_count {
        return Err(TsinkError::DataCorruption(format!(
            "timestamp search block {block_idx} starts beyond point count {point_count}"
        )));
    }
    let end_idx = point_count.min(start_idx.saturating_add(TIMESTAMP_SEARCH_BLOCK_POINTS));
    Ok((start_idx, end_idx))
}

pub(super) fn decode_fixed_step_timestamp_block(
    first_ts: i64,
    step: i64,
    point_count: usize,
    block_idx: usize,
) -> Result<Vec<i64>> {
    let (start_idx, end_idx) = timestamp_block_bounds(point_count, block_idx)?;
    let mut out = Vec::with_capacity(end_idx.saturating_sub(start_idx));
    for point_idx in start_idx..end_idx {
        let idx = i64::try_from(point_idx).map_err(|_| {
            TsinkError::DataCorruption("fixed-step payload index exceeds i64 range".to_string())
        })?;
        let offset = step.checked_mul(idx).ok_or_else(|| {
            TsinkError::DataCorruption("fixed-step timestamp overflow while indexing".to_string())
        })?;
        let timestamp = first_ts.checked_add(offset).ok_or_else(|| {
            TsinkError::DataCorruption("fixed-step timestamp overflow while indexing".to_string())
        })?;
        out.push(timestamp);
    }
    Ok(out)
}

pub(super) fn decode_delta_varint_timestamp_block(
    payload: &[u8],
    point_count: usize,
    anchors: &[DeltaVarintAnchor],
    block_idx: usize,
) -> Result<Vec<i64>> {
    let anchor = anchors.get(block_idx).copied().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "missing delta-varint timestamp anchor for block {block_idx}"
        ))
    })?;
    let end_idx = anchors
        .get(block_idx + 1)
        .map(|anchor| anchor.point_idx)
        .unwrap_or(point_count);

    let mut pos = anchor.payload_offset;
    let mut timestamp = anchor.timestamp;
    let mut out = Vec::with_capacity(end_idx.saturating_sub(anchor.point_idx));
    out.push(timestamp);

    for _ in (anchor.point_idx + 1)..end_idx {
        let delta = decode_svarint(payload, &mut pos)?;
        timestamp = timestamp.checked_add(delta).ok_or_else(|| {
            TsinkError::DataCorruption(
                "delta-varint timestamp overflow while scanning block".to_string(),
            )
        })?;
        out.push(timestamp);
    }

    Ok(out)
}

pub(super) fn decode_delta_of_delta_timestamp_block(
    payload: &[u8],
    point_count: usize,
    anchors: &[DeltaOfDeltaAnchor],
    block_idx: usize,
) -> Result<Vec<i64>> {
    let anchor = anchors.get(block_idx).copied().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "missing delta-of-delta timestamp anchor for block {block_idx}"
        ))
    })?;
    let end_idx = anchors
        .get(block_idx + 1)
        .map(|anchor| anchor.point_idx)
        .unwrap_or(point_count);

    let mut pos = anchor.payload_offset;
    let mut timestamp = anchor.timestamp;
    let mut out = Vec::with_capacity(end_idx.saturating_sub(anchor.point_idx));
    out.push(timestamp);

    if anchor.point_idx == 0 {
        if end_idx == 1 {
            return Ok(out);
        }

        let mut prev_delta = read_i64(payload, &mut pos)?;
        timestamp = timestamp.checked_add(prev_delta).ok_or_else(|| {
            TsinkError::DataCorruption(
                "delta-of-delta timestamp overflow while scanning block".to_string(),
            )
        })?;
        out.push(timestamp);

        let mut current_idx = 1usize;
        while current_idx + 1 < end_idx {
            let dod = decode_svarint(payload, &mut pos)?;
            let delta = prev_delta.checked_add(dod).ok_or_else(|| {
                TsinkError::DataCorruption(
                    "delta-of-delta delta overflow while scanning block".to_string(),
                )
            })?;
            timestamp = timestamp.checked_add(delta).ok_or_else(|| {
                TsinkError::DataCorruption(
                    "delta-of-delta timestamp overflow while scanning block".to_string(),
                )
            })?;
            out.push(timestamp);
            prev_delta = delta;
            current_idx = current_idx.saturating_add(1);
        }
        return Ok(out);
    }

    let mut prev_delta = anchor.prev_delta;
    let mut current_idx = anchor.point_idx;
    while current_idx + 1 < end_idx {
        let dod = decode_svarint(payload, &mut pos)?;
        let delta = prev_delta.checked_add(dod).ok_or_else(|| {
            TsinkError::DataCorruption(
                "delta-of-delta delta overflow while scanning block".to_string(),
            )
        })?;
        timestamp = timestamp.checked_add(delta).ok_or_else(|| {
            TsinkError::DataCorruption(
                "delta-of-delta timestamp overflow while scanning block".to_string(),
            )
        })?;
        out.push(timestamp);
        prev_delta = delta;
        current_idx = current_idx.saturating_add(1);
    }

    Ok(out)
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
