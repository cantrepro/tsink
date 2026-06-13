use super::*;

impl FramedWal {
    pub fn estimate_series_definition_frame_bytes(
        definition: &SeriesDefinitionFrame,
    ) -> Result<u64> {
        let payload = Self::encode_series_definition_frame_payload(definition)?;
        Ok(Self::frame_size_bytes_for_payload_len(payload.len()))
    }

    pub fn estimate_samples_frame_bytes(batches: &[SamplesBatchFrame]) -> Result<u64> {
        if batches.is_empty() {
            return Ok(0);
        }
        let payload = Self::encode_samples_frame_payload(batches)?;
        Ok(Self::frame_size_bytes_for_payload_len(payload.len()))
    }

    pub fn frame_size_bytes_for_payload_len(payload_len: usize) -> u64 {
        FRAME_HEADER_LEN as u64 + payload_len as u64
    }

    pub fn encode_series_definition_frame_payload(
        definition: &SeriesDefinitionFrame,
    ) -> Result<Vec<u8>> {
        encode_series_definition(definition)
    }

    pub fn encode_samples_frame_payload(batches: &[SamplesBatchFrame]) -> Result<Vec<u8>> {
        encode_samples_payload(batches)
    }
}

pub(super) fn split_encoded_payload(payload: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut pos = 0usize;
    let ts_len = read_u32(payload, &mut pos)? as usize;
    let ts_payload = read_bytes(payload, &mut pos, ts_len)?.to_vec();
    let value_len = read_u32(payload, &mut pos)? as usize;
    let value_payload = read_bytes(payload, &mut pos, value_len)?.to_vec();

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "encoded chunk payload has trailing bytes".to_string(),
        ));
    }

    Ok((ts_payload, value_payload))
}

pub(super) fn merge_encoded_payload(ts_payload: &[u8], value_payload: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + ts_payload.len() + value_payload.len());
    append_u32(&mut payload, ts_payload.len() as u32);
    payload.extend_from_slice(ts_payload);
    append_u32(&mut payload, value_payload.len() as u32);
    payload.extend_from_slice(value_payload);
    payload
}

pub(super) fn encode_series_definition(definition: &SeriesDefinitionFrame) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    append_u64(&mut payload, definition.series_id);

    write_string_u16(&mut payload, &definition.metric)?;

    let labels_len = u16::try_from(definition.labels.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("too many labels in series definition".to_string())
    })?;
    append_u16(&mut payload, labels_len);

    for label in &definition.labels {
        write_string_u16(&mut payload, &label.name)?;
        write_string_u16(&mut payload, &label.value)?;
    }

    Ok(payload)
}

pub(super) fn decode_series_definition(payload: &[u8]) -> Result<SeriesDefinitionFrame> {
    let mut pos = 0usize;
    let series_id = read_u64(payload, &mut pos)?;
    let metric = read_string_u16(payload, &mut pos)?;

    let labels_len = read_u16(payload, &mut pos)? as usize;
    let mut labels = Vec::with_capacity(labels_len);
    for _ in 0..labels_len {
        let name = read_string_u16(payload, &mut pos)?;
        let value = read_string_u16(payload, &mut pos)?;
        labels.push(Label::new(name, value));
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "series-definition payload has trailing bytes".to_string(),
        ));
    }

    Ok(SeriesDefinitionFrame {
        series_id,
        metric,
        labels,
    })
}

pub(super) fn encode_samples_payload(batches: &[SamplesBatchFrame]) -> Result<Vec<u8>> {
    let mut payload = Vec::new();

    let count = u16::try_from(batches.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("too many batches in WAL frame".to_string())
    })?;
    append_u16(&mut payload, count);

    for batch in batches {
        append_u64(&mut payload, batch.series_id);
        append_u8(&mut payload, batch.lane as u8);
        append_u8(&mut payload, batch.ts_codec as u8);
        append_u8(&mut payload, batch.value_codec as u8);
        append_u8(&mut payload, 0u8);
        append_u16(&mut payload, batch.point_count);
        append_i64(&mut payload, batch.base_ts);

        let ts_len = u32::try_from(batch.ts_payload.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("WAL ts payload exceeds u32".to_string())
        })?;
        let value_len = u32::try_from(batch.value_payload.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("WAL value payload exceeds u32".to_string())
        })?;

        append_u32(&mut payload, ts_len);
        append_u32(&mut payload, value_len);
        payload.extend_from_slice(&batch.ts_payload);
        payload.extend_from_slice(&batch.value_payload);
    }

    Ok(payload)
}

pub(super) fn decode_samples_payload(payload: &[u8]) -> Result<Vec<SamplesBatchFrame>> {
    let mut pos = 0usize;
    let count = read_u16(payload, &mut pos)? as usize;
    let mut batches = Vec::with_capacity(count);

    for _ in 0..count {
        let series_id = read_u64(payload, &mut pos)?;

        let lane = decode_lane(read_u8(payload, &mut pos)?)?;
        let ts_codec = decode_ts_codec(read_u8(payload, &mut pos)?)?;
        let value_codec = decode_value_codec(read_u8(payload, &mut pos)?)?;
        let _reserved = read_u8(payload, &mut pos)?;

        let point_count = read_u16(payload, &mut pos)?;
        let base_ts = read_i64(payload, &mut pos)?;
        let ts_len = read_u32(payload, &mut pos)? as usize;
        let value_len = read_u32(payload, &mut pos)? as usize;

        let ts_payload = read_bytes(payload, &mut pos, ts_len)?.to_vec();
        let value_payload = read_bytes(payload, &mut pos, value_len)?.to_vec();

        batches.push(SamplesBatchFrame {
            series_id,
            lane,
            ts_codec,
            value_codec,
            point_count,
            base_ts,
            ts_payload,
            value_payload,
        });
    }

    if pos != payload.len() {
        return Err(TsinkError::DataCorruption(
            "samples payload has trailing bytes".to_string(),
        ));
    }

    Ok(batches)
}

pub(super) fn decode_samples_series_ids(payload: &[u8]) -> Result<BTreeSet<SeriesId>> {
    Ok(decode_samples_payload(payload)?
        .into_iter()
        .map(|batch| batch.series_id)
        .collect())
}

fn write_string_u16(out: &mut Vec<u8>, text: &str) -> Result<()> {
    let bytes = text.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("string too long for u16 encoding".to_string())
    })?;
    append_u16(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_string_u16(payload: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_u16(payload, pos)? as usize;
    let bytes = read_bytes(payload, pos, len)?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

fn decode_lane(raw: u8) -> Result<ValueLane> {
    match raw {
        0 => Ok(ValueLane::Numeric),
        1 => Ok(ValueLane::Blob),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid value lane {raw} in WAL"
        ))),
    }
}

fn decode_ts_codec(raw: u8) -> Result<TimestampCodecId> {
    match raw {
        1 => Ok(TimestampCodecId::FixedStepRle),
        2 => Ok(TimestampCodecId::DeltaOfDeltaBitpack),
        3 => Ok(TimestampCodecId::DeltaVarint),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid timestamp codec id {raw} in WAL"
        ))),
    }
}

fn decode_value_codec(raw: u8) -> Result<ValueCodecId> {
    match raw {
        1 => Ok(ValueCodecId::GorillaXorF64),
        2 => Ok(ValueCodecId::ZigZagDeltaBitpackI64),
        3 => Ok(ValueCodecId::DeltaBitpackU64),
        4 => Ok(ValueCodecId::ConstantRle),
        5 => Ok(ValueCodecId::BoolBitpack),
        6 => Ok(ValueCodecId::BytesDeltaBlock),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid value codec id {raw} in WAL"
        ))),
    }
}
