use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;
use tracing::warn;

use crate::engine::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};
use crate::engine::encoder::{EncodedChunk, Encoder};
use crate::engine::segment::WalHighWatermark;
use crate::engine::series_registry::SeriesId;
use crate::wal::WalSyncMode;
use crate::{Label, Result, TsinkError};

const WAL_FILE_NAME: &str = "wal.log";
const DEFAULT_WAL_BUFFER_SIZE: usize = 4096;
const FRAME_MAGIC: [u8; 4] = *b"TSFR";
const FRAME_HEADER_LEN: usize = 24;
const MAX_FRAME_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

const FRAME_TYPE_SERIES_DEF: u8 = 1;
const FRAME_TYPE_SAMPLES: u8 = 2;

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

pub struct FramedWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    next_seq: AtomicU64,
    sync_mode: WalSyncMode,
    last_sync: Mutex<Instant>,
}

impl FramedWal {
    pub fn open(dir: impl AsRef<Path>, sync_mode: WalSyncMode) -> Result<Self> {
        Self::open_with_buffer_size(dir, sync_mode, DEFAULT_WAL_BUFFER_SIZE)
    }

    pub fn open_with_buffer_size(
        dir: impl AsRef<Path>,
        sync_mode: WalSyncMode,
        buffer_size: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let path = dir.join(WAL_FILE_NAME);
        if !path.exists() {
            File::create(&path)?;
        }

        let last_seq = scan_last_seq(&path)?;

        let writer_file = OpenOptions::new().create(true).append(true).open(&path)?;
        let writer = BufWriter::with_capacity(buffer_size.max(1), writer_file);

        Ok(Self {
            path,
            writer: Mutex::new(writer),
            next_seq: AtomicU64::new(last_seq.saturating_add(1)),
            sync_mode,
            last_sync: Mutex::new(Instant::now()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append_series_definition(&self, definition: &SeriesDefinitionFrame) -> Result<()> {
        let payload = encode_series_definition(definition)?;
        self.append_frame(FRAME_TYPE_SERIES_DEF, &payload)
    }

    pub fn append_samples(&self, batches: &[SamplesBatchFrame]) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let payload = encode_samples_payload(batches)?;
        self.append_frame(FRAME_TYPE_SAMPLES, &payload)
    }

    pub fn replay_frames(&self) -> Result<Vec<ReplayFrame>> {
        self.replay_frames_after(WalHighWatermark::default())
    }

    pub fn replay_frames_after(
        &self,
        replay_highwater: WalHighWatermark,
    ) -> Result<Vec<ReplayFrame>> {
        replay_from_path(&self.path, replay_highwater)
    }

    pub fn current_highwater(&self) -> WalHighWatermark {
        WalHighWatermark {
            segment: 0,
            frame: self.next_seq.load(Ordering::SeqCst).saturating_sub(1),
        }
    }

    pub fn reset(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        writer.get_mut().set_len(0)?;
        writer.get_mut().sync_data()?;
        *self.last_sync.lock() = Instant::now();
        Ok(())
    }

    pub fn ensure_min_next_seq(&self, min_next_seq: u64) {
        let mut current = self.next_seq.load(Ordering::SeqCst);
        while current < min_next_seq {
            match self.next_seq.compare_exchange_weak(
                current,
                min_next_seq,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn append_frame(&self, frame_type: u8, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "WAL frame payload too large: {} bytes",
                payload.len()
            )));
        }

        let payload_crc32 = checksum32(payload);

        let mut writer = self.writer.lock();
        let frame_start_len = writer.get_ref().metadata()?.len();
        let frame_seq = self.next_seq.load(Ordering::SeqCst);

        let mut header = Vec::with_capacity(FRAME_HEADER_LEN);
        header.extend_from_slice(&FRAME_MAGIC);
        header.push(frame_type);
        header.extend_from_slice(&[0u8; 3]);
        header.extend_from_slice(&frame_seq.to_le_bytes());
        header.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        header.extend_from_slice(&payload_crc32.to_le_bytes());

        let write_result = writer
            .write_all(&header)
            .and_then(|_| writer.write_all(payload))
            .and_then(|_| writer.flush());
        if let Err(write_err) = write_result {
            if let Err(recovery_err) = self.rollback_partial_append(&mut writer, frame_start_len) {
                return Err(TsinkError::Wal {
                    operation: "append frame rollback".to_string(),
                    details: format!("append failed: {write_err}; rollback failed: {recovery_err}"),
                });
            }

            return Err(write_err.into());
        }

        match self.sync_mode {
            WalSyncMode::PerAppend => {
                writer.get_mut().sync_data()?;
                *self.last_sync.lock() = Instant::now();
            }
            WalSyncMode::Periodic(interval) => {
                let mut last_sync = self.last_sync.lock();
                if last_sync.elapsed() >= interval {
                    writer.get_mut().sync_data()?;
                    *last_sync = Instant::now();
                }
            }
        }

        self.next_seq
            .store(frame_seq.saturating_add(1), Ordering::SeqCst);

        Ok(())
    }

    fn rollback_partial_append(
        &self,
        writer: &mut BufWriter<File>,
        frame_start_len: u64,
    ) -> Result<()> {
        writer.get_mut().set_len(frame_start_len)?;
        writer.get_mut().sync_data()?;

        let replacement = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let capacity = writer.capacity();
        let old_writer = std::mem::replace(writer, BufWriter::with_capacity(capacity, replacement));
        let _ = old_writer.into_parts();

        Ok(())
    }
}

fn split_encoded_payload(payload: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
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

fn merge_encoded_payload(ts_payload: &[u8], value_payload: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + ts_payload.len() + value_payload.len());
    payload.extend_from_slice(&(ts_payload.len() as u32).to_le_bytes());
    payload.extend_from_slice(ts_payload);
    payload.extend_from_slice(&(value_payload.len() as u32).to_le_bytes());
    payload.extend_from_slice(value_payload);
    payload
}

fn replay_from_path(path: &Path, replay_highwater: WalHighWatermark) -> Result<Vec<ReplayFrame>> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut reader = BufReader::new(file);
    let mut out = Vec::new();

    loop {
        let mut header = [0u8; FRAME_HEADER_LEN];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        if header[0..4] != FRAME_MAGIC {
            warn!("Stopping WAL replay at frame with magic mismatch");
            break;
        }

        let frame_type = header[4];
        let frame_seq = u64::from_le_bytes(header[8..16].try_into().unwrap_or([0u8; 8]));
        let payload_len =
            u32::from_le_bytes(header[16..20].try_into().unwrap_or([0u8; 4])) as usize;
        let expected_crc32 = u32::from_le_bytes(header[20..24].try_into().unwrap_or([0u8; 4]));

        if payload_len > MAX_FRAME_PAYLOAD_BYTES {
            warn!(
                payload_len,
                "Stopping WAL replay due to oversized frame payload"
            );
            break;
        }

        let mut payload = vec![0u8; payload_len];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                warn!("Stopping WAL replay at truncated frame");
                break;
            }
            Err(e) => return Err(e.into()),
        }

        if checksum32(&payload) != expected_crc32 {
            warn!("Stopping WAL replay at frame with checksum mismatch");
            break;
        }

        let frame_pos = WalHighWatermark {
            segment: 0,
            frame: frame_seq,
        };
        if frame_pos <= replay_highwater {
            continue;
        }

        let frame = match frame_type {
            FRAME_TYPE_SERIES_DEF => {
                ReplayFrame::SeriesDefinition(decode_series_definition(&payload)?)
            }
            FRAME_TYPE_SAMPLES => ReplayFrame::Samples(decode_samples_payload(&payload)?),
            other => {
                warn!(
                    frame_type = other,
                    "Stopping WAL replay at unknown frame type"
                );
                break;
            }
        };

        out.push(frame);
    }

    Ok(out)
}

fn scan_last_seq(path: &Path) -> Result<u64> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut reader = BufReader::new(file);
    let mut last_seq = 0u64;

    loop {
        let mut header = [0u8; FRAME_HEADER_LEN];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        if header[0..4] != FRAME_MAGIC {
            break;
        }

        let frame_seq = u64::from_le_bytes(header[8..16].try_into().unwrap_or([0u8; 8]));
        let payload_len =
            u32::from_le_bytes(header[16..20].try_into().unwrap_or([0u8; 4])) as usize;
        let expected_crc32 = u32::from_le_bytes(header[20..24].try_into().unwrap_or([0u8; 4]));

        if payload_len > MAX_FRAME_PAYLOAD_BYTES {
            break;
        }

        let mut payload = vec![0u8; payload_len];
        if reader.read_exact(&mut payload).is_err() {
            break;
        }

        if checksum32(&payload) != expected_crc32 {
            break;
        }

        last_seq = last_seq.max(frame_seq);
    }

    Ok(last_seq)
}

fn encode_series_definition(definition: &SeriesDefinitionFrame) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&definition.series_id.to_le_bytes());

    write_string_u16(&mut payload, &definition.metric)?;

    let labels_len = u16::try_from(definition.labels.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("too many labels in series definition".to_string())
    })?;
    payload.extend_from_slice(&labels_len.to_le_bytes());

    for label in &definition.labels {
        write_string_u16(&mut payload, &label.name)?;
        write_string_u16(&mut payload, &label.value)?;
    }

    Ok(payload)
}

fn decode_series_definition(payload: &[u8]) -> Result<SeriesDefinitionFrame> {
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

fn encode_samples_payload(batches: &[SamplesBatchFrame]) -> Result<Vec<u8>> {
    let mut payload = Vec::new();

    let count = u16::try_from(batches.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("too many batches in WAL frame".to_string())
    })?;
    payload.extend_from_slice(&count.to_le_bytes());

    for batch in batches {
        payload.extend_from_slice(&batch.series_id.to_le_bytes());
        payload.push(batch.lane as u8);
        payload.push(batch.ts_codec as u8);
        payload.push(batch.value_codec as u8);
        payload.push(0u8);
        payload.extend_from_slice(&batch.point_count.to_le_bytes());
        payload.extend_from_slice(&batch.base_ts.to_le_bytes());

        let ts_len = u32::try_from(batch.ts_payload.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("WAL ts payload exceeds u32".to_string())
        })?;
        let value_len = u32::try_from(batch.value_payload.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("WAL value payload exceeds u32".to_string())
        })?;

        payload.extend_from_slice(&ts_len.to_le_bytes());
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(&batch.ts_payload);
        payload.extend_from_slice(&batch.value_payload);
    }

    Ok(payload)
}

fn decode_samples_payload(payload: &[u8]) -> Result<Vec<SamplesBatchFrame>> {
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

fn write_string_u16(out: &mut Vec<u8>, text: &str) -> Result<()> {
    let bytes = text.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("string too long for u16 encoding".to_string())
    })?;
    out.extend_from_slice(&len.to_le_bytes());
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

fn read_u8(payload: &[u8], pos: &mut usize) -> Result<u8> {
    let byte = *payload.get(*pos).ok_or_else(|| {
        TsinkError::DataCorruption("payload truncated while reading u8".to_string())
    })?;
    *pos += 1;
    Ok(byte)
}

fn read_u16(payload: &[u8], pos: &mut usize) -> Result<u16> {
    let bytes = read_bytes(payload, pos, 2)?;
    let mut raw = [0u8; 2];
    raw.copy_from_slice(bytes);
    Ok(u16::from_le_bytes(raw))
}

fn read_u32(payload: &[u8], pos: &mut usize) -> Result<u32> {
    let bytes = read_bytes(payload, pos, 4)?;
    let mut raw = [0u8; 4];
    raw.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(payload: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes = read_bytes(payload, pos, 8)?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(raw))
}

fn read_i64(payload: &[u8], pos: &mut usize) -> Result<i64> {
    let bytes = read_bytes(payload, pos, 8)?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    Ok(i64::from_le_bytes(raw))
}

fn read_bytes<'a>(payload: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = pos.saturating_add(len);
    if end > payload.len() {
        return Err(TsinkError::DataCorruption(format!(
            "payload truncated: need {} bytes, have {}",
            len,
            payload.len().saturating_sub(*pos)
        )));
    }

    let bytes = &payload[*pos..end];
    *pos = end;
    Ok(bytes)
}

fn checksum32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use tempfile::TempDir;

    use super::{
        checksum32, encode_series_definition, replay_from_path, scan_last_seq, FramedWal,
        ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame, FRAME_HEADER_LEN, FRAME_MAGIC,
        FRAME_TYPE_SERIES_DEF, WAL_FILE_NAME,
    };
    use crate::engine::chunk::{ChunkPoint, ValueLane};
    use crate::engine::segment::WalHighWatermark;
    use crate::{wal::WalSyncMode, Label, Value};

    #[test]
    fn samples_batch_roundtrip_preserves_points() {
        let points = vec![
            ChunkPoint {
                ts: 1,
                value: Value::I64(10),
            },
            ChunkPoint {
                ts: 5,
                value: Value::I64(11),
            },
            ChunkPoint {
                ts: 8,
                value: Value::I64(20),
            },
        ];

        let batch = SamplesBatchFrame::from_points(42, ValueLane::Numeric, &points).unwrap();
        let decoded = batch.decode_points().unwrap();
        assert_eq!(decoded.len(), points.len());
        assert_eq!(decoded[0].ts, 1);
        assert_eq!(decoded[2].value, Value::I64(20));
    }

    #[test]
    fn wal_roundtrip_replays_series_defs_and_samples() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        let batch = SamplesBatchFrame::from_points(
            7,
            ValueLane::Numeric,
            &[
                ChunkPoint {
                    ts: 10,
                    value: Value::F64(1.0),
                },
                ChunkPoint {
                    ts: 20,
                    value: Value::F64(2.0),
                },
            ],
        )
        .unwrap();

        wal.append_samples(&[batch]).unwrap();

        let replay = wal.replay_frames().unwrap();
        assert_eq!(replay.len(), 2);

        assert!(matches!(replay[0], ReplayFrame::SeriesDefinition(_)));
        assert!(matches!(replay[1], ReplayFrame::Samples(_)));
    }

    #[test]
    fn replay_frames_after_skips_checkpointed_frames() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        let batch = SamplesBatchFrame::from_points(
            7,
            ValueLane::Numeric,
            &[
                ChunkPoint {
                    ts: 10,
                    value: Value::F64(1.0),
                },
                ChunkPoint {
                    ts: 20,
                    value: Value::F64(2.0),
                },
            ],
        )
        .unwrap();
        wal.append_samples(&[batch]).unwrap();

        let replay = wal
            .replay_frames_after(WalHighWatermark {
                segment: 0,
                frame: 1,
            })
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert!(matches!(replay[0], ReplayFrame::Samples(_)));
        assert_eq!(
            wal.current_highwater(),
            WalHighWatermark {
                segment: 0,
                frame: 2
            }
        );
    }

    #[test]
    fn wal_reset_clears_existing_frames() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        assert_eq!(wal.replay_frames().unwrap().len(), 1);
        wal.reset().unwrap();
        assert!(wal.replay_frames().unwrap().is_empty());
    }

    #[test]
    fn wal_reset_preserves_monotonic_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();
        assert_eq!(wal.current_highwater().frame, 1);

        wal.reset().unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 8,
            metric: "mem".to_string(),
            labels: vec![Label::new("host", "b")],
        })
        .unwrap();

        assert_eq!(wal.current_highwater().frame, 2);
    }

    #[test]
    fn ensure_min_next_seq_sets_sequence_floor() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
        wal.ensure_min_next_seq(5);

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        assert_eq!(wal.current_highwater().frame, 5);
    }

    #[test]
    fn wal_open_with_buffer_size_configures_writer_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open_with_buffer_size(temp_dir.path(), WalSyncMode::PerAppend, 128).unwrap();
        assert_eq!(wal.writer.lock().capacity(), 128);

        let wal_zero =
            FramedWal::open_with_buffer_size(temp_dir.path(), WalSyncMode::PerAppend, 0).unwrap();
        assert_eq!(wal_zero.writer.lock().capacity(), 1);
    }

    #[test]
    fn replay_stops_at_truncated_tail_frame() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "m".to_string(),
            labels: vec![],
        })
        .unwrap();

        {
            let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
            file.write_all(b"W2FR\x02\x00\x00\x00\x00").unwrap();
            file.flush().unwrap();
        }

        let replay = replay_from_path(wal.path(), WalHighWatermark::default()).unwrap();
        assert_eq!(replay.len(), 1);
        assert!(matches!(replay[0], ReplayFrame::SeriesDefinition(_)));
    }

    #[test]
    fn replay_stops_at_frame_with_magic_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "m".to_string(),
            labels: vec![],
        })
        .unwrap();

        {
            let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
            let mut header = [0u8; FRAME_HEADER_LEN];
            header[0..4].copy_from_slice(b"BAD!");
            file.write_all(&header).unwrap();
            file.flush().unwrap();
        }

        let replay = replay_from_path(wal.path(), WalHighWatermark::default()).unwrap();
        assert_eq!(replay.len(), 1);
        assert!(matches!(replay[0], ReplayFrame::SeriesDefinition(_)));
    }

    #[test]
    fn scan_last_seq_uses_max_sequence_when_frames_are_out_of_order() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join(WAL_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&wal_path)
            .unwrap();

        let payload = encode_series_definition(&SeriesDefinitionFrame {
            series_id: 42,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        let write_frame = |file: &mut std::fs::File, seq: u64, payload: &[u8]| {
            let mut header = Vec::with_capacity(24);
            header.extend_from_slice(&FRAME_MAGIC);
            header.push(FRAME_TYPE_SERIES_DEF);
            header.extend_from_slice(&[0u8; 3]);
            header.extend_from_slice(&seq.to_le_bytes());
            header.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            header.extend_from_slice(&checksum32(payload).to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(payload).unwrap();
        };

        write_frame(&mut file, 2, &payload);
        write_frame(&mut file, 1, &payload);
        file.flush().unwrap();

        assert_eq!(scan_last_seq(&wal_path).unwrap(), 2);
    }

    #[test]
    fn scan_last_seq_stops_at_frame_with_checksum_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join(WAL_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&wal_path)
            .unwrap();

        let payload = encode_series_definition(&SeriesDefinitionFrame {
            series_id: 42,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        let write_frame = |file: &mut std::fs::File, seq: u64, payload: &[u8], crc32: u32| {
            let mut header = Vec::with_capacity(FRAME_HEADER_LEN);
            header.extend_from_slice(&FRAME_MAGIC);
            header.push(FRAME_TYPE_SERIES_DEF);
            header.extend_from_slice(&[0u8; 3]);
            header.extend_from_slice(&seq.to_le_bytes());
            header.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            header.extend_from_slice(&crc32.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(payload).unwrap();
        };

        write_frame(&mut file, 2, &payload, checksum32(&payload));
        write_frame(
            &mut file,
            10_000,
            &payload,
            checksum32(&payload).wrapping_add(1),
        );
        file.flush().unwrap();

        assert_eq!(scan_last_seq(&wal_path).unwrap(), 2);
    }

    #[test]
    fn failed_append_does_not_advance_next_seq() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join(WAL_FILE_NAME);
        std::fs::File::create(&wal_path).unwrap();

        let writer_file = OpenOptions::new().read(true).open(&wal_path).unwrap();
        let wal = FramedWal {
            path: PathBuf::from(&wal_path),
            writer: parking_lot::Mutex::new(std::io::BufWriter::new(writer_file)),
            next_seq: AtomicU64::new(7),
            sync_mode: WalSyncMode::PerAppend,
            last_sync: parking_lot::Mutex::new(Instant::now()),
        };

        let err = wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 11,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        });

        assert!(err.is_err());
        assert_eq!(wal.next_seq.load(Ordering::SeqCst), 7);
    }

    #[test]
    fn rollback_partial_append_clears_buffered_bytes_and_preserves_next_frame() {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open_with_buffer_size(temp_dir.path(), WalSyncMode::PerAppend, 4096)
            .unwrap();

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        let len_before_failure = std::fs::metadata(wal.path()).unwrap().len();

        {
            let mut writer = wal.writer.lock();
            writer.write_all(b"TSFRpartial").unwrap();
            assert!(!writer.buffer().is_empty());
            wal.rollback_partial_append(&mut writer, len_before_failure)
                .unwrap();
            assert!(writer.buffer().is_empty());
        }

        assert_eq!(
            std::fs::metadata(wal.path()).unwrap().len(),
            len_before_failure
        );

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 2,
            metric: "mem".to_string(),
            labels: vec![Label::new("host", "b")],
        })
        .unwrap();

        let replay = wal.replay_frames().unwrap();
        assert_eq!(replay.len(), 2);

        let first = match &replay[0] {
            ReplayFrame::SeriesDefinition(frame) => frame,
            _ => panic!("expected series definition frame"),
        };
        let second = match &replay[1] {
            ReplayFrame::SeriesDefinition(frame) => frame,
            _ => panic!("expected series definition frame"),
        };

        assert_eq!(first.series_id, 1);
        assert_eq!(second.series_id, 2);
    }
}
