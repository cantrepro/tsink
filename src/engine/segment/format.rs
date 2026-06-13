use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use roaring::RoaringTreemap;

use crate::engine::binio::{
    append_i64, append_u16, append_u32, append_u64, append_u8, checksum32,
    decode_optional_zstd_framed_file, encode_optional_zstd_framed_file, read_array, read_bytes,
    read_i64, read_u16, read_u32, read_u32_at, read_u64, read_u8, read_u8_at, write_u64_at,
};
use crate::engine::chunk::{Chunk, ChunkHeader, TimestampCodecId, ValueCodecId, ValueLane};
use crate::engine::durability::WalHighWatermark;
use crate::engine::index::{ChunkIndex, ChunkIndexEntry};
use crate::engine::series::{LabelPairId, SeriesId, SeriesRegistry, SeriesValueFamily};
use crate::{Label, Result, TsinkError};

use super::postings::SegmentPostingsIndex;
use super::types::{PersistedSeries, SegmentManifest};

const MANIFEST_MAGIC: [u8; 4] = *b"TSM2";
const CHUNKS_MAGIC: [u8; 4] = *b"CHK2";
const CHUNK_INDEX_MAGIC: [u8; 4] = *b"CID2";
const SERIES_MAGIC: [u8; 4] = *b"SRS2";
const POSTINGS_MAGIC: [u8; 4] = *b"PST2";

const FORMAT_VERSION: u16 = 2;
pub(super) const FILE_KIND_CHUNKS: u8 = 1;
pub(super) const FILE_KIND_CHUNK_INDEX: u8 = 2;
pub(super) const FILE_KIND_SERIES: u8 = 3;
pub(super) const FILE_KIND_POSTINGS: u8 = 4;
const POSTINGS_KIND_METRIC: u8 = 1;
const POSTINGS_KIND_LABEL_NAME: u8 = 2;
const POSTINGS_KIND_LABEL_PAIR: u8 = 3;
const SERIES_FLAG_VALUE_FAMILY: u16 = 0b0000_0010;
const SERIES_FLAG_LEGACY_VALUE_FAMILY: u16 = 0b0000_0001;
pub(crate) const CHUNK_FLAG_PAYLOAD_ZSTD: u8 = 0b0000_0001;
const CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES: usize = 4;
const CHUNK_PAYLOAD_ZSTD_LEVEL_FAST: i32 = 1;

pub(super) const CHUNKS_HEADER_LEN: usize = 16;
const CHUNK_INDEX_HEADER_LEN: usize = 24;
const SERIES_HEADER_LEN: usize = 28;
pub(super) const POSTINGS_HEADER_LEN: usize = 16;
const MANIFEST_HEADER_LEN: usize = 80;
const MANIFEST_FILE_ENTRY_LEN: usize = 20;
pub(super) const MANIFEST_FILE_ENTRY_COUNT: usize = 4;

type BuildChunksAndIndexOutput = (Vec<u8>, ChunkIndex, usize, usize, Option<i64>, Option<i64>);
type BuildSeriesFileOutput = (Vec<u8>, usize);

#[derive(Debug, Clone)]
pub(super) struct ChunkRecordMeta {
    pub(super) len: u32,
    pub(super) chunk: Chunk,
}

#[derive(Debug, Clone)]
pub(super) struct ManifestFileEntry {
    pub(super) kind: u8,
    pub(super) file_len: u64,
    pub(super) hash64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ParsedManifest {
    pub(super) segment_id: u64,
    pub(super) level: u8,
    pub(super) chunk_count: usize,
    pub(super) point_count: usize,
    pub(super) series_count: usize,
    pub(super) min_ts: Option<i64>,
    pub(super) max_ts: Option<i64>,
    pub(super) wal_highwater: WalHighWatermark,
    pub(super) files: [ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT],
}

#[derive(Debug, Clone)]
pub(super) struct ParsedSeriesEntry {
    pub(super) series_id: SeriesId,
    pub(super) metric_id: u32,
    pub(super) lane: ValueLane,
    pub(super) value_family: Option<SeriesValueFamily>,
    pub(super) label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone)]
pub(super) struct SegmentSeriesEntry {
    pub(super) series_id: SeriesId,
    pub(super) lane: ValueLane,
    pub(super) value_family: SeriesValueFamily,
    pub(super) metric_id: u32,
    pub(super) label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone)]
pub(super) struct ParsedSeriesFile {
    pub(super) metrics: Vec<String>,
    pub(super) label_names: Vec<String>,
    pub(super) label_values: Vec<String>,
    pub(super) entries: Vec<ParsedSeriesEntry>,
}

#[derive(Debug, Clone)]
pub(super) struct BuiltSegmentSeriesData {
    pub(super) metrics: Vec<String>,
    pub(super) label_names: Vec<String>,
    pub(super) label_values: Vec<String>,
    pub(super) entries: Vec<SegmentSeriesEntry>,
}

pub(super) fn build_chunks_and_index<T>(
    level: u8,
    chunks_by_series: &HashMap<SeriesId, Vec<T>>,
) -> Result<BuildChunksAndIndexOutput>
where
    T: AsRef<Chunk>,
{
    let mut series_ids = chunks_by_series.keys().copied().collect::<Vec<_>>();
    series_ids.sort_unstable();

    let mut chunk_count = 0usize;
    let mut point_count = 0usize;
    let mut min_ts: Option<i64> = None;
    let mut max_ts: Option<i64> = None;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CHUNKS_MAGIC);
    append_u16(&mut bytes, FORMAT_VERSION);
    append_u16(&mut bytes, 0u16);
    append_u64(&mut bytes, 0u64);

    let mut index = ChunkIndex::default();

    for series_id in series_ids {
        let Some(chunks) = chunks_by_series.get(&series_id) else {
            continue;
        };

        let mut ordered_indices = (0..chunks.len()).collect::<Vec<_>>();
        ordered_indices.sort_by(|&a, &b| {
            let left = chunks[a].as_ref();
            let right = chunks[b].as_ref();
            (
                left.header.min_ts,
                left.header.max_ts,
                left.header.point_count,
            )
                .cmp(&(
                    right.header.min_ts,
                    right.header.max_ts,
                    right.header.point_count,
                ))
        });

        for chunk_idx in ordered_indices {
            let chunk = chunks[chunk_idx].as_ref();
            if chunk.encoded_payload.is_empty() {
                return Err(TsinkError::DataCorruption(
                    "chunk payload is empty during segment write".to_string(),
                ));
            }

            chunk_count = chunk_count.saturating_add(1);
            point_count = point_count.saturating_add(chunk.header.point_count as usize);
            min_ts = Some(min_ts.map_or(chunk.header.min_ts, |min| min.min(chunk.header.min_ts)));
            max_ts = Some(max_ts.map_or(chunk.header.max_ts, |max| max.max(chunk.header.max_ts)));

            let offset = bytes.len() as u64;
            let record_start = bytes.len();
            append_chunk_record(&mut bytes, chunk)?;
            let record_len =
                u32::try_from(bytes.len().saturating_sub(record_start)).map_err(|_| {
                    TsinkError::InvalidConfiguration(
                        "chunk record length exceeds u32 in chunks.bin".to_string(),
                    )
                })?;

            index.add_entry(ChunkIndexEntry {
                series_id,
                min_ts: chunk.header.min_ts,
                max_ts: chunk.header.max_ts,
                chunk_offset: offset,
                chunk_len: record_len,
                point_count: chunk.header.point_count,
                lane: chunk.header.lane,
                ts_codec: chunk.header.ts_codec,
                value_codec: chunk.header.value_codec,
                level,
            });
        }
    }

    write_u64_at(bytes.as_mut_slice(), 8, chunk_count as u64)?;

    Ok((bytes, index, chunk_count, point_count, min_ts, max_ts))
}

fn append_chunk_record(out: &mut Vec<u8>, chunk: &Chunk) -> Result<()> {
    let (chunk_flags, payload) = encode_chunk_payload_for_storage(&chunk.encoded_payload)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| TsinkError::InvalidConfiguration("chunk payload too large".to_string()))?;

    let mut header_body = Vec::with_capacity(34);
    append_u64(&mut header_body, chunk.header.series_id);
    append_u8(&mut header_body, chunk.header.lane as u8);
    append_u8(&mut header_body, chunk.header.ts_codec as u8);
    append_u8(&mut header_body, chunk.header.value_codec as u8);
    append_u8(&mut header_body, chunk_flags);
    append_u16(&mut header_body, chunk.header.point_count);
    append_i64(&mut header_body, chunk.header.min_ts);
    append_i64(&mut header_body, chunk.header.max_ts);
    append_u32(&mut header_body, payload_len);

    let header_crc32 = checksum32(&header_body);
    let payload_crc32 = checksum32(&payload);

    let record_len = 4usize
        .saturating_add(header_body.len())
        .saturating_add(payload.len())
        .saturating_add(4);

    let record_len_u32 = u32::try_from(record_len).map_err(|_| {
        TsinkError::InvalidConfiguration("chunk record exceeds u32 length".to_string())
    })?;

    append_u32(out, record_len_u32);
    append_u32(out, header_crc32);
    out.extend_from_slice(&header_body);
    out.extend_from_slice(&payload);
    append_u32(out, payload_crc32);

    Ok(())
}

pub(super) fn build_chunk_index_file(index: &mut ChunkIndex) -> Result<Vec<u8>> {
    index.finalize();

    let mut series_ranges = Vec::<(SeriesId, u64, u32)>::new();
    let mut i = 0usize;
    while i < index.entries.len() {
        let series_id = index.entries[i].series_id;
        let first = i;
        while i < index.entries.len() && index.entries[i].series_id == series_id {
            i += 1;
        }

        let count = u32::try_from(i.saturating_sub(first)).map_err(|_| {
            TsinkError::InvalidConfiguration("series chunk count exceeds u32".to_string())
        })?;
        series_ranges.push((series_id, first as u64, count));
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CHUNK_INDEX_MAGIC);
    append_u16(&mut bytes, FORMAT_VERSION);
    append_u16(&mut bytes, 0u16);
    append_u64(&mut bytes, index.entries.len() as u64);
    append_u64(&mut bytes, series_ranges.len() as u64);

    for entry in &index.entries {
        append_u64(&mut bytes, entry.series_id);
        append_i64(&mut bytes, entry.min_ts);
        append_i64(&mut bytes, entry.max_ts);
        append_u64(&mut bytes, entry.chunk_offset);
        append_u32(&mut bytes, entry.chunk_len);
        append_u16(&mut bytes, entry.point_count);
        append_u8(&mut bytes, entry.lane as u8);
        append_u8(&mut bytes, entry.ts_codec as u8);
        append_u8(&mut bytes, entry.value_codec as u8);
        append_u8(&mut bytes, entry.level);
    }

    for (series_id, first_entry_index, count) in series_ranges {
        append_u64(&mut bytes, series_id);
        append_u64(&mut bytes, first_entry_index);
        append_u32(&mut bytes, count);
        append_u32(&mut bytes, 0u32);
    }

    encode_optional_zstd_framed_file(&bytes)
}

pub(super) fn build_segment_series_data<T>(
    registry: &SeriesRegistry,
    chunks_by_series: &HashMap<SeriesId, Vec<T>>,
) -> Result<BuiltSegmentSeriesData>
where
    T: AsRef<Chunk>,
{
    fn intern_dict(
        map: &mut HashMap<String, u32>,
        values: &mut Vec<String>,
        value: &str,
    ) -> Result<u32> {
        if let Some(id) = map.get(value) {
            return Ok(*id);
        }

        let id = u32::try_from(values.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("segment dictionary exceeded u32 ids".to_string())
        })?;
        let owned = value.to_string();
        values.push(owned.clone());
        map.insert(owned, id);
        Ok(id)
    }

    let mut metric_ids = HashMap::<String, u32>::new();
    let mut label_name_ids = HashMap::<String, u32>::new();
    let mut label_value_ids = HashMap::<String, u32>::new();
    let mut metric_values = Vec::<String>::new();
    let mut label_name_values = Vec::<String>::new();
    let mut label_value_values = Vec::<String>::new();
    let mut series_entries = Vec::<SegmentSeriesEntry>::new();

    let mut series_ids = chunks_by_series
        .iter()
        .filter_map(|(series_id, chunks)| (!chunks.is_empty()).then_some(*series_id))
        .collect::<Vec<_>>();
    series_ids.sort_unstable();

    for series_id in series_ids {
        let Some(series_key) = registry.decode_series_key(series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "missing series definition for id {}",
                series_id
            )));
        };

        let metric_id = intern_dict(&mut metric_ids, &mut metric_values, &series_key.metric)?;
        let lane = infer_series_lane(series_id, chunks_by_series);
        let value_family = infer_series_value_family(registry, series_id, chunks_by_series)?;
        let mut label_pairs = Vec::with_capacity(series_key.labels.len());

        for label in &series_key.labels {
            let name_id = intern_dict(&mut label_name_ids, &mut label_name_values, &label.name)?;
            let value_id =
                intern_dict(&mut label_value_ids, &mut label_value_values, &label.value)?;
            label_pairs.push(LabelPairId { name_id, value_id });
        }
        label_pairs.sort_unstable();
        label_pairs.dedup();

        series_entries.push(SegmentSeriesEntry {
            series_id,
            lane,
            value_family,
            metric_id,
            label_pairs,
        });
    }

    Ok(BuiltSegmentSeriesData {
        metrics: metric_values,
        label_names: label_name_values,
        label_values: label_value_values,
        entries: series_entries,
    })
}

pub(super) fn build_series_file(
    series_data: &BuiltSegmentSeriesData,
) -> Result<BuildSeriesFileOutput> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SERIES_MAGIC);
    append_u16(&mut bytes, FORMAT_VERSION);
    append_u16(&mut bytes, SERIES_FLAG_VALUE_FAMILY);
    append_u32(&mut bytes, series_data.metrics.len() as u32);
    append_u32(&mut bytes, series_data.label_names.len() as u32);
    append_u32(&mut bytes, series_data.label_values.len() as u32);
    append_u64(&mut bytes, series_data.entries.len() as u64);

    for (id, value) in series_data.metrics.iter().enumerate() {
        write_dict_entry(&mut bytes, id as u32, value)?;
    }
    for (id, value) in series_data.label_names.iter().enumerate() {
        write_dict_entry(&mut bytes, id as u32, value)?;
    }
    for (id, value) in series_data.label_values.iter().enumerate() {
        write_dict_entry(&mut bytes, id as u32, value)?;
    }

    let series_entry_offset = bytes.len();
    let pairs_offset_base = series_entry_offset + series_data.entries.len().saturating_mul(24);

    let mut pairs_bytes = Vec::new();

    for series in &series_data.entries {
        let pairs_offset = pairs_offset_base + pairs_bytes.len();

        let label_pair_count = u16::try_from(series.label_pairs.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("series label pair count exceeds u16".to_string())
        })?;

        append_u64(&mut bytes, series.series_id);
        append_u8(&mut bytes, series.lane as u8);
        append_u8(&mut bytes, encode_series_value_family(series.value_family));
        append_u16(&mut bytes, label_pair_count);
        append_u32(&mut bytes, series.metric_id);
        append_u64(&mut bytes, pairs_offset as u64);

        for pair in &series.label_pairs {
            append_u32(&mut pairs_bytes, pair.name_id);
            append_u32(&mut pairs_bytes, pair.value_id);
        }
    }

    bytes.extend_from_slice(&pairs_bytes);
    Ok((
        encode_optional_zstd_framed_file(&bytes)?,
        series_data.entries.len(),
    ))
}

fn write_dict_entry(out: &mut Vec<u8>, id: u32, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| TsinkError::InvalidConfiguration("dictionary string too large".to_string()))?;

    append_u32(out, id);
    append_u32(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

fn infer_series_lane<T>(
    series_id: SeriesId,
    chunks_by_series: &HashMap<SeriesId, Vec<T>>,
) -> ValueLane
where
    T: AsRef<Chunk>,
{
    chunks_by_series
        .get(&series_id)
        .and_then(|chunks| chunks.first())
        .map(|chunk| chunk.as_ref().header.lane)
        .unwrap_or(ValueLane::Numeric)
}

fn infer_series_value_family<T>(
    registry: &SeriesRegistry,
    series_id: SeriesId,
    chunks_by_series: &HashMap<SeriesId, Vec<T>>,
) -> Result<SeriesValueFamily>
where
    T: AsRef<Chunk>,
{
    let chunks = chunks_by_series.get(&series_id).ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "missing persisted chunk for series {} while encoding series metadata",
            series_id
        ))
    })?;

    let mut header_family = None;
    for chunk in chunks {
        let Some(candidate) = chunk.as_ref().header.value_family else {
            continue;
        };

        match header_family {
            None => header_family = Some(candidate),
            Some(existing) if existing != candidate => {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} conflicts across chunk header value families",
                    series_id
                )));
            }
            Some(_) => {}
        }
    }

    let registry_family = registry.series_value_family(series_id);
    match (registry_family, header_family) {
        (Some(registry_family), Some(header_family)) if registry_family != header_family => {
            Err(TsinkError::DataCorruption(format!(
                "series id {} registry value family {} conflicts with chunk header family {}",
                series_id,
                registry_family.name(),
                header_family.name(),
            )))
        }
        (Some(registry_family), _) => Ok(registry_family),
        (None, Some(header_family)) => Ok(header_family),
        (None, None) => Err(TsinkError::DataCorruption(format!(
            "missing value family metadata for series {} while encoding series metadata",
            series_id
        ))),
    }
}

fn append_postings_entry(
    bytes: &mut Vec<u8>,
    kind: u8,
    primary_id: u32,
    secondary_id: u32,
    series_ids: &RoaringTreemap,
) -> Result<()> {
    let mut payload = Vec::new();
    series_ids
        .serialize_into(&mut std::io::Cursor::new(&mut payload))
        .map_err(|err| TsinkError::Other(format!("failed to encode posting list: {err}")))?;

    append_u8(bytes, kind);
    append_u8(bytes, 0u8);
    append_u16(bytes, 0u16);
    append_u32(bytes, primary_id);
    append_u32(bytes, secondary_id);
    append_u32(bytes, series_ids.len().min(u64::from(u32::MAX)) as u32);
    append_u32(bytes, payload.len().min(u32::MAX as usize) as u32);
    bytes.extend_from_slice(&payload);
    Ok(())
}

pub(super) fn build_postings_file(series_data: &BuiltSegmentSeriesData) -> Result<Vec<u8>> {
    let mut metric_postings = BTreeMap::<u32, RoaringTreemap>::new();
    let mut label_name_postings = BTreeMap::<u32, RoaringTreemap>::new();
    let mut label_postings = BTreeMap::<LabelPairId, RoaringTreemap>::new();

    for series in &series_data.entries {
        metric_postings
            .entry(series.metric_id)
            .or_default()
            .insert(series.series_id);
        for pair in &series.label_pairs {
            label_name_postings
                .entry(pair.name_id)
                .or_default()
                .insert(series.series_id);
            label_postings
                .entry(*pair)
                .or_default()
                .insert(series.series_id);
        }
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&POSTINGS_MAGIC);
    append_u16(&mut bytes, FORMAT_VERSION);
    append_u16(&mut bytes, 0u16);
    append_u64(
        &mut bytes,
        (metric_postings.len() + label_name_postings.len() + label_postings.len()) as u64,
    );

    for (metric_id, series_ids) in metric_postings {
        append_postings_entry(&mut bytes, POSTINGS_KIND_METRIC, metric_id, 0, &series_ids)?;
    }
    for (label_name_id, series_ids) in label_name_postings {
        append_postings_entry(
            &mut bytes,
            POSTINGS_KIND_LABEL_NAME,
            label_name_id,
            0,
            &series_ids,
        )?;
    }
    for (pair, series_ids) in label_postings {
        append_postings_entry(
            &mut bytes,
            POSTINGS_KIND_LABEL_PAIR,
            pair.name_id,
            pair.value_id,
            &series_ids,
        )?;
    }

    encode_optional_zstd_framed_file(&bytes)
}

pub(super) fn build_manifest_file(
    manifest: &SegmentManifest,
    file_entries: [ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&MANIFEST_MAGIC);
    append_u16(&mut bytes, FORMAT_VERSION);
    append_u16(&mut bytes, 0u16);
    append_u64(&mut bytes, manifest.segment_id);
    append_u8(&mut bytes, manifest.level);
    bytes.extend_from_slice(&[0u8; 7]);
    append_i64(&mut bytes, manifest.min_ts.unwrap_or(0));
    append_i64(&mut bytes, manifest.max_ts.unwrap_or(0));

    let created_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    append_i64(&mut bytes, created_unix_ns);

    append_u64(&mut bytes, manifest.series_count as u64);
    append_u64(&mut bytes, manifest.chunk_count as u64);
    append_u64(&mut bytes, manifest.point_count as u64);
    append_u64(&mut bytes, manifest.wal_highwater.segment);
    append_u64(&mut bytes, manifest.wal_highwater.frame);
    append_u32(&mut bytes, MANIFEST_FILE_ENTRY_COUNT as u32);
    append_u32(&mut bytes, 0u32);

    for entry in &file_entries {
        append_u8(&mut bytes, entry.kind);
        append_u8(&mut bytes, 0u8);
        append_u16(&mut bytes, 0u16);
        append_u64(&mut bytes, entry.file_len);
        append_u64(&mut bytes, entry.hash64);
    }

    let crc = checksum32(&bytes);
    append_u32(&mut bytes, crc);

    Ok(bytes)
}

pub(super) fn parse_manifest(bytes: &[u8]) -> Result<ParsedManifest> {
    if bytes.len() < MANIFEST_HEADER_LEN + (MANIFEST_FILE_ENTRY_LEN * MANIFEST_FILE_ENTRY_COUNT) + 4
    {
        return Err(TsinkError::DataCorruption(
            "manifest.bin is too short".to_string(),
        ));
    }

    let expected_crc = read_u32_at(bytes, bytes.len() - 4)?;
    let actual_crc = checksum32(&bytes[..bytes.len() - 4]);
    if expected_crc != actual_crc {
        return Err(TsinkError::DataCorruption(
            "manifest crc32 mismatch".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != MANIFEST_MAGIC {
        return Err(TsinkError::DataCorruption(
            "manifest magic mismatch".to_string(),
        ));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported manifest version {version}"
        )));
    }

    let _flags = read_u16(bytes, &mut pos)?;
    let segment_id = read_u64(bytes, &mut pos)?;
    let level = read_u8(bytes, &mut pos)?;
    let _reserved0 = read_bytes(bytes, &mut pos, 7)?;
    let min_ts_raw = read_i64(bytes, &mut pos)?;
    let max_ts_raw = read_i64(bytes, &mut pos)?;
    let _created_unix_ns = read_i64(bytes, &mut pos)?;
    let series_count = read_u64(bytes, &mut pos)? as usize;
    let chunk_count = read_u64(bytes, &mut pos)? as usize;
    let point_count = read_u64(bytes, &mut pos)? as usize;
    let wal_highwater_segment = read_u64(bytes, &mut pos)?;
    let wal_highwater_frame = read_u64(bytes, &mut pos)?;
    let file_entry_count = read_u32(bytes, &mut pos)? as usize;
    let _reserved1 = read_u32(bytes, &mut pos)?;

    if file_entry_count != MANIFEST_FILE_ENTRY_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "manifest file entry count {} is not {}",
            file_entry_count, MANIFEST_FILE_ENTRY_COUNT
        )));
    }

    let mut files = Vec::with_capacity(MANIFEST_FILE_ENTRY_COUNT);
    for _ in 0..MANIFEST_FILE_ENTRY_COUNT {
        let kind = read_u8(bytes, &mut pos)?;
        let _compression = read_u8(bytes, &mut pos)?;
        let _reserved = read_u16(bytes, &mut pos)?;
        let file_len = read_u64(bytes, &mut pos)?;
        let hash64 = read_u64(bytes, &mut pos)?;
        files.push(ManifestFileEntry {
            kind,
            file_len,
            hash64,
        });
    }

    if pos + 4 != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "manifest has unexpected trailing bytes".to_string(),
        ));
    }

    let files: [ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT] = files
        .try_into()
        .map_err(|_| TsinkError::DataCorruption("manifest file entries malformed".to_string()))?;

    Ok(ParsedManifest {
        segment_id,
        level,
        chunk_count,
        point_count,
        series_count,
        min_ts: if chunk_count == 0 {
            None
        } else {
            Some(min_ts_raw)
        },
        max_ts: if chunk_count == 0 {
            None
        } else {
            Some(max_ts_raw)
        },
        wal_highwater: WalHighWatermark {
            segment: wal_highwater_segment,
            frame: wal_highwater_frame,
        },
        files,
    })
}

pub(super) fn segment_manifest_from_parsed(parsed_manifest: &ParsedManifest) -> SegmentManifest {
    SegmentManifest {
        segment_id: parsed_manifest.segment_id,
        level: parsed_manifest.level,
        chunk_count: parsed_manifest.chunk_count,
        point_count: parsed_manifest.point_count,
        series_count: parsed_manifest.series_count,
        min_ts: parsed_manifest.min_ts,
        max_ts: parsed_manifest.max_ts,
        wal_highwater: parsed_manifest.wal_highwater,
    }
}

pub(super) fn verify_file_manifest_entry(
    entry: &ManifestFileEntry,
    expected_kind: u8,
    bytes: &[u8],
) -> Result<()> {
    if entry.kind != expected_kind {
        return Err(TsinkError::DataCorruption(format!(
            "manifest file kind mismatch: expected {}, got {}",
            expected_kind, entry.kind
        )));
    }

    if entry.file_len != bytes.len() as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "manifest file length mismatch for kind {}",
            expected_kind
        )));
    }

    if entry.hash64 != hash64(bytes) {
        return Err(TsinkError::DataCorruption(format!(
            "manifest file hash mismatch for kind {}",
            expected_kind
        )));
    }

    Ok(())
}

pub(super) fn parse_series_file(bytes: &[u8]) -> Result<ParsedSeriesFile> {
    let bytes =
        decode_optional_zstd_framed_file(bytes, SERIES_MAGIC, FORMAT_VERSION, "series.bin")?;
    if bytes.len() < SERIES_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "series.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(&bytes, &mut pos)?;
    if magic != SERIES_MAGIC {
        return Err(TsinkError::DataCorruption(
            "series.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(&bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported series.bin version {version}"
        )));
    }

    let flags = read_u16(&bytes, &mut pos)?;
    if flags & !(SERIES_FLAG_VALUE_FAMILY | SERIES_FLAG_LEGACY_VALUE_FAMILY) != 0 {
        return Err(TsinkError::DataCorruption(format!(
            "invalid series.bin flags {flags:#06x}"
        )));
    }
    let metric_count = read_u32(&bytes, &mut pos)? as usize;
    let label_name_count = read_u32(&bytes, &mut pos)? as usize;
    let label_value_count = read_u32(&bytes, &mut pos)? as usize;
    let series_count = read_u64(&bytes, &mut pos)? as usize;

    let metrics = parse_dictionary(&bytes, &mut pos, metric_count)?;
    let label_names = parse_dictionary(&bytes, &mut pos, label_name_count)?;
    let label_values = parse_dictionary(&bytes, &mut pos, label_value_count)?;

    let mut entries_stub = Vec::with_capacity(series_count);
    for _ in 0..series_count {
        let series_id = read_u64(&bytes, &mut pos)?;
        let lane = decode_lane(read_u8(&bytes, &mut pos)?)?;
        let value_family =
            if flags & (SERIES_FLAG_VALUE_FAMILY | SERIES_FLAG_LEGACY_VALUE_FAMILY) != 0 {
                Some(decode_series_value_family(read_u8(&bytes, &mut pos)?)?)
            } else {
                let _reserved = read_u8(&bytes, &mut pos)?;
                None
            };
        let label_pair_count = read_u16(&bytes, &mut pos)? as usize;
        let metric_id = read_u32(&bytes, &mut pos)?;
        let pair_offset = read_u64(&bytes, &mut pos)? as usize;

        entries_stub.push((
            series_id,
            metric_id,
            lane,
            value_family,
            label_pair_count,
            pair_offset,
        ));
    }

    let mut entries = Vec::with_capacity(entries_stub.len());
    for (series_id, metric_id, lane, value_family, pair_count, pair_offset) in entries_stub {
        let mut pair_pos = pair_offset;
        let mut label_pairs = Vec::with_capacity(pair_count);
        for _ in 0..pair_count {
            let name_id = read_u32(&bytes, &mut pair_pos)?;
            let value_id = read_u32(&bytes, &mut pair_pos)?;
            label_pairs.push(LabelPairId { name_id, value_id });
        }

        if pair_pos > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "series label pair block exceeds file size".to_string(),
            ));
        }

        entries.push(ParsedSeriesEntry {
            series_id,
            metric_id,
            lane,
            value_family,
            label_pairs,
        });
    }

    Ok(ParsedSeriesFile {
        metrics,
        label_names,
        label_values,
        entries,
    })
}

fn parse_dictionary(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<String>> {
    let mut values = Vec::with_capacity(count);
    for expected_id in 0..count {
        let id = read_u32(bytes, pos)? as usize;
        if id != expected_id {
            return Err(TsinkError::DataCorruption(format!(
                "dictionary id {} is not dense at expected {}",
                id, expected_id
            )));
        }

        let len = read_u32(bytes, pos)? as usize;
        let value = read_bytes(bytes, pos, len)?;
        values.push(String::from_utf8(value.to_vec())?);
    }

    Ok(values)
}

pub(super) fn decode_persisted_series(parsed: &ParsedSeriesFile) -> Result<Vec<PersistedSeries>> {
    let mut out = Vec::with_capacity(parsed.entries.len());

    for entry in &parsed.entries {
        let Some(metric) = parsed.metrics.get(entry.metric_id as usize) else {
            return Err(TsinkError::DataCorruption(format!(
                "series {} metric id {} not found in dictionary",
                entry.series_id, entry.metric_id
            )));
        };

        let _lane = entry.lane;

        let mut labels = Vec::with_capacity(entry.label_pairs.len());
        for pair in &entry.label_pairs {
            let Some(name) = parsed.label_names.get(pair.name_id as usize) else {
                return Err(TsinkError::DataCorruption(format!(
                    "series {} label name id {} not found",
                    entry.series_id, pair.name_id
                )));
            };
            let Some(value) = parsed.label_values.get(pair.value_id as usize) else {
                return Err(TsinkError::DataCorruption(format!(
                    "series {} label value id {} not found",
                    entry.series_id, pair.value_id
                )));
            };
            labels.push(Label::new(name, value));
        }
        labels.sort();

        out.push(PersistedSeries {
            series_id: entry.series_id,
            metric: metric.clone(),
            labels,
            value_family: entry.value_family,
        });
    }

    Ok(out)
}

pub(super) fn parse_postings_file(
    bytes: &[u8],
    parsed_series: &ParsedSeriesFile,
) -> Result<SegmentPostingsIndex> {
    let bytes =
        decode_optional_zstd_framed_file(bytes, POSTINGS_MAGIC, FORMAT_VERSION, "postings.bin")?;
    if bytes.len() < POSTINGS_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "postings.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(&bytes, &mut pos)?;
    if magic != POSTINGS_MAGIC {
        return Err(TsinkError::DataCorruption(
            "postings.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(&bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported postings.bin version {version}"
        )));
    }

    let _flags = read_u16(&bytes, &mut pos)?;
    let postings_count = read_u64(&bytes, &mut pos)? as usize;
    let mut postings = SegmentPostingsIndex::from_series_postings(
        parsed_series
            .entries
            .iter()
            .map(|entry| entry.series_id)
            .collect(),
    );

    for _ in 0..postings_count {
        let kind = read_u8(&bytes, &mut pos)?;
        let _reserved0 = read_u8(&bytes, &mut pos)?;
        let _reserved1 = read_u16(&bytes, &mut pos)?;
        let primary_id = read_u32(&bytes, &mut pos)?;
        let secondary_id = read_u32(&bytes, &mut pos)?;
        let series_count = read_u32(&bytes, &mut pos)? as usize;
        let encoded_len = read_u32(&bytes, &mut pos)? as usize;
        let payload = read_bytes(&bytes, &mut pos, encoded_len)?;

        let bitmap = RoaringTreemap::deserialize_from(&mut std::io::Cursor::new(payload)).map_err(
            |err| {
                TsinkError::DataCorruption(format!(
                    "failed to decode roaring posting payload: {err}"
                ))
            },
        )?;
        if bitmap.len() != series_count as u64 {
            return Err(TsinkError::DataCorruption(format!(
                "posting list cardinality mismatch: expected {series_count}, decoded {}",
                bitmap.len()
            )));
        }

        match kind {
            POSTINGS_KIND_METRIC => {
                if secondary_id != 0 {
                    return Err(TsinkError::DataCorruption(
                        "metric posting entry has unexpected secondary id".to_string(),
                    ));
                }
                let Some(metric) = parsed_series.metrics.get(primary_id as usize) else {
                    return Err(TsinkError::DataCorruption(format!(
                        "metric posting id {} not found in dictionary",
                        primary_id
                    )));
                };
                postings.metric_postings.insert(metric.clone(), bitmap);
            }
            POSTINGS_KIND_LABEL_NAME => {
                if secondary_id != 0 {
                    return Err(TsinkError::DataCorruption(
                        "label-name posting entry has unexpected secondary id".to_string(),
                    ));
                }
                let Some(label_name) = parsed_series.label_names.get(primary_id as usize) else {
                    return Err(TsinkError::DataCorruption(format!(
                        "label-name posting id {} not found in dictionary",
                        primary_id
                    )));
                };
                postings
                    .label_name_postings
                    .insert(label_name.clone(), bitmap);
            }
            POSTINGS_KIND_LABEL_PAIR => {
                let Some(label_name) = parsed_series.label_names.get(primary_id as usize) else {
                    return Err(TsinkError::DataCorruption(format!(
                        "label-name posting id {} not found in dictionary",
                        primary_id
                    )));
                };
                let Some(label_value) = parsed_series.label_values.get(secondary_id as usize)
                else {
                    return Err(TsinkError::DataCorruption(format!(
                        "label-value posting id {} not found in dictionary",
                        secondary_id
                    )));
                };
                postings
                    .label_postings
                    .insert((label_name.clone(), label_value.clone()), bitmap);
            }
            _ => {
                return Err(TsinkError::DataCorruption(format!(
                    "unsupported postings entry kind {kind}"
                )));
            }
        }
    }

    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "postings.bin has trailing bytes".to_string(),
        ));
    }

    Ok(postings)
}

pub(super) fn parse_chunk_index_file(bytes: &[u8]) -> Result<ChunkIndex> {
    let bytes = decode_optional_zstd_framed_file(
        bytes,
        CHUNK_INDEX_MAGIC,
        FORMAT_VERSION,
        "chunk_index.bin",
    )?;
    if bytes.len() < CHUNK_INDEX_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "chunk_index.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(&bytes, &mut pos)?;
    if magic != CHUNK_INDEX_MAGIC {
        return Err(TsinkError::DataCorruption(
            "chunk_index.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(&bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported chunk_index.bin version {version}"
        )));
    }

    let _flags = read_u16(&bytes, &mut pos)?;
    let entry_count = read_u64(&bytes, &mut pos)? as usize;
    let series_table_count = read_u64(&bytes, &mut pos)? as usize;

    let mut index = ChunkIndex::default();

    for _ in 0..entry_count {
        let series_id = read_u64(&bytes, &mut pos)?;
        let min_ts = read_i64(&bytes, &mut pos)?;
        let max_ts = read_i64(&bytes, &mut pos)?;
        let chunk_offset = read_u64(&bytes, &mut pos)?;
        let chunk_len = read_u32(&bytes, &mut pos)?;
        let point_count = read_u16(&bytes, &mut pos)?;
        let lane = decode_lane(read_u8(&bytes, &mut pos)?)?;
        let ts_codec = decode_ts_codec(read_u8(&bytes, &mut pos)?)?;
        let value_codec = decode_value_codec(read_u8(&bytes, &mut pos)?)?;
        let level = read_u8(&bytes, &mut pos)?;

        index.add_entry(ChunkIndexEntry {
            series_id,
            min_ts,
            max_ts,
            chunk_offset,
            chunk_len,
            point_count,
            lane,
            ts_codec,
            value_codec,
            level,
        });
    }

    let mut prev_series = 0u64;
    for idx in 0..series_table_count {
        let series_id = read_u64(&bytes, &mut pos)?;
        let first_entry = read_u64(&bytes, &mut pos)? as usize;
        let count = read_u32(&bytes, &mut pos)? as usize;
        let _reserved = read_u32(&bytes, &mut pos)?;

        if idx > 0 && series_id < prev_series {
            return Err(TsinkError::DataCorruption(
                "chunk index series range table is not sorted".to_string(),
            ));
        }
        prev_series = series_id;

        if first_entry.saturating_add(count) > entry_count {
            return Err(TsinkError::DataCorruption(
                "chunk index series range points outside entry table".to_string(),
            ));
        }
    }

    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "chunk_index.bin has trailing bytes".to_string(),
        ));
    }

    Ok(index)
}

pub(super) fn parse_chunks_file(bytes: &[u8]) -> Result<BTreeMap<u64, ChunkRecordMeta>> {
    if bytes.len() < CHUNKS_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "chunks.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != CHUNKS_MAGIC {
        return Err(TsinkError::DataCorruption(
            "chunks.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported chunks.bin version {version}"
        )));
    }

    let _flags = read_u16(bytes, &mut pos)?;
    let chunk_count = read_u64(bytes, &mut pos)? as usize;

    let mut records = BTreeMap::new();

    for _ in 0..chunk_count {
        let record_offset = pos as u64;
        let record_len = read_u32(bytes, &mut pos)? as usize;
        let record_end = pos.saturating_add(record_len);
        if record_end > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "chunk record exceeds chunks.bin length".to_string(),
            ));
        }

        let header_crc32 = read_u32(bytes, &mut pos)?;
        let header_start = pos;

        let series_id = read_u64(bytes, &mut pos)?;
        let lane = decode_lane(read_u8(bytes, &mut pos)?)?;
        let ts_codec = decode_ts_codec(read_u8(bytes, &mut pos)?)?;
        let value_codec = decode_value_codec(read_u8(bytes, &mut pos)?)?;
        let chunk_flags = read_u8(bytes, &mut pos)?;
        let point_count = read_u16(bytes, &mut pos)?;
        let min_ts = read_i64(bytes, &mut pos)?;
        let max_ts = read_i64(bytes, &mut pos)?;
        let payload_len = read_u32(bytes, &mut pos)? as usize;

        let header_end = pos;
        if checksum32(&bytes[header_start..header_end]) != header_crc32 {
            return Err(TsinkError::DataCorruption(
                "chunk header crc mismatch".to_string(),
            ));
        }

        let payload = read_bytes(bytes, &mut pos, payload_len)?;
        let payload_crc32 = read_u32(bytes, &mut pos)?;
        if checksum32(payload) != payload_crc32 {
            return Err(TsinkError::DataCorruption(
                "chunk payload crc mismatch".to_string(),
            ));
        }
        let payload = decode_chunk_payload_from_storage(payload, chunk_flags)?;

        if pos != record_end {
            return Err(TsinkError::DataCorruption(
                "chunk record length mismatch".to_string(),
            ));
        }

        let total_len = u32::try_from(4usize.saturating_add(record_len)).map_err(|_| {
            TsinkError::InvalidConfiguration("chunk record total length exceeds u32".to_string())
        })?;

        records.insert(
            record_offset,
            ChunkRecordMeta {
                len: total_len,
                chunk: Chunk {
                    header: ChunkHeader {
                        series_id,
                        lane,
                        value_family: None,
                        point_count,
                        min_ts,
                        max_ts,
                        ts_codec,
                        value_codec,
                    },
                    points: Vec::new(),
                    encoded_payload: payload,
                    wal_highwater: WalHighWatermark::default(),
                },
            },
        );
    }

    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "chunks.bin has trailing bytes".to_string(),
        ));
    }

    Ok(records)
}

pub(super) fn validate_chunk_index_against_chunks_file(
    bytes: &[u8],
    index: &ChunkIndex,
) -> Result<()> {
    if bytes.len() < CHUNKS_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "chunks.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != CHUNKS_MAGIC {
        return Err(TsinkError::DataCorruption(
            "chunks.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported chunks.bin version {version}"
        )));
    }

    let _flags = read_u16(bytes, &mut pos)?;
    let chunk_count = read_u64(bytes, &mut pos)? as usize;

    if chunk_count != index.entries.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk count mismatch: chunks.bin has {}, chunk_index.bin has {}",
            chunk_count,
            index.entries.len()
        )));
    }

    let mut expected_by_offset = BTreeMap::<u64, &ChunkIndexEntry>::new();
    for entry in &index.entries {
        if expected_by_offset
            .insert(entry.chunk_offset, entry)
            .is_some()
        {
            return Err(TsinkError::DataCorruption(format!(
                "duplicate chunk offset {} in chunk index",
                entry.chunk_offset
            )));
        }
    }

    for _ in 0..chunk_count {
        let record_offset = pos as u64;
        let Some(entry) = expected_by_offset.remove(&record_offset) else {
            return Err(TsinkError::DataCorruption(format!(
                "chunks.bin contains unindexed chunk record at offset {}",
                record_offset
            )));
        };

        let record_len = read_u32(bytes, &mut pos)? as usize;
        let record_end = pos.saturating_add(record_len);
        if record_end > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "chunk record exceeds chunks.bin length".to_string(),
            ));
        }

        let total_len = u32::try_from(record_len.saturating_add(4)).map_err(|_| {
            TsinkError::DataCorruption("chunk record total length exceeds u32".to_string())
        })?;
        if total_len != entry.chunk_len {
            return Err(TsinkError::DataCorruption(format!(
                "chunk length mismatch at offset {}: index {}, chunk {}",
                entry.chunk_offset, entry.chunk_len, total_len
            )));
        }

        let header_crc32 = read_u32(bytes, &mut pos)?;
        let header_start = pos;

        let series_id = read_u64(bytes, &mut pos)?;
        let lane = decode_lane(read_u8(bytes, &mut pos)?)?;
        let ts_codec = decode_ts_codec(read_u8(bytes, &mut pos)?)?;
        let value_codec = decode_value_codec(read_u8(bytes, &mut pos)?)?;
        let chunk_flags = read_u8(bytes, &mut pos)?;
        let point_count = read_u16(bytes, &mut pos)?;
        let min_ts = read_i64(bytes, &mut pos)?;
        let max_ts = read_i64(bytes, &mut pos)?;
        let payload_len = read_u32(bytes, &mut pos)? as usize;

        let header_end = pos;
        if checksum32(&bytes[header_start..header_end]) != header_crc32 {
            return Err(TsinkError::DataCorruption(
                "chunk header crc mismatch".to_string(),
            ));
        }

        if series_id != entry.series_id
            || min_ts != entry.min_ts
            || max_ts != entry.max_ts
            || point_count != entry.point_count
            || lane != entry.lane
            || ts_codec != entry.ts_codec
            || value_codec != entry.value_codec
        {
            return Err(TsinkError::DataCorruption(
                "chunk index entry does not match chunk header".to_string(),
            ));
        }
        validate_chunk_payload_flags(chunk_flags)?;

        let payload = read_bytes(bytes, &mut pos, payload_len)?;
        let payload_crc32 = read_u32(bytes, &mut pos)?;
        if checksum32(payload) != payload_crc32 {
            return Err(TsinkError::DataCorruption(
                "chunk payload crc mismatch".to_string(),
            ));
        }

        if pos != record_end {
            return Err(TsinkError::DataCorruption(
                "chunk record length mismatch".to_string(),
            ));
        }
    }

    if !expected_by_offset.is_empty() {
        let first_missing = expected_by_offset
            .keys()
            .next()
            .copied()
            .unwrap_or_default();
        return Err(TsinkError::DataCorruption(format!(
            "chunk index references missing chunk offset {}",
            first_missing
        )));
    }

    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "chunks.bin has trailing bytes".to_string(),
        ));
    }

    Ok(())
}

fn decode_lane(raw: u8) -> Result<ValueLane> {
    match raw {
        0 => Ok(ValueLane::Numeric),
        1 => Ok(ValueLane::Blob),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid value lane {raw}"
        ))),
    }
}

fn encode_series_value_family(family: SeriesValueFamily) -> u8 {
    match family {
        SeriesValueFamily::F64 => 1,
        SeriesValueFamily::I64 => 2,
        SeriesValueFamily::U64 => 3,
        SeriesValueFamily::Bool => 4,
        SeriesValueFamily::Blob => 5,
        SeriesValueFamily::Histogram => 6,
    }
}

fn decode_series_value_family(raw: u8) -> Result<SeriesValueFamily> {
    match raw {
        1 => Ok(SeriesValueFamily::F64),
        2 => Ok(SeriesValueFamily::I64),
        3 => Ok(SeriesValueFamily::U64),
        4 => Ok(SeriesValueFamily::Bool),
        5 => Ok(SeriesValueFamily::Blob),
        6 => Ok(SeriesValueFamily::Histogram),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid series value family {raw}"
        ))),
    }
}

fn decode_ts_codec(raw: u8) -> Result<TimestampCodecId> {
    match raw {
        1 => Ok(TimestampCodecId::FixedStepRle),
        2 => Ok(TimestampCodecId::DeltaOfDeltaBitpack),
        3 => Ok(TimestampCodecId::DeltaVarint),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid timestamp codec id {raw}"
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
            "invalid value codec id {raw}"
        ))),
    }
}

pub(super) fn hash64(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(bytes, 0)
}

fn encode_chunk_payload_for_storage(payload: &[u8]) -> Result<(u8, Vec<u8>)> {
    let compressed =
        zstd::bulk::compress(payload, CHUNK_PAYLOAD_ZSTD_LEVEL_FAST).map_err(|err| {
            TsinkError::Compression(format!("zstd compress chunk payload failed: {err}"))
        })?;

    let original_len = u32::try_from(payload.len())
        .map_err(|_| TsinkError::InvalidConfiguration("chunk payload too large".to_string()))?;
    let mut wrapped = Vec::with_capacity(
        CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES.saturating_add(compressed.len()),
    );
    append_u32(&mut wrapped, original_len);
    wrapped.extend_from_slice(&compressed);

    if wrapped.len() >= payload.len() {
        return Ok((0u8, payload.to_vec()));
    }

    Ok((CHUNK_FLAG_PAYLOAD_ZSTD, wrapped))
}

pub(crate) fn validate_chunk_payload_flags(chunk_flags: u8) -> Result<()> {
    if chunk_flags & !CHUNK_FLAG_PAYLOAD_ZSTD != 0 {
        return Err(TsinkError::DataCorruption(format!(
            "invalid chunk payload flags {chunk_flags:#04x}"
        )));
    }
    Ok(())
}

pub(crate) fn chunk_payload_uses_zstd(chunk_flags: u8) -> Result<bool> {
    validate_chunk_payload_flags(chunk_flags)?;
    Ok(chunk_flags & CHUNK_FLAG_PAYLOAD_ZSTD != 0)
}

pub(crate) fn decompress_chunk_payload_zstd(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() < CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES {
        return Err(TsinkError::DataCorruption(
            "compressed chunk payload missing original length prefix".to_string(),
        ));
    }

    let expected_len = usize::try_from(read_u32_at(payload, 0)?).unwrap_or(usize::MAX);
    let compressed = &payload[CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES..];
    let decoded = zstd::bulk::decompress(compressed, expected_len).map_err(|err| {
        TsinkError::Compression(format!("zstd decompress chunk payload failed: {err}"))
    })?;

    if decoded.len() != expected_len {
        return Err(TsinkError::DataCorruption(format!(
            "chunk payload decompressed length mismatch: expected {expected_len}, got {}",
            decoded.len()
        )));
    }

    Ok(decoded)
}

fn decode_chunk_payload_from_storage(payload: &[u8], chunk_flags: u8) -> Result<Vec<u8>> {
    if chunk_payload_uses_zstd(chunk_flags)? {
        return decompress_chunk_payload_zstd(payload);
    }
    Ok(payload.to_vec())
}

pub(crate) fn chunk_payload_from_record<'a>(
    bytes: &'a [u8],
    chunk_offset: u64,
    chunk_len: u32,
) -> Result<Cow<'a, [u8]>> {
    let offset = usize::try_from(chunk_offset).map_err(|_| {
        TsinkError::DataCorruption(format!("chunk offset {chunk_offset} exceeds usize"))
    })?;
    let record_len = usize::try_from(chunk_len).map_err(|_| {
        TsinkError::DataCorruption(format!("chunk length {chunk_len} exceeds usize"))
    })?;
    let record_end = offset.saturating_add(record_len);
    if record_end > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk at offset {} length {} exceeds mapped file size {}",
            chunk_offset,
            chunk_len,
            bytes.len()
        )));
    }

    let record = &bytes[offset..record_end];
    if record.len() < 42 {
        return Err(TsinkError::DataCorruption(
            "chunk record too short for header".to_string(),
        ));
    }

    let body_len = usize::try_from(read_u32_at(record, 0)?).unwrap_or(usize::MAX);
    if body_len.saturating_add(4) != record.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk record length mismatch at offset {}",
            chunk_offset
        )));
    }

    let payload_len = usize::try_from(read_u32_at(record, 38)?).unwrap_or(usize::MAX);
    let payload_start = 42usize;
    let payload_end = payload_start.saturating_add(payload_len);
    let chunk_flags = read_u8_at(record, 19)?;

    if payload_end.saturating_add(4) != record.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk payload length mismatch at offset {}",
            chunk_offset
        )));
    }

    let payload = &record[payload_start..payload_end];
    if chunk_payload_uses_zstd(chunk_flags)? {
        return Ok(Cow::Owned(decompress_chunk_payload_zstd(payload)?));
    }

    Ok(Cow::Borrowed(payload))
}
