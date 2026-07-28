use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use roaring::RoaringTreemap;

use crate::engine::binio::{
    append_i64, append_u16, append_u32, append_u64, append_u8, checksum32,
    decode_optional_zstd_framed_file_with_limit, decompress_zstd_exact_bounded,
    encode_optional_zstd_framed_file, read_array, read_bytes, read_i64, read_u16, read_u32,
    read_u32_at, read_u64, read_u8, read_u8_at, write_u64_at, MAX_DECODED_FRAMED_FILE_BYTES,
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
pub(super) const CHUNK_INDEX_MAGIC: [u8; 4] = *b"CID2";
pub(super) const SERIES_MAGIC: [u8; 4] = *b"SRS2";
pub(super) const POSTINGS_MAGIC: [u8; 4] = *b"PST2";

pub(super) const FORMAT_VERSION: u16 = 2;
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
/// Last-resort format ceiling for one decoded chunk payload.
pub(crate) const MAX_DECODED_CHUNK_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
/// Maximum raw size accepted for one `chunks.bin` file before any record decoding or allocation.
pub(crate) const MAX_SEGMENT_CHUNKS_FILE_BYTES: usize = 1024 * 1024 * 1024;
pub(crate) const MAX_SEGMENT_MANIFEST_FILE_BYTES: usize = 1024 * 1024;

pub(super) const CHUNKS_HEADER_LEN: usize = 16;
const CHUNK_INDEX_HEADER_LEN: usize = 24;
const CHUNK_INDEX_ENTRY_LEN: usize = 42;
const CHUNK_INDEX_SERIES_RANGE_LEN: usize = 24;
const SERIES_HEADER_LEN: usize = 28;
const SERIES_ENTRY_LEN: usize = 24;
const SERIES_LABEL_PAIR_LEN: usize = 8;
pub(super) const POSTINGS_HEADER_LEN: usize = 16;
const POSTINGS_ENTRY_HEADER_LEN: usize = 20;
const MANIFEST_HEADER_LEN: usize = 80;
const MANIFEST_FILE_ENTRY_LEN: usize = 20;
pub(super) const MANIFEST_FILE_ENTRY_COUNT: usize = 4;
const MIN_CHUNK_RECORD_TOTAL_LEN: usize = 46;

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

fn persisted_count(raw: u64, context: &str) -> Result<usize> {
    usize::try_from(raw).map_err(|_| {
        TsinkError::DataCorruption(format!("{context} count {raw} does not fit this platform"))
    })
}

fn persisted_offset(raw: u64, context: &str) -> Result<usize> {
    usize::try_from(raw).map_err(|_| {
        TsinkError::DataCorruption(format!("{context} offset {raw} does not fit this platform"))
    })
}

fn checked_record_bytes(count: usize, record_len: usize, context: &str) -> Result<usize> {
    count
        .checked_mul(record_len)
        .ok_or_else(|| TsinkError::DataCorruption(format!("{context} byte length overflow")))
}

fn ensure_fixed_records_fit(
    bytes_len: usize,
    pos: usize,
    count: usize,
    record_len: usize,
    context: &str,
) -> Result<()> {
    let records_len = checked_record_bytes(count, record_len, context)?;
    let end = pos
        .checked_add(records_len)
        .ok_or_else(|| TsinkError::DataCorruption(format!("{context} end offset overflow")))?;
    if end > bytes_len {
        return Err(TsinkError::DataCorruption(format!(
            "{context} declares {count} records requiring {records_len} bytes, but only {} remain",
            bytes_len.saturating_sub(pos)
        )));
    }
    Ok(())
}

fn try_vec_with_capacity<T>(count: usize, context: &str) -> Result<Vec<T>> {
    let allocation_bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| TsinkError::DataCorruption(format!("{context} allocation overflow")))?;
    if allocation_bytes > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "{context} allocation {allocation_bytes} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}"
        )));
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|err| {
        TsinkError::Other(format!(
            "failed to reserve {count} entries while decoding {context}: {err}"
        ))
    })?;
    Ok(values)
}

fn ensure_chunks_file_size(bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_SEGMENT_CHUNKS_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "chunks.bin size {} exceeds the format safety limit {}",
            bytes.len(),
            MAX_SEGMENT_CHUNKS_FILE_BYTES
        )));
    }
    Ok(())
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
                u32::try_from(bytes.len().checked_sub(record_start).ok_or_else(|| {
                    TsinkError::InvalidConfiguration(
                        "chunk record length underflow in chunks.bin".to_string(),
                    )
                })?)
                .map_err(|_| {
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
        .checked_add(header_body.len())
        .and_then(|len| len.checked_add(payload.len()))
        .and_then(|len| len.checked_add(4))
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration("chunk record length overflow".to_string())
        })?;

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
    let pairs_offset_base = series_data
        .entries
        .len()
        .checked_mul(SERIES_ENTRY_LEN)
        .and_then(|entries_len| series_entry_offset.checked_add(entries_len))
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration("series entry table length overflow".to_string())
        })?;

    let mut pairs_bytes = Vec::new();

    for series in &series_data.entries {
        let pairs_offset = pairs_offset_base
            .checked_add(pairs_bytes.len())
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration("series label-pair offset overflow".to_string())
            })?;

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

    let series_count = u32::try_from(series_ids.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("posting list cardinality exceeds u32".to_string())
    })?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("encoded posting payload exceeds u32".to_string())
    })?;

    append_u8(bytes, kind);
    append_u8(bytes, 0u8);
    append_u16(bytes, 0u16);
    append_u32(bytes, primary_id);
    append_u32(bytes, secondary_id);
    append_u32(bytes, series_count);
    append_u32(bytes, payload_len);
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
    if bytes.len() > MAX_SEGMENT_MANIFEST_FILE_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "manifest.bin size {} exceeds the format safety limit {MAX_SEGMENT_MANIFEST_FILE_BYTES}",
            bytes.len()
        )));
    }
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
    let series_count = persisted_count(read_u64(bytes, &mut pos)?, "manifest series")?;
    let chunk_count = persisted_count(read_u64(bytes, &mut pos)?, "manifest chunk")?;
    let point_count = persisted_count(read_u64(bytes, &mut pos)?, "manifest point")?;
    let wal_highwater_segment = read_u64(bytes, &mut pos)?;
    let wal_highwater_frame = read_u64(bytes, &mut pos)?;
    let file_entry_count = usize::try_from(read_u32(bytes, &mut pos)?).map_err(|_| {
        TsinkError::DataCorruption("manifest file-entry count does not fit usize".to_string())
    })?;
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

    if pos.checked_add(4) != Some(bytes.len()) {
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
    parse_series_file_with_decoded_limit(bytes, MAX_DECODED_FRAMED_FILE_BYTES)
}

/// Validates every registry-rebuild-relevant reference and canonical table boundary in
/// `series.bin` without materializing dictionaries, labels, or `PersistedSeries` values.
///
/// This is used by legacy data-directory identity detection, where cloning shared dictionary
/// strings once per series would make startup memory proportional to the expanded registry rather
/// than the bounded file representation.
pub(super) fn validate_series_file_structure_with_decoded_limit(
    bytes: &[u8],
    max_decoded_bytes: usize,
) -> Result<usize> {
    let bytes = decode_optional_zstd_framed_file_with_limit(
        bytes,
        SERIES_MAGIC,
        FORMAT_VERSION,
        "series.bin",
        max_decoded_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES),
    )?;
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
    let series_count = persisted_count(read_u64(&bytes, &mut pos)?, "series.bin series")?;

    let dictionary_count = metric_count
        .checked_add(label_name_count)
        .and_then(|count| count.checked_add(label_value_count))
        .ok_or_else(|| {
            TsinkError::DataCorruption("series.bin dictionary count overflow".to_string())
        })?;
    let minimum_dictionary_bytes =
        checked_record_bytes(dictionary_count, 8, "series.bin dictionary headers")?;
    let minimum_series_bytes =
        checked_record_bytes(series_count, SERIES_ENTRY_LEN, "series.bin entries")?;
    let minimum_remaining = minimum_dictionary_bytes
        .checked_add(minimum_series_bytes)
        .ok_or_else(|| {
            TsinkError::DataCorruption("series.bin minimum length overflow".to_string())
        })?;
    if minimum_remaining > bytes.len().saturating_sub(pos) {
        return Err(TsinkError::DataCorruption(format!(
            "series.bin declared counts require at least {minimum_remaining} bytes, but only {} remain",
            bytes.len().saturating_sub(pos)
        )));
    }

    validate_dictionary_structure(&bytes, &mut pos, metric_count)?;
    validate_dictionary_structure(&bytes, &mut pos, label_name_count)?;
    validate_dictionary_structure(&bytes, &mut pos, label_value_count)?;

    let entries_bytes =
        checked_record_bytes(series_count, SERIES_ENTRY_LEN, "series.bin entry table")?;
    let entries_end = pos.checked_add(entries_bytes).ok_or_else(|| {
        TsinkError::DataCorruption("series.bin entry table end overflow".to_string())
    })?;
    if entries_end > bytes.len() {
        return Err(TsinkError::DataCorruption(
            "series.bin entry table exceeds file size".to_string(),
        ));
    }

    let has_value_family =
        flags & (SERIES_FLAG_VALUE_FAMILY | SERIES_FLAG_LEGACY_VALUE_FAMILY) != 0;
    let mut entry_pos = pos;
    let mut expected_pair_offset = entries_end;
    let mut previous_series_id = None;
    for _ in 0..series_count {
        let series_id = read_u64(&bytes, &mut entry_pos)?;
        if previous_series_id.is_some_and(|previous| series_id <= previous) {
            return Err(TsinkError::DataCorruption(format!(
                "series.bin series ids are not strictly increasing at {series_id}"
            )));
        }
        previous_series_id = Some(series_id);

        let lane = decode_lane(read_u8(&bytes, &mut entry_pos)?)?;
        if has_value_family {
            let value_family = decode_series_value_family(read_u8(&bytes, &mut entry_pos)?)?;
            let compatible = matches!(
                (lane, value_family),
                (
                    ValueLane::Numeric,
                    SeriesValueFamily::F64
                        | SeriesValueFamily::I64
                        | SeriesValueFamily::U64
                        | SeriesValueFamily::Bool
                ) | (
                    ValueLane::Blob,
                    SeriesValueFamily::Blob | SeriesValueFamily::Histogram
                )
            );
            if !compatible {
                return Err(TsinkError::DataCorruption(format!(
                    "series.bin series {series_id} has a value family incompatible with its lane"
                )));
            }
        } else {
            let _reserved = read_u8(&bytes, &mut entry_pos)?;
        }
        let pair_count = read_u16(&bytes, &mut entry_pos)? as usize;
        let metric_id = read_u32(&bytes, &mut entry_pos)? as usize;
        if metric_id >= metric_count {
            return Err(TsinkError::DataCorruption(format!(
                "series.bin series {series_id} references missing metric id {metric_id}"
            )));
        }
        let pair_offset =
            persisted_offset(read_u64(&bytes, &mut entry_pos)?, "series.bin label-pair")?;
        if pair_offset != expected_pair_offset {
            return Err(TsinkError::DataCorruption(format!(
                "series label-pair offset {pair_offset} is not canonical; expected {expected_pair_offset}"
            )));
        }
        let pair_bytes =
            checked_record_bytes(pair_count, SERIES_LABEL_PAIR_LEN, "series label-pair block")?;
        let pair_end = pair_offset.checked_add(pair_bytes).ok_or_else(|| {
            TsinkError::DataCorruption("series label-pair end offset overflow".to_string())
        })?;
        if pair_end > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "series label pair block exceeds file size".to_string(),
            ));
        }

        let mut pair_pos = pair_offset;
        let mut previous_name_id = None;
        for _ in 0..pair_count {
            let name_id = read_u32(&bytes, &mut pair_pos)? as usize;
            let value_id = read_u32(&bytes, &mut pair_pos)? as usize;
            if name_id >= label_name_count {
                return Err(TsinkError::DataCorruption(format!(
                    "series.bin series {series_id} references missing label-name id {name_id}"
                )));
            }
            if value_id >= label_value_count {
                return Err(TsinkError::DataCorruption(format!(
                    "series.bin series {series_id} references missing label-value id {value_id}"
                )));
            }
            if previous_name_id.is_some_and(|previous| name_id <= previous) {
                return Err(TsinkError::DataCorruption(format!(
                    "series.bin series {series_id} label-name ids are not strictly increasing"
                )));
            }
            previous_name_id = Some(name_id);
        }
        if pair_pos != pair_end {
            return Err(TsinkError::DataCorruption(
                "series label pair block length mismatch".to_string(),
            ));
        }
        expected_pair_offset = pair_end;
    }
    if entry_pos != entries_end {
        return Err(TsinkError::DataCorruption(
            "series.bin entry table length mismatch".to_string(),
        ));
    }
    if expected_pair_offset != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "series.bin has trailing or unreferenced label-pair bytes".to_string(),
        ));
    }
    Ok(series_count)
}

fn validate_dictionary_structure(bytes: &[u8], pos: &mut usize, count: usize) -> Result<()> {
    ensure_fixed_records_fit(bytes.len(), *pos, count, 8, "series dictionary headers")?;
    for expected_id in 0..count {
        let id = read_u32(bytes, pos)? as usize;
        if id != expected_id {
            return Err(TsinkError::DataCorruption(format!(
                "dictionary id {id} is not dense at expected {expected_id}"
            )));
        }
        let len = read_u32(bytes, pos)? as usize;
        let value = read_bytes(bytes, pos, len)?;
        std::str::from_utf8(value).map_err(|err| {
            TsinkError::DataCorruption(format!(
                "series.bin dictionary entry {expected_id} is not UTF-8: {err}"
            ))
        })?;
    }
    Ok(())
}

pub(super) fn parse_series_file_with_decoded_limit(
    bytes: &[u8],
    max_decoded_bytes: usize,
) -> Result<ParsedSeriesFile> {
    let bytes = decode_optional_zstd_framed_file_with_limit(
        bytes,
        SERIES_MAGIC,
        FORMAT_VERSION,
        "series.bin",
        max_decoded_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES),
    )?;
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
    let series_count = persisted_count(read_u64(&bytes, &mut pos)?, "series.bin series")?;

    let dictionary_count = metric_count
        .checked_add(label_name_count)
        .and_then(|count| count.checked_add(label_value_count))
        .ok_or_else(|| {
            TsinkError::DataCorruption("series.bin dictionary count overflow".to_string())
        })?;
    let minimum_dictionary_bytes =
        checked_record_bytes(dictionary_count, 8, "series.bin dictionary headers")?;
    let minimum_series_bytes =
        checked_record_bytes(series_count, SERIES_ENTRY_LEN, "series.bin entries")?;
    let minimum_remaining = minimum_dictionary_bytes
        .checked_add(minimum_series_bytes)
        .ok_or_else(|| {
            TsinkError::DataCorruption("series.bin minimum length overflow".to_string())
        })?;
    if minimum_remaining > bytes.len().saturating_sub(pos) {
        return Err(TsinkError::DataCorruption(format!(
            "series.bin declared counts require at least {minimum_remaining} bytes, but only {} remain",
            bytes.len().saturating_sub(pos)
        )));
    }

    let metrics = parse_dictionary(&bytes, &mut pos, metric_count)?;
    let label_names = parse_dictionary(&bytes, &mut pos, label_name_count)?;
    let label_values = parse_dictionary(&bytes, &mut pos, label_value_count)?;

    ensure_fixed_records_fit(
        bytes.len(),
        pos,
        series_count,
        SERIES_ENTRY_LEN,
        "series.bin entry table",
    )?;
    let mut entries_stub = try_vec_with_capacity(series_count, "series.bin entry table")?;
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
        let pair_offset = persisted_offset(read_u64(&bytes, &mut pos)?, "series.bin label-pair")?;

        entries_stub.push((
            series_id,
            metric_id,
            lane,
            value_family,
            label_pair_count,
            pair_offset,
        ));
    }

    let mut entries = try_vec_with_capacity(entries_stub.len(), "series.bin decoded entries")?;
    let mut expected_pair_offset = pos;
    for (series_id, metric_id, lane, value_family, pair_count, pair_offset) in entries_stub {
        if pair_offset != expected_pair_offset {
            return Err(TsinkError::DataCorruption(format!(
                "series label-pair offset {pair_offset} is not canonical; expected {expected_pair_offset}"
            )));
        }
        let pair_bytes =
            checked_record_bytes(pair_count, SERIES_LABEL_PAIR_LEN, "series label-pair block")?;
        let pair_end = pair_offset.checked_add(pair_bytes).ok_or_else(|| {
            TsinkError::DataCorruption("series label-pair end offset overflow".to_string())
        })?;
        if pair_end > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "series label pair block exceeds file size".to_string(),
            ));
        }

        let mut pair_pos = pair_offset;
        let mut label_pairs = try_vec_with_capacity(pair_count, "series.bin label-pair block")?;
        for _ in 0..pair_count {
            let name_id = read_u32(&bytes, &mut pair_pos)?;
            let value_id = read_u32(&bytes, &mut pair_pos)?;
            label_pairs.push(LabelPairId { name_id, value_id });
        }

        if pair_pos != pair_end {
            return Err(TsinkError::DataCorruption(
                "series label pair block length mismatch".to_string(),
            ));
        }
        expected_pair_offset = pair_end;

        entries.push(ParsedSeriesEntry {
            series_id,
            metric_id,
            lane,
            value_family,
            label_pairs,
        });
    }

    if expected_pair_offset != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "series.bin has trailing or unreferenced label-pair bytes".to_string(),
        ));
    }

    Ok(ParsedSeriesFile {
        metrics,
        label_names,
        label_values,
        entries,
    })
}

fn parse_dictionary(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<String>> {
    ensure_fixed_records_fit(bytes.len(), *pos, count, 8, "series dictionary headers")?;
    let mut values = try_vec_with_capacity(count, "series dictionary")?;
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
    let mut out = try_vec_with_capacity(parsed.entries.len(), "decoded persisted series")?;

    for entry in &parsed.entries {
        let Some(metric) = parsed.metrics.get(entry.metric_id as usize) else {
            return Err(TsinkError::DataCorruption(format!(
                "series {} metric id {} not found in dictionary",
                entry.series_id, entry.metric_id
            )));
        };

        let _lane = entry.lane;

        let mut labels =
            try_vec_with_capacity(entry.label_pairs.len(), "decoded persisted series labels")?;
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
    parse_postings_file_with_decoded_limit(bytes, parsed_series, MAX_DECODED_FRAMED_FILE_BYTES)
}

pub(super) fn parse_postings_file_with_decoded_limit(
    bytes: &[u8],
    parsed_series: &ParsedSeriesFile,
    max_decoded_bytes: usize,
) -> Result<SegmentPostingsIndex> {
    let bytes = decode_optional_zstd_framed_file_with_limit(
        bytes,
        POSTINGS_MAGIC,
        FORMAT_VERSION,
        "postings.bin",
        max_decoded_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES),
    )?;
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
    let postings_count = persisted_count(read_u64(&bytes, &mut pos)?, "postings.bin entry")?;
    ensure_fixed_records_fit(
        bytes.len(),
        pos,
        postings_count,
        POSTINGS_ENTRY_HEADER_LEN,
        "postings.bin entry headers",
    )?;
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
        if series_count > parsed_series.entries.len() {
            return Err(TsinkError::DataCorruption(format!(
                "posting list declares {series_count} series, exceeding the segment series count {}",
                parsed_series.entries.len()
            )));
        }
        let encoded_len = read_u32(&bytes, &mut pos)? as usize;
        let payload = read_bytes(&bytes, &mut pos, encoded_len)?;

        let mut bitmap_header_pos = 0usize;
        let tree_part_count = persisted_count(
            read_u64(payload, &mut bitmap_header_pos)?,
            "roaring treemap part",
        )?;
        if tree_part_count > series_count {
            return Err(TsinkError::DataCorruption(format!(
                "roaring posting declares {tree_part_count} high-key parts for {series_count} series"
            )));
        }

        let mut cursor = std::io::Cursor::new(payload);
        let bitmap = RoaringTreemap::deserialize_from(&mut cursor).map_err(|err| {
            TsinkError::DataCorruption(format!("failed to decode roaring posting payload: {err}"))
        })?;
        if usize::try_from(cursor.position()).ok() != Some(payload.len()) {
            return Err(TsinkError::DataCorruption(
                "roaring posting payload has trailing bytes".to_string(),
            ));
        }
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
    parse_chunk_index_file_with_decoded_limit(bytes, MAX_DECODED_FRAMED_FILE_BYTES)
}

pub(super) fn parse_chunk_index_file_with_decoded_limit(
    bytes: &[u8],
    max_decoded_bytes: usize,
) -> Result<ChunkIndex> {
    let bytes = decode_optional_zstd_framed_file_with_limit(
        bytes,
        CHUNK_INDEX_MAGIC,
        FORMAT_VERSION,
        "chunk_index.bin",
        max_decoded_bytes.min(MAX_DECODED_FRAMED_FILE_BYTES),
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
    let entry_count = persisted_count(read_u64(&bytes, &mut pos)?, "chunk_index.bin entry")?;
    let series_table_count =
        persisted_count(read_u64(&bytes, &mut pos)?, "chunk_index.bin series-range")?;

    let entry_bytes = checked_record_bytes(
        entry_count,
        CHUNK_INDEX_ENTRY_LEN,
        "chunk_index.bin entries",
    )?;
    let series_table_bytes = checked_record_bytes(
        series_table_count,
        CHUNK_INDEX_SERIES_RANGE_LEN,
        "chunk_index.bin series ranges",
    )?;
    let expected_len = CHUNK_INDEX_HEADER_LEN
        .checked_add(entry_bytes)
        .and_then(|len| len.checked_add(series_table_bytes))
        .ok_or_else(|| TsinkError::DataCorruption("chunk_index.bin length overflow".to_string()))?;
    if expected_len != bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk_index.bin declared tables require {expected_len} bytes, got {}",
            bytes.len()
        )));
    }

    let mut index = ChunkIndex {
        entries: try_vec_with_capacity(entry_count, "chunk_index.bin entries")?,
    };

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
        let first_entry =
            persisted_offset(read_u64(&bytes, &mut pos)?, "chunk_index.bin first-entry")?;
        let count = read_u32(&bytes, &mut pos)? as usize;
        let _reserved = read_u32(&bytes, &mut pos)?;

        if idx > 0 && series_id < prev_series {
            return Err(TsinkError::DataCorruption(
                "chunk index series range table is not sorted".to_string(),
            ));
        }
        prev_series = series_id;

        if first_entry
            .checked_add(count)
            .is_none_or(|end| end > entry_count)
        {
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

fn preflight_chunks_file_decode(bytes: &[u8], chunk_count: usize, mut pos: usize) -> Result<usize> {
    let mut total_decoded_payload_bytes = 0usize;

    for _ in 0..chunk_count {
        let record_len = read_u32(bytes, &mut pos)? as usize;
        if record_len < MIN_CHUNK_RECORD_TOTAL_LEN - 4 {
            return Err(TsinkError::DataCorruption(
                "chunk record is shorter than its fixed fields".to_string(),
            ));
        }
        let record_end = pos.checked_add(record_len).ok_or_else(|| {
            TsinkError::DataCorruption("chunk record end offset overflow".to_string())
        })?;
        if record_end > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "chunk record exceeds chunks.bin length".to_string(),
            ));
        }

        let header_crc32 = read_u32(bytes, &mut pos)?;
        let header_start = pos;
        let _series_id = read_u64(bytes, &mut pos)?;
        let _lane = decode_lane(read_u8(bytes, &mut pos)?)?;
        let _ts_codec = decode_ts_codec(read_u8(bytes, &mut pos)?)?;
        let _value_codec = decode_value_codec(read_u8(bytes, &mut pos)?)?;
        let chunk_flags = read_u8(bytes, &mut pos)?;
        let _point_count = read_u16(bytes, &mut pos)?;
        let _min_ts = read_i64(bytes, &mut pos)?;
        let _max_ts = read_i64(bytes, &mut pos)?;
        let payload_len = read_u32(bytes, &mut pos)? as usize;

        if checksum32(&bytes[header_start..pos]) != header_crc32 {
            return Err(TsinkError::DataCorruption(
                "chunk header crc mismatch".to_string(),
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

        let decoded_len = if chunk_payload_uses_zstd(chunk_flags)? {
            if payload.len() < CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES {
                return Err(TsinkError::DataCorruption(
                    "compressed chunk payload missing original length prefix".to_string(),
                ));
            }
            usize::try_from(read_u32_at(payload, 0)?).map_err(|_| {
                TsinkError::DataCorruption(
                    "compressed chunk decoded length does not fit this platform".to_string(),
                )
            })?
        } else {
            payload.len()
        };
        if decoded_len > MAX_DECODED_CHUNK_PAYLOAD_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "chunk payload decoded size {decoded_len} exceeds the format safety limit {MAX_DECODED_CHUNK_PAYLOAD_BYTES}"
            )));
        }
        total_decoded_payload_bytes = total_decoded_payload_bytes
            .checked_add(decoded_len)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "chunks.bin aggregate decoded payload length overflow".to_string(),
                )
            })?;
        if total_decoded_payload_bytes > MAX_SEGMENT_CHUNKS_FILE_BYTES {
            return Err(TsinkError::DataCorruption(format!(
                "chunks.bin aggregate decoded payload size {total_decoded_payload_bytes} exceeds the format safety limit {MAX_SEGMENT_CHUNKS_FILE_BYTES}"
            )));
        }
    }

    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "chunks.bin has trailing bytes".to_string(),
        ));
    }
    Ok(total_decoded_payload_bytes)
}

/// Validates chunk-file framing without decompressing payloads and returns the aggregate decoded
/// payload bytes declared by all records.
pub(crate) fn decoded_chunks_file_payload_bytes(bytes: &[u8]) -> Result<usize> {
    ensure_chunks_file_size(bytes)?;
    if bytes.len() < CHUNKS_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "chunks.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    if read_array::<4>(bytes, &mut pos)? != CHUNKS_MAGIC {
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
    let chunk_count = persisted_count(read_u64(bytes, &mut pos)?, "chunks.bin chunk")?;
    ensure_fixed_records_fit(
        bytes.len(),
        pos,
        chunk_count,
        MIN_CHUNK_RECORD_TOTAL_LEN,
        "chunks.bin minimum chunk records",
    )?;
    preflight_chunks_file_decode(bytes, chunk_count, pos)
}

pub(super) fn parse_chunks_file(bytes: &[u8]) -> Result<BTreeMap<u64, ChunkRecordMeta>> {
    ensure_chunks_file_size(bytes)?;
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
    let chunk_count = persisted_count(read_u64(bytes, &mut pos)?, "chunks.bin chunk")?;
    ensure_fixed_records_fit(
        bytes.len(),
        pos,
        chunk_count,
        MIN_CHUNK_RECORD_TOTAL_LEN,
        "chunks.bin minimum chunk records",
    )?;
    let _decoded_payload_bytes = preflight_chunks_file_decode(bytes, chunk_count, pos)?;

    let mut records = BTreeMap::new();

    for _ in 0..chunk_count {
        let record_offset = pos as u64;
        let record_len = read_u32(bytes, &mut pos)? as usize;
        let record_end = pos.checked_add(record_len).ok_or_else(|| {
            TsinkError::DataCorruption("chunk record end offset overflow".to_string())
        })?;
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

        let total_len = record_len
            .checked_add(4)
            .and_then(|len| u32::try_from(len).ok())
            .ok_or_else(|| {
                TsinkError::DataCorruption("chunk record total length exceeds u32".to_string())
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
                    wal_lowwater: WalHighWatermark::default(),
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
    ensure_chunks_file_size(bytes)?;
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
    let chunk_count = persisted_count(read_u64(bytes, &mut pos)?, "chunks.bin chunk")?;
    ensure_fixed_records_fit(
        bytes.len(),
        pos,
        chunk_count,
        MIN_CHUNK_RECORD_TOTAL_LEN,
        "chunks.bin minimum chunk records",
    )?;
    let _decoded_payload_bytes = preflight_chunks_file_decode(bytes, chunk_count, pos)?;

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
        let record_end = pos.checked_add(record_len).ok_or_else(|| {
            TsinkError::DataCorruption("chunk record end offset overflow".to_string())
        })?;
        if record_end > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "chunk record exceeds chunks.bin length".to_string(),
            ));
        }

        let total_len = record_len
            .checked_add(4)
            .and_then(|len| u32::try_from(len).ok())
            .ok_or_else(|| {
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
    if payload.len() > MAX_DECODED_CHUNK_PAYLOAD_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "chunk payload size {} exceeds the format safety limit {MAX_DECODED_CHUNK_PAYLOAD_BYTES}",
            payload.len()
        )));
    }
    let compressed =
        zstd::bulk::compress(payload, CHUNK_PAYLOAD_ZSTD_LEVEL_FAST).map_err(|err| {
            TsinkError::Compression(format!("zstd compress chunk payload failed: {err}"))
        })?;

    let original_len = u32::try_from(payload.len())
        .map_err(|_| TsinkError::InvalidConfiguration("chunk payload too large".to_string()))?;
    let wrapped_len = CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES
        .checked_add(compressed.len())
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration("compressed chunk payload length overflow".to_string())
        })?;
    let mut wrapped = Vec::with_capacity(wrapped_len);
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
    decompress_chunk_payload_zstd_with_limit(payload, MAX_DECODED_CHUNK_PAYLOAD_BYTES)
}

fn decompress_chunk_payload_zstd_with_limit(
    payload: &[u8],
    max_decoded_bytes: usize,
) -> Result<Vec<u8>> {
    if payload.len() < CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES {
        return Err(TsinkError::DataCorruption(
            "compressed chunk payload missing original length prefix".to_string(),
        ));
    }

    let expected_len = usize::try_from(read_u32_at(payload, 0)?).map_err(|_| {
        TsinkError::DataCorruption(
            "compressed chunk decoded length does not fit this platform".to_string(),
        )
    })?;
    let compressed = &payload[CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES..];
    decompress_zstd_exact_bounded(compressed, expected_len, max_decoded_bytes, "chunk payload")
}

fn decode_chunk_payload_from_storage(payload: &[u8], chunk_flags: u8) -> Result<Vec<u8>> {
    if chunk_payload_uses_zstd(chunk_flags)? {
        return decompress_chunk_payload_zstd(payload);
    }
    Ok(payload.to_vec())
}

fn chunk_payload_record_parts(
    bytes: &[u8],
    chunk_offset: u64,
    chunk_len: u32,
) -> Result<(&[u8], u8)> {
    ensure_chunks_file_size(bytes)?;
    let offset = usize::try_from(chunk_offset).map_err(|_| {
        TsinkError::DataCorruption(format!("chunk offset {chunk_offset} exceeds usize"))
    })?;
    let record_len = usize::try_from(chunk_len).map_err(|_| {
        TsinkError::DataCorruption(format!("chunk length {chunk_len} exceeds usize"))
    })?;
    let record_end = offset.checked_add(record_len).ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "chunk at offset {chunk_offset} has an overflowing record length {chunk_len}"
        ))
    })?;
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

    let body_len = usize::try_from(read_u32_at(record, 0)?).map_err(|_| {
        TsinkError::DataCorruption("chunk record body length does not fit usize".to_string())
    })?;
    if body_len.checked_add(4) != Some(record.len()) {
        return Err(TsinkError::DataCorruption(format!(
            "chunk record length mismatch at offset {}",
            chunk_offset
        )));
    }

    let payload_len = usize::try_from(read_u32_at(record, 38)?).map_err(|_| {
        TsinkError::DataCorruption("chunk payload length does not fit usize".to_string())
    })?;
    let payload_start = 42usize;
    let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "chunk payload length overflow at offset {chunk_offset}"
        ))
    })?;
    let chunk_flags = read_u8_at(record, 19)?;

    if payload_end.checked_add(4) != Some(record.len()) {
        return Err(TsinkError::DataCorruption(format!(
            "chunk payload length mismatch at offset {}",
            chunk_offset
        )));
    }
    validate_chunk_payload_flags(chunk_flags)?;
    Ok((&record[payload_start..payload_end], chunk_flags))
}

pub(crate) fn chunk_payload_decoded_len_from_record(
    bytes: &[u8],
    chunk_offset: u64,
    chunk_len: u32,
) -> Result<(usize, bool)> {
    let (payload, chunk_flags) = chunk_payload_record_parts(bytes, chunk_offset, chunk_len)?;
    let compressed = chunk_payload_uses_zstd(chunk_flags)?;
    let decoded_len = if compressed {
        if payload.len() < CHUNK_PAYLOAD_ZSTD_ORIGINAL_LEN_PREFIX_BYTES {
            return Err(TsinkError::DataCorruption(
                "compressed chunk payload missing original length prefix".to_string(),
            ));
        }
        usize::try_from(read_u32_at(payload, 0)?).map_err(|_| {
            TsinkError::DataCorruption(
                "compressed chunk decoded length does not fit this platform".to_string(),
            )
        })?
    } else {
        payload.len()
    };
    if decoded_len > MAX_DECODED_CHUNK_PAYLOAD_BYTES {
        return Err(TsinkError::DataCorruption(format!(
            "chunk payload decoded size {decoded_len} exceeds the format safety limit {MAX_DECODED_CHUNK_PAYLOAD_BYTES}"
        )));
    }
    Ok((decoded_len, compressed))
}

pub(crate) fn chunk_payload_from_record<'a>(
    bytes: &'a [u8],
    chunk_offset: u64,
    chunk_len: u32,
) -> Result<Cow<'a, [u8]>> {
    let (payload, chunk_flags) = chunk_payload_record_parts(bytes, chunk_offset, chunk_len)?;
    if chunk_payload_uses_zstd(chunk_flags)? {
        return Ok(Cow::Owned(decompress_chunk_payload_zstd(payload)?));
    }

    Ok(Cow::Borrowed(payload))
}

#[cfg(test)]
mod decode_limit_tests {
    use super::*;

    fn compressed_chunk_payload(body: &[u8], declared_len: u32) -> Vec<u8> {
        let compressed = zstd::bulk::compress(body, 1).unwrap();
        let mut payload = Vec::new();
        append_u32(&mut payload, declared_len);
        payload.extend_from_slice(&compressed);
        payload
    }

    #[test]
    fn chunk_zstd_decode_accepts_exact_limit_and_rejects_n_plus_one() {
        let body = vec![11u8; 128];
        let payload = compressed_chunk_payload(&body, body.len() as u32);
        assert_eq!(
            decompress_chunk_payload_zstd_with_limit(&payload, body.len()).unwrap(),
            body
        );

        let err = decompress_chunk_payload_zstd_with_limit(&payload, body.len() - 1).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("decoded size 128 exceeds the format safety limit 127")));
    }

    #[test]
    fn chunk_zstd_decode_rejects_huge_declaration_mismatch_and_truncation() {
        let mut huge = Vec::new();
        append_u32(&mut huge, u32::MAX);
        huge.push(0);
        let err = decompress_chunk_payload_zstd_with_limit(&huge, 1024).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds the format safety limit 1024")));

        let mismatch = compressed_chunk_payload(&[4u8; 32], 31);
        let err = decompress_chunk_payload_zstd_with_limit(&mismatch, 1024).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds declared length 31")));

        let mut truncated = compressed_chunk_payload(&[4u8; 32], 32);
        truncated.truncate(truncated.len() - 2);
        assert!(matches!(
            decompress_chunk_payload_zstd_with_limit(&truncated, 1024).unwrap_err(),
            TsinkError::Compression(_) | TsinkError::DataCorruption(_)
        ));
    }

    #[test]
    fn series_parser_rejects_impossible_counts_before_capacity_reservation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SERIES_MAGIC);
        append_u16(&mut bytes, FORMAT_VERSION);
        append_u16(&mut bytes, SERIES_FLAG_VALUE_FAMILY);
        append_u32(&mut bytes, u32::MAX);
        append_u32(&mut bytes, 0);
        append_u32(&mut bytes, 0);
        append_u64(&mut bytes, 0);

        let err = parse_series_file(&bytes).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("declared counts require at least")));
    }

    #[test]
    fn chunk_index_parser_rejects_huge_and_truncated_declared_tables() {
        for entry_count in [u64::MAX, 1] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&CHUNK_INDEX_MAGIC);
            append_u16(&mut bytes, FORMAT_VERSION);
            append_u16(&mut bytes, 0);
            append_u64(&mut bytes, entry_count);
            append_u64(&mut bytes, 0);

            let err = parse_chunk_index_file(&bytes).unwrap_err();
            assert!(matches!(err, TsinkError::DataCorruption(_)));
        }
    }

    #[test]
    fn chunks_parser_rejects_huge_declared_record_count_before_iteration() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CHUNKS_MAGIC);
        append_u16(&mut bytes, FORMAT_VERSION);
        append_u16(&mut bytes, 0);
        append_u64(&mut bytes, u64::MAX);

        let err = parse_chunks_file(&bytes).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(_)));
    }
}
