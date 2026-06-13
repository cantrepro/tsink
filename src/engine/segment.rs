use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crate::engine::chunk::{Chunk, ChunkHeader, TimestampCodecId, ValueCodecId, ValueLane};
use crate::engine::index::{ChunkIndex, ChunkIndexEntry};
use crate::engine::series_registry::{LabelPairId, SeriesId, SeriesRegistry};
use crate::{Label, Result, TsinkError};

const MANIFEST_MAGIC: [u8; 4] = *b"TSM2";
const CHUNKS_MAGIC: [u8; 4] = *b"CHK2";
const CHUNK_INDEX_MAGIC: [u8; 4] = *b"CID2";
const SERIES_MAGIC: [u8; 4] = *b"SRS2";
const POSTINGS_MAGIC: [u8; 4] = *b"PST2";

const FORMAT_VERSION: u16 = 1;
const FILE_KIND_CHUNKS: u8 = 1;
const FILE_KIND_CHUNK_INDEX: u8 = 2;
const FILE_KIND_SERIES: u8 = 3;
const FILE_KIND_POSTINGS: u8 = 4;

const CHUNKS_HEADER_LEN: usize = 16;
const CHUNK_INDEX_HEADER_LEN: usize = 24;
const SERIES_HEADER_LEN: usize = 28;
const POSTINGS_HEADER_LEN: usize = 16;
const MANIFEST_HEADER_LEN: usize = 80;
const MANIFEST_FILE_ENTRY_LEN: usize = 20;
const MANIFEST_FILE_ENTRY_COUNT: u32 = 4;

type BuildChunksAndIndexOutput = (Vec<u8>, ChunkIndex, usize, usize, Option<i64>, Option<i64>);
type SegmentPostings = BTreeMap<LabelPairId, BTreeSet<SeriesId>>;
type BuildSeriesFileOutput = (Vec<u8>, SegmentPostings, usize);

#[derive(Debug, Clone)]
pub struct SegmentLayout {
    pub root: PathBuf,
    pub chunks_path: PathBuf,
    pub chunk_index_path: PathBuf,
    pub series_path: PathBuf,
    pub postings_path: PathBuf,
    pub manifest_path: PathBuf,
}

impl SegmentLayout {
    pub fn new(base: impl AsRef<Path>, level: u8, segment_id: u64) -> Self {
        let root = base
            .as_ref()
            .join("segments")
            .join(format!("L{level}"))
            .join(format!("seg-{segment_id:016x}"));
        Self {
            chunks_path: root.join("chunks.bin"),
            chunk_index_path: root.join("chunk_index.bin"),
            series_path: root.join("series.bin"),
            postings_path: root.join("postings.bin"),
            manifest_path: root.join("manifest.bin"),
            root,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentManifest {
    pub segment_id: u64,
    pub level: u8,
    pub chunk_count: usize,
    pub point_count: usize,
    pub series_count: usize,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PersistedSeries {
    pub series_id: SeriesId,
    pub metric: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Default)]
pub struct LoadedSegments {
    pub next_segment_id: u64,
    pub series: Vec<PersistedSeries>,
    pub chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

#[derive(Debug, Clone)]
pub struct LoadedSegment {
    pub root: PathBuf,
    pub manifest: SegmentManifest,
    pub series: Vec<PersistedSeries>,
    pub chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

#[derive(Debug)]
pub struct SegmentWriter {
    layout: SegmentLayout,
    segment_id: u64,
    level: u8,
}

#[derive(Debug, Clone)]
struct ChunkRecordMeta {
    len: u32,
    chunk: Chunk,
}

#[derive(Debug, Clone)]
struct ManifestFileEntry {
    kind: u8,
    file_len: u64,
    hash64: u64,
}

#[derive(Debug, Clone)]
struct ParsedManifest {
    segment_id: u64,
    level: u8,
    chunk_count: usize,
    point_count: usize,
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    files: [ManifestFileEntry; 4],
}

#[derive(Debug, Clone)]
struct ParsedSeriesEntry {
    series_id: SeriesId,
    metric_id: u32,
    lane: ValueLane,
    label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone)]
struct ParsedSeriesFile {
    metrics: Vec<String>,
    label_names: Vec<String>,
    label_values: Vec<String>,
    entries: Vec<ParsedSeriesEntry>,
}

impl SegmentWriter {
    pub fn new(base: impl AsRef<Path>, level: u8, segment_id: u64) -> Result<Self> {
        let layout = SegmentLayout::new(base, level, segment_id);
        fs::create_dir_all(&layout.root)?;
        Ok(Self {
            layout,
            segment_id,
            level,
        })
    }

    pub fn layout(&self) -> &SegmentLayout {
        &self.layout
    }

    pub fn write_segment(
        &self,
        registry: &SeriesRegistry,
        chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
    ) -> Result<SegmentManifest> {
        let (chunks_bytes, mut chunk_index, chunk_count, point_count, min_ts, max_ts) =
            build_chunks_and_index(self.level, chunks_by_series)?;
        let (series_bytes, postings, series_count) = build_series_file(registry, chunks_by_series)?;
        let postings_bytes = build_postings_file(&postings)?;
        let chunk_index_bytes = build_chunk_index_file(&mut chunk_index)?;

        let data_files = [
            (&self.layout.chunks_path, chunks_bytes),
            (&self.layout.chunk_index_path, chunk_index_bytes),
            (&self.layout.series_path, series_bytes),
            (&self.layout.postings_path, postings_bytes),
        ];

        for (path, bytes) in &data_files {
            write_tmp_and_sync(path, bytes)?;
        }

        for (path, _) in &data_files {
            rename_tmp(path)?;
        }

        let manifest = SegmentManifest {
            segment_id: self.segment_id,
            level: self.level,
            chunk_count,
            point_count,
            series_count,
            min_ts,
            max_ts,
        };

        let manifest_bytes = build_manifest_file(&self.layout, &manifest)?;
        write_tmp_and_sync(&self.layout.manifest_path, &manifest_bytes)?;
        rename_tmp(&self.layout.manifest_path)?;

        // Best effort directory sync for crash safety.
        if let Ok(dir) = File::open(&self.layout.root) {
            let _ = dir.sync_all();
        }

        Ok(manifest)
    }
}

pub fn load_segments(base: impl AsRef<Path>) -> Result<LoadedSegments> {
    let dirs = collect_segment_dirs(base.as_ref(), 0..=2u8)?;

    let mut parsed = Vec::new();
    let mut max_segment_id = 0u64;

    for dir in dirs {
        match load_segment_dir(&dir) {
            Ok(segment) => {
                max_segment_id = max_segment_id.max(segment.manifest.segment_id);
                parsed.push(segment);
            }
            Err(TsinkError::DataCorruption(msg)) => {
                warn!(path = %dir.display(), error = %msg, "Ignoring invalid segment directory");
            }
            Err(err) => return Err(err),
        }
    }

    parsed.sort_by_key(|segment| (segment.manifest.level, segment.manifest.segment_id));

    let mut series_by_id: BTreeMap<SeriesId, PersistedSeries> = BTreeMap::new();
    let mut chunks_by_series: HashMap<SeriesId, Vec<Chunk>> = HashMap::new();

    for segment in parsed {
        for series in segment.series {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels =>
                {
                    // no-op
                }
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts across segments",
                        series.series_id
                    )));
                }
                None => {
                    series_by_id.insert(series.series_id, series);
                }
            }
        }

        for (series_id, mut chunks) in segment.chunks_by_series {
            chunks_by_series
                .entry(series_id)
                .or_default()
                .append(&mut chunks);
        }
    }

    for chunks in chunks_by_series.values_mut() {
        chunks.sort_by(|a, b| {
            (a.header.min_ts, a.header.max_ts, a.header.point_count).cmp(&(
                b.header.min_ts,
                b.header.max_ts,
                b.header.point_count,
            ))
        });
    }

    Ok(LoadedSegments {
        next_segment_id: max_segment_id.saturating_add(1).max(1),
        series: series_by_id.into_values().collect(),
        chunks_by_series,
    })
}

pub fn load_segments_for_level(base: impl AsRef<Path>, level: u8) -> Result<Vec<LoadedSegment>> {
    let dirs = collect_segment_dirs(base.as_ref(), std::iter::once(level))?;
    let mut out = Vec::new();

    for dir in dirs {
        match load_segment_dir(&dir) {
            Ok(segment) => out.push(LoadedSegment {
                root: segment.root,
                manifest: segment.manifest,
                series: segment.series,
                chunks_by_series: segment.chunks_by_series,
            }),
            Err(TsinkError::DataCorruption(msg)) => {
                warn!(
                    path = %dir.display(),
                    error = %msg,
                    "Ignoring invalid segment directory"
                );
            }
            Err(err) => return Err(err),
        }
    }

    out.sort_by_key(|segment| segment.manifest.segment_id);
    Ok(out)
}

fn collect_segment_dirs(base: &Path, levels: impl IntoIterator<Item = u8>) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for level in levels {
        let level_root = base.join("segments").join(format!("L{level}"));
        let Ok(read_dir) = fs::read_dir(&level_root) else {
            continue;
        };

        for entry in read_dir {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let path = entry.path();
            if !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("seg-"))
            {
                continue;
            }

            dirs.push(path);
        }
    }

    Ok(dirs)
}

#[derive(Debug)]
struct ParsedSegment {
    root: PathBuf,
    manifest: SegmentManifest,
    series: Vec<PersistedSeries>,
    chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

fn load_segment_dir(path: &Path) -> Result<ParsedSegment> {
    let layout = SegmentLayout {
        root: path.to_path_buf(),
        chunks_path: path.join("chunks.bin"),
        chunk_index_path: path.join("chunk_index.bin"),
        series_path: path.join("series.bin"),
        postings_path: path.join("postings.bin"),
        manifest_path: path.join("manifest.bin"),
    };

    if !layout.manifest_path.exists() {
        return Err(TsinkError::DataCorruption(
            "missing manifest.bin".to_string(),
        ));
    }

    let manifest_bytes = fs::read(&layout.manifest_path)?;
    let parsed_manifest = parse_manifest(&manifest_bytes)?;

    let chunks_bytes = fs::read(&layout.chunks_path)?;
    let chunk_index_bytes = fs::read(&layout.chunk_index_path)?;
    let series_bytes = fs::read(&layout.series_path)?;
    let postings_bytes = fs::read(&layout.postings_path)?;

    verify_file_manifest_entry(&parsed_manifest.files[0], FILE_KIND_CHUNKS, &chunks_bytes)?;
    verify_file_manifest_entry(
        &parsed_manifest.files[1],
        FILE_KIND_CHUNK_INDEX,
        &chunk_index_bytes,
    )?;
    verify_file_manifest_entry(&parsed_manifest.files[2], FILE_KIND_SERIES, &series_bytes)?;
    verify_file_manifest_entry(
        &parsed_manifest.files[3],
        FILE_KIND_POSTINGS,
        &postings_bytes,
    )?;

    let parsed_series = parse_series_file(&series_bytes)?;
    let series = decode_persisted_series(&parsed_series)?;

    // Parse for corruption detection even though startup path rebuilds postings from series labels.
    parse_postings_file(&postings_bytes)?;

    let chunk_index = parse_chunk_index_file(&chunk_index_bytes)?;
    let chunk_records = parse_chunks_file(&chunks_bytes)?;

    let mut chunks_by_series: HashMap<SeriesId, Vec<Chunk>> = HashMap::new();
    for entry in chunk_index.entries {
        let Some(meta) = chunk_records.get(&entry.chunk_offset) else {
            return Err(TsinkError::DataCorruption(format!(
                "chunk index references missing chunk offset {}",
                entry.chunk_offset
            )));
        };

        if meta.len != entry.chunk_len {
            return Err(TsinkError::DataCorruption(format!(
                "chunk length mismatch at offset {}: index {}, chunk {}",
                entry.chunk_offset, entry.chunk_len, meta.len
            )));
        }

        if meta.chunk.header.series_id != entry.series_id
            || meta.chunk.header.min_ts != entry.min_ts
            || meta.chunk.header.max_ts != entry.max_ts
            || meta.chunk.header.point_count != entry.point_count
            || meta.chunk.header.lane != entry.lane
            || meta.chunk.header.ts_codec != entry.ts_codec
            || meta.chunk.header.value_codec != entry.value_codec
        {
            return Err(TsinkError::DataCorruption(
                "chunk index entry does not match chunk header".to_string(),
            ));
        }

        chunks_by_series
            .entry(entry.series_id)
            .or_default()
            .push(meta.chunk.clone());
    }

    for chunks in chunks_by_series.values_mut() {
        chunks.sort_by(|a, b| {
            (a.header.min_ts, a.header.max_ts, a.header.point_count).cmp(&(
                b.header.min_ts,
                b.header.max_ts,
                b.header.point_count,
            ))
        });
    }

    let manifest = SegmentManifest {
        segment_id: parsed_manifest.segment_id,
        level: parsed_manifest.level,
        chunk_count: parsed_manifest.chunk_count,
        point_count: parsed_manifest.point_count,
        series_count: parsed_manifest.series_count,
        min_ts: parsed_manifest.min_ts,
        max_ts: parsed_manifest.max_ts,
    };

    Ok(ParsedSegment {
        root: path.to_path_buf(),
        manifest,
        series,
        chunks_by_series,
    })
}

fn build_chunks_and_index(
    level: u8,
    chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
) -> Result<BuildChunksAndIndexOutput> {
    let mut series_ids = chunks_by_series.keys().copied().collect::<Vec<_>>();
    series_ids.sort_unstable();

    let mut chunk_count = 0usize;
    let mut point_count = 0usize;
    let mut min_ts: Option<i64> = None;
    let mut max_ts: Option<i64> = None;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CHUNKS_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());

    let mut index = ChunkIndex::default();

    for series_id in series_ids {
        let Some(chunks) = chunks_by_series.get(&series_id) else {
            continue;
        };

        let mut ordered = chunks.clone();
        ordered.sort_by(|a, b| {
            (a.header.min_ts, a.header.max_ts, a.header.point_count).cmp(&(
                b.header.min_ts,
                b.header.max_ts,
                b.header.point_count,
            ))
        });

        for chunk in ordered {
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
            append_chunk_record(&mut bytes, &chunk)?;
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

    bytes[8..16].copy_from_slice(&(chunk_count as u64).to_le_bytes());

    Ok((bytes, index, chunk_count, point_count, min_ts, max_ts))
}

fn append_chunk_record(out: &mut Vec<u8>, chunk: &Chunk) -> Result<()> {
    let payload = &chunk.encoded_payload;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| TsinkError::InvalidConfiguration("chunk payload too large".to_string()))?;

    let mut header_body = Vec::with_capacity(34);
    header_body.extend_from_slice(&chunk.header.series_id.to_le_bytes());
    header_body.push(chunk.header.lane as u8);
    header_body.push(chunk.header.ts_codec as u8);
    header_body.push(chunk.header.value_codec as u8);
    header_body.push(0u8);
    header_body.extend_from_slice(&chunk.header.point_count.to_le_bytes());
    header_body.extend_from_slice(&chunk.header.min_ts.to_le_bytes());
    header_body.extend_from_slice(&chunk.header.max_ts.to_le_bytes());
    header_body.extend_from_slice(&payload_len.to_le_bytes());

    let header_crc32 = checksum32(&header_body);
    let payload_crc32 = checksum32(payload);

    let record_len = 4usize
        .saturating_add(header_body.len())
        .saturating_add(payload.len())
        .saturating_add(4);

    let record_len_u32 = u32::try_from(record_len).map_err(|_| {
        TsinkError::InvalidConfiguration("chunk record exceeds u32 length".to_string())
    })?;

    out.extend_from_slice(&record_len_u32.to_le_bytes());
    out.extend_from_slice(&header_crc32.to_le_bytes());
    out.extend_from_slice(&header_body);
    out.extend_from_slice(payload);
    out.extend_from_slice(&payload_crc32.to_le_bytes());

    Ok(())
}

fn build_chunk_index_file(index: &mut ChunkIndex) -> Result<Vec<u8>> {
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
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&(index.entries.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&(series_ranges.len() as u64).to_le_bytes());

    for entry in &index.entries {
        bytes.extend_from_slice(&entry.series_id.to_le_bytes());
        bytes.extend_from_slice(&entry.min_ts.to_le_bytes());
        bytes.extend_from_slice(&entry.max_ts.to_le_bytes());
        bytes.extend_from_slice(&entry.chunk_offset.to_le_bytes());
        bytes.extend_from_slice(&entry.chunk_len.to_le_bytes());
        bytes.extend_from_slice(&entry.point_count.to_le_bytes());
        bytes.push(entry.lane as u8);
        bytes.push(entry.ts_codec as u8);
        bytes.push(entry.value_codec as u8);
        bytes.push(entry.level);
    }

    for (series_id, first_entry_index, count) in series_ranges {
        bytes.extend_from_slice(&series_id.to_le_bytes());
        bytes.extend_from_slice(&first_entry_index.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
    }

    Ok(bytes)
}

fn build_series_file(
    registry: &SeriesRegistry,
    chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
) -> Result<BuildSeriesFileOutput> {
    #[derive(Debug, Clone)]
    struct SegmentSeriesEntry {
        series_id: SeriesId,
        lane: ValueLane,
        metric_id: u32,
        label_pairs: Vec<LabelPairId>,
    }

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
    let mut postings = BTreeMap::<LabelPairId, BTreeSet<SeriesId>>::new();

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
        let mut label_pairs = Vec::with_capacity(series_key.labels.len());

        for label in &series_key.labels {
            let name_id = intern_dict(&mut label_name_ids, &mut label_name_values, &label.name)?;
            let value_id =
                intern_dict(&mut label_value_ids, &mut label_value_values, &label.value)?;
            label_pairs.push(LabelPairId { name_id, value_id });
        }
        label_pairs.sort_unstable();
        label_pairs.dedup();

        for pair in &label_pairs {
            postings.entry(*pair).or_default().insert(series_id);
        }

        series_entries.push(SegmentSeriesEntry {
            series_id,
            lane,
            metric_id,
            label_pairs,
        });
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SERIES_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&(metric_values.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(label_name_values.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(label_value_values.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(series_entries.len() as u64).to_le_bytes());

    for (id, value) in metric_values.iter().enumerate() {
        write_dict_entry(&mut bytes, id as u32, value)?;
    }
    for (id, value) in label_name_values.iter().enumerate() {
        write_dict_entry(&mut bytes, id as u32, value)?;
    }
    for (id, value) in label_value_values.iter().enumerate() {
        write_dict_entry(&mut bytes, id as u32, value)?;
    }

    let series_entry_offset = bytes.len();
    let pairs_offset_base = series_entry_offset + series_entries.len().saturating_mul(24);

    let mut pairs_bytes = Vec::new();

    for series in &series_entries {
        let pairs_offset = pairs_offset_base + pairs_bytes.len();

        let label_pair_count = u16::try_from(series.label_pairs.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("series label pair count exceeds u16".to_string())
        })?;

        bytes.extend_from_slice(&series.series_id.to_le_bytes());
        bytes.push(series.lane as u8);
        bytes.push(0u8);
        bytes.extend_from_slice(&label_pair_count.to_le_bytes());
        bytes.extend_from_slice(&series.metric_id.to_le_bytes());
        bytes.extend_from_slice(&(pairs_offset as u64).to_le_bytes());

        for pair in &series.label_pairs {
            pairs_bytes.extend_from_slice(&pair.name_id.to_le_bytes());
            pairs_bytes.extend_from_slice(&pair.value_id.to_le_bytes());
        }
    }

    bytes.extend_from_slice(&pairs_bytes);
    Ok((bytes, postings, series_entries.len()))
}

fn write_dict_entry(out: &mut Vec<u8>, id: u32, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| TsinkError::InvalidConfiguration("dictionary string too large".to_string()))?;

    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn infer_series_lane(
    series_id: SeriesId,
    chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
) -> ValueLane {
    chunks_by_series
        .get(&series_id)
        .and_then(|chunks| chunks.first())
        .map(|chunk| chunk.header.lane)
        .unwrap_or(ValueLane::Numeric)
}

fn build_postings_file(postings: &BTreeMap<LabelPairId, BTreeSet<SeriesId>>) -> Result<Vec<u8>> {
    let postings = postings.iter().collect::<Vec<_>>();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&POSTINGS_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&(postings.len() as u64).to_le_bytes());

    for (pair, series_ids) in postings {
        let deltas = encode_series_id_deltas(series_ids.iter().copied());
        let series_count = u32::try_from(series_ids.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("posting list exceeds u32".to_string())
        })?;
        let encoded_len = u32::try_from(deltas.len()).map_err(|_| {
            TsinkError::InvalidConfiguration("posting payload exceeds u32".to_string())
        })?;

        bytes.extend_from_slice(&pair.name_id.to_le_bytes());
        bytes.extend_from_slice(&pair.value_id.to_le_bytes());
        bytes.extend_from_slice(&series_count.to_le_bytes());
        bytes.extend_from_slice(&encoded_len.to_le_bytes());
        bytes.extend_from_slice(&deltas);
    }

    Ok(bytes)
}

fn encode_series_id_deltas<I>(ids: I) -> Vec<u8>
where
    I: IntoIterator<Item = SeriesId>,
{
    let mut out = Vec::new();
    let mut prev = 0u64;

    for id in ids {
        let delta = id.saturating_sub(prev);
        encode_uvarint(delta, &mut out);
        prev = id;
    }

    out
}

fn build_manifest_file(layout: &SegmentLayout, manifest: &SegmentManifest) -> Result<Vec<u8>> {
    let chunks_bytes = fs::read(&layout.chunks_path)?;
    let chunk_index_bytes = fs::read(&layout.chunk_index_path)?;
    let series_bytes = fs::read(&layout.series_path)?;
    let postings_bytes = fs::read(&layout.postings_path)?;

    let file_entries = [
        ManifestFileEntry {
            kind: FILE_KIND_CHUNKS,
            file_len: chunks_bytes.len() as u64,
            hash64: hash64(&chunks_bytes),
        },
        ManifestFileEntry {
            kind: FILE_KIND_CHUNK_INDEX,
            file_len: chunk_index_bytes.len() as u64,
            hash64: hash64(&chunk_index_bytes),
        },
        ManifestFileEntry {
            kind: FILE_KIND_SERIES,
            file_len: series_bytes.len() as u64,
            hash64: hash64(&series_bytes),
        },
        ManifestFileEntry {
            kind: FILE_KIND_POSTINGS,
            file_len: postings_bytes.len() as u64,
            hash64: hash64(&postings_bytes),
        },
    ];

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&MANIFEST_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&manifest.segment_id.to_le_bytes());
    bytes.push(manifest.level);
    bytes.extend_from_slice(&[0u8; 7]);
    bytes.extend_from_slice(&manifest.min_ts.unwrap_or(0).to_le_bytes());
    bytes.extend_from_slice(&manifest.max_ts.unwrap_or(0).to_le_bytes());

    let created_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    bytes.extend_from_slice(&created_unix_ns.to_le_bytes());

    bytes.extend_from_slice(&(manifest.series_count as u64).to_le_bytes());
    bytes.extend_from_slice(&(manifest.chunk_count as u64).to_le_bytes());
    bytes.extend_from_slice(&(manifest.point_count as u64).to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&MANIFEST_FILE_ENTRY_COUNT.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());

    for entry in &file_entries {
        bytes.push(entry.kind);
        bytes.push(0u8);
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&entry.file_len.to_le_bytes());
        bytes.extend_from_slice(&entry.hash64.to_le_bytes());
    }

    let crc = checksum32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());

    Ok(bytes)
}

fn parse_manifest(bytes: &[u8]) -> Result<ParsedManifest> {
    if bytes.len() < MANIFEST_HEADER_LEN + (MANIFEST_FILE_ENTRY_LEN * 4) + 4 {
        return Err(TsinkError::DataCorruption(
            "manifest.bin is too short".to_string(),
        ));
    }

    let expected_crc = {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(&bytes[bytes.len() - 4..]);
        u32::from_le_bytes(raw)
    };
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
    let _wal_highwater_segment = read_u64(bytes, &mut pos)?;
    let _wal_highwater_frame = read_u64(bytes, &mut pos)?;
    let file_entry_count = read_u32(bytes, &mut pos)?;
    let _reserved1 = read_u32(bytes, &mut pos)?;

    if file_entry_count != MANIFEST_FILE_ENTRY_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "manifest file entry count {} is not {}",
            file_entry_count, MANIFEST_FILE_ENTRY_COUNT
        )));
    }

    let mut files = Vec::with_capacity(MANIFEST_FILE_ENTRY_COUNT as usize);
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

    let files: [ManifestFileEntry; 4] = files
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
        files,
    })
}

fn verify_file_manifest_entry(
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

fn parse_series_file(bytes: &[u8]) -> Result<ParsedSeriesFile> {
    if bytes.len() < SERIES_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "series.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != SERIES_MAGIC {
        return Err(TsinkError::DataCorruption(
            "series.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported series.bin version {version}"
        )));
    }

    let _flags = read_u16(bytes, &mut pos)?;
    let metric_count = read_u32(bytes, &mut pos)? as usize;
    let label_name_count = read_u32(bytes, &mut pos)? as usize;
    let label_value_count = read_u32(bytes, &mut pos)? as usize;
    let series_count = read_u64(bytes, &mut pos)? as usize;

    let metrics = parse_dictionary(bytes, &mut pos, metric_count)?;
    let label_names = parse_dictionary(bytes, &mut pos, label_name_count)?;
    let label_values = parse_dictionary(bytes, &mut pos, label_value_count)?;

    let mut entries_stub = Vec::with_capacity(series_count);
    for _ in 0..series_count {
        let series_id = read_u64(bytes, &mut pos)?;
        let lane = decode_lane(read_u8(bytes, &mut pos)?)?;
        let _reserved = read_u8(bytes, &mut pos)?;
        let label_pair_count = read_u16(bytes, &mut pos)? as usize;
        let metric_id = read_u32(bytes, &mut pos)?;
        let pair_offset = read_u64(bytes, &mut pos)? as usize;

        entries_stub.push((series_id, metric_id, lane, label_pair_count, pair_offset));
    }

    let mut entries = Vec::with_capacity(entries_stub.len());
    for (series_id, metric_id, lane, pair_count, pair_offset) in entries_stub {
        let mut pair_pos = pair_offset;
        let mut label_pairs = Vec::with_capacity(pair_count);
        for _ in 0..pair_count {
            let name_id = read_u32(bytes, &mut pair_pos)?;
            let value_id = read_u32(bytes, &mut pair_pos)?;
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

fn decode_persisted_series(parsed: &ParsedSeriesFile) -> Result<Vec<PersistedSeries>> {
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
        });
    }

    Ok(out)
}

fn parse_postings_file(bytes: &[u8]) -> Result<()> {
    if bytes.len() < POSTINGS_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "postings.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != POSTINGS_MAGIC {
        return Err(TsinkError::DataCorruption(
            "postings.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported postings.bin version {version}"
        )));
    }

    let _flags = read_u16(bytes, &mut pos)?;
    let postings_count = read_u64(bytes, &mut pos)? as usize;

    for _ in 0..postings_count {
        let _label_name_id = read_u32(bytes, &mut pos)?;
        let _label_value_id = read_u32(bytes, &mut pos)?;
        let series_count = read_u32(bytes, &mut pos)? as usize;
        let encoded_len = read_u32(bytes, &mut pos)? as usize;
        let payload = read_bytes(bytes, &mut pos, encoded_len)?;

        let mut payload_pos = 0usize;
        let mut prev = 0u64;
        for _ in 0..series_count {
            let delta = decode_uvarint(payload, &mut payload_pos)?;
            let id = prev.saturating_add(delta);
            prev = id;
        }

        if payload_pos != payload.len() {
            return Err(TsinkError::DataCorruption(
                "posting list payload has trailing bytes".to_string(),
            ));
        }
    }

    if pos != bytes.len() {
        return Err(TsinkError::DataCorruption(
            "postings.bin has trailing bytes".to_string(),
        ));
    }

    Ok(())
}

fn parse_chunk_index_file(bytes: &[u8]) -> Result<ChunkIndex> {
    if bytes.len() < CHUNK_INDEX_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "chunk_index.bin is too short".to_string(),
        ));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != CHUNK_INDEX_MAGIC {
        return Err(TsinkError::DataCorruption(
            "chunk_index.bin magic mismatch".to_string(),
        ));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != FORMAT_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported chunk_index.bin version {version}"
        )));
    }

    let _flags = read_u16(bytes, &mut pos)?;
    let entry_count = read_u64(bytes, &mut pos)? as usize;
    let series_table_count = read_u64(bytes, &mut pos)? as usize;

    let mut index = ChunkIndex::default();

    for _ in 0..entry_count {
        let series_id = read_u64(bytes, &mut pos)?;
        let min_ts = read_i64(bytes, &mut pos)?;
        let max_ts = read_i64(bytes, &mut pos)?;
        let chunk_offset = read_u64(bytes, &mut pos)?;
        let chunk_len = read_u32(bytes, &mut pos)?;
        let point_count = read_u16(bytes, &mut pos)?;
        let lane = decode_lane(read_u8(bytes, &mut pos)?)?;
        let ts_codec = decode_ts_codec(read_u8(bytes, &mut pos)?)?;
        let value_codec = decode_value_codec(read_u8(bytes, &mut pos)?)?;
        let level = read_u8(bytes, &mut pos)?;

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
        let series_id = read_u64(bytes, &mut pos)?;
        let first_entry = read_u64(bytes, &mut pos)? as usize;
        let count = read_u32(bytes, &mut pos)? as usize;
        let _reserved = read_u32(bytes, &mut pos)?;

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

fn parse_chunks_file(bytes: &[u8]) -> Result<BTreeMap<u64, ChunkRecordMeta>> {
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
        let _chunk_flags = read_u8(bytes, &mut pos)?;
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

        let payload = read_bytes(bytes, &mut pos, payload_len)?.to_vec();
        let payload_crc32 = read_u32(bytes, &mut pos)?;
        if checksum32(&payload) != payload_crc32 {
            return Err(TsinkError::DataCorruption(
                "chunk payload crc mismatch".to_string(),
            ));
        }

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
                        point_count,
                        min_ts,
                        max_ts,
                        ts_codec,
                        value_codec,
                    },
                    points: Vec::new(),
                    encoded_payload: payload,
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

fn decode_lane(raw: u8) -> Result<ValueLane> {
    match raw {
        0 => Ok(ValueLane::Numeric),
        1 => Ok(ValueLane::Blob),
        _ => Err(TsinkError::DataCorruption(format!(
            "invalid value lane {raw}"
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

fn write_tmp_and_sync(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = tmp_path_for(path);
    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn rename_tmp(path: &Path) -> Result<()> {
    let tmp_path = tmp_path_for(path);
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    path.with_file_name(format!("{file_name}.tmp"))
}

fn hash64(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(bytes, 0)
}

fn checksum32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
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

fn read_u8(bytes: &[u8], pos: &mut usize) -> Result<u8> {
    let byte = *bytes.get(*pos).ok_or_else(|| {
        TsinkError::DataCorruption("payload truncated while reading u8".to_string())
    })?;
    *pos += 1;
    Ok(byte)
}

fn read_u16(bytes: &[u8], pos: &mut usize) -> Result<u16> {
    let mut raw = [0u8; 2];
    raw.copy_from_slice(read_bytes(bytes, pos, 2)?);
    Ok(u16::from_le_bytes(raw))
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(read_bytes(bytes, pos, 4)?);
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(read_bytes(bytes, pos, 8)?);
    Ok(u64::from_le_bytes(raw))
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> Result<i64> {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(read_bytes(bytes, pos, 8)?);
    Ok(i64::from_le_bytes(raw))
}

fn read_array<const N: usize>(bytes: &[u8], pos: &mut usize) -> Result<[u8; N]> {
    let mut raw = [0u8; N];
    raw.copy_from_slice(read_bytes(bytes, pos, N)?);
    Ok(raw)
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

    let out = &bytes[*pos..end];
    *pos = end;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::{load_segments, SegmentWriter};
    use crate::engine::chunk::{Chunk, ChunkPoint, ValueLane};
    use crate::engine::encoder::Encoder;
    use crate::engine::series_registry::SeriesRegistry;
    use crate::{Label, Value};

    #[test]
    fn segment_roundtrip_preserves_series_and_chunks() {
        let tmp = TempDir::new().unwrap();

        let mut registry = SeriesRegistry::new();
        let series = registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap();

        let points = vec![
            ChunkPoint {
                ts: 10,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 20,
                value: Value::F64(2.0),
            },
        ];
        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        let chunk = Chunk {
            header: crate::engine::chunk::ChunkHeader {
                series_id: series.series_id,
                lane: ValueLane::Numeric,
                point_count: points.len() as u16,
                min_ts: 10,
                max_ts: 20,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points,
            encoded_payload: encoded.payload,
        };

        let mut chunks_by_series = HashMap::new();
        chunks_by_series.insert(series.series_id, vec![chunk]);

        let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
        writer.write_segment(&registry, &chunks_by_series).unwrap();

        let loaded = load_segments(tmp.path()).unwrap();
        assert_eq!(loaded.series.len(), 1);
        assert_eq!(loaded.series[0].metric, "cpu");
        assert_eq!(loaded.next_segment_id, 2);
        assert_eq!(loaded.chunks_by_series.len(), 1);
    }
}
