use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    load_segments, load_segments_for_level, LoadedSegment, PersistedSeries, SegmentWriter,
};
use crate::engine::series_registry::{SeriesId, SeriesRegistry};
use crate::{Result, TsinkError};

const DEFAULT_L0_TRIGGER: usize = 4;
const DEFAULT_L1_TRIGGER: usize = 4;
const DEFAULT_SOURCE_WINDOW_SEGMENTS: usize = 8;
const DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER: usize = 512;

type SeriesChunkRefs<'a> = HashMap<SeriesId, Vec<&'a Chunk>>;
type MergeSegmentsOutput<'a> = (Vec<PersistedSeries>, SeriesChunkRefs<'a>);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionRunStats {
    pub compacted: bool,
    pub source_level: Option<u8>,
    pub target_level: Option<u8>,
    pub source_segments: usize,
    pub output_segments: usize,
    pub source_chunks: usize,
    pub output_chunks: usize,
    pub source_points: usize,
    pub output_points: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionLevel {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone)]
pub struct Compactor {
    data_path: PathBuf,
    point_cap: usize,
    l0_trigger: usize,
    l1_trigger: usize,
    next_segment_id: Option<Arc<AtomicU64>>,
}

impl Compactor {
    pub fn new(data_path: impl AsRef<Path>, point_cap: usize) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: None,
        }
    }

    pub fn new_with_segment_id_allocator(
        data_path: impl AsRef<Path>,
        point_cap: usize,
        next_segment_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: Some(next_segment_id),
        }
    }

    pub fn compact_once(&self) -> Result<bool> {
        Ok(self.compact_once_with_stats()?.compacted)
    }

    pub fn compact_once_with_stats(&self) -> Result<CompactionRunStats> {
        if let Some(stats) =
            self.try_compact_level(CompactionLevel::L0, CompactionLevel::L1, self.l0_trigger)?
        {
            return Ok(stats);
        }

        if let Some(stats) =
            self.try_compact_level(CompactionLevel::L1, CompactionLevel::L2, self.l1_trigger)?
        {
            return Ok(stats);
        }

        Ok(CompactionRunStats::default())
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
    ) -> Result<Option<CompactionRunStats>> {
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        let mut segments = load_segments_for_level(&self.data_path, source_level)?;
        if segments.len() < 2 {
            return Ok(None);
        }

        segments.sort_by_key(|segment| segment.manifest.segment_id);

        let should_compact = segments.len() >= count_trigger || has_time_overlap(&segments);
        if !should_compact {
            return Ok(None);
        }

        let window =
            select_compaction_window(&segments, count_trigger, DEFAULT_SOURCE_WINDOW_SEGMENTS);
        if window.len() < 2 {
            return Ok(None);
        }

        let mut stats = self.compact_segments(target_level, &window)?;
        stats.compacted = true;
        stats.source_level = Some(source_level);
        stats.target_level = Some(target_level);
        Ok(Some(stats))
    }

    fn compact_segments(
        &self,
        target_level: u8,
        segments: &[&LoadedSegment],
    ) -> Result<CompactionRunStats> {
        let source_segments = segments.len();
        let (series, chunks_by_series) = collect_series_and_chunk_refs(segments)?;
        let source_chunks = chunks_by_series
            .values()
            .map(std::vec::Vec::len)
            .sum::<usize>();
        let source_points = chunks_by_series
            .values()
            .flat_map(|chunks| chunks.iter())
            .map(|chunk| chunk.header.point_count as usize)
            .sum::<usize>();

        if chunks_by_series.is_empty() {
            return Ok(CompactionRunStats {
                compacted: false,
                source_level: None,
                target_level: Some(target_level),
                source_segments,
                output_segments: 0,
                source_chunks,
                output_chunks: 0,
                source_points,
                output_points: 0,
            });
        }

        let mut registry = SeriesRegistry::new();
        for series_def in &series {
            registry.register_series_with_id(
                series_def.series_id,
                &series_def.metric,
                &series_def.labels,
            )?;
        }

        let output_segment_point_budget = self
            .point_cap
            .saturating_mul(DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER)
            .max(self.point_cap);
        let mut pending_chunks = HashMap::<SeriesId, Vec<Chunk>>::new();
        let mut pending_points = 0usize;
        let mut emitted_segments = 0usize;
        let mut emitted_chunks = 0usize;
        let mut emitted_points = 0usize;

        for series_def in &series {
            let series_id = series_def.series_id;
            let Some(chunks) = chunks_by_series.get(&series_id) else {
                continue;
            };

            stream_merge_series_chunks(series_id, chunks, self.point_cap, |chunk| {
                emitted_chunks = emitted_chunks.saturating_add(1);
                emitted_points = emitted_points.saturating_add(chunk.header.point_count as usize);
                pending_points = pending_points.saturating_add(chunk.header.point_count as usize);
                pending_chunks.entry(series_id).or_default().push(chunk);
                if pending_points >= output_segment_point_budget {
                    self.flush_compacted_segment(target_level, &registry, &pending_chunks)?;
                    pending_chunks.clear();
                    pending_points = 0;
                    emitted_segments = emitted_segments.saturating_add(1);
                }
                Ok(())
            })?;
        }

        if !pending_chunks.is_empty() {
            self.flush_compacted_segment(target_level, &registry, &pending_chunks)?;
            emitted_segments = emitted_segments.saturating_add(1);
        }

        if emitted_segments == 0 {
            return Ok(CompactionRunStats {
                compacted: false,
                source_level: None,
                target_level: Some(target_level),
                source_segments,
                output_segments: emitted_segments,
                source_chunks,
                output_chunks: emitted_chunks,
                source_points,
                output_points: emitted_points,
            });
        }

        for segment in segments {
            fs::remove_dir_all(&segment.root)?;
        }

        Ok(CompactionRunStats {
            compacted: true,
            source_level: None,
            target_level: Some(target_level),
            source_segments,
            output_segments: emitted_segments,
            source_chunks,
            output_chunks: emitted_chunks,
            source_points,
            output_points: emitted_points,
        })
    }

    fn flush_compacted_segment(
        &self,
        target_level: u8,
        registry: &SeriesRegistry,
        chunks_by_series: &HashMap<SeriesId, Vec<Chunk>>,
    ) -> Result<()> {
        if chunks_by_series.is_empty() {
            return Ok(());
        }

        let next_segment_id = match &self.next_segment_id {
            Some(next_segment_id) => next_segment_id.fetch_add(1, Ordering::SeqCst),
            None => load_segments(&self.data_path)?.next_segment_id,
        };
        let writer = SegmentWriter::new(&self.data_path, target_level, next_segment_id)?;
        writer.write_segment(registry, chunks_by_series)?;
        Ok(())
    }
}

fn collect_series_and_chunk_refs<'a>(
    segments: &[&'a LoadedSegment],
) -> Result<MergeSegmentsOutput<'a>> {
    let mut series_by_id = BTreeMap::<SeriesId, PersistedSeries>::new();
    let mut chunks_by_series = SeriesChunkRefs::new();

    for segment in segments {
        for series in &segment.series {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels => {}
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts during compaction",
                        series.series_id
                    )));
                }
                None => {
                    series_by_id.insert(series.series_id, series.clone());
                }
            }
        }

        for (series_id, chunks) in &segment.chunks_by_series {
            let entry = chunks_by_series.entry(*series_id).or_default();
            entry.extend(chunks.iter());
        }
    }

    Ok((series_by_id.into_values().collect(), chunks_by_series))
}

fn stream_merge_series_chunks<F>(
    series_id: SeriesId,
    chunks: &[&Chunk],
    point_cap: usize,
    mut emit_chunk: F,
) -> Result<()>
where
    F: FnMut(Chunk) -> Result<()>,
{
    let lane = infer_lane_for_refs(chunks)?;
    let point_cap = point_cap.max(1);

    let mut cursors = Vec::with_capacity(chunks.len());
    let mut heap = BinaryHeap::<MergeCursorKey>::new();

    for (chunk_order, chunk) in chunks.iter().enumerate() {
        let Some(cursor) = ChunkPointCursor::from_chunk(chunk_order, chunk)? else {
            continue;
        };

        let cursor_idx = cursors.len();
        let first_ts = cursor.current().map(|point| point.ts).unwrap_or_default();
        cursors.push(cursor);
        heap.push(MergeCursorKey {
            ts: first_ts,
            chunk_order,
            cursor_idx,
        });
    }

    if heap.is_empty() {
        return Ok(());
    }

    let mut pending_point: Option<ChunkPoint> = None;
    let mut chunk_points = Vec::with_capacity(point_cap);

    while let Some(key) = heap.pop() {
        let Some(cursor) = cursors.get_mut(key.cursor_idx) else {
            return Err(TsinkError::DataCorruption(
                "compaction cursor index out of bounds".to_string(),
            ));
        };

        let Some(point) = cursor.current().cloned() else {
            return Err(TsinkError::DataCorruption(
                "compaction cursor missing point".to_string(),
            ));
        };

        cursor.advance();
        if let Some(next_point) = cursor.current() {
            heap.push(MergeCursorKey {
                ts: next_point.ts,
                chunk_order: cursor.chunk_order,
                cursor_idx: key.cursor_idx,
            });
        }

        if pending_point
            .as_ref()
            .is_some_and(|pending| pending.ts == point.ts)
        {
            pending_point = Some(point);
            continue;
        }

        if let Some(previous) = pending_point.take() {
            chunk_points.push(previous);
            if chunk_points.len() >= point_cap {
                let points = std::mem::replace(&mut chunk_points, Vec::with_capacity(point_cap));
                emit_chunk(encode_compacted_chunk(series_id, lane, points)?)?;
            }
        }

        pending_point = Some(point);
    }

    if let Some(point) = pending_point.take() {
        chunk_points.push(point);
    }

    if !chunk_points.is_empty() {
        emit_chunk(encode_compacted_chunk(series_id, lane, chunk_points)?)?;
    }

    Ok(())
}

fn infer_lane_for_refs(chunks: &[&Chunk]) -> Result<ValueLane> {
    let Some(first) = chunks.first() else {
        return Ok(ValueLane::Numeric);
    };

    let expected = first.header.lane;
    for chunk in chunks {
        if chunk.header.lane != expected {
            return Err(TsinkError::DataCorruption(
                "series mixes numeric and blob lanes across chunks".to_string(),
            ));
        }
    }

    Ok(expected)
}

fn encode_compacted_chunk(
    series_id: SeriesId,
    lane: ValueLane,
    points: Vec<ChunkPoint>,
) -> Result<Chunk> {
    if points.is_empty() {
        return Err(TsinkError::DataCorruption(
            "attempted to encode empty compacted chunk".to_string(),
        ));
    }

    let encoded = Encoder::encode_chunk_points(&points, lane)?;
    let point_count = u16::try_from(points.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("compacted chunk point_count exceeds u16".to_string())
    })?;

    let min_ts = points.first().map(|point| point.ts).unwrap_or(0);
    let max_ts = points.last().map(|point| point.ts).unwrap_or(min_ts);

    Ok(Chunk {
        header: ChunkHeader {
            series_id,
            lane,
            point_count,
            min_ts,
            max_ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
    })
}

fn decode_chunk_points_for_compaction(chunk: &Chunk) -> Result<Vec<ChunkPoint>> {
    if !chunk.points.is_empty() {
        return Ok(chunk.points.clone());
    }

    if chunk.encoded_payload.is_empty() {
        return Ok(Vec::new());
    }

    Encoder::decode_chunk_points_from_payload(
        chunk.header.lane,
        chunk.header.ts_codec,
        chunk.header.value_codec,
        chunk.header.point_count as usize,
        &chunk.encoded_payload,
    )
}

fn has_time_overlap(segments: &[LoadedSegment]) -> bool {
    let mut ranges = segments
        .iter()
        .filter_map(|segment| {
            Some((
                segment.manifest.min_ts?,
                segment.manifest.max_ts?,
                segment.manifest.segment_id,
            ))
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return false;
    }

    ranges.sort_by_key(|(min_ts, _, segment_id)| (*min_ts, *segment_id));

    let mut current_max = ranges[0].1;
    for (min_ts, max_ts, _) in ranges.into_iter().skip(1) {
        if min_ts <= current_max {
            return true;
        }
        current_max = current_max.max(max_ts);
    }

    false
}

fn select_compaction_window(
    segments: &[LoadedSegment],
    count_trigger: usize,
    max_segments: usize,
) -> Vec<&LoadedSegment> {
    let max_segments = max_segments.max(2);
    if let Some(indexes) = overlapping_window_indexes(segments, max_segments) {
        return indexes
            .into_iter()
            .filter_map(|index| segments.get(index))
            .collect();
    }

    let window_len = count_trigger.max(2).min(max_segments).min(segments.len());
    segments.iter().take(window_len).collect()
}

#[derive(Debug, Clone, Copy)]
struct SegmentTimeRange {
    index: usize,
    segment_id: u64,
    min_ts: i64,
    max_ts: i64,
}

fn overlapping_window_indexes(
    segments: &[LoadedSegment],
    max_segments: usize,
) -> Option<Vec<usize>> {
    let mut ranges = segments
        .iter()
        .enumerate()
        .filter_map(|(index, segment)| {
            Some(SegmentTimeRange {
                index,
                segment_id: segment.manifest.segment_id,
                min_ts: segment.manifest.min_ts?,
                max_ts: segment.manifest.max_ts?,
            })
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return None;
    }

    ranges.sort_by_key(|range| (range.min_ts, range.segment_id));

    let mut cluster_start = 0usize;
    let mut cluster_max = ranges[0].max_ts;

    for idx in 1..ranges.len() {
        if ranges[idx].min_ts <= cluster_max {
            cluster_max = cluster_max.max(ranges[idx].max_ts);
            continue;
        }

        if idx.saturating_sub(cluster_start) >= 2 {
            return Some(select_cluster_indexes(
                &ranges[cluster_start..idx],
                max_segments,
            ));
        }

        cluster_start = idx;
        cluster_max = ranges[idx].max_ts;
    }

    if ranges.len().saturating_sub(cluster_start) >= 2 {
        return Some(select_cluster_indexes(
            &ranges[cluster_start..],
            max_segments,
        ));
    }

    None
}

fn select_cluster_indexes(cluster: &[SegmentTimeRange], max_segments: usize) -> Vec<usize> {
    let mut indexes = cluster.iter().map(|range| range.index).collect::<Vec<_>>();
    indexes.sort_unstable();
    indexes.truncate(max_segments.max(2));
    indexes
}

#[derive(Debug)]
struct ChunkPointCursor {
    chunk_order: usize,
    points: Vec<ChunkPoint>,
    point_idx: usize,
}

impl ChunkPointCursor {
    fn from_chunk(chunk_order: usize, chunk: &Chunk) -> Result<Option<Self>> {
        let points = decode_chunk_points_for_compaction(chunk)?;
        if points.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self {
            chunk_order,
            points,
            point_idx: 0,
        }))
    }

    fn current(&self) -> Option<&ChunkPoint> {
        self.points.get(self.point_idx)
    }

    fn advance(&mut self) {
        self.point_idx = self.point_idx.saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MergeCursorKey {
    ts: i64,
    chunk_order: usize,
    cursor_idx: usize,
}

impl PartialOrd for MergeCursorKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeCursorKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .ts
            .cmp(&self.ts)
            .then_with(|| other.chunk_order.cmp(&self.chunk_order))
            .then_with(|| other.cursor_idx.cmp(&self.cursor_idx))
    }
}

fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::Compactor;
    use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::{load_segments, load_segments_for_level, SegmentWriter};
    use crate::engine::series_registry::SeriesRegistry;
    use crate::{Label, Value};

    #[test]
    fn compacts_overlapping_l0_segments_into_l1() {
        let temp_dir = TempDir::new().unwrap();
        let mut registry = SeriesRegistry::new();

        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap()
            .series_id;

        let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
        let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

        let mut seg1_chunks = HashMap::new();
        seg1_chunks.insert(series_id, vec![first_chunk]);

        let mut seg2_chunks = HashMap::new();
        seg2_chunks.insert(series_id, vec![second_chunk]);

        SegmentWriter::new(temp_dir.path(), 0, 1)
            .unwrap()
            .write_segment(&registry, &seg1_chunks)
            .unwrap();

        SegmentWriter::new(temp_dir.path(), 0, 2)
            .unwrap()
            .write_segment(&registry, &seg2_chunks)
            .unwrap();

        let compactor = Compactor::new(temp_dir.path(), 8);
        compactor.compact_once().unwrap();

        let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
        let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

        assert!(l0.is_empty());
        assert_eq!(l1.len(), 1);

        let loaded = load_segments(temp_dir.path()).unwrap();
        let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].header.point_count, 4);
        let decoded = super::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
        assert_eq!(decoded[0].ts, 10);
        assert_eq!(decoded[3].ts, 30);
    }

    #[test]
    fn compactor_uses_shared_segment_id_allocator() {
        let temp_dir = TempDir::new().unwrap();
        let mut registry = SeriesRegistry::new();

        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap()
            .series_id;

        let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
        let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

        let mut seg1_chunks = HashMap::new();
        seg1_chunks.insert(series_id, vec![first_chunk]);

        let mut seg2_chunks = HashMap::new();
        seg2_chunks.insert(series_id, vec![second_chunk]);

        SegmentWriter::new(temp_dir.path(), 0, 1)
            .unwrap()
            .write_segment(&registry, &seg1_chunks)
            .unwrap();

        SegmentWriter::new(temp_dir.path(), 0, 2)
            .unwrap()
            .write_segment(&registry, &seg2_chunks)
            .unwrap();

        let next_segment_id = Arc::new(AtomicU64::new(100));
        let compactor = Compactor::new_with_segment_id_allocator(
            temp_dir.path(),
            8,
            Arc::clone(&next_segment_id),
        );
        compactor.compact_once().unwrap();

        let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].manifest.segment_id, 100);
        assert_eq!(next_segment_id.load(Ordering::SeqCst), 101);
    }

    #[test]
    fn compactor_limits_source_window_per_pass() {
        let temp_dir = TempDir::new().unwrap();
        let mut registry = SeriesRegistry::new();

        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap()
            .series_id;

        for segment_id in 1..=6 {
            let mut chunks = HashMap::new();
            chunks.insert(
                series_id,
                vec![make_numeric_chunk(
                    series_id,
                    &[(segment_id as i64, segment_id as f64)],
                )],
            );
            SegmentWriter::new(temp_dir.path(), 0, segment_id)
                .unwrap()
                .write_segment(&registry, &chunks)
                .unwrap();
        }

        let compactor = Compactor::new(temp_dir.path(), 8);
        compactor.compact_once().unwrap();

        let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
        let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

        assert_eq!(l0.len(), 2);
        assert_eq!(l1.len(), 1);
        assert_eq!(
            l0.iter()
                .map(|segment| segment.manifest.segment_id)
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
    }

    #[test]
    fn compactor_splits_large_output_into_multiple_segments() {
        let temp_dir = TempDir::new().unwrap();
        let mut registry = SeriesRegistry::new();

        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap()
            .series_id;

        for segment_id in 1..=4 {
            let start = (segment_id as i64 - 1) * 300;
            let points = (start..start + 300)
                .map(|ts| (ts, ts as f64))
                .collect::<Vec<_>>();

            let mut chunks = HashMap::new();
            chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);
            SegmentWriter::new(temp_dir.path(), 0, segment_id)
                .unwrap()
                .write_segment(&registry, &chunks)
                .unwrap();
        }

        let compactor = Compactor::new(temp_dir.path(), 2);
        compactor.compact_once().unwrap();

        let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
        let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
        assert!(l0.is_empty());
        assert!(l1.len() >= 2);
        assert!(l1.iter().all(|segment| {
            segment.manifest.point_count <= 2 * super::DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER
        }));

        let loaded = load_segments(temp_dir.path()).unwrap();
        let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
        let total_points = chunks
            .iter()
            .map(|chunk| chunk.header.point_count as usize)
            .sum::<usize>();
        assert_eq!(total_points, 1200);
    }

    fn make_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
        let points = points
            .iter()
            .map(|(ts, value)| ChunkPoint {
                ts: *ts,
                value: Value::F64(*value),
            })
            .collect::<Vec<_>>();

        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();

        Chunk {
            header: ChunkHeader {
                series_id,
                lane: ValueLane::Numeric,
                point_count: points.len() as u16,
                min_ts: points.first().unwrap().ts,
                max_ts: points.last().unwrap().ts,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points,
            encoded_payload: encoded.payload,
        }
    }
}
