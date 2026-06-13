use std::collections::{BTreeMap, HashMap};
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

type SeriesChunks = HashMap<SeriesId, Vec<Chunk>>;
type MergeSegmentsOutput = (Vec<PersistedSeries>, SeriesChunks);

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

    pub fn compact_once(&self) -> Result<()> {
        if self.try_compact_level(CompactionLevel::L0, CompactionLevel::L1, self.l0_trigger)? {
            return Ok(());
        }

        let _ =
            self.try_compact_level(CompactionLevel::L1, CompactionLevel::L2, self.l1_trigger)?;
        Ok(())
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
    ) -> Result<bool> {
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        let mut segments = load_segments_for_level(&self.data_path, source_level)?;
        if segments.len() < 2 {
            return Ok(false);
        }

        segments.sort_by_key(|segment| segment.manifest.segment_id);

        let should_compact = segments.len() >= count_trigger || has_time_overlap(&segments);
        if !should_compact {
            return Ok(false);
        }

        self.compact_segments(source_level, target_level, &segments)?;
        Ok(true)
    }

    fn compact_segments(
        &self,
        _source_level: u8,
        target_level: u8,
        segments: &[LoadedSegment],
    ) -> Result<()> {
        let (series, chunks_by_series) = merge_segments(segments)?;
        if chunks_by_series.is_empty() {
            return Ok(());
        }

        let mut registry = SeriesRegistry::new();
        for series_def in &series {
            registry.register_series_with_id(
                series_def.series_id,
                &series_def.metric,
                &series_def.labels,
            )?;
        }

        let repacked = repack_chunks(chunks_by_series, self.point_cap)?;

        let next_segment_id = match &self.next_segment_id {
            Some(next_segment_id) => next_segment_id.fetch_add(1, Ordering::SeqCst),
            None => load_segments(&self.data_path)?.next_segment_id,
        };
        let writer = SegmentWriter::new(&self.data_path, target_level, next_segment_id)?;
        writer.write_segment(&registry, &repacked)?;

        for segment in segments {
            fs::remove_dir_all(&segment.root)?;
        }

        Ok(())
    }
}

fn merge_segments(segments: &[LoadedSegment]) -> Result<MergeSegmentsOutput> {
    let mut series_by_id = BTreeMap::<SeriesId, PersistedSeries>::new();
    let mut chunks_by_series = SeriesChunks::new();

    for segment in segments {
        for series in &segment.series {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels =>
                {
                    // no-op
                }
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
            chunks_by_series
                .entry(*series_id)
                .or_default()
                .extend(chunks.clone());
        }
    }

    Ok((series_by_id.into_values().collect(), chunks_by_series))
}

fn repack_chunks(mut chunks_by_series: SeriesChunks, point_cap: usize) -> Result<SeriesChunks> {
    let mut out = HashMap::new();

    for (series_id, chunks) in chunks_by_series.drain() {
        let lane = infer_lane(&chunks)?;

        let mut all_points = Vec::new();
        for chunk in chunks {
            all_points.extend(decode_chunk_points_for_compaction(&chunk)?);
        }

        if all_points.is_empty() {
            continue;
        }

        all_points.sort_by_key(|point| point.ts);
        let deduped = dedupe_last_value_per_timestamp(all_points);

        let mut series_chunks = Vec::new();
        for points in deduped.chunks(point_cap.max(1)) {
            if points.is_empty() {
                continue;
            }

            let encoded = Encoder::encode_chunk_points(points, lane)?;
            let point_count = u16::try_from(points.len()).map_err(|_| {
                TsinkError::InvalidConfiguration(
                    "compacted chunk point_count exceeds u16".to_string(),
                )
            })?;

            let min_ts = points.first().map(|point| point.ts).unwrap_or(0);
            let max_ts = points.last().map(|point| point.ts).unwrap_or(min_ts);

            series_chunks.push(Chunk {
                header: ChunkHeader {
                    series_id,
                    lane,
                    point_count,
                    min_ts,
                    max_ts,
                    ts_codec: encoded.ts_codec,
                    value_codec: encoded.value_codec,
                },
                points: points.to_vec(),
                encoded_payload: encoded.payload,
            });
        }

        if !series_chunks.is_empty() {
            out.insert(series_id, series_chunks);
        }
    }

    Ok(out)
}

fn infer_lane(chunks: &[Chunk]) -> Result<ValueLane> {
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

fn dedupe_last_value_per_timestamp(points: Vec<ChunkPoint>) -> Vec<ChunkPoint> {
    let mut out: Vec<ChunkPoint> = Vec::with_capacity(points.len());

    for point in points {
        if let Some(last) = out.last_mut() {
            if last.ts == point.ts {
                *last = point;
                continue;
            }
        }

        out.push(point);
    }

    out
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
