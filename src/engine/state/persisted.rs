use std::sync::OnceLock;

use roaring::RoaringTreemap;

use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
use super::super::*;
use crate::engine::encoder::TimestampSearchIndex;
use crate::engine::segment::SegmentManifest;

const PERSISTED_SEGMENT_TIME_BUCKET_TARGET_COUNT: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::storage_engine) struct PersistedChunkRef {
    pub(in crate::engine::storage_engine) level: u8,
    pub(in crate::engine::storage_engine) min_ts: i64,
    pub(in crate::engine::storage_engine) max_ts: i64,
    pub(in crate::engine::storage_engine) point_count: u16,
    pub(in crate::engine::storage_engine) sequence: u64,
    pub(in crate::engine::storage_engine) chunk_offset: u64,
    pub(in crate::engine::storage_engine) chunk_len: u32,
    pub(in crate::engine::storage_engine) lane: ValueLane,
    pub(in crate::engine::storage_engine) ts_codec: chunk::TimestampCodecId,
    pub(in crate::engine::storage_engine) value_codec: chunk::ValueCodecId,
    pub(in crate::engine::storage_engine) segment_slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::storage_engine) struct PersistedSeriesTimeRangeSummary {
    pub(in crate::engine::storage_engine) min_ts: i64,
    pub(in crate::engine::storage_engine) max_ts: i64,
}

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct PersistedSegmentTimeBucketIndex {
    pub(in crate::engine::storage_engine) min_ts: i64,
    pub(in crate::engine::storage_engine) max_ts: i64,
    pub(in crate::engine::storage_engine) bucket_width: i64,
    pub(in crate::engine::storage_engine) buckets: Vec<RoaringTreemap>,
}

impl PersistedSegmentTimeBucketIndex {
    pub(in crate::engine::storage_engine) fn new(min_ts: i64, max_ts: i64) -> Self {
        let span = max_ts.saturating_sub(min_ts).saturating_add(1).max(1);
        let target = i64::try_from(PERSISTED_SEGMENT_TIME_BUCKET_TARGET_COUNT)
            .unwrap_or(i64::MAX)
            .max(1);
        let bucket_width = (span.saturating_add(target.saturating_sub(1)) / target).max(1);
        let bucket_count =
            usize::try_from(span.saturating_add(bucket_width.saturating_sub(1)) / bucket_width)
                .unwrap_or(PERSISTED_SEGMENT_TIME_BUCKET_TARGET_COUNT)
                .max(1);

        Self {
            min_ts,
            max_ts,
            bucket_width,
            buckets: vec![RoaringTreemap::new(); bucket_count],
        }
    }

    fn bucket_idx_for_timestamp(&self, timestamp: i64) -> usize {
        if self.buckets.len() <= 1 {
            return 0;
        }

        let clamped = timestamp.clamp(self.min_ts, self.max_ts);
        let offset = clamped.saturating_sub(self.min_ts);
        usize::try_from(offset / self.bucket_width)
            .unwrap_or(usize::MAX)
            .min(self.buckets.len().saturating_sub(1))
    }

    pub(in crate::engine::storage_engine) fn insert_series_range(
        &mut self,
        series_id: SeriesId,
        min_ts: i64,
        max_ts: i64,
    ) {
        if max_ts < self.min_ts || min_ts > self.max_ts {
            return;
        }

        let start_idx = self.bucket_idx_for_timestamp(min_ts);
        let end_idx = self.bucket_idx_for_timestamp(max_ts);
        for bucket in &mut self.buckets[start_idx..=end_idx] {
            bucket.insert(series_id);
        }
    }

    pub(in crate::engine::storage_engine) fn series_ids_in_time_range(
        &self,
        start: i64,
        end: i64,
    ) -> RoaringTreemap {
        if end <= self.min_ts || start > self.max_ts {
            return RoaringTreemap::new();
        }

        let start_idx = self.bucket_idx_for_timestamp(start.max(self.min_ts));
        let end_idx = self.bucket_idx_for_timestamp(end.saturating_sub(1).min(self.max_ts));
        let Some(first) = self.buckets.get(start_idx) else {
            return RoaringTreemap::new();
        };
        let mut merged = first.clone();
        if start_idx == end_idx {
            return merged;
        }
        for bucket in &self.buckets[start_idx.saturating_add(1)..=end_idx] {
            merged |= bucket;
        }
        merged
    }
}

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct PersistedSegmentState {
    pub(in crate::engine::storage_engine) segment_slot: usize,
    pub(in crate::engine::storage_engine) lane: SegmentLaneFamily,
    pub(in crate::engine::storage_engine) tier: PersistedSegmentTier,
    pub(in crate::engine::storage_engine) manifest: SegmentManifest,
    pub(in crate::engine::storage_engine) time_bucket_postings:
        Option<PersistedSegmentTimeBucketIndex>,
    pub(in crate::engine::storage_engine) series_time_summaries:
        HashMap<SeriesId, PersistedSeriesTimeRangeSummary>,
    pub(in crate::engine::storage_engine) chunk_refs_by_series:
        HashMap<SeriesId, Vec<PersistedChunkRef>>,
}

#[derive(Default)]
pub(in crate::engine::storage_engine) struct PersistedIndexState {
    pub(in crate::engine::storage_engine) chunk_refs: HashMap<SeriesId, Vec<PersistedChunkRef>>,
    pub(in crate::engine::storage_engine) chunk_timestamp_indexes:
        HashMap<u64, OnceLock<TimestampSearchIndex>>,
    pub(in crate::engine::storage_engine) segment_maps: HashMap<usize, Arc<PlatformMmap>>,
    pub(in crate::engine::storage_engine) segment_tiers: HashMap<usize, PersistedSegmentTier>,
    pub(in crate::engine::storage_engine) segments_by_root:
        BTreeMap<PathBuf, PersistedSegmentState>,
    pub(in crate::engine::storage_engine) merged_postings:
        crate::engine::segment::SegmentPostingsIndex,
    pub(in crate::engine::storage_engine) runtime_metadata_delta_series_ids: RoaringTreemap,
    pub(in crate::engine::storage_engine) next_segment_slot: usize,
    pub(in crate::engine::storage_engine) next_chunk_sequence: u64,
}
