use std::collections::HashMap;

use roaring::RoaringTreemap;

use crate::engine::query::TieredQueryPlan;

use super::state::{PersistedChunkRef, PersistedSegmentState};
use super::{ChunkStorage, SeriesId};

#[derive(Default)]
pub(super) struct PersistedTimeRangePruneResult {
    pub(super) overlapping_series_ids: RoaringTreemap,
    pub(super) exact_scan_chunk_refs: HashMap<SeriesId, Vec<PersistedChunkRef>>,
}

fn bitmap_cardinality(bitmap: &RoaringTreemap) -> usize {
    usize::try_from(bitmap.len()).unwrap_or(usize::MAX)
}

fn persisted_segment_overlaps_time_range(
    segment: &PersistedSegmentState,
    start: i64,
    end: i64,
    plan: TieredQueryPlan,
) -> bool {
    let (Some(min_ts), Some(max_ts)) = (segment.manifest.min_ts, segment.manifest.max_ts) else {
        return false;
    };
    max_ts >= start
        && min_ts < end
        && ChunkStorage::plan_includes_persisted_tier(plan, segment.tier)
}

fn persisted_segment_candidate_series_ids_for_time_range(
    segment: &PersistedSegmentState,
    start: i64,
    end: i64,
) -> RoaringTreemap {
    segment
        .time_bucket_postings
        .as_ref()
        .map(|time_bucket_postings| time_bucket_postings.series_ids_in_time_range(start, end))
        .unwrap_or_else(|| {
            let mut series_ids = RoaringTreemap::new();
            for series_id in segment.chunk_refs_by_series.keys().copied() {
                series_ids.insert(series_id);
            }
            series_ids
        })
}

fn append_overlapping_persisted_chunk_refs(
    exact_scan_chunk_refs: &mut HashMap<SeriesId, Vec<PersistedChunkRef>>,
    segment: &PersistedSegmentState,
    series_id: SeriesId,
    start: i64,
    end: i64,
) -> bool {
    let Some(chunk_refs) = segment.chunk_refs_by_series.get(&series_id) else {
        return false;
    };

    let mut appended = false;
    for chunk_ref in chunk_refs {
        if chunk_ref.max_ts < start || chunk_ref.min_ts >= end {
            continue;
        }
        exact_scan_chunk_refs
            .entry(series_id)
            .or_default()
            .push(*chunk_ref);
        appended = true;
    }
    appended
}

pub(super) fn prune_persisted_exact_scan_candidates<'a>(
    segments: impl IntoIterator<Item = &'a PersistedSegmentState>,
    series_ids: &RoaringTreemap,
    start: i64,
    end: i64,
    plan: TieredQueryPlan,
) -> PersistedTimeRangePruneResult {
    if series_ids.is_empty() {
        return PersistedTimeRangePruneResult::default();
    }

    let mut result = PersistedTimeRangePruneResult::default();
    let candidate_count = bitmap_cardinality(series_ids);
    for segment in segments {
        if !persisted_segment_overlaps_time_range(segment, start, end, plan) {
            continue;
        }

        let mut overlapping_candidates =
            persisted_segment_candidate_series_ids_for_time_range(segment, start, end);
        overlapping_candidates &= series_ids;
        if overlapping_candidates.is_empty() {
            continue;
        }

        let mut overlapping_series_in_segment = RoaringTreemap::new();
        for series_id in overlapping_candidates {
            if append_overlapping_persisted_chunk_refs(
                &mut result.exact_scan_chunk_refs,
                segment,
                series_id,
                start,
                end,
            ) {
                overlapping_series_in_segment.insert(series_id);
            }
        }
        if overlapping_series_in_segment.is_empty() {
            continue;
        }

        result.overlapping_series_ids |= overlapping_series_in_segment;
        if result.exact_scan_chunk_refs.len() >= candidate_count {
            break;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use roaring::RoaringTreemap;

    use super::super::state::PersistedSegmentTimeBucketIndex;
    use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
    use super::*;
    use crate::engine::chunk::{TimestampCodecId, ValueCodecId, ValueLane};
    use crate::engine::segment::{SegmentManifest, WalHighWatermark};

    fn bitmap(series_ids: &[SeriesId]) -> RoaringTreemap {
        series_ids.iter().copied().collect()
    }

    fn chunk_ref(segment_slot: usize, min_ts: i64, max_ts: i64) -> PersistedChunkRef {
        PersistedChunkRef {
            level: 0,
            min_ts,
            max_ts,
            point_count: 1,
            sequence: 0,
            chunk_offset: 0,
            chunk_len: 0,
            lane: ValueLane::Numeric,
            ts_codec: TimestampCodecId::DeltaVarint,
            value_codec: ValueCodecId::GorillaXorF64,
            segment_slot,
        }
    }

    fn segment(
        slot: usize,
        tier: PersistedSegmentTier,
        min_ts: i64,
        max_ts: i64,
        chunk_refs_by_series: HashMap<SeriesId, Vec<PersistedChunkRef>>,
        time_bucket_members: Option<&[(SeriesId, i64, i64)]>,
    ) -> PersistedSegmentState {
        let time_bucket_postings = time_bucket_members.map(|members| {
            let mut index = PersistedSegmentTimeBucketIndex::new(min_ts, max_ts);
            for (series_id, series_min_ts, series_max_ts) in members {
                index.insert_series_range(*series_id, *series_min_ts, *series_max_ts);
            }
            index
        });

        PersistedSegmentState {
            segment_slot: slot,
            lane: SegmentLaneFamily::Numeric,
            tier,
            manifest: SegmentManifest {
                segment_id: slot as u64,
                level: 0,
                chunk_count: chunk_refs_by_series.values().map(Vec::len).sum(),
                point_count: 0,
                series_count: chunk_refs_by_series.len(),
                min_ts: Some(min_ts),
                max_ts: Some(max_ts),
                wal_highwater: WalHighWatermark::default(),
            },
            time_bucket_postings,
            series_time_summaries: HashMap::new(),
            chunk_refs_by_series,
        }
    }

    #[test]
    fn prune_result_stays_bounded_to_candidate_series_ids() {
        let series_id = 7;
        let spill_series_id = 9;
        let segment = segment(
            1,
            PersistedSegmentTier::Hot,
            0,
            100,
            HashMap::from([
                (series_id, vec![chunk_ref(1, 40, 50)]),
                (spill_series_id, vec![chunk_ref(1, 40, 50)]),
            ]),
            Some(&[(series_id, 40, 50), (spill_series_id, 40, 50)]),
        );
        let candidates = bitmap(&[series_id]);

        let result = prune_persisted_exact_scan_candidates(
            [&segment],
            &candidates,
            45,
            46,
            TieredQueryPlan::from_cutoffs(45, 46, None, None),
        );

        assert_eq!(
            result.overlapping_series_ids.iter().collect::<Vec<_>>(),
            vec![series_id],
        );
        assert_eq!(
            result
                .exact_scan_chunk_refs
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![series_id],
        );
    }

    #[test]
    fn prune_skips_segments_outside_the_query_tier_plan() {
        let hot_series = 1;
        let cold_series = 2;
        let hot_segment = segment(
            1,
            PersistedSegmentTier::Hot,
            0,
            100,
            HashMap::from([(hot_series, vec![chunk_ref(1, 40, 50)])]),
            Some(&[(hot_series, 40, 50)]),
        );
        let cold_segment = segment(
            2,
            PersistedSegmentTier::Cold,
            0,
            100,
            HashMap::from([(cold_series, vec![chunk_ref(2, 40, 50)])]),
            Some(&[(cold_series, 40, 50)]),
        );
        let candidates = bitmap(&[hot_series, cold_series]);

        let result = prune_persisted_exact_scan_candidates(
            [&hot_segment, &cold_segment],
            &candidates,
            45,
            46,
            TieredQueryPlan::from_cutoffs(45, 46, None, None),
        );

        assert_eq!(
            result.overlapping_series_ids.iter().collect::<Vec<_>>(),
            vec![hot_series],
        );
        assert_eq!(
            result
                .exact_scan_chunk_refs
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![hot_series],
        );
    }
}
