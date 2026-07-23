use roaring::RoaringTreemap;

use crate::engine::{query::TieredQueryPlan, tombstone};

use super::time_range_planner::{
    prune_persisted_exact_scan_candidates, PersistedTimeRangePruneResult,
};
use super::{
    Chunk, ChunkStorage, PersistedChunkRef, PersistedIndexState, Result, SealedChunkKey, SeriesId,
    SeriesVisibilitySummary, TimeRangeFilterContext,
};
use crate::QueryExecution;

enum SeriesVisibilitySummaryDecision {
    Match,
    Reject,
    NeedsExactScan,
}

impl TimeRangeFilterContext<'_> {
    fn series_visibility_summary_decision_for_time_range(
        summary: &SeriesVisibilitySummary,
        start: i64,
        end: i64,
    ) -> SeriesVisibilitySummaryDecision {
        if summary
            .latest_visible_timestamp
            .is_none_or(|latest| latest < start)
        {
            return SeriesVisibilitySummaryDecision::Reject;
        }

        let mut overlaps_coarse = false;
        for range in &summary.ranges {
            if range.max_ts < start || range.min_ts >= end {
                continue;
            }
            if range.exact {
                return SeriesVisibilitySummaryDecision::Match;
            }
            overlaps_coarse = true;
        }

        if overlaps_coarse {
            return SeriesVisibilitySummaryDecision::NeedsExactScan;
        }
        if !summary.truncated_before_floor {
            return SeriesVisibilitySummaryDecision::Reject;
        }
        if summary
            .exhaustive_floor_inclusive
            .is_some_and(|floor| start >= floor)
        {
            return SeriesVisibilitySummaryDecision::Reject;
        }
        SeriesVisibilitySummaryDecision::NeedsExactScan
    }

    fn chunk_has_visible_timestamp_in_time_range(
        chunk: &Chunk,
        start: i64,
        end: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        execution: &QueryExecution,
    ) -> Result<bool> {
        if chunk.header.max_ts < start || chunk.header.min_ts >= end {
            return Ok(false);
        }

        if tombstone_ranges.is_none() && start <= chunk.header.min_ts && chunk.header.max_ts < end {
            return Ok(true);
        }
        if let Some(tombstone_ranges) = tombstone_ranges {
            if tombstone::interval_fully_tombstoned(
                chunk.header.min_ts,
                chunk.header.max_ts,
                tombstone_ranges,
            ) {
                return Ok(false);
            }
        }

        execution.checkpoint()?;
        execution.charge_samples_scanned(u64::from(chunk.header.point_count))?;
        Ok(ChunkStorage::latest_visible_timestamp_in_chunk(
            chunk,
            tombstone_ranges,
            start.saturating_sub(1),
            end.saturating_sub(1),
        )?
        .is_some())
    }

    fn persisted_chunk_has_visible_timestamp_in_time_range(
        self,
        persisted_index: &PersistedIndexState,
        chunk_ref: &PersistedChunkRef,
        start: i64,
        end: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        execution: &QueryExecution,
    ) -> Result<bool> {
        if chunk_ref.max_ts < start || chunk_ref.min_ts >= end {
            return Ok(false);
        }

        if tombstone_ranges.is_none() && start <= chunk_ref.min_ts && chunk_ref.max_ts < end {
            return Ok(true);
        }
        if let Some(tombstone_ranges) = tombstone_ranges {
            if tombstone::interval_fully_tombstoned(
                chunk_ref.min_ts,
                chunk_ref.max_ts,
                tombstone_ranges,
            ) {
                return Ok(false);
            }
        }

        execution.checkpoint()?;
        execution.charge_samples_scanned(u64::from(chunk_ref.point_count))?;
        Ok(self
            .ops
            .latest_visible_timestamp_in_persisted_chunk(
                persisted_index,
                chunk_ref,
                tombstone_ranges,
                start.saturating_sub(1),
                end.saturating_sub(1),
            )?
            .is_some())
    }

    fn series_has_visible_in_memory_data_in_time_range(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<bool> {
        execution.checkpoint()?;
        let _visibility_guard = self.visibility_read_fence();
        let tombstones = self.tombstones.read();
        let tombstone_ranges = tombstones.get(&series_id).map(Vec::as_slice);
        let active = self.chunks.active_shard(series_id).read();
        let sealed = self.chunks.sealed_shard(series_id).read();
        if let Some(chunks) = sealed.get(&series_id) {
            let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
            for (_, chunk) in chunks.range(..end_bound) {
                execution.checkpoint()?;
                if chunk.header.max_ts < start {
                    continue;
                }
                if Self::chunk_has_visible_timestamp_in_time_range(
                    chunk,
                    start,
                    end,
                    tombstone_ranges,
                    execution,
                )? {
                    return Ok(true);
                }
            }
        }

        let Some(state) = active.get(&series_id) else {
            return Ok(false);
        };
        for point in state.points_in_partition_order() {
            execution.checkpoint()?;
            execution.charge_samples_scanned(1)?;
            if point.ts >= start
                && point.ts < end
                && ChunkStorage::timestamp_survives_tombstones(point.ts, tombstone_ranges)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn prune_series_postings_without_persisted_segment_overlap_in_time_range(
        self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<PersistedTimeRangePruneResult> {
        execution.checkpoint()?;
        if series_ids.is_empty() {
            return Ok(PersistedTimeRangePruneResult::default());
        }

        let persisted_index = self.persisted_index.read();
        #[cfg(test)]
        let consulted_segments = persisted_index.segments_by_root.values().any(|segment| {
            let (Some(min_ts), Some(max_ts)) = (segment.manifest.min_ts, segment.manifest.max_ts)
            else {
                return false;
            };
            max_ts >= start
                && min_ts < end
                && ChunkStorage::plan_includes_persisted_tier(plan, segment.tier)
        });
        let result = prune_persisted_exact_scan_candidates(
            persisted_index.segments_by_root.values(),
            &series_ids,
            start,
            end,
            plan,
        );
        drop(persisted_index);
        execution.checkpoint()?;

        #[cfg(test)]
        if consulted_segments {
            self.ops.invoke_metadata_time_range_segment_prune_hook();
        }

        Ok(result)
    }

    fn series_has_visible_persisted_chunk_refs_in_time_range(
        self,
        series_id: SeriesId,
        chunk_refs: &[PersistedChunkRef],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<bool> {
        execution.checkpoint()?;
        let persisted_index = self.persisted_index.read();
        #[cfg(test)]
        if !chunk_refs.is_empty() {
            self.ops
                .invoke_metadata_time_range_persisted_exact_scan_hook();
        }

        let tombstones = self.tombstones.read();
        let tombstone_ranges = tombstones.get(&series_id).map(Vec::as_slice);
        for chunk_ref in chunk_refs {
            execution.checkpoint()?;
            if self.persisted_chunk_has_visible_timestamp_in_time_range(
                &persisted_index,
                chunk_ref,
                start,
                end,
                tombstone_ranges,
                execution,
            )? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(super) fn series_postings_with_data_in_time_range(
        self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        execution.checkpoint()?;
        if series_ids.is_empty() {
            return Ok(series_ids);
        }

        let effective_start = start.max(self.active_retention_cutoff.unwrap_or(i64::MIN));
        if effective_start >= end {
            return Ok(RoaringTreemap::new());
        }

        self.refresh_missing_visibility_summaries(&series_ids)?;

        #[cfg(test)]
        if !series_ids.is_empty() {
            self.ops.invoke_metadata_time_range_summary_hook();
        }

        let mut filtered = RoaringTreemap::new();
        let mut exact_scan_series_ids = RoaringTreemap::new();
        let mut control_error = None;
        self.visibility_cache.with_visibility_cache_state(
            |summaries: &std::collections::HashMap<SeriesId, SeriesVisibilitySummary>, _, _| {
                for series_id in series_ids {
                    if let Err(error) = execution.checkpoint() {
                        control_error = Some(error);
                        break;
                    }
                    let Some(summary) = summaries.get(&series_id) else {
                        continue;
                    };
                    match Self::series_visibility_summary_decision_for_time_range(
                        summary,
                        effective_start,
                        end,
                    ) {
                        SeriesVisibilitySummaryDecision::Match => {
                            filtered.insert(series_id);
                        }
                        SeriesVisibilitySummaryDecision::Reject => {}
                        SeriesVisibilitySummaryDecision::NeedsExactScan => {
                            exact_scan_series_ids.insert(series_id);
                        }
                    }
                }
            },
        );
        if let Some(error) = control_error {
            return Err(error.into());
        }

        let mut persisted_exact_scan_series_ids = RoaringTreemap::new();
        for series_id in exact_scan_series_ids {
            execution.checkpoint()?;
            if self.series_has_visible_in_memory_data_in_time_range(
                series_id,
                effective_start,
                end,
                execution,
            )? {
                filtered.insert(series_id);
            } else {
                persisted_exact_scan_series_ids.insert(series_id);
            }
        }

        for (series_id, chunk_refs) in self
            .prune_series_postings_without_persisted_segment_overlap_in_time_range(
                persisted_exact_scan_series_ids,
                effective_start,
                end,
                plan,
                execution,
            )?
            .exact_scan_chunk_refs
        {
            execution.checkpoint()?;
            if self.series_has_visible_persisted_chunk_refs_in_time_range(
                series_id,
                &chunk_refs,
                effective_start,
                end,
                execution,
            )? {
                filtered.insert(series_id);
            }
        }
        Ok(filtered)
    }
}

impl ChunkStorage {
    pub(super) fn series_postings_with_data_in_time_range(
        &self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        self.time_range_filter_context()
            .series_postings_with_data_in_time_range(series_ids, start, end, plan, execution)
    }

    #[cfg(test)]
    pub(in crate::engine) fn filter_series_ids_in_time_range_for_tests(
        &self,
        series_ids: Vec<SeriesId>,
        start: i64,
        end: i64,
    ) -> Result<Vec<SeriesId>> {
        let budget = crate::QueryBudget::new(crate::QueryBudgetLimits::default())
            .map_err(crate::QueryBudgetError::from)?;
        let execution = budget.begin_query()?;
        Ok(self
            .series_postings_with_data_in_time_range(
                series_ids.into_iter().collect(),
                start,
                end,
                self.query_tier_plan(start, end),
                &execution,
            )?
            .iter()
            .collect())
    }
}
