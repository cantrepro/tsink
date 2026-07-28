use super::metadata_context::QueryPlanningContext;
use super::{
    ChunkContext, ChunkStorage, DataPoint, Label, MetricSeries, PersistedChunkRef,
    PersistedIndexState, PersistedTierFetchStats, RawSeriesPagination, RawSeriesScanPage, Result,
    RoaringTreemap, RollupQueryCandidate, RwLock, RwLockReadGuard, SeriesId, SeriesRegistry,
    TieredQueryPlan, VisibilityCacheReadContext,
};
use crate::{QueryExecution, QueryMemoryReservation};

#[allow(clippy::too_many_arguments)]
trait SeriesQueryReadOps {
    fn select_into_impl(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats>;

    fn select_raw_series_page_with_plan(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        pagination: RawSeriesPagination,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<RawSeriesScanPage>;

    fn collect_points_for_series_into_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats>;

    fn collect_points_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<(Vec<DataPoint>, PersistedTierFetchStats)>;

    fn collect_raw_series_page_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        offset: u64,
        limit: Option<usize>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<RawSeriesScanPage>;

    fn rollup_query_candidate(
        &self,
        metric: &str,
        labels: &[Label],
        interval: i64,
        aggregation: crate::Aggregation,
        start: i64,
        end: i64,
    ) -> Option<RollupQueryCandidate>;

    fn record_rollup_query_use(&self, points_read: usize, partial: bool);
}

#[derive(Clone, Copy)]
pub(super) struct SeriesQueryContext<'a> {
    planning: QueryPlanningContext<'a>,
    registry: &'a RwLock<SeriesRegistry>,
    visibility_fence: &'a RwLock<()>,
    ops: &'a dyn SeriesQueryReadOps,
}

pub(super) struct ResolvedMetricSeriesBatch {
    pub(super) series: Vec<(MetricSeries, Option<SeriesId>)>,
    // Keep the reservation after the protected vector so the vector is destroyed first.
    _query_reservation: QueryMemoryReservation,
}

impl<'a> SeriesQueryContext<'a> {
    pub(super) fn query_tier_plan(self, start: i64, end: i64) -> TieredQueryPlan {
        self.planning.query_tier_plan(start, end)
    }

    pub(super) fn visibility_read_fence(self) -> RwLockReadGuard<'a, ()> {
        self.visibility_fence.read()
    }

    pub(super) fn resolve_series_batch(
        self,
        series: &[MetricSeries],
    ) -> Vec<(MetricSeries, Option<SeriesId>)> {
        let registry = self.registry.read();
        let mut resolved = Vec::with_capacity(series.len());
        for item in series {
            let series_id = registry
                .resolve_existing(&item.name, &item.labels)
                .map(|resolution| resolution.series_id);
            resolved.push((item.clone(), series_id));
        }
        resolved
    }

    pub(super) fn metric_series(self, series_id: SeriesId) -> Option<MetricSeries> {
        self.registry
            .read()
            .decode_series_key(series_id)
            .map(|series_key| MetricSeries {
                name: series_key.metric,
                labels: series_key.labels,
            })
    }

    pub(super) fn resolved_series_for_metric(
        self,
        metric: &str,
        execution: &QueryExecution,
    ) -> Result<ResolvedMetricSeriesBatch> {
        let registry = self.registry.read();
        let mut resolution_reservation = None;
        let mut resolved_slots = 0u64;
        let series_ids = registry.series_ids_for_metric_with_preflight(
            metric,
            |series_count| -> Result<()> {
                let series_count_u64 = u64::try_from(series_count).unwrap_or(u64::MAX);
                execution.charge_series_matched(series_count_u64)?;
                execution.observe_intermediate_vector_size(series_count_u64)?;

                let vector_capacity = super::modeled_vec_growth_capacity_upper(series_count);
                let id_slots = super::modeled_vec_capacity_bytes::<SeriesId>(vector_capacity);
                resolved_slots = super::modeled_vec_capacity_bytes::<(
                    MetricSeries,
                    Option<SeriesId>,
                )>(vector_capacity);
                resolution_reservation =
                    Some(execution.reserve_memory(id_slots.saturating_add(resolved_slots))?);
                Ok(())
            },
        )?;
        let mut identity_bytes = 0u64;
        for series_id in &series_ids {
            execution.checkpoint()?;
            let Some((metric_bytes, label_count, label_text_bytes)) =
                registry.decoded_series_key_shape(*series_id)
            else {
                continue;
            };
            identity_bytes =
                identity_bytes.saturating_add(super::modeled_metric_series_shape_retained_bytes(
                    metric_bytes,
                    label_count,
                    label_text_bytes,
                ));
        }
        resolution_reservation
            .as_mut()
            .expect("metric postings preflight always initializes a reservation")
            .resize(
                super::modeled_vec_capacity_bytes::<SeriesId>(series_ids.capacity())
                    .saturating_add(resolved_slots)
                    .saturating_add(identity_bytes),
            )?;

        let mut resolved = Vec::with_capacity(series_ids.len());
        for series_id in &series_ids {
            execution.checkpoint()?;
            let Some(series_key) = registry.decode_series_key(*series_id) else {
                continue;
            };
            resolved.push((
                MetricSeries {
                    name: series_key.metric,
                    labels: series_key.labels,
                },
                Some(*series_id),
            ));
        }

        drop(series_ids);
        let retained_bytes = super::modeled_vec_capacity_bytes::<(MetricSeries, Option<SeriesId>)>(
            resolved.capacity(),
        )
        .saturating_add(resolved.iter().fold(0u64, |bytes, (series, _)| {
            bytes.saturating_add(super::modeled_metric_series_retained_bytes(series))
        }));
        resolution_reservation
            .as_mut()
            .expect("metric postings preflight always initializes a reservation")
            .resize(retained_bytes)?;
        Ok(ResolvedMetricSeriesBatch {
            series: resolved,
            _query_reservation: resolution_reservation
                .take()
                .expect("metric postings preflight always initializes a reservation"),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_into(
        self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats> {
        self.ops.select_into_impl(
            metric,
            labels,
            start,
            end,
            plan,
            out,
            execution,
            enforce_return_limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_raw_series_page(
        self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        pagination: RawSeriesPagination,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<RawSeriesScanPage> {
        self.ops.select_raw_series_page_with_plan(
            metric,
            labels,
            start,
            end,
            plan,
            pagination,
            execution,
            enforce_return_limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn collect_points_for_series_into(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats> {
        self.ops.collect_points_for_series_into_with_plan(
            series_id,
            start,
            end,
            plan,
            out,
            execution,
            enforce_return_limit,
        )
    }

    pub(super) fn collect_points_for_series(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<(Vec<DataPoint>, PersistedTierFetchStats)> {
        self.ops.collect_points_for_series_with_plan(
            series_id,
            start,
            end,
            plan,
            execution,
            enforce_return_limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn collect_raw_series_page(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        offset: u64,
        limit: Option<usize>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<RawSeriesScanPage> {
        self.ops.collect_raw_series_page_with_plan(
            series_id,
            start,
            end,
            plan,
            offset,
            limit,
            execution,
            enforce_return_limit,
        )
    }

    pub(super) fn rollup_query_candidate(
        self,
        metric: &str,
        labels: &[Label],
        interval: i64,
        aggregation: crate::Aggregation,
        start: i64,
        end: i64,
    ) -> Option<RollupQueryCandidate> {
        self.ops
            .rollup_query_candidate(metric, labels, interval, aggregation, start, end)
    }

    pub(super) fn record_rollup_query_use(self, points_read: usize, partial: bool) {
        self.ops.record_rollup_query_use(points_read, partial);
    }
}

pub(super) trait TimeRangeFilterOps {
    fn refresh_missing_visibility_summaries(
        &self,
        series_ids: &RoaringTreemap,
        execution: &QueryExecution,
    ) -> Result<()>;

    fn latest_visible_timestamp_in_persisted_chunk(
        &self,
        persisted_index: &PersistedIndexState,
        chunk_ref: &PersistedChunkRef,
        tombstone_ranges: Option<&[crate::engine::tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>>;

    #[cfg(test)]
    fn invoke_metadata_time_range_summary_hook(&self) {}

    #[cfg(test)]
    fn invoke_metadata_time_range_persisted_exact_scan_hook(&self) {}

    #[cfg(test)]
    fn invoke_metadata_time_range_segment_prune_hook(&self) {}
}

#[derive(Clone, Copy)]
pub(super) struct TimeRangeFilterContext<'a> {
    pub(super) chunks: ChunkContext<'a>,
    pub(super) persisted_index: &'a RwLock<PersistedIndexState>,
    pub(super) visibility_cache: VisibilityCacheReadContext<'a>,
    pub(super) visibility_fence: &'a RwLock<()>,
    pub(super) tombstones: super::super::visibility::TombstoneReadContext<'a>,
    pub(super) active_retention_cutoff: Option<i64>,
    pub(super) ops: &'a dyn TimeRangeFilterOps,
}

impl<'a> TimeRangeFilterContext<'a> {
    pub(super) fn visibility_read_fence(self) -> RwLockReadGuard<'a, ()> {
        self.visibility_fence.read()
    }

    pub(super) fn refresh_missing_visibility_summaries(
        self,
        series_ids: &RoaringTreemap,
        execution: &QueryExecution,
    ) -> Result<()> {
        self.ops
            .refresh_missing_visibility_summaries(series_ids, execution)
    }
}

impl ChunkStorage {
    pub(super) fn series_query_context(&self) -> SeriesQueryContext<'_> {
        SeriesQueryContext {
            planning: self.query_planning_context(),
            registry: &self.catalog.registry,
            visibility_fence: &self.visibility.flush_visibility_lock,
            ops: self,
        }
    }

    pub(super) fn time_range_filter_context(&self) -> TimeRangeFilterContext<'_> {
        TimeRangeFilterContext {
            chunks: self.chunk_context(),
            persisted_index: &self.persisted.persisted_index,
            visibility_cache: self.visibility_cache_read_context(),
            visibility_fence: &self.visibility.flush_visibility_lock,
            tombstones: self.tombstone_read_context(),
            active_retention_cutoff: self.active_retention_cutoff(),
            ops: self,
        }
    }
}

#[allow(clippy::too_many_arguments)]
impl SeriesQueryReadOps for ChunkStorage {
    fn select_into_impl(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats> {
        ChunkStorage::select_into_impl(
            self,
            metric,
            labels,
            start,
            end,
            plan,
            out,
            execution,
            enforce_return_limit,
        )
    }

    fn select_raw_series_page_with_plan(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        pagination: RawSeriesPagination,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<RawSeriesScanPage> {
        ChunkStorage::select_raw_series_page_with_plan(
            self,
            metric,
            labels,
            start,
            end,
            plan,
            pagination,
            execution,
            enforce_return_limit,
        )
    }

    fn collect_points_for_series_into_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats> {
        ChunkStorage::collect_points_for_series_into_with_plan(
            self,
            series_id,
            start,
            end,
            plan,
            out,
            execution,
            enforce_return_limit,
        )
    }

    fn collect_points_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<(Vec<DataPoint>, PersistedTierFetchStats)> {
        ChunkStorage::collect_points_for_series_with_plan(
            self,
            series_id,
            start,
            end,
            plan,
            execution,
            enforce_return_limit,
        )
    }

    fn collect_raw_series_page_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        offset: u64,
        limit: Option<usize>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<RawSeriesScanPage> {
        ChunkStorage::collect_raw_series_page_with_plan(
            self,
            series_id,
            start,
            end,
            plan,
            offset,
            limit,
            execution,
            enforce_return_limit,
        )
    }

    fn rollup_query_candidate(
        &self,
        metric: &str,
        labels: &[Label],
        interval: i64,
        aggregation: crate::Aggregation,
        start: i64,
        end: i64,
    ) -> Option<RollupQueryCandidate> {
        ChunkStorage::rollup_query_candidate(
            self,
            metric,
            labels,
            interval,
            aggregation,
            start,
            end,
        )
    }

    fn record_rollup_query_use(&self, points_read: usize, partial: bool) {
        ChunkStorage::record_rollup_query_use(self, points_read, partial);
    }
}

impl TimeRangeFilterOps for ChunkStorage {
    fn refresh_missing_visibility_summaries(
        &self,
        series_ids: &RoaringTreemap,
        execution: &QueryExecution,
    ) -> Result<()> {
        self.refresh_missing_visibility_summaries_for_query(series_ids, execution)
    }

    fn latest_visible_timestamp_in_persisted_chunk(
        &self,
        persisted_index: &PersistedIndexState,
        chunk_ref: &PersistedChunkRef,
        tombstone_ranges: Option<&[crate::engine::tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        ChunkStorage::latest_visible_timestamp_in_persisted_chunk(
            self,
            persisted_index,
            chunk_ref,
            tombstone_ranges,
            floor_exclusive,
            ceiling_inclusive,
        )
    }

    #[cfg(test)]
    fn invoke_metadata_time_range_summary_hook(&self) {
        ChunkStorage::invoke_metadata_query_time_range_summary_hook(self);
    }

    #[cfg(test)]
    fn invoke_metadata_time_range_persisted_exact_scan_hook(&self) {
        ChunkStorage::invoke_metadata_time_range_persisted_exact_scan_hook(self);
    }

    #[cfg(test)]
    fn invoke_metadata_time_range_segment_prune_hook(&self) {
        ChunkStorage::invoke_metadata_time_range_segment_prune_hook(self);
    }
}
