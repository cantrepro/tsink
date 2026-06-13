use super::{
    rollups, ChunkStorage, CompiledSeriesMatcher, MetadataShardScope, MetricSeries, Result,
    RetentionTierPolicy, RoaringTreemap, SeriesId, SeriesSelection, TieredQueryPlan,
};

#[derive(Clone, Copy)]
pub(super) struct QueryPlanningContext<'a> {
    retention_recency_reference_timestamp: Option<i64>,
    tiered_storage: Option<&'a super::super::config::TieredStorageConfig>,
}

impl QueryPlanningContext<'_> {
    pub(super) fn query_tier_plan(self, start: i64, end: i64) -> TieredQueryPlan {
        RetentionTierPolicy::new(
            i64::MIN,
            self.retention_recency_reference_timestamp,
            self.tiered_storage,
        )
        .query_plan(start, end)
    }
}

pub(super) struct RuntimeMetadataCandidatePlan {
    pub(super) candidate_series_ids: RoaringTreemap,
    #[cfg(test)]
    pub(super) used_all_series_seed: bool,
    #[cfg(test)]
    pub(super) used_persisted_postings: bool,
}

trait MetadataCandidatePlanningOps {
    fn build_runtime_metadata_candidate_plan(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
    ) -> RuntimeMetadataCandidatePlan;

    #[cfg(test)]
    fn record_metadata_candidate_plan_hooks(
        &self,
        _plan: &RuntimeMetadataCandidatePlan,
        _compiled_matchers: &[CompiledSeriesMatcher],
    ) {
    }
}

trait MetadataPostingsReadOps {
    fn live_series_postings(
        &self,
        series_ids: RoaringTreemap,
        prune_dead: bool,
    ) -> Result<RoaringTreemap>;

    fn filter_series_postings_in_time_range(
        &self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<RoaringTreemap>;

    #[cfg(test)]
    fn invoke_metadata_time_range_summary_hook(&self) {}
}

trait MetadataSeriesMaterializationOps {
    fn materialize_metric_series(&self, series_ids: RoaringTreemap) -> Vec<MetricSeries>;
}

trait MetadataListingReadOps {
    fn live_series_pruning_generation(&self) -> u64;

    fn materialized_series_page_after(
        &self,
        cursor: Option<SeriesId>,
        page_size: usize,
    ) -> Vec<SeriesId>;

    fn append_live_metric_series_page(
        &self,
        series_ids: &[SeriesId],
        listed: &mut Vec<MetricSeries>,
        dead_series_ids: &mut Vec<SeriesId>,
    ) -> Result<()>;

    fn prune_dead_materialized_series_ids_if_stable(
        &self,
        dead_series_ids: Vec<SeriesId>,
        generation_before: Option<u64>,
    );

    fn wal_metric_series(&self) -> Result<Vec<MetricSeries>>;
}

trait MetadataShardScopeReadOps {
    fn live_series_ids_for_scope(
        &self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<SeriesId>>;

    fn live_metric_series_for_scope(
        &self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<MetricSeries>>;
}

#[derive(Clone, Copy)]
pub(super) struct MetadataSelectionContext<'a> {
    planning: QueryPlanningContext<'a>,
    candidate_planning: &'a dyn MetadataCandidatePlanningOps,
    postings: &'a dyn MetadataPostingsReadOps,
    materialization: &'a dyn MetadataSeriesMaterializationOps,
}

impl MetadataSelectionContext<'_> {
    pub(super) fn query_tier_plan(self, start: i64, end: i64) -> TieredQueryPlan {
        self.planning.query_tier_plan(start, end)
    }

    pub(super) fn live_candidate_series_ids(
        self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
    ) -> Result<RoaringTreemap> {
        let plan = self
            .candidate_planning
            .build_runtime_metadata_candidate_plan(selection, compiled_matchers, None);
        #[cfg(test)]
        self.candidate_planning
            .record_metadata_candidate_plan_hooks(&plan, compiled_matchers);
        self.postings
            .live_series_postings(plan.candidate_series_ids, true)
    }

    pub(super) fn shard_scoped_candidate_series_ids(
        self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_series_ids: &[SeriesId],
    ) -> Result<RoaringTreemap> {
        let live_scope_series_ids = self
            .postings
            .live_series_postings(scope_series_ids.iter().copied().collect(), true)?;
        let plan = self
            .candidate_planning
            .build_runtime_metadata_candidate_plan(
                selection,
                compiled_matchers,
                Some(&live_scope_series_ids),
            );
        #[cfg(test)]
        self.candidate_planning
            .record_metadata_candidate_plan_hooks(&plan, compiled_matchers);
        Ok(plan.candidate_series_ids)
    }

    pub(super) fn retain_postings_in_time_range(
        self,
        items: &mut RoaringTreemap,
        start: i64,
        end: i64,
        time_range_plan: Option<TieredQueryPlan>,
    ) -> Result<()> {
        #[cfg(test)]
        if !items.is_empty() {
            self.postings.invoke_metadata_time_range_summary_hook();
        }

        let filtered = self.postings.filter_series_postings_in_time_range(
            std::mem::take(items),
            start,
            end,
            time_range_plan.unwrap_or_else(|| self.query_tier_plan(start, end)),
        )?;
        *items = filtered;
        Ok(())
    }

    pub(super) fn metric_series_for_postings(
        self,
        series_ids: RoaringTreemap,
    ) -> Vec<MetricSeries> {
        self.materialization.materialize_metric_series(series_ids)
    }
}

#[derive(Clone, Copy)]
pub(super) struct MetadataListingContext<'a> {
    ops: &'a dyn MetadataListingReadOps,
}

impl MetadataListingContext<'_> {
    pub(super) fn live_series_pruning_generation(self) -> u64 {
        self.ops.live_series_pruning_generation()
    }

    pub(super) fn materialized_series_page_after(
        self,
        cursor: Option<SeriesId>,
        page_size: usize,
    ) -> Vec<SeriesId> {
        self.ops.materialized_series_page_after(cursor, page_size)
    }

    pub(super) fn append_live_metric_series_page(
        self,
        series_ids: &[SeriesId],
        listed: &mut Vec<MetricSeries>,
        dead_series_ids: &mut Vec<SeriesId>,
    ) -> Result<()> {
        self.ops
            .append_live_metric_series_page(series_ids, listed, dead_series_ids)
    }

    pub(super) fn prune_dead_materialized_series_ids_if_stable(
        self,
        dead_series_ids: Vec<SeriesId>,
        generation_before: Option<u64>,
    ) {
        self.ops
            .prune_dead_materialized_series_ids_if_stable(dead_series_ids, generation_before);
    }

    pub(super) fn wal_metric_series(self) -> Result<Vec<MetricSeries>> {
        self.ops.wal_metric_series()
    }
}

#[derive(Clone, Copy)]
pub(super) struct MetadataShardScopeContext<'a> {
    ops: &'a dyn MetadataShardScopeReadOps,
}

impl MetadataShardScopeContext<'_> {
    pub(super) fn live_series_ids_for_scope(
        self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<SeriesId>> {
        self.ops.live_series_ids_for_scope(scope, operation)
    }

    pub(super) fn live_metric_series_for_scope(
        self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<MetricSeries>> {
        self.ops.live_metric_series_for_scope(scope, operation)
    }
}

impl ChunkStorage {
    pub(super) fn query_planning_context(&self) -> QueryPlanningContext<'_> {
        QueryPlanningContext {
            retention_recency_reference_timestamp: self.retention_recency_reference_timestamp(),
            tiered_storage: self.persisted.tiered_storage.as_ref(),
        }
    }

    pub(super) fn metadata_selection_context(&self) -> MetadataSelectionContext<'_> {
        MetadataSelectionContext {
            planning: self.query_planning_context(),
            candidate_planning: self,
            postings: self,
            materialization: self,
        }
    }

    pub(super) fn metadata_listing_context(&self) -> MetadataListingContext<'_> {
        MetadataListingContext { ops: self }
    }

    pub(super) fn metadata_shard_scope_context(&self) -> MetadataShardScopeContext<'_> {
        MetadataShardScopeContext { ops: self }
    }

    #[cfg(test)]
    pub(super) fn invoke_metadata_query_time_range_summary_hook(&self) {
        self.invoke_metadata_time_range_summary_hook();
    }

    fn append_live_metric_series_page_impl(
        &self,
        series_ids: &[SeriesId],
        listed: &mut Vec<MetricSeries>,
        dead_series_ids: &mut Vec<SeriesId>,
    ) -> Result<()> {
        if series_ids.is_empty() {
            return Ok(());
        }

        let missing_series_ids =
            self.missing_visibility_summary_series_ids(series_ids.iter().copied());
        if !missing_series_ids.is_empty() {
            self.refresh_series_visible_timestamp_cache(missing_series_ids)?;
        }

        let retention_cutoff = self.active_retention_cutoff().unwrap_or(i64::MIN);
        let (live_series_ids, dead_series_page) =
            self.partition_series_by_retention(series_ids.iter().copied(), retention_cutoff);
        dead_series_ids.extend(dead_series_page);
        listed.extend(self.metric_series_for_ids(live_series_ids));
        Ok(())
    }

    fn wal_metric_series_impl(&self) -> Result<Vec<MetricSeries>> {
        let Some(wal) = &self.persisted.wal else {
            return Ok(Vec::new());
        };

        let mut series = Vec::new();
        for definition in wal.committed_series_definitions_snapshot()? {
            if rollups::is_internal_rollup_metric(&definition.metric) {
                continue;
            }
            series.push(MetricSeries {
                name: definition.metric,
                labels: definition.labels,
            });
        }
        Ok(series)
    }
}

impl MetadataCandidatePlanningOps for ChunkStorage {
    fn build_runtime_metadata_candidate_plan(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
    ) -> RuntimeMetadataCandidatePlan {
        ChunkStorage::runtime_metadata_candidate_plan(
            self,
            selection,
            compiled_matchers,
            scope_filter,
        )
    }

    #[cfg(test)]
    fn record_metadata_candidate_plan_hooks(
        &self,
        plan: &RuntimeMetadataCandidatePlan,
        compiled_matchers: &[CompiledSeriesMatcher],
    ) {
        ChunkStorage::record_runtime_metadata_candidate_plan_hooks(self, plan, compiled_matchers);
    }
}

impl MetadataPostingsReadOps for ChunkStorage {
    fn live_series_postings(
        &self,
        series_ids: RoaringTreemap,
        prune_dead: bool,
    ) -> Result<RoaringTreemap> {
        ChunkStorage::live_series_postings(self, series_ids, prune_dead)
    }

    fn filter_series_postings_in_time_range(
        &self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<RoaringTreemap> {
        ChunkStorage::series_postings_with_data_in_time_range(self, series_ids, start, end, plan)
    }

    #[cfg(test)]
    fn invoke_metadata_time_range_summary_hook(&self) {
        ChunkStorage::invoke_metadata_query_time_range_summary_hook(self);
    }
}

impl MetadataSeriesMaterializationOps for ChunkStorage {
    fn materialize_metric_series(&self, series_ids: RoaringTreemap) -> Vec<MetricSeries> {
        self.metric_series_for_ids(series_ids)
    }
}

impl MetadataListingReadOps for ChunkStorage {
    fn live_series_pruning_generation(&self) -> u64 {
        ChunkStorage::live_series_pruning_generation(self)
    }

    fn materialized_series_page_after(
        &self,
        cursor: Option<SeriesId>,
        page_size: usize,
    ) -> Vec<SeriesId> {
        ChunkStorage::materialized_series_page_after(self, cursor, page_size)
    }

    fn append_live_metric_series_page(
        &self,
        series_ids: &[SeriesId],
        listed: &mut Vec<MetricSeries>,
        dead_series_ids: &mut Vec<SeriesId>,
    ) -> Result<()> {
        self.append_live_metric_series_page_impl(series_ids, listed, dead_series_ids)
    }

    fn prune_dead_materialized_series_ids_if_stable(
        &self,
        dead_series_ids: Vec<SeriesId>,
        generation_before: Option<u64>,
    ) {
        ChunkStorage::prune_dead_materialized_series_ids_if_stable(
            self,
            dead_series_ids,
            generation_before,
        );
    }

    fn wal_metric_series(&self) -> Result<Vec<MetricSeries>> {
        self.wal_metric_series_impl()
    }
}

impl MetadataShardScopeReadOps for ChunkStorage {
    fn live_series_ids_for_scope(
        &self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<SeriesId>> {
        self.bounded_metadata_series_ids_for_scope(scope, operation)
            .and_then(|series_ids| self.live_series_ids(series_ids, true))
    }

    fn live_metric_series_for_scope(
        &self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<MetricSeries>> {
        self.bounded_metadata_series_ids_for_scope(scope, operation)
            .and_then(|series_ids| self.live_series_ids(series_ids, true))
            .map(|series_ids| self.metric_series_for_ids(series_ids))
    }
}
