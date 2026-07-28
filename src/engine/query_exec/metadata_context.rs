use super::{
    rollups, ChunkStorage, CompiledSeriesMatcher, MetadataShardScope, MetricSeries, Result,
    RetentionTierPolicy, RoaringTreemap, SeriesId, SeriesSelection, TieredQueryPlan,
};
use crate::{QueryExecution, QueryMemoryReservation, SelectSeriesExecutionResult};
use std::collections::BTreeSet;

const WAL_METADATA_BTREE_BOOKKEEPING_WORDS_PER_ENTRY: u64 = 4;

fn modeled_wal_metadata_identity_set_bytes(entries: usize) -> u64 {
    if entries == 0 {
        return 0;
    }
    let key_bytes =
        u64::try_from(std::mem::size_of::<(&str, &[crate::Label])>()).unwrap_or(u64::MAX);
    let bookkeeping_bytes = u64::try_from(std::mem::size_of::<usize>())
        .unwrap_or(u64::MAX)
        .saturating_mul(WAL_METADATA_BTREE_BOOKKEEPING_WORDS_PER_ENTRY);
    u64::try_from(entries)
        .unwrap_or(u64::MAX)
        .saturating_mul(key_bytes.saturating_add(bookkeeping_bytes))
        .saturating_add(super::QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn contains_metric_series_identity(
    sorted_series: &[MetricSeries],
    metric: &str,
    labels: &[crate::Label],
) -> bool {
    sorted_series
        .binary_search_by(|series| {
            series
                .name
                .as_str()
                .cmp(metric)
                .then_with(|| series.labels.as_slice().cmp(labels))
        })
        .is_ok()
}

fn projected_growing_vec_capacity(current: usize, required: usize) -> usize {
    if required <= current {
        return current;
    }
    current
        .saturating_mul(2)
        .max(required)
        .max(4)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
}

pub(super) struct MetadataListMaterialization<'a> {
    pub(super) listed: &'a mut Vec<MetricSeries>,
    pub(super) dead_series_ids: &'a mut Vec<SeriesId>,
    pub(super) execution: &'a QueryExecution,
    pub(super) page_scratch_bytes: u64,
    pub(super) retained_identity_bytes: &'a mut u64,
    pub(super) reservation: &'a mut QueryMemoryReservation,
}

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
        execution: &QueryExecution,
    ) -> Result<RuntimeMetadataCandidatePlan>;

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
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap>;

    fn filter_series_postings_in_time_range(
        &self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap>;

    #[cfg(test)]
    fn invoke_metadata_time_range_summary_hook(&self) {}
}

trait MetadataSeriesMaterializationOps {
    fn materialize_metric_series(&self, series_ids: RoaringTreemap) -> Vec<MetricSeries>;

    fn modeled_metric_series_shapes(&self, series_ids: &RoaringTreemap) -> (u64, u64);
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
        materialization: &mut MetadataListMaterialization<'_>,
    ) -> Result<()>;

    fn prune_dead_materialized_series_ids_if_stable(
        &self,
        dead_series_ids: Vec<SeriesId>,
        generation_before: Option<u64>,
    );

    fn wal_metric_series_result(
        &self,
        sorted_live_series: &[MetricSeries],
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult>;
}

trait MetadataShardScopeReadOps {
    fn live_series_ids_for_scope(
        &self,
        scope: &MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<SeriesId>>;
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
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        let plan = self
            .candidate_planning
            .build_runtime_metadata_candidate_plan(selection, compiled_matchers, None, execution)?;
        #[cfg(test)]
        self.candidate_planning
            .record_metadata_candidate_plan_hooks(&plan, compiled_matchers);
        self.postings
            .live_series_postings(plan.candidate_series_ids, true, execution)
    }

    pub(super) fn shard_scoped_candidate_series_ids(
        self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_series_ids: &[SeriesId],
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        let live_scope_series_ids = self.postings.live_series_postings(
            scope_series_ids.iter().copied().collect(),
            true,
            execution,
        )?;
        let plan = self
            .candidate_planning
            .build_runtime_metadata_candidate_plan(
                selection,
                compiled_matchers,
                Some(&live_scope_series_ids),
                execution,
            )?;
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
        execution: &QueryExecution,
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
            execution,
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

    pub(super) fn modeled_metric_series_shapes(self, series_ids: &RoaringTreemap) -> (u64, u64) {
        self.materialization
            .modeled_metric_series_shapes(series_ids)
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
        materialization: &mut MetadataListMaterialization<'_>,
    ) -> Result<()> {
        self.ops
            .append_live_metric_series_page(series_ids, materialization)
    }

    pub(super) fn prune_dead_materialized_series_ids_if_stable(
        self,
        dead_series_ids: Vec<SeriesId>,
        generation_before: Option<u64>,
        execution: &QueryExecution,
    ) -> Result<()> {
        execution.checkpoint()?;
        let dead_len = dead_series_ids.len();
        execution.observe_intermediate_vector_size(u64::try_from(dead_len).unwrap_or(u64::MAX))?;
        // Stable pruning can hold the accumulated dead IDs plus one equally sized companion
        // vector. The companion is reused by lifetime across removal, runtime-delta
        // reconciliation, and shard-index unpublication, so charge it once rather than once per
        // phase. The accumulated dead vector remains covered by the list materialization
        // reservation.
        let _pruning_reservation =
            execution.reserve_memory(super::modeled_vec_capacity_bytes::<SeriesId>(dead_len))?;
        self.ops
            .prune_dead_materialized_series_ids_if_stable(dead_series_ids, generation_before);
        Ok(())
    }

    pub(super) fn wal_metric_series_result(
        self,
        sorted_live_series: &[MetricSeries],
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.ops
            .wal_metric_series_result(sorted_live_series, execution)
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
        materialization: &mut MetadataListMaterialization<'_>,
    ) -> Result<()> {
        if series_ids.is_empty() {
            return Ok(());
        }

        let missing_series_ids =
            self.missing_visibility_summary_series_ids(series_ids.iter().copied());
        if !missing_series_ids.is_empty() {
            self.refresh_series_visible_timestamp_cache_for_query(
                missing_series_ids,
                materialization.execution,
            )?;
        }

        let retention_cutoff = self.active_retention_cutoff().unwrap_or(i64::MIN);
        let (live_series_ids, dead_series_page) =
            self.partition_series_by_retention(series_ids.iter().copied(), retention_cutoff);

        let listed = &mut *materialization.listed;
        let dead_series_ids = &mut *materialization.dead_series_ids;
        let execution = materialization.execution;
        execution.checkpoint()?;
        let desired_listed_len = listed.len().saturating_add(live_series_ids.len());
        let desired_dead_len = dead_series_ids.len().saturating_add(dead_series_page.len());
        execution
            .charge_series_matched(u64::try_from(live_series_ids.len()).unwrap_or(u64::MAX))?;
        execution.observe_intermediate_vector_size(
            u64::try_from(desired_listed_len).unwrap_or(u64::MAX),
        )?;
        execution.observe_intermediate_vector_size(
            u64::try_from(desired_dead_len).unwrap_or(u64::MAX),
        )?;

        let (returned_bytes, identity_bytes) = {
            let registry = self.catalog.registry.read();
            live_series_ids
                .iter()
                .fold((0u64, 0u64), |(returned, retained), series_id| {
                    let Some((metric_bytes, label_count, label_text_bytes)) =
                        registry.decoded_series_key_shape(*series_id)
                    else {
                        return (returned, retained);
                    };
                    (
                        returned.saturating_add(super::modeled_metric_series_shape_bytes(
                            metric_bytes,
                            label_count,
                            label_text_bytes,
                        )),
                        retained.saturating_add(super::modeled_metric_series_shape_retained_bytes(
                            metric_bytes,
                            label_count,
                            label_text_bytes,
                        )),
                    )
                })
        };
        execution.ensure_returned_bytes(returned_bytes)?;
        let next_identity_bytes = materialization
            .retained_identity_bytes
            .saturating_add(identity_bytes);
        let projected_listed_capacity =
            projected_growing_vec_capacity(listed.capacity(), desired_listed_len);
        let projected_dead_capacity =
            projected_growing_vec_capacity(dead_series_ids.capacity(), desired_dead_len);
        let modeled_bytes = materialization
            .page_scratch_bytes
            .saturating_add(super::modeled_vec_capacity_bytes::<MetricSeries>(
                projected_listed_capacity,
            ))
            .saturating_add(next_identity_bytes)
            .saturating_add(super::modeled_vec_capacity_bytes::<SeriesId>(
                projected_dead_capacity,
            ));
        materialization.reservation.resize(modeled_bytes)?;
        execution.charge_returned_bytes(returned_bytes)?;

        listed.reserve(live_series_ids.len());
        dead_series_ids.reserve(dead_series_page.len());
        dead_series_ids.extend(dead_series_page);
        self.append_metric_series_for_ids(live_series_ids, listed);
        *materialization.retained_identity_bytes = next_identity_bytes;
        Ok(())
    }

    fn wal_metric_series_impl(
        &self,
        sorted_live_series: &[MetricSeries],
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        let Some(wal) = &self.persisted.wal else {
            return execution
                .reserve_memory(0)
                .map(|reservation| SelectSeriesExecutionResult::accounted(Vec::new(), reservation))
                .map_err(Into::into);
        };

        let mut reservation = execution.reserve_memory(0)?;
        let mut snapshot_retained_bytes = 0u64;
        let definitions =
            wal.committed_series_definitions_snapshot_with_preflight(|committed_definitions| {
                execution.checkpoint()?;
                execution.observe_intermediate_vector_size(
                    u64::try_from(committed_definitions.len()).unwrap_or(u64::MAX),
                )?;
                let candidate_count =
                    u64::try_from(committed_definitions.len()).unwrap_or(u64::MAX);
                execution.ensure_pattern_expansion(candidate_count)?;
                // Exact live/WAL union admission must happen before cloning the cached snapshot.
                // The borrowed identity set owns no metric or label text and grows only after the
                // candidate fits both exact output limits. Duplicates within the WAL and against
                // the already-sorted live result therefore retain their historical union
                // semantics without forcing an unbounded definition clone on a rejected query.
                let mut unique_wal_identities = BTreeSet::new();
                let mut unique_wal_returned_bytes = 0u64;
                for definition in committed_definitions.values() {
                    execution.checkpoint()?;
                    execution.charge_pattern_expansion(1)?;
                    if rollups::is_internal_rollup_metric(&definition.metric)
                        || contains_metric_series_identity(
                            sorted_live_series,
                            &definition.metric,
                            &definition.labels,
                        )
                    {
                        continue;
                    }

                    let identity = (definition.metric.as_str(), definition.labels.as_slice());
                    if unique_wal_identities.contains(&identity) {
                        continue;
                    }

                    let next_unique_count = unique_wal_identities.len().saturating_add(1);
                    let label_text_bytes =
                        definition.labels.iter().fold(0usize, |label_bytes, label| {
                            label_bytes
                                .saturating_add(label.name.len())
                                .saturating_add(label.value.len())
                        });
                    let next_returned_bytes = unique_wal_returned_bytes.saturating_add(
                        super::modeled_metric_series_shape_bytes(
                            definition.metric.len(),
                            definition.labels.len(),
                            label_text_bytes,
                        ),
                    );
                    execution.ensure_series_matched(
                        u64::try_from(next_unique_count).unwrap_or(u64::MAX),
                    )?;
                    execution.ensure_returned_bytes(next_returned_bytes)?;
                    reservation
                        .resize(modeled_wal_metadata_identity_set_bytes(next_unique_count))?;
                    unique_wal_identities.insert(identity);
                    unique_wal_returned_bytes = next_returned_bytes;
                }

                let candidate_identity_bytes =
                    modeled_wal_metadata_identity_set_bytes(unique_wal_identities.len());
                let identity_bytes =
                    committed_definitions
                        .values()
                        .fold(0u64, |bytes, definition| {
                            let label_text_bytes =
                                definition.labels.iter().fold(0usize, |label_bytes, label| {
                                    label_bytes
                                        .saturating_add(label.name.len())
                                        .saturating_add(label.value.len())
                                });
                            bytes.saturating_add(super::modeled_metric_series_shape_retained_bytes(
                                definition.metric.len(),
                                definition.labels.len(),
                                label_text_bytes,
                            ))
                        });
                snapshot_retained_bytes =
                    super::modeled_vec_capacity_bytes::<crate::engine::wal::SeriesDefinitionFrame>(
                        super::modeled_vec_growth_capacity_upper(committed_definitions.len()),
                    )
                    .saturating_add(identity_bytes);
                // The borrowed candidate set is dropped before the snapshot clone. Holding the
                // larger of these two phase peaks covers the transition without summing
                // allocations that are never simultaneously live.
                reservation.resize(candidate_identity_bytes.max(snapshot_retained_bytes))?;
                Ok(())
            })?;
        reservation.resize(snapshot_retained_bytes)?;

        let output_capacity = super::modeled_vec_growth_capacity_upper(definitions.len());
        reservation.resize(snapshot_retained_bytes.saturating_add(
            super::modeled_vec_capacity_bytes::<MetricSeries>(output_capacity),
        ))?;
        let mut series = Vec::new();
        series.try_reserve(definitions.len()).map_err(|err| {
            crate::TsinkError::Other(format!(
                "failed to reserve WAL metric-series materialization: {err}"
            ))
        })?;
        for definition in definitions {
            execution.checkpoint()?;
            if rollups::is_internal_rollup_metric(&definition.metric) {
                continue;
            }
            series.push(MetricSeries {
                name: definition.metric,
                labels: definition.labels,
            });
        }
        reservation.resize(
            super::metadata_series_selection::modeled_metric_series_vec_retained_bytes(&series),
        )?;
        Ok(SelectSeriesExecutionResult::accounted(series, reservation))
    }
}

impl MetadataCandidatePlanningOps for ChunkStorage {
    fn build_runtime_metadata_candidate_plan(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
        execution: &QueryExecution,
    ) -> Result<RuntimeMetadataCandidatePlan> {
        ChunkStorage::runtime_metadata_candidate_plan(
            self,
            selection,
            compiled_matchers,
            scope_filter,
            execution,
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
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        ChunkStorage::live_series_postings_for_query(self, series_ids, prune_dead, execution)
    }

    fn filter_series_postings_in_time_range(
        &self,
        series_ids: RoaringTreemap,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        ChunkStorage::series_postings_with_data_in_time_range(
            self, series_ids, start, end, plan, execution,
        )
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

    fn modeled_metric_series_shapes(&self, series_ids: &RoaringTreemap) -> (u64, u64) {
        let registry = self.catalog.registry.read();
        series_ids
            .iter()
            .fold((0u64, 0u64), |(returned, retained), series_id| {
                let Some((metric_bytes, label_count, label_text_bytes)) =
                    registry.decoded_series_key_shape(series_id)
                else {
                    return (returned, retained);
                };
                (
                    returned.saturating_add(super::modeled_metric_series_shape_bytes(
                        metric_bytes,
                        label_count,
                        label_text_bytes,
                    )),
                    retained.saturating_add(super::modeled_metric_series_shape_retained_bytes(
                        metric_bytes,
                        label_count,
                        label_text_bytes,
                    )),
                )
            })
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
        materialization: &mut MetadataListMaterialization<'_>,
    ) -> Result<()> {
        self.append_live_metric_series_page_impl(series_ids, materialization)
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

    fn wal_metric_series_result(
        &self,
        sorted_live_series: &[MetricSeries],
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.wal_metric_series_impl(sorted_live_series, execution)
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
}
