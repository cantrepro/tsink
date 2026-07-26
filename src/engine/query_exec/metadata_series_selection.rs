use std::cell::RefCell;

use roaring::RoaringTreemap;

use crate::engine::query::TieredQueryPlan;
use crate::query_matcher::CompiledSeriesMatcher;
use crate::query_selection::{PreparedSeriesSelection, SeriesSelectionBackend};
use crate::{Label, QueryExecution, SeriesSelection};

use super::candidate_planner::{CandidatePlanningResult, MetadataCandidatePlanner};
use super::metadata_postings::RuntimeMetadataPostingsProvider;
use super::{
    saturating_u64_from_usize, ChunkStorage, MetadataSelectionContext, MetricSeries, Result,
    RuntimeMetadataCandidatePlan, SeriesId,
};

fn modeled_string_capacity_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        saturating_u64_from_usize(value.capacity())
            .saturating_add(super::QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_metric_series_vec_retained_bytes(series: &Vec<MetricSeries>) -> u64 {
    super::modeled_vec_capacity_bytes::<MetricSeries>(series.capacity()).saturating_add(
        series.iter().fold(0u64, |bytes, item| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(&item.name))
                .saturating_add(super::modeled_vec_capacity_bytes::<Label>(
                    item.labels.capacity(),
                ))
                .saturating_add(item.labels.iter().fold(0u64, |label_bytes, label| {
                    label_bytes
                        .saturating_add(modeled_string_capacity_bytes(&label.name))
                        .saturating_add(modeled_string_capacity_bytes(&label.value))
                }))
        }),
    )
}

struct PostingsSeriesSelectionBackend<'a> {
    context: MetadataSelectionContext<'a>,
    time_range_plan: Option<TieredQueryPlan>,
    execution: &'a QueryExecution,
    _candidate_reservation: crate::QueryMemoryReservation,
    result_reservation: RefCell<Option<crate::QueryMemoryReservation>>,
}

impl SeriesSelectionBackend for PostingsSeriesSelectionBackend<'_> {
    type Candidates = RoaringTreemap;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Self::Candidates> {
        let candidates = self.context.live_candidate_series_ids(
            selection,
            &prepared.compiled_matchers,
            self.execution,
        )?;
        Ok(candidates)
    }

    fn retain_items_in_time_range(
        &self,
        items: &mut Self::Candidates,
        start: i64,
        end: i64,
    ) -> Result<()> {
        self.context.retain_postings_in_time_range(
            items,
            start,
            end,
            self.time_range_plan,
            self.execution,
        )
    }

    fn materialize_items(&self, items: Self::Candidates) -> Result<Vec<MetricSeries>> {
        self.execution.checkpoint()?;
        self.execution.charge_series_matched(items.len())?;
        self.execution
            .observe_intermediate_vector_size(items.len())?;
        let (returned_bytes_upper, retained_identity_bytes) =
            self.context.modeled_metric_series_shapes(&items);
        self.execution.ensure_returned_bytes(returned_bytes_upper)?;
        let mut reservation = self.execution.reserve_memory(
            super::modeled_vec_capacity_bytes::<MetricSeries>(
                super::modeled_vec_growth_capacity_upper(
                    usize::try_from(items.len()).unwrap_or(usize::MAX),
                ),
            )
            .saturating_add(retained_identity_bytes),
        )?;
        let series = self.context.metric_series_for_postings(items);
        reservation.resize(modeled_metric_series_vec_retained_bytes(&series))?;
        *self.result_reservation.borrow_mut() = Some(reservation);
        Ok(series)
    }
}

struct ShardScopedPostingsSeriesSelectionBackend<'a> {
    context: MetadataSelectionContext<'a>,
    candidate_series_ids: Vec<SeriesId>,
    time_range_plan: Option<TieredQueryPlan>,
    execution: &'a QueryExecution,
    _candidate_reservation: crate::QueryMemoryReservation,
    result_reservation: RefCell<Option<crate::QueryMemoryReservation>>,
}

impl SeriesSelectionBackend for ShardScopedPostingsSeriesSelectionBackend<'_> {
    type Candidates = RoaringTreemap;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Self::Candidates> {
        let candidates = self.context.shard_scoped_candidate_series_ids(
            selection,
            &prepared.compiled_matchers,
            &self.candidate_series_ids,
            self.execution,
        )?;
        Ok(candidates)
    }

    fn retain_items_in_time_range(
        &self,
        items: &mut Self::Candidates,
        start: i64,
        end: i64,
    ) -> Result<()> {
        self.context.retain_postings_in_time_range(
            items,
            start,
            end,
            self.time_range_plan,
            self.execution,
        )
    }

    fn materialize_items(&self, items: Self::Candidates) -> Result<Vec<MetricSeries>> {
        self.execution.checkpoint()?;
        self.execution.charge_series_matched(items.len())?;
        self.execution
            .observe_intermediate_vector_size(items.len())?;
        let (returned_bytes_upper, retained_identity_bytes) =
            self.context.modeled_metric_series_shapes(&items);
        self.execution.ensure_returned_bytes(returned_bytes_upper)?;
        let mut reservation = self.execution.reserve_memory(
            super::modeled_vec_capacity_bytes::<MetricSeries>(
                super::modeled_vec_growth_capacity_upper(
                    usize::try_from(items.len()).unwrap_or(usize::MAX),
                ),
            )
            .saturating_add(retained_identity_bytes),
        )?;
        let series = self.context.metric_series_for_postings(items);
        reservation.resize(modeled_metric_series_vec_retained_bytes(&series))?;
        *self.result_reservation.borrow_mut() = Some(reservation);
        Ok(series)
    }
}

impl ChunkStorage {
    fn prepare_series_selection_for_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<PreparedSeriesSelection> {
        #[cfg(test)]
        {
            let before_regex_compile = || self.invoke_metadata_matcher_regex_compile_hook();
            crate::query_selection::prepare_series_selection_with_execution(
                selection,
                execution,
                Some(&before_regex_compile),
            )
        }
        #[cfg(not(test))]
        {
            crate::query_selection::prepare_series_selection_with_execution(
                selection, execution, None,
            )
        }
    }

    pub(super) fn runtime_metadata_candidate_plan(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
        execution: &QueryExecution,
    ) -> Result<RuntimeMetadataCandidatePlan> {
        {
            let registry = self.catalog.registry.read();
            let persisted_index = self.persisted.persisted_index.read();
            #[cfg(test)]
            let all_series_postings_hook = self
                .persist_test_hooks
                .metadata_all_series_postings_hook
                .read()
                .clone();
            #[cfg(test)]
            let postings = RuntimeMetadataPostingsProvider::new(
                &registry,
                &persisted_index,
                all_series_postings_hook,
            );
            #[cfg(not(test))]
            let postings = RuntimeMetadataPostingsProvider::new(&registry, &persisted_index);

            #[cfg(test)]
            let direct_scan_hook = || self.invoke_metadata_direct_candidate_scan_hook();
            #[cfg(test)]
            let planner =
                MetadataCandidatePlanner::new(&postings, &registry, Some(&direct_scan_hook));
            #[cfg(not(test))]
            let planner = MetadataCandidatePlanner::new(&postings, &registry, None);

            #[cfg(test)]
            {
                let CandidatePlanningResult {
                    candidate_series_ids,
                    used_all_series_seed,
                } = planner.plan_series_candidates(
                    selection,
                    compiled_matchers,
                    scope_filter,
                    execution,
                )?;
                let used_persisted_postings = postings.uses_persisted_postings();
                Ok(RuntimeMetadataCandidatePlan {
                    candidate_series_ids,
                    used_all_series_seed,
                    used_persisted_postings,
                })
            }
            #[cfg(not(test))]
            {
                let CandidatePlanningResult {
                    candidate_series_ids,
                    ..
                } = planner.plan_series_candidates(
                    selection,
                    compiled_matchers,
                    scope_filter,
                    execution,
                )?;
                Ok(RuntimeMetadataCandidatePlan {
                    candidate_series_ids,
                })
            }
        }
    }

    #[cfg(test)]
    pub(super) fn record_runtime_metadata_candidate_plan_hooks(
        &self,
        plan: &RuntimeMetadataCandidatePlan,
        compiled_matchers: &[CompiledSeriesMatcher],
    ) {
        if plan.used_all_series_seed {
            self.invoke_metadata_all_series_seed_hook();
        }
        if plan.used_persisted_postings && !compiled_matchers.is_empty() {
            self.invoke_metadata_persisted_postings_hook();
        }
    }

    pub(in crate::engine) fn select_series_impl(
        &self,
        selection: &SeriesSelection,
    ) -> Result<Vec<MetricSeries>> {
        let budget = crate::QueryBudget::new(crate::QueryBudgetLimits::default())
            .map_err(crate::QueryBudgetError::from)?;
        let execution = budget.begin_query()?;
        self.select_series_impl_with_execution(selection, &execution)
    }

    pub(in crate::engine) fn select_series_impl_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<Vec<MetricSeries>> {
        self.select_series_impl_with_execution_result(selection, execution)
            .map(|(series, _reservation)| series)
    }

    pub(in crate::engine) fn select_series_impl_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<(Vec<MetricSeries>, crate::QueryMemoryReservation)> {
        // Reject matcher shape before candidate planning or shard-scope materialization. The
        // small candidate reservation is acquired before matcher preparation so the complete
        // preparation peak is admitted before the fail-before-compile hook can fire.
        crate::query_selection::validate_series_selection(selection)?;
        let context = self.metadata_selection_context();
        let candidate_reservation = self.reserve_metadata_candidate_working_set(execution)?;
        let prepared = self.prepare_series_selection_for_execution(selection, execution)?;
        let backend = PostingsSeriesSelectionBackend {
            context,
            time_range_plan: prepared
                .time_range
                .map(|(start, end)| context.query_tier_plan(start, end)),
            execution,
            _candidate_reservation: candidate_reservation,
            result_reservation: RefCell::new(None),
        };
        let series = crate::query_selection::execute_prepared_series_selection(
            &backend, selection, prepared,
        )?;
        let reservation = match backend.result_reservation.into_inner() {
            Some(reservation) => reservation,
            None => execution.reserve_memory(0)?,
        };
        Ok((series, reservation))
    }

    pub(in crate::engine) fn select_series_in_shards_impl_result(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
        execution: &QueryExecution,
    ) -> Result<(Vec<MetricSeries>, crate::QueryMemoryReservation)> {
        crate::query_selection::validate_series_selection(selection)?;
        let context = self.metadata_selection_context();
        let candidate_reservation = self.reserve_metadata_candidate_working_set(execution)?;
        let prepared = self.prepare_series_selection_for_execution(selection, execution)?;
        execution.ensure_pattern_expansion(
            u64::try_from(self.catalog.registry.read().series_count()).unwrap_or(u64::MAX),
        )?;
        let candidate_series_ids =
            self.bounded_metadata_series_ids_for_scope(scope, "select_series_in_shards")?;
        let backend = ShardScopedPostingsSeriesSelectionBackend {
            context,
            candidate_series_ids,
            time_range_plan: prepared
                .time_range
                .map(|(start, end)| context.query_tier_plan(start, end)),
            execution,
            _candidate_reservation: candidate_reservation,
            result_reservation: RefCell::new(None),
        };
        let series = crate::query_selection::execute_prepared_series_selection(
            &backend, selection, prepared,
        )?;
        let reservation = match backend.result_reservation.into_inner() {
            Some(reservation) => reservation,
            None => execution.reserve_memory(0)?,
        };
        Ok((series, reservation))
    }
}
