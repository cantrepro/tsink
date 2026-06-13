use roaring::RoaringTreemap;

use crate::engine::query::TieredQueryPlan;
use crate::query_matcher::CompiledSeriesMatcher;
use crate::query_selection::{PreparedSeriesSelection, SeriesSelectionBackend};
use crate::SeriesSelection;

use super::candidate_planner::{CandidatePlanningResult, MetadataCandidatePlanner};
use super::metadata_postings::RuntimeMetadataPostingsProvider;
use super::{
    ChunkStorage, MetadataSelectionContext, MetricSeries, Result, RuntimeMetadataCandidatePlan,
    SeriesId,
};

struct PostingsSeriesSelectionBackend<'a> {
    context: MetadataSelectionContext<'a>,
    time_range_plan: Option<TieredQueryPlan>,
}

impl SeriesSelectionBackend for PostingsSeriesSelectionBackend<'_> {
    type Candidates = RoaringTreemap;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Self::Candidates> {
        self.context
            .live_candidate_series_ids(selection, &prepared.compiled_matchers)
    }

    fn retain_items_in_time_range(
        &self,
        items: &mut Self::Candidates,
        start: i64,
        end: i64,
    ) -> Result<()> {
        self.context
            .retain_postings_in_time_range(items, start, end, self.time_range_plan)
    }

    fn materialize_items(&self, items: Self::Candidates) -> Result<Vec<MetricSeries>> {
        Ok(self.context.metric_series_for_postings(items))
    }
}

struct ShardScopedPostingsSeriesSelectionBackend<'a> {
    context: MetadataSelectionContext<'a>,
    candidate_series_ids: Vec<SeriesId>,
    time_range_plan: Option<TieredQueryPlan>,
}

impl SeriesSelectionBackend for ShardScopedPostingsSeriesSelectionBackend<'_> {
    type Candidates = RoaringTreemap;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Self::Candidates> {
        self.context.shard_scoped_candidate_series_ids(
            selection,
            &prepared.compiled_matchers,
            &self.candidate_series_ids,
        )
    }

    fn retain_items_in_time_range(
        &self,
        items: &mut Self::Candidates,
        start: i64,
        end: i64,
    ) -> Result<()> {
        self.context
            .retain_postings_in_time_range(items, start, end, self.time_range_plan)
    }

    fn materialize_items(&self, items: Self::Candidates) -> Result<Vec<MetricSeries>> {
        Ok(self.context.metric_series_for_postings(items))
    }
}

impl ChunkStorage {
    pub(super) fn runtime_metadata_candidate_plan(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
    ) -> RuntimeMetadataCandidatePlan {
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
                } = planner.plan_series_candidates(selection, compiled_matchers, scope_filter);
                let used_persisted_postings = postings.uses_persisted_postings();
                RuntimeMetadataCandidatePlan {
                    candidate_series_ids,
                    used_all_series_seed,
                    used_persisted_postings,
                }
            }
            #[cfg(not(test))]
            {
                let CandidatePlanningResult {
                    candidate_series_ids,
                    ..
                } = planner.plan_series_candidates(selection, compiled_matchers, scope_filter);
                RuntimeMetadataCandidatePlan {
                    candidate_series_ids,
                }
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
        let context = self.metadata_selection_context();
        crate::query_selection::execute_series_selection(
            &PostingsSeriesSelectionBackend {
                context,
                time_range_plan: selection
                    .normalized_time_range()?
                    .map(|(start, end)| context.query_tier_plan(start, end)),
            },
            selection,
        )
    }

    pub(in crate::engine) fn select_series_in_shards_impl(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        let context = self.metadata_selection_context();
        let candidate_series_ids =
            self.bounded_metadata_series_ids_for_scope(scope, "select_series_in_shards")?;
        crate::query_selection::execute_series_selection(
            &ShardScopedPostingsSeriesSelectionBackend {
                context,
                candidate_series_ids,
                time_range_plan: selection
                    .normalized_time_range()?
                    .map(|(start, end)| context.query_tier_plan(start, end)),
            },
            selection,
        )
    }
}
