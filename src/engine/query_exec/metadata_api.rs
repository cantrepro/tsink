use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::storage::SeriesSelection;

use super::{elapsed_nanos_u64, saturating_u64_from_usize, ChunkStorage, MetricSeries, Result};

const METADATA_LIST_PAGE_SIZE: usize = 4_096;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn list_metrics_api(&self) -> Result<Vec<MetricSeries>> {
        let context = self.metadata_listing_context();
        self.ensure_open()?;
        self.request_background_persisted_refresh_if_needed();
        let generation_before = context.live_series_pruning_generation();
        let mut listed = Vec::new();
        let mut dead_series_ids = Vec::new();
        let mut cursor = None;

        loop {
            let page = context.materialized_series_page_after(cursor, METADATA_LIST_PAGE_SIZE);
            if page.is_empty() {
                break;
            }

            cursor = page.last().copied();
            context.append_live_metric_series_page(&page, &mut listed, &mut dead_series_ids)?;

            if page.len() < METADATA_LIST_PAGE_SIZE {
                break;
            }
        }

        context
            .prune_dead_materialized_series_ids_if_stable(dead_series_ids, Some(generation_before));

        Ok(listed)
    }

    pub(in crate::engine::storage_engine) fn list_metrics_with_wal_api(
        &self,
    ) -> Result<Vec<MetricSeries>> {
        let context = self.metadata_listing_context();
        self.ensure_open()?;
        let mut series = self
            .list_metrics_api()?
            .into_iter()
            .collect::<BTreeSet<_>>();
        for definition in context.wal_metric_series()? {
            series.insert(definition);
        }
        Ok(series.into_iter().collect())
    }

    pub(in crate::engine::storage_engine) fn list_metrics_in_shards_api(
        &self,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        let context = self.metadata_shard_scope_context();
        self.ensure_open()?;
        self.request_background_persisted_refresh_if_needed();
        let scope = scope.normalized()?;
        context.live_metric_series_for_scope(&scope, "list_metrics_in_shards")
    }
    pub(in crate::engine::storage_engine) fn select_series_api(
        &self,
        selection: &SeriesSelection,
    ) -> Result<Vec<MetricSeries>> {
        self.select_series_with_optional_scope_api(selection, None)
    }

    pub(in crate::engine::storage_engine) fn select_series_in_shards_api(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        let scope = scope.normalized()?;
        self.select_series_with_optional_scope_api(selection, Some(scope))
    }

    fn select_series_with_optional_scope_api(
        &self,
        selection: &SeriesSelection,
        scope: Option<crate::storage::MetadataShardScope>,
    ) -> Result<Vec<MetricSeries>> {
        self.observability
            .query
            .select_series_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<MetricSeries>> {
            self.ensure_open()?;
            self.request_background_persisted_refresh_if_needed();
            if let Some((start, end)) = selection.normalized_time_range()? {
                self.record_query_tier_plan(self.query_tier_plan(start, end));
                #[cfg(test)]
                self.invoke_metadata_query_time_range_summary_hook();
            }
            match scope.as_ref() {
                Some(scope) => self.select_series_in_shards_impl(selection, scope),
                None => self.select_series_impl(selection),
            }
        })();

        self.observability
            .query
            .select_series_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                self.observability
                    .query
                    .select_series_returned_total
                    .fetch_add(saturating_u64_from_usize(series.len()), Ordering::Relaxed);
                Ok(series)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_series_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }
}
