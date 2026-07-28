use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::storage::SeriesSelection;
use crate::{QueryExecution, SelectSeriesExecutionResult};

use super::metadata_context::MetadataListMaterialization;
use super::{
    elapsed_nanos_u64, modeled_vec_capacity_bytes, saturating_u64_from_usize, ChunkStorage,
    MetricSeries, Result, SeriesId,
};

const METADATA_LIST_PAGE_SIZE: usize = 4_096;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn list_metrics_result_api(
        &self,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        let context = self.metadata_listing_context();
        execution.checkpoint()?;
        self.ensure_open()?;
        self.request_background_persisted_refresh_if_needed();
        let generation_before = context.live_series_pruning_generation();
        let mut listed = Vec::new();
        let mut dead_series_ids = Vec::new();
        // One page can transiently retain the source IDs plus either the missing-summary
        // collection/normalized input or the live/dead retention partition. Three ID vectors are
        // therefore the simultaneous high-water; cold-summary update/range staging is admitted
        // separately, without charging these IDs twice.
        let page_scratch_bytes =
            modeled_vec_capacity_bytes::<SeriesId>(METADATA_LIST_PAGE_SIZE).saturating_mul(3);
        let mut retained_identity_bytes = 0u64;
        let mut reservation = execution.reserve_memory(page_scratch_bytes)?;
        let mut cursor = None;

        loop {
            execution.checkpoint()?;
            let page = context.materialized_series_page_after(cursor, METADATA_LIST_PAGE_SIZE);
            if page.is_empty() {
                break;
            }

            cursor = page.last().copied();
            context.append_live_metric_series_page(
                &page,
                &mut MetadataListMaterialization {
                    listed: &mut listed,
                    dead_series_ids: &mut dead_series_ids,
                    execution,
                    page_scratch_bytes,
                    retained_identity_bytes: &mut retained_identity_bytes,
                    reservation: &mut reservation,
                },
            )?;

            if page.len() < METADATA_LIST_PAGE_SIZE {
                break;
            }
        }

        // Page scratch is no longer live during stable pruning. Retain only the returned
        // identities and accumulated dead-ID buffer before reserving the one pruning companion
        // vector, otherwise the same scratch capacity would be charged in both phases.
        reservation.resize(
            modeled_vec_capacity_bytes::<MetricSeries>(listed.capacity())
                .saturating_add(retained_identity_bytes)
                .saturating_add(modeled_vec_capacity_bytes::<SeriesId>(
                    dead_series_ids.capacity(),
                )),
        )?;
        context.prune_dead_materialized_series_ids_if_stable(
            dead_series_ids,
            Some(generation_before),
            execution,
        )?;

        reservation.resize(
            super::metadata_series_selection::modeled_metric_series_vec_retained_bytes(&listed),
        )?;
        Ok(SelectSeriesExecutionResult::accounted(listed, reservation))
    }

    pub(in crate::engine::storage_engine) fn list_metrics_with_wal_result_api(
        &self,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        let context = self.metadata_listing_context();
        self.ensure_open()?;
        let mut listed = self.list_metrics_result_api(execution)?;
        let mut listed_reservation = listed.take_memory_reservation().ok_or_else(|| {
            crate::TsinkError::Other(
                "list_metrics omitted its retained-memory reservation".to_string(),
            )
        })?;

        listed.series.sort_unstable();
        listed.series.dedup();
        listed_reservation.resize(
            super::metadata_series_selection::modeled_metric_series_vec_retained_bytes(
                &listed.series,
            ),
        )?;
        let mut wal_series = context.wal_metric_series_result(&listed.series, execution)?;
        let mut wal_reservation = wal_series.take_memory_reservation().ok_or_else(|| {
            crate::TsinkError::Other(
                "WAL metadata listing omitted its retained-memory reservation".to_string(),
            )
        })?;
        wal_series.series.sort_unstable();
        wal_series.series.dedup();
        wal_series
            .series
            .retain(|series| listed.series.binary_search(series).is_err());
        wal_reservation.resize(
            super::metadata_series_selection::modeled_metric_series_vec_retained_bytes(
                &wal_series.series,
            ),
        )?;

        if wal_series.series.is_empty() {
            drop(wal_series);
            drop(wal_reservation);
            return Ok(SelectSeriesExecutionResult::accounted(
                std::mem::take(&mut listed.series),
                listed_reservation,
            ));
        }

        let final_len = listed
            .series
            .len()
            .checked_add(wal_series.series.len())
            .ok_or_else(|| {
                crate::TsinkError::Other(
                    "WAL metadata union exceeds the supported series count".to_string(),
                )
            })?;
        let additional_series = u64::try_from(wal_series.series.len()).unwrap_or(u64::MAX);
        let additional_returned_bytes = super::modeled_metric_series_bytes(&wal_series.series);
        execution.ensure_series_matched(additional_series)?;
        execution.ensure_returned_bytes(additional_returned_bytes)?;
        execution.observe_intermediate_vector_size(u64::try_from(final_len).unwrap_or(u64::MAX))?;

        let projected_capacity = super::modeled_vec_growth_capacity_upper(final_len);
        let mut merged_reservation = execution.reserve_memory(modeled_vec_capacity_bytes::<
            MetricSeries,
        >(projected_capacity))?;
        let mut merged = Vec::new();
        merged.try_reserve(final_len).map_err(|err| {
            crate::TsinkError::Other(format!("failed to reserve WAL metadata union: {err}"))
        })?;
        merged_reservation.resize(modeled_vec_capacity_bytes::<MetricSeries>(
            merged.capacity(),
        ))?;
        execution.charge_series_matched(additional_series)?;
        execution.charge_returned_bytes(additional_returned_bytes)?;

        merged.extend(std::mem::take(&mut listed.series));
        merged.extend(std::mem::take(&mut wal_series.series));
        merged.sort_unstable();
        let retained_bytes =
            super::metadata_series_selection::modeled_metric_series_vec_retained_bytes(&merged);
        let mut source_reservations = [listed_reservation, wal_reservation, merged_reservation];
        let reservation = match execution
            .coalesce_memory_reservation_array(&mut source_reservations, retained_bytes)
        {
            Ok(reservation) => reservation,
            Err(error) => {
                // The merged vector owns the allocations represented by all three guards.
                drop(merged);
                drop(source_reservations);
                return Err(match error {
                    crate::query_budget::QueryMemoryCoalesceError::Budget(error) => error.into(),
                    crate::query_budget::QueryMemoryCoalesceError::InvalidReservations => {
                        crate::TsinkError::Other(
                            "WAL metadata union received incompatible query-memory reservations"
                                .to_string(),
                        )
                    }
                });
            }
        };
        Ok(SelectSeriesExecutionResult::accounted(merged, reservation))
    }

    pub(in crate::engine::storage_engine) fn list_metrics_in_shards_result_api(
        &self,
        scope: &crate::storage::MetadataShardScope,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        let scope = scope.normalized()?;
        // The full metadata listing is ordered by the canonical SeriesId-backed materialized
        // set. The shard backend emits that same order, so keep it instead of applying the
        // selection API's public MetricSeries ordering.
        self.select_series_with_optional_scope_result_impl(
            &SeriesSelection::new(),
            Some(scope),
            execution,
            "list_metrics_in_shards",
            true,
        )
    }

    pub(in crate::engine::storage_engine) fn select_series_result_api(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.select_series_with_optional_scope_result_api(
            selection,
            None,
            execution,
            "select_series_in_shards",
        )
    }

    pub(in crate::engine::storage_engine) fn select_series_in_shards_result_api(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        let scope = scope.normalized()?;
        self.select_series_with_optional_scope_result_api(
            selection,
            Some(scope),
            execution,
            "select_series_in_shards",
        )
    }

    fn select_series_with_optional_scope_result_api(
        &self,
        selection: &SeriesSelection,
        scope: Option<crate::storage::MetadataShardScope>,
        execution: &QueryExecution,
        shard_operation: &'static str,
    ) -> Result<SelectSeriesExecutionResult> {
        self.observability
            .query
            .select_series_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = self.select_series_with_optional_scope_result_impl(
            selection,
            scope,
            execution,
            shard_operation,
            false,
        );

        self.observability
            .query
            .select_series_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                self.observability
                    .query
                    .select_series_returned_total
                    .fetch_add(
                        saturating_u64_from_usize(series.series.len()),
                        Ordering::Relaxed,
                    );
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

    fn select_series_with_optional_scope_result_impl(
        &self,
        selection: &SeriesSelection,
        scope: Option<crate::storage::MetadataShardScope>,
        execution: &QueryExecution,
        shard_operation: &'static str,
        preserve_backend_order: bool,
    ) -> Result<SelectSeriesExecutionResult> {
        execution.checkpoint()?;
        self.ensure_open()?;
        self.request_background_persisted_refresh_if_needed();
        if let Some((start, end)) = selection.normalized_time_range()? {
            self.record_query_tier_plan(self.query_tier_plan(start, end));
            #[cfg(test)]
            self.invoke_metadata_query_time_range_summary_hook();
        }
        let (series, reservation) = match scope.as_ref() {
            Some(scope) => self.select_series_in_shards_impl_result(
                selection,
                scope,
                execution,
                shard_operation,
                preserve_backend_order,
            ),
            None => self.select_series_impl_with_execution_result(selection, execution),
        }?;
        self.charge_series_query_result(execution, 0, 0, &series)?;
        Ok(SelectSeriesExecutionResult::accounted(series, reservation))
    }
}
