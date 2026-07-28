use std::sync::atomic::Ordering;

use parking_lot::{RwLock, RwLockReadGuard};
use roaring::RoaringTreemap;

use crate::engine::query::TieredQueryPlan;
use crate::engine::series::SeriesId;
use crate::query_matcher::CompiledSeriesMatcher;
use crate::storage::{
    MetadataShardScope, QueryRowsExecutionResult, QueryRowsPage, QueryRowsScanOptions,
    ShardWindowDigest, ShardWindowRowsExecutionResult, ShardWindowRowsPage, ShardWindowScanOptions,
};
use crate::value::modeled_query_value_payload_bytes;
use crate::{DataPoint, Label, MetricSeries, Result, Row, SeriesSelection, TsinkError, Value};

use super::core_impl::VisibilityCacheReadContext;
use super::query_read::{
    apply_offset_limit_in_place, dedupe_last_value_per_timestamp, PersistedTierFetchStats,
    RawSeriesPagination, RawSeriesScanPage,
};
use super::rollups::RollupQueryCandidate;
#[cfg(test)]
use super::tiering;
use super::tiering::RetentionTierPolicy;
#[cfg(test)]
use super::IngestCommitHook;
use super::{
    elapsed_nanos_u64, rollups, saturating_u64_from_usize, state, value_heap_bytes, Chunk,
    ChunkContext, ChunkStorage, PersistedChunkRef, PersistedIndexState, QueryExecution,
    SealedChunkKey, SeriesRegistry, SeriesVisibilitySummary,
};

#[path = "query_exec/candidate_planner.rs"]
mod candidate_planner;
#[path = "query_exec/metadata_api.rs"]
mod metadata_api;
#[path = "query_exec/metadata_context.rs"]
mod metadata_context;
#[path = "query_exec/metadata_postings.rs"]
mod metadata_postings;
#[path = "query_exec/metadata_series_selection.rs"]
mod metadata_series_selection;
#[path = "query_exec/read_context.rs"]
mod read_context;
#[path = "query_exec/scan_api.rs"]
mod scan_api;
#[path = "query_exec/select_api.rs"]
mod select_api;
#[path = "query_exec/time_range_filter.rs"]
mod time_range_filter;
#[path = "query_exec/time_range_planner.rs"]
mod time_range_planner;

use metadata_context::{MetadataSelectionContext, RuntimeMetadataCandidatePlan};
pub(crate) use metadata_series_selection::modeled_metric_series_vec_retained_bytes;
use read_context::TimeRangeFilterContext;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn reserve_metadata_candidate_working_set(
        &self,
        execution: &QueryExecution,
    ) -> Result<crate::QueryMemoryReservation> {
        execution.checkpoint()?;
        let series_count = {
            let registry = self.catalog.registry.read();
            saturating_u64_from_usize(registry.series_count())
        };
        // Candidate planning can transiently retain a seed, a matcher bitmap, a live-filtered
        // bitmap, and scope IDs. Admit the complete collection model before any of those
        // collections are cloned or built.
        execution
            .reserve_memory(modeled_metadata_candidate_working_set_bytes(series_count))
            .map_err(Into::into)
    }

    pub(in crate::engine::storage_engine) fn charge_point_query_result(
        &self,
        execution: &QueryExecution,
        matched_series: u64,
        samples_scanned: u64,
        points: &[DataPoint],
    ) -> Result<()> {
        execution.checkpoint()?;
        execution.charge_series_matched(matched_series)?;
        execution.charge_samples_scanned(samples_scanned)?;
        let returned = saturating_u64_from_usize(points.len());
        execution.observe_intermediate_vector_size(returned)?;
        let bytes = modeled_points_returned_bytes(points);
        execution.charge_samples_returned(returned)?;
        execution.charge_returned_bytes(bytes)?;
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn charge_series_query_result(
        &self,
        execution: &QueryExecution,
        matched_series: u64,
        pattern_expansion: u64,
        series: &[MetricSeries],
    ) -> Result<()> {
        execution.checkpoint()?;
        execution.charge_pattern_expansion(pattern_expansion)?;
        execution.charge_series_matched(matched_series)?;
        let size = saturating_u64_from_usize(series.len());
        execution.observe_intermediate_vector_size(size)?;
        let bytes = modeled_metric_series_bytes(series);
        execution.charge_returned_bytes(bytes)?;
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn charge_row_before_materialization(
        &self,
        execution: &QueryExecution,
        metric: &str,
        labels: &[Label],
        point: &DataPoint,
    ) -> Result<()> {
        execution.checkpoint()?;
        execution.charge_samples_returned(1)?;
        execution.charge_returned_bytes(modeled_row_parts_returned_bytes(metric, labels, point))?;
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn series_exists(
        &self,
        metric: &str,
        labels: &[Label],
    ) -> bool {
        self.catalog
            .registry
            .read()
            .resolve_existing(metric, labels)
            .is_some()
    }

    pub(super) fn query_tier_plan(&self, start: i64, end: i64) -> TieredQueryPlan {
        self.query_planning_context().query_tier_plan(start, end)
    }

    fn record_query_tier_plan(&self, plan: TieredQueryPlan) {
        if plan.is_hot_only() {
            self.observability
                .query
                .hot_only_query_plans_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if plan.includes_warm() {
            self.observability
                .query
                .warm_tier_query_plans_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if plan.includes_cold() {
            self.observability
                .query
                .cold_tier_query_plans_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_persisted_tier_fetch_stats(&self, stats: PersistedTierFetchStats) {
        self.observability
            .query
            .hot_tier_persisted_chunks_read_total
            .fetch_add(stats.hot_persisted_chunks_read, Ordering::Relaxed);
        self.observability
            .query
            .warm_tier_persisted_chunks_read_total
            .fetch_add(stats.warm_persisted_chunks_read, Ordering::Relaxed);
        self.observability
            .query
            .cold_tier_persisted_chunks_read_total
            .fetch_add(stats.cold_persisted_chunks_read, Ordering::Relaxed);
        self.observability
            .query
            .warm_tier_fetch_duration_nanos_total
            .fetch_add(stats.warm_fetch_duration_nanos, Ordering::Relaxed);
        self.observability
            .query
            .cold_tier_fetch_duration_nanos_total
            .fetch_add(stats.cold_fetch_duration_nanos, Ordering::Relaxed);
    }

    fn request_background_persisted_refresh_if_needed(&self) {
        if self.persisted.persisted_index_dirty.load(Ordering::SeqCst)
            || self.should_refresh_remote_catalog()
        {
            self.notify_persisted_refresh_thread();
        }
    }
}

pub(super) fn modeled_points_returned_bytes(points: &[DataPoint]) -> u64 {
    points.iter().fold(0u64, |bytes, point| {
        bytes
            .saturating_add(u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX))
            .saturating_add(modeled_query_value_payload_bytes(&point.value))
    })
}

/// Fixed allocator/slack allowance used by the query-memory model for one non-empty heap
/// collection allocation.
///
/// `max_memory_bytes` is intentionally a portable allocation *model*, rather than a claim about
/// the private bookkeeping of every Rust global allocator. The element payload and observed
/// capacities are exact; this allowance covers collection headers, alignment, and allocator
/// metadata in the model. Keeping it named makes every estimate auditable and boundary-testable.
pub(super) const QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

/// Number of simultaneously live candidate bitmaps in the metadata planner: seed, matcher,
/// live-filtered result, and time/scope-filtered result.
const QUERY_METADATA_CANDIDATE_BITMAP_COPIES: u64 = 4;

/// Roaring's sparse representation can require an identifier plus container/index bookkeeping.
/// The model charges four machine words for each candidate in each live bitmap.
const QUERY_MODELED_BITMAP_BYTES_PER_SERIES: u64 =
    (std::mem::size_of::<SeriesId>() as u64).saturating_mul(4);

/// One materialized scope/candidate-id vector can coexist with the planner bitmaps.
const QUERY_METADATA_CANDIDATE_ID_VECTOR_COPIES: u64 = 1;

pub(super) fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    let elements = saturating_u64_from_usize(capacity)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX));
    if capacity == 0 {
        0
    } else {
        elements.saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

pub(super) fn modeled_vec_growth_capacity_upper(elements: usize) -> usize {
    if elements == 0 {
        return 0;
    }
    elements
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4)
}

pub(super) fn modeled_points_retained_bytes(points: &Vec<DataPoint>) -> u64 {
    modeled_vec_capacity_bytes::<DataPoint>(points.capacity()).saturating_add(
        points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(modeled_value_retained_bytes(&point.value))
        }),
    )
}

pub(super) fn modeled_point_output_upper_bound_bytes(points: &[DataPoint], capacity: usize) -> u64 {
    // A built-in aggregation emits each input value at most once. First/last/min/max can clone a
    // variable-size value; numeric aggregations emit inline values. Summing every input payload is
    // therefore a conservative heap upper bound for both one-shot and bucketed built-ins.
    modeled_vec_capacity_bytes::<DataPoint>(capacity).saturating_add(
        points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(modeled_value_retained_bytes(&point.value))
        }),
    )
}

pub(super) fn modeled_point_result_upper_bound_bytes(points: &[DataPoint], elements: usize) -> u64 {
    saturating_u64_from_usize(elements)
        .saturating_mul(u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX))
        .saturating_add(points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(modeled_query_value_payload_bytes(&point.value))
        }))
}

pub(super) fn modeled_metadata_candidate_working_set_bytes(series_count: u64) -> u64 {
    let bitmap_bytes = series_count
        .saturating_mul(QUERY_MODELED_BITMAP_BYTES_PER_SERIES)
        .saturating_mul(QUERY_METADATA_CANDIDATE_BITMAP_COPIES);
    let id_vector_bytes = series_count
        .saturating_mul(u64::try_from(std::mem::size_of::<SeriesId>()).unwrap_or(u64::MAX))
        .saturating_mul(QUERY_METADATA_CANDIDATE_ID_VECTOR_COPIES);
    let allocations = QUERY_METADATA_CANDIDATE_BITMAP_COPIES
        .saturating_add(QUERY_METADATA_CANDIDATE_ID_VECTOR_COPIES)
        .saturating_mul(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES);
    bitmap_bytes
        .saturating_add(id_vector_bytes)
        .saturating_add(allocations)
}

pub(super) fn modeled_labels_bytes(labels: &[Label]) -> u64 {
    labels.iter().fold(0u64, |bytes, label| {
        bytes.saturating_add(
            u64::try_from(
                std::mem::size_of::<Label>()
                    .saturating_add(label.name.len())
                    .saturating_add(label.value.len()),
            )
            .unwrap_or(u64::MAX),
        )
    })
}

pub(super) fn modeled_metric_series_bytes(series: &[MetricSeries]) -> u64 {
    series.iter().fold(0u64, |bytes, series| {
        bytes
            .saturating_add(u64::try_from(std::mem::size_of::<MetricSeries>()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(series.name.len()).unwrap_or(u64::MAX))
            .saturating_add(modeled_labels_bytes(&series.labels))
    })
}

pub(super) fn modeled_metric_series_shape_bytes(
    metric_bytes: usize,
    label_count: usize,
    label_text_bytes: usize,
) -> u64 {
    u64::try_from(std::mem::size_of::<MetricSeries>())
        .unwrap_or(u64::MAX)
        .saturating_add(saturating_u64_from_usize(metric_bytes))
        .saturating_add(
            saturating_u64_from_usize(label_count)
                .saturating_mul(u64::try_from(std::mem::size_of::<Label>()).unwrap_or(u64::MAX)),
        )
        .saturating_add(saturating_u64_from_usize(label_text_bytes))
}

pub(super) fn modeled_metric_series_shape_retained_bytes(
    metric_bytes: usize,
    label_count: usize,
    label_text_bytes: usize,
) -> u64 {
    let metric_allocation = if metric_bytes == 0 {
        0
    } else {
        saturating_u64_from_usize(metric_bytes)
            .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    };
    // Each label can retain a name allocation and a value allocation. Charging both allowances
    // is conservative for empty strings, which do not allocate.
    let label_string_allocations = saturating_u64_from_usize(label_count)
        .saturating_mul(2)
        .saturating_mul(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES);
    metric_allocation
        .saturating_add(modeled_vec_capacity_bytes::<Label>(
            modeled_vec_growth_capacity_upper(label_count),
        ))
        .saturating_add(saturating_u64_from_usize(label_text_bytes))
        .saturating_add(label_string_allocations)
}

pub(super) fn modeled_string_capacity_bytes(capacity: usize) -> u64 {
    if capacity == 0 {
        0
    } else {
        saturating_u64_from_usize(capacity)
            .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

pub(super) fn modeled_value_retained_bytes(value: &Value) -> u64 {
    let heap_bytes = u64::try_from(value_heap_bytes(value)).unwrap_or(u64::MAX);
    match value {
        Value::Bytes(bytes) => heap_bytes.saturating_add(if bytes.capacity() != 0 {
            QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES
        } else {
            0
        }),
        Value::String(text) => heap_bytes.saturating_add(if text.capacity() != 0 {
            QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES
        } else {
            0
        }),
        Value::Histogram(histogram) => {
            let vector_allocations = [
                histogram.negative_spans.capacity(),
                histogram.negative_deltas.capacity(),
                histogram.negative_counts.capacity(),
                histogram.positive_spans.capacity(),
                histogram.positive_deltas.capacity(),
                histogram.positive_counts.capacity(),
                histogram.custom_values.capacity(),
            ]
            .into_iter()
            .filter(|capacity| *capacity != 0)
            .count();
            heap_bytes.saturating_add(
                saturating_u64_from_usize(vector_allocations.saturating_add(1))
                    .saturating_mul(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
            )
        }
        Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => 0,
    }
}

pub(super) fn modeled_metric_series_retained_bytes(series: &MetricSeries) -> u64 {
    modeled_string_capacity_bytes(series.name.capacity())
        .saturating_add(modeled_vec_capacity_bytes::<Label>(
            series.labels.capacity(),
        ))
        .saturating_add(series.labels.iter().fold(0u64, |bytes, label| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(label.name.capacity()))
                .saturating_add(modeled_string_capacity_bytes(label.value.capacity()))
        }))
}

fn modeled_row_owned_retained_bytes(row: &Row) -> u64 {
    modeled_string_capacity_bytes(row.metric_capacity())
        .saturating_add(modeled_vec_capacity_bytes::<Label>(row.labels_capacity()))
        .saturating_add(row.labels().iter().fold(0u64, |bytes, label| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(label.name.capacity()))
                .saturating_add(modeled_string_capacity_bytes(label.value.capacity()))
        }))
        .saturating_add(modeled_value_retained_bytes(&row.data_point().value))
}

fn modeled_row_parts_retained_upper_bytes(
    metric: &String,
    labels: &Vec<Label>,
    point: &DataPoint,
) -> u64 {
    modeled_string_capacity_bytes(metric.capacity())
        .saturating_add(modeled_vec_capacity_bytes::<Label>(labels.capacity()))
        .saturating_add(labels.iter().fold(0u64, |bytes, label| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(label.name.capacity()))
                .saturating_add(modeled_string_capacity_bytes(label.value.capacity()))
        }))
        .saturating_add(modeled_value_retained_bytes(&point.value))
}

fn modeled_rows_append_upper_bytes(
    rows: &Vec<Row>,
    metric: &String,
    labels: &Vec<Label>,
    points: &[DataPoint],
) -> u64 {
    let next_len = rows.len().saturating_add(points.len());
    let row_capacity = rows
        .capacity()
        .max(modeled_vec_growth_capacity_upper(next_len));
    modeled_vec_capacity_bytes::<Row>(row_capacity)
        .saturating_add(rows.iter().fold(0u64, |bytes, row| {
            bytes.saturating_add(modeled_row_owned_retained_bytes(row))
        }))
        .saturating_add(points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(modeled_row_parts_retained_upper_bytes(
                metric, labels, point,
            ))
        }))
}

/// Returns the complete retained-memory model for an owned query row vector.
///
/// This is public so execution-aware adapters can verify that a backend claiming complete
/// accounting retained a reservation for every live allocation in its returned page.
#[doc(hidden)]
#[must_use]
pub fn modeled_query_rows_retained_bytes(rows: &Vec<Row>) -> u64 {
    modeled_vec_capacity_bytes::<Row>(rows.capacity()).saturating_add(
        rows.iter().fold(0u64, |bytes, row| {
            bytes.saturating_add(modeled_row_owned_retained_bytes(row))
        }),
    )
}

fn modeled_row_parts_returned_bytes(metric: &str, labels: &[Label], point: &DataPoint) -> u64 {
    u64::try_from(std::mem::size_of::<crate::Row>())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(metric.len()).unwrap_or(u64::MAX))
        .saturating_add(modeled_labels_bytes(labels))
        // `Row` owns its `DataPoint` inline, so only the value's logical payload is additional.
        .saturating_add(modeled_query_value_payload_bytes(&point.value))
}

const SHARD_WINDOW_FNV_OFFSET_BASIS: u64 = crate::storage::SHARD_WINDOW_FNV_OFFSET_BASIS;

fn validate_shard_window_request(
    shard: u32,
    shard_count: u32,
    window_start: i64,
    window_end: i64,
) -> Result<()> {
    crate::storage::validate_shard_window_request(shard, shard_count, window_start, window_end)
}

fn validate_shard_window_scan_options(options: ShardWindowScanOptions) -> Result<()> {
    crate::storage::validate_shard_window_scan_options(options)
}

fn validate_query_rows_scan_options(options: QueryRowsScanOptions) -> Result<()> {
    crate::storage::validate_query_rows_scan_options(options)
}

fn shard_window_hash_data_point(point: &DataPoint) -> Result<u64> {
    crate::storage::shard_window_hash_data_point(point)
}

fn shard_window_fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    crate::storage::shard_window_fnv1a_update(hash, bytes)
}

struct ShardWindowSortablePoint {
    point: DataPoint,
    json_key: Vec<u8>,
}

#[derive(Default)]
struct JsonByteCounter {
    bytes: usize,
}

impl std::io::Write for JsonByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("JSON byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn shard_window_value_json_bytes(value: &Value) -> Result<usize> {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.bytes)
}

pub(super) fn sort_data_points_for_shard_window_with_execution(
    points: &mut Vec<DataPoint>,
    execution: &QueryExecution,
) -> Result<()> {
    execution.checkpoint()?;
    execution.observe_intermediate_vector_size(u64::try_from(points.len()).unwrap_or(u64::MAX))?;

    let pair_capacity = modeled_vec_growth_capacity_upper(points.len());
    let mut scratch_bytes = modeled_vec_capacity_bytes::<ShardWindowSortablePoint>(pair_capacity);
    for point in points.iter() {
        execution.checkpoint()?;
        let json_bytes = shard_window_value_json_bytes(&point.value)?;
        scratch_bytes = scratch_bytes.saturating_add(modeled_vec_capacity_bytes::<u8>(
            modeled_vec_growth_capacity_upper(json_bytes),
        ));
    }

    let mut scratch_reservation = execution.reserve_memory(scratch_bytes)?;
    let point_count = points.len();
    let mut sortable = Vec::with_capacity(point_count);
    for point in points.drain(..) {
        execution.checkpoint()?;
        let json_bytes = shard_window_value_json_bytes(&point.value)?;
        let mut json_key = Vec::with_capacity(json_bytes);
        serde_json::to_writer(&mut json_key, &point.value)?;
        if json_key.len() != json_bytes {
            return Err(TsinkError::Other(
                "shard-window JSON key length changed during serialization".to_string(),
            ));
        }
        sortable.push(ShardWindowSortablePoint { point, json_key });
    }

    let actual_scratch_bytes = modeled_vec_capacity_bytes::<ShardWindowSortablePoint>(
        sortable.capacity(),
    )
    .saturating_add(sortable.iter().fold(0u64, |bytes, item| {
        bytes.saturating_add(modeled_vec_capacity_bytes::<u8>(item.json_key.capacity()))
    }));
    scratch_reservation.resize(actual_scratch_bytes)?;

    execution.checkpoint()?;
    sortable.sort_unstable_by(|left, right| {
        left.point
            .timestamp
            .cmp(&right.point.timestamp)
            .then_with(|| left.json_key.cmp(&right.json_key))
    });
    execution.checkpoint()?;
    for item in sortable {
        execution.checkpoint()?;
        points.push(item.point);
    }
    Ok(())
}
