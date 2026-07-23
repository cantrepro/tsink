use crate::engine::query::{
    decode_chunk_points_in_range_into, decode_encoded_chunk_payload_in_range_into,
    EncodedChunkDescriptor, TieredQueryPlan,
};
use crate::engine::tombstone;
use parking_lot::RwLockReadGuard;

use super::query_exec::{
    modeled_points_bytes, modeled_vec_capacity_bytes, QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES,
};
use super::state::{ActiveSeriesSnapshot, ActiveSeriesSnapshotCursor};
use super::tiering::PersistedSegmentTier;
use super::*;

#[path = "query_read/analysis.rs"]
mod analysis;
#[path = "query_read/append_sort.rs"]
mod append_sort;
#[path = "query_read/merge.rs"]
mod merge;
#[path = "query_read/pagination.rs"]
mod pagination;
#[path = "query_read/snapshot.rs"]
mod snapshot;

#[cfg(test)]
#[path = "query_read/tests.rs"]
mod tests;

pub(super) use pagination::{RawSeriesPagination, RawSeriesScanPage};

/// Upper bound used by the query allocation model for variable-width decoded payload per encoded
/// byte. Fixed `DataPoint`/`Value` storage is charged separately from this payload expansion.
const QUERY_DECODE_VARIABLE_PAYLOAD_BYTES_PER_ENCODED_BYTE: u64 = 8;

/// One modeled control byte per occupied hash-table bucket, plus the exact key/value payload.
const QUERY_HASH_TABLE_CONTROL_BYTES_PER_BUCKET: u64 = 1;

/// A B-tree entry retains the key plus parent/child/index bookkeeping. Four machine words per
/// entry is the portable query model; the collection allocation allowance is charged separately.
const QUERY_BTREE_BOOKKEEPING_WORDS_PER_ENTRY: u64 = 4;

fn encoded_decode_heap_upper_bound(encoded_bytes: u64) -> u64 {
    // Every decoded variable-size primitive consumes at most eight payload bytes while its
    // encoded form consumes at least one. Fixed DataPoint/Value storage is accounted separately.
    encoded_bytes.saturating_mul(QUERY_DECODE_VARIABLE_PAYLOAD_BYTES_PER_ENCODED_BYTE)
}

fn modeled_hash_map_capacity_bytes<K, V>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    let bucket_payload = u64::try_from(
        std::mem::size_of::<K>()
            .saturating_add(std::mem::size_of::<V>())
            .saturating_add(
                usize::try_from(QUERY_HASH_TABLE_CONTROL_BYTES_PER_BUCKET).unwrap_or(1),
            ),
    )
    .unwrap_or(u64::MAX);
    saturating_u64_from_usize(capacity)
        .saturating_mul(bucket_payload)
        .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_btree_set_entries_bytes<T>(entries: usize) -> u64 {
    if entries == 0 {
        return 0;
    }
    let entry = u64::try_from(std::mem::size_of::<T>())
        .unwrap_or(u64::MAX)
        .saturating_add(
            u64::try_from(std::mem::size_of::<usize>())
                .unwrap_or(u64::MAX)
                .saturating_mul(QUERY_BTREE_BOOKKEEPING_WORDS_PER_ENTRY),
        );
    saturating_u64_from_usize(entries)
        .saturating_mul(entry)
        .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_persisted_snapshot_build_upper_bound(candidate_chunks: usize) -> u64 {
    modeled_vec_capacity_bytes::<PersistedChunkRef>(candidate_chunks)
        .saturating_add(modeled_btree_set_entries_bytes::<usize>(candidate_chunks))
        .saturating_add(modeled_hash_map_capacity_bytes::<usize, Arc<PlatformMmap>>(
            candidate_chunks,
        ))
        .saturating_add(
            modeled_hash_map_capacity_bytes::<usize, PersistedSegmentTier>(candidate_chunks),
        )
}

fn modeled_persisted_snapshot_retained_bytes(
    snapshot: &snapshot::PersistedSeriesSourceSnapshot,
) -> u64 {
    modeled_vec_capacity_bytes::<PersistedChunkRef>(snapshot.chunks.capacity())
        .saturating_add(modeled_hash_map_capacity_bytes::<usize, Arc<PlatformMmap>>(
            snapshot.segment_maps.capacity(),
        ))
        .saturating_add(
            modeled_hash_map_capacity_bytes::<usize, PersistedSegmentTier>(
                snapshot.segment_tiers.capacity(),
            ),
        )
}

fn sealed_chunk_heap_upper_bound(chunk: &Chunk) -> u64 {
    if !chunk.points.is_empty() {
        return chunk.points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(u64::try_from(value_heap_bytes(&point.value)).unwrap_or(u64::MAX))
        });
    }
    encoded_decode_heap_upper_bound(u64::try_from(chunk.encoded_payload.len()).unwrap_or(u64::MAX))
}

fn reserve_query_read_working_set(
    execution: Option<&QueryExecution>,
    snapshot: &snapshot::SeriesReadSnapshot,
    output_points: usize,
) -> Result<Option<crate::QueryMemoryReservation>> {
    let Some(execution) = execution else {
        return Ok(None);
    };
    execution.checkpoint()?;
    let output_points = saturating_u64_from_usize(output_points);
    execution.observe_intermediate_vector_size(output_points)?;

    let fixed_point_bytes = u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX);
    let fixed_output_bytes = output_points.saturating_mul(fixed_point_bytes);
    let mut output_heap_upper = 0u64;
    let mut max_chunk_scratch = 0u64;
    for chunk_ref in &snapshot.persisted.chunks {
        let heap_upper = encoded_decode_heap_upper_bound(u64::from(chunk_ref.chunk_len));
        output_heap_upper = output_heap_upper.saturating_add(heap_upper);
        max_chunk_scratch = max_chunk_scratch.max(
            u64::from(chunk_ref.point_count)
                .saturating_mul(fixed_point_bytes)
                .saturating_add(heap_upper),
        );
    }
    for chunk in &snapshot.sealed_chunks {
        let heap_upper = sealed_chunk_heap_upper_bound(chunk);
        output_heap_upper = output_heap_upper.saturating_add(heap_upper);
        max_chunk_scratch = max_chunk_scratch.max(
            u64::from(chunk.header.point_count)
                .saturating_mul(fixed_point_bytes)
                .saturating_add(heap_upper),
        );
    }
    for point in snapshot.active_points.iter_points_in_partition_order() {
        output_heap_upper = output_heap_upper
            .saturating_add(u64::try_from(value_heap_bytes(&point.value)).unwrap_or(u64::MAX));
    }

    let bytes = fixed_output_bytes
        .saturating_add(output_heap_upper)
        .saturating_add(max_chunk_scratch);
    execution
        .reserve_memory(bytes)
        .map(Some)
        .map_err(Into::into)
}

#[derive(Clone, Copy)]
struct QuerySnapshotContext<'a> {
    chunks: ChunkContext<'a>,
    persisted_index: &'a RwLock<PersistedIndexState>,
    visibility_fence: &'a RwLock<()>,
    partition_window: i64,
}

impl<'a> QuerySnapshotContext<'a> {
    fn visibility_read_fence(self) -> RwLockReadGuard<'a, ()> {
        self.visibility_fence.read()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct PersistedTierFetchStats {
    pub(super) hot_persisted_chunks_read: u64,
    pub(super) warm_persisted_chunks_read: u64,
    pub(super) cold_persisted_chunks_read: u64,
    pub(super) warm_fetch_duration_nanos: u64,
    pub(super) cold_fetch_duration_nanos: u64,
}

impl PersistedTierFetchStats {
    fn record_chunk(&mut self, tier: PersistedSegmentTier, duration_nanos: u64) {
        match tier {
            PersistedSegmentTier::Hot => {
                self.hot_persisted_chunks_read = self.hot_persisted_chunks_read.saturating_add(1);
            }
            PersistedSegmentTier::Warm => {
                self.warm_persisted_chunks_read = self.warm_persisted_chunks_read.saturating_add(1);
                self.warm_fetch_duration_nanos = self
                    .warm_fetch_duration_nanos
                    .saturating_add(duration_nanos);
            }
            PersistedSegmentTier::Cold => {
                self.cold_persisted_chunks_read = self.cold_persisted_chunks_read.saturating_add(1);
                self.cold_fetch_duration_nanos = self
                    .cold_fetch_duration_nanos
                    .saturating_add(duration_nanos);
            }
        }
    }

    pub(super) fn accumulate(&mut self, other: Self) {
        self.hot_persisted_chunks_read = self
            .hot_persisted_chunks_read
            .saturating_add(other.hot_persisted_chunks_read);
        self.warm_persisted_chunks_read = self
            .warm_persisted_chunks_read
            .saturating_add(other.warm_persisted_chunks_read);
        self.cold_persisted_chunks_read = self
            .cold_persisted_chunks_read
            .saturating_add(other.cold_persisted_chunks_read);
        self.warm_fetch_duration_nanos = self
            .warm_fetch_duration_nanos
            .saturating_add(other.warm_fetch_duration_nanos);
        self.cold_fetch_duration_nanos = self
            .cold_fetch_duration_nanos
            .saturating_add(other.cold_fetch_duration_nanos);
    }
}

impl ChunkStorage {
    fn query_snapshot_context(&self) -> QuerySnapshotContext<'_> {
        QuerySnapshotContext {
            chunks: self.chunk_context(),
            persisted_index: &self.persisted.persisted_index,
            visibility_fence: &self.visibility.flush_visibility_lock,
            partition_window: self.runtime.partition_window,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn collect_points_for_series_into_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<PersistedTierFetchStats> {
        let (snapshot, shard_snapshot_stats) = self
            .query_snapshot_context()
            .snapshot_series_read_sources(series_id, start, end, plan, execution)?;
        self.record_merge_path_shard_snapshot_stats(shard_snapshot_stats);
        if enforce_return_limit {
            if let Some(execution) = execution {
                execution.ensure_samples_returned(saturating_u64_from_usize(
                    snapshot.analysis.estimated_points,
                ))?;
                execution.ensure_returned_bytes(
                    saturating_u64_from_usize(snapshot.analysis.estimated_points).saturating_mul(
                        u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX),
                    ),
                )?;
            }
        }
        #[cfg(test)]
        self.invoke_query_merge_in_memory_source_snapshot_hook();

        if snapshot.analysis.can_use_merge_path() {
            self.observability
                .query
                .merge_path_queries_total
                .fetch_add(1, Ordering::Relaxed);
            return self
                .execute_series_read_merge_path(series_id, start, end, snapshot, out, execution);
        }

        self.observability
            .query
            .append_sort_path_queries_total
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.invoke_query_append_sort_in_memory_source_snapshot_hook();
        self.execute_series_read_append_sort_path(series_id, start, end, snapshot, out, execution)
    }

    pub(super) fn collect_points_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: Option<&QueryExecution>,
        enforce_return_limit: bool,
    ) -> Result<(Vec<DataPoint>, PersistedTierFetchStats)> {
        let mut out = Vec::new();
        let stats = self.collect_points_for_series_into_with_plan(
            series_id,
            start,
            end,
            plan,
            &mut out,
            execution,
            enforce_return_limit,
        )?;
        Ok((out, stats))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_into_impl(
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
        let Some(series_id) = self
            .catalog
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            out.clear();
            return Ok(PersistedTierFetchStats::default());
        };
        self.collect_points_for_series_into_with_plan(
            series_id,
            start,
            end,
            plan,
            out,
            execution,
            enforce_return_limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_raw_series_page_with_plan(
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
        let Some(series_id) = self
            .catalog
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            return Ok(RawSeriesScanPage::default());
        };
        self.collect_raw_series_page_with_plan(
            series_id,
            start,
            end,
            plan,
            pagination.offset,
            pagination.limit,
            execution,
            enforce_return_limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn collect_raw_series_page_with_plan(
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
        let pagination = RawSeriesPagination::new(offset, limit);
        let (snapshot, shard_snapshot_stats) = self
            .query_snapshot_context()
            .snapshot_series_read_sources(series_id, start, end, plan, execution)?;
        self.record_merge_path_shard_snapshot_stats(shard_snapshot_stats);
        if enforce_return_limit {
            if let Some(execution) = execution {
                let upper = pagination
                    .limit
                    .unwrap_or(snapshot.analysis.estimated_points)
                    .min(snapshot.analysis.estimated_points);
                execution.ensure_samples_returned(saturating_u64_from_usize(upper))?;
                execution.ensure_returned_bytes(
                    saturating_u64_from_usize(upper).saturating_mul(
                        u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX),
                    ),
                )?;
            }
        }
        #[cfg(test)]
        self.invoke_query_merge_in_memory_source_snapshot_hook();

        if snapshot.analysis.can_use_merge_path() {
            self.observability
                .query
                .merge_path_queries_total
                .fetch_add(1, Ordering::Relaxed);
            return self.collect_raw_series_page_with_merge(
                series_id, start, end, snapshot, pagination, execution,
            );
        }

        self.observability
            .query
            .append_sort_path_queries_total
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.invoke_query_append_sort_in_memory_source_snapshot_hook();
        self.collect_raw_series_page_with_append_sort(
            series_id, start, end, snapshot, pagination, execution,
        )
    }
}

pub(super) fn apply_offset_limit_in_place(
    points: &mut Vec<DataPoint>,
    offset: u64,
    limit: Option<usize>,
) {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    if offset > 0 && offset < points.len() {
        points.drain(0..offset);
    } else if offset >= points.len() {
        points.clear();
    }

    if let Some(limit) = limit {
        points.truncate(limit);
    }
}

fn points_are_sorted_by_timestamp(points: &[DataPoint]) -> bool {
    points
        .windows(2)
        .all(|window| window[0].timestamp <= window[1].timestamp)
}

pub(super) fn dedupe_last_value_per_timestamp(points: &mut Vec<DataPoint>) {
    if points.len() < 2 {
        return;
    }

    points.dedup_by(|current, next| {
        if current.timestamp == next.timestamp {
            // `dedup_by` removes `next`; swap first so the latest value survives.
            std::mem::swap(current, next);
            true
        } else {
            false
        }
    });
}

fn dedupe_exact_duplicate_points(points: &mut Vec<DataPoint>) {
    if points.len() < 2 {
        return;
    }

    points.dedup_by(|current, next| {
        current.timestamp == next.timestamp && current.value == next.value
    });
}
