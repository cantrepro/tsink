use crate::engine::query::{
    decode_chunk_points_in_range_into, decode_encoded_chunk_payload_in_range_into,
    EncodedChunkDescriptor, TieredQueryPlan,
};
use crate::engine::tombstone;
use parking_lot::RwLockReadGuard;

use super::query_exec::{
    modeled_points_retained_bytes, modeled_value_retained_bytes, modeled_vec_capacity_bytes,
    modeled_vec_growth_capacity_upper, QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES,
};
use super::state::{ActivePartitionSnapshot, ActiveSeriesSnapshot, ActiveSeriesSnapshotCursor};
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

/// One modeled control byte per occupied hash-table bucket, plus the exact key/value payload.
const QUERY_HASH_TABLE_CONTROL_BYTES_PER_BUCKET: u64 = 1;

/// Conservative fixed context/input-buffer allowance for one zstd decoder. The decoded output
/// buffer and a full logical-window allowance are charged separately.
const QUERY_ZSTD_DECODE_FIXED_WORKSPACE_BYTES: u64 = 1024 * 1024;

fn decoded_value_allocation_allowance(lane: ValueLane, point_count: usize) -> u64 {
    match lane {
        ValueLane::Numeric => 0,
        // A native histogram owns its box plus seven vectors. String/Bytes values allocate fewer
        // collections, so eight allocations per point covers every blob-lane value shape.
        ValueLane::Blob => saturating_u64_from_usize(point_count)
            .saturating_mul(8)
            .saturating_mul(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
    }
}

fn modeled_vec_reserve_capacity_upper(current_capacity: usize, required_len: usize) -> usize {
    if current_capacity >= required_len {
        return current_capacity;
    }
    current_capacity
        .saturating_mul(2)
        .max(modeled_vec_growth_capacity_upper(required_len))
}

fn publish_vec_reusing_capacity<T>(out: &mut Vec<T>, mut replacement: Vec<T>) {
    if out.capacity() >= replacement.len() {
        out.clear();
        out.append(&mut replacement);
    } else {
        *out = replacement;
    }
}

fn modeled_hash_map_bucket_capacity_upper(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    modeled_vec_growth_capacity_upper(entries).saturating_mul(2)
}

fn modeled_hash_map_bucket_bytes<K, V>(bucket_capacity: usize) -> u64 {
    if bucket_capacity == 0 {
        return 0;
    }
    let bucket_payload = u64::try_from(std::mem::size_of::<(K, V)>())
        .unwrap_or(u64::MAX)
        .saturating_add(QUERY_HASH_TABLE_CONTROL_BYTES_PER_BUCKET);
    saturating_u64_from_usize(bucket_capacity)
        .saturating_mul(bucket_payload)
        .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_persisted_snapshot_build_upper_bound(candidate_chunks: usize) -> u64 {
    let vec_capacity = modeled_vec_growth_capacity_upper(candidate_chunks);
    let map_bucket_capacity = modeled_hash_map_bucket_capacity_upper(candidate_chunks);
    modeled_vec_capacity_bytes::<PersistedChunkRef>(vec_capacity)
        .saturating_add(modeled_hash_map_bucket_bytes::<usize, Arc<PlatformMmap>>(
            map_bucket_capacity,
        ))
        .saturating_add(
            modeled_hash_map_bucket_bytes::<usize, PersistedSegmentTier>(map_bucket_capacity),
        )
}

fn modeled_encoded_decode_bytes(
    lane: ValueLane,
    value_codec: chunk::ValueCodecId,
    point_count: usize,
    payload: &[u8],
) -> Result<(u64, u64)> {
    let exact_fixed_bytes = point_count
        .checked_mul(
            std::mem::size_of::<i64>()
                .saturating_add(std::mem::size_of::<Value>())
                .saturating_add(std::mem::size_of::<ChunkPoint>()),
        )
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    let modeled_peak = Encoder::modeled_decoded_chunk_peak_bytes_from_payload(
        lane,
        value_codec,
        point_count,
        payload,
    )?;
    let heap_bytes = modeled_peak.checked_sub(exact_fixed_bytes).ok_or_else(|| {
        TsinkError::DataCorruption(
            "decoded chunk memory model is smaller than its fixed vectors".to_string(),
        )
    })?;
    // Histogram decoding can leave Vec capacity below twice its logical payload. Doubling the
    // codec's exact logical heap model covers that slack; the named per-allocation allowance
    // covers boxes and allocator metadata.
    let heap_upper = saturating_u64_from_usize(heap_bytes)
        .saturating_mul(2)
        .saturating_add(decoded_value_allocation_allowance(lane, point_count));
    let capacity = modeled_vec_growth_capacity_upper(point_count);
    let scratch = modeled_vec_capacity_bytes::<i64>(capacity)
        .saturating_add(modeled_vec_capacity_bytes::<Value>(capacity))
        .saturating_add(modeled_vec_capacity_bytes::<ChunkPoint>(capacity))
        .saturating_add(modeled_vec_capacity_bytes::<DataPoint>(capacity))
        .saturating_add(heap_upper);
    Ok((heap_upper, scratch))
}

fn modeled_numeric_encoded_decode_bytes(point_count: usize) -> (u64, u64) {
    let capacity = modeled_vec_growth_capacity_upper(point_count);
    (
        0,
        modeled_vec_capacity_bytes::<i64>(capacity)
            .saturating_add(modeled_vec_capacity_bytes::<Value>(capacity))
            .saturating_add(modeled_vec_capacity_bytes::<ChunkPoint>(capacity))
            .saturating_add(modeled_vec_capacity_bytes::<DataPoint>(capacity)),
    )
}

fn modeled_zstd_decode_scratch(decoded_len: usize) -> u64 {
    modeled_vec_capacity_bytes::<u8>(modeled_vec_growth_capacity_upper(decoded_len))
        .saturating_add(saturating_u64_from_usize(decoded_len))
        .saturating_add(QUERY_ZSTD_DECODE_FIXED_WORKSPACE_BYTES)
        .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_sealed_chunk_decode_bytes(chunk: &Chunk) -> Result<(u64, u64)> {
    let point_count = usize::from(chunk.header.point_count);
    if !chunk.points.is_empty() {
        let heap_upper = chunk.points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(modeled_value_retained_bytes(&point.value))
        });
        return Ok((
            heap_upper,
            modeled_vec_capacity_bytes::<DataPoint>(modeled_vec_growth_capacity_upper(point_count))
                .saturating_add(heap_upper),
        ));
    }
    modeled_encoded_decode_bytes(
        chunk.header.lane,
        chunk.header.value_codec,
        point_count,
        &chunk.encoded_payload,
    )
}

fn reserve_query_read_working_set(
    execution: Option<&QueryExecution>,
    snapshot: &snapshot::SeriesReadSnapshot,
    output_points: usize,
    output_current_capacity: usize,
) -> Result<Option<crate::QueryMemoryReservation>> {
    let Some(execution) = execution else {
        return Ok(None);
    };
    execution.checkpoint()?;
    let output_capacity =
        modeled_vec_reserve_capacity_upper(output_current_capacity, output_points);
    let persisted_decode_capacity = snapshot
        .persisted
        .chunks
        .iter()
        .map(|chunk| usize::from(chunk.point_count))
        .max()
        .unwrap_or(0);
    let sealed_decode_capacity = snapshot
        .sealed_chunks
        .iter()
        .map(|chunk| usize::from(chunk.header.point_count))
        .max()
        .unwrap_or(0);
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(
        output_points
            .max(persisted_decode_capacity)
            .max(sealed_decode_capacity),
    ))?;

    let mut max_decompression_scratch = 0u64;
    for chunk_ref in &snapshot.persisted.chunks {
        let segment_map = snapshot
            .persisted
            .segment_maps
            .get(&chunk_ref.segment_slot)
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "missing mapped segment slot {}",
                    chunk_ref.segment_slot
                ))
            })?;
        let (decoded_len, compressed) =
            crate::engine::segment::chunk_payload_decoded_len_from_record(
                segment_map.as_slice(),
                chunk_ref.chunk_offset,
                chunk_ref.chunk_len,
            )?;
        if compressed {
            max_decompression_scratch =
                max_decompression_scratch.max(modeled_zstd_decode_scratch(decoded_len));
        }
    }
    // Blob payloads may need one preflight decompression so the codec-aware heap model can inspect
    // the logical value payload. This reservation exists before that allocation.
    let mut reservation = execution.reserve_memory(max_decompression_scratch)?;

    let fixed_output_bytes = modeled_vec_capacity_bytes::<DataPoint>(output_capacity);
    let mut output_heap_upper = 0u64;
    let mut max_persisted_chunk_scratch = 0u64;
    for chunk_ref in &snapshot.persisted.chunks {
        execution.checkpoint()?;
        let segment_map = snapshot
            .persisted
            .segment_maps
            .get(&chunk_ref.segment_slot)
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "missing mapped segment slot {}",
                    chunk_ref.segment_slot
                ))
            })?;
        let (decoded_len, compressed) =
            crate::engine::segment::chunk_payload_decoded_len_from_record(
                segment_map.as_slice(),
                chunk_ref.chunk_offset,
                chunk_ref.chunk_len,
            )?;
        let decompression_scratch = if compressed {
            modeled_zstd_decode_scratch(decoded_len)
        } else {
            0
        };
        let point_count = usize::from(chunk_ref.point_count);
        let (heap_upper, decode_scratch) = if compressed && chunk_ref.lane == ValueLane::Numeric {
            modeled_numeric_encoded_decode_bytes(point_count)
        } else {
            let payload = persisted_chunk_payload(&snapshot.persisted.segment_maps, chunk_ref)?;
            modeled_encoded_decode_bytes(
                chunk_ref.lane,
                chunk_ref.value_codec,
                point_count,
                payload.as_ref(),
            )?
        };
        output_heap_upper = output_heap_upper.saturating_add(heap_upper);
        max_persisted_chunk_scratch =
            max_persisted_chunk_scratch.max(decode_scratch.saturating_add(decompression_scratch));
    }
    let mut max_sealed_chunk_scratch = 0u64;
    for chunk in &snapshot.sealed_chunks {
        let (heap_upper, decode_scratch) = modeled_sealed_chunk_decode_bytes(chunk)?;
        output_heap_upper = output_heap_upper.saturating_add(heap_upper);
        max_sealed_chunk_scratch = max_sealed_chunk_scratch.max(decode_scratch);
    }
    for point in snapshot.active_points.iter_points_in_partition_order() {
        output_heap_upper =
            output_heap_upper.saturating_add(modeled_value_retained_bytes(&point.value));
    }

    let bytes = fixed_output_bytes
        .saturating_add(output_heap_upper)
        // The merge cursor peeks every source before choosing a point, so one persisted decoded
        // chunk and one sealed decoded chunk can remain live simultaneously.
        .saturating_add(max_persisted_chunk_scratch)
        .saturating_add(max_sealed_chunk_scratch);
    reservation.resize(bytes)?;
    Ok(Some(reservation))
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
