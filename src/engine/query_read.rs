use crate::engine::query::{
    decode_chunk_points_in_range_into, decode_encoded_chunk_payload_in_range_into,
    EncodedChunkDescriptor, TieredQueryPlan,
};
use crate::engine::tombstone;
use parking_lot::RwLockReadGuard;

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

    pub(super) fn collect_points_for_series_into_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
    ) -> Result<PersistedTierFetchStats> {
        let (snapshot, shard_snapshot_stats) = self
            .query_snapshot_context()
            .snapshot_series_read_sources(series_id, start, end, plan)?;
        self.record_merge_path_shard_snapshot_stats(shard_snapshot_stats);
        #[cfg(test)]
        self.invoke_query_merge_in_memory_source_snapshot_hook();

        if snapshot.analysis.can_use_merge_path() {
            self.observability
                .query
                .merge_path_queries_total
                .fetch_add(1, Ordering::Relaxed);
            return self.execute_series_read_merge_path(series_id, start, end, snapshot, out);
        }

        self.observability
            .query
            .append_sort_path_queries_total
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.invoke_query_append_sort_in_memory_source_snapshot_hook();
        self.execute_series_read_append_sort_path(series_id, start, end, snapshot, out)
    }

    pub(super) fn collect_points_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<(Vec<DataPoint>, PersistedTierFetchStats)> {
        let mut out = Vec::new();
        let stats =
            self.collect_points_for_series_into_with_plan(series_id, start, end, plan, &mut out)?;
        Ok((out, stats))
    }

    pub(super) fn select_into_impl(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        out: &mut Vec<DataPoint>,
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
        self.collect_points_for_series_into_with_plan(series_id, start, end, plan, out)
    }

    pub(super) fn select_raw_series_page_with_plan(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        pagination: RawSeriesPagination,
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
        )
    }

    pub(super) fn collect_raw_series_page_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        offset: u64,
        limit: Option<usize>,
    ) -> Result<RawSeriesScanPage> {
        let pagination = RawSeriesPagination::new(offset, limit);
        let (snapshot, shard_snapshot_stats) = self
            .query_snapshot_context()
            .snapshot_series_read_sources(series_id, start, end, plan)?;
        self.record_merge_path_shard_snapshot_stats(shard_snapshot_stats);
        #[cfg(test)]
        self.invoke_query_merge_in_memory_source_snapshot_hook();

        if snapshot.analysis.can_use_merge_path() {
            self.observability
                .query
                .merge_path_queries_total
                .fetch_add(1, Ordering::Relaxed);
            return self
                .collect_raw_series_page_with_merge(series_id, start, end, snapshot, pagination);
        }

        self.observability
            .query
            .append_sort_path_queries_total
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.invoke_query_append_sort_in_memory_source_snapshot_hook();
        self.collect_raw_series_page_with_append_sort(series_id, start, end, snapshot, pagination)
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
