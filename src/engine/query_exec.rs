use std::sync::atomic::Ordering;

use parking_lot::{RwLock, RwLockReadGuard};
use roaring::RoaringTreemap;

use crate::engine::query::TieredQueryPlan;
use crate::engine::series::SeriesId;
use crate::query_matcher::CompiledSeriesMatcher;
use crate::storage::{
    MetadataShardScope, QueryRowsPage, QueryRowsScanOptions, ShardWindowDigest,
    ShardWindowRowsPage, ShardWindowScanOptions,
};
use crate::{DataPoint, Label, MetricSeries, Result, SeriesSelection};

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
    elapsed_nanos_u64, rollups, saturating_u64_from_usize, state, Chunk, ChunkContext,
    ChunkStorage, PersistedChunkRef, PersistedIndexState, SealedChunkKey, SeriesRegistry,
    SeriesVisibilitySummary,
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
use read_context::TimeRangeFilterContext;

impl ChunkStorage {
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

fn shard_window_series_identity_key(metric: &str, labels: &[Label]) -> String {
    crate::storage::shard_window_series_identity_key(metric, labels)
}

fn shard_window_hash_data_point(point: &DataPoint) -> u64 {
    crate::storage::shard_window_hash_data_point(point)
}

fn shard_window_fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    crate::storage::shard_window_fnv1a_update(hash, bytes)
}

fn sort_data_points_for_shard_window(points: &mut [DataPoint]) {
    crate::storage::sort_data_points_for_shard_window(points)
}
