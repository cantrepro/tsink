use super::metrics::{
    CompactionObservabilityCounters, FlushObservabilityCounters, QueryObservabilityCounters,
    RollupObservabilityCounters, WalObservabilityCounters,
};
use super::*;
use crate::storage::StorageHealthSnapshot;
use crate::{
    CompactionObservabilitySnapshot, FlushObservabilitySnapshot, QueryObservabilitySnapshot,
    RetentionObservabilitySnapshot, RollupObservabilitySnapshot, WalObservabilitySnapshot,
};

#[derive(Debug, Clone, Copy)]
struct WalRuntimeSnapshot {
    enabled: bool,
    sync_mode: &'static str,
    acknowledged_writes_durable: bool,
    size_bytes: u64,
    segment_count: u64,
    active_segment: u64,
    highwater_segment: u64,
    highwater_frame: u64,
    durable_highwater_segment: u64,
    durable_highwater_frame: u64,
}

impl WalRuntimeSnapshot {
    fn from_wal(wal: Option<&FramedWal>) -> Self {
        match wal {
            Some(wal) => {
                let highwater = wal.current_appended_highwater();
                let durable_highwater = wal.current_durable_highwater();
                Self {
                    enabled: true,
                    sync_mode: wal.sync_mode().as_str(),
                    acknowledged_writes_durable: wal.sync_mode().acknowledged_writes_are_durable(),
                    size_bytes: wal.total_size_bytes().unwrap_or(0),
                    segment_count: wal.segment_count().unwrap_or(0),
                    active_segment: wal.active_segment(),
                    highwater_segment: highwater.segment,
                    highwater_frame: highwater.frame,
                    durable_highwater_segment: durable_highwater.segment,
                    durable_highwater_frame: durable_highwater.frame,
                }
            }
            None => Self {
                enabled: false,
                sync_mode: "disabled",
                acknowledged_writes_durable: false,
                size_bytes: 0,
                segment_count: 0,
                active_segment: 0,
                highwater_segment: 0,
                highwater_frame: 0,
                durable_highwater_segment: 0,
                durable_highwater_frame: 0,
            },
        }
    }
}

#[derive(Clone, Copy)]
struct WalSnapshotView<'a> {
    counters: &'a WalObservabilityCounters,
    runtime: WalRuntimeSnapshot,
}

#[derive(Clone, Copy)]
struct RetentionSnapshotView<'a> {
    counters: &'a super::metrics::RetentionObservabilityCounters,
    max_observed_timestamp: Option<i64>,
    recency_reference_timestamp: Option<i64>,
    future_skew_window: i64,
}

impl From<WalSnapshotView<'_>> for WalObservabilitySnapshot {
    fn from(value: WalSnapshotView<'_>) -> Self {
        Self {
            enabled: value.runtime.enabled,
            sync_mode: value.runtime.sync_mode.to_string(),
            acknowledged_writes_durable: value.runtime.acknowledged_writes_durable,
            size_bytes: value.runtime.size_bytes,
            segment_count: value.runtime.segment_count,
            active_segment: value.runtime.active_segment,
            highwater_segment: value.runtime.highwater_segment,
            highwater_frame: value.runtime.highwater_frame,
            durable_highwater_segment: value.runtime.durable_highwater_segment,
            durable_highwater_frame: value.runtime.durable_highwater_frame,
            replay_runs_total: value.counters.replay_runs_total.load(Ordering::Relaxed),
            replay_frames_total: value.counters.replay_frames_total.load(Ordering::Relaxed),
            replay_series_definitions_total: value
                .counters
                .replay_series_definitions_total
                .load(Ordering::Relaxed),
            replay_sample_batches_total: value
                .counters
                .replay_sample_batches_total
                .load(Ordering::Relaxed),
            replay_points_total: value.counters.replay_points_total.load(Ordering::Relaxed),
            replay_errors_total: value.counters.replay_errors_total.load(Ordering::Relaxed),
            replay_duration_nanos_total: value
                .counters
                .replay_duration_nanos_total
                .load(Ordering::Relaxed),
            append_series_definitions_total: value
                .counters
                .append_series_definitions_total
                .load(Ordering::Relaxed),
            append_sample_batches_total: value
                .counters
                .append_sample_batches_total
                .load(Ordering::Relaxed),
            append_points_total: value.counters.append_points_total.load(Ordering::Relaxed),
            append_bytes_total: value.counters.append_bytes_total.load(Ordering::Relaxed),
            append_errors_total: value.counters.append_errors_total.load(Ordering::Relaxed),
            resets_total: value.counters.resets_total.load(Ordering::Relaxed),
            reset_errors_total: value.counters.reset_errors_total.load(Ordering::Relaxed),
        }
    }
}

impl From<RetentionSnapshotView<'_>> for RetentionObservabilitySnapshot {
    fn from(value: RetentionSnapshotView<'_>) -> Self {
        let future_skew_max_timestamp = value
            .counters
            .future_skew_max_timestamp
            .load(Ordering::Relaxed);
        Self {
            max_observed_timestamp: value.max_observed_timestamp,
            recency_reference_timestamp: value.recency_reference_timestamp,
            future_skew_window: value.future_skew_window,
            future_skew_points_total: value
                .counters
                .future_skew_points_total
                .load(Ordering::Relaxed),
            future_skew_max_timestamp: (future_skew_max_timestamp != i64::MIN)
                .then_some(future_skew_max_timestamp),
        }
    }
}

impl From<&FlushObservabilityCounters> for FlushObservabilitySnapshot {
    fn from(counters: &FlushObservabilityCounters) -> Self {
        Self {
            pipeline_runs_total: counters.pipeline_runs_total.load(Ordering::Relaxed),
            pipeline_success_total: counters.pipeline_success_total.load(Ordering::Relaxed),
            pipeline_timeout_total: counters.pipeline_timeout_total.load(Ordering::Relaxed),
            pipeline_errors_total: counters.pipeline_errors_total.load(Ordering::Relaxed),
            pipeline_duration_nanos_total: counters
                .pipeline_duration_nanos_total
                .load(Ordering::Relaxed),
            admission_backpressure_delays_total: counters
                .admission_backpressure_delays_total
                .load(Ordering::Relaxed),
            admission_backpressure_delay_nanos_total: counters
                .admission_backpressure_delay_nanos_total
                .load(Ordering::Relaxed),
            admission_pressure_relief_requests_total: counters
                .admission_pressure_relief_requests_total
                .load(Ordering::Relaxed),
            admission_pressure_relief_observed_total: counters
                .admission_pressure_relief_observed_total
                .load(Ordering::Relaxed),
            active_flush_runs_total: counters.active_flush_runs_total.load(Ordering::Relaxed),
            active_flush_errors_total: counters.active_flush_errors_total.load(Ordering::Relaxed),
            active_flushed_series_total: counters
                .active_flushed_series_total
                .load(Ordering::Relaxed),
            active_flushed_chunks_total: counters
                .active_flushed_chunks_total
                .load(Ordering::Relaxed),
            active_flushed_points_total: counters
                .active_flushed_points_total
                .load(Ordering::Relaxed),
            persist_runs_total: counters.persist_runs_total.load(Ordering::Relaxed),
            persist_success_total: counters.persist_success_total.load(Ordering::Relaxed),
            persist_noop_total: counters.persist_noop_total.load(Ordering::Relaxed),
            persist_errors_total: counters.persist_errors_total.load(Ordering::Relaxed),
            persisted_series_total: counters.persisted_series_total.load(Ordering::Relaxed),
            persisted_chunks_total: counters.persisted_chunks_total.load(Ordering::Relaxed),
            persisted_points_total: counters.persisted_points_total.load(Ordering::Relaxed),
            persisted_segments_total: counters.persisted_segments_total.load(Ordering::Relaxed),
            persist_duration_nanos_total: counters
                .persist_duration_nanos_total
                .load(Ordering::Relaxed),
            evicted_sealed_chunks_total: counters
                .evicted_sealed_chunks_total
                .load(Ordering::Relaxed),
            tier_moves_total: counters.tier_moves_total.load(Ordering::Relaxed),
            tier_move_errors_total: counters.tier_move_errors_total.load(Ordering::Relaxed),
            expired_segments_total: counters.expired_segments_total.load(Ordering::Relaxed),
            hot_segments_visible: counters.hot_segments_visible.load(Ordering::Relaxed),
            warm_segments_visible: counters.warm_segments_visible.load(Ordering::Relaxed),
            cold_segments_visible: counters.cold_segments_visible.load(Ordering::Relaxed),
        }
    }
}

impl From<&CompactionObservabilityCounters> for CompactionObservabilitySnapshot {
    fn from(counters: &CompactionObservabilityCounters) -> Self {
        Self {
            runs_total: counters.runs_total.load(Ordering::Relaxed),
            success_total: counters.success_total.load(Ordering::Relaxed),
            noop_total: counters.noop_total.load(Ordering::Relaxed),
            errors_total: counters.errors_total.load(Ordering::Relaxed),
            source_segments_total: counters.source_segments_total.load(Ordering::Relaxed),
            output_segments_total: counters.output_segments_total.load(Ordering::Relaxed),
            source_chunks_total: counters.source_chunks_total.load(Ordering::Relaxed),
            output_chunks_total: counters.output_chunks_total.load(Ordering::Relaxed),
            source_points_total: counters.source_points_total.load(Ordering::Relaxed),
            output_points_total: counters.output_points_total.load(Ordering::Relaxed),
            duration_nanos_total: counters.duration_nanos_total.load(Ordering::Relaxed),
        }
    }
}

impl From<&QueryObservabilityCounters> for QueryObservabilitySnapshot {
    fn from(counters: &QueryObservabilityCounters) -> Self {
        Self {
            select_calls_total: counters.select_calls_total.load(Ordering::Relaxed),
            select_errors_total: counters.select_errors_total.load(Ordering::Relaxed),
            select_duration_nanos_total: counters
                .select_duration_nanos_total
                .load(Ordering::Relaxed),
            select_points_returned_total: counters
                .select_points_returned_total
                .load(Ordering::Relaxed),
            select_with_options_calls_total: counters
                .select_with_options_calls_total
                .load(Ordering::Relaxed),
            select_with_options_errors_total: counters
                .select_with_options_errors_total
                .load(Ordering::Relaxed),
            select_with_options_duration_nanos_total: counters
                .select_with_options_duration_nanos_total
                .load(Ordering::Relaxed),
            select_with_options_points_returned_total: counters
                .select_with_options_points_returned_total
                .load(Ordering::Relaxed),
            select_all_calls_total: counters.select_all_calls_total.load(Ordering::Relaxed),
            select_all_errors_total: counters.select_all_errors_total.load(Ordering::Relaxed),
            select_all_duration_nanos_total: counters
                .select_all_duration_nanos_total
                .load(Ordering::Relaxed),
            select_all_series_returned_total: counters
                .select_all_series_returned_total
                .load(Ordering::Relaxed),
            select_all_points_returned_total: counters
                .select_all_points_returned_total
                .load(Ordering::Relaxed),
            select_series_calls_total: counters.select_series_calls_total.load(Ordering::Relaxed),
            select_series_errors_total: counters.select_series_errors_total.load(Ordering::Relaxed),
            select_series_duration_nanos_total: counters
                .select_series_duration_nanos_total
                .load(Ordering::Relaxed),
            select_series_returned_total: counters
                .select_series_returned_total
                .load(Ordering::Relaxed),
            merge_path_queries_total: counters.merge_path_queries_total.load(Ordering::Relaxed),
            merge_path_shard_snapshots_total: counters
                .merge_path_shard_snapshots_total
                .load(Ordering::Relaxed),
            merge_path_shard_snapshot_wait_nanos_total: counters
                .merge_path_shard_snapshot_wait_nanos_total
                .load(Ordering::Relaxed),
            merge_path_shard_snapshot_hold_nanos_total: counters
                .merge_path_shard_snapshot_hold_nanos_total
                .load(Ordering::Relaxed),
            append_sort_path_queries_total: counters
                .append_sort_path_queries_total
                .load(Ordering::Relaxed),
            hot_only_query_plans_total: counters.hot_only_query_plans_total.load(Ordering::Relaxed),
            warm_tier_query_plans_total: counters
                .warm_tier_query_plans_total
                .load(Ordering::Relaxed),
            cold_tier_query_plans_total: counters
                .cold_tier_query_plans_total
                .load(Ordering::Relaxed),
            hot_tier_persisted_chunks_read_total: counters
                .hot_tier_persisted_chunks_read_total
                .load(Ordering::Relaxed),
            warm_tier_persisted_chunks_read_total: counters
                .warm_tier_persisted_chunks_read_total
                .load(Ordering::Relaxed),
            cold_tier_persisted_chunks_read_total: counters
                .cold_tier_persisted_chunks_read_total
                .load(Ordering::Relaxed),
            warm_tier_fetch_duration_nanos_total: counters
                .warm_tier_fetch_duration_nanos_total
                .load(Ordering::Relaxed),
            cold_tier_fetch_duration_nanos_total: counters
                .cold_tier_fetch_duration_nanos_total
                .load(Ordering::Relaxed),
            rollup_query_plans_total: counters.rollup_query_plans_total.load(Ordering::Relaxed),
            partial_rollup_query_plans_total: counters
                .partial_rollup_query_plans_total
                .load(Ordering::Relaxed),
            rollup_points_read_total: counters.rollup_points_read_total.load(Ordering::Relaxed),
        }
    }
}

impl From<&RollupObservabilityCounters> for RollupObservabilitySnapshot {
    fn from(counters: &RollupObservabilityCounters) -> Self {
        Self {
            worker_runs_total: counters.worker_runs_total.load(Ordering::Relaxed),
            worker_success_total: counters.worker_success_total.load(Ordering::Relaxed),
            worker_errors_total: counters.worker_errors_total.load(Ordering::Relaxed),
            policy_runs_total: counters.policy_runs_total.load(Ordering::Relaxed),
            buckets_materialized_total: counters.buckets_materialized_total.load(Ordering::Relaxed),
            points_materialized_total: counters.points_materialized_total.load(Ordering::Relaxed),
            last_run_duration_nanos: counters.last_run_duration_nanos.load(Ordering::Relaxed),
            policies: Vec::new(),
        }
    }
}

impl ChunkStorage {
    pub(super) fn observability_snapshot_impl(&self) -> StorageObservabilitySnapshot {
        let now_unix_ms = current_unix_millis_u64();
        let last_refresh_attempt_unix_ms = {
            let ts = self
                .observability
                .remote
                .last_refresh_attempt_unix_ms
                .load(Ordering::Relaxed);
            (ts > 0).then_some(ts)
        };
        let last_successful_refresh_unix_ms = {
            let ts = self
                .observability
                .remote
                .last_successful_refresh_unix_ms
                .load(Ordering::Relaxed);
            (ts > 0).then_some(ts)
        };
        let next_refresh_retry_unix_ms = {
            let ts = self
                .observability
                .remote
                .next_refresh_retry_unix_ms
                .load(Ordering::Relaxed);
            (ts > 0).then_some(ts)
        };
        StorageObservabilitySnapshot {
            memory: self.memory_observability_snapshot(),
            wal: WalObservabilitySnapshot::from(WalSnapshotView {
                counters: &self.observability.wal,
                runtime: WalRuntimeSnapshot::from_wal(self.persisted.wal.as_ref()),
            }),
            retention: RetentionObservabilitySnapshot::from(RetentionSnapshotView {
                counters: &self.observability.retention,
                max_observed_timestamp: self.max_observed_timestamp(),
                recency_reference_timestamp: self.retention_recency_reference_timestamp(),
                future_skew_window: self.runtime.future_skew_window,
            }),
            flush: FlushObservabilitySnapshot::from(&self.observability.flush),
            compaction: CompactionObservabilitySnapshot::from(&self.observability.compaction),
            query: QueryObservabilitySnapshot::from(&self.observability.query),
            rollups: self.rollup_observability_snapshot(),
            remote: RemoteStorageObservabilitySnapshot {
                enabled: self.persisted.tiered_storage.is_some(),
                runtime_mode: self.runtime.runtime_mode,
                cache_policy: self.persisted.remote_segment_cache_policy,
                metadata_refresh_interval_ms: u64::try_from(
                    self.persisted.remote_segment_refresh_interval.as_millis(),
                )
                .unwrap_or(u64::MAX),
                mirror_hot_segments: self
                    .persisted
                    .tiered_storage
                    .as_ref()
                    .is_some_and(|config| config.mirror_hot_segments),
                catalog_refreshes_total: self
                    .observability
                    .remote
                    .catalog_refreshes_total
                    .load(Ordering::Relaxed),
                catalog_refresh_errors_total: self
                    .observability
                    .remote
                    .catalog_refresh_errors_total
                    .load(Ordering::Relaxed),
                accessible: self
                    .observability
                    .remote
                    .accessible_or_default(self.persisted.tiered_storage.is_some()),
                last_refresh_attempt_unix_ms,
                last_successful_refresh_unix_ms,
                consecutive_refresh_failures: self
                    .observability
                    .remote
                    .consecutive_refresh_failures
                    .load(Ordering::Relaxed),
                next_refresh_retry_unix_ms,
                backoff_active: next_refresh_retry_unix_ms
                    .is_some_and(|retry_at| retry_at > now_unix_ms),
                last_refresh_error: self.observability.remote.last_refresh_error.read().clone(),
            },
            health: StorageHealthSnapshot {
                background_errors_total: self
                    .observability
                    .health
                    .background_errors_total
                    .load(Ordering::Relaxed),
                maintenance_errors_total: self
                    .observability
                    .health
                    .maintenance_errors_total
                    .load(Ordering::Relaxed),
                degraded: self
                    .observability
                    .health
                    .background_errors_total
                    .load(Ordering::Relaxed)
                    > 0
                    || self
                        .observability
                        .health
                        .maintenance_errors_total
                        .load(Ordering::Relaxed)
                        > 0
                    || (self.persisted.tiered_storage.is_some()
                        && !self.observability.remote.accessible_or_default(true)),
                fail_fast_enabled: self.background.fail_fast_enabled,
                fail_fast_triggered: self
                    .observability
                    .health
                    .fail_fast_triggered
                    .load(Ordering::SeqCst),
                last_background_error: self
                    .observability
                    .health
                    .last_background_error
                    .read()
                    .clone(),
                last_maintenance_error: self
                    .observability
                    .health
                    .last_maintenance_error
                    .read()
                    .clone(),
            },
        }
    }
}
