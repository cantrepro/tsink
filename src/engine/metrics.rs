use super::*;
use crate::engine::compactor::CompactionRunStats;

#[derive(Default)]
pub(super) struct StorageObservabilityCounters {
    pub(super) wal: WalObservabilityCounters,
    pub(super) retention: RetentionObservabilityCounters,
    pub(super) flush: FlushObservabilityCounters,
    pub(super) compaction: CompactionObservabilityCounters,
    pub(super) query: QueryObservabilityCounters,
    pub(super) rollup: RollupObservabilityCounters,
    pub(super) remote: RemoteStorageObservabilityState,
    pub(super) health: HealthObservabilityState,
}

impl StorageObservabilityCounters {
    pub(super) fn record_admission_backpressure_delay(&self, duration: Duration) {
        self.flush
            .admission_backpressure_delays_total
            .fetch_add(1, Ordering::Relaxed);
        self.flush
            .admission_backpressure_delay_nanos_total
            .fetch_add(
                duration.as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
    }

    pub(super) fn record_admission_pressure_relief_request(&self) {
        self.flush
            .admission_pressure_relief_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_admission_pressure_relief_observed(&self) {
        self.flush
            .admission_pressure_relief_observed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_compaction_result(&self, stats: CompactionRunStats, duration_nanos: u64) {
        self.compaction.runs_total.fetch_add(1, Ordering::Relaxed);
        self.compaction
            .duration_nanos_total
            .fetch_add(duration_nanos, Ordering::Relaxed);
        self.compaction.source_segments_total.fetch_add(
            saturating_u64_from_usize(stats.source_segments),
            Ordering::Relaxed,
        );
        self.compaction.output_segments_total.fetch_add(
            saturating_u64_from_usize(stats.output_segments),
            Ordering::Relaxed,
        );
        self.compaction.source_chunks_total.fetch_add(
            saturating_u64_from_usize(stats.source_chunks),
            Ordering::Relaxed,
        );
        self.compaction.output_chunks_total.fetch_add(
            saturating_u64_from_usize(stats.output_chunks),
            Ordering::Relaxed,
        );
        self.compaction.source_points_total.fetch_add(
            saturating_u64_from_usize(stats.source_points),
            Ordering::Relaxed,
        );
        self.compaction.output_points_total.fetch_add(
            saturating_u64_from_usize(stats.output_points),
            Ordering::Relaxed,
        );
        self.compaction
            .planning_directory_entries_inspected_total
            .fetch_add(
                saturating_u64_from_usize(stats.planning_directory_entries_inspected),
                Ordering::Relaxed,
            );
        self.compaction
            .planning_manifests_inspected_total
            .fetch_add(
                saturating_u64_from_usize(stats.planning_manifests_inspected),
                Ordering::Relaxed,
            );
        self.compaction
            .planning_candidates_observed_total
            .fetch_add(
                saturating_u64_from_usize(stats.planning_candidates_observed),
                Ordering::Relaxed,
            );
        self.compaction
            .planning_source_bytes_total
            .fetch_add(stats.planning_source_bytes, Ordering::Relaxed);
        if stats.planning_backlog_observed {
            self.compaction
                .planning_backlog_observed_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if stats.planning_budget_exhausted {
            self.compaction
                .planning_budget_exhaustions_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if stats.compacted {
            self.compaction
                .success_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.compaction.noop_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn record_compaction_error(&self, duration_nanos: u64) {
        self.compaction.runs_total.fetch_add(1, Ordering::Relaxed);
        self.compaction.errors_total.fetch_add(1, Ordering::Relaxed);
        self.compaction
            .duration_nanos_total
            .fetch_add(duration_nanos, Ordering::Relaxed);
    }

    pub(super) fn record_background_worker_error(
        &self,
        worker: &'static str,
        error: &TsinkError,
        fail_fast_enabled: bool,
    ) {
        self.health
            .background_errors_total
            .fetch_add(1, Ordering::Relaxed);
        *self.health.last_background_error.write() =
            Some(format!("{worker} worker error: {error}"));
        if fail_fast_enabled {
            self.health
                .fail_fast_triggered
                .store(true, Ordering::SeqCst);
        }
    }

    pub(super) fn record_maintenance_error(&self, operation: &'static str, error: &TsinkError) {
        self.health
            .maintenance_errors_total
            .fetch_add(1, Ordering::Relaxed);
        *self.health.last_maintenance_error.write() = Some(format!("{operation}: {error}"));
    }

    pub(super) fn record_remote_catalog_refresh_success(&self, refreshed_at_unix_ms: u64) {
        self.remote
            .catalog_refreshes_total
            .fetch_add(1, Ordering::Relaxed);
        self.remote.accessible.store(true, Ordering::Relaxed);
        self.remote
            .last_successful_refresh_unix_ms
            .store(refreshed_at_unix_ms, Ordering::Relaxed);
        self.remote
            .last_refresh_attempt_unix_ms
            .store(refreshed_at_unix_ms, Ordering::Relaxed);
        self.remote
            .consecutive_refresh_failures
            .store(0, Ordering::Relaxed);
        self.remote
            .next_refresh_retry_unix_ms
            .store(0, Ordering::Relaxed);
        *self.remote.last_refresh_error.write() = None;
    }

    pub(super) fn record_remote_catalog_refresh_error(
        &self,
        operation: &'static str,
        error: &TsinkError,
        attempted_at_unix_ms: u64,
        next_retry_unix_ms: u64,
        consecutive_failures: u32,
    ) {
        self.remote
            .catalog_refreshes_total
            .fetch_add(1, Ordering::Relaxed);
        self.remote
            .catalog_refresh_errors_total
            .fetch_add(1, Ordering::Relaxed);
        self.remote.accessible.store(false, Ordering::Relaxed);
        self.remote
            .last_refresh_attempt_unix_ms
            .store(attempted_at_unix_ms, Ordering::Relaxed);
        self.remote
            .next_refresh_retry_unix_ms
            .store(next_retry_unix_ms, Ordering::Relaxed);
        self.remote
            .consecutive_refresh_failures
            .store(u64::from(consecutive_failures), Ordering::Relaxed);
        *self.remote.last_refresh_error.write() = Some(format!("{operation}: {error}"));
    }
}

#[derive(Default)]
pub(super) struct WalObservabilityCounters {
    pub(super) replay_runs_total: AtomicU64,
    pub(super) replay_frames_total: AtomicU64,
    pub(super) replay_series_definitions_total: AtomicU64,
    pub(super) replay_sample_batches_total: AtomicU64,
    pub(super) replay_points_total: AtomicU64,
    pub(super) replay_errors_total: AtomicU64,
    pub(super) replay_duration_nanos_total: AtomicU64,
    pub(super) append_series_definitions_total: AtomicU64,
    pub(super) append_sample_batches_total: AtomicU64,
    pub(super) append_points_total: AtomicU64,
    pub(super) append_bytes_total: AtomicU64,
    pub(super) append_errors_total: AtomicU64,
    pub(super) resets_total: AtomicU64,
    pub(super) reset_errors_total: AtomicU64,
}

pub(super) struct RetentionObservabilityCounters {
    pub(super) future_skew_points_total: AtomicU64,
    pub(super) future_skew_max_timestamp: AtomicI64,
}

impl Default for RetentionObservabilityCounters {
    fn default() -> Self {
        Self {
            future_skew_points_total: AtomicU64::new(0),
            future_skew_max_timestamp: AtomicI64::new(i64::MIN),
        }
    }
}

#[derive(Default)]
pub(super) struct FlushObservabilityCounters {
    pub(super) pipeline_runs_total: AtomicU64,
    pub(super) pipeline_success_total: AtomicU64,
    pub(super) pipeline_timeout_total: AtomicU64,
    pub(super) pipeline_errors_total: AtomicU64,
    pub(super) pipeline_duration_nanos_total: AtomicU64,
    pub(super) admission_backpressure_delays_total: AtomicU64,
    pub(super) admission_backpressure_delay_nanos_total: AtomicU64,
    pub(super) admission_pressure_relief_requests_total: AtomicU64,
    pub(super) admission_pressure_relief_observed_total: AtomicU64,
    pub(super) active_flush_runs_total: AtomicU64,
    pub(super) active_flush_errors_total: AtomicU64,
    pub(super) active_flush_inspected_series_total: AtomicU64,
    pub(super) active_flush_selected_input_bytes_total: AtomicU64,
    pub(super) active_flush_item_limit_hits_total: AtomicU64,
    pub(super) active_flush_byte_limit_skips_total: AtomicU64,
    pub(super) active_flushed_series_total: AtomicU64,
    pub(super) active_flushed_chunks_total: AtomicU64,
    pub(super) active_flushed_points_total: AtomicU64,
    pub(super) persist_runs_total: AtomicU64,
    pub(super) persist_success_total: AtomicU64,
    pub(super) persist_noop_total: AtomicU64,
    pub(super) persist_errors_total: AtomicU64,
    pub(super) persist_inspected_chunks_total: AtomicU64,
    pub(super) persist_selected_input_bytes_total: AtomicU64,
    pub(super) persist_item_limit_hits_total: AtomicU64,
    pub(super) persist_byte_limit_hits_total: AtomicU64,
    pub(super) persisted_series_total: AtomicU64,
    pub(super) persisted_chunks_total: AtomicU64,
    pub(super) persisted_points_total: AtomicU64,
    pub(super) persisted_segments_total: AtomicU64,
    pub(super) persist_duration_nanos_total: AtomicU64,
    pub(super) evicted_sealed_chunks_total: AtomicU64,
    pub(super) tier_moves_total: AtomicU64,
    pub(super) tier_move_errors_total: AtomicU64,
    pub(super) expired_segments_total: AtomicU64,
    pub(super) hot_segments_visible: AtomicU64,
    pub(super) warm_segments_visible: AtomicU64,
    pub(super) cold_segments_visible: AtomicU64,
}

#[derive(Default)]
pub(super) struct CompactionObservabilityCounters {
    pub(super) runs_total: AtomicU64,
    pub(super) success_total: AtomicU64,
    pub(super) noop_total: AtomicU64,
    pub(super) errors_total: AtomicU64,
    pub(super) source_segments_total: AtomicU64,
    pub(super) output_segments_total: AtomicU64,
    pub(super) source_chunks_total: AtomicU64,
    pub(super) output_chunks_total: AtomicU64,
    pub(super) source_points_total: AtomicU64,
    pub(super) output_points_total: AtomicU64,
    pub(super) planning_directory_entries_inspected_total: AtomicU64,
    pub(super) planning_manifests_inspected_total: AtomicU64,
    pub(super) planning_candidates_observed_total: AtomicU64,
    pub(super) planning_source_bytes_total: AtomicU64,
    pub(super) planning_backlog_observed_total: AtomicU64,
    pub(super) planning_budget_exhaustions_total: AtomicU64,
    pub(super) duration_nanos_total: AtomicU64,
}

#[derive(Default)]
pub(super) struct QueryObservabilityCounters {
    pub(super) select_calls_total: AtomicU64,
    pub(super) select_errors_total: AtomicU64,
    pub(super) select_duration_nanos_total: AtomicU64,
    pub(super) select_points_returned_total: AtomicU64,
    pub(super) select_with_options_calls_total: AtomicU64,
    pub(super) select_with_options_errors_total: AtomicU64,
    pub(super) select_with_options_duration_nanos_total: AtomicU64,
    pub(super) select_with_options_points_returned_total: AtomicU64,
    pub(super) select_all_calls_total: AtomicU64,
    pub(super) select_all_errors_total: AtomicU64,
    pub(super) select_all_duration_nanos_total: AtomicU64,
    pub(super) select_all_series_returned_total: AtomicU64,
    pub(super) select_all_points_returned_total: AtomicU64,
    pub(super) select_series_calls_total: AtomicU64,
    pub(super) select_series_errors_total: AtomicU64,
    pub(super) select_series_duration_nanos_total: AtomicU64,
    pub(super) select_series_returned_total: AtomicU64,
    pub(super) merge_path_queries_total: AtomicU64,
    pub(super) merge_path_shard_snapshots_total: AtomicU64,
    pub(super) merge_path_shard_snapshot_wait_nanos_total: AtomicU64,
    pub(super) merge_path_shard_snapshot_hold_nanos_total: AtomicU64,
    pub(super) append_sort_path_queries_total: AtomicU64,
    pub(super) hot_only_query_plans_total: AtomicU64,
    pub(super) warm_tier_query_plans_total: AtomicU64,
    pub(super) cold_tier_query_plans_total: AtomicU64,
    pub(super) hot_tier_persisted_chunks_read_total: AtomicU64,
    pub(super) warm_tier_persisted_chunks_read_total: AtomicU64,
    pub(super) cold_tier_persisted_chunks_read_total: AtomicU64,
    pub(super) warm_tier_fetch_duration_nanos_total: AtomicU64,
    pub(super) cold_tier_fetch_duration_nanos_total: AtomicU64,
    pub(super) rollup_query_plans_total: AtomicU64,
    pub(super) partial_rollup_query_plans_total: AtomicU64,
    pub(super) rollup_points_read_total: AtomicU64,
}

#[derive(Default)]
pub(super) struct RollupObservabilityCounters {
    pub(super) worker_runs_total: AtomicU64,
    pub(super) worker_success_total: AtomicU64,
    pub(super) worker_errors_total: AtomicU64,
    pub(super) policy_runs_total: AtomicU64,
    pub(super) buckets_materialized_total: AtomicU64,
    pub(super) points_materialized_total: AtomicU64,
    pub(super) last_run_duration_nanos: AtomicU64,
}

pub(super) struct RemoteStorageObservabilityState {
    pub(super) catalog_refreshes_total: AtomicU64,
    pub(super) catalog_refresh_errors_total: AtomicU64,
    pub(super) accessible: AtomicBool,
    pub(super) last_refresh_attempt_unix_ms: AtomicU64,
    pub(super) last_successful_refresh_unix_ms: AtomicU64,
    pub(super) consecutive_refresh_failures: AtomicU64,
    pub(super) next_refresh_retry_unix_ms: AtomicU64,
    pub(super) last_refresh_error: RwLock<Option<String>>,
}

impl RemoteStorageObservabilityState {
    pub(super) fn accessible_or_default(&self, enabled: bool) -> bool {
        if enabled {
            self.accessible.load(Ordering::Relaxed)
        } else {
            true
        }
    }
}

impl Default for RemoteStorageObservabilityState {
    fn default() -> Self {
        Self {
            catalog_refreshes_total: AtomicU64::new(0),
            catalog_refresh_errors_total: AtomicU64::new(0),
            accessible: AtomicBool::new(true),
            last_refresh_attempt_unix_ms: AtomicU64::new(0),
            last_successful_refresh_unix_ms: AtomicU64::new(0),
            consecutive_refresh_failures: AtomicU64::new(0),
            next_refresh_retry_unix_ms: AtomicU64::new(0),
            last_refresh_error: RwLock::new(None),
        }
    }
}

#[cfg(test)]
mod compaction_planning_tests {
    use super::*;

    #[test]
    fn compaction_result_records_bounded_planner_work() {
        let counters = StorageObservabilityCounters::default();
        counters.record_compaction_result(
            CompactionRunStats {
                planning_directory_entries_inspected: 7,
                planning_manifests_inspected: 5,
                planning_candidates_observed: 4,
                planning_source_bytes: 123,
                planning_backlog_observed: true,
                planning_budget_exhausted: true,
                ..CompactionRunStats::default()
            },
            11,
        );

        assert_eq!(
            counters
                .compaction
                .planning_directory_entries_inspected_total
                .load(Ordering::Relaxed),
            7
        );
        assert_eq!(
            counters
                .compaction
                .planning_manifests_inspected_total
                .load(Ordering::Relaxed),
            5
        );
        assert_eq!(
            counters
                .compaction
                .planning_candidates_observed_total
                .load(Ordering::Relaxed),
            4
        );
        assert_eq!(
            counters
                .compaction
                .planning_source_bytes_total
                .load(Ordering::Relaxed),
            123
        );
        assert_eq!(
            counters
                .compaction
                .planning_backlog_observed_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters
                .compaction
                .planning_budget_exhaustions_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn failed_remote_catalog_refresh_counts_as_an_attempt() {
        let counters = StorageObservabilityCounters::default();
        counters.record_remote_catalog_refresh_error(
            "remote catalog refresh",
            &TsinkError::Other("injected failure".to_string()),
            10,
            20,
            1,
        );

        assert_eq!(
            counters
                .remote
                .catalog_refreshes_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters
                .remote
                .catalog_refresh_errors_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters
                .remote
                .last_refresh_attempt_unix_ms
                .load(Ordering::Relaxed),
            10
        );
    }
}

#[derive(Default)]
pub(super) struct HealthObservabilityState {
    pub(super) background_errors_total: AtomicU64,
    pub(super) maintenance_errors_total: AtomicU64,
    pub(super) fail_fast_triggered: AtomicBool,
    pub(super) last_background_error: RwLock<Option<String>>,
    pub(super) last_maintenance_error: RwLock<Option<String>>,
}
