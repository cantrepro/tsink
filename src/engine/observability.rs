use super::metrics::{
    CompactionObservabilityCounters, FlushObservabilityCounters, QueryObservabilityCounters,
    RollupObservabilityCounters, WalObservabilityCounters,
};
use super::*;
use crate::storage::StorageHealthSnapshot;
use crate::{
    CompactionObservabilitySnapshot, FlushObservabilitySnapshot, QueryObservabilitySnapshot,
    ResourceLimitOverride, RetentionObservabilitySnapshot, RollupObservabilitySnapshot,
    RollupPolicyStatus, WalMetricsObservabilitySnapshot, WalObservabilitySnapshot,
};

#[derive(Debug, Clone, Copy)]
struct WalRuntimeSnapshot {
    enabled: bool,
    sync_mode: &'static str,
    acknowledged_writes_durable: bool,
    write_buffer_capacity_bytes: u64,
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
                    write_buffer_capacity_bytes: u64::try_from(wal.write_buffer_capacity_bytes())
                        .unwrap_or(u64::MAX),
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
                write_buffer_capacity_bytes: 0,
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
            write_buffer_capacity_bytes: value.runtime.write_buffer_capacity_bytes,
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

impl From<WalSnapshotView<'_>> for WalMetricsObservabilitySnapshot {
    fn from(value: WalSnapshotView<'_>) -> Self {
        Self {
            enabled: value.runtime.enabled,
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
            active_flush_inspected_series_total: counters
                .active_flush_inspected_series_total
                .load(Ordering::Relaxed),
            active_flush_selected_input_bytes_total: counters
                .active_flush_selected_input_bytes_total
                .load(Ordering::Relaxed),
            active_flush_item_limit_hits_total: counters
                .active_flush_item_limit_hits_total
                .load(Ordering::Relaxed),
            active_flush_byte_limit_skips_total: counters
                .active_flush_byte_limit_skips_total
                .load(Ordering::Relaxed),
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
            persist_inspected_chunks_total: counters
                .persist_inspected_chunks_total
                .load(Ordering::Relaxed),
            persist_selected_input_bytes_total: counters
                .persist_selected_input_bytes_total
                .load(Ordering::Relaxed),
            persist_item_limit_hits_total: counters
                .persist_item_limit_hits_total
                .load(Ordering::Relaxed),
            persist_byte_limit_hits_total: counters
                .persist_byte_limit_hits_total
                .load(Ordering::Relaxed),
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
            planning_directory_entries_inspected_total: counters
                .planning_directory_entries_inspected_total
                .load(Ordering::Relaxed),
            planning_manifests_inspected_total: counters
                .planning_manifests_inspected_total
                .load(Ordering::Relaxed),
            planning_candidates_observed_total: counters
                .planning_candidates_observed_total
                .load(Ordering::Relaxed),
            planning_source_bytes_total: counters
                .planning_source_bytes_total
                .load(Ordering::Relaxed),
            planning_backlog_observed_total: counters
                .planning_backlog_observed_total
                .load(Ordering::Relaxed),
            planning_budget_exhaustions_total: counters
                .planning_budget_exhaustions_total
                .load(Ordering::Relaxed),
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
            source_traversal_complete: false,
            continuation_policy_id: None,
            continuation_after_series_id: None,
            policies: Vec::new(),
        }
    }
}

fn add_status_observability_bytes(total: &mut u64, bytes: u64) -> Result<()> {
    *total = total.checked_add(bytes).ok_or_else(|| {
        TsinkError::Other(
            "storage status observability retained-byte model exceeds the supported range"
                .to_string(),
        )
    })?;
    Ok(())
}

fn modeled_status_option_string(value: Option<&str>) -> Result<u64> {
    value.map_or(Ok(0), |value| {
        crate::storage::modeled_status_observability_string_bytes(value.len())
    })
}

fn clone_status_string(value: &str) -> Result<String> {
    let mut cloned = String::new();
    cloned.try_reserve_exact(value.len()).map_err(|_| {
        TsinkError::Other("storage status observability string allocation failed".to_string())
    })?;
    cloned.push_str(value);
    Ok(cloned)
}

fn clone_status_option_string(value: Option<&str>) -> Result<Option<String>> {
    value.map(clone_status_string).transpose()
}

fn modeled_status_observability_snapshot_retained_bytes(
    snapshot: &StorageObservabilitySnapshot,
) -> Result<u64> {
    let mut total = crate::storage::modeled_status_observability_vec_bytes::<ResourceLimitOverride>(
        snapshot.resource_configuration.overrides.capacity(),
    )?;
    if let Some(local_disk) = &snapshot.local_disk {
        add_status_observability_bytes(
            &mut total,
            crate::storage::modeled_status_observability_vec_bytes::<crate::DiskCategoryUsage>(
                local_disk.categories.capacity(),
            )?,
        )?;
    }
    add_status_observability_bytes(
        &mut total,
        crate::storage::modeled_status_observability_vec_bytes::<String>(
            snapshot.memory.excluded_categories.capacity(),
        )?,
    )?;
    for category in &snapshot.memory.excluded_categories {
        add_status_observability_bytes(
            &mut total,
            crate::storage::modeled_status_observability_string_bytes(category.capacity())?,
        )?;
    }
    add_status_observability_bytes(
        &mut total,
        crate::storage::modeled_status_observability_string_bytes(
            snapshot.wal.sync_mode.capacity(),
        )?,
    )?;
    add_status_observability_bytes(
        &mut total,
        modeled_status_option_string(snapshot.rollups.continuation_policy_id.as_deref())?,
    )?;
    add_status_observability_bytes(
        &mut total,
        crate::storage::modeled_status_observability_vec_bytes::<RollupPolicyStatus>(
            snapshot.rollups.policies.capacity(),
        )?,
    )?;
    for status in &snapshot.rollups.policies {
        add_status_observability_bytes(
            &mut total,
            crate::storage::modeled_status_observability_string_bytes(status.policy.id.capacity())?,
        )?;
        add_status_observability_bytes(
            &mut total,
            crate::storage::modeled_status_observability_string_bytes(
                status.policy.metric.capacity(),
            )?,
        )?;
        add_status_observability_bytes(
            &mut total,
            crate::storage::modeled_status_observability_vec_bytes::<Label>(
                status.policy.match_labels.capacity(),
            )?,
        )?;
        for label in &status.policy.match_labels {
            add_status_observability_bytes(
                &mut total,
                crate::storage::modeled_status_observability_string_bytes(label.name.capacity())?,
            )?;
            add_status_observability_bytes(
                &mut total,
                crate::storage::modeled_status_observability_string_bytes(label.value.capacity())?,
            )?;
        }
        add_status_observability_bytes(
            &mut total,
            modeled_status_option_string(status.last_error.as_deref())?,
        )?;
    }
    add_status_observability_bytes(
        &mut total,
        modeled_status_option_string(snapshot.remote.last_refresh_error.as_deref())?,
    )?;
    add_status_observability_bytes(
        &mut total,
        modeled_status_option_string(snapshot.health.last_background_error.as_deref())?,
    )?;
    add_status_observability_bytes(
        &mut total,
        modeled_status_option_string(snapshot.health.last_maintenance_error.as_deref())?,
    )?;
    Ok(total)
}

impl ChunkStorage {
    pub(super) fn storage_health_degraded(&self) -> bool {
        self.observability
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
                && !self.observability.remote.accessible_or_default(true))
    }

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
        let creation_rate = self
            .catalog
            .series_creation_rate_limiter
            .snapshot(self.current_timestamp_units());
        StorageObservabilitySnapshot {
            limits: self.effective_storage_limits(),
            resource_configuration: self.resource_configuration_snapshot(),
            local_disk: self
                .persisted
                .local_disk_budget
                .as_ref()
                .map(|budget| budget.snapshot()),
            memory: self.memory_observability_snapshot(),
            cardinality: CardinalityObservabilitySnapshot {
                series_count: u64::try_from(self.catalog.registry.read().series_count())
                    .unwrap_or(u64::MAX),
                pending_new_series: u64::try_from(creation_rate.pending).unwrap_or(u64::MAX),
                committed_in_window: u64::try_from(creation_rate.committed_in_window)
                    .unwrap_or(u64::MAX),
                current_window_start: creation_rate.window_start,
                admitted_new_series_total: creation_rate.admitted_total,
                committed_new_series_total: creation_rate.committed_total,
                creation_rate_rejections_total: creation_rate.rejections_total,
            },
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
            query_budget: self.query_budget.snapshot(),
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
            background: self.background.observability_snapshot(),
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
                degraded: self.storage_health_degraded(),
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

    pub(super) fn status_observability_snapshot_impl(
        &self,
        execution: &QueryExecution,
    ) -> Result<crate::StorageStatusObservabilitySnapshot> {
        execution.checkpoint()?;

        // Keep every mutable dynamic producer stable from the allocation-free model through the
        // clone. Rollup's run guard also keeps its traversal cursor and policy status coherent.
        let rollup_source = self.rollup_status_snapshot_source();
        let resource_configuration_source = self.resource_configuration.read();
        let remote_error_source = self.observability.remote.last_refresh_error.read();
        let background_error_source = self.observability.health.last_background_error.read();
        let maintenance_error_source = self.observability.health.last_maintenance_error.read();
        execution.checkpoint()?;

        let wal_runtime = WalRuntimeSnapshot::from_wal(self.persisted.wal.as_ref());
        let mut retained_bytes = crate::storage::modeled_status_observability_vec_bytes::<
            ResourceLimitOverride,
        >(resource_configuration_source.overrides.len())?;
        if let Some(local_disk_budget) = &self.persisted.local_disk_budget {
            add_status_observability_bytes(
                &mut retained_bytes,
                local_disk_budget.status_snapshot_modeled_retained_bytes()?,
            )?;
        }
        add_status_observability_bytes(
            &mut retained_bytes,
            self.memory_observability_snapshot_modeled_retained_bytes()?,
        )?;
        add_status_observability_bytes(
            &mut retained_bytes,
            crate::storage::modeled_status_observability_string_bytes(wal_runtime.sync_mode.len())?,
        )?;
        add_status_observability_bytes(
            &mut retained_bytes,
            rollup_source.modeled_retained_bytes()?,
        )?;
        add_status_observability_bytes(
            &mut retained_bytes,
            modeled_status_option_string(remote_error_source.as_deref())?,
        )?;
        add_status_observability_bytes(
            &mut retained_bytes,
            modeled_status_option_string(background_error_source.as_deref())?,
        )?;
        add_status_observability_bytes(
            &mut retained_bytes,
            modeled_status_option_string(maintenance_error_source.as_deref())?,
        )?;

        let memory_reservation = execution.reserve_memory(retained_bytes)?;
        execution.checkpoint()?;

        let effective_limits = self.effective_storage_limits();
        let mut resolved_limits = resource_configuration_source.resolved_limits.clone();
        resolved_limits.storage = effective_limits;
        resolved_limits.query = self.query_budget.limits();
        let mut overrides = Vec::new();
        overrides
            .try_reserve_exact(resource_configuration_source.overrides.len())
            .map_err(|_| {
                TsinkError::Other("storage status resource override allocation failed".to_string())
            })?;
        overrides.extend_from_slice(&resource_configuration_source.overrides);
        let resource_configuration = ResourceConfigurationSnapshot {
            schema_version: resource_configuration_source.schema_version,
            reported_by_backend: resource_configuration_source.reported_by_backend,
            selected_profile: resource_configuration_source.selected_profile,
            resolved_limits,
            overrides,
        };
        drop(resource_configuration_source);
        let last_refresh_error = clone_status_option_string(remote_error_source.as_deref())?;
        let last_background_error = clone_status_option_string(background_error_source.as_deref())?;
        let last_maintenance_error =
            clone_status_option_string(maintenance_error_source.as_deref())?;
        drop(remote_error_source);
        drop(background_error_source);
        drop(maintenance_error_source);
        execution.checkpoint()?;

        let rollups = rollup_source.materialize(execution)?;
        execution.checkpoint()?;
        let local_disk = self
            .persisted
            .local_disk_budget
            .as_ref()
            .map(|budget| budget.status_snapshot_after_reservation(execution))
            .transpose()?;
        execution.checkpoint()?;
        let memory = self.memory_observability_snapshot();
        execution.checkpoint()?;
        let creation_rate = self
            .catalog
            .series_creation_rate_limiter
            .snapshot(self.current_timestamp_units());

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
        let snapshot = StorageObservabilitySnapshot {
            limits: effective_limits,
            resource_configuration,
            local_disk,
            memory,
            cardinality: CardinalityObservabilitySnapshot {
                series_count: u64::try_from(self.catalog.registry.read().series_count())
                    .unwrap_or(u64::MAX),
                pending_new_series: u64::try_from(creation_rate.pending).unwrap_or(u64::MAX),
                committed_in_window: u64::try_from(creation_rate.committed_in_window)
                    .unwrap_or(u64::MAX),
                current_window_start: creation_rate.window_start,
                admitted_new_series_total: creation_rate.admitted_total,
                committed_new_series_total: creation_rate.committed_total,
                creation_rate_rejections_total: creation_rate.rejections_total,
            },
            wal: WalObservabilitySnapshot::from(WalSnapshotView {
                counters: &self.observability.wal,
                runtime: wal_runtime,
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
            query_budget: self.query_budget.snapshot(),
            rollups,
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
                last_refresh_error,
            },
            background: self.background.observability_snapshot(),
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
                degraded: self.storage_health_degraded(),
                fail_fast_enabled: self.background.fail_fast_enabled,
                fail_fast_triggered: self
                    .observability
                    .health
                    .fail_fast_triggered
                    .load(Ordering::SeqCst),
                last_background_error,
                last_maintenance_error,
            },
        };
        let actual_retained_bytes =
            modeled_status_observability_snapshot_retained_bytes(&snapshot)?;
        if actual_retained_bytes > retained_bytes {
            return Err(TsinkError::Other(format!(
                "storage status observability allocated {actual_retained_bytes} modeled bytes after reserving {retained_bytes}"
            )));
        }
        debug_assert_eq!(actual_retained_bytes, retained_bytes);
        Ok(crate::StorageStatusObservabilitySnapshot::new(
            snapshot,
            memory_reservation,
        ))
    }

    pub(super) fn metrics_observability_snapshot_impl(
        &self,
        execution: &QueryExecution,
    ) -> Result<crate::StorageMetricsObservabilitySnapshot> {
        execution.checkpoint()?;
        let mut memory_reservation = execution.reserve_memory(0)?;
        let now_unix_ms = current_unix_millis_u64();
        let next_refresh_retry_unix_ms = {
            let ts = self
                .observability
                .remote
                .next_refresh_retry_unix_ms
                .load(Ordering::Relaxed);
            (ts > 0).then_some(ts)
        };

        let local_disk = self
            .persisted
            .local_disk_budget
            .as_ref()
            .map(|budget| budget.metrics_snapshot_with_execution(execution))
            .transpose()?;
        execution.checkpoint()?;
        let creation_rate = self
            .catalog
            .series_creation_rate_limiter
            .snapshot(self.current_timestamp_units());
        let cardinality = CardinalityObservabilitySnapshot {
            series_count: u64::try_from(self.catalog.registry.read().series_count())
                .unwrap_or(u64::MAX),
            pending_new_series: u64::try_from(creation_rate.pending).unwrap_or(u64::MAX),
            committed_in_window: u64::try_from(creation_rate.committed_in_window)
                .unwrap_or(u64::MAX),
            current_window_start: creation_rate.window_start,
            admitted_new_series_total: creation_rate.admitted_total,
            committed_new_series_total: creation_rate.committed_total,
            creation_rate_rejections_total: creation_rate.rejections_total,
        };
        execution.checkpoint()?;
        let rollups =
            self.rollup_metrics_observability_snapshot(execution, &mut memory_reservation)?;
        execution.checkpoint()?;

        Ok(crate::StorageMetricsObservabilitySnapshot::new(
            local_disk,
            self.memory_metrics_observability_snapshot(),
            cardinality,
            WalMetricsObservabilitySnapshot::from(WalSnapshotView {
                counters: &self.observability.wal,
                runtime: WalRuntimeSnapshot::from_wal(self.persisted.wal.as_ref()),
            }),
            FlushObservabilitySnapshot::from(&self.observability.flush),
            CompactionObservabilitySnapshot::from(&self.observability.compaction),
            QueryObservabilitySnapshot::from(&self.observability.query),
            self.query_budget.snapshot(),
            rollups,
            crate::RemoteStorageMetricsObservabilitySnapshot {
                runtime_mode: self.runtime.runtime_mode,
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
                consecutive_refresh_failures: self
                    .observability
                    .remote
                    .consecutive_refresh_failures
                    .load(Ordering::Relaxed),
                backoff_active: next_refresh_retry_unix_ms
                    .is_some_and(|retry_at| retry_at > now_unix_ms),
            },
            self.background.observability_snapshot(),
            memory_reservation,
        ))
    }
}
