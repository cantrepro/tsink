use crate::enums::{
    UAggregation, UDiskCategory, UHistogramCount, UHistogramResetHint, UMemoryPressureLevel,
    URemoteSegmentCachePolicy, URowWriteStatus, USeriesMatcherOp, UStorageRuntimeMode, UValue,
    UWriteAcknowledgement, UWriteRejectionCategory,
};

#[derive(Debug, Clone, uniffi::Record)]
pub struct ULabel {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UDataPoint {
    pub value: UValue,
    pub timestamp: i64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UHistogramBucketSpan {
    pub offset: i32,
    pub length: u32,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UNativeHistogram {
    pub count: Option<UHistogramCount>,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: Option<UHistogramCount>,
    pub negative_spans: Vec<UHistogramBucketSpan>,
    pub negative_deltas: Vec<i64>,
    pub negative_counts: Vec<f64>,
    pub positive_spans: Vec<UHistogramBucketSpan>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
    pub reset_hint: UHistogramResetHint,
    pub custom_values: Vec<f64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URow {
    pub metric: String,
    pub labels: Vec<ULabel>,
    pub data_point: UDataPoint,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UMetricSeries {
    pub name: String,
    pub labels: Vec<ULabel>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct USeriesPoints {
    pub series: UMetricSeries,
    pub points: Vec<UDataPoint>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UDownsampleOptions {
    pub interval: i64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct USeriesMatcher {
    pub name: String,
    pub op: USeriesMatcherOp,
    pub value: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct ULabeledDataPoints {
    pub labels: Vec<ULabel>,
    pub data_points: Vec<UDataPoint>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UMetadataShardScope {
    pub shard_count: u32,
    pub shards: Vec<u32>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UShardWindowDigest {
    pub shard: u32,
    pub shard_count: u32,
    pub window_start: i64,
    pub window_end: i64,
    pub series_count: u64,
    pub point_count: u64,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UShardWindowScanOptions {
    pub max_series: Option<u64>,
    pub max_rows: Option<u64>,
    pub row_offset: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UShardWindowRowsPage {
    pub shard: u32,
    pub shard_count: u32,
    pub window_start: i64,
    pub window_end: i64,
    pub series_scanned: u64,
    pub rows_scanned: u64,
    pub truncated: bool,
    pub next_row_offset: Option<u64>,
    pub rows: Vec<URow>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryRowsScanOptions {
    pub max_rows: Option<u64>,
    pub row_offset: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryRowsPage {
    pub rows_scanned: u64,
    pub truncated: bool,
    pub next_row_offset: Option<u64>,
    pub rows: Vec<URow>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UWriteResult {
    pub acknowledgement: UWriteAcknowledgement,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UWriteRejection {
    pub category: UWriteRejectionCategory,
    pub cause_index: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URowWriteOutcome {
    pub index: u64,
    pub status: URowWriteStatus,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UBatchWriteResult {
    pub submitted: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub acknowledgement: Option<UWriteAcknowledgement>,
    pub outcomes: Vec<URowWriteOutcome>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UDeleteSeriesResult {
    pub matched_series: u64,
    pub tombstones_applied: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UWriteBatchLimits {
    #[uniffi(default)]
    pub max_rows: Option<u64>,
    #[uniffi(default)]
    pub max_modeled_input_bytes: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UEffectiveStorageLimits {
    pub reported_by_backend: bool,
    pub persistent: bool,
    pub wal_enabled: bool,
    pub accounted_memory_bytes: Option<u64>,
    pub cardinality: Option<u64>,
    pub max_labels_per_series: Option<u64>,
    pub max_series_identity_bytes: Option<u64>,
    pub max_new_series_per_window: Option<u64>,
    pub new_series_window_nanos: Option<u64>,
    #[uniffi(default)]
    pub max_write_batch_rows: Option<u64>,
    #[uniffi(default)]
    pub max_write_batch_input_bytes: Option<u64>,
    pub wal_bytes: Option<u64>,
    #[uniffi(default)]
    pub wal_write_buffer_bytes: Option<u64>,
    pub local_disk_bytes: Option<u64>,
    pub filesystem_free_headroom_bytes: Option<u64>,
    pub maintenance_temp_reserve_bytes: Option<u64>,
    pub max_concurrent_writers: Option<u64>,
    pub write_timeout_nanos: Option<u64>,
    pub max_background_threads: Option<u64>,
    pub max_flush_concurrency: Option<u64>,
    pub max_compaction_concurrency: Option<u64>,
    pub max_retention_tiering_concurrency: Option<u64>,
    pub max_remote_catalog_refresh_concurrency: Option<u64>,
    pub max_remote_tier_fetch_concurrency: Option<u64>,
    pub max_rollup_concurrency: Option<u64>,
    pub flush_interval_nanos: Option<u64>,
    pub compaction_interval_nanos: Option<u64>,
    pub persisted_refresh_poll_interval_nanos: Option<u64>,
    pub rollup_interval_nanos: Option<u64>,
    pub max_active_partition_heads_per_series: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UAsyncResourceLimits {
    pub queue_command_capacity: u64,
    pub write_queue_byte_capacity: u64,
    pub read_queue_byte_capacity: u64,
    pub read_workers: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UResolvedResourceLimits {
    pub storage: UEffectiveStorageLimits,
    pub query: UQueryBudgetLimits,
    pub async_runtime: Option<UAsyncResourceLimits>,
    pub maintenance_max_items_per_pass: Option<u64>,
    pub maintenance_max_bytes_per_pass: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UResourceConfigurationSnapshot {
    pub schema_version: u32,
    pub reported_by_backend: bool,
    pub selected_profile: crate::enums::UResourceProfileName,
    /// Stable snake-case low-level override names.
    pub overrides: Vec<String>,
    pub resolved_limits: UResolvedResourceLimits,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UCardinalityObservabilitySnapshot {
    pub series_count: u64,
    pub pending_new_series: u64,
    pub committed_in_window: u64,
    pub current_window_start: Option<i64>,
    pub admitted_new_series_total: u64,
    pub committed_new_series_total: u64,
    pub creation_rate_rejections_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct ULocalDiskLimits {
    pub max_bytes: Option<u64>,
    pub filesystem_free_headroom_bytes: u64,
    pub maintenance_temp_reserve_bytes: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UDiskCategoryUsage {
    pub category: UDiskCategory,
    pub bytes: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct ULocalDiskBudgetSnapshot {
    pub limits: ULocalDiskLimits,
    pub accounted_bytes: u64,
    pub reserved_bytes: u64,
    pub maintenance_reserved_bytes: u64,
    pub unknown_bytes: u64,
    pub filesystem_available_bytes: Option<u64>,
    pub over_limit: bool,
    pub active_reservations: u64,
    pub rejections_total: u64,
    pub reconciliations_total: u64,
    pub reservation_overruns_total: u64,
    pub categories: Vec<UDiskCategoryUsage>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UMemoryPressureSnapshot {
    pub level: Option<UMemoryPressureLevel>,
    pub approaching_limit_basis_points: Option<u16>,
    pub approaching_limit_bytes: Option<u64>,
    pub active_backpressured_writers: u64,
    pub backpressure_events_total: u64,
    pub rejections_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UMemoryObservabilitySnapshot {
    pub accounted_bytes: u64,
    pub estimated_accounted_bytes: u64,
    pub budgeted_bytes: u64,
    pub excluded_bytes: u64,
    pub excluded_bytes_known: bool,
    pub excluded_categories: Vec<String>,
    pub active_and_sealed_bytes: u64,
    pub registry_bytes: u64,
    pub metadata_cache_bytes: u64,
    pub persisted_index_bytes: u64,
    pub persisted_mmap_bytes: u64,
    pub tombstone_bytes: u64,
    #[uniffi(default)]
    pub remote_catalog_staging_bytes: u64,
    #[uniffi(default)]
    pub wal_writer_buffer_bytes: u64,
    #[uniffi(default)]
    pub wal_series_definition_cache_bytes: u64,
    #[uniffi(default)]
    pub write_transient_bytes: u64,
    #[uniffi(default)]
    pub peak_write_transient_bytes: u64,
    #[uniffi(default)]
    pub write_transient_reservations_total: u64,
    #[uniffi(default)]
    pub write_transient_rejections_total: u64,
    #[uniffi(default)]
    pub write_transient_bytes_estimated: bool,
    pub excluded_persisted_mmap_bytes: u64,
    pub pressure: UMemoryPressureSnapshot,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UWalObservabilitySnapshot {
    pub enabled: bool,
    pub sync_mode: String,
    pub acknowledged_writes_durable: bool,
    #[uniffi(default)]
    pub write_buffer_capacity_bytes: u64,
    pub size_bytes: u64,
    pub segment_count: u64,
    pub active_segment: u64,
    pub highwater_segment: u64,
    pub highwater_frame: u64,
    pub durable_highwater_segment: u64,
    pub durable_highwater_frame: u64,
    pub replay_runs_total: u64,
    pub replay_frames_total: u64,
    pub replay_series_definitions_total: u64,
    pub replay_sample_batches_total: u64,
    pub replay_points_total: u64,
    pub replay_errors_total: u64,
    pub replay_duration_nanos_total: u64,
    pub append_series_definitions_total: u64,
    pub append_sample_batches_total: u64,
    pub append_points_total: u64,
    pub append_bytes_total: u64,
    pub append_errors_total: u64,
    pub resets_total: u64,
    pub reset_errors_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URetentionObservabilitySnapshot {
    pub max_observed_timestamp: Option<i64>,
    pub recency_reference_timestamp: Option<i64>,
    pub future_skew_window: i64,
    pub future_skew_points_total: u64,
    pub future_skew_max_timestamp: Option<i64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UFlushObservabilitySnapshot {
    pub pipeline_runs_total: u64,
    pub pipeline_success_total: u64,
    pub pipeline_timeout_total: u64,
    pub pipeline_errors_total: u64,
    pub pipeline_duration_nanos_total: u64,
    pub admission_backpressure_delays_total: u64,
    pub admission_backpressure_delay_nanos_total: u64,
    pub admission_pressure_relief_requests_total: u64,
    pub admission_pressure_relief_observed_total: u64,
    pub active_flush_runs_total: u64,
    pub active_flush_errors_total: u64,
    pub active_flushed_series_total: u64,
    pub active_flushed_chunks_total: u64,
    pub active_flushed_points_total: u64,
    pub persist_runs_total: u64,
    pub persist_success_total: u64,
    pub persist_noop_total: u64,
    pub persist_errors_total: u64,
    pub persist_inspected_chunks_total: u64,
    pub persist_selected_input_bytes_total: u64,
    pub persist_item_limit_hits_total: u64,
    pub persist_byte_limit_hits_total: u64,
    pub persisted_series_total: u64,
    pub persisted_chunks_total: u64,
    pub persisted_points_total: u64,
    pub persisted_segments_total: u64,
    pub persist_duration_nanos_total: u64,
    pub evicted_sealed_chunks_total: u64,
    pub tier_moves_total: u64,
    pub tier_move_errors_total: u64,
    pub expired_segments_total: u64,
    pub hot_segments_visible: u64,
    pub warm_segments_visible: u64,
    pub cold_segments_visible: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UCompactionObservabilitySnapshot {
    pub runs_total: u64,
    pub success_total: u64,
    pub noop_total: u64,
    pub errors_total: u64,
    pub source_segments_total: u64,
    pub output_segments_total: u64,
    pub source_chunks_total: u64,
    pub output_chunks_total: u64,
    pub source_points_total: u64,
    pub output_points_total: u64,
    pub duration_nanos_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryObservabilitySnapshot {
    pub select_calls_total: u64,
    pub select_errors_total: u64,
    pub select_duration_nanos_total: u64,
    pub select_points_returned_total: u64,
    pub select_with_options_calls_total: u64,
    pub select_with_options_errors_total: u64,
    pub select_with_options_duration_nanos_total: u64,
    pub select_with_options_points_returned_total: u64,
    pub select_all_calls_total: u64,
    pub select_all_errors_total: u64,
    pub select_all_duration_nanos_total: u64,
    pub select_all_series_returned_total: u64,
    pub select_all_points_returned_total: u64,
    pub select_series_calls_total: u64,
    pub select_series_errors_total: u64,
    pub select_series_duration_nanos_total: u64,
    pub select_series_returned_total: u64,
    pub merge_path_queries_total: u64,
    pub merge_path_shard_snapshots_total: u64,
    pub merge_path_shard_snapshot_wait_nanos_total: u64,
    pub merge_path_shard_snapshot_hold_nanos_total: u64,
    pub append_sort_path_queries_total: u64,
    pub hot_only_query_plans_total: u64,
    pub warm_tier_query_plans_total: u64,
    pub cold_tier_query_plans_total: u64,
    pub hot_tier_persisted_chunks_read_total: u64,
    pub warm_tier_persisted_chunks_read_total: u64,
    pub cold_tier_persisted_chunks_read_total: u64,
    pub warm_tier_fetch_duration_nanos_total: u64,
    pub cold_tier_fetch_duration_nanos_total: u64,
    pub rollup_query_plans_total: u64,
    pub partial_rollup_query_plans_total: u64,
    pub rollup_points_read_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryWorkLimits {
    pub max_series_matched: Option<u64>,
    pub max_samples_scanned: Option<u64>,
    pub max_samples_returned: Option<u64>,
    pub max_returned_bytes: Option<u64>,
    pub max_pattern_expansion: Option<u64>,
    pub max_steps: Option<u64>,
    pub max_intermediate_vector_size: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_wall_time_nanos: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryBudgetLimits {
    pub max_concurrent_queries: Option<u64>,
    pub max_shared_memory_bytes: Option<u64>,
    pub per_query: UQueryWorkLimits,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryBudgetSnapshot {
    pub limits: UQueryBudgetLimits,
    pub active_queries: u64,
    pub peak_active_queries: u64,
    pub shared_reserved_memory_bytes: u64,
    pub peak_shared_reserved_memory_bytes: u64,
    pub queries_started_total: u64,
    pub queries_completed_total: u64,
    pub limit_rejections_total: u64,
    pub concurrency_rejections_total: u64,
    pub shared_memory_rejections_total: u64,
    pub per_query_memory_rejections_total: u64,
    pub series_matched_rejections_total: u64,
    pub samples_scanned_rejections_total: u64,
    pub samples_returned_rejections_total: u64,
    pub returned_bytes_rejections_total: u64,
    pub pattern_expansion_rejections_total: u64,
    pub steps_rejections_total: u64,
    pub intermediate_vector_size_rejections_total: u64,
    pub cancellations_total: u64,
    pub deadline_exceeded_total: u64,
    pub accounting_invariant_violations_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URemoteStorageObservabilitySnapshot {
    pub enabled: bool,
    pub runtime_mode: UStorageRuntimeMode,
    pub cache_policy: URemoteSegmentCachePolicy,
    pub metadata_refresh_interval_ms: u64,
    pub mirror_hot_segments: bool,
    pub catalog_refreshes_total: u64,
    pub catalog_refresh_errors_total: u64,
    pub accessible: bool,
    pub last_refresh_attempt_unix_ms: Option<u64>,
    pub last_successful_refresh_unix_ms: Option<u64>,
    pub consecutive_refresh_failures: u64,
    pub next_refresh_retry_unix_ms: Option<u64>,
    pub backoff_active: bool,
    pub last_refresh_error: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UBackgroundWorkerObservabilitySnapshot {
    pub installed: bool,
    pub running: bool,
    pub interval_nanos: Option<u64>,
    pub max_concurrency: u64,
    pub starts_total: u64,
    pub exits_total: u64,
    pub notifications_total: u64,
    pub idle_waits_total: u64,
    pub passes_started_total: u64,
    pub passes_completed_total: u64,
    pub shutdown_joins_total: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UBackgroundWorkObservabilitySnapshot {
    pub max_threads: u64,
    pub installed_threads: u64,
    pub running_threads: u64,
    pub close_attempts_total: u64,
    pub close_success_total: u64,
    pub close_errors_total: u64,
    pub close_coordination_wait_nanos_total: u64,
    pub close_coordination_timeouts_total: u64,
    pub close_compaction_passes_total: u64,
    pub close_compaction_pass_limit: u64,
    pub close_duration_nanos_total: u64,
    pub shutdown_join_wait_nanos_total: u64,
    pub flush: UBackgroundWorkerObservabilitySnapshot,
    pub compaction: UBackgroundWorkerObservabilitySnapshot,
    pub persisted_refresh: UBackgroundWorkerObservabilitySnapshot,
    pub rollup: UBackgroundWorkerObservabilitySnapshot,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UStorageHealthSnapshot {
    pub background_errors_total: u64,
    pub maintenance_errors_total: u64,
    pub degraded: bool,
    pub fail_fast_enabled: bool,
    pub fail_fast_triggered: bool,
    pub last_background_error: Option<String>,
    pub last_maintenance_error: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URollupPolicy {
    pub id: String,
    pub metric: String,
    pub match_labels: Vec<ULabel>,
    pub interval: i64,
    pub aggregation: UAggregation,
    pub bucket_origin: i64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URollupPolicyStatus {
    pub policy: URollupPolicy,
    pub matched_series: u64,
    pub materialized_series: u64,
    pub materialized_through: Option<i64>,
    pub lag: Option<i64>,
    pub source_traversal_complete: bool,
    pub last_run_started_at_ms: Option<u64>,
    pub last_run_completed_at_ms: Option<u64>,
    pub last_run_duration_nanos: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URollupObservabilitySnapshot {
    pub worker_runs_total: u64,
    pub worker_success_total: u64,
    pub worker_errors_total: u64,
    pub policy_runs_total: u64,
    pub buckets_materialized_total: u64,
    pub points_materialized_total: u64,
    pub last_run_duration_nanos: u64,
    pub source_traversal_complete: bool,
    pub continuation_policy_id: Option<String>,
    pub continuation_after_series_id: Option<u64>,
    pub policies: Vec<URollupPolicyStatus>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UStorageObservabilitySnapshot {
    pub limits: UEffectiveStorageLimits,
    pub resource_configuration: UResourceConfigurationSnapshot,
    pub local_disk: Option<ULocalDiskBudgetSnapshot>,
    pub memory: UMemoryObservabilitySnapshot,
    pub cardinality: UCardinalityObservabilitySnapshot,
    pub wal: UWalObservabilitySnapshot,
    pub retention: URetentionObservabilitySnapshot,
    pub flush: UFlushObservabilitySnapshot,
    pub compaction: UCompactionObservabilitySnapshot,
    pub query: UQueryObservabilitySnapshot,
    pub query_budget: UQueryBudgetSnapshot,
    pub rollups: URollupObservabilitySnapshot,
    pub remote: URemoteStorageObservabilitySnapshot,
    pub background: UBackgroundWorkObservabilitySnapshot,
    pub health: UStorageHealthSnapshot,
}
