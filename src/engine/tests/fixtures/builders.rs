use super::super::*;

pub(in crate::engine::storage_engine::tests) fn builder_at_time(now: i64) -> StorageBuilder {
    StorageBuilder::new().with_current_time_override_for_tests(now)
}

pub(in crate::engine::storage_engine::tests) fn default_future_skew_window(
    precision: TimestampPrecision,
) -> i64 {
    super::super::super::duration_to_timestamp_units(
        super::super::super::DEFAULT_FUTURE_SKEW_ALLOWANCE,
        precision,
    )
}

pub(in crate::engine::storage_engine::tests) fn base_storage_test_options(
    timestamp_precision: TimestampPrecision,
    current_time_override: Option<i64>,
) -> ChunkStorageOptions {
    ChunkStorageOptions {
        timestamp_precision,
        retention_window: i64::MAX,
        future_skew_window: default_future_skew_window(timestamp_precision),
        max_future_skew_window: None,
        retention_enforced: true,
        runtime_mode: StorageRuntimeMode::ReadWrite,
        partition_window: i64::MAX,
        max_active_partition_heads_per_series:
            crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
        max_writers: 2,
        write_timeout: Duration::from_secs(1),
        memory_budget_bytes: u64::MAX,
        cardinality_limit: usize::MAX,
        max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
        max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
        max_new_series_per_window: None,
        new_series_window_units: 1,
        new_series_window_nanos: 60_000_000_000,
        write_batch_limits: Default::default(),
        wal_size_limit_bytes: u64::MAX,
        admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
        compaction_interval: DEFAULT_COMPACTION_INTERVAL,
        maintenance_max_items_per_pass: 1_024,
        maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
        background_threads_enabled: false,
        background_fail_fast: false,
        metadata_shard_count: None,
        remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
        remote_segment_refresh_interval: Duration::from_secs(5),
        tiered_storage: None,
        #[cfg(test)]
        current_time_override,
    }
}
