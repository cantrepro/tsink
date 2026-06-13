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
        retention_enforced: true,
        runtime_mode: StorageRuntimeMode::ReadWrite,
        partition_window: i64::MAX,
        max_active_partition_heads_per_series:
            crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
        max_writers: 2,
        write_timeout: Duration::from_secs(1),
        memory_budget_bytes: u64::MAX,
        cardinality_limit: usize::MAX,
        wal_size_limit_bytes: u64::MAX,
        admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
        compaction_interval: DEFAULT_COMPACTION_INTERVAL,
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
