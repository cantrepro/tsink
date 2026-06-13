use super::tiering::{PersistedSegmentTier, SegmentLaneFamily, SEGMENT_CATALOG_FILE_NAME};
use super::*;

#[derive(Debug, Clone)]
pub(super) struct TieredStorageConfig {
    pub(super) object_store_root: PathBuf,
    pub(super) segment_catalog_path: Option<PathBuf>,
    pub(super) mirror_hot_segments: bool,
    pub(super) hot_retention_window: i64,
    pub(super) warm_retention_window: i64,
}

impl TieredStorageConfig {
    pub(super) fn lane_path(&self, lane: SegmentLaneFamily, tier: PersistedSegmentTier) -> PathBuf {
        let tier_name = match tier {
            PersistedSegmentTier::Hot => "hot",
            PersistedSegmentTier::Warm => "warm",
            PersistedSegmentTier::Cold => "cold",
        };
        self.object_store_root
            .join(tier_name)
            .join(lane.root_name())
    }
}

#[derive(Debug, Clone)]
pub(super) struct ChunkStorageOptions {
    pub(super) timestamp_precision: TimestampPrecision,
    pub(super) retention_window: i64,
    pub(super) future_skew_window: i64,
    pub(super) retention_enforced: bool,
    pub(super) runtime_mode: StorageRuntimeMode,
    pub(super) partition_window: i64,
    pub(super) max_active_partition_heads_per_series: usize,
    pub(super) max_writers: usize,
    pub(super) write_timeout: Duration,
    pub(super) memory_budget_bytes: u64,
    pub(super) cardinality_limit: usize,
    pub(super) wal_size_limit_bytes: u64,
    pub(super) admission_poll_interval: Duration,
    pub(super) compaction_interval: Duration,
    pub(super) background_threads_enabled: bool,
    pub(super) background_fail_fast: bool,
    pub(super) metadata_shard_count: Option<u32>,
    pub(super) remote_segment_cache_policy: RemoteSegmentCachePolicy,
    pub(super) remote_segment_refresh_interval: Duration,
    pub(super) tiered_storage: Option<TieredStorageConfig>,
    #[cfg(test)]
    pub(super) current_time_override: Option<i64>,
}

impl Default for ChunkStorageOptions {
    fn default() -> Self {
        Self {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: duration_to_timestamp_units(
                DEFAULT_RETENTION,
                TimestampPrecision::Nanoseconds,
            ),
            future_skew_window: duration_to_timestamp_units(
                DEFAULT_FUTURE_SKEW_ALLOWANCE,
                TimestampPrecision::Nanoseconds,
            )
            .max(0),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: duration_to_timestamp_units(
                DEFAULT_PARTITION_DURATION,
                TimestampPrecision::Nanoseconds,
            )
            .max(1),
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: crate::cgroup::default_workers_limit().max(1),
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: true,
            background_fail_fast: true,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval:
                crate::storage::DEFAULT_REMOTE_SEGMENT_REFRESH_INTERVAL,
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct StoragePathLayout {
    pub(super) numeric_lane_path: Option<PathBuf>,
    pub(super) blob_lane_path: Option<PathBuf>,
    pub(super) series_index_path: Option<PathBuf>,
    pub(super) wal_path: Option<PathBuf>,
}

impl From<&StorageBuilder> for StoragePathLayout {
    fn from(builder: &StorageBuilder) -> Self {
        let base_data_path = if builder.runtime_mode() == StorageRuntimeMode::ComputeOnly {
            None
        } else {
            builder.data_path().map(|path| path.to_path_buf())
        };
        let (numeric_lane_path, blob_lane_path) = if let Some(base_path) = &base_data_path {
            (
                Some(base_path.join(NUMERIC_LANE_ROOT)),
                Some(base_path.join(BLOB_LANE_ROOT)),
            )
        } else {
            (None, None)
        };

        let series_index_path = base_data_path
            .as_ref()
            .map(|base_path| base_path.join(SERIES_INDEX_FILE_NAME));
        let wal_path = base_data_path
            .as_ref()
            .map(|base_path| base_path.join(WAL_DIR_NAME));

        Self {
            numeric_lane_path,
            blob_lane_path,
            series_index_path,
            wal_path,
        }
    }
}

impl From<&StorageBuilder> for ChunkStorageOptions {
    fn from(builder: &StorageBuilder) -> Self {
        let timestamp_precision = builder.timestamp_precision();
        let runtime_mode = builder.runtime_mode();
        let has_data_path =
            builder.data_path().is_some() && runtime_mode == StorageRuntimeMode::ReadWrite;
        let needs_compute_only_refresh_worker = runtime_mode == StorageRuntimeMode::ComputeOnly
            && builder.object_store_path().is_some();
        Self {
            timestamp_precision,
            retention_window: duration_to_timestamp_units(builder.retention(), timestamp_precision),
            future_skew_window: duration_to_timestamp_units(
                DEFAULT_FUTURE_SKEW_ALLOWANCE,
                timestamp_precision,
            )
            .max(0),
            retention_enforced: builder.retention_enforced(),
            runtime_mode,
            partition_window: duration_to_timestamp_units(
                builder.partition_duration(),
                timestamp_precision,
            )
            .max(1),
            max_active_partition_heads_per_series: builder.max_active_partition_heads_per_series(),
            max_writers: builder.max_writers(),
            write_timeout: builder.write_timeout(),
            memory_budget_bytes: builder.memory_limit_bytes().min(u64::MAX as usize) as u64,
            cardinality_limit: builder.cardinality_limit(),
            wal_size_limit_bytes: builder.wal_size_limit_bytes().min(u64::MAX as usize) as u64,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: has_data_path || needs_compute_only_refresh_worker,
            background_fail_fast: builder.background_fail_fast(),
            metadata_shard_count: builder.metadata_shard_count().filter(|count| *count > 0),
            remote_segment_cache_policy: builder.remote_segment_cache_policy(),
            remote_segment_refresh_interval: builder.remote_segment_refresh_interval(),
            tiered_storage: builder.object_store_path().map(|path| TieredStorageConfig {
                object_store_root: path.to_path_buf(),
                segment_catalog_path: builder
                    .data_path()
                    .filter(|_| runtime_mode == StorageRuntimeMode::ReadWrite)
                    .map(|data_path| data_path.join(SEGMENT_CATALOG_FILE_NAME)),
                mirror_hot_segments: runtime_mode == StorageRuntimeMode::ReadWrite
                    && builder.mirror_hot_segments_to_object_store(),
                hot_retention_window: duration_to_timestamp_units(
                    builder
                        .hot_tier_retention()
                        .unwrap_or_else(|| builder.retention()),
                    timestamp_precision,
                )
                .max(0),
                warm_retention_window: duration_to_timestamp_units(
                    builder
                        .warm_tier_retention()
                        .unwrap_or_else(|| builder.retention()),
                    timestamp_precision,
                )
                .max(0),
            }),
            #[cfg(test)]
            current_time_override: builder.current_time_override_for_tests(),
        }
    }
}
