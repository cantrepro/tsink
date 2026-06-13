use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct ChunkStorageOptions {
    pub(super) retention_window: i64,
    pub(super) retention_enforced: bool,
    pub(super) partition_window: i64,
    pub(super) max_writers: usize,
    pub(super) write_timeout: Duration,
    pub(super) memory_budget_bytes: u64,
    pub(super) cardinality_limit: usize,
    pub(super) wal_size_limit_bytes: u64,
    pub(super) admission_poll_interval: Duration,
    pub(super) compaction_interval: Duration,
    pub(super) background_threads_enabled: bool,
}

impl Default for ChunkStorageOptions {
    fn default() -> Self {
        Self {
            retention_window: duration_to_timestamp_units(
                DEFAULT_RETENTION,
                TimestampPrecision::Nanoseconds,
            ),
            retention_enforced: true,
            partition_window: duration_to_timestamp_units(
                DEFAULT_PARTITION_DURATION,
                TimestampPrecision::Nanoseconds,
            )
            .max(1),
            max_writers: crate::cgroup::default_workers_limit().max(1),
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: true,
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
        let base_data_path = builder.data_path().map(|path| path.to_path_buf());
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
        let wal_enabled = builder.wal_enabled();
        Self {
            retention_window: duration_to_timestamp_units(builder.retention(), timestamp_precision),
            retention_enforced: builder.retention_enforced(),
            partition_window: duration_to_timestamp_units(
                builder.partition_duration(),
                timestamp_precision,
            )
            .max(1),
            max_writers: builder.max_writers(),
            write_timeout: builder.write_timeout(),
            memory_budget_bytes: builder.memory_limit_bytes().min(u64::MAX as usize) as u64,
            cardinality_limit: builder.cardinality_limit(),
            wal_size_limit_bytes: builder.wal_size_limit_bytes().min(u64::MAX as usize) as u64,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: wal_enabled,
        }
    }
}
