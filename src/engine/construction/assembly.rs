use super::super::*;
use super::{PendingPersistedSegmentDiff, RemoteCatalogRefreshState, StorageAssemblyResources};

pub(super) struct StorageStateAssembly {
    pub(super) catalog: CatalogState,
    pub(super) chunks: ChunkBufferState,
    pub(super) visibility: VisibilityState,
    pub(super) persisted: PersistedStorageState,
    pub(super) runtime: RuntimeConfigState,
    pub(super) memory: MemoryAccountingState,
    pub(super) coordination: CoordinationState,
    pub(super) background: BackgroundWorkerSupervisorState,
    pub(super) rollups: RollupState,
    pub(super) observability: Arc<StorageObservabilityCounters>,
}

impl StorageStateAssembly {
    pub(super) fn build(
        chunk_point_cap: usize,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        wal: Option<FramedWal>,
        options: &ChunkStorageOptions,
        resources: StorageAssemblyResources,
    ) -> Self {
        let StorageAssemblyResources {
            series_index_path,
            next_segment_id,
            numeric_compactor,
            blob_compactor,
            lifecycle,
            compaction_lock,
            compaction_thread,
            persisted_index_dirty,
            pending_persisted_segment_diff,
            observability,
        } = resources;

        Self {
            catalog: Self::build_catalog_state(options.metadata_shard_count),
            chunks: Self::build_chunk_buffer_state(chunk_point_cap),
            visibility: ChunkStorage::build_visibility_state(),
            persisted: Self::build_persisted_storage_state(
                numeric_lane_path,
                blob_lane_path,
                series_index_path.clone(),
                next_segment_id,
                numeric_compactor,
                blob_compactor,
                wal,
                options.tiered_storage.clone(),
                options.remote_segment_cache_policy,
                options.remote_segment_refresh_interval,
                persisted_index_dirty,
                pending_persisted_segment_diff,
            ),
            runtime: Self::build_runtime_config_state(options),
            memory: Self::build_memory_accounting_state(options),
            coordination: Self::build_coordination_state(lifecycle, compaction_lock),
            background: Self::build_background_worker_supervision_state(
                compaction_thread,
                options.background_fail_fast,
            ),
            rollups: Self::build_rollup_state(series_index_path),
            observability,
        }
    }

    fn build_catalog_state(metadata_shard_count: Option<u32>) -> CatalogState {
        CatalogState {
            registry: RwLock::new(SeriesRegistry::new()),
            pending_series_ids: RwLock::new(BTreeSet::new()),
            delta_series_count: AtomicU64::new(0),
            persistence_lock: Mutex::new(()),
            metadata_shard_index: metadata_shard_count.map(MetadataShardIndex::new),
            write_txn_shards: std::array::from_fn(|_| Mutex::new(())),
        }
    }

    fn build_chunk_buffer_state(chunk_point_cap: usize) -> ChunkBufferState {
        ChunkBufferState {
            active_builders: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            sealed_chunks: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            persisted_chunk_watermarks: RwLock::new(HashMap::new()),
            next_chunk_sequence: AtomicU64::new(1),
            chunk_point_cap: chunk_point_cap.clamp(1, u16::MAX as usize),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_persisted_storage_state(
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        series_index_path: Option<PathBuf>,
        next_segment_id: Arc<AtomicU64>,
        numeric_compactor: Option<Compactor>,
        blob_compactor: Option<Compactor>,
        wal: Option<FramedWal>,
        tiered_storage: Option<config::TieredStorageConfig>,
        remote_segment_cache_policy: RemoteSegmentCachePolicy,
        remote_segment_refresh_interval: Duration,
        persisted_index_dirty: Arc<AtomicBool>,
        pending_persisted_segment_diff: Arc<Mutex<PendingPersistedSegmentDiff>>,
    ) -> PersistedStorageState {
        PersistedStorageState {
            persisted_index: RwLock::new(PersistedIndexState::default()),
            persisted_index_dirty,
            numeric_lane_path,
            blob_lane_path,
            series_index_path,
            next_segment_id,
            numeric_compactor,
            blob_compactor,
            wal,
            tiered_storage,
            remote_segment_cache_policy,
            remote_segment_refresh_interval,
            remote_catalog_refresh_state: Mutex::new(RemoteCatalogRefreshState::default()),
            pending_persisted_segment_diff,
            persisted_refresh_in_progress: AtomicBool::new(false),
        }
    }

    fn build_runtime_config_state(options: &ChunkStorageOptions) -> RuntimeConfigState {
        RuntimeConfigState {
            timestamp_precision: options.timestamp_precision,
            retention_window: options.retention_window.max(0),
            future_skew_window: options.future_skew_window.max(0),
            retention_enforced: options.retention_enforced,
            runtime_mode: options.runtime_mode,
            partition_window: options.partition_window.max(1),
            max_active_partition_heads_per_series: options
                .max_active_partition_heads_per_series
                .max(1),
            write_limiter: Semaphore::new(options.max_writers.max(1)),
            write_timeout: options.write_timeout,
            cardinality_limit: options.cardinality_limit,
            wal_size_limit_bytes: options.wal_size_limit_bytes,
            admission_poll_interval: options.admission_poll_interval,
        }
    }

    fn build_memory_accounting_state(options: &ChunkStorageOptions) -> MemoryAccountingState {
        MemoryAccountingState {
            accounting_enabled: options.memory_budget_bytes != u64::MAX,
            used_bytes: AtomicU64::new(0),
            used_bytes_by_shard: std::array::from_fn(|_| AtomicU64::new(0)),
            shared_used_bytes: AtomicU64::new(0),
            registry_used_bytes: AtomicU64::new(0),
            metadata_used_bytes: AtomicU64::new(0),
            persisted_index_used_bytes: AtomicU64::new(0),
            persisted_mmap_used_bytes: AtomicU64::new(0),
            tombstone_used_bytes: AtomicU64::new(0),
            budget_bytes: AtomicU64::new(options.memory_budget_bytes),
            backpressure_lock: Mutex::new(()),
            admission_backpressure_lock: Mutex::new(()),
        }
    }

    fn build_coordination_state(
        lifecycle: Arc<AtomicU8>,
        compaction_lock: Arc<Mutex<()>>,
    ) -> CoordinationState {
        CoordinationState {
            post_flush_maintenance_pending: AtomicBool::new(false),
            startup_metadata_reconcile_pending: AtomicBool::new(false),
            lifecycle,
            background_maintenance_lock: Mutex::new(()),
            compaction_lock,
            data_path_process_lock: Mutex::new(None),
        }
    }

    fn build_background_worker_supervision_state(
        compaction_thread: Option<std::thread::JoinHandle<()>>,
        background_fail_fast: bool,
    ) -> BackgroundWorkerSupervisorState {
        BackgroundWorkerSupervisorState {
            compaction_thread: Mutex::new(compaction_thread),
            flush_thread: Mutex::new(None),
            flush_thread_wakeup_requested: AtomicBool::new(false),
            persisted_refresh_thread: Mutex::new(None),
            rollup_thread: Mutex::new(None),
            fail_fast_enabled: background_fail_fast,
        }
    }

    fn build_rollup_state(series_index_path: Option<PathBuf>) -> RollupState {
        RollupState {
            runtime: rollups::RollupRuntimeState::new(
                series_index_path
                    .as_ref()
                    .and_then(|path| path.parent().map(|parent| parent.to_path_buf())),
            ),
            run_lock: Mutex::new(()),
        }
    }
}
