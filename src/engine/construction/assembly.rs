use super::super::*;
use super::{PendingPersistedSegmentDiff, RemoteCatalogRefreshState, StorageAssemblyResources};

pub(super) struct StorageStateAssembly {
    pub(super) catalog: CatalogState,
    pub(super) chunks: ChunkBufferState,
    pub(super) visibility: VisibilityState,
    pub(super) persisted: PersistedStorageState,
    pub(super) runtime: RuntimeConfigState,
    pub(super) memory: Arc<MemoryAccountingState>,
    pub(super) coordination: CoordinationState,
    pub(super) background: BackgroundWorkerSupervisorState,
    pub(super) rollups: RollupState,
    pub(super) query_budget: QueryBudget,
    pub(super) observability: Arc<StorageObservabilityCounters>,
}

impl StorageStateAssembly {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build(
        chunk_point_cap: usize,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        wal: Option<FramedWal>,
        options: &ChunkStorageOptions,
        query_budget_limits: crate::QueryBudgetLimits,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
        resources: StorageAssemblyResources,
    ) -> Result<Self> {
        let StorageAssemblyResources {
            series_index_path,
            next_segment_id,
            numeric_compactor,
            blob_compactor,
            lifecycle,
            compaction_lock,
            persisted_index_dirty,
            pending_persisted_segment_diff,
            observability,
        } = resources;
        // Charge the capacity retained by the live WAL writer, not the nominal builder value.
        // The buffer already exists at this point and coexists with runtime hydration, so seed the
        // shared counter before any hydrated state can be published.
        let wal_writer_buffer_bytes = wal
            .as_ref()
            .map(FramedWal::write_buffer_capacity_bytes)
            .unwrap_or(0);

        Ok(Self {
            catalog: Self::build_catalog_state(options),
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
                local_disk_budget.clone(),
                options.tiered_storage.clone(),
                options.remote_segment_cache_policy,
                options.remote_segment_refresh_interval,
                persisted_index_dirty,
                pending_persisted_segment_diff,
            ),
            runtime: Self::build_runtime_config_state(options),
            memory: Self::build_memory_accounting_state(options, wal_writer_buffer_bytes),
            coordination: Self::build_coordination_state(lifecycle, compaction_lock),
            background: Self::build_background_worker_supervision_state(
                options.compaction_interval,
                options.background_fail_fast,
            ),
            rollups: Self::build_rollup_state(series_index_path, local_disk_budget),
            query_budget: QueryBudget::new(query_budget_limits)
                .map_err(crate::QueryBudgetError::from)?,
            observability,
        })
    }

    fn build_catalog_state(options: &ChunkStorageOptions) -> CatalogState {
        CatalogState {
            registry: RwLock::new(SeriesRegistry::new()),
            pending_series_ids: RwLock::new(BTreeSet::new()),
            delta_series_count: AtomicU64::new(0),
            persistence_lock: Mutex::new(()),
            metadata_shard_index: options.metadata_shard_count.map(MetadataShardIndex::new),
            write_txn_shards: std::array::from_fn(|_| Mutex::new(())),
            series_creation_rate_limiter: SeriesCreationRateLimiter::new(
                options.max_new_series_per_window,
                options.new_series_window_units,
                options.new_series_window_nanos,
            ),
        }
    }

    fn build_chunk_buffer_state(chunk_point_cap: usize) -> ChunkBufferState {
        ChunkBufferState {
            active_builders: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            active_wal_index: Mutex::new(ActiveWalIndex::default()),
            sealed_chunks: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            pending_sealed_chunks: RwLock::new(PendingSealedChunkIndex::default()),
            persisted_chunk_watermarks: RwLock::new(HashMap::new()),
            next_chunk_sequence: AtomicU64::new(1),
            chunk_point_cap: chunk_point_cap.clamp(1, u16::MAX as usize),
            background_active_flush_cursor: Mutex::new(BackgroundActiveFlushCursor::default()),
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
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
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
            local_disk_budget,
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
            max_future_skew_window: options.max_future_skew_window.map(|window| window.max(0)),
            retention_enforced: options.retention_enforced,
            runtime_mode: options.runtime_mode,
            partition_window: options.partition_window.max(1),
            max_active_partition_heads_per_series: options
                .max_active_partition_heads_per_series
                .max(1),
            write_limiter: Semaphore::new(options.max_writers.max(1)),
            write_timeout: options.write_timeout,
            cardinality_limit: options.cardinality_limit,
            max_labels_per_series: options.max_labels_per_series,
            max_series_identity_bytes: options.max_series_identity_bytes,
            max_new_series_per_window: options.max_new_series_per_window,
            new_series_window_nanos: options.new_series_window_nanos,
            write_batch_limits: options.write_batch_limits,
            wal_size_limit_bytes: options.wal_size_limit_bytes,
            admission_poll_interval: options.admission_poll_interval,
            maintenance_max_items_per_pass: options.maintenance_max_items_per_pass,
            maintenance_max_bytes_per_pass: options.maintenance_max_bytes_per_pass,
        }
    }

    fn build_memory_accounting_state(
        options: &ChunkStorageOptions,
        wal_writer_buffer_bytes: usize,
    ) -> Arc<MemoryAccountingState> {
        let initial_tombstone_bytes =
            crate::engine::tombstone::ImmutableTombstoneSnapshot::empty_memory_usage_bytes();
        let initial_wal_writer_buffer_bytes =
            u64::try_from(wal_writer_buffer_bytes).unwrap_or(u64::MAX);
        let initial_accounted_bytes =
            u64::try_from(wal_writer_buffer_bytes.saturating_add(initial_tombstone_bytes))
                .unwrap_or(u64::MAX);
        let initial_tombstone_bytes = u64::try_from(initial_tombstone_bytes).unwrap_or(u64::MAX);
        Arc::new(MemoryAccountingState {
            accounting_enabled: options.memory_budget_bytes != u64::MAX,
            used_bytes: AtomicU64::new(initial_accounted_bytes),
            used_bytes_by_shard: std::array::from_fn(|_| AtomicU64::new(0)),
            shared_used_bytes: AtomicU64::new(initial_accounted_bytes),
            registry_used_bytes: AtomicU64::new(0),
            metadata_used_bytes: AtomicU64::new(0),
            persisted_index_used_bytes: AtomicU64::new(0),
            persisted_mmap_used_bytes: AtomicU64::new(0),
            tombstone_used_bytes: AtomicU64::new(initial_tombstone_bytes),
            tombstone_staged_bytes: AtomicU64::new(0),
            remote_catalog_staging: Arc::new(RemoteCatalogMemoryAccounting::default()),
            wal_writer_buffer_used_bytes: AtomicU64::new(initial_wal_writer_buffer_bytes),
            wal_series_definition_cache_used_bytes: AtomicU64::new(0),
            write_transient: Arc::new(WriteTransientMemoryAccounting::default()),
            reservation_admission_lock: Mutex::new(()),
            budget_bytes: AtomicU64::new(options.memory_budget_bytes),
            active_backpressured_writers: AtomicU64::new(0),
            backpressure_events_total: AtomicU64::new(0),
            rejections_total: AtomicU64::new(0),
            backpressure_lock: Mutex::new(()),
            admission_backpressure_lock: Mutex::new(()),
        })
    }

    fn build_coordination_state(
        lifecycle: Arc<AtomicU8>,
        compaction_lock: Arc<Mutex<()>>,
    ) -> CoordinationState {
        CoordinationState {
            post_flush_maintenance_pending: AtomicBool::new(false),
            post_flush_marker_generation: Arc::new(AtomicU64::new(0)),
            startup_metadata_reconcile_pending: AtomicBool::new(false),
            prefer_metadata_reconcile_on_maintenance_tie: AtomicBool::new(false),
            bounded_registry_reconciliation_required: AtomicBool::new(false),
            background_retention_maintenance_cursor: Mutex::new(
                BackgroundRetentionMaintenanceCursor::default(),
            ),
            background_post_flush_recovery_cursor: Mutex::new(
                BackgroundPostFlushRecoveryCursor::default(),
            ),
            background_post_flush_clean_fence_cursor: Arc::new(Mutex::new(
                BackgroundPostFlushCleanFenceCursor::default(),
            )),
            background_metadata_reconciliation_cursor: Mutex::new(
                BackgroundMetadataReconciliationCursor::default(),
            ),
            background_tombstone_recovery_snapshot_cursor: Mutex::new(
                BackgroundTombstoneRecoverySnapshotCursor::default(),
            ),
            background_catalog_refresh_cursor: Mutex::new(BackgroundCatalogRefreshCursor::default()),
            lifecycle,
            background_maintenance_lock: Mutex::new(()),
            compaction_lock,
            data_path_process_lock: Mutex::new(None),
            shared_object_store_process_lock: Mutex::new(None),
        }
    }

    fn build_background_worker_supervision_state(
        compaction_interval: Duration,
        background_fail_fast: bool,
    ) -> BackgroundWorkerSupervisorState {
        BackgroundWorkerSupervisorState {
            compaction_thread: Mutex::new(None),
            compaction_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            flush_thread: Mutex::new(None),
            flush_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            flush_thread_wakeup_requested: AtomicBool::new(false),
            persisted_refresh_thread: Mutex::new(None),
            persisted_refresh_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            rollup_thread: Mutex::new(None),
            rollup_runtime: Arc::new(BackgroundWorkerRuntimeState::default()),
            close_attempts_total: AtomicU64::new(0),
            close_success_total: AtomicU64::new(0),
            close_errors_total: AtomicU64::new(0),
            close_coordination_wait_nanos_total: AtomicU64::new(0),
            close_coordination_timeouts_total: AtomicU64::new(0),
            close_compaction_passes_total: AtomicU64::new(0),
            close_duration_nanos_total: AtomicU64::new(0),
            shutdown_join_wait_nanos_total: AtomicU64::new(0),
            compaction_interval,
            fail_fast_enabled: background_fail_fast,
        }
    }

    fn build_rollup_state(
        series_index_path: Option<PathBuf>,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> RollupState {
        RollupState {
            runtime: rollups::RollupRuntimeState::new_with_disk_budget(
                series_index_path
                    .as_ref()
                    .and_then(|path| path.parent().map(|parent| parent.to_path_buf())),
                local_disk_budget,
            ),
            run_lock: Mutex::new(()),
            traversal_cursor: Mutex::new(rollups::BackgroundRollupCursor::default()),
        }
    }
}
