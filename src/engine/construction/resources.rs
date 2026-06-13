use super::*;
use std::time::Instant;

pub(in crate::engine::storage_engine) struct StorageAssemblyResources {
    pub(in crate::engine::storage_engine) series_index_path: Option<PathBuf>,
    pub(in crate::engine::storage_engine) next_segment_id: Arc<AtomicU64>,
    pub(in crate::engine::storage_engine) numeric_compactor: Option<Compactor>,
    pub(in crate::engine::storage_engine) blob_compactor: Option<Compactor>,
    pub(in crate::engine::storage_engine) lifecycle: Arc<AtomicU8>,
    pub(in crate::engine::storage_engine) compaction_lock: Arc<Mutex<()>>,
    pub(in crate::engine::storage_engine) compaction_thread: Option<std::thread::JoinHandle<()>>,
    pub(in crate::engine::storage_engine) persisted_index_dirty: Arc<AtomicBool>,
    pub(in crate::engine::storage_engine) pending_persisted_segment_diff:
        Arc<Mutex<PendingPersistedSegmentDiff>>,
    pub(in crate::engine::storage_engine) observability: Arc<StorageObservabilityCounters>,
}

#[derive(Debug, Clone, Default)]
pub(in crate::engine::storage_engine) struct RemoteCatalogRefreshState {
    pub(in crate::engine::storage_engine) last_successful_refresh: Option<Instant>,
    pub(in crate::engine::storage_engine) consecutive_failures: u32,
    pub(in crate::engine::storage_engine) next_retry_at: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub(in crate::engine::storage_engine) struct PendingPersistedSegmentDiff {
    pub(in crate::engine::storage_engine) added_roots: BTreeSet<PathBuf>,
    pub(in crate::engine::storage_engine) removed_roots: BTreeSet<PathBuf>,
}

impl PendingPersistedSegmentDiff {
    pub(in crate::engine::storage_engine) fn is_empty(&self) -> bool {
        self.added_roots.is_empty() && self.removed_roots.is_empty()
    }

    pub(in crate::engine::storage_engine) fn record_changes<I, J>(
        &mut self,
        added_roots: I,
        removed_roots: J,
    ) where
        I: IntoIterator<Item = PathBuf>,
        J: IntoIterator<Item = PathBuf>,
    {
        for root in added_roots {
            self.removed_roots.remove(&root);
            self.added_roots.insert(root);
        }
        for root in removed_roots {
            self.added_roots.remove(&root);
            self.removed_roots.insert(root);
        }
    }

    pub(in crate::engine::storage_engine) fn merge(&mut self, other: Self) {
        self.record_changes(other.added_roots, other.removed_roots);
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn prepare_construction_resources(
        chunk_point_cap: usize,
        numeric_lane_path: Option<&PathBuf>,
        blob_lane_path: Option<&PathBuf>,
        next_segment_id: u64,
        options: &ChunkStorageOptions,
    ) -> Result<StorageAssemblyResources> {
        let series_index_path = Self::series_index_path_for_lanes(
            numeric_lane_path.map(|path| path.as_path()),
            blob_lane_path.map(|path| path.as_path()),
        );
        let next_segment_id = Arc::new(AtomicU64::new(next_segment_id.max(1)));
        let numeric_compactor = numeric_lane_path.map(|path| {
            Compactor::new_with_segment_id_allocator(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
            )
        });
        let blob_compactor = blob_lane_path.map(|path| {
            Compactor::new_with_segment_id_allocator(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
            )
        });
        let lifecycle = Arc::new(AtomicU8::new(STORAGE_OPEN));
        let compaction_lock = Arc::new(Mutex::new(()));
        let persisted_index_dirty = Arc::new(AtomicBool::new(false));
        let pending_persisted_segment_diff =
            Arc::new(Mutex::new(PendingPersistedSegmentDiff::default()));
        let observability = Arc::new(StorageObservabilityCounters::default());
        let compaction_thread = if options.background_threads_enabled {
            Self::spawn_background_compaction_thread(
                Arc::downgrade(&lifecycle),
                Arc::clone(&compaction_lock),
                numeric_compactor.clone(),
                blob_compactor.clone(),
                Arc::clone(&persisted_index_dirty),
                Arc::clone(&pending_persisted_segment_diff),
                options.compaction_interval,
                Arc::clone(&observability),
                options.background_fail_fast,
            )?
        } else {
            None
        };

        Ok(StorageAssemblyResources {
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
        })
    }

    fn series_index_path_for_lanes(
        numeric_lane_path: Option<&Path>,
        blob_lane_path: Option<&Path>,
    ) -> Option<PathBuf> {
        numeric_lane_path
            .and_then(|path| {
                path.parent()
                    .map(|parent| parent.join(SERIES_INDEX_FILE_NAME))
            })
            .or_else(|| {
                blob_lane_path.and_then(|path| {
                    path.parent()
                        .map(|parent| parent.join(SERIES_INDEX_FILE_NAME))
                })
            })
    }
}
