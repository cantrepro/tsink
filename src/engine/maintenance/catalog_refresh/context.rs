use super::super::tiering::{self, PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry};
#[cfg(test)]
use super::super::Arc;
use super::super::{
    current_unix_millis_u64, AtomicBool, AtomicU64, ChunkStorage, Duration, Instant, Mutex,
    Ordering, Path, PathBuf, PendingPersistedSegmentDiff, PersistedIndexState,
    PersistedRefreshContext, RemoteCatalogRefreshState, Result, RwLock,
    StorageObservabilityCounters, StorageRuntimeMode, TsinkError,
    MAX_REMOTE_CATALOG_FAILURE_BACKOFF, MIN_REMOTE_CATALOG_FAILURE_BACKOFF,
};
use super::*;
use crate::engine::segment::{
    is_not_found_error, runtime_refresh_disappearance_error, segment_validation_error,
    segment_validation_error_message, IndexedSegment, SegmentValidationContext,
};

struct PersistedCatalogRefreshSnapshot {
    visibility_fence: PersistedCatalogVisibilityFence,
    visible_roots: BTreeSet<PathBuf>,
    tombstones: crate::engine::tombstone::TombstoneMap,
}

#[derive(Clone, Copy)]
pub(super) struct CatalogRefreshContext<'a> {
    refresh: PersistedRefreshContext<'a>,
    persisted_index: &'a RwLock<PersistedIndexState>,
    persisted_index_dirty: &'a AtomicBool,
    pending_persisted_segment_diff: &'a Mutex<PendingPersistedSegmentDiff>,
    numeric_lane_path: Option<&'a Path>,
    blob_lane_path: Option<&'a Path>,
    tiered_storage: Option<&'a super::super::config::TieredStorageConfig>,
    runtime_mode: StorageRuntimeMode,
    remote_segment_refresh_interval: Duration,
    remote_catalog_refresh_state: &'a Mutex<RemoteCatalogRefreshState>,
    visibility_state_generation: &'a AtomicU64,
    observability: &'a StorageObservabilityCounters,
    #[cfg(test)]
    persist_test_hooks: &'a super::super::PersistTestHooks,
}

impl<'a> CatalogRefreshContext<'a> {
    pub(super) fn try_claim_persisted_refresh(self) -> Option<PersistedRefreshClaim<'a>> {
        self.refresh
            .persisted_refresh_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| PersistedRefreshClaim {
                refresh: self.refresh,
            })
    }

    pub(super) fn take_known_persisted_segment_changes(self) -> PendingPersistedSegmentDiff {
        std::mem::take(&mut *self.pending_persisted_segment_diff.lock())
    }

    pub(super) fn restore_known_persisted_segment_changes(self, diff: PendingPersistedSegmentDiff) {
        if !diff.is_empty() {
            self.pending_persisted_segment_diff.lock().merge(diff);
        }
    }

    pub(super) fn has_known_persisted_segment_changes(self) -> bool {
        !self.pending_persisted_segment_diff.lock().is_empty()
    }

    pub(super) fn persisted_index_dirty(self) -> bool {
        self.persisted_index_dirty.load(Ordering::SeqCst)
    }

    pub(super) fn set_persisted_index_dirty(self, dirty: bool) {
        self.persisted_index_dirty.store(dirty, Ordering::SeqCst);
    }

    pub(super) fn runtime_refresh_segment_inventory(self) -> Result<SegmentInventory> {
        #[cfg(test)]
        self.invoke_full_inventory_scan_hook();

        tiering::build_segment_inventory_runtime_strict(
            self.numeric_lane_path,
            self.blob_lane_path,
            self.tiered_storage,
        )
    }

    #[cfg(test)]
    fn invoke_full_inventory_scan_hook(self) {
        let hook = self
            .persist_test_hooks
            .full_inventory_scan_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn visibility_generation(self) -> u64 {
        self.visibility_state_generation.load(Ordering::Acquire)
    }

    pub(super) fn should_refresh_remote_catalog(self) -> bool {
        if self.runtime_mode != StorageRuntimeMode::ComputeOnly || self.tiered_storage.is_none() {
            return false;
        }
        let now = Instant::now();
        let state = self.remote_catalog_refresh_state.lock();
        state
            .last_successful_refresh
            .as_ref()
            .is_none_or(|instant| instant.elapsed() >= self.remote_segment_refresh_interval)
            && state.next_retry_at.is_none_or(|retry_at| now >= retry_at)
    }

    fn remote_catalog_refresh_failure_backoff(self, consecutive_failures: u32) -> Duration {
        let base = self
            .remote_segment_refresh_interval
            .max(MIN_REMOTE_CATALOG_FAILURE_BACKOFF);
        let base_ms = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
        let max_ms =
            u64::try_from(MAX_REMOTE_CATALOG_FAILURE_BACKOFF.as_millis()).unwrap_or(u64::MAX);
        let shift = consecutive_failures.saturating_sub(1).min(8);
        let multiplier = 1u64 << shift;
        Duration::from_millis(base_ms.saturating_mul(multiplier).min(max_ms))
    }

    pub(super) fn mark_remote_catalog_refresh_success(self) {
        let refreshed_at = Instant::now();
        let refreshed_at_unix_ms = current_unix_millis_u64();
        {
            let mut state = self.remote_catalog_refresh_state.lock();
            state.last_successful_refresh = Some(refreshed_at);
            state.consecutive_failures = 0;
            state.next_retry_at = None;
        }
        self.observability
            .record_remote_catalog_refresh_success(refreshed_at_unix_ms);
    }

    pub(super) fn mark_remote_catalog_refresh_error(
        self,
        operation: &'static str,
        error: &TsinkError,
    ) -> Duration {
        let attempted_at = Instant::now();
        let attempted_at_unix_ms = current_unix_millis_u64();
        let (consecutive_failures, backoff) = {
            let mut state = self.remote_catalog_refresh_state.lock();
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            let backoff = self.remote_catalog_refresh_failure_backoff(state.consecutive_failures);
            state.next_retry_at = Some(attempted_at + backoff);
            (state.consecutive_failures, backoff)
        };
        let next_retry_unix_ms = attempted_at_unix_ms
            .saturating_add(u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX));
        self.observability.record_remote_catalog_refresh_error(
            operation,
            error,
            attempted_at_unix_ms,
            next_retry_unix_ms,
            consecutive_failures,
        );
        backoff
    }

    pub(super) fn persisted_segment_inventory(self) -> SegmentInventory {
        let entries = self
            .persisted_index
            .read()
            .segments_by_root
            .iter()
            .map(|(root, state)| SegmentInventoryEntry {
                lane: state.lane,
                tier: state.tier,
                root: root.clone(),
                manifest: state.manifest.clone(),
            })
            .collect::<Vec<_>>();
        SegmentInventory::from_entries(entries)
    }

    fn capture_persisted_catalog_refresh_snapshot(self) -> Result<PersistedCatalogRefreshSnapshot> {
        let visibility_fence = PersistedCatalogVisibilityFence {
            visibility_generation: self.visibility_generation(),
        };
        let visible_roots = self
            .persisted_index
            .read()
            .segments_by_root
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut tombstones = crate::engine::tombstone::TombstoneMap::new();
        let mut paths = BTreeSet::new();
        if let Some(path) = self.numeric_lane_path {
            paths.insert(path.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME));
        }
        if let Some(path) = self.blob_lane_path {
            paths.insert(path.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME));
        }
        if let Some(config) = self.tiered_storage {
            for lane in [
                super::super::tiering::SegmentLaneFamily::Numeric,
                super::super::tiering::SegmentLaneFamily::Blob,
            ] {
                for tier in [
                    PersistedSegmentTier::Hot,
                    PersistedSegmentTier::Warm,
                    PersistedSegmentTier::Cold,
                ] {
                    paths.insert(
                        config
                            .lane_path(lane, tier)
                            .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
                    );
                }
            }
        }
        for path in paths {
            for (series_id, ranges) in crate::engine::tombstone::load_tombstones(&path)? {
                for range in ranges {
                    crate::engine::tombstone::merge_tombstone_range(
                        tombstones.entry(series_id).or_default(),
                        range,
                    );
                }
            }
        }
        Ok(PersistedCatalogRefreshSnapshot {
            visibility_fence,
            visible_roots,
            tombstones,
        })
    }

    fn current_persisted_segment_inventory_with_exclusions(
        self,
        numeric_exclusions: Option<&HashSet<PathBuf>>,
        blob_exclusions: Option<&HashSet<PathBuf>>,
    ) -> Result<SegmentInventory> {
        if self.numeric_lane_path.is_none()
            && self.blob_lane_path.is_none()
            && self.tiered_storage.is_none()
        {
            return Ok(SegmentInventory::default());
        }

        let entries =
            self.runtime_refresh_segment_inventory()?
                .entries()
                .iter()
                .filter(|entry| match entry.lane {
                    super::super::tiering::SegmentLaneFamily::Numeric => !numeric_exclusions
                        .is_some_and(|exclusions| exclusions.contains(&entry.root)),
                    super::super::tiering::SegmentLaneFamily::Blob => {
                        !blob_exclusions.is_some_and(|exclusions| exclusions.contains(&entry.root))
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
        Ok(SegmentInventory::from_entries(entries))
    }

    fn load_inventory_catalog_refresh(
        self,
        snapshot: PersistedCatalogRefreshSnapshot,
        inventory: SegmentInventory,
    ) -> LoadedInventoryCatalogRefresh {
        LoadedInventoryCatalogRefresh {
            visibility_fence: snapshot.visibility_fence,
            visible_roots: snapshot.visible_roots,
            tombstones: snapshot.tombstones,
            inventory,
        }
    }

    pub(super) fn load_scanned_catalog_refresh_phase(
        self,
    ) -> Result<LoadedInventoryCatalogRefresh> {
        let snapshot = self.capture_persisted_catalog_refresh_snapshot()?;
        let inventory = self.current_persisted_segment_inventory_with_exclusions(None, None)?;
        Ok(self.load_inventory_catalog_refresh(snapshot, inventory))
    }

    fn load_shared_catalog_refresh(self) -> Result<Option<LoadedInventoryCatalogRefresh>> {
        let Some(config) = self.tiered_storage else {
            return Ok(None);
        };
        let catalog_path = tiering::shared_segment_catalog_path(config);
        if !crate::engine::fs_utils::path_exists_no_follow(&catalog_path)? {
            return Ok(None);
        }

        let snapshot = self.capture_persisted_catalog_refresh_snapshot()?;
        let inventory = tiering::load_segment_catalog(
            &catalog_path,
            self.numeric_lane_path,
            self.blob_lane_path,
            self.tiered_storage,
        )?;
        Ok(Some(
            self.load_inventory_catalog_refresh(snapshot, inventory),
        ))
    }

    pub(super) fn load_remote_catalog_refresh_phase(self) -> Result<LoadedInventoryCatalogRefresh> {
        match self.load_shared_catalog_refresh() {
            Ok(Some(loaded)) => Ok(loaded),
            Ok(None) => self.load_scanned_catalog_refresh_phase(),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Failed to load the shared remote segment catalog; falling back to a full inventory scan"
                );
                self.load_scanned_catalog_refresh_phase()
            }
        }
    }
}

impl ChunkStorage {
    pub(super) fn catalog_refresh_context(&self) -> CatalogRefreshContext<'_> {
        CatalogRefreshContext {
            refresh: self.persisted_refresh_context(),
            persisted_index: &self.persisted.persisted_index,
            persisted_index_dirty: self.persisted.persisted_index_dirty.as_ref(),
            pending_persisted_segment_diff: &self.persisted.pending_persisted_segment_diff,
            numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
            blob_lane_path: self.persisted.blob_lane_path.as_deref(),
            tiered_storage: self.persisted.tiered_storage.as_ref(),
            runtime_mode: self.runtime.runtime_mode,
            remote_segment_refresh_interval: self.persisted.remote_segment_refresh_interval,
            remote_catalog_refresh_state: &self.persisted.remote_catalog_refresh_state,
            visibility_state_generation: &self.visibility.visibility_state_generation,
            observability: self.observability.as_ref(),
            #[cfg(test)]
            persist_test_hooks: &self.persist_test_hooks,
        }
    }

    pub(in crate::engine::storage_engine) fn restore_known_persisted_segment_changes(
        &self,
        diff: PendingPersistedSegmentDiff,
    ) {
        self.catalog_refresh_context()
            .restore_known_persisted_segment_changes(diff);
    }

    pub(in crate::engine::storage_engine) fn has_known_persisted_segment_changes(&self) -> bool {
        self.catalog_refresh_context()
            .has_known_persisted_segment_changes()
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_full_inventory_scan_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.persist_test_hooks.full_inventory_scan_hook.write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn clear_full_inventory_scan_hook(&self) {
        *self.persist_test_hooks.full_inventory_scan_hook.write() = None;
    }

    pub(in crate::engine::storage_engine) fn load_segment_index_for_runtime_refresh(
        root: &Path,
    ) -> Result<IndexedSegment> {
        match crate::engine::segment::load_segment_index(root) {
            Ok(segment) => Ok(segment),
            Err(err) if is_not_found_error(&err) => {
                Err(runtime_refresh_disappearance_error(root, &err))
            }
            Err(err) => {
                if let Some(details) = segment_validation_error_message(&err) {
                    Err(segment_validation_error(
                        root,
                        SegmentValidationContext::RuntimeRefresh,
                        &details,
                    ))
                } else {
                    Err(err)
                }
            }
        }
    }

    pub(in crate::engine::storage_engine) fn should_refresh_remote_catalog(&self) -> bool {
        self.catalog_refresh_context()
            .should_refresh_remote_catalog()
    }

    pub(in crate::engine::storage_engine) fn mark_remote_catalog_refresh_success(&self) {
        self.catalog_refresh_context()
            .mark_remote_catalog_refresh_success();
    }

    pub(super) fn mark_remote_catalog_refresh_error(
        &self,
        operation: &'static str,
        error: &TsinkError,
    ) -> Duration {
        self.catalog_refresh_context()
            .mark_remote_catalog_refresh_error(operation, error)
    }

    pub(in crate::engine::storage_engine) fn persisted_segment_inventory(
        &self,
    ) -> SegmentInventory {
        self.catalog_refresh_context().persisted_segment_inventory()
    }
}
