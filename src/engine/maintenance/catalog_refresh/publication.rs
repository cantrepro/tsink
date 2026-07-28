use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::ReadDir;
use std::io::Write;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};

use xxhash_rust::xxh64::Xxh64;

use super::super::super::tiering::{SegmentCatalogPointer, SegmentLaneFamily};

const SEGMENT_CATALOG_PUBLICATION_BASE_STAGING_BYTES: usize = 16 * 1024;
const SEGMENT_CATALOG_SHARED_PATH_ALLOWANCE_BYTES: usize = 512;
const SEGMENT_CATALOG_SHARED_PATH_ALLOCATOR_BYTES: usize = 64;
const BOUNDED_TIERED_CATALOG_PUBLICATION_OPERATION: &str =
    "finite read-write tiered segment catalog publication";
const BOUNDED_TIERED_CATALOG_CURSOR_BASE_BYTES: usize = 16 * 1024;
const BOUNDED_TIERED_CATALOG_ENTRY_ALLOWANCE_BYTES: usize = 512;
const BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES: usize = 16 * 1024;
const BOUNDED_TIERED_CATALOG_STAGE_SUFFIX: &str = ".tiered-publish-stage";
const BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES: u64 = 256 * 1024;
const BOUNDED_REGISTRY_MANIFEST_WORK_BYTES: u64 = 16 * 1024;
const BOUNDED_REGISTRY_ENTRY_WORK_BYTES: u64 =
    crate::engine::segment::MAX_SEGMENT_MANIFEST_FILE_BYTES as u64 + 32 * 1024;
const BOUNDED_REGISTRY_SWEEP_ENTRY_WORK_BYTES: u64 = 256 * 1024;
// Removal accounting can retain decoded series definitions, deduplicated accounting-scope keys,
// registry keys/postings, and merged-postings keys at the same time. Keep this aligned with the
// finite one-root remote-apply preflight, but aggregate it across every root in a transition.
const FINITE_TRANSITION_REMOVAL_SERIES_METADATA_COPIES: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BoundedTieredCatalogKey {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
}

impl BoundedTieredCatalogKey {
    fn from_entry(entry: &SegmentInventoryEntry) -> Self {
        Self {
            lane: entry.lane,
            level: entry.manifest.level,
            segment_id: entry.manifest.segment_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedTieredCatalogPublicationPhase {
    PrepareGeneration,
    ScanGenerationNamespace,
    OpenGenerationCleanup,
    CleanupGenerationNamespace,
    FinalizeGenerationPreparation,
    Scanning,
    LocalLegacyHeader,
    LocalLegacyEntries,
    LocalLegacyFooter,
    LocalLegacyPublish,
    GenerationHeader,
    GenerationEntries,
    SharedLegacyHeader,
    SharedLegacyEntries,
    SharedLegacyFooter,
    SharedLegacyPublish,
    Pointer,
    RegistryPrepare,
    RegistryEntries,
    RegistrySweepOpen,
    RegistrySweepEntries,
    RegistryComplete,
    RegistryLegacyRetire,
}

/// Process-local continuation for a finite read-write catalog publication.
///
/// The visible persisted index remains authoritative while this cursor constructs immutable
/// staging files. A visibility-generation change discards the unpublished generation and restarts
/// from the beginning. Only the final pointer replacement commits the v3 generation. Registry
/// reconciliation retains one bounded directory reader while sweeping stale sidecar entries; close
/// and invalidation drop it with the rest of the process-local cursor.
pub(super) struct BoundedTieredCatalogPublicationCycle {
    expected_visibility_generation: u64,
    phase: BoundedTieredCatalogPublicationPhase,
    scan_after_root: Option<PathBuf>,
    entries: BTreeMap<BoundedTieredCatalogKey, SegmentInventoryEntry>,
    shared_keys: BTreeSet<BoundedTieredCatalogKey>,
    retained_entry_bytes: usize,
    local_after_key: Option<BoundedTieredCatalogKey>,
    local_entries_written: usize,
    generation_after_key: Option<BoundedTieredCatalogKey>,
    generation_entries_written: usize,
    shared_after_key: Option<BoundedTieredCatalogKey>,
    shared_entries_written: usize,
    local_target: Option<PathBuf>,
    local_stage: Option<PathBuf>,
    shared_target: PathBuf,
    shared_stage: PathBuf,
    pointer_path: PathBuf,
    generation_directory: PathBuf,
    generation_path: Option<PathBuf>,
    generation: Option<u64>,
    prior_pointer: Option<SegmentCatalogPointer>,
    generation_namespace_reader: Option<ReadDir>,
    generation_namespace_count: usize,
    generation_cleanup_expected_entries: usize,
    generation_cleanup_entries_seen: usize,
    generation_max_observed: Option<u64>,
    generation_predecessor: Option<u64>,
    generation_current_observed: bool,
    local_file_len: u64,
    shared_file_len: u64,
    generation_file_len: u64,
    generation_hash: Xxh64,
    pointer_may_be_visible: bool,
    reconcile_registry: bool,
    registry_snapshot_path: Option<PathBuf>,
    registry_store_path: Option<PathBuf>,
    registry_after_key: Option<BoundedTieredCatalogKey>,
    registry_reader: Option<ReadDir>,
    registry_sweep_entries_seen: usize,
    memory_reservation: RemoteCatalogMemoryReservation,
}

impl BoundedTieredCatalogPublicationCycle {
    fn stage_path(target: &Path) -> Result<PathBuf> {
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "tiered catalog target has no parent directory: {}",
                target.display()
            ))
        })?;
        let file_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "tiered catalog target has no UTF-8 file name: {}",
                    target.display()
                ))
            })?;
        Ok(parent.join(format!(".{file_name}{BOUNDED_TIERED_CATALOG_STAGE_SUFFIX}")))
    }

    fn new(
        storage: &ChunkStorage,
        config: &super::super::config::TieredStorageConfig,
        expected_visibility_generation: u64,
    ) -> Result<Self> {
        let shared_target = tiering::shared_segment_catalog_path(config);
        let shared_stage = Self::stage_path(&shared_target)?;
        let local_target = config
            .segment_catalog_path
            .as_ref()
            .filter(|path| *path != &shared_target)
            .cloned();
        let local_stage = local_target.as_deref().map(Self::stage_path).transpose()?;
        let pointer_path = tiering::shared_segment_catalog_pointer_path(config);
        let generation_directory = tiering::shared_segment_catalog_generation_directory(config);
        let registry_snapshot_path = storage.persisted.series_index_path.clone();
        let registry_store_path = registry_snapshot_path
            .as_deref()
            .map(registry_catalog::catalog_store_path);
        let retained_paths = local_target
            .iter()
            .chain(local_stage.iter())
            .chain([
                &shared_target,
                &shared_stage,
                &pointer_path,
                &generation_directory,
            ])
            .chain(registry_snapshot_path.iter())
            .chain(registry_store_path.iter())
            .fold(BOUNDED_TIERED_CATALOG_CURSOR_BASE_BYTES, |total, path| {
                total.saturating_add(
                    path.as_os_str()
                        .as_encoded_bytes()
                        .len()
                        .saturating_add(SEGMENT_CATALOG_SHARED_PATH_ALLOCATOR_BYTES),
                )
            });
        let memory_reservation = storage.remote_catalog_memory_reservation(retained_paths)?;
        Ok(Self {
            expected_visibility_generation,
            // Admit and inspect the complete fixed namespace dependency before consuming any
            // incremental inventory page. Doing this first preserves a single bounded pass for
            // small catalogs; deferring preparation after the first scanned entry otherwise
            // forced every non-empty publication to wait for an unrelated later wake.
            phase: BoundedTieredCatalogPublicationPhase::PrepareGeneration,
            scan_after_root: None,
            entries: BTreeMap::new(),
            shared_keys: BTreeSet::new(),
            retained_entry_bytes: 0,
            local_after_key: None,
            local_entries_written: 0,
            generation_after_key: None,
            generation_entries_written: 0,
            shared_after_key: None,
            shared_entries_written: 0,
            local_target,
            local_stage,
            shared_target,
            shared_stage,
            pointer_path,
            generation_directory,
            generation_path: None,
            generation: None,
            prior_pointer: None,
            generation_namespace_reader: None,
            generation_namespace_count: 0,
            generation_cleanup_expected_entries: 0,
            generation_cleanup_entries_seen: 0,
            generation_max_observed: None,
            generation_predecessor: None,
            generation_current_observed: false,
            local_file_len: 0,
            shared_file_len: 0,
            generation_file_len: 0,
            generation_hash: Xxh64::new(0),
            pointer_may_be_visible: false,
            reconcile_registry: storage
                .coordination
                .bounded_registry_reconciliation_required
                .load(Ordering::Acquire),
            registry_snapshot_path,
            registry_store_path,
            registry_after_key: None,
            registry_reader: None,
            registry_sweep_entries_seen: 0,
            memory_reservation,
        })
    }

    fn modeled_retained_bytes(&self) -> usize {
        BOUNDED_TIERED_CATALOG_CURSOR_BASE_BYTES
            .saturating_add(
                self.local_target
                    .iter()
                    .chain(self.local_stage.iter())
                    .chain([
                        &self.shared_target,
                        &self.shared_stage,
                        &self.pointer_path,
                        &self.generation_directory,
                    ])
                    .chain(self.registry_snapshot_path.iter())
                    .chain(self.registry_store_path.iter())
                    .fold(0usize, |total, path| {
                        total.saturating_add(
                            path.as_os_str()
                                .as_encoded_bytes()
                                .len()
                                .saturating_add(SEGMENT_CATALOG_SHARED_PATH_ALLOCATOR_BYTES),
                        )
                    }),
            )
            .saturating_add(self.retained_entry_bytes)
            .saturating_add(self.generation_path.as_ref().map_or(0, |path| {
                path.as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .saturating_add(SEGMENT_CATALOG_SHARED_PATH_ALLOCATOR_BYTES)
            }))
            .saturating_add(if self.registry_reader.is_some() {
                BOUNDED_REGISTRY_SWEEP_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize
            } else {
                0
            })
            .saturating_add(if self.generation_namespace_reader.is_some() {
                BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize
            } else {
                0
            })
    }

    fn resize_for_scratch(&mut self, storage: &ChunkStorage, scratch: usize) -> Result<()> {
        let requested = self.modeled_retained_bytes().saturating_add(scratch);
        storage.resize_remote_catalog_memory_reservation(&mut self.memory_reservation, requested)
    }

    fn restore_retained_reservation(&mut self, storage: &ChunkStorage) -> Result<()> {
        let requested = self.modeled_retained_bytes();
        storage.resize_remote_catalog_memory_reservation(&mut self.memory_reservation, requested)
    }

    fn modeled_entry_retained_bytes(entry: &SegmentInventoryEntry, shared: bool) -> usize {
        std::mem::size_of::<SegmentInventoryEntry>()
            .saturating_add(std::mem::size_of::<BoundedTieredCatalogKey>())
            .saturating_add(
                entry
                    .root
                    .as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .saturating_add(BOUNDED_TIERED_CATALOG_ENTRY_ALLOWANCE_BYTES),
            )
            .saturating_add(if shared {
                std::mem::size_of::<BoundedTieredCatalogKey>()
                    .saturating_add(BOUNDED_TIERED_CATALOG_ENTRY_ALLOWANCE_BYTES)
            } else {
                0
            })
    }

    fn generation_pointer(&self) -> Result<SegmentCatalogPointer> {
        tiering::finalized_segment_catalog_pointer(
            self.generation.ok_or_else(|| {
                TsinkError::Other(
                    "bounded tiered catalog generation was not initialized".to_string(),
                )
            })?,
            self.shared_keys.len(),
            self.generation_file_len,
            self.generation_hash.digest(),
        )
    }
}

struct BoundedTieredCatalogPassBudget {
    item_limit: usize,
    byte_limit: u64,
    remaining_items: usize,
    remaining_bytes: u64,
}

impl BoundedTieredCatalogPassBudget {
    fn new(item_limit: usize, byte_limit: u64) -> Self {
        Self {
            item_limit,
            byte_limit,
            remaining_items: item_limit,
            remaining_bytes: byte_limit,
        }
    }

    fn charge(&mut self, items: usize, bytes: u64) -> Result<bool> {
        if items > self.item_limit {
            return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                operation: BOUNDED_TIERED_CATALOG_PUBLICATION_OPERATION,
                item_limit: self.item_limit,
                byte_limit: self.byte_limit,
                selected_items: self.item_limit.saturating_sub(self.remaining_items),
                selected_bytes: self.byte_limit.saturating_sub(self.remaining_bytes),
            });
        }
        if bytes > self.byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: BOUNDED_TIERED_CATALOG_PUBLICATION_OPERATION,
                limit: self.byte_limit,
                required: bytes,
            });
        }
        if items > self.remaining_items || bytes > self.remaining_bytes {
            return Ok(false);
        }
        self.remaining_items -= items;
        self.remaining_bytes -= bytes;
        Ok(true)
    }

    fn used_any(&self) -> bool {
        self.remaining_items != self.item_limit || self.remaining_bytes != self.byte_limit
    }

    fn exhausted(&self) -> bool {
        self.remaining_items == 0 || self.remaining_bytes == 0
    }
}

fn shared_catalog_contains_entry(
    config: &super::super::config::TieredStorageConfig,
    entry: &SegmentInventoryEntry,
) -> bool {
    let shared_lane_root = config.lane_path(entry.lane, entry.tier);
    entry.root.starts_with(shared_lane_root)
        || (entry.tier == PersistedSegmentTier::Hot && config.mirror_hot_segments)
}

fn append_catalog_stage_bytes(
    path: &Path,
    expected_len: u64,
    bytes: &[u8],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
) -> Result<u64> {
    append_catalog_stage_bytes_impl(path, expected_len, bytes, local_disk_budget, category, None)
}

fn append_catalog_stage_bytes_impl(
    path: &Path,
    expected_len: u64,
    bytes: &[u8],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    category: crate::DiskCategory,
    injected_failure_after: Option<usize>,
) -> Result<u64> {
    let added = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let governed_budget = match local_disk_budget {
        Some(budget) if budget.governs_entry(path)? => Some(budget),
        _ => None,
    };
    let reservation = governed_budget
        .map(|budget| budget.reserve(category, added, crate::DiskReservationKind::Maintenance))
        .transpose()?;

    let parent = path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "catalog staging path has no parent directory: {}",
            path.display()
        ))
    })?;
    crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;

    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    if expected_len == 0 {
        options.create_new(true);
    } else {
        let metadata =
            std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
            || metadata.len() != expected_len
        {
            return Err(TsinkError::DataCorruption(format!(
                "catalog staging file changed before append: expected {expected_len} bytes at {}, found {}",
                path.display(),
                metadata.len()
            )));
        }
        options.append(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut file = options
        .open(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
    let write_result = (|| -> Result<u64> {
        let opened = file.metadata().map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
        if opened.len() != expected_len {
            return Err(TsinkError::DataCorruption(format!(
                "opened catalog staging file changed before append: expected {expected_len} bytes at {}, found {}",
                path.display(),
                opened.len()
            )));
        }
        if let Some(failure_after) = injected_failure_after {
            let prefix_len = failure_after.min(bytes.len());
            file.write_all(&bytes[..prefix_len])
                .map_err(|source| TsinkError::IoWithPath {
                    path: path.to_path_buf(),
                    source,
                })?;
            file.sync_all().map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?;
            return Err(TsinkError::Other(
                "injected partial catalog staging append failure".to_string(),
            ));
        }
        file.write_all(bytes)
            .map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?;
        file.sync_all().map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
        let expected_after = expected_len.saturating_add(added);
        let actual_after = file
            .metadata()
            .map_err(|source| TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })?
            .len();
        if actual_after != expected_after {
            return Err(TsinkError::DataCorruption(format!(
                "catalog staging append produced {actual_after} bytes, expected {expected_after}: {}",
                path.display()
            )));
        }
        if expected_len == 0 {
            crate::engine::fs_utils::sync_parent_dir(path)?;
        }
        Ok(expected_after)
    })();
    drop(file);

    match (write_result, reservation) {
        (Ok(next_len), Some(reservation)) => {
            reservation.commit(added, 0)?;
            Ok(next_len)
        }
        (Ok(next_len), None) => Ok(next_len),
        (Err(err), Some(reservation)) => {
            // `write_all` may have extended the file before returning an error. Settle only the
            // observed regular-file growth; charging the full requested fragment would leave a
            // phantom disk-budget allocation after exact artifact cleanup.
            let observed_growth = std::fs::symlink_metadata(path)
                .ok()
                .filter(|metadata| {
                    !crate::engine::fs_utils::is_link_or_reparse_point(metadata)
                        && metadata.file_type().is_file()
                })
                .map(|metadata| metadata.len().saturating_sub(expected_len).min(added))
                .unwrap_or(0);
            let settlement = reservation.commit_as(category, observed_growth, 0);
            match settlement {
                Ok(()) => Err(err),
                Err(settlement_err) => Err(TsinkError::Other(format!(
                    "catalog staging append failed: {err}; disk settlement failed: {settlement_err}"
                ))),
            }
        }
        (Err(err), None) => Err(err),
    }
}

fn publish_catalog_stage(
    stage: &Path,
    target: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    crate::engine::fs_utils::rename_and_sync_parents_budgeted_reclassify(
        stage,
        target,
        local_disk_budget,
        crate::DiskCategory::Temporary,
        crate::DiskCategory::Registry,
    )
}

struct SegmentInventoryDeltaGuard<'a> {
    storage: &'a ChunkStorage,
    roots: BTreeSet<PathBuf>,
    before: Vec<SegmentInventoryEntry>,
}

impl<'a> SegmentInventoryDeltaGuard<'a> {
    fn new(storage: &'a ChunkStorage, roots: BTreeSet<PathBuf>) -> Self {
        let before = storage.persisted_inventory_entries_for_roots(&roots);
        Self {
            storage,
            roots,
            before,
        }
    }
}

impl Drop for SegmentInventoryDeltaGuard<'_> {
    fn drop(&mut self) {
        let after = self
            .storage
            .persisted_inventory_entries_for_roots(&self.roots);
        self.storage
            .publish_segment_inventory_delta(&self.before, &after);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedTieredCatalogStep {
    Progressed,
    Deferred,
    Complete,
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn remove_persisted_segment_roots_with_observability_delta(
        &self,
        roots: &[PathBuf],
    ) -> Result<bool> {
        let snapshot = || {
            let persisted_index = self.persisted.persisted_index.read();
            roots
                .iter()
                .filter_map(|root| {
                    persisted_index
                        .segments_by_root
                        .get(root)
                        .map(|state| SegmentInventoryEntry {
                            lane: state.lane,
                            tier: state.tier,
                            root: root.clone(),
                            manifest: state.manifest.clone(),
                        })
                })
                .collect::<Vec<_>>()
        };
        let before = snapshot();
        let result = self.remove_persisted_segment_roots(roots);
        let after = snapshot();
        self.publish_segment_inventory_delta(&before, &after);
        result
    }

    pub(in crate::engine::storage_engine) fn finite_tiered_catalog_publication_enabled(
        &self,
    ) -> bool {
        self.persisted.tiered_storage.is_some()
            && self.runtime.runtime_mode == StorageRuntimeMode::ReadWrite
            && (self.runtime.maintenance_max_items_per_pass != usize::MAX
                || self.runtime.maintenance_max_bytes_per_pass != u64::MAX)
    }

    fn catalog_reconciliation_memory_limit(&self) -> usize {
        usize::try_from(self.runtime.maintenance_max_bytes_per_pass).unwrap_or(usize::MAX)
    }

    fn remove_bounded_catalog_artifact(
        &self,
        path: &Path,
        category: crate::DiskCategory,
    ) -> Result<()> {
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted_with_reconciliation_memory_limit(
            path,
            self.persisted.local_disk_budget.as_ref(),
            category,
            self.catalog_reconciliation_memory_limit(),
        )
    }

    fn cleanup_exact_bounded_tiered_catalog_stages(
        &self,
        config: &super::super::config::TieredStorageConfig,
    ) -> Result<()> {
        let shared_target = tiering::shared_segment_catalog_path(config);
        let shared_stage = BoundedTieredCatalogPublicationCycle::stage_path(&shared_target)?;
        let local_stage = config
            .segment_catalog_path
            .as_ref()
            .filter(|target| *target != &shared_target)
            .map(|target| BoundedTieredCatalogPublicationCycle::stage_path(target))
            .transpose()?;
        for path in local_stage.iter().chain([&shared_stage]) {
            if crate::engine::fs_utils::path_exists_no_follow(path)? {
                self.remove_bounded_catalog_artifact(path, crate::DiskCategory::Temporary)?;
            }
        }
        Ok(())
    }

    fn cleanup_bounded_tiered_catalog_cycle(
        &self,
        cycle: &BoundedTieredCatalogPublicationCycle,
    ) -> Result<()> {
        let mut errors = Vec::new();
        for path in cycle.local_stage.iter().chain([&cycle.shared_stage]) {
            match crate::engine::fs_utils::path_exists_no_follow(path) {
                Ok(true) => {
                    if let Err(err) =
                        self.remove_bounded_catalog_artifact(path, crate::DiskCategory::Temporary)
                    {
                        errors.push(format!("{}: {err}", path.display()));
                    }
                }
                Ok(false) => {}
                Err(err) => errors.push(format!("{}: {err}", path.display())),
            }
        }
        if !cycle.pointer_may_be_visible {
            if let Some(path) = cycle.generation_path.as_deref() {
                match crate::engine::fs_utils::path_exists_no_follow(path) {
                    Ok(true) => {
                        if let Err(err) = self
                            .remove_bounded_catalog_artifact(path, crate::DiskCategory::Registry)
                        {
                            errors.push(format!("{}: {err}", path.display()));
                        }
                    }
                    Ok(false) => {}
                    Err(err) => errors.push(format!("{}: {err}", path.display())),
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(TsinkError::Other(format!(
                "bounded tiered catalog staging cleanup failed: {}",
                errors.join("; ")
            )))
        }
    }

    pub(super) fn reset_bounded_tiered_catalog_publication(&self) {
        let cycle = {
            let mut cursor = self.coordination.background_catalog_refresh_cursor.lock();
            cursor.writer_publication_completed_visibility_generation = None;
            cursor.writer_publication_cycle.take()
        };
        if let Some(cycle) = cycle {
            if cycle.reconcile_registry {
                self.coordination
                    .bounded_registry_reconciliation_required
                    .store(true, Ordering::Release);
                self.persisted
                    .persisted_index_dirty
                    .store(true, Ordering::SeqCst);
            }
            if let Err(err) = self.cleanup_bounded_tiered_catalog_cycle(&cycle) {
                tracing::warn!(
                    error = %err,
                    "failed to clean an unpublished bounded tiered catalog generation"
                );
            }
        }
    }

    pub(in crate::engine::storage_engine) fn bounded_tiered_catalog_publication_is_pending(
        &self,
    ) -> bool {
        self.coordination
            .background_catalog_refresh_cursor
            .lock()
            .writer_publication_cycle
            .is_some()
    }

    pub(in crate::engine::storage_engine) fn bounded_tiered_catalog_publication_matches_current_visibility(
        &self,
    ) -> bool {
        let current = self.visibility_state_generation();
        let cursor = self.coordination.background_catalog_refresh_cursor.lock();
        cursor.writer_publication_cycle.is_none()
            && cursor.writer_publication_completed_visibility_generation == Some(current)
    }

    fn bounded_catalog_fragment_work_bytes(fragment_len: usize) -> u64 {
        u64::try_from(BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES.saturating_add(fragment_len))
            .unwrap_or(u64::MAX)
    }

    fn scan_bounded_tiered_catalog_entry(
        &self,
        config: &super::super::config::TieredStorageConfig,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let next = {
            let index = self.persisted.persisted_index.read();
            let next = match cycle.scan_after_root.as_ref() {
                Some(after) => index
                    .segments_by_root
                    .range((Excluded(after.clone()), Unbounded))
                    .next(),
                None => index.segments_by_root.iter().next(),
            };
            next.map(|(root, state)| {
                #[cfg(test)]
                {
                    let hook = self
                        .persist_test_hooks
                        .persisted_catalog_inventory_entry_hook
                        .read()
                        .clone();
                    if let Some(hook) = hook {
                        hook();
                    }
                }
                SegmentInventoryEntry {
                    lane: state.lane,
                    tier: state.tier,
                    root: root.clone(),
                    manifest: state.manifest.clone(),
                }
            })
        };
        let Some(entry) = next else {
            cycle.phase = if cycle.local_target.is_some() {
                BoundedTieredCatalogPublicationPhase::LocalLegacyHeader
            } else {
                BoundedTieredCatalogPublicationPhase::GenerationHeader
            };
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        if cycle.entries.len() == tiering::SEGMENT_CATALOG_MAX_ENTRIES {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: BOUNDED_TIERED_CATALOG_PUBLICATION_OPERATION,
                limit: tiering::SEGMENT_CATALOG_MAX_ENTRIES,
                required: cycle.entries.len().saturating_add(1),
            });
        }
        let shared = shared_catalog_contains_entry(config, &entry);
        let retained =
            BoundedTieredCatalogPublicationCycle::modeled_entry_retained_bytes(&entry, shared);
        if !budget.charge(1, u64::try_from(retained).unwrap_or(u64::MAX))? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            retained.saturating_add(BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES),
        )?;
        let key = BoundedTieredCatalogKey::from_entry(&entry);
        let root = entry.root.clone();
        if cycle.entries.insert(key, entry).is_some() {
            return Err(TsinkError::DataCorruption(format!(
                "tiered catalog publication contains duplicate segment identity {:?}",
                key
            )));
        }
        if shared {
            cycle.shared_keys.insert(key);
        }
        cycle.retained_entry_bytes = cycle.retained_entry_bytes.saturating_add(retained);
        cycle.scan_after_root = Some(root);
        cycle.restore_retained_reservation(self)?;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn prepare_bounded_tiered_catalog_generation(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        // Fixed owned-path probes are metadata bytes, not catalog data items. Admit them at the
        // beginning of a fresh pass, then enumerate the generation namespace through one charged
        // item per later cursor step.
        if budget.used_any() {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        let fixed_stage_probes = cycle.local_stage.is_some() as usize + 1;
        let required_bytes = u64::try_from(
            fixed_stage_probes
                .saturating_add(2)
                .saturating_mul(SEGMENT_CATALOG_SHARED_PATH_ALLOWANCE_BYTES),
        )
        .unwrap_or(u64::MAX);
        if !budget.charge(0, required_bytes)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(self, required_bytes.min(usize::MAX as u64) as usize)?;
        tiering::ensure_segment_catalog_generation_directory(&cycle.generation_directory)?;
        self.cleanup_exact_bounded_tiered_catalog_stages(
            self.persisted
                .tiered_storage
                .as_ref()
                .expect("bounded writer publication requires tiered storage"),
        )?;
        cycle.prior_pointer = tiering::load_shared_segment_catalog_pointer(
            self.persisted
                .tiered_storage
                .as_ref()
                .expect("bounded writer publication requires tiered storage"),
        )?;
        cycle.generation_namespace_reader = Some(
            std::fs::read_dir(&cycle.generation_directory).map_err(|source| {
                TsinkError::IoWithPath {
                    path: cycle.generation_directory.clone(),
                    source,
                }
            })?,
        );
        cycle.restore_retained_reservation(self)?;
        cycle.phase = BoundedTieredCatalogPublicationPhase::ScanGenerationNamespace;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn scan_next_bounded_generation_namespace_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if !budget.charge(1, BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        let next = cycle
            .generation_namespace_reader
            .as_mut()
            .expect("bounded generation namespace reader initialized")
            .next();
        let Some(directory_entry) = next else {
            cycle.generation_namespace_reader = None;
            cycle.restore_retained_reservation(self)?;
            if cycle.prior_pointer.is_some() && !cycle.generation_current_observed {
                return Err(TsinkError::DataCorruption(
                    "current segment catalog pointer generation is missing from its namespace"
                        .to_string(),
                ));
            }
            cycle.generation_cleanup_expected_entries = cycle.generation_namespace_count;
            cycle.phase = BoundedTieredCatalogPublicationPhase::OpenGenerationCleanup;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        let directory_entry = directory_entry.map_err(|source| TsinkError::IoWithPath {
            path: cycle.generation_directory.clone(),
            source,
        })?;
        cycle.generation_namespace_count = cycle.generation_namespace_count.saturating_add(1);
        if cycle.generation_namespace_count
            > crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES
        {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "segment catalog generation namespace",
                limit: crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
                required: cycle.generation_namespace_count,
            });
        }
        if let Some(generation) = directory_entry
            .file_name()
            .to_str()
            .and_then(tiering::parse_segment_catalog_generation_file_name)
        {
            cycle.generation_max_observed = Some(
                cycle
                    .generation_max_observed
                    .map_or(generation, |current| current.max(generation)),
            );
            let file_type =
                directory_entry
                    .file_type()
                    .map_err(|source| TsinkError::IoWithPath {
                        path: directory_entry.path(),
                        source,
                    })?;
            if !file_type.is_symlink() && file_type.is_file() {
                if cycle
                    .prior_pointer
                    .is_some_and(|pointer| pointer.generation == generation)
                {
                    cycle.generation_current_observed = true;
                } else if cycle
                    .prior_pointer
                    .is_some_and(|pointer| generation < pointer.generation)
                {
                    cycle.generation_predecessor = Some(
                        cycle
                            .generation_predecessor
                            .map_or(generation, |prior| prior.max(generation)),
                    );
                }
            }
        }
        cycle.restore_retained_reservation(self)?;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn open_bounded_generation_namespace_cleanup(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let work = u64::try_from(SEGMENT_CATALOG_SHARED_PATH_ALLOWANCE_BYTES).unwrap_or(u64::MAX);
        if !budget.charge(0, work)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        cycle.generation_namespace_reader = Some(
            std::fs::read_dir(&cycle.generation_directory).map_err(|source| {
                TsinkError::IoWithPath {
                    path: cycle.generation_directory.clone(),
                    source,
                }
            })?,
        );
        cycle.restore_retained_reservation(self)?;
        cycle.phase = BoundedTieredCatalogPublicationPhase::CleanupGenerationNamespace;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn cleanup_next_bounded_generation_namespace_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if !budget.charge(1, BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        let next = cycle
            .generation_namespace_reader
            .as_mut()
            .expect("bounded generation cleanup reader initialized")
            .next();
        let Some(directory_entry) = next else {
            cycle.generation_namespace_reader = None;
            cycle.restore_retained_reservation(self)?;
            if cycle.generation_cleanup_entries_seen != cycle.generation_cleanup_expected_entries {
                return Err(TsinkError::DataCorruption(
                    "segment catalog generation namespace changed during bounded cleanup"
                        .to_string(),
                ));
            }
            cycle.phase = BoundedTieredCatalogPublicationPhase::FinalizeGenerationPreparation;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        let directory_entry = directory_entry.map_err(|source| TsinkError::IoWithPath {
            path: cycle.generation_directory.clone(),
            source,
        })?;
        cycle.generation_cleanup_entries_seen =
            cycle.generation_cleanup_entries_seen.saturating_add(1);
        if cycle.generation_cleanup_entries_seen
            > crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES
        {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "segment catalog generation namespace cleanup",
                limit: crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
                required: cycle.generation_cleanup_entries_seen,
            });
        }
        let generation = directory_entry
            .file_name()
            .to_str()
            .and_then(tiering::parse_segment_catalog_generation_file_name);
        if let Some(generation) = generation {
            let protected = cycle
                .prior_pointer
                .is_some_and(|pointer| pointer.generation == generation)
                || cycle.generation_predecessor == Some(generation);
            if !protected {
                let file_type =
                    directory_entry
                        .file_type()
                        .map_err(|source| TsinkError::IoWithPath {
                            path: directory_entry.path(),
                            source,
                        })?;
                if !file_type.is_symlink() && file_type.is_file() {
                    self.remove_bounded_catalog_artifact(
                        &directory_entry.path(),
                        crate::DiskCategory::Registry,
                    )?;
                    cycle.generation_namespace_count =
                        cycle.generation_namespace_count.saturating_sub(1);
                }
            }
        }
        cycle.restore_retained_reservation(self)?;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn finalize_bounded_tiered_catalog_generation(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let work = u64::try_from(SEGMENT_CATALOG_SHARED_PATH_ALLOWANCE_BYTES).unwrap_or(u64::MAX);
        if !budget.charge(1, work)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        let generation = tiering::choose_next_segment_catalog_generation_after_observed(
            &cycle.generation_directory,
            cycle.prior_pointer,
            cycle.generation_namespace_count,
            cycle.generation_max_observed,
        )?;
        let generation_path = cycle
            .generation_directory
            .join(format!("catalog-{generation:016x}.bin"));
        let added_path_bytes = generation_path
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .saturating_add(SEGMENT_CATALOG_SHARED_PATH_ALLOCATOR_BYTES);
        cycle.resize_for_scratch(self, added_path_bytes)?;
        cycle.generation = Some(generation);
        cycle.generation_path = Some(generation_path);
        cycle.restore_retained_reservation(self)?;
        cycle.phase = BoundedTieredCatalogPublicationPhase::Scanning;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn append_bounded_catalog_fragment(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
        path: &Path,
        expected_len: u64,
        fragment: &[u8],
        category: crate::DiskCategory,
    ) -> Result<Option<u64>> {
        let work_bytes = Self::bounded_catalog_fragment_work_bytes(fragment.len());
        if !budget.charge(1, work_bytes)? {
            return Ok(None);
        }
        cycle.resize_for_scratch(
            self,
            fragment
                .len()
                .saturating_add(BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES),
        )?;
        let result = append_catalog_stage_bytes(
            path,
            expected_len,
            fragment,
            self.persisted.local_disk_budget.as_ref(),
            category,
        );
        cycle.restore_retained_reservation(self)?;
        result.map(Some)
    }

    fn append_next_local_legacy_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let next = match cycle.local_after_key {
            Some(after) => cycle
                .entries
                .range((Excluded(after), Unbounded))
                .next()
                .map(|(key, entry)| (*key, entry.clone())),
            None => cycle
                .entries
                .iter()
                .next()
                .map(|(key, entry)| (*key, entry.clone())),
        };
        let Some((key, entry)) = next else {
            cycle.phase = BoundedTieredCatalogPublicationPhase::LocalLegacyFooter;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        cycle.resize_for_scratch(
            self,
            BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES.saturating_add(2 * 1024),
        )?;
        let fragment = tiering::encode_legacy_segment_catalog_entry_fragment(
            &entry,
            cycle.local_entries_written == 0,
        )?;
        cycle.restore_retained_reservation(self)?;
        let stage = cycle
            .local_stage
            .as_ref()
            .expect("local stage exists in local legacy phase")
            .clone();
        let Some(next_len) = self.append_bounded_catalog_fragment(
            cycle,
            budget,
            &stage,
            cycle.local_file_len,
            &fragment,
            crate::DiskCategory::Temporary,
        )?
        else {
            return Ok(BoundedTieredCatalogStep::Deferred);
        };
        cycle.local_file_len = next_len;
        cycle.local_after_key = Some(key);
        cycle.local_entries_written = cycle.local_entries_written.saturating_add(1);
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn append_next_generation_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let next_key = match cycle.generation_after_key {
            Some(after) => cycle
                .shared_keys
                .range((Excluded(after), Unbounded))
                .next()
                .copied(),
            None => cycle.shared_keys.iter().next().copied(),
        };
        let Some(key) = next_key else {
            if cycle.generation_entries_written != cycle.shared_keys.len() {
                return Err(TsinkError::DataCorruption(format!(
                    "bounded tiered catalog generation wrote {} of {} entries",
                    cycle.generation_entries_written,
                    cycle.shared_keys.len()
                )));
            }
            cycle.phase = BoundedTieredCatalogPublicationPhase::SharedLegacyHeader;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        let entry = cycle
            .entries
            .get(&key)
            .expect("shared catalog key must reference a retained entry")
            .clone();
        cycle.resize_for_scratch(
            self,
            BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES
                .saturating_add(tiering::SEGMENT_CATALOG_MAX_FRAME_BYTES),
        )?;
        let (_, frame) = tiering::encode_segment_catalog_generation_frame(&entry)?;
        cycle.restore_retained_reservation(self)?;
        let generation_path = cycle
            .generation_path
            .as_ref()
            .expect("generation path exists in generation phase")
            .clone();
        let Some(next_len) = self.append_bounded_catalog_fragment(
            cycle,
            budget,
            &generation_path,
            cycle.generation_file_len,
            &frame,
            crate::DiskCategory::Registry,
        )?
        else {
            return Ok(BoundedTieredCatalogStep::Deferred);
        };
        cycle.generation_hash.update(&frame);
        cycle.generation_file_len = next_len;
        cycle.generation_after_key = Some(key);
        cycle.generation_entries_written = cycle.generation_entries_written.saturating_add(1);
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn append_next_shared_legacy_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let next_key = match cycle.shared_after_key {
            Some(after) => cycle
                .shared_keys
                .range((Excluded(after), Unbounded))
                .next()
                .copied(),
            None => cycle.shared_keys.iter().next().copied(),
        };
        let Some(key) = next_key else {
            cycle.phase = BoundedTieredCatalogPublicationPhase::SharedLegacyFooter;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        let entry = cycle
            .entries
            .get(&key)
            .expect("shared catalog key must reference a retained entry")
            .clone();
        cycle.resize_for_scratch(
            self,
            BOUNDED_TIERED_CATALOG_PAGE_SCRATCH_BYTES.saturating_add(2 * 1024),
        )?;
        let fragment = tiering::encode_legacy_segment_catalog_entry_fragment(
            &entry,
            cycle.shared_entries_written == 0,
        )?;
        cycle.restore_retained_reservation(self)?;
        let stage = cycle.shared_stage.clone();
        let Some(next_len) = self.append_bounded_catalog_fragment(
            cycle,
            budget,
            &stage,
            cycle.shared_file_len,
            &fragment,
            crate::DiskCategory::Temporary,
        )?
        else {
            return Ok(BoundedTieredCatalogStep::Deferred);
        };
        cycle.shared_file_len = next_len;
        cycle.shared_after_key = Some(key);
        cycle.shared_entries_written = cycle.shared_entries_written.saturating_add(1);
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn prepare_bounded_registry_catalog_reconciliation(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if cycle.registry_snapshot_path.is_none() {
            cycle.phase = BoundedTieredCatalogPublicationPhase::Pointer;
            return Ok(BoundedTieredCatalogStep::Progressed);
        }
        if !budget.charge(1, BOUNDED_REGISTRY_MANIFEST_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_REGISTRY_MANIFEST_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        {
            let snapshot_path = cycle
                .registry_snapshot_path
                .as_deref()
                .expect("bounded registry preparation requires a registry snapshot");
            let _registry_persistence_guard = self.catalog.persistence_lock.lock();
            registry_catalog::begin_bounded_registry_catalog_reconciliation(
                snapshot_path,
                cycle.entries.len(),
                self.persisted.local_disk_budget.as_ref(),
            )?;
        }
        cycle.restore_retained_reservation(self)?;
        // Remove stale identities before adding missing ones so a complete replacement cannot
        // transiently exceed the same hard namespace ceiling that startup must enumerate.
        cycle.phase = BoundedTieredCatalogPublicationPhase::RegistrySweepOpen;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn persist_next_bounded_registry_catalog_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        let next_key = match cycle.registry_after_key {
            Some(after) => cycle
                .entries
                .range((Excluded(after), Unbounded))
                .next()
                .map(|(key, _)| *key),
            None => cycle.entries.keys().next().copied(),
        };
        let Some(key) = next_key else {
            cycle.phase = BoundedTieredCatalogPublicationPhase::RegistryComplete;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        if !budget.charge(1, BOUNDED_REGISTRY_ENTRY_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_REGISTRY_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        {
            let entry = cycle
                .entries
                .get(&key)
                .expect("bounded registry key must reference a retained inventory entry");
            let snapshot_path = cycle
                .registry_snapshot_path
                .as_deref()
                .expect("bounded registry entry phase requires a registry snapshot");
            let _registry_persistence_guard = self.catalog.persistence_lock.lock();
            registry_catalog::persist_bounded_registry_catalog_entry(
                snapshot_path,
                entry.lane,
                &entry.root,
                self.persisted.local_disk_budget.as_ref(),
            )?;
        }
        cycle.restore_retained_reservation(self)?;
        cycle.registry_after_key = Some(key);
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn open_bounded_registry_catalog_sweep(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if !budget.charge(1, BOUNDED_REGISTRY_SWEEP_ENTRY_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_REGISTRY_SWEEP_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        let store_path = cycle
            .registry_store_path
            .as_ref()
            .expect("bounded registry sweep requires a registry store");
        cycle.registry_reader =
            Some(
                std::fs::read_dir(store_path).map_err(|source| TsinkError::IoWithPath {
                    path: store_path.clone(),
                    source,
                })?,
            );
        cycle.restore_retained_reservation(self)?;
        cycle.phase = BoundedTieredCatalogPublicationPhase::RegistrySweepEntries;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn sweep_next_bounded_registry_catalog_entry(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        // Charge the terminal directory probe as an item too. This keeps the observed namespace
        // work at or below the configured per-pass item count even when the store is empty.
        if !budget.charge(1, BOUNDED_REGISTRY_SWEEP_ENTRY_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_REGISTRY_SWEEP_ENTRY_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        let next = cycle
            .registry_reader
            .as_mut()
            .expect("bounded registry sweep reader initialized")
            .next();
        let Some(directory_entry) = next else {
            cycle.registry_reader = None;
            cycle.restore_retained_reservation(self)?;
            cycle.phase = BoundedTieredCatalogPublicationPhase::RegistryEntries;
            return Ok(BoundedTieredCatalogStep::Progressed);
        };
        let directory_entry = directory_entry?;
        cycle.registry_sweep_entries_seen = cycle.registry_sweep_entries_seen.saturating_add(1);
        if cycle.registry_sweep_entries_seen
            > crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES
        {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "bounded persisted registry catalog reconciliation",
                limit: crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
                required: cycle.registry_sweep_entries_seen,
            });
        }
        let _registry_persistence_guard = self.catalog.persistence_lock.lock();
        registry_catalog::reconcile_bounded_registry_catalog_store_entry(
            directory_entry,
            |key| {
                cycle.entries.contains_key(&BoundedTieredCatalogKey {
                    lane: key.lane,
                    level: key.level,
                    segment_id: key.segment_id,
                })
            },
            self.persisted.local_disk_budget.as_ref(),
        )?;
        cycle.restore_retained_reservation(self)?;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn complete_bounded_registry_catalog_reconciliation(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if !budget.charge(1, BOUNDED_REGISTRY_MANIFEST_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_REGISTRY_MANIFEST_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        let snapshot_path = cycle
            .registry_snapshot_path
            .as_deref()
            .expect("bounded registry completion requires a registry snapshot");
        let _registry_persistence_guard = self.catalog.persistence_lock.lock();
        registry_catalog::complete_bounded_registry_catalog_reconciliation(
            snapshot_path,
            cycle.entries.len(),
            self.persisted.local_disk_budget.as_ref(),
        )?;
        cycle.restore_retained_reservation(self)?;
        cycle.phase = BoundedTieredCatalogPublicationPhase::RegistryLegacyRetire;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn retire_legacy_registry_catalog_after_bounded_reconciliation(
        &self,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if !budget.charge(1, BOUNDED_REGISTRY_MANIFEST_WORK_BYTES)? {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        cycle.resize_for_scratch(
            self,
            BOUNDED_REGISTRY_MANIFEST_WORK_BYTES.min(usize::MAX as u64) as usize,
        )?;
        let snapshot_path = cycle
            .registry_snapshot_path
            .as_deref()
            .expect("bounded registry legacy retirement requires a registry snapshot");
        let _registry_persistence_guard = self.catalog.persistence_lock.lock();
        registry_catalog::retire_legacy_registry_catalog_after_bounded_reconciliation(
            snapshot_path,
            self.persisted.local_disk_budget.as_ref(),
        )?;
        cycle.restore_retained_reservation(self)?;
        cycle.phase = BoundedTieredCatalogPublicationPhase::Pointer;
        Ok(BoundedTieredCatalogStep::Progressed)
    }

    fn advance_bounded_tiered_catalog_step(
        &self,
        config: &super::super::config::TieredStorageConfig,
        cycle: &mut BoundedTieredCatalogPublicationCycle,
        budget: &mut BoundedTieredCatalogPassBudget,
    ) -> Result<BoundedTieredCatalogStep> {
        if self.visibility_state_generation() != cycle.expected_visibility_generation {
            return Ok(BoundedTieredCatalogStep::Deferred);
        }
        match cycle.phase {
            BoundedTieredCatalogPublicationPhase::PrepareGeneration => {
                self.prepare_bounded_tiered_catalog_generation(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::ScanGenerationNamespace => {
                self.scan_next_bounded_generation_namespace_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::OpenGenerationCleanup => {
                self.open_bounded_generation_namespace_cleanup(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::CleanupGenerationNamespace => {
                self.cleanup_next_bounded_generation_namespace_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::FinalizeGenerationPreparation => {
                self.finalize_bounded_tiered_catalog_generation(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::Scanning => {
                self.scan_bounded_tiered_catalog_entry(config, cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::LocalLegacyHeader => {
                let Some(stage) = cycle.local_stage.clone() else {
                    cycle.phase = BoundedTieredCatalogPublicationPhase::GenerationHeader;
                    return Ok(BoundedTieredCatalogStep::Progressed);
                };
                let Some(next_len) = self.append_bounded_catalog_fragment(
                    cycle,
                    budget,
                    &stage,
                    0,
                    tiering::SEGMENT_CATALOG_LEGACY_STREAM_PREFIX,
                    crate::DiskCategory::Temporary,
                )?
                else {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                };
                cycle.local_file_len = next_len;
                cycle.phase = BoundedTieredCatalogPublicationPhase::LocalLegacyEntries;
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::LocalLegacyEntries => {
                self.append_next_local_legacy_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::LocalLegacyFooter => {
                let stage = cycle
                    .local_stage
                    .as_ref()
                    .expect("local stage exists in local footer phase")
                    .clone();
                let Some(next_len) = self.append_bounded_catalog_fragment(
                    cycle,
                    budget,
                    &stage,
                    cycle.local_file_len,
                    tiering::SEGMENT_CATALOG_LEGACY_STREAM_SUFFIX,
                    crate::DiskCategory::Temporary,
                )?
                else {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                };
                cycle.local_file_len = next_len;
                cycle.phase = BoundedTieredCatalogPublicationPhase::LocalLegacyPublish;
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::LocalLegacyPublish => {
                let work = Self::bounded_catalog_fragment_work_bytes(0);
                if !budget.charge(1, work)? {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                }
                let stage = cycle
                    .local_stage
                    .as_ref()
                    .expect("local stage exists in local publish phase");
                let target = cycle
                    .local_target
                    .as_ref()
                    .expect("local target exists in local publish phase");
                publish_catalog_stage(stage, target, self.persisted.local_disk_budget.as_ref())?;
                cycle.phase = BoundedTieredCatalogPublicationPhase::GenerationHeader;
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::GenerationHeader => {
                let generation = cycle
                    .generation
                    .expect("generation exists in generation header phase");
                let header = tiering::encode_segment_catalog_generation_header(
                    generation,
                    u64::try_from(cycle.shared_keys.len()).unwrap_or(u64::MAX),
                )?;
                let generation_path = cycle
                    .generation_path
                    .as_ref()
                    .expect("generation path exists in generation header phase")
                    .clone();
                let Some(next_len) = self.append_bounded_catalog_fragment(
                    cycle,
                    budget,
                    &generation_path,
                    0,
                    &header,
                    crate::DiskCategory::Registry,
                )?
                else {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                };
                cycle.generation_hash.update(&header);
                cycle.generation_file_len = next_len;
                cycle.phase = BoundedTieredCatalogPublicationPhase::GenerationEntries;
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::GenerationEntries => {
                self.append_next_generation_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::SharedLegacyHeader => {
                let stage = cycle.shared_stage.clone();
                let Some(next_len) = self.append_bounded_catalog_fragment(
                    cycle,
                    budget,
                    &stage,
                    0,
                    tiering::SEGMENT_CATALOG_LEGACY_STREAM_PREFIX,
                    crate::DiskCategory::Temporary,
                )?
                else {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                };
                cycle.shared_file_len = next_len;
                cycle.phase = BoundedTieredCatalogPublicationPhase::SharedLegacyEntries;
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::SharedLegacyEntries => {
                self.append_next_shared_legacy_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::SharedLegacyFooter => {
                let stage = cycle.shared_stage.clone();
                let Some(next_len) = self.append_bounded_catalog_fragment(
                    cycle,
                    budget,
                    &stage,
                    cycle.shared_file_len,
                    tiering::SEGMENT_CATALOG_LEGACY_STREAM_SUFFIX,
                    crate::DiskCategory::Temporary,
                )?
                else {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                };
                cycle.shared_file_len = next_len;
                cycle.phase = BoundedTieredCatalogPublicationPhase::SharedLegacyPublish;
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::SharedLegacyPublish => {
                let work = Self::bounded_catalog_fragment_work_bytes(0);
                if !budget.charge(1, work)? {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                }
                publish_catalog_stage(
                    &cycle.shared_stage,
                    &cycle.shared_target,
                    self.persisted.local_disk_budget.as_ref(),
                )?;
                cycle.phase = if cycle.reconcile_registry {
                    BoundedTieredCatalogPublicationPhase::RegistryPrepare
                } else {
                    BoundedTieredCatalogPublicationPhase::Pointer
                };
                Ok(BoundedTieredCatalogStep::Progressed)
            }
            BoundedTieredCatalogPublicationPhase::Pointer => {
                let pointer = cycle.generation_pointer()?;
                let pointer_bytes = tiering::encode_segment_catalog_pointer(pointer)?;
                let work = Self::bounded_catalog_fragment_work_bytes(pointer_bytes.len());
                if !budget.charge(1, work)? {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                }
                if self.visibility_state_generation() != cycle.expected_visibility_generation {
                    return Ok(BoundedTieredCatalogStep::Deferred);
                }
                let publication =
                    crate::engine::fs_utils::write_owned_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
                        &cycle.pointer_path,
                        pointer_bytes,
                        self.persisted.local_disk_budget.as_ref(),
                        crate::DiskCategory::Registry,
                        crate::DiskReservationKind::Maintenance,
                        self.catalog_reconciliation_memory_limit(),
                    );
                match publication {
                    Ok(()) => {
                        cycle.pointer_may_be_visible = true;
                        Ok(BoundedTieredCatalogStep::Complete)
                    }
                    Err(err) => {
                        cycle.pointer_may_be_visible =
                            tiering::load_shared_segment_catalog_pointer(config)
                                .is_ok_and(|visible| visible == Some(pointer));
                        Err(err)
                    }
                }
            }
            BoundedTieredCatalogPublicationPhase::RegistryPrepare => {
                self.prepare_bounded_registry_catalog_reconciliation(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::RegistryEntries => {
                self.persist_next_bounded_registry_catalog_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::RegistrySweepOpen => {
                self.open_bounded_registry_catalog_sweep(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::RegistrySweepEntries => {
                self.sweep_next_bounded_registry_catalog_entry(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::RegistryComplete => {
                self.complete_bounded_registry_catalog_reconciliation(cycle, budget)
            }
            BoundedTieredCatalogPublicationPhase::RegistryLegacyRetire => {
                self.retire_legacy_registry_catalog_after_bounded_reconciliation(cycle, budget)
            }
        }
    }

    /// Advances at most one configured item/byte pass, unless `drain` requests a complete
    /// lifecycle/manual operation. The retained cursor is charged to remote-catalog staging.
    pub(in crate::engine::storage_engine) fn advance_bounded_tiered_catalog_publication(
        &self,
        drain: bool,
    ) -> Result<bool> {
        let config = self.persisted.tiered_storage.as_ref().ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "bounded tiered catalog publication requires tiered storage".to_string(),
            )
        })?;
        self.validate_shared_object_store_writer_lock()?;

        loop {
            let current_visibility_generation = self.visibility_state_generation();
            let stale = {
                let cursor = self.coordination.background_catalog_refresh_cursor.lock();
                cursor
                    .writer_publication_cycle
                    .as_ref()
                    .is_some_and(|cycle| {
                        cycle.expected_visibility_generation != current_visibility_generation
                    })
            };
            if stale {
                self.reset_bounded_tiered_catalog_publication();
            }

            let mut cursor = self.coordination.background_catalog_refresh_cursor.lock();
            if cursor.writer_publication_cycle.is_none() {
                cursor.writer_publication_cycle = Some(BoundedTieredCatalogPublicationCycle::new(
                    self,
                    config,
                    current_visibility_generation,
                )?);
            }
            let cycle = cursor
                .writer_publication_cycle
                .as_mut()
                .expect("bounded writer publication cycle initialized above");
            let mut budget = BoundedTieredCatalogPassBudget::new(
                self.runtime.maintenance_max_items_per_pass,
                self.runtime.maintenance_max_bytes_per_pass,
            );
            let mut completed = false;
            let mut error = None;
            loop {
                match self.advance_bounded_tiered_catalog_step(config, cycle, &mut budget) {
                    Ok(BoundedTieredCatalogStep::Progressed) if !budget.exhausted() => {}
                    Ok(BoundedTieredCatalogStep::Progressed)
                    | Ok(BoundedTieredCatalogStep::Deferred) => break,
                    Ok(BoundedTieredCatalogStep::Complete) => {
                        completed = true;
                        break;
                    }
                    Err(err) => {
                        error = Some(err);
                        break;
                    }
                }
            }
            let finished_cycle = if completed || error.is_some() {
                cursor.writer_publication_cycle.take()
            } else {
                None
            };
            let completed_registry_reconciliation = completed
                && finished_cycle
                    .as_ref()
                    .is_some_and(|cycle| cycle.reconcile_registry);
            if completed {
                cursor.writer_publication_completed_visibility_generation = finished_cycle
                    .as_ref()
                    .map(|cycle| cycle.expected_visibility_generation);
                if completed_registry_reconciliation {
                    self.coordination
                        .bounded_registry_reconciliation_required
                        .store(false, Ordering::Release);
                }
            }
            drop(cursor);

            if let Some(cycle) = finished_cycle.as_ref() {
                if error.is_some() {
                    if let Err(cleanup_err) = self.cleanup_bounded_tiered_catalog_cycle(cycle) {
                        let primary = error.take().expect("error checked above");
                        return Err(TsinkError::Other(format!(
                            "bounded tiered catalog publication failed: {primary}; cleanup failed: {cleanup_err}"
                        )));
                    }
                }
            }
            if let Some(err) = error {
                return Err(err);
            }
            if completed {
                if completed_registry_reconciliation {
                    self.synchronize_persisted_index_dirty_with_pending();
                }
                return Ok(true);
            }
            if !drain {
                return Ok(false);
            }
        }
    }
}

impl ChunkStorage {
    fn modeled_shared_inventory_clone_bytes(
        config: &super::super::config::TieredStorageConfig,
        inventory: &SegmentInventory,
    ) -> usize {
        let entries = inventory.entries().len();
        // Filter/collect can geometrically retain up to two entry buffers and the stable
        // inventory sort may allocate one more. The fixed base covers their allocation headers.
        let vector_and_sort = entries
            .saturating_mul(std::mem::size_of::<SegmentInventoryEntry>())
            .saturating_mul(3);
        let destination_path_upper_bound = config
            .object_store_root
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .saturating_add(SEGMENT_CATALOG_SHARED_PATH_ALLOWANCE_BYTES);
        let root_payloads = inventory.entries().iter().fold(0usize, |total, entry| {
            total.saturating_add(
                entry
                    .root
                    .as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .max(destination_path_upper_bound)
                    .saturating_mul(2)
                    .saturating_add(SEGMENT_CATALOG_SHARED_PATH_ALLOCATOR_BYTES),
            )
        });
        SEGMENT_CATALOG_PUBLICATION_BASE_STAGING_BYTES
            .saturating_add(vector_and_sort)
            .saturating_add(root_payloads)
    }

    fn modeled_segment_catalog_publication_bytes(&self, inventory: &SegmentInventory) -> usize {
        let Some(config) = &self.persisted.tiered_storage else {
            return 0;
        };
        let local_peak = config.segment_catalog_path.as_deref().map_or(0, |path| {
            tiering::modeled_legacy_segment_catalog_publication_bytes(path, inventory)
        });
        let shared_peak = if self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly {
            0
        } else {
            Self::modeled_shared_inventory_clone_bytes(config, inventory).saturating_add(
                tiering::modeled_shared_segment_catalog_publication_bytes(config, inventory),
            )
        };
        local_peak.max(shared_peak)
    }

    fn publish_segment_inventory_with_hooks<F, S>(
        &self,
        inventory: &SegmentInventory,
        after_admission: F,
        mut shared_stage_hook: S,
    ) -> Result<()>
    where
        F: FnOnce(usize),
        S: FnMut(tiering::SegmentCatalogPublishStage) -> Result<()>,
    {
        let bounded_publication = self.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.runtime.maintenance_max_bytes_per_pass != u64::MAX
            || self.memory_budget_value() != usize::MAX;
        let publication_bytes = if bounded_publication {
            self.modeled_segment_catalog_publication_bytes(inventory)
        } else {
            0
        };
        if self.runtime.maintenance_max_bytes_per_pass != u64::MAX
            && u64::try_from(publication_bytes).unwrap_or(u64::MAX)
                > self.runtime.maintenance_max_bytes_per_pass
        {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: tiering::SEGMENT_CATALOG_PUBLICATION_MEMORY_OPERATION,
                limit: self.runtime.maintenance_max_bytes_per_pass,
                required: u64::try_from(publication_bytes).unwrap_or(u64::MAX),
            });
        }
        let mut memory_reservation = if publication_bytes == 0 {
            None
        } else {
            Some(self.remote_catalog_memory_reservation(publication_bytes)?)
        };
        after_admission(publication_bytes);

        if let Some(config) = self.persisted.tiered_storage.as_ref() {
            if self.runtime.runtime_mode != StorageRuntimeMode::ComputeOnly {
                self.validate_shared_object_store_writer_lock()?;
                // Startup hydration and explicit one-shot publication supersede any process-local
                // finite cursor. A crash can leave only these two deterministic owned stage names;
                // remove those exact paths before the first new publication instead of globbing
                // the surrounding host-owned namespace.
                self.reset_bounded_tiered_catalog_publication();
                self.cleanup_exact_bounded_tiered_catalog_stages(config)?;
            }
        }

        let (hot, warm, cold) = inventory.tier_counts();
        self.observability
            .flush
            .hot_segments_visible
            .store(hot, Ordering::Relaxed);
        self.observability
            .flush
            .warm_segments_visible
            .store(warm, Ordering::Relaxed);
        self.observability
            .flush
            .cold_segments_visible
            .store(cold, Ordering::Relaxed);
        if let Some(config) = &self.persisted.tiered_storage {
            if let Some(path) = config.segment_catalog_path.as_deref() {
                if let Some(reservation) = memory_reservation.as_mut() {
                    tiering::persist_segment_catalog_budgeted_with_memory_admission(
                        path,
                        inventory,
                        self.persisted.local_disk_budget.as_ref(),
                        |required| {
                            if required > reservation.reserved_bytes() {
                                self.resize_remote_catalog_memory_reservation(reservation, required)
                            } else {
                                Ok(())
                            }
                        },
                    )?;
                } else {
                    tiering::persist_segment_catalog_budgeted(
                        path,
                        inventory,
                        self.persisted.local_disk_budget.as_ref(),
                    )?;
                }
            }
            if self.runtime.runtime_mode != StorageRuntimeMode::ComputeOnly {
                let shared_inventory = self.shared_remote_segment_inventory(inventory);
                if let Some(reservation) = memory_reservation.as_mut() {
                    tiering::persist_shared_segment_catalog_budgeted_with_stage_hook(
                        config,
                        &shared_inventory,
                        self.persisted.local_disk_budget.as_ref(),
                        |required| {
                            if required > reservation.reserved_bytes() {
                                self.resize_remote_catalog_memory_reservation(reservation, required)
                            } else {
                                Ok(())
                            }
                        },
                        &mut shared_stage_hook,
                    )?;
                } else {
                    tiering::persist_shared_segment_catalog_budgeted_with_stage_hook(
                        config,
                        &shared_inventory,
                        self.persisted.local_disk_budget.as_ref(),
                        |_| Ok(()),
                        &mut shared_stage_hook,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn publish_segment_inventory_with_admission_hook<F>(
        &self,
        inventory: &SegmentInventory,
        after_admission: F,
    ) -> Result<()>
    where
        F: FnOnce(usize),
    {
        self.publish_segment_inventory_with_hooks(inventory, after_admission, |_| Ok(()))
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn publish_segment_inventory_with_test_admission_hook<F>(
        &self,
        inventory: &SegmentInventory,
        after_admission: F,
    ) -> Result<()>
    where
        F: FnOnce(usize),
    {
        self.publish_segment_inventory_with_admission_hook(inventory, after_admission)
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn publish_segment_inventory_with_test_hooks<F, S>(
        &self,
        inventory: &SegmentInventory,
        after_admission: F,
        shared_stage_hook: S,
    ) -> Result<()>
    where
        F: FnOnce(usize),
        S: FnMut(tiering::SegmentCatalogPublishStage) -> Result<()>,
    {
        self.publish_segment_inventory_with_hooks(inventory, after_admission, shared_stage_hook)
    }

    fn persisted_inventory_entries_for_roots(
        &self,
        roots: &BTreeSet<PathBuf>,
    ) -> Vec<SegmentInventoryEntry> {
        let persisted_index = self.persisted.persisted_index.read();
        roots
            .iter()
            .filter_map(|root| {
                persisted_index
                    .segments_by_root
                    .get(root)
                    .map(|state| SegmentInventoryEntry {
                        lane: state.lane,
                        tier: state.tier,
                        root: root.clone(),
                        manifest: state.manifest.clone(),
                    })
            })
            .collect()
    }

    fn update_visible_segment_counter(
        counter: &std::sync::atomic::AtomicU64,
        before: u64,
        after: u64,
    ) {
        if before == after {
            return;
        }
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(before).saturating_add(after))
        });
    }

    fn publish_segment_inventory_delta(
        &self,
        before: &[SegmentInventoryEntry],
        after: &[SegmentInventoryEntry],
    ) {
        let tier_count = |entries: &[SegmentInventoryEntry], tier| {
            saturating_u64_from_usize(entries.iter().filter(|entry| entry.tier == tier).count())
        };
        Self::update_visible_segment_counter(
            &self.observability.flush.hot_segments_visible,
            tier_count(before, PersistedSegmentTier::Hot),
            tier_count(after, PersistedSegmentTier::Hot),
        );
        Self::update_visible_segment_counter(
            &self.observability.flush.warm_segments_visible,
            tier_count(before, PersistedSegmentTier::Warm),
            tier_count(after, PersistedSegmentTier::Warm),
        );
        Self::update_visible_segment_counter(
            &self.observability.flush.cold_segments_visible,
            tier_count(before, PersistedSegmentTier::Cold),
            tier_count(after, PersistedSegmentTier::Cold),
        );
    }

    #[cfg(test)]
    fn invoke_catalog_transition_post_catalog_publication_hook(&self) -> Result<()> {
        let hook = self
            .persist_test_hooks
            .catalog_transition_post_catalog_publication_hook
            .read()
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_catalog_transition_post_catalog_publication_hook<
        F,
    >(
        &self,
        hook: F,
    ) where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .catalog_transition_post_catalog_publication_hook
            .write() = Some(std::sync::Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn clear_catalog_transition_post_catalog_publication_hook(
        &self,
    ) {
        self.persist_test_hooks
            .catalog_transition_post_catalog_publication_hook
            .write()
            .take();
    }

    #[cfg(test)]
    fn invoke_catalog_transition_post_index_mutation_hook(&self) -> Result<()> {
        let hook = self
            .persist_test_hooks
            .catalog_transition_post_index_mutation_hook
            .read()
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_catalog_transition_post_index_mutation_hook<F>(
        &self,
        hook: F,
    ) where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .catalog_transition_post_index_mutation_hook
            .write() = Some(std::sync::Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn clear_catalog_transition_post_index_mutation_hook(
        &self,
    ) {
        self.persist_test_hooks
            .catalog_transition_post_index_mutation_hook
            .write()
            .take();
    }

    pub(super) fn mirror_segment_inventory_entries_if_configured(
        &self,
        entries: &[SegmentInventoryEntry],
    ) -> Result<()> {
        let Some(config) = &self.persisted.tiered_storage else {
            return Ok(());
        };
        if !config.mirror_hot_segments {
            return Ok(());
        }

        self.validate_shared_object_store_writer_lock()?;

        for entry in entries {
            if entry.tier != PersistedSegmentTier::Hot {
                continue;
            }
            let destination = tiering::destination_segment_root(
                config,
                entry.lane,
                PersistedSegmentTier::Hot,
                &entry.manifest,
            );
            if destination == entry.root {
                continue;
            }
            tiering::move_segment_to_tier(&entry.root, &destination)?;
        }

        Ok(())
    }

    pub(super) fn publish_segment_inventory(&self, inventory: &SegmentInventory) -> Result<()> {
        self.publish_segment_inventory_with_admission_hook(inventory, |_| {})
    }

    fn publish_scanned_segment_inventory(&self, inventory: &SegmentInventory) -> Result<()> {
        self.mirror_segment_inventory_entries_if_configured(inventory.entries())?;
        self.publish_segment_inventory(inventory)
    }

    pub(in crate::engine::storage_engine) fn modeled_finite_transition_staging_bytes(
        &self,
        transition: &PersistedCatalogTransition,
    ) -> usize {
        let representative_root = transition
            .loaded_segments
            .first()
            .map(|segment| segment.root.as_path())
            .or_else(|| transition.removed_roots.first().map(PathBuf::as_path))
            .or_else(|| match &transition.publication {
                PersistedCatalogPublication::PersistedState {
                    published_segment_roots,
                    ..
                } => published_segment_roots.first().map(PathBuf::as_path),
                PersistedCatalogPublication::Inventory { inventory, .. } => inventory
                    .entries()
                    .first()
                    .map(|entry| entry.root.as_path()),
            })
            .unwrap_or_else(|| std::path::Path::new(""));
        let base = super::bounded_remote::modeled_transition_publication_capacity_bytes(
            transition,
            representative_root,
            None,
        );
        if transition.removed_roots.is_empty() {
            return base;
        }

        // `remove_persisted_segment_roots` builds one accounting scope spanning the whole input,
        // then decodes all affected series identities while the original visible states remain
        // borrowed. Model that aggregate without cloning manifests, roots, or identities during
        // preflight. Duplicate series across roots are deliberately charged more than once: this
        // avoids allocating a temporary deduplication set and remains a safe upper bound.
        let persisted_index = self.persisted.persisted_index.read();
        let registry = self.catalog.registry.read();
        let mut representative_removal_consumed = false;
        transition
            .removed_roots
            .iter()
            .fold(base, |staging_bytes, root| {
                let additional_root_auxiliary =
                    if !representative_removal_consumed && root.as_path() == representative_root {
                        representative_removal_consumed = true;
                        0
                    } else {
                        super::bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(
                            root,
                        )
                    };
                let Some(state) = persisted_index.segments_by_root.get(root) else {
                    return staging_bytes.saturating_add(additional_root_auxiliary);
                };
                let mutation_bytes =
                    super::bounded_scan::modeled_removal_bytes(root, &state.manifest)
                        .min(usize::MAX as u64) as usize;
                let identity_bytes = state.chunk_refs_by_series.keys().fold(
                    0usize,
                    |identity_bytes, &series_id| {
                        let Some((metric_bytes, label_count, label_text_bytes)) =
                            registry.decoded_series_key_shape(series_id)
                        else {
                            // The mutation itself will reject this corrupt state. Saturating the
                            // preflight prevents an unreserved identity materialization first.
                            return usize::MAX;
                        };
                        let one_identity = metric_bytes
                            .saturating_add(label_text_bytes)
                            .saturating_add(
                                label_count.saturating_mul(std::mem::size_of::<crate::Label>()),
                            )
                            .saturating_add(
                                1usize
                                    .saturating_add(label_count.saturating_mul(2))
                                    .saturating_mul(64),
                            );
                        identity_bytes
                            .saturating_add(one_identity.saturating_mul(
                                FINITE_TRANSITION_REMOVAL_SERIES_METADATA_COPIES,
                            ))
                            .saturating_add(std::mem::size_of::<SeriesId>().saturating_mul(4))
                    },
                );
                staging_bytes
                    .saturating_add(additional_root_auxiliary)
                    .saturating_add(mutation_bytes)
                    .saturating_add(identity_bytes)
            })
    }

    pub(in super::super) fn apply_persisted_catalog_transition_phase(
        &self,
        transition: PersistedCatalogTransition,
        current_visibility_generation: u64,
    ) -> Result<PersistedCatalogRefreshApply> {
        if transition
            .visibility_fence
            .is_some_and(|fence| !fence.matches(current_visibility_generation))
        {
            return Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState);
        }

        // Without tiered storage there is no durable segment-catalog file to rewrite. Retain an
        // exact before-image for only the roots named by this transition. The drop guard publishes
        // the corresponding counter delta on every exit, including a failure after index mutation,
        // so retry cannot lose or double-apply the visibility counters. Tiered read-write mode
        // deliberately retains the complete inventory path because local v2 compatibility and
        // shared generation publication still require the complete final image.
        let finite_tiered_writer = self.finite_tiered_catalog_publication_enabled();
        let delta_roots = if self.persisted.tiered_storage.is_none()
            || self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly
            || finite_tiered_writer
        {
            match &transition.publication {
                PersistedCatalogPublication::PersistedState {
                    published_segment_roots,
                    refresh_tombstones: _,
                } => {
                    let mut roots = transition
                        .removed_roots
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    roots.extend(published_segment_roots.iter().cloned());
                    Some(roots)
                }
                PersistedCatalogPublication::Inventory { .. } if finite_tiered_writer => {
                    let mut roots = transition
                        .removed_roots
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    roots.extend(
                        transition
                            .loaded_segments
                            .iter()
                            .map(|segment| segment.root.clone()),
                    );
                    Some(roots)
                }
                PersistedCatalogPublication::Inventory { .. } => None,
            }
        } else {
            None
        };
        let segment_inventory_delta =
            delta_roots.map(|roots| SegmentInventoryDeltaGuard::new(self, roots));

        let refresh_tombstones = match &transition.publication {
            PersistedCatalogPublication::PersistedState {
                refresh_tombstones, ..
            }
            | PersistedCatalogPublication::Inventory {
                refresh_tombstones, ..
            } => *refresh_tombstones,
        };
        let finite_maintenance = self.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.runtime.maintenance_max_bytes_per_pass != u64::MAX;
        // Ordinary segment transitions update their accounted components incrementally. A full
        // engine recount here made every bounded flush proportional to all live state. Tombstone
        // replacement is the exception: its conservative admission compares a newly decoded map
        // with the complete current visibility footprint, so reconcile before creating that
        // reservation only on that path.
        if refresh_tombstones && !finite_maintenance {
            self.refresh_memory_usage();
        }
        let mut tombstone_reservation = self.tombstone_memory_reservation();
        let loaded_tombstones = if finite_maintenance {
            // Finite read-write runtimes exclusively own their local data path and shared-writer
            // lease. Startup hydration loaded every preexisting lane, and ordinary deletes
            // publish durable-before-live, so an inventory transition must not rescan/clone the
            // complete manifest set. The only possible newer authoritative state is a durable
            // Committing coordinator left by this writer; its bounded read-before-mutate recovery
            // publication guard publishes the preloaded candidate before segment visibility
            // changes.
            None
        } else if refresh_tombstones {
            self.validate_shared_object_store_writer_lock()?;
            let tombstone_index = self.tombstone_index_context();
            // A durable Committing coordinator is authoritative even before its first lane
            // manifest is published. Recover (or fail closed) before reading any manifest so a
            // catalog refresh cannot replace the live map with a stale predecessor image.
            tombstone_index.recover_pending_transaction(&mut tombstone_reservation)?;
            let merged = tombstone_index.read_tombstones_index(&mut tombstone_reservation)?;
            let transition_visibility_headroom =
                transition
                    .loaded_segments
                    .iter()
                    .fold(16 * 1024usize, |total, segment| {
                        // A loaded chunk can add at most two transient visibility-range entries
                        // (source plus normalization destination, currently under 64 bytes total)
                        // and one persisted ref. 1 KiB per chunk therefore dominates the second
                        // post-install visibility estimate; removals can only reduce it. The per-
                        // series allowance covers capped retained summaries and cache-map growth.
                        let chunk_headroom = segment.chunk_index.entries.len().saturating_mul(1024);
                        let series_headroom = segment
                            .series
                            .len()
                            .max(segment.manifest.series_count)
                            .saturating_mul(4096);
                        total
                            .saturating_add(chunk_headroom)
                            .saturating_add(series_headroom)
                    });
            self.tombstone_publication_context()
                .admit_loaded_tombstone_publication(
                    self,
                    &merged,
                    transition_visibility_headroom,
                    &mut tombstone_reservation,
                )?;
            Some(merged)
        } else {
            None
        };

        // Publish authoritative deletes first. If a later segment/catalog operation fails, the
        // conservative state hides data rather than exposing newly visible deleted samples.
        if let Some(tombstones) = loaded_tombstones {
            self.tombstone_publication_context()
                .replace_loaded_tombstones_index_locked(
                    self,
                    tombstones,
                    &mut tombstone_reservation,
                )?;
        }

        if finite_tiered_writer {
            // Either state-update call below can mutate the visible segment index before returning
            // an error. Publish the durable-sidecar debt first so every post-mutation failure has
            // a retry owner. A pre-mutation failure can leave a harmless false positive; only an
            // exact terminal registry reconciliation clears this sticky bit.
            self.coordination
                .bounded_registry_reconciliation_required
                .store(true, Ordering::Release);
            self.persisted
                .persisted_index_dirty
                .store(true, Ordering::SeqCst);
        }

        self.add_persisted_segments_from_loaded(transition.loaded_segments)?;
        self.remove_persisted_segment_roots(&transition.removed_roots)?;

        #[cfg(test)]
        self.invoke_catalog_transition_post_index_mutation_hook()?;

        let catalog_publication_complete = match transition.publication {
            PersistedCatalogPublication::PersistedState {
                published_segment_roots,
                refresh_tombstones: _,
            } => {
                if finite_tiered_writer {
                    let roots = published_segment_roots
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let published_entries = self.persisted_inventory_entries_for_roots(&roots);
                    self.mirror_segment_inventory_entries_if_configured(&published_entries)?;
                    self.advance_bounded_tiered_catalog_publication(
                        self.coordination.lifecycle.load(Ordering::Acquire) != STORAGE_OPEN,
                    )?
                } else {
                    if segment_inventory_delta.is_none() {
                        self.refresh_segment_catalog_and_observability_from_persisted_state(
                            &published_segment_roots,
                        )?;
                    }
                    true
                }
            }
            PersistedCatalogPublication::Inventory {
                inventory,
                refresh_tombstones: _,
            } => {
                if finite_tiered_writer {
                    self.mirror_segment_inventory_entries_if_configured(inventory.entries())?;
                    self.advance_bounded_tiered_catalog_publication(
                        self.coordination.lifecycle.load(Ordering::Acquire) != STORAGE_OPEN,
                    )?
                } else {
                    self.publish_scanned_segment_inventory(&inventory)?;
                    true
                }
            }
        };
        if !catalog_publication_complete {
            return Ok(PersistedCatalogRefreshApply::Deferred);
        }

        #[cfg(test)]
        self.invoke_catalog_transition_post_catalog_publication_hook()?;

        if !finite_tiered_writer {
            if let Some(registry_catalog_update) = transition.registry_catalog_update {
                self.persist_series_registry_index_with_catalog_update(&registry_catalog_update)?;
            }
        }

        Ok(PersistedCatalogRefreshApply::Applied)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::super::config::{ChunkStorageOptions, TieredStorageConfig};
    use super::*;
    use crate::engine::segment::{SegmentManifest, WalHighWatermark};
    use crate::engine::storage_engine::tiering::SegmentLaneFamily;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc};
    use tempfile::TempDir;

    fn config(root: &Path) -> TieredStorageConfig {
        TieredStorageConfig {
            object_store_root: root.join("shared"),
            segment_catalog_path: Some(root.join("local").join("segment_catalog.json")),
            mirror_hot_segments: false,
            hot_retention_window: 10,
            warm_retention_window: 50,
        }
    }

    fn inventory(config: &TieredStorageConfig, entries: usize) -> SegmentInventory {
        SegmentInventory::from_entries(
            (0..entries)
                .map(|index| {
                    let segment_id = u64::try_from(index).unwrap().saturating_add(1);
                    let manifest = SegmentManifest {
                        segment_id,
                        level: 0,
                        chunk_count: 1,
                        point_count: 1,
                        series_count: 1,
                        min_ts: Some(segment_id as i64),
                        max_ts: Some(segment_id as i64),
                        wal_highwater: WalHighWatermark::default(),
                    };
                    SegmentInventoryEntry {
                        lane: SegmentLaneFamily::Numeric,
                        tier: PersistedSegmentTier::Hot,
                        root: config
                            .lane_path(SegmentLaneFamily::Numeric, PersistedSegmentTier::Hot)
                            .join("segments")
                            .join("L0")
                            .join(format!("seg-{segment_id:016x}")),
                        manifest,
                    }
                })
                .collect(),
        )
    }

    fn modeled_publication_bytes(
        config: &TieredStorageConfig,
        inventory: &SegmentInventory,
    ) -> usize {
        let local = tiering::modeled_legacy_segment_catalog_publication_bytes(
            config.segment_catalog_path.as_deref().unwrap(),
            inventory,
        );
        let shared = ChunkStorage::modeled_shared_inventory_clone_bytes(config, inventory)
            .saturating_add(tiering::modeled_shared_segment_catalog_publication_bytes(
                config, inventory,
            ));
        local.max(shared)
    }

    fn storage(root: &Path, config: TieredStorageConfig, maintenance_bytes: u64) -> ChunkStorage {
        storage_with_items(root, config, usize::MAX, maintenance_bytes)
    }

    fn storage_with_items(
        root: &Path,
        config: TieredStorageConfig,
        maintenance_items: usize,
        maintenance_bytes: u64,
    ) -> ChunkStorage {
        storage_with_items_and_disk_budget(root, config, maintenance_items, maintenance_bytes, None)
    }

    fn storage_with_items_and_disk_budget(
        root: &Path,
        config: TieredStorageConfig,
        maintenance_items: usize,
        maintenance_bytes: u64,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> ChunkStorage {
        let storage = ChunkStorage::new_with_data_path_and_options_and_disk_budget(
            2,
            None,
            Some(root.join("data").join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                memory_budget_bytes: u64::MAX,
                maintenance_max_items_per_pass: maintenance_items,
                maintenance_max_bytes_per_pass: maintenance_bytes,
                tiered_storage: Some(config.clone()),
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
            local_disk_budget,
        )
        .unwrap();
        crate::engine::storage_engine::tests::install_shared_object_store_writer_lock_for_test(
            &storage,
            &config.object_store_root,
        );
        storage
    }

    fn seed_persisted_catalog_entries(
        storage: &ChunkStorage,
        config: &TieredStorageConfig,
        start_segment_id: u64,
        entries: usize,
    ) {
        let mut persisted = storage.persisted.persisted_index.write();
        for offset in 0..entries {
            let segment_id = start_segment_id.saturating_add(u64::try_from(offset).unwrap());
            let manifest = SegmentManifest {
                segment_id,
                level: 0,
                chunk_count: 1,
                point_count: 1,
                series_count: 1,
                min_ts: Some(segment_id as i64),
                max_ts: Some(segment_id as i64),
                wal_highwater: WalHighWatermark::default(),
            };
            let root = config
                .lane_path(SegmentLaneFamily::Numeric, PersistedSegmentTier::Hot)
                .join("segments")
                .join("L0")
                .join(format!("seg-{segment_id:016x}"));
            persisted.segments_by_root.insert(
                root,
                crate::engine::storage_engine::state::PersistedSegmentState {
                    segment_slot: usize::try_from(segment_id).unwrap(),
                    lane: SegmentLaneFamily::Numeric,
                    tier: PersistedSegmentTier::Hot,
                    manifest,
                    time_bucket_postings: None,
                    series_time_summaries: HashMap::new(),
                    chunk_refs_by_series: HashMap::new(),
                },
            );
        }
        drop(persisted);
        storage.bump_visibility_state_generation();
    }

    fn cursor_retained_bytes(storage: &ChunkStorage) -> Option<usize> {
        storage
            .coordination
            .background_catalog_refresh_cursor
            .lock()
            .writer_publication_cycle
            .as_ref()
            .map(BoundedTieredCatalogPublicationCycle::modeled_retained_bytes)
    }

    fn load_published_generation_entries(
        config: &TieredStorageConfig,
    ) -> Vec<SegmentInventoryEntry> {
        let pointer = tiering::require_shared_segment_catalog_pointer(config).unwrap();
        let generation_path =
            tiering::shared_segment_catalog_generation_path(config, pointer.generation);
        let mut cursor = tiering::SegmentCatalogGenerationReadCursor::new(pointer);
        let mut entries = Vec::new();
        loop {
            let page = tiering::read_segment_catalog_generation_page(
                &mut cursor,
                &generation_path,
                None,
                None,
                Some(config),
                usize::MAX,
                u64::MAX,
            )
            .unwrap();
            assert!(!page.deferred_frame);
            entries.extend(page.entries);
            if page.complete {
                return entries;
            }
        }
    }

    fn advance_until_bounded_catalog_complete(
        storage: &ChunkStorage,
        config: &TieredStorageConfig,
        prior_pointer: Option<SegmentCatalogPointer>,
    ) -> usize {
        for pass in 1..=128 {
            let complete = storage
                .advance_bounded_tiered_catalog_publication(false)
                .unwrap();
            if complete {
                return pass;
            }
            assert_eq!(
                tiering::load_shared_segment_catalog_pointer(config).unwrap(),
                prior_pointer,
                "a deferred pass must not advance the authoritative pointer"
            );
            assert!(storage.bounded_tiered_catalog_publication_is_pending());
            assert_eq!(
                storage
                    .observability_snapshot()
                    .memory
                    .remote_catalog_staging_bytes,
                cursor_retained_bytes(storage).unwrap()
            );
        }
        panic!("bounded tiered catalog publication did not complete in 128 passes");
    }

    fn one_entry_maximum_bounded_work_bytes(
        config: &TieredStorageConfig,
        entry: &SegmentInventoryEntry,
    ) -> u64 {
        let shared = shared_catalog_contains_entry(config, entry);
        let retained =
            BoundedTieredCatalogPublicationCycle::modeled_entry_retained_bytes(entry, shared);
        let legacy = tiering::encode_legacy_segment_catalog_entry_fragment(entry, true).unwrap();
        let generation_header =
            tiering::encode_segment_catalog_generation_header(1, u64::from(shared)).unwrap();
        let (_, generation_frame) =
            tiering::encode_segment_catalog_generation_frame(entry).unwrap();
        let generation_len =
            generation_header
                .len()
                .saturating_add(if shared { generation_frame.len() } else { 0 });
        let pointer = tiering::finalized_segment_catalog_pointer(
            1,
            usize::from(shared),
            u64::try_from(generation_len).unwrap(),
            0,
        )
        .unwrap();
        let pointer = tiering::encode_segment_catalog_pointer(pointer).unwrap();
        let shared_target = tiering::shared_segment_catalog_path(config);
        let local_stage = config
            .segment_catalog_path
            .as_ref()
            .is_some_and(|path| path != &shared_target);
        let fixed_prepare_probes = usize::from(local_stage).saturating_add(3);
        [
            u64::try_from(retained).unwrap(),
            u64::try_from(
                fixed_prepare_probes.saturating_mul(SEGMENT_CATALOG_SHARED_PATH_ALLOWANCE_BYTES),
            )
            .unwrap(),
            BOUNDED_GENERATION_NAMESPACE_ENTRY_WORK_BYTES,
            ChunkStorage::bounded_catalog_fragment_work_bytes(
                tiering::SEGMENT_CATALOG_LEGACY_STREAM_PREFIX.len(),
            ),
            ChunkStorage::bounded_catalog_fragment_work_bytes(legacy.len()),
            ChunkStorage::bounded_catalog_fragment_work_bytes(
                tiering::SEGMENT_CATALOG_LEGACY_STREAM_SUFFIX.len(),
            ),
            ChunkStorage::bounded_catalog_fragment_work_bytes(generation_header.len()),
            ChunkStorage::bounded_catalog_fragment_work_bytes(generation_frame.len()),
            ChunkStorage::bounded_catalog_fragment_work_bytes(pointer.len()),
            ChunkStorage::bounded_catalog_fragment_work_bytes(0),
        ]
        .into_iter()
        .max()
        .unwrap()
    }

    #[test]
    fn finite_writer_catalog_publication_resumes_and_commits_pointer_last() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let storage = storage_with_items(temp.path(), config.clone(), 4, u64::MAX);
        seed_persisted_catalog_entries(&storage, &config, 1, 3);

        assert!(tiering::load_shared_segment_catalog_pointer(&config)
            .unwrap()
            .is_none());
        let passes = advance_until_bounded_catalog_complete(&storage, &config, None);
        assert!(passes > 1);
        assert!(!storage.bounded_tiered_catalog_publication_is_pending());
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );

        let pointer = tiering::require_shared_segment_catalog_pointer(&config).unwrap();
        assert_eq!(pointer.entry_count, 3);
        assert_eq!(load_published_generation_entries(&config).len(), 3);
        let shared_v2 = tiering::load_segment_catalog(
            &tiering::shared_segment_catalog_path(&config),
            None,
            None,
            Some(&config),
        )
        .unwrap();
        assert_eq!(shared_v2.entries().len(), 3);
        let local_v2 = tiering::load_segment_catalog(
            config.segment_catalog_path.as_deref().unwrap(),
            None,
            None,
            Some(&config),
        )
        .unwrap();
        assert_eq!(local_v2.entries().len(), 3);
    }

    #[test]
    fn finite_registry_reconcile_scans_one_of_sixteen_thousand_roots_with_item_limit_one() {
        use std::sync::atomic::AtomicUsize;

        const LIVE_ROOTS: usize = 16_000;

        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let storage = storage_with_items(temp.path(), config.clone(), 1, u64::MAX);
        seed_persisted_catalog_entries(&storage, &config, 1, LIVE_ROOTS);
        let inspected = Arc::new(AtomicUsize::new(0));
        storage.set_persisted_catalog_inventory_entry_hook({
            let inspected = Arc::clone(&inspected);
            move || {
                inspected.fetch_add(1, Ordering::Relaxed);
            }
        });
        storage
            .coordination
            .bounded_registry_reconciliation_required
            .store(true, Ordering::Release);
        storage
            .persisted
            .persisted_index_dirty
            .store(true, Ordering::SeqCst);

        // Generation preparation has its own fixed namespace dependency window. Seed the cursor
        // immediately after that already-admitted phase so this test isolates the live-inventory
        // and registry-reconcile page boundary at the minimum logical item budget.
        let mut cycle = BoundedTieredCatalogPublicationCycle::new(
            &storage,
            &config,
            storage.visibility_state_generation(),
        )
        .unwrap();
        cycle.phase = BoundedTieredCatalogPublicationPhase::Scanning;
        storage
            .coordination
            .background_catalog_refresh_cursor
            .lock()
            .writer_publication_cycle = Some(cycle);

        assert!(!storage
            .advance_bounded_tiered_catalog_publication(false)
            .unwrap());
        assert_eq!(inspected.load(Ordering::Relaxed), 1);
        let cursor = storage
            .coordination
            .background_catalog_refresh_cursor
            .lock();
        let cycle = cursor.writer_publication_cycle.as_ref().unwrap();
        assert_eq!(cycle.entries.len(), 1);
        assert!(cycle.reconcile_registry);
        drop(cursor);
        assert!(storage
            .coordination
            .bounded_registry_reconciliation_required
            .load(Ordering::Acquire));
        assert!(storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst));

        storage.clear_persisted_catalog_inventory_entry_hook();
        storage.reset_bounded_tiered_catalog_publication();
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn finite_writer_generation_namespace_scan_and_gc_are_one_item_per_pass() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let storage = storage_with_items(temp.path(), config.clone(), 1, u64::MAX);
        let first = tiering::persist_shared_segment_catalog_budgeted(
            &config,
            &SegmentInventory::default(),
            None,
        )
        .unwrap();
        let second = tiering::persist_shared_segment_catalog_budgeted(
            &config,
            &SegmentInventory::default(),
            None,
        )
        .unwrap();
        assert!(second.generation > first.generation);

        let generation_directory = tiering::shared_segment_catalog_generation_directory(&config);
        let stale = tiering::shared_segment_catalog_generation_path(&config, 1);
        std::fs::write(&stale, b"stale").unwrap();
        let unknown = generation_directory.join("host-owned.entry");
        std::fs::write(&unknown, b"unknown").unwrap();
        let initial_namespace_entries = std::fs::read_dir(&generation_directory).unwrap().count();
        assert_eq!(initial_namespace_entries, 4);

        assert!(!storage
            .advance_bounded_tiered_catalog_publication(false)
            .unwrap());
        {
            let cursor = storage
                .coordination
                .background_catalog_refresh_cursor
                .lock();
            let cycle = cursor.writer_publication_cycle.as_ref().unwrap();
            assert_eq!(
                cycle.phase,
                BoundedTieredCatalogPublicationPhase::ScanGenerationNamespace
            );
            assert_eq!(
                cycle.generation_namespace_count, 1,
                "prepare may inspect only the one namespace entry charged to this pass"
            );
        }
        assert_eq!(
            tiering::load_shared_segment_catalog_pointer(&config).unwrap(),
            Some(second)
        );

        loop {
            let before = {
                let cursor = storage
                    .coordination
                    .background_catalog_refresh_cursor
                    .lock();
                cursor
                    .writer_publication_cycle
                    .as_ref()
                    .unwrap()
                    .generation_namespace_count
            };
            assert!(!storage
                .advance_bounded_tiered_catalog_publication(false)
                .unwrap());
            let cursor = storage
                .coordination
                .background_catalog_refresh_cursor
                .lock();
            let cycle = cursor.writer_publication_cycle.as_ref().unwrap();
            match cycle.phase {
                BoundedTieredCatalogPublicationPhase::ScanGenerationNamespace => {
                    assert_eq!(cycle.generation_namespace_count, before + 1);
                }
                BoundedTieredCatalogPublicationPhase::OpenGenerationCleanup => {
                    assert_eq!(cycle.generation_namespace_count, before);
                    assert_eq!(cycle.generation_namespace_count, initial_namespace_entries);
                    break;
                }
                phase => panic!("unexpected generation scan phase: {phase:?}"),
            }
        }

        let regular_files = || {
            std::fs::read_dir(&generation_directory)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.file_type().is_ok_and(|kind| kind.is_file())
                        && entry
                            .file_name()
                            .to_str()
                            .and_then(tiering::parse_segment_catalog_generation_file_name)
                            .is_some()
                })
                .count()
        };
        loop {
            let (before_seen, phase) = {
                let cursor = storage
                    .coordination
                    .background_catalog_refresh_cursor
                    .lock();
                let cycle = cursor.writer_publication_cycle.as_ref().unwrap();
                (cycle.generation_cleanup_entries_seen, cycle.phase)
            };
            assert!(matches!(
                phase,
                BoundedTieredCatalogPublicationPhase::OpenGenerationCleanup
                    | BoundedTieredCatalogPublicationPhase::CleanupGenerationNamespace
            ));
            let before_files = regular_files();
            assert!(!storage
                .advance_bounded_tiered_catalog_publication(false)
                .unwrap());
            let after_files = regular_files();
            assert!(
                before_files.saturating_sub(after_files) <= 1,
                "one finite pass may collect at most one generation"
            );
            let cursor = storage
                .coordination
                .background_catalog_refresh_cursor
                .lock();
            let cycle = cursor.writer_publication_cycle.as_ref().unwrap();
            match cycle.phase {
                BoundedTieredCatalogPublicationPhase::CleanupGenerationNamespace => {
                    assert_eq!(cycle.generation_cleanup_entries_seen, before_seen + 1);
                }
                BoundedTieredCatalogPublicationPhase::FinalizeGenerationPreparation => {
                    assert_eq!(cycle.generation_cleanup_entries_seen, before_seen);
                    break;
                }
                phase => panic!("unexpected generation cleanup phase: {phase:?}"),
            }
        }

        assert!(
            !stale.exists(),
            "stale regular generations must be collected"
        );
        assert!(
            tiering::shared_segment_catalog_generation_path(&config, first.generation).exists()
        );
        assert!(
            tiering::shared_segment_catalog_generation_path(&config, second.generation).exists()
        );
        assert!(unknown.exists(), "unknown namespace entries are host-owned");

        let passes = advance_until_bounded_catalog_complete(&storage, &config, Some(second));
        assert!(passes > 1);
        let published = tiering::require_shared_segment_catalog_pointer(&config).unwrap();
        assert!(published.generation > second.generation);
        assert!(
            tiering::shared_segment_catalog_generation_path(&config, first.generation).exists()
        );
        assert!(
            tiering::shared_segment_catalog_generation_path(&config, second.generation).exists()
        );
        assert!(
            tiering::shared_segment_catalog_generation_path(&config, published.generation).exists()
        );
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn finite_writer_catalog_has_exact_byte_n_minus_one_n_boundary() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let entry = inventory(&config, 1).entries()[0].clone();
        let required = one_entry_maximum_bounded_work_bytes(&config, &entry);
        let too_small = storage_with_items(
            temp.path(),
            config.clone(),
            usize::MAX,
            required.saturating_sub(1),
        );
        seed_persisted_catalog_entries(&too_small, &config, 1, 1);
        let err = (0..32)
            .find_map(|_| {
                too_small
                    .advance_bounded_tiered_catalog_publication(false)
                    .err()
            })
            .expect("the N-1 byte limit must reject the largest indivisible publication step");
        assert!(matches!(
            err,
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: BOUNDED_TIERED_CATALOG_PUBLICATION_OPERATION,
                limit,
                required: reported,
            } if limit == required - 1 && reported == required
        ));
        assert!(tiering::load_shared_segment_catalog_pointer(&config)
            .unwrap()
            .is_none());
        assert_eq!(
            too_small
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
        drop(too_small);

        let exact = storage_with_items(temp.path(), config.clone(), usize::MAX, required);
        seed_persisted_catalog_entries(&exact, &config, 1, 1);
        let passes = advance_until_bounded_catalog_complete(&exact, &config, None);
        assert!(passes > 1);
        assert_eq!(
            tiering::require_shared_segment_catalog_pointer(&config)
                .unwrap()
                .entry_count,
            1
        );
        assert_eq!(load_published_generation_entries(&config).len(), 1);
        assert_eq!(
            exact
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn finite_multi_root_transition_staging_has_exact_aggregate_removal_boundary() {
        fn transition(removed_roots: Vec<PathBuf>) -> PersistedCatalogTransition {
            PersistedCatalogTransition {
                visibility_fence: None,
                loaded_segments: Vec::new(),
                removed_roots,
                publication: PersistedCatalogPublication::PersistedState {
                    published_segment_roots: Vec::new(),
                    refresh_tombstones: false,
                },
                registry_catalog_update: None,
            }
        }

        fn identity_staging_bytes(storage: &ChunkStorage, series_id: SeriesId) -> usize {
            let registry = storage.catalog.registry.read();
            let (metric_bytes, label_count, label_text_bytes) =
                registry.decoded_series_key_shape(series_id).unwrap();
            metric_bytes
                .saturating_add(label_text_bytes)
                .saturating_add(label_count.saturating_mul(std::mem::size_of::<crate::Label>()))
                .saturating_add(
                    1usize
                        .saturating_add(label_count.saturating_mul(2))
                        .saturating_mul(64),
                )
                .saturating_mul(FINITE_TRANSITION_REMOVAL_SERIES_METADATA_COPIES)
                .saturating_add(std::mem::size_of::<SeriesId>().saturating_mul(4))
        }

        let temp = TempDir::new().unwrap();
        let lane_path = temp.path().join(NUMERIC_LANE_ROOT);
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            3,
            ChunkStorageOptions {
                retention_enforced: false,
                maintenance_max_items_per_pass: 8,
                maintenance_max_bytes_per_pass: u64::MAX,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        let first_root = lane_path
            .join("segments")
            .join("L0")
            .join("seg-0000000000000001");
        let second_root = lane_path
            .join("segments")
            .join("L0")
            .join("seg-0000000000000002");
        let first_labels = [crate::Label::new("host", "first")];
        let second_labels = [crate::Label::new("host", "s".repeat(8 * 1024))];
        let first_series_id = storage
            .catalog
            .registry
            .read()
            .resolve_or_insert("finite_transition_first", &first_labels)
            .unwrap()
            .series_id;
        let second_series_id = storage
            .catalog
            .registry
            .read()
            .resolve_or_insert("finite_transition_second", &second_labels)
            .unwrap()
            .series_id;
        let manifest = |segment_id| SegmentManifest {
            segment_id,
            level: 0,
            chunk_count: 1,
            point_count: 1,
            series_count: 1,
            min_ts: Some(segment_id as i64),
            max_ts: Some(segment_id as i64),
            wal_highwater: WalHighWatermark::default(),
        };
        {
            let mut persisted = storage.persisted.persisted_index.write();
            for (segment_slot, root, series_id) in [
                (1usize, first_root.clone(), first_series_id),
                (2usize, second_root.clone(), second_series_id),
            ] {
                let mut chunk_refs_by_series = HashMap::new();
                chunk_refs_by_series.insert(series_id, Vec::new());
                persisted.chunk_refs.insert(series_id, Vec::new());
                persisted.segments_by_root.insert(
                    root,
                    crate::engine::storage_engine::state::PersistedSegmentState {
                        segment_slot,
                        lane: SegmentLaneFamily::Numeric,
                        tier: PersistedSegmentTier::Hot,
                        manifest: manifest(segment_slot as u64),
                        time_bucket_postings: None,
                        series_time_summaries: HashMap::new(),
                        chunk_refs_by_series,
                    },
                );
            }
        }
        storage.bump_visibility_state_generation();

        let roots = vec![first_root.clone(), second_root.clone()];
        let staged = transition(roots.clone());
        let representative_root = first_root.as_path();
        let base = super::bounded_remote::modeled_transition_publication_capacity_bytes(
            &staged,
            representative_root,
            None,
        );
        let second_root_auxiliary =
            super::bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(
                &second_root,
            );
        let expected = base
            .saturating_add(
                super::bounded_scan::modeled_removal_bytes(&first_root, &manifest(1))
                    .min(usize::MAX as u64) as usize,
            )
            .saturating_add(identity_staging_bytes(&storage, first_series_id))
            .saturating_add(second_root_auxiliary)
            .saturating_add(
                super::bounded_scan::modeled_removal_bytes(&second_root, &manifest(2))
                    .min(usize::MAX as u64) as usize,
            )
            .saturating_add(identity_staging_bytes(&storage, second_series_id));
        let required = storage.modeled_finite_transition_staging_bytes(&staged);
        assert_eq!(
            required, expected,
            "the transition lease must aggregate every removed root, manifest, and identity",
        );

        let publication = storage.begin_persisted_catalog_publication();
        let err = match publication.publish_transition_with_finite_recovery_budget(
            transition(roots.clone()),
            usize::MAX,
            u64::try_from(required.saturating_sub(1)).unwrap(),
            false,
        ) {
            Ok(_) => panic!("N-1 bytes must reject before either root is removed"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: "finite catalog transition staging",
                limit,
                required: reported,
            } if limit == u64::try_from(required - 1).unwrap()
                && reported == u64::try_from(required).unwrap()
        ));
        assert!(roots.iter().all(|root| storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(root)));

        assert!(matches!(
            publication
                .publish_transition_with_finite_recovery_budget(
                    transition(roots.clone()),
                    usize::MAX,
                    u64::try_from(required).unwrap(),
                    false,
                )
                .unwrap(),
            PersistedCatalogRefreshApply::Applied
        ));
        assert!(roots.iter().all(|root| !storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(root)));
    }

    #[test]
    fn finite_tiered_post_mutation_failure_keeps_registry_reconciliation_sticky() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let storage = storage_with_items(temp.path(), config.clone(), 8, u64::MAX);
        seed_persisted_catalog_entries(&storage, &config, 1, 1);
        let removed_root = storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .first_key_value()
            .unwrap()
            .0
            .clone();
        storage.set_catalog_transition_post_index_mutation_hook(|| {
            Err(TsinkError::Other(
                "injected finite tiered post-mutation failure".to_string(),
            ))
        });

        let publication = storage.begin_persisted_catalog_publication();
        let err = match publication.publish_transition(PersistedCatalogTransition {
            visibility_fence: None,
            loaded_segments: Vec::new(),
            removed_roots: vec![removed_root.clone()],
            publication: PersistedCatalogPublication::PersistedState {
                published_segment_roots: Vec::new(),
                refresh_tombstones: false,
            },
            registry_catalog_update: None,
        }) {
            Ok(_) => panic!("the injected post-mutation failure must be returned"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            TsinkError::Other(message)
                if message == "injected finite tiered post-mutation failure"
        ));
        assert!(!storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(&removed_root));
        assert!(storage
            .coordination
            .bounded_registry_reconciliation_required
            .load(Ordering::Acquire));
        assert!(storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst));
        storage.clear_catalog_transition_post_index_mutation_hook();
        drop(publication);

        for pass in 0..128 {
            if storage
                .advance_bounded_tiered_catalog_publication(false)
                .unwrap()
            {
                assert!(
                    pass > 0,
                    "finite tiered reconciliation should retain a paged continuation",
                );
                break;
            }
            assert!(
                storage
                    .coordination
                    .bounded_registry_reconciliation_required
                    .load(Ordering::Acquire),
                "only exact terminal registry publication may clear the sticky debt",
            );
            assert!(pass < 127, "finite tiered reconciliation did not converge");
        }
        assert!(!storage
            .coordination
            .bounded_registry_reconciliation_required
            .load(Ordering::Acquire));
        assert_eq!(
            tiering::require_shared_segment_catalog_pointer(&config)
                .unwrap()
                .entry_count,
            0,
        );
    }

    #[test]
    fn finite_writer_catalog_visibility_churn_restarts_without_reservation_leak() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let storage = storage_with_items(temp.path(), config.clone(), 4, u64::MAX);
        seed_persisted_catalog_entries(&storage, &config, 1, 2);

        assert!(!storage
            .advance_bounded_tiered_catalog_publication(false)
            .unwrap());
        assert!(!storage
            .advance_bounded_tiered_catalog_publication(false)
            .unwrap());
        let local_stage = BoundedTieredCatalogPublicationCycle::stage_path(
            config.segment_catalog_path.as_deref().unwrap(),
        )
        .unwrap();
        assert!(local_stage.exists());
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            cursor_retained_bytes(&storage).unwrap()
        );

        seed_persisted_catalog_entries(&storage, &config, 3, 1);
        assert!(!storage
            .advance_bounded_tiered_catalog_publication(false)
            .unwrap());
        assert!(
            !local_stage.exists(),
            "a visibility-generation change must discard the prior stage before restarting"
        );
        assert!(tiering::load_shared_segment_catalog_pointer(&config)
            .unwrap()
            .is_none());
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            cursor_retained_bytes(&storage).unwrap()
        );

        advance_until_bounded_catalog_complete(&storage, &config, None);
        assert_eq!(
            tiering::require_shared_segment_catalog_pointer(&config)
                .unwrap()
                .entry_count,
            3
        );
        assert_eq!(load_published_generation_entries(&config).len(), 3);
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn finite_writer_catalog_corruption_and_restart_keep_prior_pointer_until_retry() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let initial_inventory = inventory(&config, 1);
        let replacement_entries = 9;
        let publisher = storage_with_items(temp.path(), config.clone(), 8, u64::MAX);
        publisher
            .publish_segment_inventory(&initial_inventory)
            .unwrap();
        let initial_pointer = tiering::require_shared_segment_catalog_pointer(&config).unwrap();
        seed_persisted_catalog_entries(&publisher, &config, 100, replacement_entries);

        let generation_path = loop {
            assert!(!publisher
                .advance_bounded_tiered_catalog_publication(false)
                .unwrap());
            assert_eq!(
                tiering::require_shared_segment_catalog_pointer(&config).unwrap(),
                initial_pointer
            );
            let path = {
                let cursor = publisher
                    .coordination
                    .background_catalog_refresh_cursor
                    .lock();
                cursor.writer_publication_cycle.as_ref().and_then(|cycle| {
                    (cycle.phase == BoundedTieredCatalogPublicationPhase::GenerationEntries
                        && cycle.generation_entries_written > 0
                        && cycle.generation_entries_written < replacement_entries)
                        .then(|| cycle.generation_path.clone().unwrap())
                })
            };
            if let Some(path) = path {
                break path;
            }
        };
        let mut generation = std::fs::OpenOptions::new()
            .append(true)
            .open(&generation_path)
            .unwrap();
        generation.write_all(b"!").unwrap();
        generation.sync_all().unwrap();
        drop(generation);

        let err = publisher
            .advance_bounded_tiered_catalog_publication(false)
            .unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(_)));
        assert_eq!(
            tiering::require_shared_segment_catalog_pointer(&config).unwrap(),
            initial_pointer
        );
        assert!(!publisher.bounded_tiered_catalog_publication_is_pending());
        assert_eq!(
            publisher
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
        drop(publisher);

        let interrupted = storage_with_items(temp.path(), config.clone(), 8, u64::MAX);
        seed_persisted_catalog_entries(&interrupted, &config, 100, replacement_entries);
        let abandoned_generation = loop {
            assert!(!interrupted
                .advance_bounded_tiered_catalog_publication(false)
                .unwrap());
            assert_eq!(
                tiering::require_shared_segment_catalog_pointer(&config).unwrap(),
                initial_pointer
            );
            let generation = {
                let cursor = interrupted
                    .coordination
                    .background_catalog_refresh_cursor
                    .lock();
                cursor.writer_publication_cycle.as_ref().and_then(|cycle| {
                    (cycle.phase == BoundedTieredCatalogPublicationPhase::GenerationEntries
                        && cycle.generation_entries_written > 0)
                        .then(|| {
                            (
                                cycle.generation.unwrap(),
                                cycle.generation_path.clone().unwrap(),
                            )
                        })
                })
            };
            if let Some(generation) = generation {
                break generation;
            }
        };
        assert!(abandoned_generation.1.exists());
        let crashed_cycle = interrupted
            .coordination
            .background_catalog_refresh_cursor
            .lock()
            .writer_publication_cycle
            .take()
            .unwrap();
        // Model abrupt process termination: neither the cursor's reservation destructor nor the
        // storage's orderly cursor cleanup gets an opportunity to remove unpublished artifacts.
        std::mem::forget(crashed_cycle);
        drop(interrupted);
        assert!(abandoned_generation.1.exists());
        assert_eq!(
            tiering::require_shared_segment_catalog_pointer(&config).unwrap(),
            initial_pointer
        );

        let restarted = storage_with_items(temp.path(), config.clone(), 8, u64::MAX);
        seed_persisted_catalog_entries(&restarted, &config, 100, replacement_entries);
        let passes =
            advance_until_bounded_catalog_complete(&restarted, &config, Some(initial_pointer));
        assert!(passes > 1);
        let replacement_pointer = tiering::require_shared_segment_catalog_pointer(&config).unwrap();
        assert!(replacement_pointer.generation > abandoned_generation.0);
        assert_eq!(
            replacement_pointer.entry_count,
            u64::try_from(replacement_entries).unwrap()
        );
        assert_eq!(
            load_published_generation_entries(&config).len(),
            replacement_entries
        );
        assert!(
            !abandoned_generation.1.exists(),
            "successful retry cleanup must remove the abandoned generation"
        );
        assert_eq!(
            restarted
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn expert_unlimited_catalog_publication_remains_one_shot() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let storage = storage_with_items(temp.path(), config.clone(), usize::MAX, u64::MAX);
        let inventory = inventory(&config, 2);

        assert!(!storage.finite_tiered_catalog_publication_enabled());
        storage.publish_segment_inventory(&inventory).unwrap();
        assert!(!storage.bounded_tiered_catalog_publication_is_pending());
        assert_eq!(
            tiering::require_shared_segment_catalog_pointer(&config)
                .unwrap()
                .entry_count,
            2
        );
        assert_eq!(load_published_generation_entries(&config).len(), 2);
    }

    #[test]
    fn initial_one_shot_publication_removes_only_exact_crash_orphan_stages() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        std::fs::create_dir_all(config.object_store_root.as_path()).unwrap();
        let unrelated = config
            .object_store_root
            .join(".host-owned.tiered-publish-stage");
        std::fs::write(&unrelated, b"host").unwrap();
        let disk_budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let local_target = config.segment_catalog_path.as_deref().unwrap();
        let local_stage = BoundedTieredCatalogPublicationCycle::stage_path(local_target).unwrap();
        let shared_stage = BoundedTieredCatalogPublicationCycle::stage_path(
            &tiering::shared_segment_catalog_path(&config),
        )
        .unwrap();
        append_catalog_stage_bytes(
            &local_stage,
            0,
            b"partial-local",
            Some(&disk_budget),
            crate::DiskCategory::Temporary,
        )
        .unwrap();
        append_catalog_stage_bytes(
            &shared_stage,
            0,
            b"partial-shared",
            Some(&disk_budget),
            crate::DiskCategory::Temporary,
        )
        .unwrap();
        let before_restart = disk_budget.snapshot();
        assert_eq!(before_restart.active_reservations, 0);
        assert_eq!(before_restart.reserved_bytes, 0);
        assert!(local_stage.exists());
        assert!(shared_stage.exists());

        let restarted = storage_with_items_and_disk_budget(
            temp.path(),
            config.clone(),
            usize::MAX,
            u64::MAX,
            Some(Arc::clone(&disk_budget)),
        );
        restarted
            .publish_segment_inventory(&SegmentInventory::from_entries(Vec::new()))
            .unwrap();
        assert!(!local_stage.exists());
        assert!(!shared_stage.exists());
        assert!(
            unrelated.exists(),
            "exact-owned recovery must not glob-delete a similarly suffixed host file"
        );
        assert_eq!(
            restarted
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );

        let before_reconcile = disk_budget.snapshot();
        let reconciled = disk_budget.reconcile().unwrap();
        assert_eq!(before_reconcile.accounted_bytes, reconciled.accounted_bytes);
        assert_eq!(reconciled.active_reservations, 0);
        assert_eq!(reconciled.reserved_bytes, 0);
        assert_eq!(reconciled.reservation_overruns_total, 0);
    }

    #[test]
    fn partial_stage_append_failure_settles_observed_growth_and_exact_cleanup() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let disk_budget =
            crate::LocalDiskBudget::open(temp.path(), crate::LocalDiskLimits::default()).unwrap();
        let storage = storage_with_items_and_disk_budget(
            temp.path(),
            config.clone(),
            4,
            u64::MAX,
            Some(Arc::clone(&disk_budget)),
        );
        let stage = BoundedTieredCatalogPublicationCycle::stage_path(
            &tiering::shared_segment_catalog_path(&config),
        )
        .unwrap();
        let accounted_before = disk_budget.snapshot().accounted_bytes;
        let err = append_catalog_stage_bytes_impl(
            &stage,
            0,
            b"partial-fragment",
            Some(&disk_budget),
            crate::DiskCategory::Temporary,
            Some(7),
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("injected partial catalog staging append failure"));
        assert_eq!(std::fs::metadata(&stage).unwrap().len(), 7);
        let after_failure = disk_budget.snapshot();
        assert_eq!(
            after_failure.accounted_bytes,
            accounted_before.saturating_add(7)
        );
        assert_eq!(after_failure.active_reservations, 0);
        assert_eq!(after_failure.reserved_bytes, 0);

        storage
            .remove_bounded_catalog_artifact(&stage, crate::DiskCategory::Temporary)
            .unwrap();
        assert!(!stage.exists());
        let before_reconcile = disk_budget.snapshot();
        let reconciled = disk_budget.reconcile().unwrap();
        assert_eq!(before_reconcile.accounted_bytes, accounted_before);
        assert_eq!(reconciled.accounted_bytes, accounted_before);
        assert_eq!(reconciled.active_reservations, 0);
        assert_eq!(reconciled.reserved_bytes, 0);
        assert_eq!(reconciled.reservation_overruns_total, 0);
    }

    #[test]
    fn finite_catalog_publication_memory_has_exact_n_minus_one_n_and_n_plus_one_boundaries() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let inventory = inventory(&config, 3);
        let required = modeled_publication_bytes(&config, &inventory);

        let too_small = storage(
            temp.path(),
            config.clone(),
            u64::try_from(required - 1).unwrap(),
        );
        let err = too_small
            .publish_segment_inventory_with_test_admission_hook(&inventory, |_| {})
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: tiering::SEGMENT_CATALOG_PUBLICATION_MEMORY_OPERATION,
                limit,
                required: reported,
            } if limit == u64::try_from(required - 1).unwrap()
                && reported == u64::try_from(required).unwrap()
        ));
        assert!(!tiering::shared_segment_catalog_path(&config).exists());
        assert_eq!(
            too_small
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
        drop(too_small);

        for limit in [required, required + 1] {
            let admitted = storage(temp.path(), config.clone(), limit as u64);
            admitted
                .publish_segment_inventory_with_test_admission_hook(&inventory, |observed| {
                    assert_eq!(observed, required);
                    assert_eq!(
                        admitted
                            .observability_snapshot()
                            .memory
                            .remote_catalog_staging_bytes,
                        required
                    );
                })
                .unwrap();
            assert_eq!(
                admitted
                    .observability_snapshot()
                    .memory
                    .remote_catalog_staging_bytes,
                0
            );
            let legacy = tiering::load_segment_catalog(
                &tiering::shared_segment_catalog_path(&config),
                None,
                None,
                Some(&config),
            )
            .unwrap();
            assert_eq!(legacy.entries().len(), inventory.entries().len());
            assert!(tiering::require_shared_segment_catalog_pointer(&config).is_ok());
            drop(admitted);
        }
    }

    #[test]
    fn catalog_publication_reservation_serializes_concurrent_writer_admission() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let inventory = Arc::new(inventory(&config, 2));
        let required = modeled_publication_bytes(&config, &inventory);
        let storage = Arc::new(storage(temp.path(), config, u64::MAX));
        storage.refresh_memory_usage();
        let used = storage.memory_used_value();
        storage.memory.budget_bytes.store(
            u64::try_from(used.saturating_add(required)).unwrap(),
            Ordering::Release,
        );

        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let publisher = {
            let storage = Arc::clone(&storage);
            let inventory = Arc::clone(&inventory);
            std::thread::spawn(move || {
                storage.publish_segment_inventory_with_test_admission_hook(&inventory, |observed| {
                    admitted_tx.send(observed).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };

        assert_eq!(admitted_rx.recv().unwrap(), required);
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            required
        );
        assert!(matches!(
            storage.reserve_write_transient_memory(1),
            Err(TsinkError::MemoryBudgetExceeded { .. })
        ));
        release_tx.send(()).unwrap();
        publisher.join().unwrap().unwrap();
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn catalog_publication_global_memory_budget_rejects_one_under_and_admits_exact() {
        let temp = TempDir::new().unwrap();
        let config = config(temp.path());
        let inventory = inventory(&config, 2);
        let required = modeled_publication_bytes(&config, &inventory);
        let storage = storage(temp.path(), config.clone(), u64::MAX);
        storage.refresh_memory_usage();
        let used = storage.memory_used_value();

        storage.memory.budget_bytes.store(
            u64::try_from(used.saturating_add(required - 1)).unwrap(),
            Ordering::Release,
        );
        let err = storage.publish_segment_inventory(&inventory).unwrap_err();
        assert!(matches!(
            err,
            TsinkError::MemoryBudgetExceeded {
                budget,
                required: reported,
            } if budget == used.saturating_add(required - 1)
                && reported == used.saturating_add(required)
        ));
        assert!(!tiering::shared_segment_catalog_path(&config).exists());
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );

        storage.memory.budget_bytes.store(
            u64::try_from(used.saturating_add(required)).unwrap(),
            Ordering::Release,
        );
        storage.publish_segment_inventory(&inventory).unwrap();
        assert!(tiering::require_shared_segment_catalog_pointer(&config).is_ok());
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn mirrored_hot_destination_clone_stays_inside_the_admitted_publication_peak() {
        let temp = TempDir::new().unwrap();
        let mut config = config(temp.path());
        config.mirror_hot_segments = true;
        let mut entry = inventory(&config, 1).entries()[0].clone();
        entry.root = temp
            .path()
            .join("local-hot")
            .join("segments")
            .join("L0")
            .join(format!("seg-{:016x}", entry.manifest.segment_id));
        let inventory = SegmentInventory::from_entries(vec![entry]);
        let required = modeled_publication_bytes(&config, &inventory);
        let storage = storage(temp.path(), config.clone(), required as u64);

        storage
            .publish_segment_inventory_with_test_admission_hook(&inventory, |observed| {
                assert_eq!(observed, required);
                assert_eq!(
                    storage
                        .observability_snapshot()
                        .memory
                        .remote_catalog_staging_bytes,
                    required
                );
            })
            .unwrap();
        let shared = tiering::load_segment_catalog(
            &tiering::shared_segment_catalog_path(&config),
            None,
            None,
            Some(&config),
        )
        .unwrap();
        assert_eq!(shared.entries().len(), 1);
        assert!(shared.entries()[0]
            .root
            .starts_with(config.lane_path(SegmentLaneFamily::Numeric, PersistedSegmentTier::Hot)));
        assert_eq!(
            storage
                .observability_snapshot()
                .memory
                .remote_catalog_staging_bytes,
            0
        );
    }

    #[test]
    fn catalog_stage_failures_keep_prior_pointer_release_memory_and_retry_after_restart() {
        for failed_stage in [
            tiering::SegmentCatalogPublishStage::Generation,
            tiering::SegmentCatalogPublishStage::LegacyV2,
            tiering::SegmentCatalogPublishStage::PointerPrePublication,
        ] {
            let temp = TempDir::new().unwrap();
            let config = config(temp.path());
            let initial_inventory = inventory(&config, 1);
            let replacement_inventory = inventory(&config, 2);
            let required = modeled_publication_bytes(&config, &replacement_inventory);
            let publisher_storage = storage(temp.path(), config.clone(), required as u64);
            publisher_storage
                .publish_segment_inventory(&initial_inventory)
                .unwrap();
            let initial_pointer = tiering::require_shared_segment_catalog_pointer(&config).unwrap();

            let err = publisher_storage
                .publish_segment_inventory_with_test_hooks(
                    &replacement_inventory,
                    |observed| {
                        assert_eq!(observed, required);
                        assert_eq!(
                            publisher_storage
                                .observability_snapshot()
                                .memory
                                .remote_catalog_staging_bytes,
                            required
                        );
                    },
                    |stage| {
                        if stage == failed_stage {
                            return Err(TsinkError::Other(
                                "injected catalog publication stage failure".to_string(),
                            ));
                        }
                        Ok(())
                    },
                )
                .unwrap_err();
            assert!(err
                .to_string()
                .contains("injected catalog publication stage failure"));
            assert_eq!(
                tiering::require_shared_segment_catalog_pointer(&config).unwrap(),
                initial_pointer,
                "a failure before pointer publication must leave finite readers on the prior generation"
            );
            assert_eq!(
                publisher_storage
                    .observability_snapshot()
                    .memory
                    .remote_catalog_staging_bytes,
                0
            );
            drop(publisher_storage);

            let restarted = storage(temp.path(), config.clone(), required as u64);
            restarted
                .publish_segment_inventory(&replacement_inventory)
                .unwrap();
            let replacement_pointer =
                tiering::require_shared_segment_catalog_pointer(&config).unwrap();
            assert!(replacement_pointer.generation > initial_pointer.generation);
            let legacy = tiering::load_segment_catalog(
                &tiering::shared_segment_catalog_path(&config),
                None,
                None,
                Some(&config),
            )
            .unwrap();
            assert_eq!(
                legacy.entries().len(),
                replacement_inventory.entries().len()
            );
            assert_eq!(
                restarted
                    .observability_snapshot()
                    .memory
                    .remote_catalog_staging_bytes,
                0
            );
            let generation_files = std::fs::read_dir(
                config
                    .object_store_root
                    .join(tiering::SEGMENT_CATALOG_GENERATION_DIRECTORY_NAME),
            )
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
            .count();
            assert_eq!(
                generation_files, 2,
                "retry cleanup must retain exactly the current generation and its predecessor"
            );
            assert!(tiering::shared_segment_catalog_generation_path(
                &config,
                initial_pointer.generation
            )
            .exists());
            assert!(tiering::shared_segment_catalog_generation_path(
                &config,
                replacement_pointer.generation
            )
            .exists());
        }
    }
}
