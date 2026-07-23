use std::collections::{BTreeMap, BTreeSet};
use std::fs::ReadDir;
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use super::*;
use crate::engine::segment::{
    is_not_found_error, read_segment_manifest, read_segment_manifest_fingerprint,
    segment_validation_error, SegmentManifest, SegmentValidationContext,
    MAX_SEGMENT_MANIFEST_FILE_BYTES,
};

const CATALOG_SCAN_DIRECTORY_OPEN_BYTES: u64 = 4 * 1024;
// This covers the retained path, `DirEntry`/`PathBuf` bookkeeping, and a deliberately generous
// path-component allowance before the entry is classified. A larger path is rejected explicitly.
const CATALOG_SCAN_DIRECTORY_ENTRY_BYTES: u64 = 256 * 1024;
const CATALOG_SCAN_MAX_PATH_BYTES: usize = 256 * 1024;
const CATALOG_SCAN_MANIFEST_INSPECTION_BYTES: u64 = MAX_SEGMENT_MANIFEST_FILE_BYTES as u64;
const CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD: u64 = 512;
const CATALOG_TOMBSTONE_REFRESH_DESCRIPTOR_BYTES: u64 = 16 * 1024;
const CATALOG_SCAN_OPERATION: &str = "unknown-dirty persisted catalog scan";
const CATALOG_APPLY_OPERATION: &str = "unknown-dirty persisted catalog apply";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CatalogScanKey {
    lane: tiering::SegmentLaneFamily,
    level: u8,
    segment_id: u64,
}

#[derive(Debug, Clone)]
struct CatalogScanTarget {
    base_path: PathBuf,
    lane: tiering::SegmentLaneFamily,
    tier: PersistedSegmentTier,
}

#[derive(Debug, Clone)]
struct PendingManifestRead {
    root: PathBuf,
    lane: tiering::SegmentLaneFamily,
    tier: PersistedSegmentTier,
}

enum CatalogRefreshPhase {
    Scanning {
        target_index: usize,
        level: u8,
        reader: Option<ReadDir>,
        pending_manifest: Option<PendingManifestRead>,
    },
    Adding {
        after_key: Option<CatalogScanKey>,
        tombstones_refreshed: bool,
    },
    Removing {
        after_root: Option<PathBuf>,
    },
}

enum PendingCatalogRefreshPage {
    Add {
        key: CatalogScanKey,
        root: PathBuf,
        refresh_tombstones: bool,
    },
    RefreshTombstonesOnly,
    Remove {
        root: PathBuf,
    },
}

struct BackgroundCatalogRefreshCycle {
    expected_visibility_generation: u64,
    targets: Vec<CatalogScanTarget>,
    entries: BTreeMap<CatalogScanKey, SegmentInventoryEntry>,
    final_roots: BTreeSet<PathBuf>,
    retained_bytes: u64,
    observed_namespace_entries: usize,
    phase: CatalogRefreshPhase,
    pending_page: Option<PendingCatalogRefreshPage>,
    invalidated: bool,
}

/// Process-local continuation for a finite non-tiered unknown-dirty refresh.
///
/// Startup always performs strict hydration before this cursor exists, so an interrupted process
/// deliberately discards the partial scan and restarts from the startup-authoritative inventory.
/// No partial scan is represented as a complete catalog.
#[derive(Default)]
pub(in crate::engine::storage_engine) struct BackgroundCatalogRefreshCursor {
    cycle: Option<BackgroundCatalogRefreshCycle>,
}

struct CatalogRefreshPassBudget {
    item_limit: usize,
    byte_limit: u64,
    remaining_items: usize,
    remaining_bytes: u64,
}

impl CatalogRefreshPassBudget {
    fn new(item_limit: usize, byte_limit: u64) -> Self {
        Self {
            item_limit,
            byte_limit,
            remaining_items: item_limit,
            remaining_bytes: byte_limit,
        }
    }

    fn charge(&mut self, operation: &'static str, items: usize, bytes: u64) -> Result<bool> {
        if items > self.item_limit {
            return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                operation,
                item_limit: self.item_limit,
                byte_limit: self.byte_limit,
                selected_items: self.item_limit.saturating_sub(self.remaining_items),
                selected_bytes: self.byte_limit.saturating_sub(self.remaining_bytes),
            });
        }
        if bytes > self.byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation,
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

    fn exhausted(&self) -> bool {
        self.remaining_items == 0 || self.remaining_bytes == 0
    }
}

impl BackgroundCatalogRefreshCycle {
    fn new(storage: &ChunkStorage, visibility_generation: u64) -> Result<Self> {
        let mut targets = Vec::new();
        if let Some(path) = storage.persisted.numeric_lane_path.as_ref() {
            targets.push(CatalogScanTarget {
                base_path: path.clone(),
                lane: tiering::SegmentLaneFamily::Numeric,
                tier: PersistedSegmentTier::Hot,
            });
        }
        if let Some(path) = storage.persisted.blob_lane_path.as_ref() {
            targets.push(CatalogScanTarget {
                base_path: path.clone(),
                lane: tiering::SegmentLaneFamily::Blob,
                tier: PersistedSegmentTier::Hot,
            });
        }
        targets.sort_by(|left, right| {
            (left.lane, left.tier, &left.base_path).cmp(&(right.lane, right.tier, &right.base_path))
        });

        let retained_bytes = targets.iter().fold(0u64, |total, target| {
            total.saturating_add(
                CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD.saturating_add(
                    u64::try_from(target.base_path.as_os_str().as_encoded_bytes().len())
                        .unwrap_or(u64::MAX),
                ),
            )
        });
        let byte_limit = storage.runtime.maintenance_max_bytes_per_pass;
        if retained_bytes > byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: CATALOG_SCAN_OPERATION,
                limit: byte_limit,
                required: retained_bytes,
            });
        }

        Ok(Self {
            expected_visibility_generation: visibility_generation,
            targets,
            entries: BTreeMap::new(),
            final_roots: BTreeSet::new(),
            retained_bytes,
            observed_namespace_entries: 0,
            phase: CatalogRefreshPhase::Scanning {
                target_index: 0,
                level: 0,
                reader: None,
                pending_manifest: None,
            },
            pending_page: None,
            invalidated: false,
        })
    }

    fn modeled_retained_entry_bytes(entry: &SegmentInventoryEntry) -> u64 {
        CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD.saturating_add(
            u64::try_from(entry.root.as_os_str().as_encoded_bytes().len())
                .unwrap_or(u64::MAX)
                .saturating_mul(2),
        )
    }

    fn insert_preferred(&mut self, entry: SegmentInventoryEntry, byte_limit: u64) -> Result<()> {
        let path_bytes = entry.root.as_os_str().as_encoded_bytes().len();
        if path_bytes > CATALOG_SCAN_MAX_PATH_BYTES {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: CATALOG_SCAN_OPERATION,
                limit: CATALOG_SCAN_MAX_PATH_BYTES as u64,
                required: u64::try_from(path_bytes).unwrap_or(u64::MAX),
            });
        }
        let key = CatalogScanKey {
            lane: entry.lane,
            level: entry.manifest.level,
            segment_id: entry.manifest.segment_id,
        };
        let should_replace = self
            .entries
            .get(&key)
            .is_none_or(|current| (entry.tier, &entry.root) > (current.tier, &current.root));
        if !should_replace {
            return Ok(());
        }

        let old_bytes = self
            .entries
            .get(&key)
            .map(Self::modeled_retained_entry_bytes)
            .unwrap_or(0);
        let new_bytes = Self::modeled_retained_entry_bytes(&entry);
        let required = self
            .retained_bytes
            .saturating_sub(old_bytes)
            .saturating_add(new_bytes);
        if required > byte_limit {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: "unknown-dirty persisted catalog retained snapshot",
                limit: byte_limit,
                required,
            });
        }

        if let Some(old) = self.entries.insert(key, entry.clone()) {
            self.final_roots.remove(&old.root);
        }
        self.final_roots.insert(entry.root);
        self.retained_bytes = required;
        Ok(())
    }
}

fn checked_directory(path: &Path, description: &str) -> Result<Option<()>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "{description} is link-like or not a directory: {}",
            path.display()
        )));
    }
    Ok(Some(()))
}

fn open_segment_level(target: &CatalogScanTarget, level: u8) -> Result<Option<ReadDir>> {
    if checked_directory(&target.base_path, "segment base")?.is_none() {
        return Ok(None);
    }
    let segments_root = target.base_path.join("segments");
    if checked_directory(&segments_root, "segment root")?.is_none() {
        return Ok(None);
    }
    let level_root = segments_root.join(format!("L{level}"));
    if checked_directory(&level_root, "segment level root")?.is_none() {
        return Ok(None);
    }
    std::fs::read_dir(&level_root)
        .map(Some)
        .map_err(|source| TsinkError::IoWithPath {
            path: level_root,
            source,
        })
}

fn modeled_segment_source_bytes(entry: &SegmentInventoryEntry) -> Result<u64> {
    let fingerprint = read_segment_manifest_fingerprint(&entry.root).map_err(|err| {
        segment_validation_error(
            &entry.root,
            SegmentValidationContext::RuntimeRefresh,
            &err.to_string(),
        )
    })?;
    if fingerprint.manifest != entry.manifest {
        return Err(segment_validation_error(
            &entry.root,
            SegmentValidationContext::RuntimeRefresh,
            "manifest changed during bounded catalog reconciliation",
        ));
    }
    let manifest_len = std::fs::symlink_metadata(entry.root.join("manifest.bin"))
        .map_err(|source| TsinkError::IoWithPath {
            path: entry.root.join("manifest.bin"),
            source,
        })?
        .len();
    fingerprint
        .files
        .iter()
        .try_fold(manifest_len, |total, file| {
            total
                .checked_add(file.file_len)
                .ok_or(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: CATALOG_APPLY_OPERATION,
                    limit: u64::MAX,
                    required: u64::MAX,
                })
        })
}

impl ChunkStorage {
    pub(super) fn reset_bounded_unknown_dirty_catalog_refresh(&self) {
        self.coordination
            .background_catalog_refresh_cursor
            .lock()
            .cycle = None;
    }

    fn scan_unknown_dirty_catalog_page(
        &self,
        cycle: &mut BackgroundCatalogRefreshCycle,
        budget: &mut CatalogRefreshPassBudget,
    ) -> Result<bool> {
        loop {
            let pending = match &cycle.phase {
                CatalogRefreshPhase::Scanning {
                    pending_manifest, ..
                } => pending_manifest.clone(),
                _ => return Ok(true),
            };
            if let Some(pending) = pending {
                if !budget.charge(
                    CATALOG_SCAN_OPERATION,
                    1,
                    CATALOG_SCAN_MANIFEST_INSPECTION_BYTES,
                )? {
                    return Ok(false);
                }
                let manifest = match read_segment_manifest(&pending.root) {
                    Ok(manifest) => manifest,
                    Err(err) if is_not_found_error(&err) => {
                        let CatalogRefreshPhase::Scanning {
                            pending_manifest, ..
                        } = &mut cycle.phase
                        else {
                            unreachable!("scan phase changed while reading a manifest");
                        };
                        *pending_manifest = None;
                        continue;
                    }
                    Err(TsinkError::DataCorruption(message)) => {
                        return Err(segment_validation_error(
                            &pending.root,
                            SegmentValidationContext::RuntimeRefresh,
                            &message,
                        ));
                    }
                    Err(err) => return Err(err),
                };
                let entry = SegmentInventoryEntry {
                    lane: pending.lane,
                    tier: pending.tier,
                    root: pending.root,
                    manifest,
                };
                cycle.insert_preferred(entry, budget.byte_limit)?;
                let CatalogRefreshPhase::Scanning {
                    pending_manifest, ..
                } = &mut cycle.phase
                else {
                    unreachable!("scan phase changed while retaining a manifest");
                };
                *pending_manifest = None;
                continue;
            }

            let (target_index, level, reader_is_none) = match &cycle.phase {
                CatalogRefreshPhase::Scanning {
                    target_index,
                    level,
                    reader,
                    ..
                } => (*target_index, *level, reader.is_none()),
                _ => return Ok(true),
            };
            if target_index >= cycle.targets.len() {
                cycle.phase = CatalogRefreshPhase::Adding {
                    after_key: None,
                    tombstones_refreshed: false,
                };
                return Ok(true);
            }

            if reader_is_none {
                if !budget.charge(CATALOG_SCAN_OPERATION, 1, CATALOG_SCAN_DIRECTORY_OPEN_BYTES)? {
                    return Ok(false);
                }
                let opened = open_segment_level(&cycle.targets[target_index], level)?;
                let CatalogRefreshPhase::Scanning {
                    target_index,
                    level,
                    reader,
                    ..
                } = &mut cycle.phase
                else {
                    unreachable!("scan phase changed while opening a level");
                };
                *reader = opened;
                if reader.is_none() {
                    if *level == 2 {
                        *target_index = target_index.saturating_add(1);
                        *level = 0;
                    } else {
                        *level += 1;
                    }
                    continue;
                }
            }

            if !budget.charge(
                CATALOG_SCAN_OPERATION,
                1,
                CATALOG_SCAN_DIRECTORY_ENTRY_BYTES,
            )? {
                return Ok(false);
            }
            let level_root = cycle.targets[target_index]
                .base_path
                .join("segments")
                .join(format!("L{level}"));
            let next = {
                let CatalogRefreshPhase::Scanning { reader, .. } = &mut cycle.phase else {
                    unreachable!("scan phase changed before reading a directory entry");
                };
                reader.as_mut().expect("reader initialized above").next()
            };
            let Some(next) = next else {
                let CatalogRefreshPhase::Scanning {
                    target_index,
                    level,
                    reader,
                    ..
                } = &mut cycle.phase
                else {
                    unreachable!("scan phase changed at a terminal directory probe");
                };
                *reader = None;
                if *level == 2 {
                    *target_index = target_index.saturating_add(1);
                    *level = 0;
                } else {
                    *level += 1;
                }
                continue;
            };
            let entry = next.map_err(|source| TsinkError::IoWithPath {
                path: level_root,
                source,
            })?;
            cycle.observed_namespace_entries = cycle.observed_namespace_entries.saturating_add(1);
            if cycle.observed_namespace_entries
                > crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES
            {
                return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                    operation: CATALOG_SCAN_OPERATION,
                    limit: crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
                    required: cycle.observed_namespace_entries,
                });
            }

            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(TsinkError::IoWithPath {
                        path: entry.path(),
                        source,
                    });
                }
            };
            if !file_type.is_dir() || !entry.file_name().as_encoded_bytes().starts_with(b"seg-") {
                continue;
            }
            let root = entry.path();
            let root_bytes = root.as_os_str().as_encoded_bytes().len();
            if root_bytes > CATALOG_SCAN_MAX_PATH_BYTES {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: CATALOG_SCAN_OPERATION,
                    limit: CATALOG_SCAN_MAX_PATH_BYTES as u64,
                    required: u64::try_from(root_bytes).unwrap_or(u64::MAX),
                });
            }
            let target = &cycle.targets[target_index];
            let pending = PendingManifestRead {
                root,
                lane: target.lane,
                tier: target.tier,
            };
            let CatalogRefreshPhase::Scanning {
                pending_manifest, ..
            } = &mut cycle.phase
            else {
                unreachable!("scan phase changed before deferring a manifest");
            };
            *pending_manifest = Some(pending);
        }
    }

    fn apply_pending_bounded_catalog_page(
        &self,
        cycle: &mut BackgroundCatalogRefreshCycle,
        budget: &mut CatalogRefreshPassBudget,
    ) -> Result<bool> {
        let Some(pending) = cycle.pending_page.as_ref() else {
            return Ok(true);
        };
        let transition = match pending {
            PendingCatalogRefreshPage::Add {
                key,
                root,
                refresh_tombstones,
            } => {
                let entry = cycle
                    .entries
                    .get(key)
                    .filter(|entry| entry.root == *root)
                    .cloned()
                    .ok_or_else(|| {
                        TsinkError::DataCorruption(format!(
                            "bounded catalog add cursor lost scanned root {}",
                            root.display()
                        ))
                    })?;
                let descriptor_bytes =
                    BackgroundCatalogRefreshCycle::modeled_retained_entry_bytes(&entry);
                if !budget.charge(CATALOG_APPLY_OPERATION, 1, descriptor_bytes)? {
                    return Ok(false);
                }
                if !budget.charge(
                    CATALOG_APPLY_OPERATION,
                    0,
                    CATALOG_SCAN_MANIFEST_INSPECTION_BYTES,
                )? {
                    return Ok(false);
                }
                let source_bytes = modeled_segment_source_bytes(&entry)?;
                let required = descriptor_bytes
                    .saturating_add(CATALOG_SCAN_MANIFEST_INSPECTION_BYTES)
                    .saturating_add(source_bytes);
                if required > budget.byte_limit {
                    return Err(TsinkError::MaintenanceWorkItemTooLarge {
                        operation: CATALOG_APPLY_OPERATION,
                        limit: budget.byte_limit,
                        required,
                    });
                }
                if !budget.charge(CATALOG_APPLY_OPERATION, 0, source_bytes)? {
                    return Ok(false);
                }

                let already_visible = self
                    .persisted
                    .persisted_index
                    .read()
                    .segments_by_root
                    .contains_key(root);
                let loaded_segments = if already_visible {
                    Vec::new()
                } else {
                    vec![Self::load_segment_index_for_runtime_refresh(root)?]
                };
                let added_roots = vec![root.clone()];
                let registry_catalog_delta =
                    self.persisted_registry_catalog_delta_for_root_changes(&added_roots, &[])?;
                PersistedCatalogTransition {
                    visibility_fence: Some(PersistedCatalogVisibilityFence {
                        visibility_generation: cycle.expected_visibility_generation,
                    }),
                    loaded_segments,
                    removed_roots: Vec::new(),
                    publication: PersistedCatalogPublication::PersistedState {
                        published_segment_roots: added_roots,
                        refresh_tombstones: *refresh_tombstones,
                    },
                    registry_catalog_update: Some(
                        registry_catalog::PersistedRegistryCatalogUpdate::Delta(
                            registry_catalog_delta,
                        ),
                    ),
                }
            }
            PendingCatalogRefreshPage::RefreshTombstonesOnly => {
                if !budget.charge(
                    CATALOG_APPLY_OPERATION,
                    1,
                    CATALOG_TOMBSTONE_REFRESH_DESCRIPTOR_BYTES,
                )? {
                    return Ok(false);
                }
                PersistedCatalogTransition {
                    visibility_fence: Some(PersistedCatalogVisibilityFence {
                        visibility_generation: cycle.expected_visibility_generation,
                    }),
                    loaded_segments: Vec::new(),
                    removed_roots: Vec::new(),
                    publication: PersistedCatalogPublication::PersistedState {
                        published_segment_roots: Vec::new(),
                        refresh_tombstones: true,
                    },
                    registry_catalog_update: None,
                }
            }
            PendingCatalogRefreshPage::Remove { root } => {
                let state = self
                    .persisted
                    .persisted_index
                    .read()
                    .segments_by_root
                    .get(root)
                    .map(|state| state.manifest.clone());
                let modeled_bytes = state
                    .as_ref()
                    .map_or(CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD, |manifest| {
                        modeled_removal_bytes(root, manifest)
                    });
                if !budget.charge(CATALOG_APPLY_OPERATION, 1, modeled_bytes)? {
                    return Ok(false);
                }
                let removed_roots = vec![root.clone()];
                let registry_catalog_delta =
                    self.persisted_registry_catalog_delta_for_root_changes(&[], &removed_roots)?;
                PersistedCatalogTransition {
                    visibility_fence: Some(PersistedCatalogVisibilityFence {
                        visibility_generation: cycle.expected_visibility_generation,
                    }),
                    loaded_segments: Vec::new(),
                    removed_roots,
                    publication: PersistedCatalogPublication::PersistedState {
                        published_segment_roots: Vec::new(),
                        refresh_tombstones: false,
                    },
                    registry_catalog_update: Some(
                        registry_catalog::PersistedRegistryCatalogUpdate::Delta(
                            registry_catalog_delta,
                        ),
                    ),
                }
            }
        };

        let publication = self.begin_persisted_catalog_publication();
        let result = publication.publish_transition(transition);
        drop(publication);
        let current_generation = self.visibility_state_generation();
        match result {
            Ok(PersistedCatalogRefreshApply::Applied) => {
                match cycle
                    .pending_page
                    .take()
                    .expect("pending page checked above")
                {
                    PendingCatalogRefreshPage::Add {
                        key,
                        refresh_tombstones,
                        ..
                    } => {
                        let CatalogRefreshPhase::Adding {
                            after_key,
                            tombstones_refreshed,
                        } = &mut cycle.phase
                        else {
                            unreachable!("add page must retain add phase");
                        };
                        *after_key = Some(key);
                        *tombstones_refreshed |= refresh_tombstones;
                    }
                    PendingCatalogRefreshPage::RefreshTombstonesOnly => {
                        let CatalogRefreshPhase::Adding {
                            tombstones_refreshed,
                            ..
                        } = &mut cycle.phase
                        else {
                            unreachable!("tombstone page must retain add phase");
                        };
                        *tombstones_refreshed = true;
                    }
                    PendingCatalogRefreshPage::Remove { root } => {
                        let CatalogRefreshPhase::Removing { after_root } = &mut cycle.phase else {
                            unreachable!("remove page must retain remove phase");
                        };
                        *after_root = Some(root);
                    }
                }
                cycle.expected_visibility_generation = current_generation;
                Ok(true)
            }
            Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState) => {
                cycle.expected_visibility_generation = current_generation;
                cycle.invalidated = true;
                Ok(false)
            }
            Err(err) => {
                // Publication can fail after mutating the visible index. Retain the exact page
                // intent and rebase only its fence so retry finishes the same sidecar/root delta.
                cycle.expected_visibility_generation = current_generation;
                Err(err)
            }
        }
    }

    fn advance_bounded_catalog_additions(
        &self,
        cycle: &mut BackgroundCatalogRefreshCycle,
        budget: &mut CatalogRefreshPassBudget,
    ) -> Result<bool> {
        if cycle.pending_page.is_some() {
            return self.apply_pending_bounded_catalog_page(cycle, budget);
        }
        let CatalogRefreshPhase::Adding {
            after_key,
            tombstones_refreshed,
        } = &cycle.phase
        else {
            return Ok(true);
        };
        let start = after_key.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
        let next = cycle
            .entries
            .range((start, Bound::Unbounded))
            .next()
            .map(|(key, entry)| (key.clone(), entry.root.clone()));
        match next {
            Some((key, root)) => {
                cycle.pending_page = Some(PendingCatalogRefreshPage::Add {
                    key,
                    root,
                    refresh_tombstones: !*tombstones_refreshed,
                });
                self.apply_pending_bounded_catalog_page(cycle, budget)
            }
            None if !*tombstones_refreshed => {
                cycle.pending_page = Some(PendingCatalogRefreshPage::RefreshTombstonesOnly);
                self.apply_pending_bounded_catalog_page(cycle, budget)
            }
            None => {
                cycle.phase = CatalogRefreshPhase::Removing { after_root: None };
                Ok(true)
            }
        }
    }

    fn advance_bounded_catalog_removals(
        &self,
        cycle: &mut BackgroundCatalogRefreshCycle,
        budget: &mut CatalogRefreshPassBudget,
    ) -> Result<Option<bool>> {
        if cycle.pending_page.is_some() {
            return self
                .apply_pending_bounded_catalog_page(cycle, budget)
                .map(Some);
        }
        let CatalogRefreshPhase::Removing { after_root } = &cycle.phase else {
            return Ok(Some(true));
        };
        let start = after_root
            .as_ref()
            .map_or(Bound::Unbounded, Bound::Excluded);
        let next = self
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .range::<PathBuf, _>((start, Bound::Unbounded))
            .next()
            .map(|(root, state)| (root.clone(), state.manifest.clone()));
        let Some((root, manifest)) = next else {
            return Ok(None);
        };
        if cycle.final_roots.contains(&root) {
            let modeled_bytes = modeled_removal_bytes(&root, &manifest);
            if !budget.charge(CATALOG_APPLY_OPERATION, 1, modeled_bytes)? {
                return Ok(Some(false));
            }
            let CatalogRefreshPhase::Removing { after_root } = &mut cycle.phase else {
                unreachable!("removal phase checked above");
            };
            *after_root = Some(root);
            return Ok(Some(true));
        }

        cycle.pending_page = Some(PendingCatalogRefreshPage::Remove { root });
        self.apply_pending_bounded_catalog_page(cycle, budget)
            .map(Some)
    }

    /// Runs at most one finite maintenance work envelope.
    ///
    /// Returns `true` only after both terminal probes complete and the scanned snapshot has been
    /// reconciled. Partial pages are exact deltas and intentionally leave the dirty bit set.
    pub(super) fn refresh_unknown_dirty_catalog_bounded(&self) -> Result<bool> {
        let mut budget = CatalogRefreshPassBudget::new(
            self.runtime.maintenance_max_items_per_pass,
            self.runtime.maintenance_max_bytes_per_pass,
        );
        let mut cursor = self.coordination.background_catalog_refresh_cursor.lock();
        if cursor.cycle.is_none() {
            let visibility_generation = self.visibility_state_generation();
            cursor.cycle = Some(BackgroundCatalogRefreshCycle::new(
                self,
                visibility_generation,
            )?);
            #[cfg(test)]
            self.catalog_refresh_context()
                .invoke_full_inventory_scan_hook();
        }

        loop {
            let cycle = cursor
                .cycle
                .as_mut()
                .expect("bounded catalog cycle initialized above");
            if self.visibility_state_generation() != cycle.expected_visibility_generation {
                cursor.cycle = None;
                return Ok(false);
            }

            let step = match cycle.phase {
                CatalogRefreshPhase::Scanning { .. } => self
                    .scan_unknown_dirty_catalog_page(cycle, &mut budget)
                    .map(Some),
                CatalogRefreshPhase::Adding { .. } => self
                    .advance_bounded_catalog_additions(cycle, &mut budget)
                    .map(Some),
                CatalogRefreshPhase::Removing { .. } => {
                    self.advance_bounded_catalog_removals(cycle, &mut budget)
                }
            };
            let step = match step {
                Ok(step) => step,
                Err(err @ TsinkError::MaintenanceNamespaceLimitExceeded { .. }) => {
                    cursor.cycle = None;
                    return Err(err);
                }
                Err(err) => return Err(err),
            };
            if cycle.invalidated {
                cursor.cycle = None;
                return Ok(false);
            }
            match step {
                None => {
                    cursor.cycle = None;
                    return Ok(true);
                }
                Some(false) => return Ok(false),
                Some(true) if budget.exhausted() => return Ok(false),
                Some(true) => {}
            }
        }
    }
}

fn modeled_removal_bytes(root: &Path, manifest: &SegmentManifest) -> u64 {
    let root_bytes = u64::try_from(root.as_os_str().as_encoded_bytes().len()).unwrap_or(u64::MAX);
    let chunk_bytes = u64::try_from(manifest.chunk_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(1024);
    let series_bytes = u64::try_from(manifest.series_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(4096);
    CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD
        .saturating_add(root_bytes)
        .saturating_add(chunk_bytes)
        .saturating_add(series_bytes)
}
