use std::collections::BTreeMap;
use std::mem;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use super::super::super::tiering::{
    self, SegmentCatalogGenerationReadCursor, SegmentCatalogPointer, SegmentInventoryEntry,
    SEGMENT_CATALOG_MAX_ENTRIES, SEGMENT_CATALOG_MAX_GENERATION_BYTES,
    SEGMENT_CATALOG_POINTER_BYTES,
};
use super::bounded_scan::{
    modeled_removal_bytes, modeled_segment_source_bytes, CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD,
    CATALOG_SCAN_MANIFEST_INSPECTION_BYTES,
};
use super::bounded_tombstones::{
    BoundedRemoteTombstoneRefreshCycle, BoundedRemoteTombstoneRefreshOutcome,
};
use super::*;

const REMOTE_CATALOG_SCAN_OPERATION: &str = "finite remote segment catalog validation";
const REMOTE_CATALOG_APPLY_OPERATION: &str = "finite remote segment catalog apply";
const REMOTE_CATALOG_MAX_ROOT_BYTES: usize = 256 * 1024;
const REMOTE_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES: usize = 64;
const REMOTE_CATALOG_MAX_RETAINED_BYTES: u64 = SEGMENT_CATALOG_MAX_GENERATION_BYTES
    + SEGMENT_CATALOG_MAX_ENTRIES as u64 * CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD;
// One page can simultaneously retain the cursor/root map, the loaded segment, transition vectors,
// registry-catalog delta, inventory-delta guard, accounting scopes, and the new visible state.
// The proportional terms below account for segment content; this base dominates collection nodes,
// vector headers, locks/guards, and allocator slack for the one-root publication.
const REMOTE_CATALOG_APPLY_FIXED_STAGING_BYTES: usize = 64 * 1024;
// PathBuf does not expose allocation capacity. Count every root-bearing page representation plus
// two inventory before/after images, and give each clone the same allocator allowance as cursors.
const REMOTE_CATALOG_APPLY_ROOT_PATH_COPIES: usize = 12;
// Publication can hold the loaded definitions, the deduplicated series vector, accounting-scope
// keys, registry keys/postings, and merged-postings keys at the same time.
const REMOTE_CATALOG_APPLY_SERIES_METADATA_COPIES: usize = 6;
// Chunk-index entries are transformed into per-segment and global persisted refs while the loaded
// vector is still live. Four copies dominates those vectors and their temporary sort state.
const REMOTE_CATALOG_APPLY_CHUNK_METADATA_COPIES: usize = 4;
// Segment postings coexist with merged-postings reconstruction and accounting snapshots.
const REMOTE_CATALOG_APPLY_POSTINGS_COPIES: usize = 3;

enum BoundedRemoteCatalogRefreshPhase {
    Scanning,
    ValidatePointerBeforeApply,
    Tombstones,
    Adding { after_root: Option<PathBuf> },
    Removing { after_root: Option<PathBuf> },
    Terminal,
}

#[derive(Clone)]
enum PendingRemoteCatalogRefreshPage {
    Add { root: PathBuf },
    Remove { root: PathBuf },
}

#[derive(Clone, Copy)]
struct RemoteCatalogApplyPreflight {
    already_visible: bool,
    source_bytes: u64,
    staging_bytes: usize,
    work_bytes: u64,
}

pub(super) struct BoundedRemoteCatalogRefreshCycle {
    pointer: SegmentCatalogPointer,
    generation_path: PathBuf,
    expected_visibility_generation: u64,
    reader: SegmentCatalogGenerationReadCursor,
    entries_by_root: BTreeMap<PathBuf, SegmentInventoryEntry>,
    entries_retained_bytes: u64,
    memory_reservation: RemoteCatalogMemoryReservation,
    phase: BoundedRemoteCatalogRefreshPhase,
    tombstone_cycle: Option<BoundedRemoteTombstoneRefreshCycle>,
    pending_page: Option<PendingRemoteCatalogRefreshPage>,
    /// Once publication has been entered, an error may have happened after the live index was
    /// updated but before its exact registry-catalog delta was durable. Preserve that page across
    /// error backoff until the same intent either applies or is invalidated by a newer visibility
    /// generation.
    publication_retry_pending: bool,
    invalidated: bool,
}

impl BoundedRemoteCatalogRefreshCycle {
    fn new(
        storage: &ChunkStorage,
        config: &super::super::config::TieredStorageConfig,
        pointer: SegmentCatalogPointer,
        visibility_generation: u64,
    ) -> Result<Self> {
        let object_store_root_bytes = config
            .object_store_root
            .as_os_str()
            .as_encoded_bytes()
            .len();
        let generation_directory_bytes = object_store_root_bytes
            .saturating_add("/segment_catalog.d".len())
            .saturating_add(REMOTE_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES);
        let generation_file_name_bytes = "catalog-0000000000000000.bin"
            .len()
            .saturating_add(REMOTE_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES);
        let generation_path_bytes = object_store_root_bytes
            .saturating_add("/segment_catalog.d/catalog-0000000000000000.bin".len())
            .saturating_add(REMOTE_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES);
        let construction_peak_bytes = std::mem::size_of::<Self>()
            .saturating_add(generation_directory_bytes)
            .saturating_add(generation_file_name_bytes)
            .saturating_add(generation_path_bytes);
        let memory_reservation =
            storage.remote_catalog_memory_reservation(construction_peak_bytes)?;
        let mut cycle = Self {
            pointer,
            generation_path: tiering::shared_segment_catalog_generation_path(
                config,
                pointer.generation,
            ),
            expected_visibility_generation: visibility_generation,
            reader: SegmentCatalogGenerationReadCursor::new(pointer),
            entries_by_root: BTreeMap::new(),
            entries_retained_bytes: 0,
            memory_reservation,
            phase: BoundedRemoteCatalogRefreshPhase::Scanning,
            tombstone_cycle: None,
            pending_page: None,
            publication_retry_pending: false,
            invalidated: false,
        };
        debug_assert!(
            cycle.modeled_retained_bytes() <= cycle.memory_reservation.reserved_bytes(),
            "remote catalog cycle construction preflight must cover its retained state"
        );
        cycle.restore_retained_memory_reservation(storage);
        Ok(cycle)
    }

    fn modeled_retained_entry_bytes(entry: &SegmentInventoryEntry) -> u64 {
        CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD.saturating_add(
            u64::try_from(entry.root.as_os_str().as_encoded_bytes().len())
                .unwrap_or(u64::MAX)
                .saturating_mul(2),
        )
    }

    fn modeled_cursor_root_bytes(root: &Path) -> usize {
        root.as_os_str()
            .as_encoded_bytes()
            .len()
            .saturating_add(REMOTE_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES)
    }

    pub(super) fn modeled_apply_auxiliary_bytes(root: &Path) -> usize {
        REMOTE_CATALOG_APPLY_FIXED_STAGING_BYTES
            .saturating_add(
                Self::modeled_cursor_root_bytes(root)
                    .saturating_mul(REMOTE_CATALOG_APPLY_ROOT_PATH_COPIES),
            )
            .saturating_add(
                mem::size_of::<PathBuf>().saturating_mul(REMOTE_CATALOG_APPLY_ROOT_PATH_COPIES),
            )
            .saturating_add(mem::size_of::<SegmentInventoryEntry>().saturating_mul(2))
            .saturating_add(mem::size_of::<PersistedCatalogTransition>())
    }

    pub(super) fn modeled_apply_transition_descriptor_bytes(root: &Path) -> usize {
        Self::modeled_cursor_root_bytes(root)
            .saturating_mul(3)
            .saturating_add(mem::size_of::<PathBuf>().saturating_mul(3))
            .saturating_add(mem::size_of::<IndexedSegment>())
            .saturating_add(mem::size_of::<
                registry_catalog::PersistedRegistryCatalogSource,
            >())
            .saturating_add(mem::size_of::<
                registry_catalog::PersistedRegistryCatalogEntryKey,
            >())
    }

    fn modeled_apply_inspection_bytes(&self) -> Result<usize> {
        match self.pending_page.as_ref() {
            Some(PendingRemoteCatalogRefreshPage::Add { root }) => {
                let entry = self.entries_by_root.get(root).ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "finite remote catalog add cursor lost staged root {}",
                        root.display()
                    ))
                })?;
                Ok(Self::modeled_apply_auxiliary_bytes(root).saturating_add(
                    Self::modeled_retained_entry_bytes(entry).min(usize::MAX as u64) as usize,
                ))
            }
            Some(PendingRemoteCatalogRefreshPage::Remove { root }) => {
                Ok(Self::modeled_apply_auxiliary_bytes(root))
            }
            None => Ok(0),
        }
    }

    fn modeled_retained_bytes(&self) -> usize {
        let base = std::mem::size_of::<Self>()
            .saturating_add(self.generation_path.as_os_str().as_encoded_bytes().len())
            .saturating_add(REMOTE_CATALOG_PATH_ALLOCATION_ALLOWANCE_BYTES);
        let cursor_root_bytes = match &self.phase {
            BoundedRemoteCatalogRefreshPhase::Adding {
                after_root: Some(root),
            }
            | BoundedRemoteCatalogRefreshPhase::Removing {
                after_root: Some(root),
            } => Self::modeled_cursor_root_bytes(root),
            _ => 0,
        };
        let pending_root_bytes = match &self.pending_page {
            Some(PendingRemoteCatalogRefreshPage::Add { root })
            | Some(PendingRemoteCatalogRefreshPage::Remove { root }) => {
                Self::modeled_cursor_root_bytes(root)
            }
            None => 0,
        };
        base.saturating_add(self.entries_retained_bytes.min(usize::MAX as u64) as usize)
            .saturating_add(cursor_root_bytes)
            .saturating_add(pending_root_bytes)
    }

    fn resize_memory_reservation(
        &mut self,
        storage: &ChunkStorage,
        requested_bytes: usize,
    ) -> Result<()> {
        storage
            .resize_remote_catalog_memory_reservation(&mut self.memory_reservation, requested_bytes)
    }

    fn restore_retained_memory_reservation(&mut self, storage: &ChunkStorage) {
        let retained = self.modeled_retained_bytes();
        storage
            .resize_remote_catalog_memory_reservation(&mut self.memory_reservation, retained)
            .expect("shrinking an admitted remote catalog reservation cannot fail");
    }

    fn insert_validated_entry(
        &mut self,
        storage: &ChunkStorage,
        entry: SegmentInventoryEntry,
    ) -> Result<()> {
        let root_bytes = entry.root.as_os_str().as_encoded_bytes().len();
        if root_bytes > REMOTE_CATALOG_MAX_ROOT_BYTES {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_CATALOG_SCAN_OPERATION,
                limit: REMOTE_CATALOG_MAX_ROOT_BYTES as u64,
                required: u64::try_from(root_bytes).unwrap_or(u64::MAX),
            });
        }
        if self.entries_by_root.contains_key(&entry.root) {
            return Err(TsinkError::DataCorruption(format!(
                "segment catalog v3 generation contains duplicate canonical root {}",
                entry.root.display()
            )));
        }
        let retained = Self::modeled_retained_entry_bytes(&entry);
        let required = self.entries_retained_bytes.saturating_add(retained);
        if required > REMOTE_CATALOG_MAX_RETAINED_BYTES {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: REMOTE_CATALOG_SCAN_OPERATION,
                limit: REMOTE_CATALOG_MAX_RETAINED_BYTES,
                required,
            });
        }
        let requested = self
            .modeled_retained_bytes()
            .saturating_add(retained.min(usize::MAX as u64) as usize)
            .max(self.memory_reservation.reserved_bytes());
        self.resize_memory_reservation(storage, requested)?;
        self.entries_by_root.insert(entry.root.clone(), entry);
        self.entries_retained_bytes = required;
        Ok(())
    }

    fn release_retained_tombstone_bytes(&mut self, storage: &ChunkStorage) {
        if let Some(tombstone_cycle) = self.tombstone_cycle.as_mut() {
            tombstone_cycle.release_retained_bytes(storage);
        }
    }
}

fn modeled_string_capacity_bytes(value: &String) -> usize {
    if value.capacity() == 0 {
        0
    } else {
        value.capacity().saturating_add(64)
    }
}

fn modeled_persisted_series_capacity_bytes(
    series: &crate::engine::segment::PersistedSeries,
) -> usize {
    modeled_string_capacity_bytes(&series.metric)
        .saturating_add(
            series
                .labels
                .capacity()
                .saturating_mul(mem::size_of::<crate::Label>()),
        )
        .saturating_add(series.labels.iter().fold(0usize, |bytes, label| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(&label.name))
                .saturating_add(modeled_string_capacity_bytes(&label.value))
        }))
}

fn modeled_indexed_segment_publication_capacity_bytes(segment: &IndexedSegment) -> usize {
    let series_vector_bytes = segment
        .series
        .capacity()
        .saturating_mul(mem::size_of::<crate::engine::segment::PersistedSeries>());
    let series_payload_bytes = segment.series.iter().fold(0usize, |bytes, series| {
        bytes.saturating_add(modeled_persisted_series_capacity_bytes(series))
    });
    let series_metadata_bytes = series_vector_bytes.saturating_add(series_payload_bytes);
    let chunk_metadata_bytes = segment
        .chunk_index
        .entries
        .capacity()
        .saturating_mul(mem::size_of::<crate::engine::index::ChunkIndexEntry>());
    let postings_bytes = segment.postings.memory_usage_bytes();
    mem::size_of::<IndexedSegment>()
        .saturating_add(BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(
            &segment.root,
        ))
        .saturating_add(segment.chunks_mmap.len())
        .saturating_add(
            series_metadata_bytes.saturating_mul(REMOTE_CATALOG_APPLY_SERIES_METADATA_COPIES),
        )
        .saturating_add(
            chunk_metadata_bytes.saturating_mul(REMOTE_CATALOG_APPLY_CHUNK_METADATA_COPIES),
        )
        .saturating_add(postings_bytes.saturating_mul(REMOTE_CATALOG_APPLY_POSTINGS_COPIES))
}

fn modeled_root_vector_capacity_bytes(roots: &[PathBuf], capacity: usize) -> usize {
    capacity
        .saturating_mul(mem::size_of::<PathBuf>())
        .saturating_add(roots.iter().fold(0usize, |bytes, root| {
            bytes.saturating_add(BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(
                root,
            ))
        }))
}

fn modeled_registry_catalog_update_capacity_bytes(
    update: &registry_catalog::PersistedRegistryCatalogUpdate,
) -> usize {
    match update {
        registry_catalog::PersistedRegistryCatalogUpdate::Complete(sources) => sources
            .capacity()
            .saturating_mul(mem::size_of::<
                registry_catalog::PersistedRegistryCatalogSource,
            >())
            .saturating_add(sources.iter().fold(0usize, |bytes, source| {
                bytes.saturating_add(BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(
                    &source.root,
                ))
            })),
        registry_catalog::PersistedRegistryCatalogUpdate::Delta(delta) => delta
            .added
            .capacity()
            .saturating_mul(mem::size_of::<
                registry_catalog::PersistedRegistryCatalogSource,
            >())
            .saturating_add(delta.removed.capacity().saturating_mul(mem::size_of::<
                registry_catalog::PersistedRegistryCatalogEntryKey,
            >()))
            .saturating_add(delta.added.iter().fold(0usize, |bytes, source| {
                bytes.saturating_add(BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(
                    &source.root,
                ))
            })),
    }
}

pub(super) fn modeled_transition_publication_capacity_bytes(
    transition: &PersistedCatalogTransition,
    root: &Path,
    manifest: Option<&crate::engine::segment::SegmentManifest>,
) -> usize {
    let loaded_segments = transition
        .loaded_segments
        .capacity()
        .saturating_mul(mem::size_of::<IndexedSegment>())
        .saturating_add(
            transition
                .loaded_segments
                .iter()
                .fold(0usize, |bytes, segment| {
                    bytes
                        .saturating_add(modeled_indexed_segment_publication_capacity_bytes(segment))
                }),
        );
    let publication_roots = match &transition.publication {
        PersistedCatalogPublication::PersistedState {
            published_segment_roots,
            ..
        } => modeled_root_vector_capacity_bytes(
            published_segment_roots,
            published_segment_roots.capacity(),
        ),
        PersistedCatalogPublication::Inventory { inventory, .. } => {
            inventory.entries().iter().fold(0usize, |bytes, entry| {
                bytes.saturating_add(mem::size_of::<SegmentInventoryEntry>().saturating_add(
                    BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(&entry.root),
                ))
            })
        }
    };
    let mutation_bytes = manifest.map_or(0usize, |manifest| {
        modeled_removal_bytes(root, manifest).min(usize::MAX as u64) as usize
    });
    BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(root)
        .saturating_add(mutation_bytes)
        .saturating_add(loaded_segments)
        .saturating_add(modeled_root_vector_capacity_bytes(
            &transition.removed_roots,
            transition.removed_roots.capacity(),
        ))
        .saturating_add(publication_roots)
        .saturating_add(
            transition
                .registry_catalog_update
                .as_ref()
                .map_or(0, modeled_registry_catalog_update_capacity_bytes),
        )
}

pub(super) struct RemoteCatalogPassBudget {
    item_limit: usize,
    byte_limit: u64,
    remaining_items: usize,
    remaining_bytes: u64,
}

impl RemoteCatalogPassBudget {
    pub(super) fn new(item_limit: usize, byte_limit: u64) -> Self {
        Self {
            item_limit,
            byte_limit,
            remaining_items: item_limit,
            remaining_bytes: byte_limit,
        }
    }

    pub(super) fn charge_for(
        &mut self,
        operation: &'static str,
        items: usize,
        bytes: u64,
    ) -> Result<bool> {
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

    fn charge(&mut self, items: usize, bytes: u64) -> Result<bool> {
        self.charge_for(REMOTE_CATALOG_APPLY_OPERATION, items, bytes)
    }

    pub(super) fn exhausted(&self) -> bool {
        self.remaining_items == 0 || self.remaining_bytes == 0
    }

    pub(super) fn byte_limit(&self) -> u64 {
        self.byte_limit
    }
}

impl ChunkStorage {
    pub(super) fn reset_bounded_remote_catalog_refresh(&self) {
        let mut cursor = self.coordination.background_catalog_refresh_cursor.lock();
        if let Some(mut cycle) = cursor.remote_cycle.take() {
            cycle.release_retained_tombstone_bytes(self);
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn bounded_remote_catalog_generation_for_test(
        &self,
    ) -> Option<u64> {
        self.coordination
            .background_catalog_refresh_cursor
            .lock()
            .remote_cycle
            .as_ref()
            .map(|cycle| cycle.pointer.generation)
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn bounded_remote_catalog_addition_ready_for_test(
        &self,
    ) -> bool {
        let cursor = self.coordination.background_catalog_refresh_cursor.lock();
        cursor.remote_cycle.as_ref().is_some_and(|cycle| {
            matches!(
                cycle.pending_page.as_ref(),
                Some(PendingRemoteCatalogRefreshPage::Add { .. })
            ) || matches!(
                &cycle.phase,
                BoundedRemoteCatalogRefreshPhase::Adding { .. }
            )
        })
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn advance_bounded_remote_catalog_without_addition_for_test(
        &self,
    ) -> Result<bool> {
        self.refresh_remote_catalog_bounded_impl(true)
    }

    fn remote_catalog_pointer_matches(&self, pointer: SegmentCatalogPointer) -> Result<bool> {
        let config = self.persisted.tiered_storage.as_ref().ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "finite remote catalog refresh requires tiered storage".to_string(),
            )
        })?;
        Ok(tiering::require_shared_segment_catalog_pointer(config)? == pointer)
    }

    fn scan_bounded_remote_catalog_page(
        &self,
        cycle: &mut BoundedRemoteCatalogRefreshCycle,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<bool> {
        if budget.remaining_items == 0 || budget.remaining_bytes == 0 {
            return Ok(false);
        }
        let retained_before_read = cycle.modeled_retained_bytes();
        let reader_staging_bytes = tiering::modeled_segment_catalog_generation_entry_read_bytes(
            self.persisted.numeric_lane_path.as_deref(),
            self.persisted.blob_lane_path.as_deref(),
            self.persisted.tiered_storage.as_ref(),
        );
        cycle.resize_memory_reservation(
            self,
            retained_before_read.saturating_add(reader_staging_bytes),
        )?;
        // Decode one frame at a time. The outer pass still consumes as many frames as its work
        // budget permits, while each page has one exactly preflighted Vec slot and one payload.
        let page = tiering::read_segment_catalog_generation_page(
            &mut cycle.reader,
            &cycle.generation_path,
            self.persisted.numeric_lane_path.as_deref(),
            self.persisted.blob_lane_path.as_deref(),
            self.persisted.tiered_storage.as_ref(),
            1,
            budget.remaining_bytes,
        );
        let page = match page {
            Ok(page) => page,
            Err(err) => {
                cycle.restore_retained_memory_reservation(self);
                return Err(err);
            }
        };
        let page_items = page.entries.len();
        let page_bytes = page.file_bytes_read;
        let deferred_frame = page.deferred_frame;
        let complete = page.complete;
        let charge = budget.charge(page_items, page_bytes);
        let charged = match charge {
            Ok(charged) => charged,
            Err(err) => {
                drop(page);
                cycle.restore_retained_memory_reservation(self);
                return Err(err);
            }
        };
        if !charged {
            unreachable!("generation reader must stay within the supplied remaining budget");
        }
        for entry in page.entries {
            if let Err(err) = cycle.insert_validated_entry(self, entry) {
                cycle.restore_retained_memory_reservation(self);
                return Err(err);
            }
        }
        cycle.restore_retained_memory_reservation(self);
        if complete {
            if u64::try_from(cycle.entries_by_root.len()).unwrap_or(u64::MAX)
                != cycle.pointer.entry_count
            {
                return Err(TsinkError::DataCorruption(format!(
                    "segment catalog v3 staged {} roots for {} declared entries",
                    cycle.entries_by_root.len(),
                    cycle.pointer.entry_count
                )));
            }
            cycle.phase = BoundedRemoteCatalogRefreshPhase::ValidatePointerBeforeApply;
        }
        Ok(!deferred_frame && (page_bytes != 0 || page_items != 0 || complete))
    }

    fn advance_bounded_remote_tombstones(
        &self,
        cycle: &mut BoundedRemoteCatalogRefreshCycle,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<bool> {
        if cycle.tombstone_cycle.is_none() {
            let Some(tombstone_cycle) =
                BoundedRemoteTombstoneRefreshCycle::new_from_storage(self, budget)?
            else {
                return Ok(false);
            };
            cycle.tombstone_cycle = Some(tombstone_cycle);
        }
        let outcome = cycle
            .tombstone_cycle
            .as_mut()
            .expect("bounded remote tombstone cycle initialized above")
            .advance(self, budget, cycle.expected_visibility_generation);
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(err) => {
                // A pinned immutable shard can disappear after its manifest is replaced and the
                // writer cleans the predecessor generation. Never strand that stale cursor (or its
                // retained decoded fragments) across refresh backoff.
                if let Some(mut tombstone_cycle) = cycle.tombstone_cycle.take() {
                    tombstone_cycle.release_retained_bytes(self);
                }
                return Err(err);
            }
        };
        match outcome {
            BoundedRemoteTombstoneRefreshOutcome::Progressed => Ok(true),
            BoundedRemoteTombstoneRefreshOutcome::Deferred => {
                if cycle
                    .tombstone_cycle
                    .as_ref()
                    .is_some_and(|tombstones| tombstones.has_no_retained_bytes())
                {
                    cycle.tombstone_cycle = None;
                }
                Ok(false)
            }
            BoundedRemoteTombstoneRefreshOutcome::Restart => {
                if let Some(mut tombstone_cycle) = cycle.tombstone_cycle.take() {
                    tombstone_cycle.release_retained_bytes(self);
                }
                Ok(false)
            }
            BoundedRemoteTombstoneRefreshOutcome::Published {
                visibility_generation,
            } => {
                cycle.tombstone_cycle = None;
                cycle.expected_visibility_generation = visibility_generation;
                cycle.phase = BoundedRemoteCatalogRefreshPhase::Adding { after_root: None };
                Ok(true)
            }
        }
    }

    fn preflight_remote_catalog_add(
        &self,
        entry: &SegmentInventoryEntry,
    ) -> Result<RemoteCatalogApplyPreflight> {
        let root = &entry.root;
        let already_visible = {
            let persisted_index = self.persisted.persisted_index.read();
            match persisted_index.segments_by_root.get(root) {
                Some(state)
                    if state.lane == entry.lane
                        && state.tier == entry.tier
                        && state.manifest == entry.manifest =>
                {
                    true
                }
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "visible segment state disagrees with authoritative v3 catalog at {}",
                        root.display()
                    )));
                }
                None => false,
            }
        };
        let descriptor_bytes =
            BoundedRemoteCatalogRefreshCycle::modeled_retained_entry_bytes(entry);
        let auxiliary_bytes = BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(root)
            .saturating_add(
                BoundedRemoteCatalogRefreshCycle::modeled_apply_transition_descriptor_bytes(root),
            )
            .saturating_add(descriptor_bytes.min(usize::MAX as u64) as usize);
        let (source_bytes, staging_bytes, io_work_bytes) = if already_visible {
            (0, auxiliary_bytes, descriptor_bytes)
        } else {
            let runtime = tiering::preflight_segment_runtime_refresh_memory(root)?;
            let mutation_bytes =
                modeled_removal_bytes(root, &entry.manifest).min(usize::MAX as u64) as usize;
            let staging_bytes = auxiliary_bytes
                .saturating_add(runtime.reservation_bytes)
                .saturating_add(mutation_bytes);
            let io_work_bytes = descriptor_bytes
                .saturating_add(CATALOG_SCAN_MANIFEST_INSPECTION_BYTES)
                .saturating_add(runtime.source_bytes);
            (runtime.source_bytes, staging_bytes, io_work_bytes)
        };
        Ok(RemoteCatalogApplyPreflight {
            already_visible,
            source_bytes,
            staging_bytes,
            work_bytes: io_work_bytes.max(u64::try_from(staging_bytes).unwrap_or(u64::MAX)),
        })
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn remote_catalog_add_apply_limits_for_test(
        &self,
        entry: &SegmentInventoryEntry,
    ) -> Result<(u64, usize)> {
        let preflight = self.preflight_remote_catalog_add(entry)?;
        Ok((
            preflight.work_bytes,
            preflight.staging_bytes.saturating_add(
                BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(&entry.root),
            ),
        ))
    }

    fn preflight_remote_catalog_remove(&self, root: &Path) -> Result<RemoteCatalogApplyPreflight> {
        let persisted_index = self.persisted.persisted_index.read();
        let Some(state) = persisted_index.segments_by_root.get(root) else {
            let staging_bytes =
                BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(root)
                    .saturating_add(
                        BoundedRemoteCatalogRefreshCycle::modeled_apply_transition_descriptor_bytes(
                            root,
                        ),
                    )
                    .saturating_add(
                        CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD.min(usize::MAX as u64) as usize
                    );
            return Ok(RemoteCatalogApplyPreflight {
                already_visible: false,
                source_bytes: 0,
                staging_bytes,
                work_bytes: u64::try_from(staging_bytes).unwrap_or(u64::MAX),
            });
        };
        let mutation_bytes = modeled_removal_bytes(root, &state.manifest);
        // Removal accounting decodes each affected identity, clones its metric/label keys into
        // scoped sets, and can decode it again while pruning merged postings. Derive that shape
        // without allocating so long identities are not hidden behind a per-series average.
        let registry = self.catalog.registry.read();
        let identity_bytes = state.chunk_refs_by_series.keys().try_fold(
            0usize,
            |bytes, &series_id| -> Result<usize> {
                let (metric_bytes, label_count, label_text_bytes) = registry
                    .decoded_series_key_shape(series_id)
                    .ok_or_else(|| {
                        TsinkError::DataCorruption(format!(
                            "persisted series id {} is missing from the runtime registry",
                            series_id
                        ))
                    })?;
                let one_identity = metric_bytes
                    .saturating_add(label_text_bytes)
                    .saturating_add(label_count.saturating_mul(mem::size_of::<crate::Label>()))
                    .saturating_add(
                        1usize
                            .saturating_add(label_count.saturating_mul(2))
                            .saturating_mul(64),
                    );
                Ok(bytes
                    .saturating_add(
                        one_identity.saturating_mul(REMOTE_CATALOG_APPLY_SERIES_METADATA_COPIES),
                    )
                    .saturating_add(mem::size_of::<SeriesId>().saturating_mul(4)))
            },
        )?;
        drop(registry);
        drop(persisted_index);
        let staging_bytes = BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(root)
            .saturating_add(
                BoundedRemoteCatalogRefreshCycle::modeled_apply_transition_descriptor_bytes(root),
            )
            .saturating_add(mutation_bytes.min(usize::MAX as u64) as usize)
            .saturating_add(identity_bytes);
        Ok(RemoteCatalogApplyPreflight {
            already_visible: false,
            source_bytes: 0,
            staging_bytes,
            work_bytes: mutation_bytes.max(u64::try_from(staging_bytes).unwrap_or(u64::MAX)),
        })
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn remote_catalog_remove_apply_limits_for_test(
        &self,
        root: &Path,
    ) -> Result<(u64, usize)> {
        let preflight = self.preflight_remote_catalog_remove(root)?;
        Ok((
            preflight.work_bytes,
            preflight.staging_bytes.saturating_add(
                BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(root),
            ),
        ))
    }

    fn preflight_pending_remote_catalog_apply(
        &self,
        cycle: &BoundedRemoteCatalogRefreshCycle,
    ) -> Result<RemoteCatalogApplyPreflight> {
        match cycle.pending_page.as_ref() {
            Some(PendingRemoteCatalogRefreshPage::Add { root }) => {
                let entry = cycle.entries_by_root.get(root).ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "finite remote catalog add cursor lost staged root {}",
                        root.display()
                    ))
                })?;
                self.preflight_remote_catalog_add(entry)
            }
            Some(PendingRemoteCatalogRefreshPage::Remove { root }) => {
                self.preflight_remote_catalog_remove(root)
            }
            None => Ok(RemoteCatalogApplyPreflight {
                already_visible: false,
                source_bytes: 0,
                staging_bytes: 0,
                work_bytes: 0,
            }),
        }
    }

    fn apply_pending_remote_catalog_page(
        &self,
        cycle: &mut BoundedRemoteCatalogRefreshCycle,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<bool> {
        let retained_before_apply = cycle.modeled_retained_bytes();
        if cycle.pending_page.is_none() {
            return Ok(true);
        }

        // Root joins used by the fixed-file preflight are themselves owned PathBuf allocations.
        // Install a small inspection lease first, then compute and admit the complete one-root
        // load/publication peak before cloning the pending page, validating the source, or loading
        // any runtime segment structure.
        let inspection_bytes = cycle.modeled_apply_inspection_bytes()?;
        if let Err(err) = cycle
            .resize_memory_reservation(self, retained_before_apply.saturating_add(inspection_bytes))
        {
            cycle.restore_retained_memory_reservation(self);
            return Err(err);
        }
        let preflight = match self.preflight_pending_remote_catalog_apply(cycle) {
            Ok(preflight) => preflight,
            Err(err) => {
                cycle.restore_retained_memory_reservation(self);
                return Err(err);
            }
        };
        let charged = match budget.charge(1, preflight.work_bytes) {
            Ok(charged) => charged,
            Err(err) => {
                cycle.restore_retained_memory_reservation(self);
                return Err(err);
            }
        };
        if !charged {
            cycle.restore_retained_memory_reservation(self);
            return Ok(false);
        }
        if let Err(err) = cycle.resize_memory_reservation(
            self,
            retained_before_apply.saturating_add(preflight.staging_bytes),
        ) {
            cycle.restore_retained_memory_reservation(self);
            return Err(err);
        }

        let pending = cycle
            .pending_page
            .as_ref()
            .expect("pending remote catalog page checked above")
            .clone();
        let prepared = (|| -> Result<(
            PersistedCatalogTransition,
            PathBuf,
            Option<crate::engine::segment::SegmentManifest>,
        )> {
            match pending {
                PendingRemoteCatalogRefreshPage::Add { root } => {
                    let entry = cycle.entries_by_root.get(&root).ok_or_else(|| {
                        TsinkError::DataCorruption(format!(
                            "finite remote catalog add cursor lost staged root {}",
                            root.display()
                        ))
                    })?;
                    let loaded_segments = if preflight.already_visible {
                        Vec::new()
                    } else {
                        let validated_source_bytes = modeled_segment_source_bytes(entry)?;
                        if validated_source_bytes != preflight.source_bytes {
                            return Err(TsinkError::DataCorruption(format!(
                                "remote segment source changed after apply preflight: {}",
                                root.display()
                            )));
                        }
                        vec![Self::load_segment_index_for_runtime_refresh(&root)?]
                    };
                    let manifest = entry.manifest.clone();
                    let added_roots = vec![root.clone()];
                    let registry_catalog_delta =
                        self.persisted_registry_catalog_delta_for_root_changes(&added_roots, &[])?;
                    Ok((
                        PersistedCatalogTransition {
                            visibility_fence: Some(PersistedCatalogVisibilityFence {
                                visibility_generation: cycle.expected_visibility_generation,
                            }),
                            loaded_segments,
                            removed_roots: Vec::new(),
                            publication: PersistedCatalogPublication::PersistedState {
                                published_segment_roots: added_roots,
                                // Remote tombstone reconciliation is a separate finite protocol.
                                // Segment catalog v3 must not invoke the legacy whole-map path.
                                refresh_tombstones: false,
                            },
                            registry_catalog_update: Some(
                                registry_catalog::PersistedRegistryCatalogUpdate::Delta(
                                    registry_catalog_delta,
                                ),
                            ),
                        },
                        root,
                        (!preflight.already_visible).then_some(manifest),
                    ))
                }
                PendingRemoteCatalogRefreshPage::Remove { root } => {
                    let manifest = self
                        .persisted
                        .persisted_index
                        .read()
                        .segments_by_root
                        .get(&root)
                        .map(|state| state.manifest.clone());
                    let removed_roots = vec![root.clone()];
                    let registry_catalog_delta =
                        self.persisted_registry_catalog_delta_for_root_changes(
                            &[],
                            &removed_roots,
                        )?;
                    Ok((
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
                        },
                        root,
                        manifest,
                    ))
                }
            }
        })();
        let (transition, transition_root, transition_manifest) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                cycle.restore_retained_memory_reservation(self);
                return Err(err);
            }
        };

        // Reconcile the conservative file/count preflight with actual Vec/String capacities after
        // load. Decode buffers are gone at this point, so the reservation can shrink to the
        // publication phase. A defensive upward reconciliation is still charged before the
        // visibility fence is acquired; it can therefore defer or fail without publication.
        let publication_staging_bytes = modeled_transition_publication_capacity_bytes(
            &transition,
            &transition_root,
            transition_manifest.as_ref(),
        );
        let publication_work_bytes = u64::try_from(publication_staging_bytes).unwrap_or(u64::MAX);
        if publication_work_bytes > preflight.work_bytes {
            if publication_work_bytes > budget.byte_limit() {
                drop(transition);
                cycle.restore_retained_memory_reservation(self);
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: REMOTE_CATALOG_APPLY_OPERATION,
                    limit: budget.byte_limit(),
                    required: publication_work_bytes,
                });
            }
            let additional = publication_work_bytes.saturating_sub(preflight.work_bytes);
            let charged = match budget.charge(0, additional) {
                Ok(charged) => charged,
                Err(err) => {
                    drop(transition);
                    cycle.restore_retained_memory_reservation(self);
                    return Err(err);
                }
            };
            if !charged {
                drop(transition);
                cycle.restore_retained_memory_reservation(self);
                return Ok(false);
            }
        }
        if let Err(err) = cycle.resize_memory_reservation(
            self,
            retained_before_apply.saturating_add(publication_staging_bytes),
        ) {
            drop(transition);
            cycle.restore_retained_memory_reservation(self);
            return Err(err);
        }

        let publication = self.begin_persisted_catalog_publication();
        let result = publication.publish_transition(transition);
        drop(publication);
        let current_generation = self.visibility_state_generation();
        let outcome = match result {
            Ok(PersistedCatalogRefreshApply::Applied) => {
                cycle.publication_retry_pending = false;
                match cycle
                    .pending_page
                    .take()
                    .expect("pending remote catalog page checked above")
                {
                    PendingRemoteCatalogRefreshPage::Add { root } => {
                        let BoundedRemoteCatalogRefreshPhase::Adding { after_root } =
                            &mut cycle.phase
                        else {
                            unreachable!("remote add page must retain add phase");
                        };
                        *after_root = Some(root);
                    }
                    PendingRemoteCatalogRefreshPage::Remove { root } => {
                        let BoundedRemoteCatalogRefreshPhase::Removing { after_root } =
                            &mut cycle.phase
                        else {
                            unreachable!("remote remove page must retain remove phase");
                        };
                        *after_root = Some(root);
                    }
                }
                cycle.expected_visibility_generation = current_generation;
                Ok(true)
            }
            Ok(PersistedCatalogRefreshApply::Deferred) => {
                unreachable!("compute-only remote catalog deltas do not publish writer catalogs")
            }
            Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState) => {
                cycle.publication_retry_pending = false;
                cycle.expected_visibility_generation = current_generation;
                cycle.invalidated = true;
                Ok(false)
            }
            Err(err) => {
                // Publication may fail after the live index changes. Keep the exact page intent
                // and rebase only its fence so retry finishes its sidecar work without double
                // counting the visible tier.
                cycle.expected_visibility_generation = current_generation;
                cycle.publication_retry_pending = true;
                Err(err)
            }
        };
        cycle.restore_retained_memory_reservation(self);
        outcome
    }

    fn advance_bounded_remote_catalog_addition(
        &self,
        cycle: &mut BoundedRemoteCatalogRefreshCycle,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<bool> {
        if cycle.pending_page.is_some() {
            return self.apply_pending_remote_catalog_page(cycle, budget);
        }
        let BoundedRemoteCatalogRefreshPhase::Adding { .. } = &cycle.phase else {
            return Ok(true);
        };
        let retained_before_clone = cycle.modeled_retained_bytes();
        let next_root_bytes = {
            let BoundedRemoteCatalogRefreshPhase::Adding { after_root } = &cycle.phase else {
                unreachable!("remote addition phase checked above");
            };
            let start = after_root
                .as_ref()
                .map_or(Bound::Unbounded, Bound::Excluded);
            cycle
                .entries_by_root
                .range::<PathBuf, _>((start, Bound::Unbounded))
                .next()
                .map(|(root, _)| BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(root))
        };
        if let Some(root_bytes) = next_root_bytes {
            cycle.resize_memory_reservation(
                self,
                retained_before_clone.saturating_add(root_bytes),
            )?;
        }
        let next = {
            let BoundedRemoteCatalogRefreshPhase::Adding { after_root } = &cycle.phase else {
                unreachable!("remote addition phase checked above");
            };
            let start = after_root
                .as_ref()
                .map_or(Bound::Unbounded, Bound::Excluded);
            cycle
                .entries_by_root
                .range::<PathBuf, _>((start, Bound::Unbounded))
                .next()
                .map(|(root, _)| root.clone())
        };
        match next {
            Some(root) => {
                let entry = cycle.entries_by_root.get(&root).ok_or_else(|| {
                    TsinkError::DataCorruption(format!(
                        "finite remote catalog addition lost staged root {}",
                        root.display()
                    ))
                })?;
                let visible_state = {
                    let persisted_index = self.persisted.persisted_index.read();
                    persisted_index.segments_by_root.get(&root).map(|state| {
                        state.lane == entry.lane
                            && state.tier == entry.tier
                            && state.manifest == entry.manifest
                    })
                };
                match visible_state {
                    Some(true) => {
                        let descriptor_bytes =
                            BoundedRemoteCatalogRefreshCycle::modeled_retained_entry_bytes(entry);
                        if !budget.charge(1, descriptor_bytes)? {
                            cycle.restore_retained_memory_reservation(self);
                            return Ok(false);
                        }
                        let BoundedRemoteCatalogRefreshPhase::Adding { after_root } =
                            &mut cycle.phase
                        else {
                            unreachable!("remote addition cursor changed phase");
                        };
                        *after_root = Some(root);
                        cycle.restore_retained_memory_reservation(self);
                        return Ok(true);
                    }
                    Some(false) => {
                        return Err(TsinkError::DataCorruption(format!(
                            "visible segment state disagrees with authoritative v3 catalog at {}",
                            root.display()
                        )))
                    }
                    None => {}
                }
                cycle.pending_page = Some(PendingRemoteCatalogRefreshPage::Add { root });
                cycle.restore_retained_memory_reservation(self);
                self.apply_pending_remote_catalog_page(cycle, budget)
            }
            None => {
                // This separate terminal addition probe makes an exact item-limit boundary
                // observably in-progress until the next pass.
                cycle.phase = BoundedRemoteCatalogRefreshPhase::Removing { after_root: None };
                cycle.restore_retained_memory_reservation(self);
                Ok(true)
            }
        }
    }

    fn advance_bounded_remote_catalog_removal(
        &self,
        cycle: &mut BoundedRemoteCatalogRefreshCycle,
        budget: &mut RemoteCatalogPassBudget,
    ) -> Result<bool> {
        if cycle.pending_page.is_some() {
            return self.apply_pending_remote_catalog_page(cycle, budget);
        }
        let BoundedRemoteCatalogRefreshPhase::Removing { .. } = &cycle.phase else {
            return Ok(true);
        };
        let retained_before_clone = cycle.modeled_retained_bytes();
        let next_root_bytes = {
            let BoundedRemoteCatalogRefreshPhase::Removing { after_root } = &cycle.phase else {
                unreachable!("remote removal phase checked above");
            };
            let start = after_root
                .as_ref()
                .map_or(Bound::Unbounded, Bound::Excluded);
            self.persisted
                .persisted_index
                .read()
                .segments_by_root
                .range::<PathBuf, _>((start, Bound::Unbounded))
                .next()
                .map(|(root, _)| BoundedRemoteCatalogRefreshCycle::modeled_cursor_root_bytes(root))
        };
        if let Some(root_bytes) = next_root_bytes {
            cycle.resize_memory_reservation(
                self,
                retained_before_clone.saturating_add(root_bytes),
            )?;
        }
        let next = {
            let BoundedRemoteCatalogRefreshPhase::Removing { after_root } = &cycle.phase else {
                unreachable!("remote removal phase checked above");
            };
            let start = after_root
                .as_ref()
                .map_or(Bound::Unbounded, Bound::Excluded);
            self.persisted
                .persisted_index
                .read()
                .segments_by_root
                .range::<PathBuf, _>((start, Bound::Unbounded))
                .next()
                .map(|(root, state)| (root.clone(), state.manifest.clone()))
        };
        let Some((root, manifest)) = next else {
            // Success is deferred to a distinct terminal pointer probe.
            cycle.phase = BoundedRemoteCatalogRefreshPhase::Terminal;
            cycle.restore_retained_memory_reservation(self);
            return Ok(true);
        };
        if cycle.entries_by_root.contains_key(&root) {
            let modeled_bytes = modeled_removal_bytes(&root, &manifest);
            if budget.charge(1, modeled_bytes)? {
                let BoundedRemoteCatalogRefreshPhase::Removing { after_root } = &mut cycle.phase
                else {
                    unreachable!("remote removal cursor changed phase");
                };
                *after_root = Some(root);
                cycle.restore_retained_memory_reservation(self);
                return Ok(true);
            }
            cycle.restore_retained_memory_reservation(self);
            return Ok(false);
        }
        cycle.pending_page = Some(PendingRemoteCatalogRefreshPage::Remove { root });
        cycle.restore_retained_memory_reservation(self);
        self.apply_pending_remote_catalog_page(cycle, budget)
    }

    /// Advances one bounded, authoritative remote segment-catalog pass.
    ///
    /// The immutable generation is fully validated before any live mutation. Additions precede
    /// removals, and success requires a distinct terminal pointer fence.
    pub(in crate::engine::storage_engine) fn refresh_remote_catalog_bounded(&self) -> Result<bool> {
        self.refresh_remote_catalog_bounded_impl(false)
    }

    fn refresh_remote_catalog_bounded_impl(&self, stop_before_addition: bool) -> Result<bool> {
        let config = self.persisted.tiered_storage.as_ref().ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "finite remote catalog refresh requires tiered storage".to_string(),
            )
        })?;
        let mut budget = RemoteCatalogPassBudget::new(
            self.runtime.maintenance_max_items_per_pass,
            self.runtime.maintenance_max_bytes_per_pass,
        );
        let mut cursor = self.coordination.background_catalog_refresh_cursor.lock();
        if cursor.remote_cycle.is_none() {
            if !budget.charge_for(
                REMOTE_CATALOG_SCAN_OPERATION,
                1,
                SEGMENT_CATALOG_POINTER_BYTES as u64,
            )? {
                return Ok(false);
            }
            let observed_pointer = tiering::require_shared_segment_catalog_pointer(config)?;
            cursor.remote_cycle = Some(BoundedRemoteCatalogRefreshCycle::new(
                self,
                config,
                observed_pointer,
                self.visibility_state_generation(),
            )?);
        }
        let cycle = cursor
            .remote_cycle
            .as_mut()
            .expect("bounded remote catalog cycle initialized above");
        if self.visibility_state_generation() != cycle.expected_visibility_generation {
            if let Some(mut cycle) = cursor.remote_cycle.take() {
                cycle.release_retained_tombstone_bytes(self);
            }
            return Ok(false);
        }
        if cycle.reader.pointer() != cycle.pointer {
            if let Some(mut cycle) = cursor.remote_cycle.take() {
                cycle.release_retained_tombstone_bytes(self);
            }
            return Err(TsinkError::DataCorruption(
                "finite remote catalog reader lost its pinned pointer".to_string(),
            ));
        }

        let mut clear_cycle = false;
        let mut completed = false;
        let mut cycle_error = None;
        loop {
            if stop_before_addition
                && matches!(
                    &cycle.phase,
                    BoundedRemoteCatalogRefreshPhase::Adding { .. }
                )
            {
                break;
            }
            let step = match cycle.phase {
                BoundedRemoteCatalogRefreshPhase::Scanning => {
                    self.scan_bounded_remote_catalog_page(cycle, &mut budget)
                }
                BoundedRemoteCatalogRefreshPhase::ValidatePointerBeforeApply => {
                    let charged = budget.charge_for(
                        REMOTE_CATALOG_SCAN_OPERATION,
                        1,
                        SEGMENT_CATALOG_POINTER_BYTES as u64,
                    );
                    match charged {
                        Ok(false) => break,
                        Err(err) => Err(err),
                        Ok(true) => match self.remote_catalog_pointer_matches(cycle.pointer) {
                            Ok(false) => {
                                cycle.invalidated = true;
                                Ok(true)
                            }
                            Ok(true) => {
                                cycle.phase = BoundedRemoteCatalogRefreshPhase::Tombstones;
                                Ok(true)
                            }
                            Err(err) => Err(err),
                        },
                    }
                }
                BoundedRemoteCatalogRefreshPhase::Tombstones => {
                    self.advance_bounded_remote_tombstones(cycle, &mut budget)
                }
                BoundedRemoteCatalogRefreshPhase::Adding { .. } => {
                    self.advance_bounded_remote_catalog_addition(cycle, &mut budget)
                }
                BoundedRemoteCatalogRefreshPhase::Removing { .. } => {
                    self.advance_bounded_remote_catalog_removal(cycle, &mut budget)
                }
                BoundedRemoteCatalogRefreshPhase::Terminal => {
                    let charged = budget.charge_for(
                        REMOTE_CATALOG_SCAN_OPERATION,
                        1,
                        SEGMENT_CATALOG_POINTER_BYTES as u64,
                    );
                    match charged {
                        Ok(false) => break,
                        Err(err) => {
                            clear_cycle = true;
                            cycle_error = Some(err);
                        }
                        Ok(true) => {
                            clear_cycle = true;
                            match self.remote_catalog_pointer_matches(cycle.pointer) {
                                Ok(matches) => completed = matches,
                                Err(err) => cycle_error = Some(err),
                            }
                        }
                    }
                    break;
                }
            };
            let progressed = match step {
                Ok(progressed) => progressed,
                Err(err) => {
                    cycle_error = Some(err);
                    break;
                }
            };
            if cycle.invalidated {
                clear_cycle = true;
                break;
            }
            if !progressed || budget.exhausted() {
                break;
            }
        }
        // Scan, pointer, tombstone, and preflight failures cannot have published an exact page:
        // discard their leases so a later wake restarts from authoritative state. Publication
        // errors are different. Once publication is entered, the live index may already have
        // changed while its registry sidecar is still stale, so retain that exact pending page
        // (with the fence rebased above) for an idempotent retry.
        let retain_publication_retry =
            cycle_error.is_some() && cycle.publication_retry_pending && !cycle.invalidated;
        if clear_cycle || (cycle_error.is_some() && !retain_publication_retry) {
            if let Some(mut cycle) = cursor.remote_cycle.take() {
                cycle.release_retained_tombstone_bytes(self);
            }
        }
        if let Some(err) = cycle_error {
            return Err(err);
        }
        Ok(completed)
    }
}
