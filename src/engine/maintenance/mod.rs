//! Persisted refresh, retention, tiering, and registry-persistence coordination.
//!
//! Stage filesystem work first and hold `flush_visibility_lock` only while
//! swapping the visible persisted state.

mod budget_enforcement;
mod catalog_refresh;
mod memory_accounting;
mod post_flush;
mod registry_persistence;

pub(in crate::engine::storage_engine) use self::catalog_refresh::BackgroundCatalogRefreshCursor;
pub(in crate::engine::storage_engine) use self::memory_accounting::{
    MemoryReservationAdmissionContext, RemoteCatalogMemoryAccounting,
    RemoteCatalogMemoryReservation, TombstoneMemoryReservation, WriteTransientMemoryAccounting,
    WriteTransientMemoryReservation,
};

pub(in crate::engine::storage_engine) use self::post_flush::recovery::{
    ensure_no_pending_post_flush_replacement, finalize_pending_post_flush_replacements_for_startup,
    is_post_flush_replacement_marker_name, POST_FLUSH_REPLACEMENT_DIR_NAME,
};

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use parking_lot::RwLockWriteGuard;

use super::registry_catalog;
use super::tiering::SegmentInventory;
use super::*;
use crate::engine::segment::IndexedSegment;

const POST_FLUSH_COPY_STAGE_PURPOSE: &str = "post-flush-stage-copy";
const POST_FLUSH_REWRITE_STAGE_PURPOSE: &str = "post-flush-retention-rewrite";

struct PersistedRefreshClaim<'a> {
    refresh: PersistedRefreshContext<'a>,
}

impl Drop for PersistedRefreshClaim<'_> {
    fn drop(&mut self) {
        self.refresh
            .persisted_refresh_in_progress
            .store(false, Ordering::Release);
    }
}

pub(super) struct LoadedInventoryCatalogRefresh {
    visibility_fence: PersistedCatalogVisibilityFence,
    visible_roots: BTreeSet<PathBuf>,
    inventory: SegmentInventory,
}

pub(super) struct PlannedKnownDirtyCatalogRefresh {
    diff: PendingPersistedSegmentDiff,
    loaded_segments: Vec<IndexedSegment>,
    finite_staging_reservation: Option<RemoteCatalogMemoryReservation>,
    finite_recovery_items: Option<usize>,
    finite_recovery_bytes: Option<u64>,
}

#[derive(Clone, Copy)]
pub(super) struct PersistedCatalogVisibilityFence {
    visibility_generation: u64,
}

impl PersistedCatalogVisibilityFence {
    fn matches(self, current_visibility_generation: u64) -> bool {
        self.visibility_generation == current_visibility_generation
    }
}

pub(super) struct PlannedInventoryCatalogRefresh {
    visibility_fence: PersistedCatalogVisibilityFence,
    inventory: SegmentInventory,
    loaded_segments: Vec<IndexedSegment>,
    removed_roots: Vec<PathBuf>,
}

pub(super) enum PlannedPersistedCatalogRefresh {
    KnownDirty(PlannedKnownDirtyCatalogRefresh),
    Inventory(PlannedInventoryCatalogRefresh),
}

pub(super) enum PersistedCatalogPublication {
    PersistedState {
        published_segment_roots: Vec<PathBuf>,
        refresh_tombstones: bool,
    },
    Inventory {
        inventory: SegmentInventory,
        refresh_tombstones: bool,
    },
}

pub(super) struct PersistedCatalogTransition {
    pub(super) visibility_fence: Option<PersistedCatalogVisibilityFence>,
    pub(super) loaded_segments: Vec<IndexedSegment>,
    pub(super) removed_roots: Vec<PathBuf>,
    pub(super) publication: PersistedCatalogPublication,
    pub(super) registry_catalog_update: Option<registry_catalog::PersistedRegistryCatalogUpdate>,
}

impl PlannedPersistedCatalogRefresh {
    pub(super) fn restore_known_dirty_diff(&self) -> Option<PendingPersistedSegmentDiff> {
        match self {
            Self::KnownDirty(planned) if planned.finite_staging_reservation.is_none() => {
                Some(planned.diff.clone())
            }
            Self::Inventory(_) => None,
            Self::KnownDirty(_) => None,
        }
    }

    pub(super) fn restore_known_dirty_diff_conditionally(&self) -> bool {
        matches!(
            self,
            Self::KnownDirty(planned) if planned.finite_staging_reservation.is_some()
        )
    }
}

#[derive(Clone, Copy)]
pub(super) enum PersistedCatalogRefreshApply {
    Applied,
    Deferred,
    SkippedStaleVisibleState,
}

impl PersistedCatalogRefreshApply {
    pub(super) fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    pub(super) fn is_deferred(self) -> bool {
        matches!(self, Self::Deferred)
    }
}

#[derive(Debug)]
struct StagedSegmentPromotion {
    staging_root: PathBuf,
    final_root: PathBuf,
}

#[derive(Debug)]
struct RetiredPostFlushRoot {
    root: PathBuf,
    counts_as_expired: bool,
}

struct StagedPostFlushMaintenance {
    publication: StagedPostFlushPublication,
    promotions: Vec<StagedSegmentPromotion>,
    staging_cleanup_paths: Vec<PathBuf>,
    loaded_segments: Vec<IndexedSegment>,
    removed_roots: Vec<PathBuf>,
    retired_roots: Vec<RetiredPostFlushRoot>,
    tier_moves: usize,
}

enum StagedPostFlushPublication {
    CompleteInventory(SegmentInventory),
    PersistedStateDelta { published_roots: Vec<PathBuf> },
}

#[derive(Clone, Copy)]
enum PostFlushMaintenanceStageScope {
    CompleteInventory,
    SelectedPage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceInventorySource {
    PersistedState,
    Scanned,
}

pub(super) struct PersistedCatalogPublicationGuard<'a> {
    storage: &'a ChunkStorage,
    _visibility_guard: RwLockWriteGuard<'a, ()>,
}

impl<'a> PersistedCatalogPublicationGuard<'a> {
    fn new(storage: &'a ChunkStorage) -> Self {
        Self {
            storage,
            _visibility_guard: storage.visibility_write_fence(),
        }
    }

    fn current_visibility_generation(&self) -> u64 {
        self.storage.visibility_state_generation()
    }

    pub(super) fn publish_transition(
        &self,
        transition: PersistedCatalogTransition,
    ) -> Result<PersistedCatalogRefreshApply> {
        self.publish_transition_with_finite_recovery_budget(
            transition,
            self.storage.runtime.maintenance_max_items_per_pass,
            self.storage.runtime.maintenance_max_bytes_per_pass,
            false,
        )
    }

    pub(super) fn publish_transition_with_finite_recovery_budget(
        &self,
        transition: PersistedCatalogTransition,
        available_items: usize,
        available_bytes: u64,
        transition_staging_already_reserved: bool,
    ) -> Result<PersistedCatalogRefreshApply> {
        let current_visibility_generation = self.current_visibility_generation();
        if transition
            .visibility_fence
            .is_some_and(|fence| !fence.matches(current_visibility_generation))
        {
            return Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState);
        }

        let finite_maintenance = self.storage.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.storage.runtime.maintenance_max_bytes_per_pass != u64::MAX;
        let mut transition_reservation = None;
        if finite_maintenance && self.storage.runtime.runtime_mode == StorageRuntimeMode::ReadWrite
        {
            let (recovery_items, recovery_bytes) = if transition_staging_already_reserved {
                (available_items, available_bytes)
            } else {
                let staging_bytes = self
                    .storage
                    .modeled_finite_transition_staging_bytes(&transition);
                let staging_work = u64::try_from(staging_bytes).unwrap_or(u64::MAX);
                if staging_work > available_bytes {
                    return Err(TsinkError::MaintenanceWorkItemTooLarge {
                        operation: "finite catalog transition staging",
                        limit: available_bytes,
                        required: staging_work,
                    });
                }
                if available_items == 0 {
                    return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                        operation: "finite catalog transition staging",
                        item_limit: available_items,
                        byte_limit: available_bytes,
                        selected_items: 1,
                        selected_bytes: staging_work,
                    });
                }
                transition_reservation = Some(
                    self.storage
                        .remote_catalog_memory_reservation(staging_bytes)?,
                );
                (
                    available_items.saturating_sub(1),
                    available_bytes.saturating_sub(staging_work),
                )
            };
            self.storage
                .recover_and_reload_tombstones_locked_with_limits(recovery_items, recovery_bytes)?;
        }

        let result = self
            .storage
            .apply_persisted_catalog_transition_phase(transition, current_visibility_generation);
        drop(transition_reservation);
        result
    }

    pub(super) fn apply_planned_refresh(
        &self,
        mut planned: PlannedPersistedCatalogRefresh,
    ) -> Result<PersistedCatalogRefreshApply> {
        let mut finite_restore_diff = match &planned {
            PlannedPersistedCatalogRefresh::KnownDirty(planned)
                if planned.finite_staging_reservation.is_some() =>
            {
                Some(planned.diff.clone())
            }
            _ => None,
        };
        let (finite_staging_reservation, finite_recovery_items, finite_recovery_bytes) =
            match &mut planned {
                PlannedPersistedCatalogRefresh::KnownDirty(planned) => (
                    planned.finite_staging_reservation.take(),
                    planned.finite_recovery_items,
                    planned.finite_recovery_bytes,
                ),
                PlannedPersistedCatalogRefresh::Inventory(_) => (None, None, None),
            };
        let result = match self
            .storage
            .plan_persisted_catalog_transition_phase(planned)
        {
            Ok(transition) => match (finite_recovery_items, finite_recovery_bytes) {
                (Some(items), Some(bytes)) => self.publish_transition_with_finite_recovery_budget(
                    transition,
                    items,
                    bytes,
                    finite_staging_reservation.is_some(),
                ),
                _ => self.publish_transition(transition),
            },
            Err(err) => Err(err),
        };
        if result.is_err() {
            if let Some(diff) = finite_restore_diff.take() {
                self.storage
                    .restore_known_persisted_segment_change_if_unmodified(diff);
            }
        }
        drop(finite_restore_diff);
        drop(finite_staging_reservation);
        result
    }
}

impl ChunkStorage {
    pub(super) fn begin_persisted_catalog_publication(
        &self,
    ) -> PersistedCatalogPublicationGuard<'_> {
        PersistedCatalogPublicationGuard::new(self)
    }
}
