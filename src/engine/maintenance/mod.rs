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
    TombstoneMemoryReservation, WriteTransientMemoryAccounting, WriteTransientMemoryReservation,
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
            Self::KnownDirty(planned) => Some(planned.diff.clone()),
            Self::Inventory(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum PersistedCatalogRefreshApply {
    Applied,
    SkippedStaleVisibleState,
}

impl PersistedCatalogRefreshApply {
    pub(super) fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
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
        self.storage.apply_persisted_catalog_transition_phase(
            transition,
            self.current_visibility_generation(),
        )
    }

    pub(super) fn apply_planned_refresh(
        &self,
        planned: PlannedPersistedCatalogRefresh,
    ) -> Result<PersistedCatalogRefreshApply> {
        let transition = self
            .storage
            .plan_persisted_catalog_transition_phase(planned)?;
        self.publish_transition(transition)
    }
}

impl ChunkStorage {
    pub(super) fn begin_persisted_catalog_publication(
        &self,
    ) -> PersistedCatalogPublicationGuard<'_> {
        PersistedCatalogPublicationGuard::new(self)
    }
}
