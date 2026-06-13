use std::collections::BTreeSet;

use tracing::warn;

use super::super::tiering::{
    build_segment_inventory_startup_recoverable, PersistedSegmentTier, SegmentInventory,
    SegmentLaneFamily,
};
use super::planning::StartupPlan;
use super::*;
use crate::engine::segment::StartupQuarantinedSegment;
use crate::engine::series::LoadedSeriesRegistry;

pub(super) struct StartupDiscoveryPhase;

pub(super) struct StartupDiscoveredState {
    pub(super) registry: StartupRegistryState,
    pub(super) inventory: StartupInventoryState,
}

pub(super) struct StartupRegistryState {
    pub(super) persisted_registry: Option<LoadedSeriesRegistry>,
    pub(super) reconcile_registry_with_persisted: bool,
    pub(super) force_registry_checkpoint_after_startup: bool,
}

pub(super) struct StartupInventoryState {
    pub(super) segment_inventory: SegmentInventory,
    pub(super) startup_quarantined: Vec<StartupQuarantinedSegment>,
}

impl StartupDiscoveryPhase {
    pub(super) fn discover(
        builder: &StorageBuilder,
        plan: &StartupPlan,
    ) -> Result<StartupDiscoveredState> {
        finalize_pending_compaction_replacements_for_storage_paths(
            builder,
            plan.paths().numeric_lane_path.as_deref(),
            plan.paths().blob_lane_path.as_deref(),
            plan.storage_options().tiered_storage.as_ref(),
        )?;

        let mut registry = StartupRegistryState::load(plan.paths().series_index_path.as_deref())?;
        let inventory = StartupInventoryState::discover(plan, &mut registry)?;

        Ok(StartupDiscoveredState {
            registry,
            inventory,
        })
    }
}

impl StartupRegistryState {
    fn load(series_index_path: Option<&Path>) -> Result<Self> {
        let (persisted_registry, force_registry_checkpoint_after_startup) = if let Some(
            index_path,
        ) = series_index_path
        {
            match SeriesRegistry::load_persisted_state(index_path) {
                Ok(registry) => (registry, false),
                Err(err) => {
                    warn!(
                        path = %index_path.display(),
                        error = %err,
                        "Failed to load persisted series registry index; rebuilding from segments"
                    );
                    (None, true)
                }
            }
        } else {
            (None, false)
        };

        Ok(Self {
            reconcile_registry_with_persisted: persisted_registry.is_some(),
            persisted_registry,
            force_registry_checkpoint_after_startup,
        })
    }

    pub(super) fn discard_persisted_registry(&mut self) {
        self.persisted_registry = None;
        self.reconcile_registry_with_persisted = false;
    }

    pub(super) fn force_registry_checkpoint(&mut self) {
        self.force_registry_checkpoint_after_startup = true;
    }
}

impl StartupInventoryState {
    fn discover(plan: &StartupPlan, registry: &mut StartupRegistryState) -> Result<Self> {
        let startup_inventory = build_segment_inventory_startup_recoverable(
            plan.paths().numeric_lane_path.as_deref(),
            plan.paths().blob_lane_path.as_deref(),
            plan.storage_options().tiered_storage.as_ref(),
        )?;
        let inventory = Self {
            segment_inventory: startup_inventory.inventory,
            startup_quarantined: startup_inventory.quarantined,
        };
        if !inventory.startup_quarantined.is_empty() {
            registry.discard_persisted_registry();
            registry.force_registry_checkpoint();
        }
        Ok(inventory)
    }

    pub(super) fn apply_quarantines(
        &mut self,
        newly_quarantined: Vec<StartupQuarantinedSegment>,
    ) -> bool {
        apply_startup_segment_quarantines(
            &mut self.segment_inventory,
            &mut self.startup_quarantined,
            newly_quarantined,
        )
    }
}

fn apply_startup_segment_quarantines(
    inventory: &mut SegmentInventory,
    startup_quarantined: &mut Vec<StartupQuarantinedSegment>,
    newly_quarantined: Vec<StartupQuarantinedSegment>,
) -> bool {
    if newly_quarantined.is_empty() {
        return false;
    }

    let excluded_roots = newly_quarantined
        .iter()
        .map(|entry| entry.original_root.clone())
        .collect::<BTreeSet<_>>();
    *inventory = inventory.without_roots(&excluded_roots);
    startup_quarantined.extend(newly_quarantined);
    true
}

fn finalize_pending_compaction_replacements_for_storage_paths(
    builder: &StorageBuilder,
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&config::TieredStorageConfig>,
) -> Result<()> {
    let mut paths = BTreeSet::new();
    if let Some(path) = numeric_lane_path {
        paths.insert(path.to_path_buf());
    }
    if let Some(path) = blob_lane_path {
        paths.insert(path.to_path_buf());
    }

    if builder.runtime_mode() != StorageRuntimeMode::ComputeOnly {
        if let Some(config) = tiered_storage {
            for lane in [SegmentLaneFamily::Numeric, SegmentLaneFamily::Blob] {
                for tier in [
                    PersistedSegmentTier::Hot,
                    PersistedSegmentTier::Warm,
                    PersistedSegmentTier::Cold,
                ] {
                    paths.insert(config.lane_path(lane, tier));
                }
            }
        }
    }

    for path in paths {
        crate::engine::compactor::finalize_pending_compaction_replacements(&path)?;
    }

    Ok(())
}
