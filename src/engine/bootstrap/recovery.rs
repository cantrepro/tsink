use tracing::warn;

use super::super::tiering::{SegmentInventory, SegmentLaneFamily};
use super::discovery::{StartupDiscoveredState, StartupInventoryState, StartupRegistryState};
use super::finalize::StartupFinalizeState;
use super::planning::StartupPlan;
use super::*;
use crate::engine::segment::{
    load_segment_indexes_from_dirs_startup_recoverable_with_series, LoadedSegmentIndexes,
    StartupQuarantinedSegment,
};

pub(super) struct StartupRecoveryPhase;

pub(super) struct StartupRecoveryState {
    pub(super) registry: StartupRegistryState,
    pub(super) loaded_segments: LoadedSegmentIndexes,
    pub(super) startup_quarantined: Vec<StartupQuarantinedSegment>,
}

impl StartupRecoveryPhase {
    pub(super) fn recover(
        plan: &StartupPlan,
        discovered: StartupDiscoveredState,
    ) -> Result<StartupRecoveryState> {
        let StartupDiscoveredState {
            mut registry,
            mut inventory,
        } = discovered;

        let validated_catalog = validate_startup_registry_catalog(
            plan.paths().series_index_path.as_deref(),
            &mut registry,
            &inventory,
        );
        let mut loaded_segments = load_startup_segment_state(
            plan,
            &mut registry,
            &mut inventory,
            validated_catalog.as_ref(),
        )?;
        reconcile_startup_registry_snapshot(
            plan.paths().series_index_path.as_deref(),
            plan,
            &mut registry,
            &mut inventory,
            validated_catalog.as_ref(),
            &mut loaded_segments,
        )?;

        Ok(StartupRecoveryState {
            registry,
            loaded_segments,
            startup_quarantined: inventory.startup_quarantined,
        })
    }
}

impl StartupRecoveryState {
    pub(super) fn finalize_state(&self) -> StartupFinalizeState {
        StartupFinalizeState {
            force_registry_checkpoint_after_startup: self
                .registry
                .force_registry_checkpoint_after_startup,
            reconcile_registry_with_persisted: self.registry.reconcile_registry_with_persisted,
        }
    }

    pub(super) fn loaded_segments(&self) -> &LoadedSegmentIndexes {
        &self.loaded_segments
    }
}

fn validate_startup_registry_catalog(
    series_index_path: Option<&Path>,
    registry: &mut StartupRegistryState,
    inventory: &StartupInventoryState,
) -> Option<registry_catalog::ValidatedRegistryCatalog> {
    if !registry.reconcile_registry_with_persisted {
        return None;
    }

    let index_path = series_index_path.expect("persisted registry requires a snapshot path");
    let catalog_sources = registry_catalog::inventory_sources(&inventory.segment_inventory);
    match registry_catalog::validate_registry_catalog(index_path, &catalog_sources) {
        Ok(Some(validated_catalog)) => Some(validated_catalog),
        Ok(None) => {
            registry.force_registry_checkpoint();
            None
        }
        Err(err) => {
            warn!(
                path = %index_path.display(),
                error = %err,
                "Failed to validate persisted registry catalog sidecar; rebuilding startup metadata from segments"
            );
            registry.force_registry_checkpoint();
            None
        }
    }
}

fn load_startup_segment_state(
    plan: &StartupPlan,
    registry: &mut StartupRegistryState,
    inventory: &mut StartupInventoryState,
    validated_catalog: Option<&registry_catalog::ValidatedRegistryCatalog>,
) -> Result<LoadedSegmentIndexes> {
    let (numeric_lane_enabled, blob_lane_enabled) = plan.lane_flags();
    let load_segment_series = should_load_segment_series(validated_catalog);
    let (mut loaded_segments, load_quarantined) = load_startup_segments_with_recovery(
        &inventory.segment_inventory,
        load_segment_series,
        numeric_lane_enabled,
        blob_lane_enabled,
    )?;

    if inventory.apply_quarantines(load_quarantined) {
        registry.force_registry_checkpoint();
        if registry.reconcile_registry_with_persisted || !load_segment_series {
            registry.discard_persisted_registry();
            let (reloaded_segments, reloaded_quarantined) = load_startup_segments_with_recovery(
                &inventory.segment_inventory,
                true,
                numeric_lane_enabled,
                blob_lane_enabled,
            )?;
            loaded_segments = reloaded_segments;
            if inventory.apply_quarantines(reloaded_quarantined) {
                registry.force_registry_checkpoint();
            }
        }
    }

    Ok(loaded_segments)
}

fn reconcile_startup_registry_snapshot(
    series_index_path: Option<&Path>,
    plan: &StartupPlan,
    registry: &mut StartupRegistryState,
    inventory: &mut StartupInventoryState,
    validated_catalog: Option<&registry_catalog::ValidatedRegistryCatalog>,
    loaded_segments: &mut LoadedSegmentIndexes,
) -> Result<()> {
    let (
        Some(loaded_registry),
        Some(registry_catalog::ValidatedRegistryCatalog {
            series_fingerprint: Some(series_fingerprint),
        }),
    ) = (registry.persisted_registry.as_ref(), validated_catalog)
    else {
        return Ok(());
    };

    if let Err(err) = registry_catalog::validate_registry_snapshot(
        &loaded_registry.registry,
        &loaded_segments.indexed_segments,
        series_fingerprint,
    ) {
        let index_path = series_index_path.expect("persisted registry requires a snapshot path");
        warn!(
            path = %index_path.display(),
            error = %err,
            "Persisted registry snapshot conflicts with persisted segment metadata; rebuilding startup metadata from segments"
        );
        registry.force_registry_checkpoint();
        registry.discard_persisted_registry();

        let (numeric_lane_enabled, blob_lane_enabled) = plan.lane_flags();
        let (reloaded_segments, reloaded_quarantined) = load_startup_segments_with_recovery(
            &inventory.segment_inventory,
            true,
            numeric_lane_enabled,
            blob_lane_enabled,
        )?;
        *loaded_segments = reloaded_segments;
        if inventory.apply_quarantines(reloaded_quarantined) {
            registry.force_registry_checkpoint();
        }
    }

    Ok(())
}

fn should_load_segment_series(
    validated_catalog: Option<&registry_catalog::ValidatedRegistryCatalog>,
) -> bool {
    !matches!(
        validated_catalog,
        Some(registry_catalog::ValidatedRegistryCatalog {
            series_fingerprint: Some(_),
        })
    )
}

fn load_startup_segments_with_recovery(
    inventory: &SegmentInventory,
    load_series: bool,
    numeric_lane_enabled: bool,
    blob_lane_enabled: bool,
) -> Result<(LoadedSegmentIndexes, Vec<StartupQuarantinedSegment>)> {
    let loaded_numeric = load_segment_indexes_from_dirs_startup_recoverable_with_series(
        inventory.roots_for_lane(SegmentLaneFamily::Numeric),
        load_series,
    )?;
    let loaded_blob = load_segment_indexes_from_dirs_startup_recoverable_with_series(
        inventory.roots_for_lane(SegmentLaneFamily::Blob),
        load_series,
    )?;
    let mut quarantined = loaded_numeric.quarantined;
    quarantined.extend(loaded_blob.quarantined);
    Ok((
        super::merge_loaded_segment_indexes(
            loaded_numeric.loaded,
            loaded_blob.loaded,
            numeric_lane_enabled,
            blob_lane_enabled,
        )?,
        quarantined,
    ))
}
