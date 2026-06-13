use std::path::Path;

use tracing::warn;

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::inventory::SegmentInventoryAccumulator;
use super::layout::SegmentScanTarget;
use super::{SegmentInventory, SegmentInventoryEntry, SegmentPathResolver};
use crate::engine::segment::{
    list_segment_dirs, quarantine_invalid_startup_segment, read_segment_manifest,
    segment_validation_error, segment_validation_error_message, SegmentValidationContext,
    StartupQuarantinedSegment,
};

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct StartupRecoveredSegmentInventory {
    pub(in crate::engine::storage_engine) inventory: SegmentInventory,
    pub(in crate::engine::storage_engine) quarantined: Vec<StartupQuarantinedSegment>,
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_startup_recoverable(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<StartupRecoveredSegmentInventory> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();
    let mut quarantined = Vec::new();

    for target in resolver.inventory_scan_targets() {
        scan_startup_segment_roots_recoverable(&target, &mut deduped, &mut quarantined)?;
    }

    Ok(StartupRecoveredSegmentInventory {
        inventory: deduped.finish(),
        quarantined,
    })
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_runtime_strict(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<SegmentInventory> {
    build_segment_inventory_fail_on_invalid(
        numeric_lane_path,
        blob_lane_path,
        tiered_storage,
        SegmentValidationContext::RuntimeRefresh,
    )
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_fail_on_invalid(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
    context: SegmentValidationContext,
) -> Result<SegmentInventory> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();

    for target in resolver.inventory_scan_targets() {
        scan_segment_roots(&target, context, &mut deduped)?;
    }

    Ok(deduped.finish())
}

fn scan_segment_roots(
    target: &SegmentScanTarget,
    context: SegmentValidationContext,
    deduped: &mut SegmentInventoryAccumulator,
) -> Result<()> {
    for root in list_segment_dirs(&target.base_path)? {
        let manifest = match read_segment_manifest(&root) {
            Ok(manifest) => manifest,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory disappeared during tiered inventory scan; skipping"
                );
                continue;
            }
            Err(TsinkError::DataCorruption(msg)) => {
                return Err(segment_validation_error(&root, context, &msg));
            }
            Err(err) => return Err(err),
        };

        deduped.insert(SegmentInventoryEntry {
            lane: target.lane,
            tier: target.tier,
            root,
            manifest,
        });
    }

    Ok(())
}

fn scan_startup_segment_roots_recoverable(
    target: &SegmentScanTarget,
    deduped: &mut SegmentInventoryAccumulator,
    quarantined: &mut Vec<StartupQuarantinedSegment>,
) -> Result<()> {
    for root in list_segment_dirs(&target.base_path)? {
        let manifest = match read_segment_manifest(&root) {
            Ok(manifest) => manifest,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory disappeared during tiered inventory scan; skipping"
                );
                continue;
            }
            Err(err) => {
                let Some(details) = segment_validation_error_message(&err) else {
                    return Err(err);
                };
                quarantined.push(quarantine_invalid_startup_segment(&root, &details)?);
                continue;
            }
        };

        deduped.insert(SegmentInventoryEntry {
            lane: target.lane,
            tier: target.tier,
            root,
            manifest,
        });
    }

    Ok(())
}

fn is_not_found_error(err: &TsinkError) -> bool {
    matches!(err, TsinkError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound)
}
