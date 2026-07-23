use super::*;
use crate::engine::fs_utils::{
    copy_dir_contents_bounded, create_dir_all_and_sync_parents, is_link_or_reparse_point,
    measure_restore_directory, path_exists_no_follow, paths_overlap_resolved,
    remove_path_if_exists_and_sync_parent, rename_path, stage_dir_path, sync_dir,
};
use crate::engine::segment::{LoadedSegmentIndexes, WalHighWatermark};

#[path = "bootstrap/discovery.rs"]
mod discovery;
#[path = "bootstrap/finalize.rs"]
mod finalize;
#[path = "bootstrap/hydrate.rs"]
mod hydrate;
#[path = "bootstrap/memory.rs"]
mod memory;
#[path = "bootstrap/planning.rs"]
mod planning;
#[path = "bootstrap/recovery.rs"]
mod recovery;
#[path = "bootstrap/wal_open.rs"]
mod wal_open;

#[cfg(test)]
#[path = "bootstrap/tests.rs"]
mod tests;

use discovery::StartupDiscoveryPhase;
use finalize::StartupFinalizePhase;
use hydrate::StartupHydrationPhase;
use memory::StartupMemoryAdmission;
use planning::StartupPlanningPhase;
use recovery::StartupRecoveryPhase;
use wal_open::StartupWalOpenPhase;

pub(super) fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    let plan = StartupPlanningPhase::prepare(&builder)?;
    let discovered = StartupDiscoveryPhase::discover(&builder, &plan)?;
    let recovered = StartupRecoveryPhase::recover(&plan, discovered)?;
    let finalize_state = recovered.finalize_state();
    let replay_highwater = recovered.loaded_segments().wal_replay_highwater;
    let next_segment_id = recovered.loaded_segments().next_segment_id;

    let wal = StartupWalOpenPhase::open(&builder, &plan, replay_highwater)?;
    let storage_options = plan.storage_options().clone();
    let paths = plan.paths().clone();
    let local_disk_budget = plan.local_disk_budget().cloned();
    let runtime_inputs = plan.into_runtime_inputs();

    let storage = StartupHydrationPhase::create_storage(
        builder.chunk_points(),
        storage_options,
        &paths,
        next_segment_id,
        wal,
        local_disk_budget,
        builder.query_budget_limits(),
    )?;
    *storage.resource_configuration.write() = builder.resource_configuration_snapshot();
    StartupHydrationPhase::hydrate(
        storage.as_ref(),
        &builder,
        recovered,
        runtime_inputs.data_path_process_lock,
        runtime_inputs.shared_object_store_process_lock,
    )?;
    StartupFinalizePhase::run(
        &storage,
        finalize_state,
        runtime_inputs.background_threads_enabled,
        runtime_inputs.background_fail_fast,
    )?;

    Ok(storage as Arc<dyn Storage>)
}

pub(super) fn restore_storage_from_snapshot(snapshot_path: &Path, data_path: &Path) -> Result<()> {
    if snapshot_path == data_path {
        return Err(TsinkError::InvalidConfiguration(
            "snapshot and restore paths must differ".to_string(),
        ));
    }

    let snapshot_meta =
        std::fs::symlink_metadata(snapshot_path).map_err(|err| TsinkError::IoWithPath {
            path: snapshot_path.to_path_buf(),
            source: err,
        })?;
    if is_link_or_reparse_point(&snapshot_meta) || !snapshot_meta.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a plain directory: {}",
            snapshot_path.display()
        )));
    }
    if paths_overlap_resolved(snapshot_path, data_path)? {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot and restore target paths must not overlap: {} and {}",
            snapshot_path.display(),
            data_path.display()
        )));
    }
    let snapshot_measurement = measure_restore_directory(snapshot_path)?;

    let Some(parent) = data_path.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "restore target has no parent directory: {}",
            data_path.display()
        )));
    };
    create_dir_all_and_sync_parents(parent)?;

    let staging = stage_dir_path(data_path, "restore-staging")?;
    std::fs::create_dir_all(&staging)?;
    if let Err(copy_err) = copy_dir_contents_bounded(snapshot_path, &staging, snapshot_measurement)
    {
        return Err(cleanup_restore_staging_after_error(&staging, copy_err));
    }

    if let Err(sync_err) = sync_dir(&staging) {
        return Err(cleanup_restore_staging_after_error(&staging, sync_err));
    }

    let backup = match (|| -> Result<Option<PathBuf>> {
        if path_exists_no_follow(data_path)? {
            Ok(Some(stage_dir_path(data_path, "restore-backup")?))
        } else {
            Ok(None)
        }
    })() {
        Ok(backup) => backup,
        Err(pre_activation_err) => {
            return Err(cleanup_restore_staging_after_error(
                &staging,
                pre_activation_err,
            ));
        }
    };
    activate_staged_restore(&staging, data_path, backup.as_deref())
}

pub(super) fn restore_storage_from_snapshot_with_disk_budget(
    snapshot_path: &Path,
    data_path: &Path,
    disk_budget: Arc<crate::LocalDiskBudget>,
) -> Result<()> {
    let snapshot_meta =
        std::fs::symlink_metadata(snapshot_path).map_err(|source| TsinkError::IoWithPath {
            path: snapshot_path.to_path_buf(),
            source,
        })?;
    if is_link_or_reparse_point(&snapshot_meta) || !snapshot_meta.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a plain directory: {}",
            snapshot_path.display()
        )));
    }
    if disk_budget.overlaps(snapshot_path)? {
        return Err(TsinkError::InvalidConfiguration(format!(
            "budgeted restore requires an external snapshot that does not overlap local disk root {}: {}",
            disk_budget.root().display(),
            snapshot_path.display()
        )));
    }

    // Complete source validation and bounded byte/entry measurement before quota admission can
    // create a destination parent or staging directory.
    let snapshot_measurement = measure_restore_directory(snapshot_path)?;
    let entry_allowance_bytes = disk_budget.snapshot_restore_entry_staging_allowance_bytes()?;
    let staging_admission_bytes =
        snapshot_measurement.staging_admission_bytes(entry_allowance_bytes)?;
    let staging = stage_dir_path(data_path, "restore-staging")?;
    let backup = stage_dir_path(data_path, "restore-backup")?;

    disk_budget.with_managed_directory_replacement(
        data_path,
        &staging,
        &backup,
        staging_admission_bytes,
        entry_allowance_bytes,
        |target, staging, backup| {
            let parent = target.parent().ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "restore target has no parent directory: {}",
                    target.display()
                ))
            })?;
            disk_budget.create_dir_all_and_sync_parents(parent)?;

            // Conservatively revalidate the final entries immediately before creating staging.
            // The budget coordinator serializes cooperating writers; these checks also reject a
            // static symlink or unexpected entry introduced since the initial preflight.
            disk_budget.validate_managed_directory_path(target)?;
            if path_exists_no_follow(staging)? || path_exists_no_follow(backup)? {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "budgeted restore staging or backup path appeared after preflight: {}, {}",
                    staging.display(),
                    backup.display()
                )));
            }

            disk_budget.create_dir_all_and_sync_parents(staging)?;
            if let Err(copy_err) =
                copy_dir_contents_bounded(snapshot_path, staging, snapshot_measurement)
            {
                return Err(cleanup_restore_staging_after_error(staging, copy_err));
            }
            if let Err(sync_err) = sync_dir(staging) {
                return Err(cleanup_restore_staging_after_error(staging, sync_err));
            }

            disk_budget.validate_managed_directory_path(target)?;
            disk_budget.validate_managed_directory_path(staging)?;
            if path_exists_no_follow(backup)? {
                return Err(cleanup_restore_staging_after_error(
                    staging,
                    TsinkError::InvalidConfiguration(format!(
                        "budgeted restore backup path appeared after staging: {}",
                        backup.display()
                    )),
                ));
            }

            let backup_used = path_exists_no_follow(target)?;
            activate_staged_restore(staging, target, backup_used.then_some(backup))
        },
    )
}

fn cleanup_restore_staging_after_error(staging: &Path, operation_err: TsinkError) -> TsinkError {
    match remove_path_if_exists_and_sync_parent(staging) {
        Ok(()) => operation_err,
        Err(cleanup_err) => TsinkError::Other(format!(
            "restore staging operation failed: {operation_err}; cleanup of {} failed: {cleanup_err}",
            staging.display()
        )),
    }
}

fn activate_staged_restore(staging: &Path, target: &Path, backup: Option<&Path>) -> Result<()> {
    let parent = target.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "restore target has no parent directory: {}",
            target.display()
        ))
    })?;

    if let Some(backup) = backup {
        if let Err(rename_err) = rename_path(target, backup) {
            return Err(cleanup_restore_staging_after_error(staging, rename_err));
        }
        if let Err(sync_err) = sync_dir(parent) {
            let rollback_result = rename_path(backup, target).and_then(|()| sync_dir(parent));
            let cleanup_result = remove_path_if_exists_and_sync_parent(staging);
            return Err(combine_restore_failure(
                format!("restore backup publication failed: {sync_err}"),
                rollback_result,
                cleanup_result,
            ));
        }
    }

    if let Err(activate_err) = rename_path(staging, target) {
        let rollback_result =
            restore_backup_after_failed_activation(target, staging, backup, parent, false);
        let cleanup_result = remove_path_if_exists_and_sync_parent(staging);
        return Err(combine_restore_failure(
            format!("restore activation failed: {activate_err}"),
            rollback_result,
            cleanup_result,
        ));
    }
    if let Err(sync_err) = sync_dir(parent) {
        let rollback_result =
            restore_backup_after_failed_activation(target, staging, backup, parent, true);
        let cleanup_result = remove_path_if_exists_and_sync_parent(staging);
        return Err(combine_restore_failure(
            format!("restore activation parent sync failed: {sync_err}"),
            rollback_result,
            cleanup_result,
        ));
    }

    if let Some(backup) = backup {
        remove_path_if_exists_and_sync_parent(backup).map_err(|cleanup_err| {
            TsinkError::Other(format!(
                "restore succeeded but failed to remove backup {}: {cleanup_err}",
                backup.display()
            ))
        })?;
    }
    Ok(())
}

fn restore_backup_after_failed_activation(
    target: &Path,
    staging: &Path,
    backup: Option<&Path>,
    parent: &Path,
    activation_published: bool,
) -> Result<()> {
    if activation_published {
        rename_path(target, staging)?;
    }
    if let Some(backup) = backup {
        rename_path(backup, target)?;
    }
    sync_dir(parent)
}

fn combine_restore_failure(
    primary: String,
    rollback: Result<()>,
    cleanup: Result<()>,
) -> TsinkError {
    let mut errors = vec![primary];
    if let Err(err) = rollback {
        errors.push(format!("rollback failed: {err}"));
    }
    if let Err(err) = cleanup {
        errors.push(format!("staging cleanup failed: {err}"));
    }
    TsinkError::Other(errors.join("; "))
}

pub(super) fn merge_loaded_segment_indexes(
    mut numeric: LoadedSegmentIndexes,
    mut blob: LoadedSegmentIndexes,
    numeric_lane_enabled: bool,
    blob_lane_enabled: bool,
) -> Result<LoadedSegmentIndexes> {
    let mut series_by_id = BTreeMap::new();
    for series in numeric.series.drain(..) {
        series_by_id.insert(series.series_id, series);
    }

    for series in blob.series.drain(..) {
        match series_by_id.get(&series.series_id) {
            Some(existing)
                if existing.metric == series.metric && existing.labels == series.labels => {}
            Some(_) => {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} conflicts across lane segment families",
                    series.series_id
                )));
            }
            None => {
                series_by_id.insert(series.series_id, series);
            }
        }
    }

    let numeric_has_segments = !numeric.indexed_segments.is_empty();
    let blob_has_segments = !blob.indexed_segments.is_empty();

    let mut indexed_segments = numeric.indexed_segments;
    indexed_segments.append(&mut blob.indexed_segments);
    indexed_segments.sort_by_key(|segment| (segment.manifest.level, segment.manifest.segment_id));

    let replay_highwater = match (numeric_lane_enabled, blob_lane_enabled) {
        (true, true) => match (numeric_has_segments, blob_has_segments) {
            (true, true) => numeric.wal_replay_highwater.min(blob.wal_replay_highwater),
            _ => WalHighWatermark::default(),
        },
        (true, false) => numeric.wal_replay_highwater,
        (false, true) => blob.wal_replay_highwater,
        (false, false) => WalHighWatermark::default(),
    };

    Ok(LoadedSegmentIndexes {
        next_segment_id: numeric.next_segment_id.max(blob.next_segment_id).max(1),
        series: series_by_id.into_values().collect(),
        indexed_segments,
        wal_replay_highwater: replay_highwater,
    })
}
