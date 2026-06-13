use super::*;
use crate::engine::fs_utils::{
    copy_dir_contents, path_exists_no_follow, remove_path_if_exists_and_sync_parent,
    rename_and_sync_parents, stage_dir_path, sync_dir,
};
use crate::engine::segment::{LoadedSegmentIndexes, WalHighWatermark};

#[path = "bootstrap/discovery.rs"]
mod discovery;
#[path = "bootstrap/finalize.rs"]
mod finalize;
#[path = "bootstrap/hydrate.rs"]
mod hydrate;
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
    let runtime_inputs = plan.into_runtime_inputs();

    let storage = StartupHydrationPhase::create_storage(
        builder.chunk_points(),
        storage_options,
        &paths,
        next_segment_id,
        wal,
    )?;
    StartupHydrationPhase::hydrate(
        storage.as_ref(),
        &builder,
        recovered,
        runtime_inputs.data_path_process_lock,
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
    if !snapshot_meta.is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a directory: {}",
            snapshot_path.display()
        )));
    }

    let Some(parent) = data_path.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "restore target has no parent directory: {}",
            data_path.display()
        )));
    };
    std::fs::create_dir_all(parent)?;

    let staging = stage_dir_path(data_path, "restore-staging")?;
    std::fs::create_dir_all(&staging)?;
    if let Err(err) = copy_dir_contents(snapshot_path, &staging) {
        let _ = remove_path_if_exists_and_sync_parent(&staging);
        return Err(err);
    }

    sync_dir(&staging)?;

    let backup = if path_exists_no_follow(data_path)? {
        Some(stage_dir_path(data_path, "restore-backup")?)
    } else {
        None
    };

    if let Some(backup_path) = backup.as_ref() {
        if let Err(err) = rename_and_sync_parents(data_path, backup_path) {
            let _ = remove_path_if_exists_and_sync_parent(&staging);
            return Err(err);
        }
    }

    if let Err(activate_err) = rename_and_sync_parents(&staging, data_path) {
        let mut rollback_err = None;
        if let Some(backup_path) = backup.as_ref() {
            if let Err(err) = rename_and_sync_parents(backup_path, data_path) {
                rollback_err = Some(err);
            }
        }
        let _ = remove_path_if_exists_and_sync_parent(&staging);

        if let Some(rollback_err) = rollback_err {
            return Err(TsinkError::Other(format!(
                "restore activation failed: {activate_err}; rollback failed: {rollback_err}"
            )));
        }
        return Err(activate_err);
    }

    if let Some(backup_path) = backup {
        if let Err(cleanup_err) = remove_path_if_exists_and_sync_parent(&backup_path) {
            return Err(TsinkError::Other(format!(
                "restore succeeded but failed to remove backup {}: {cleanup_err}",
                backup_path.display()
            )));
        }
    }

    Ok(())
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
