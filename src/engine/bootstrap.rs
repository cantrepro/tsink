use super::*;
use tracing::warn;

use crate::engine::fs_utils::{copy_dir_contents, remove_path_if_exists, stage_dir_path};
use crate::engine::segment::load_segment_indexes_with_series;

pub(super) fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    let wal_enabled = builder.wal_enabled();
    let storage_options = ChunkStorageOptions::from(&builder);
    let config::StoragePathLayout {
        numeric_lane_path,
        blob_lane_path,
        series_index_path,
        wal_path,
    } = config::StoragePathLayout::from(&builder);
    let persisted_registry = if let Some(index_path) = &series_index_path {
        match SeriesRegistry::load_from_path(index_path) {
            Ok(registry) => Some(registry),
            Err(TsinkError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                warn!(
                    path = %index_path.display(),
                    error = %err,
                    "Failed to load persisted series registry index; rebuilding from segments"
                );
                None
            }
        }
    } else {
        None
    };
    let load_series_from_segments = persisted_registry.is_none();

    let loaded_numeric = if let Some(path) = &numeric_lane_path {
        load_segment_indexes_with_series(path, load_series_from_segments)?
    } else {
        crate::engine::segment::LoadedSegmentIndexes::default()
    };
    let loaded_blob = if let Some(path) = &blob_lane_path {
        load_segment_indexes_with_series(path, load_series_from_segments)?
    } else {
        crate::engine::segment::LoadedSegmentIndexes::default()
    };
    let loaded_segments = merge_loaded_segment_indexes(
        loaded_numeric,
        loaded_blob,
        numeric_lane_path.is_some(),
        blob_lane_path.is_some(),
    )?;
    let replay_highwater = loaded_segments.wal_replay_highwater;

    let wal = if let Some(wal_path) = wal_path {
        if wal_enabled {
            let wal = FramedWal::open_with_buffer_size(
                wal_path,
                builder.wal_sync_mode(),
                builder.wal_buffer_size(),
            )?;
            wal.ensure_min_highwater(replay_highwater)?;
            Some(wal)
        } else {
            remove_path_if_exists(&wal_path)?;
            None
        }
    } else {
        None
    };

    let storage = Arc::new(ChunkStorage::new_with_data_path_and_options(
        builder.chunk_points(),
        wal,
        numeric_lane_path,
        blob_lane_path,
        loaded_segments.next_segment_id,
        storage_options,
    )?);
    if let Some(registry) = persisted_registry {
        storage.replace_registry_from_snapshot(registry);
    }
    storage.apply_loaded_segment_indexes(loaded_segments, !load_series_from_segments)?;
    storage.replay_from_wal(replay_highwater)?;
    storage.sweep_expired_persisted_segments()?;
    storage.persist_series_registry_index()?;
    if storage.memory_budget_value() != usize::MAX {
        storage.refresh_memory_usage();
        storage.enforce_memory_budget_if_needed()?;
    }
    if wal_enabled {
        storage.start_background_flush_thread(DEFAULT_FLUSH_INTERVAL)?;
    }

    Ok(storage as Arc<dyn Storage>)
}

pub(super) fn restore_storage_from_snapshot(snapshot_path: &Path, data_path: &Path) -> Result<()> {
    if snapshot_path == data_path {
        return Err(TsinkError::InvalidConfiguration(
            "snapshot and restore paths must differ".to_string(),
        ));
    }

    let snapshot_meta = std::fs::metadata(snapshot_path).map_err(|err| TsinkError::IoWithPath {
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
        let _ = remove_path_if_exists(&staging);
        return Err(err);
    }

    let backup = if data_path.exists() {
        Some(stage_dir_path(data_path, "restore-backup")?)
    } else {
        None
    };

    if let Some(backup_path) = backup.as_ref() {
        if let Err(err) = std::fs::rename(data_path, backup_path) {
            let _ = remove_path_if_exists(&staging);
            return Err(err.into());
        }
    }

    if let Err(activate_err) = std::fs::rename(&staging, data_path) {
        let mut rollback_err = None;
        if let Some(backup_path) = backup.as_ref() {
            if let Err(err) = std::fs::rename(backup_path, data_path) {
                rollback_err = Some(err);
            }
        }
        let _ = remove_path_if_exists(&staging);

        if let Some(rollback_err) = rollback_err {
            return Err(TsinkError::Other(format!(
                "restore activation failed: {activate_err}; rollback failed: {rollback_err}"
            )));
        }
        return Err(activate_err.into());
    }

    if let Some(backup_path) = backup {
        if let Err(cleanup_err) = remove_path_if_exists(&backup_path) {
            return Err(TsinkError::Other(format!(
                "restore succeeded but failed to remove backup {}: {cleanup_err}",
                backup_path.display()
            )));
        }
    }

    Ok(())
}

pub(super) fn merge_loaded_segment_indexes(
    mut numeric: crate::engine::segment::LoadedSegmentIndexes,
    mut blob: crate::engine::segment::LoadedSegmentIndexes,
    numeric_lane_enabled: bool,
    blob_lane_enabled: bool,
) -> Result<crate::engine::segment::LoadedSegmentIndexes> {
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
            // Both lane families are configured, so one-sided segment visibility can be a
            // failed/crashed split persist. Fall back to full WAL replay to avoid skipping
            // frames needed by the missing lane.
            (true, true) => numeric.wal_replay_highwater.min(blob.wal_replay_highwater),
            _ => WalHighWatermark::default(),
        },
        (true, false) => numeric.wal_replay_highwater,
        (false, true) => blob.wal_replay_highwater,
        (false, false) => WalHighWatermark::default(),
    };

    Ok(crate::engine::segment::LoadedSegmentIndexes {
        next_segment_id: numeric.next_segment_id.max(blob.next_segment_id).max(1),
        series: series_by_id.into_values().collect(),
        indexed_segments,
        wal_replay_highwater: replay_highwater,
    })
}
