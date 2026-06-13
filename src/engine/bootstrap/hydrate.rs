use tracing::warn;

use super::recovery::StartupRecoveryState;
use super::*;
use crate::engine::segment::StartupQuarantinedSegment;

pub(super) struct StartupHydrationPhase;

impl StartupHydrationPhase {
    pub(super) fn create_storage(
        chunk_points: usize,
        storage_options: ChunkStorageOptions,
        paths: &config::StoragePathLayout,
        next_segment_id: u64,
        wal: Option<FramedWal>,
    ) -> Result<Arc<ChunkStorage>> {
        Ok(Arc::new(ChunkStorage::new_with_data_path_and_options(
            chunk_points,
            wal,
            paths.numeric_lane_path.clone(),
            paths.blob_lane_path.clone(),
            next_segment_id,
            storage_options,
        )?))
    }

    pub(super) fn hydrate(
        storage: &ChunkStorage,
        builder: &StorageBuilder,
        recovered: StartupRecoveryState,
        data_path_process_lock: Option<DataPathProcessLock>,
    ) -> Result<()> {
        let StartupRecoveryState {
            registry,
            loaded_segments,
            startup_quarantined,
        } = recovered;
        let replay_highwater = loaded_segments.wal_replay_highwater;

        storage.load_tombstones_index()?;
        if let Some(data_path_process_lock) = data_path_process_lock {
            storage.install_data_path_process_lock(data_path_process_lock);
        }
        if let Some(loaded_registry) = registry.persisted_registry {
            storage.replace_registry_from_persisted_state(
                loaded_registry.registry,
                loaded_registry.delta_series_count,
            )?;
        }
        storage.apply_loaded_segment_indexes(
            loaded_segments,
            registry.reconcile_registry_with_persisted,
        )?;
        storage.replay_from_wal(replay_highwater, builder.wal_replay_mode())?;
        storage.load_rollup_runtime_state()?;
        storage.refresh_segment_catalog_and_observability_from_persisted_state(&[])?;
        if storage.persisted.tiered_storage.is_some() {
            storage.mark_remote_catalog_refresh_success();
        }
        record_startup_segment_quarantines(storage, &startup_quarantined);

        Ok(())
    }
}

fn record_startup_segment_quarantines(
    storage: &ChunkStorage,
    startup_quarantined: &[StartupQuarantinedSegment],
) {
    for quarantined in startup_quarantined {
        warn!(
            segment = %quarantined.original_root.display(),
            quarantine = %quarantined.quarantined.path.display(),
            details = %quarantined.details,
            "Startup quarantined persisted segment"
        );
        storage.observability.record_maintenance_error(
            "startup persisted segment recovery",
            &TsinkError::DataCorruption(format!(
                "quarantined invalid persisted segment {} at {}: {}",
                quarantined.original_root.display(),
                quarantined.quarantined.path.display(),
                quarantined.details
            )),
        );
    }
}
