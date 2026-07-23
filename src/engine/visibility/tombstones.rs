use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
use super::*;
use crate::engine::tombstone::TOMBSTONES_FILE_NAME;
use std::path::Path;

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstoneIndexContext<'a> {
    pub(in crate::engine::storage_engine) data_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) numeric_lane_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) blob_lane_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) tiered_storage:
        Option<&'a super::super::config::TieredStorageConfig>,
    pub(in crate::engine::storage_engine) runtime_mode: StorageRuntimeMode,
    pub(in crate::engine::storage_engine) local_disk_budget:
        Option<&'a Arc<crate::LocalDiskBudget>>,
}

impl<'a> TombstoneIndexContext<'a> {
    fn tombstone_index_lanes(self, include_tiered_storage: bool) -> Vec<tombstone::TombstoneLane> {
        let mut lanes = Vec::new();
        if let Some(path) = self.numeric_lane_path {
            lanes.push(tombstone::TombstoneLane {
                role: tombstone::TombstoneLaneRole::LocalNumeric,
                namespace_root: self
                    .data_path
                    .or_else(|| path.parent())
                    .unwrap_or(path)
                    .to_path_buf(),
                manifest_path: path.join(TOMBSTONES_FILE_NAME),
            });
        }
        if let Some(path) = self.blob_lane_path {
            lanes.push(tombstone::TombstoneLane {
                role: tombstone::TombstoneLaneRole::LocalBlob,
                namespace_root: self
                    .data_path
                    .or_else(|| path.parent())
                    .unwrap_or(path)
                    .to_path_buf(),
                manifest_path: path.join(TOMBSTONES_FILE_NAME),
            });
        }
        if include_tiered_storage {
            if let Some(config) = self.tiered_storage {
                for (tier, numeric_role, blob_role) in [
                    (
                        PersistedSegmentTier::Hot,
                        tombstone::TombstoneLaneRole::HotNumeric,
                        tombstone::TombstoneLaneRole::HotBlob,
                    ),
                    (
                        PersistedSegmentTier::Warm,
                        tombstone::TombstoneLaneRole::WarmNumeric,
                        tombstone::TombstoneLaneRole::WarmBlob,
                    ),
                    (
                        PersistedSegmentTier::Cold,
                        tombstone::TombstoneLaneRole::ColdNumeric,
                        tombstone::TombstoneLaneRole::ColdBlob,
                    ),
                ] {
                    lanes.push(tombstone::TombstoneLane {
                        role: numeric_role,
                        namespace_root: config.object_store_root.clone(),
                        manifest_path: config
                            .lane_path(SegmentLaneFamily::Numeric, tier)
                            .join(TOMBSTONES_FILE_NAME),
                    });
                    lanes.push(tombstone::TombstoneLane {
                        role: blob_role,
                        namespace_root: config.object_store_root.clone(),
                        manifest_path: config
                            .lane_path(SegmentLaneFamily::Blob, tier)
                            .join(TOMBSTONES_FILE_NAME),
                    });
                }
            }
        }
        lanes
    }

    fn tombstone_index_load_lanes(self) -> Vec<tombstone::TombstoneLane> {
        self.tombstone_index_lanes(true)
    }

    fn tombstone_index_persist_lanes(self) -> Vec<tombstone::TombstoneLane> {
        self.tombstone_index_lanes(self.runtime_mode != StorageRuntimeMode::ComputeOnly)
    }

    fn transaction_data_path(self) -> Result<&'a Path> {
        self.data_path.ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "durable tombstone publication requires a process-leased data path".to_string(),
            )
        })
    }

    fn has_durable_tombstone_state(self) -> Result<bool> {
        if let Some(data_path) = self.data_path {
            let coordinator = data_path
                .join(tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
                .join(tombstone::TOMBSTONE_TRANSACTION_FILE_NAME);
            if crate::engine::fs_utils::path_exists_no_follow(&coordinator)? {
                return Ok(true);
            }
        }
        for lane in self.tombstone_index_persist_lanes() {
            if crate::engine::fs_utils::path_exists_no_follow(&lane.manifest_path)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::engine::storage_engine) fn recover_pending_transaction(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<tombstone::TombstoneRecoveryOutcome> {
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(tombstone::TombstoneRecoveryOutcome::NoTransaction);
        }
        tombstone::recover_tombstone_transaction_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            self.local_disk_budget,
            |bytes| reservation.resize(bytes),
        )
    }

    pub(in crate::engine::storage_engine) fn read_tombstones_index(
        self,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<TombstoneMap> {
        let mut merged = TombstoneMap::new();
        let lanes = tombstone::normalize_tombstone_lanes(&self.tombstone_index_load_lanes())?;
        if lanes.is_empty() {
            return Ok(merged);
        }
        tombstone::validate_tombstone_lanes(&lanes)?;
        for lane in lanes {
            let live = ChunkStorage::tombstone_map_memory_usage_bytes(&merged);
            let loaded = tombstone::load_tombstones_with_memory_admission(
                &lane.manifest_path,
                |lane_peak| reservation.resize(live.saturating_add(lane_peak)),
            )?;
            let loaded_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&loaded);
            // HashMap growth can temporarily retain its predecessor allocation. Keep an extra
            // decoded-map charge until the merged map has reached its measured final capacity.
            reservation.resize(
                live.saturating_mul(3)
                    .saturating_add(loaded_bytes.saturating_mul(2))
                    .saturating_add(4096),
            )?;
            for (series_id, ranges) in loaded {
                for range in ranges {
                    tombstone::merge_tombstone_range(merged.entry(series_id).or_default(), range);
                }
            }
            reservation.resize(ChunkStorage::tombstone_map_memory_usage_bytes(&merged))?;
        }
        Ok(merged)
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index_updates(
        self,
        updates: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> tombstone::TombstonePersistenceResult<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(());
        }
        tombstone::persist_tombstone_updates_across_paths_with_disk_budget_outcome_and_memory_admission(
            self.transaction_data_path()
                .map_err(tombstone::TombstonePersistenceError::definitively_clean)?,
            &lanes,
            updates,
            self.local_disk_budget,
            |bytes| reservation.ensure(bytes),
        )
    }

    pub(in crate::engine::storage_engine) fn transaction_probe_memory_upper_bound(
        self,
    ) -> Result<usize> {
        tombstone::tombstone_transaction_probe_memory_upper_bound(
            &self.tombstone_index_persist_lanes(),
        )
    }

    pub(in crate::engine::storage_engine) fn transaction_staging_memory_upper_bound(
        self,
        updates: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<usize> {
        tombstone::tombstone_transaction_staging_memory_upper_bound_with_admission(
            &self.tombstone_index_persist_lanes(),
            updates,
            |bytes| reservation.ensure(bytes),
        )
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index_snapshot_for_recovery(
        self,
        snapshot: &TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let lanes = self.tombstone_index_persist_lanes();
        if lanes.is_empty() {
            return Ok(());
        }
        match tombstone::persist_tombstone_snapshot_transactionally_with_memory_admission(
            self.transaction_data_path()?,
            &lanes,
            snapshot,
            self.local_disk_budget,
            |bytes| reservation.ensure(bytes),
        ) {
            Ok(()) => Ok(()),
            Err(error) if error.is_committed() => {
                let error = error.into_tsink_error();
                tracing::warn!(
                    error = %error,
                    "Committed tombstone recovery snapshot left durable coordinator recovery debt"
                );
                Ok(())
            }
            Err(error) => Err(error.into_tsink_error()),
        }
    }

    pub(in crate::engine::storage_engine) fn ensure_delete_tombstone_persistence_supported(
        self,
    ) -> Result<()> {
        if self.runtime_mode == StorageRuntimeMode::ComputeOnly {
            return Err(TsinkError::UnsupportedOperation {
                operation: "delete_series",
                reason: "compute-only storage mode cannot durably persist delete tombstones; send the request to a read-write node".to_string(),
            });
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstoneReadContext<'a> {
    pub(in crate::engine::storage_engine) tombstones: &'a RwLock<TombstoneMap>,
}

impl<'a> TombstoneReadContext<'a> {
    pub(in crate::engine::storage_engine) fn snapshot(self) -> TombstoneMap {
        self.tombstones.read().clone()
    }

    pub(in crate::engine::storage_engine) fn with_tombstones<R>(
        self,
        f: impl FnOnce(&TombstoneMap) -> R,
    ) -> R {
        let tombstones = self.tombstones.read();
        f(&tombstones)
    }

    pub(in crate::engine::storage_engine) fn max_tombstoned_series_id(self) -> Option<SeriesId> {
        self.tombstones.read().keys().copied().max()
    }

    pub(in crate::engine::storage_engine) fn with_series_tombstone_ranges<R>(
        self,
        series_id: SeriesId,
        f: impl FnOnce(Option<&[TombstoneRange]>) -> R,
    ) -> R {
        let tombstones = self.tombstones.read();
        f(tombstones.get(&series_id).map(Vec::as_slice))
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstonePublicationContext<'a> {
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
    pub(in crate::engine::storage_engine) tombstones: &'a RwLock<TombstoneMap>,
    pub(in crate::engine::storage_engine) tombstone_used_bytes: &'a AtomicU64,
}

impl<'a> TombstonePublicationContext<'a> {
    fn reserve_series_ids_referenced_by_tombstones(self, tombstones: &TombstoneMap) -> Result<()> {
        let Some(max_series_id) = tombstones.keys().copied().max() else {
            return Ok(());
        };
        self.registry.write().reserve_series_id(max_series_id)
    }

    pub(in crate::engine::storage_engine) fn timestamp_survives_tombstones(
        timestamp: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> bool {
        !tombstone_ranges
            .is_some_and(|ranges| tombstone::timestamp_is_tombstoned(timestamp, ranges))
    }

    pub(in crate::engine::storage_engine) fn admit_loaded_tombstone_publication(
        self,
        storage: &ChunkStorage,
        merged: &TombstoneMap,
        transition_visibility_headroom: usize,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let merged_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(merged);
        let changed_count = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .count()
        };
        if changed_count == 0 {
            return reservation.ensure(merged_bytes.saturating_add(transition_visibility_headroom));
        }
        let changed_set_upper_bound = changed_count.saturating_mul(128).saturating_add(4096);
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(transition_visibility_headroom),
        )?;
        let changed_series_ids = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(changed_series_ids.iter());
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(visibility_staging)
                .saturating_add(transition_visibility_headroom),
        )?;
        self.reserve_series_ids_referenced_by_tombstones(merged)
    }

    pub(in crate::engine::storage_engine) fn replace_loaded_tombstones_index(
        self,
        storage: &ChunkStorage,
        merged: TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let _visibility_guard = storage.visibility_write_fence();
        self.replace_loaded_tombstones_index_locked(storage, merged, reservation)
    }

    pub(in crate::engine::storage_engine) fn replace_loaded_tombstones_index_locked(
        self,
        storage: &ChunkStorage,
        merged: TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> Result<()> {
        let merged_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&merged);
        let changed_count = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .count()
        };
        if changed_count == 0 {
            return Ok(());
        }
        let changed_set_upper_bound = changed_count.saturating_mul(128).saturating_add(4096);
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(4096),
        )?;

        let changed_series_ids = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(changed_series_ids.iter());
        reservation.ensure(
            merged_bytes
                .saturating_add(changed_set_upper_bound)
                .saturating_add(visibility_staging),
        )?;

        // The registry and visible tombstone/cache state remain untouched until every
        // publication allocation has passed admission.
        self.reserve_series_ids_referenced_by_tombstones(&merged)?;

        let mut tombstones = self.tombstones.write();
        storage.with_included_memory_delta(
            self.tombstone_used_bytes,
            &mut tombstones,
            |tombstones| ChunkStorage::tombstone_map_memory_usage_bytes(tombstones),
            |tombstones| **tombstones = merged,
        );
        drop(tombstones);
        #[cfg(test)]
        storage.invoke_tombstone_post_swap_pre_visibility_hook();
        #[cfg(test)]
        let refresh_result = storage
            .invoke_tombstone_post_commit_error_hook()
            .and_then(|()| {
                storage.refresh_series_visible_timestamp_cache_locked(
                    changed_series_ids.iter().copied(),
                )
            });
        #[cfg(not(test))]
        let refresh_result = storage
            .refresh_series_visible_timestamp_cache_locked(changed_series_ids.iter().copied());
        if let Err(err) = refresh_result {
            // The authoritative map has already swapped. Clear every affected summary before
            // returning success so readers rebuild from the new tombstone state; a retry may see
            // no map diff and therefore cannot be relied on to repair stale cache entries.
            storage.clear_series_visible_timestamp_cache(changed_series_ids.iter().copied());
            tracing::warn!(
                error = %err,
                "Tombstone reload deferred series visibility summary rebuild"
            );
        }
        storage.bump_visibility_state_generation();
        Ok(())
    }

    // Caller must hold the visibility write fence so rollup invalidation and tombstone
    // publication become visible to readers as one transition.
    pub(in crate::engine::storage_engine) fn publish_tombstone_updates_locked(
        self,
        storage: &ChunkStorage,
        index: TombstoneIndexContext<'a>,
        updates: TombstoneMap,
        reservation: &mut super::super::maintenance::TombstoneMemoryReservation<'_>,
    ) -> tombstone::TombstonePersistenceResult<()> {
        let update_bytes = ChunkStorage::tombstone_map_memory_usage_bytes(&updates);
        let visibility_staging =
            storage.series_visibility_refresh_staging_upper_bound(updates.keys());
        let changed_set_upper_bound = updates.len().saturating_mul(128).saturating_add(4096);
        let live_rehash_headroom = {
            let current = self.tombstones.read();
            ChunkStorage::tombstone_map_memory_usage_bytes(&current)
                .saturating_mul(2)
                .saturating_add(update_bytes.saturating_mul(2))
        };
        reservation
            .ensure(
                update_bytes
                    .saturating_add(visibility_staging)
                    .saturating_add(changed_set_upper_bound)
                    .saturating_add(live_rehash_headroom),
            )
            .map_err(tombstone::TombstonePersistenceError::definitively_clean)?;
        // Reserve the registry high-water mark before the durable commit boundary. The only
        // possible failure is series-id exhaustion; reporting that after manifest publication
        // would incorrectly present an effective delete as rejected.
        self.reserve_series_ids_referenced_by_tombstones(&updates)
            .map_err(tombstone::TombstonePersistenceError::definitively_clean)?;
        storage
            .validate_shared_object_store_writer_lock()
            .map_err(tombstone::TombstonePersistenceError::definitively_clean)?;
        match index.persist_tombstones_index_updates(&updates, reservation) {
            Ok(()) => {}
            Err(error) if error.is_committed() => {
                let error = error.into_tsink_error();
                tracing::warn!(
                    error = %error,
                    "Committed tombstone transaction left durable coordinator recovery debt"
                );
            }
            Err(error) => return Err(error),
        }
        self.apply_tombstone_updates_locked(storage, updates)
            .map_err(tombstone::TombstonePersistenceError::indeterminate)
    }

    fn apply_tombstone_updates_locked(
        self,
        storage: &ChunkStorage,
        updates: TombstoneMap,
    ) -> Result<()> {
        let changed_series_ids = updates.keys().copied().collect::<BTreeSet<_>>();
        let mut tombstones = self.tombstones.write();
        storage.with_included_memory_delta(
            self.tombstone_used_bytes,
            &mut tombstones,
            |tombstones| ChunkStorage::tombstone_map_memory_usage_bytes(tombstones),
            |tombstones| {
                for (series_id, ranges) in updates {
                    if ranges.is_empty() {
                        tombstones.remove(&series_id);
                    } else {
                        tombstones.insert(series_id, ranges);
                    }
                }
            },
        );
        drop(tombstones);
        #[cfg(test)]
        storage.invoke_tombstone_post_swap_pre_visibility_hook();
        #[cfg(test)]
        let refresh_result = storage
            .invoke_tombstone_post_commit_error_hook()
            .and_then(|()| {
                storage.refresh_series_visible_timestamp_cache_locked(
                    changed_series_ids.iter().copied(),
                )
            });
        #[cfg(not(test))]
        let refresh_result = storage
            .refresh_series_visible_timestamp_cache_locked(changed_series_ids.iter().copied());
        if let Err(err) = refresh_result {
            // The disk manifests and tombstone map have committed. Drop stale summaries so later
            // readers either rebuild them or surface the underlying storage error, while this
            // delete retains honest committed-success semantics.
            storage.clear_series_visible_timestamp_cache(changed_series_ids.iter().copied());
            tracing::warn!(
                error = %err,
                "Committed delete deferred series visibility summary rebuild"
            );
        }
        storage.bump_visibility_state_generation();
        Ok(())
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn recover_and_reload_tombstones_locked(
        &self,
    ) -> Result<bool> {
        self.validate_shared_object_store_writer_lock()?;
        let index = self.tombstone_index_context();
        self.refresh_memory_usage();
        let mut reservation = self.tombstone_memory_reservation();
        let recovery = index.recover_pending_transaction(&mut reservation)?;
        if !recovery.requires_authoritative_reload() {
            return Ok(false);
        }
        let merged = index.read_tombstones_index(&mut reservation)?;
        self.tombstone_publication_context()
            .replace_loaded_tombstones_index_locked(self, merged, &mut reservation)?;
        Ok(true)
    }

    pub(in crate::engine::storage_engine) fn tombstone_index_context(
        &self,
    ) -> TombstoneIndexContext<'_> {
        TombstoneIndexContext {
            data_path: self
                .persisted
                .series_index_path
                .as_deref()
                .and_then(Path::parent),
            numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
            blob_lane_path: self.persisted.blob_lane_path.as_deref(),
            tiered_storage: self.persisted.tiered_storage.as_ref(),
            runtime_mode: self.runtime.runtime_mode,
            local_disk_budget: self.persisted.local_disk_budget.as_ref(),
        }
    }

    pub(in crate::engine::storage_engine) fn tombstone_read_context(
        &self,
    ) -> TombstoneReadContext<'_> {
        TombstoneReadContext {
            tombstones: &self.visibility.tombstones,
        }
    }

    pub(in crate::engine::storage_engine) fn tombstone_publication_context(
        &self,
    ) -> TombstonePublicationContext<'_> {
        TombstonePublicationContext {
            registry: &self.catalog.registry,
            tombstones: &self.visibility.tombstones,
            tombstone_used_bytes: &self.memory.tombstone_used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn load_tombstones_index(&self) -> Result<()> {
        self.refresh_memory_usage();
        let mut reservation = self.tombstone_memory_reservation();
        let merged = self
            .tombstone_index_context()
            .read_tombstones_index(&mut reservation)?;
        self.tombstone_publication_context()
            .replace_loaded_tombstones_index(self, merged, &mut reservation)?;
        drop(reservation);
        Ok(())
    }

    /// Persists the full tombstone snapshot while the caller holds `rollups.run_lock`.
    pub(in crate::engine::storage_engine) fn persist_tombstones_index_for_recovery_locked(
        &self,
    ) -> Result<()> {
        let _visibility_guard = self.visibility_write_fence();
        let index = self.tombstone_index_context();
        let live_is_empty = self
            .tombstone_read_context()
            .with_tombstones(|current| current.is_empty());
        // A brand-new/never-deleted store has nothing to recover or snapshot. Avoid imposing the
        // bounded recovery scanner's fixed scratch floor during close after callers deliberately
        // tighten the live memory budget below that floor.
        if live_is_empty && !index.has_durable_tombstone_state()? {
            return Ok(());
        }
        self.validate_shared_object_store_writer_lock()?;
        self.recover_and_reload_tombstones_locked()?;
        self.refresh_memory_usage();
        let index = self.tombstone_index_context();
        let probe = index.transaction_probe_memory_upper_bound()?;
        let live_bytes = self
            .tombstone_read_context()
            .with_tombstones(ChunkStorage::tombstone_map_memory_usage_bytes);
        let initial_reservation = probe
            .saturating_add(live_bytes.saturating_mul(3))
            .saturating_add(16 * 1024);
        let mut memory_reservation = self.tombstone_memory_reservation();
        memory_reservation.resize(initial_reservation)?;
        let snapshot = self.tombstone_read_context().snapshot();
        let transaction_staging =
            index.transaction_staging_memory_upper_bound(&snapshot, &mut memory_reservation)?;
        memory_reservation.resize(initial_reservation.max(transaction_staging))?;
        let result = index
            .persist_tombstones_index_snapshot_for_recovery(&snapshot, &mut memory_reservation);
        drop(memory_reservation);
        result
    }

    pub(in crate::engine::storage_engine) fn timestamp_survives_tombstones(
        timestamp: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> bool {
        TombstonePublicationContext::timestamp_survives_tombstones(timestamp, tombstone_ranges)
    }
}
