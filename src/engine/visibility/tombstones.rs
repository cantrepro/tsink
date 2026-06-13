use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
use super::*;
use crate::engine::tombstone::TOMBSTONES_FILE_NAME;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct TombstoneIndexContext<'a> {
    pub(in crate::engine::storage_engine) numeric_lane_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) blob_lane_path: Option<&'a Path>,
    pub(in crate::engine::storage_engine) tiered_storage:
        Option<&'a super::super::config::TieredStorageConfig>,
    pub(in crate::engine::storage_engine) runtime_mode: StorageRuntimeMode,
}

impl<'a> TombstoneIndexContext<'a> {
    fn tombstone_index_paths(self, include_tiered_storage: bool) -> Vec<PathBuf> {
        let mut paths = BTreeSet::new();
        if let Some(path) = self.numeric_lane_path {
            paths.insert(path.join(TOMBSTONES_FILE_NAME));
        }
        if let Some(path) = self.blob_lane_path {
            paths.insert(path.join(TOMBSTONES_FILE_NAME));
        }
        if include_tiered_storage {
            if let Some(config) = self.tiered_storage {
                for lane in [SegmentLaneFamily::Numeric, SegmentLaneFamily::Blob] {
                    for tier in [
                        PersistedSegmentTier::Hot,
                        PersistedSegmentTier::Warm,
                        PersistedSegmentTier::Cold,
                    ] {
                        paths.insert(config.lane_path(lane, tier).join(TOMBSTONES_FILE_NAME));
                    }
                }
            }
        }
        paths.into_iter().collect()
    }

    fn tombstone_index_load_paths(self) -> Vec<PathBuf> {
        self.tombstone_index_paths(true)
    }

    fn tombstone_index_persist_paths(self) -> Vec<PathBuf> {
        self.tombstone_index_paths(self.runtime_mode != StorageRuntimeMode::ComputeOnly)
    }

    pub(in crate::engine::storage_engine) fn read_tombstones_index(self) -> Result<TombstoneMap> {
        let mut merged = TombstoneMap::new();
        for path in self.tombstone_index_load_paths() {
            let loaded = tombstone::load_tombstones(&path)?;
            for (series_id, ranges) in loaded {
                for range in ranges {
                    tombstone::merge_tombstone_range(merged.entry(series_id).or_default(), range);
                }
            }
        }
        Ok(merged)
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index_updates(
        self,
        updates: &TombstoneMap,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        for path in self.tombstone_index_persist_paths() {
            tombstone::persist_tombstone_updates(&path, updates)?;
        }
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index_snapshot(
        self,
        snapshot: &TombstoneMap,
    ) -> Result<()> {
        for path in self.tombstone_index_persist_paths() {
            tombstone::persist_tombstones(&path, snapshot)?;
        }
        Ok(())
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

    pub(in crate::engine::storage_engine) fn replace_loaded_tombstones_index(
        self,
        storage: &ChunkStorage,
        merged: TombstoneMap,
    ) -> Result<()> {
        let _visibility_guard = storage.visibility_write_fence();
        self.replace_loaded_tombstones_index_locked(storage, merged)
    }

    pub(in crate::engine::storage_engine) fn replace_loaded_tombstones_index_locked(
        self,
        storage: &ChunkStorage,
        merged: TombstoneMap,
    ) -> Result<()> {
        self.reserve_series_ids_referenced_by_tombstones(&merged)?;

        let changed_series_ids = {
            let current = self.tombstones.read();
            current
                .keys()
                .chain(merged.keys())
                .filter(|series_id| current.get(series_id) != merged.get(series_id))
                .copied()
                .collect::<BTreeSet<_>>()
        };
        if changed_series_ids.is_empty() {
            return Ok(());
        }

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
        storage.refresh_series_visible_timestamp_cache_locked(changed_series_ids)?;
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
    ) -> Result<()> {
        index.persist_tombstones_index_updates(&updates)?;
        self.apply_tombstone_updates_locked(storage, updates)
    }

    fn apply_tombstone_updates_locked(
        self,
        storage: &ChunkStorage,
        updates: TombstoneMap,
    ) -> Result<()> {
        self.reserve_series_ids_referenced_by_tombstones(&updates)?;

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
        storage.refresh_series_visible_timestamp_cache_locked(changed_series_ids)?;
        storage.bump_visibility_state_generation();
        Ok(())
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn tombstone_index_context(
        &self,
    ) -> TombstoneIndexContext<'_> {
        TombstoneIndexContext {
            numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
            blob_lane_path: self.persisted.blob_lane_path.as_deref(),
            tiered_storage: self.persisted.tiered_storage.as_ref(),
            runtime_mode: self.runtime.runtime_mode,
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
        let merged = self.tombstone_index_context().read_tombstones_index()?;
        self.tombstone_publication_context()
            .replace_loaded_tombstones_index(self, merged)
    }

    pub(in crate::engine::storage_engine) fn persist_tombstones_index(&self) -> Result<()> {
        self.tombstone_index_context()
            .persist_tombstones_index_snapshot(&self.tombstone_read_context().snapshot())
    }

    pub(in crate::engine::storage_engine) fn timestamp_survives_tombstones(
        timestamp: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> bool {
        TombstonePublicationContext::timestamp_survives_tombstones(timestamp, tombstone_ranges)
    }
}
