use super::*;

impl ChunkStorage {
    pub(super) fn mirror_segment_inventory_entries_if_configured(
        &self,
        entries: &[SegmentInventoryEntry],
    ) -> Result<()> {
        let Some(config) = &self.persisted.tiered_storage else {
            return Ok(());
        };
        if !config.mirror_hot_segments {
            return Ok(());
        }

        for entry in entries {
            if entry.tier != PersistedSegmentTier::Hot {
                continue;
            }
            let destination = tiering::destination_segment_root(
                config,
                entry.lane,
                PersistedSegmentTier::Hot,
                &entry.manifest,
            );
            if destination == entry.root {
                continue;
            }
            tiering::move_segment_to_tier(&entry.root, &destination)?;
        }

        Ok(())
    }

    pub(super) fn publish_segment_inventory(&self, inventory: &SegmentInventory) -> Result<()> {
        let (hot, warm, cold) = inventory.tier_counts();
        self.observability
            .flush
            .hot_segments_visible
            .store(hot, Ordering::Relaxed);
        self.observability
            .flush
            .warm_segments_visible
            .store(warm, Ordering::Relaxed);
        self.observability
            .flush
            .cold_segments_visible
            .store(cold, Ordering::Relaxed);
        if let Some(config) = &self.persisted.tiered_storage {
            if let Some(path) = config.segment_catalog_path.as_deref() {
                tiering::persist_segment_catalog(path, inventory)?;
            }
            if self.runtime.runtime_mode != StorageRuntimeMode::ComputeOnly {
                let shared_inventory = self.shared_remote_segment_inventory(inventory);
                tiering::persist_segment_catalog(
                    &tiering::shared_segment_catalog_path(config),
                    &shared_inventory,
                )?;
            }
        }
        Ok(())
    }

    fn publish_scanned_segment_inventory(&self, inventory: &SegmentInventory) -> Result<()> {
        self.mirror_segment_inventory_entries_if_configured(inventory.entries())?;
        self.publish_segment_inventory(inventory)
    }

    pub(in super::super) fn apply_persisted_catalog_transition_phase(
        &self,
        transition: PersistedCatalogTransition,
        current_visibility_generation: u64,
    ) -> Result<PersistedCatalogRefreshApply> {
        if transition
            .visibility_fence
            .is_some_and(|fence| !fence.matches(current_visibility_generation))
        {
            return Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState);
        }

        self.add_persisted_segments_from_loaded(transition.loaded_segments)?;
        self.remove_persisted_segment_roots(&transition.removed_roots)?;

        match transition.publication {
            PersistedCatalogPublication::PersistedState {
                published_segment_roots,
            } => self.refresh_segment_catalog_and_observability_from_persisted_state(
                &published_segment_roots,
            )?,
            PersistedCatalogPublication::Inventory {
                inventory,
                tombstones,
            } => {
                self.publish_scanned_segment_inventory(&inventory)?;
                if let Some(tombstones) = tombstones {
                    self.tombstone_publication_context()
                        .replace_loaded_tombstones_index_locked(self, tombstones)?;
                }
            }
        }

        if let Some(registry_catalog_sources) = transition.registry_catalog_sources {
            self.persist_series_registry_index_with_catalog_sources(&registry_catalog_sources)?;
        }

        Ok(PersistedCatalogRefreshApply::Applied)
    }
}
