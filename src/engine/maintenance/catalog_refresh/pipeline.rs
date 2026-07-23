use super::*;

impl<'a> CatalogRefreshContext<'a> {
    pub(super) fn load_known_dirty_catalog_refresh_segments_phase(
        self,
        diff: &PendingPersistedSegmentDiff,
    ) -> Result<Vec<IndexedSegment>> {
        let mut loaded_segments = Vec::with_capacity(diff.added_roots.len());
        for root in &diff.added_roots {
            loaded_segments.push(ChunkStorage::load_segment_index_for_runtime_refresh(root)?);
        }
        Ok(loaded_segments)
    }

    pub(super) fn plan_loaded_inventory_catalog_refresh_phase(
        self,
        loaded: LoadedInventoryCatalogRefresh,
    ) -> Result<PlannedPersistedCatalogRefresh> {
        let current_roots = loaded.inventory.root_set();
        let added_roots = current_roots
            .difference(&loaded.visible_roots)
            .cloned()
            .collect::<Vec<_>>();
        let removed_roots = loaded
            .visible_roots
            .difference(&current_roots)
            .cloned()
            .collect::<Vec<_>>();

        let mut loaded_segments = Vec::with_capacity(added_roots.len());
        for root in &added_roots {
            loaded_segments.push(ChunkStorage::load_segment_index_for_runtime_refresh(root)?);
        }

        Ok(PlannedPersistedCatalogRefresh::Inventory(
            PlannedInventoryCatalogRefresh {
                visibility_fence: loaded.visibility_fence,
                inventory: loaded.inventory,
                loaded_segments,
                removed_roots,
            },
        ))
    }
}

impl ChunkStorage {
    pub(in super::super) fn plan_persisted_catalog_transition_phase(
        &self,
        planned: PlannedPersistedCatalogRefresh,
    ) -> Result<PersistedCatalogTransition> {
        match planned {
            PlannedPersistedCatalogRefresh::KnownDirty(planned) => {
                let added_roots = planned.diff.added_roots.into_iter().collect::<Vec<_>>();
                let removed_roots = planned.diff.removed_roots.into_iter().collect::<Vec<_>>();
                let registry_catalog_delta = self
                    .persisted_registry_catalog_delta_for_root_changes(
                        &added_roots,
                        &removed_roots,
                    )?;
                Ok(PersistedCatalogTransition {
                    visibility_fence: None,
                    loaded_segments: planned.loaded_segments,
                    removed_roots,
                    publication: PersistedCatalogPublication::PersistedState {
                        published_segment_roots: added_roots,
                        refresh_tombstones: false,
                    },
                    registry_catalog_update: Some(
                        registry_catalog::PersistedRegistryCatalogUpdate::Delta(
                            registry_catalog_delta,
                        ),
                    ),
                })
            }
            PlannedPersistedCatalogRefresh::Inventory(planned) => Ok(PersistedCatalogTransition {
                visibility_fence: Some(planned.visibility_fence),
                loaded_segments: planned.loaded_segments,
                removed_roots: planned.removed_roots,
                publication: PersistedCatalogPublication::Inventory {
                    inventory: planned.inventory.clone(),
                    refresh_tombstones: true,
                },
                registry_catalog_update: Some(
                    registry_catalog::PersistedRegistryCatalogUpdate::Complete(
                        registry_catalog::inventory_sources(&planned.inventory),
                    ),
                ),
            }),
        }
    }
}
