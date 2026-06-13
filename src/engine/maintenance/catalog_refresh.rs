use super::super::tiering::{self, PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry};
use super::super::{ChunkStorage, HashSet, PathBuf, Result, StorageRuntimeMode};
use super::*;

mod context;
mod pipeline;
mod publication;

use self::context::CatalogRefreshContext;

impl ChunkStorage {
    fn shared_remote_segment_inventory(&self, inventory: &SegmentInventory) -> SegmentInventory {
        let Some(config) = &self.persisted.tiered_storage else {
            return inventory.clone();
        };

        let entries = inventory
            .entries()
            .iter()
            .filter_map(|entry| {
                let shared_lane_root = config.lane_path(entry.lane, entry.tier);
                if entry.root.starts_with(&shared_lane_root) {
                    return Some(entry.clone());
                }

                if entry.tier != PersistedSegmentTier::Hot || !config.mirror_hot_segments {
                    return None;
                }

                Some(SegmentInventoryEntry {
                    root: tiering::destination_segment_root(
                        config,
                        entry.lane,
                        PersistedSegmentTier::Hot,
                        &entry.manifest,
                    ),
                    ..entry.clone()
                })
            })
            .collect::<Vec<_>>();
        SegmentInventory::from_entries(entries)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in super::super) fn refresh_segment_catalog_and_observability(&self) -> Result<()> {
        let inventory = self
            .catalog_refresh_context()
            .runtime_refresh_segment_inventory()?;
        let transition = PersistedCatalogTransition {
            visibility_fence: None,
            loaded_segments: Vec::new(),
            removed_roots: Vec::new(),
            publication: PersistedCatalogPublication::Inventory {
                inventory: inventory.clone(),
                tombstones: None,
            },
            registry_catalog_sources: Some(registry_catalog::inventory_sources(&inventory)),
        };
        let publication = self.begin_persisted_catalog_publication();
        match publication.publish_transition(transition)? {
            PersistedCatalogRefreshApply::Applied => Ok(()),
            PersistedCatalogRefreshApply::SkippedStaleVisibleState => {
                unreachable!("direct segment catalog refresh should not use a visibility fence")
            }
        }
    }

    pub(in super::super) fn refresh_segment_catalog_and_observability_from_persisted_state(
        &self,
        published_segment_roots: &[PathBuf],
    ) -> Result<()> {
        let inventory = self.persisted_segment_inventory();
        if !published_segment_roots.is_empty() {
            let published_roots = published_segment_roots
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            let published_entries = inventory
                .entries()
                .iter()
                .filter(|entry| published_roots.contains(&entry.root))
                .cloned()
                .collect::<Vec<_>>();
            self.mirror_segment_inventory_entries_if_configured(&published_entries)?;
        }
        self.publish_segment_inventory(&inventory)
    }

    pub(in super::super) fn load_scanned_catalog_refresh(
        &self,
    ) -> Result<LoadedInventoryCatalogRefresh> {
        self.catalog_refresh_context()
            .load_scanned_catalog_refresh_phase()
    }

    pub(in super::super) fn plan_known_dirty_catalog_refresh(
        &self,
    ) -> Result<Option<PlannedPersistedCatalogRefresh>> {
        let ctx = self.catalog_refresh_context();
        let diff = ctx.take_known_persisted_segment_changes();
        if diff.is_empty() {
            return Ok(None);
        }

        match ctx.load_known_dirty_catalog_refresh_segments_phase(&diff) {
            Ok(loaded_segments) => Ok(Some(PlannedPersistedCatalogRefresh::KnownDirty(
                PlannedKnownDirtyCatalogRefresh {
                    diff,
                    loaded_segments,
                },
            ))),
            Err(err) => {
                ctx.restore_known_persisted_segment_changes(diff);
                Err(err)
            }
        }
    }

    pub(in super::super) fn apply_known_dirty_persisted_refresh_if_pending(&self) -> Result<bool> {
        let ctx = self.catalog_refresh_context();
        let Some(planned) = self.plan_known_dirty_catalog_refresh()? else {
            return Ok(false);
        };

        let restore_diff = planned.restore_known_dirty_diff();
        let publication = self.begin_persisted_catalog_publication();
        let apply_result = match publication.apply_planned_refresh(planned) {
            Ok(result) => result,
            Err(err) => {
                if let Some(restore_diff) = restore_diff {
                    ctx.restore_known_persisted_segment_changes(restore_diff);
                }
                ctx.set_persisted_index_dirty(true);
                return Err(err);
            }
        };
        if !apply_result.is_applied() {
            unreachable!("known dirty catalog refresh should not use a visibility fence");
        }

        ctx.set_persisted_index_dirty(ctx.has_known_persisted_segment_changes());
        Ok(true)
    }

    pub(in super::super) fn refresh_dirty_persisted_segments_claimed(&self) -> Result<()> {
        if self.apply_known_dirty_persisted_refresh_if_pending()? {
            return Ok(());
        }

        let _compaction_guard = self.compaction_gate();
        if self.apply_known_dirty_persisted_refresh_if_pending()? {
            return Ok(());
        }

        let loaded = self.load_scanned_catalog_refresh()?;
        let planned = self
            .catalog_refresh_context()
            .plan_loaded_inventory_catalog_refresh_phase(loaded)?;
        let publication = self.begin_persisted_catalog_publication();
        if publication.apply_planned_refresh(planned)?.is_applied() {
            self.catalog_refresh_context()
                .set_persisted_index_dirty(false);
        }
        Ok(())
    }

    fn refresh_remote_catalog_claimed(&self) -> Result<()> {
        let ctx = self.catalog_refresh_context();
        let planned = ctx.plan_loaded_inventory_catalog_refresh_phase(
            ctx.load_remote_catalog_refresh_phase()?,
        )?;
        let publication = self.begin_persisted_catalog_publication();
        if publication.apply_planned_refresh(planned)?.is_applied() {
            ctx.mark_remote_catalog_refresh_success();
        }
        Ok(())
    }

    pub(in super::super) fn sync_persisted_segments_from_disk_if_dirty(&self) -> Result<()> {
        let ctx = self.catalog_refresh_context();
        if ctx.should_refresh_remote_catalog() {
            let Some(_refresh_claim) = ctx.try_claim_persisted_refresh() else {
                return Ok(());
            };
            if !ctx.should_refresh_remote_catalog() {
                return Ok(());
            }
            match self.refresh_remote_catalog_claimed() {
                Ok(()) => return Ok(()),
                Err(err) => {
                    let backoff =
                        self.mark_remote_catalog_refresh_error("remote catalog refresh", &err);
                    tracing::warn!(
                        error = %err,
                        retry_after_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                        "Remote catalog refresh failed; serving the last visible catalog until retry"
                    );
                    return Ok(());
                }
            }
        }

        if !ctx.persisted_index_dirty() {
            return Ok(());
        }

        let Some(_refresh_claim) = ctx.try_claim_persisted_refresh() else {
            return Ok(());
        };
        if !ctx.persisted_index_dirty() {
            return Ok(());
        }

        self.refresh_dirty_persisted_segments_claimed()
    }
}
