#[path = "post_flush/context.rs"]
mod context;

use super::super::*;
use super::*;

impl ChunkStorage {
    pub(in super::super) fn active_retention_cutoff(&self) -> Option<i64> {
        self.retention_maintenance_context()
            .active_retention_cutoff(self.retention_recency_reference_timestamp())
    }

    pub(in super::super) fn apply_retention_filter(&self, points: &mut Vec<DataPoint>) {
        self.retention_maintenance_context()
            .apply_retention_filter(points, self.retention_recency_reference_timestamp());
    }

    #[cfg(test)]
    pub(in super::super) fn set_post_flush_maintenance_stage_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .post_flush_maintenance_stage_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in super::super) fn clear_post_flush_maintenance_stage_hook(&self) {
        *self
            .persist_test_hooks
            .post_flush_maintenance_stage_hook
            .write() = None;
    }

    pub(in super::super) fn run_post_flush_maintenance_if_pending(&self) -> Result<bool> {
        let workflow = self.post_flush_workflow_context();
        let work = workflow.claim_pending_work();
        if !work.any() {
            return Ok(false);
        }

        if work.run_post_flush {
            if let Err(err) = self.sweep_expired_persisted_segments() {
                workflow.restore_post_flush_pending();
                if work.run_metadata_reconcile {
                    workflow.restore_startup_metadata_reconcile_pending();
                }
                return Err(err);
            }
        }

        if work.run_metadata_reconcile {
            if let Err(err) = self.reconcile_live_metadata_indexes() {
                workflow.restore_startup_metadata_reconcile_pending();
                return Err(err);
            }
        }

        Ok(true)
    }

    pub(in super::super) fn schedule_post_flush_maintenance(&self) -> Result<()> {
        let workflow = self.post_flush_workflow_context();
        if !workflow.retention().enabled() {
            return Ok(());
        }

        workflow.mark_post_flush_pending();
        if self.has_persisted_refresh_thread() {
            self.notify_persisted_refresh_thread();
            return Ok(());
        }

        self.run_post_flush_maintenance_if_pending()?;
        Ok(())
    }

    pub(in super::super) fn schedule_startup_maintenance(&self) {
        self.post_flush_workflow_context()
            .schedule_startup_maintenance();
    }

    pub(in super::super) fn sweep_expired_persisted_segments(&self) -> Result<usize> {
        let retention = self.retention_maintenance_context();
        if !retention.has_persisted_lane_paths() {
            return Ok(0);
        }

        let recency_reference = self.retention_recency_reference_timestamp();
        let Some(cutoff) = retention.active_retention_cutoff(recency_reference) else {
            return Ok(0);
        };
        let policy = retention.retention_tier_policy(cutoff, recency_reference);

        let _compaction_guard = self.compaction_gate();
        if retention.catalog_requires_refresh() {
            // Apply any known root diffs before maintenance decides whether it can reuse the
            // published catalog or must fall back to a full inventory scan.
            self.apply_known_dirty_persisted_refresh_if_pending()?;
        }
        let (inventory, inventory_source) = retention.post_flush_maintenance_inventory()?;
        let mut plan = policy.post_flush_maintenance_plan(&inventory);
        if plan.is_empty() {
            let publication = self.begin_persisted_catalog_publication();
            match publication.publish_transition(
                retention.plan_noop_inventory_transition(&inventory, inventory_source),
            )? {
                PersistedCatalogRefreshApply::Applied => return Ok(0),
                PersistedCatalogRefreshApply::SkippedStaleVisibleState => unreachable!(
                    "post-flush maintenance no-op publication should not use a visibility fence"
                ),
            }
        }

        let inventory = if inventory_source == MaintenanceInventorySource::PersistedState {
            let loaded = self.load_scanned_catalog_refresh()?;
            let scanned_inventory = loaded.inventory;
            plan = policy.post_flush_maintenance_plan(&scanned_inventory);
            if plan.is_empty() {
                let publication = self.begin_persisted_catalog_publication();
                match publication.publish_transition(retention.plan_noop_inventory_transition(
                    &scanned_inventory,
                    MaintenanceInventorySource::Scanned,
                ))? {
                    PersistedCatalogRefreshApply::Applied => return Ok(0),
                    PersistedCatalogRefreshApply::SkippedStaleVisibleState => unreachable!(
                        "post-flush maintenance no-op publication should not use a visibility fence"
                    ),
                }
            }
            scanned_inventory
        } else {
            inventory
        };

        let staged = retention.stage_post_flush_maintenance(&inventory, plan, policy)?;
        let StagedPostFlushMaintenance {
            final_inventory,
            promotions,
            staging_cleanup_paths,
            loaded_segments,
            removed_roots,
            retired_roots,
            tier_moves,
        } = staged;
        let transition = retention.plan_inventory_publication_transition(
            final_inventory,
            loaded_segments,
            removed_roots,
        );

        {
            // Publish staged retention/tiering outputs before retiring old roots so queries never
            // observe a window where superseded data disappeared before its replacement became
            // visible.
            let publication = self.begin_persisted_catalog_publication();
            let mut promoted_roots: Vec<PathBuf> = Vec::new();
            for promotion in &promotions {
                if let Err(err) = retention.promote_staged_segment_for_publish(promotion) {
                    drop(publication);
                    let _ = self.rollback_published_segment_roots(&promoted_roots);
                    retention.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                    return Err(err);
                }
                promoted_roots.push(promotion.final_root.clone());
            }

            if let Err(err) = publication.publish_transition(transition) {
                drop(publication);
                let rollback_result = self.rollback_published_segment_roots(&promoted_roots);
                retention.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                return match rollback_result {
                    Ok(()) => Err(err),
                    Err(rollback_err) => Err(TsinkError::Other(format!(
                        "post-flush publication failed and rollback failed: publish={err}, rollback={rollback_err}"
                    ))),
                };
            }
            self.evict_persisted_sealed_chunks();
            retention.record_tier_moves(tier_moves);
        }

        self.reconcile_live_metadata_indexes()?;

        for path in &staging_cleanup_paths {
            crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(path)?;
        }

        retention.finalize_retired_roots(&retired_roots)
    }
}
