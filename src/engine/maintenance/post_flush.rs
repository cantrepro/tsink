#[path = "post_flush/context.rs"]
mod context;
#[path = "post_flush/recovery.rs"]
pub(in crate::engine::storage_engine) mod recovery;

use super::super::*;
use super::*;
use context::RetentionMaintenanceContext;
use recovery::*;

fn post_flush_failure_with_recovery(
    primary: TsinkError,
    rollback: Result<()>,
    cleanup: Result<()>,
) -> TsinkError {
    let mut recovery_errors = Vec::new();
    if let Err(err) = rollback {
        recovery_errors.push(format!("rollback failed: {err}"));
    }
    if let Err(err) = cleanup {
        recovery_errors.push(format!("cleanup failed: {err}"));
    }
    if recovery_errors.is_empty() {
        primary
    } else {
        TsinkError::Other(format!(
            "post-flush operation failed: {primary}; {}",
            recovery_errors.join("; ")
        ))
    }
}

impl ChunkStorage {
    fn post_flush_replacement_data_path(&self) -> Result<PathBuf> {
        self.persisted
            .series_index_path
            .as_deref()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration(
                    "post-flush replacement recovery requires a persistent data path".to_string(),
                )
            })
    }

    fn post_flush_segment_path_resolver(&self) -> tiering::SegmentPathResolver<'_> {
        tiering::SegmentPathResolver::new(
            self.persisted.numeric_lane_path.as_deref(),
            self.persisted.blob_lane_path.as_deref(),
            self.persisted.tiered_storage.as_ref(),
        )
    }

    fn apply_committing_post_flush_replacement(
        &self,
        retention: RetentionMaintenanceContext<'_>,
        replacement: &PostFlushReplacement,
    ) -> Result<usize> {
        // Rebuild only the marker-owned delta from the currently visible catalog. A prior
        // publication attempt may have inserted some outputs or removed some sources before a
        // later catalog/registry persistence error. Skipping already-visible outputs prevents
        // duplicate chunk refs while this exact transition converges on retry.
        let output_entries = replacement.output_entries()?;
        let source_roots = replacement.source_roots();
        let (loaded_segments, removed_roots) = {
            let persisted = self.persisted.persisted_index.read();
            let removed = source_roots
                .iter()
                .filter(|root| persisted.segments_by_root.contains_key(*root))
                .cloned()
                .collect::<Vec<_>>();
            let missing_outputs = output_entries
                .iter()
                .filter(|entry| !persisted.segments_by_root.contains_key(&entry.root))
                .map(|entry| entry.root.clone())
                .collect::<Vec<_>>();
            drop(persisted);
            let loaded = missing_outputs
                .iter()
                .map(|root| ChunkStorage::load_segment_index_for_runtime_refresh(root.as_path()))
                .collect::<Result<Vec<_>>>()?;
            (loaded, removed)
        };
        let published_roots = output_entries
            .iter()
            .map(|entry| entry.root.clone())
            .collect::<Vec<_>>();
        let registry_catalog_delta = self
            .persisted_registry_catalog_delta_for_root_changes(&published_roots, &source_roots)?;
        let transition = PersistedCatalogTransition {
            visibility_fence: None,
            loaded_segments,
            removed_roots,
            publication: PersistedCatalogPublication::PersistedState {
                published_segment_roots: published_roots,
                refresh_tombstones: false,
            },
            registry_catalog_update: Some(registry_catalog::PersistedRegistryCatalogUpdate::Delta(
                registry_catalog_delta,
            )),
        };
        let publication = self.begin_persisted_catalog_publication();
        if let Err(err) = publication.publish_transition(transition) {
            self.persisted
                .persisted_index_dirty
                .store(true, Ordering::SeqCst);
            return Err(err);
        }
        drop(publication);

        let expired = replacement.finish_committing(self.persisted.local_disk_budget.as_ref())?;
        retention.record_expired_segments(expired);
        retention.record_tier_moves(replacement.tier_moves());
        Ok(expired)
    }

    fn recover_pending_post_flush_replacements(
        &self,
        retention: RetentionMaintenanceContext<'_>,
        data_path: &Path,
    ) -> Result<usize> {
        let mut expired = 0usize;
        loop {
            let Some(replacement) = next_runtime_replacement(
                data_path,
                self.post_flush_segment_path_resolver(),
                self.persisted.local_disk_budget.as_ref(),
            )?
            else {
                return Ok(expired);
            };
            expired = expired.saturating_add(
                self.apply_committing_post_flush_replacement(retention, &replacement)?,
            );
        }
    }

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

    #[cfg(test)]
    pub(in super::super) fn set_background_retention_inspect_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .background_retention_inspect_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in super::super) fn clear_background_retention_inspect_hook(&self) {
        *self
            .persist_test_hooks
            .background_retention_inspect_hook
            .write() = None;
    }

    pub(in super::super) fn run_post_flush_maintenance_if_pending(&self) -> Result<bool> {
        let workflow = self.post_flush_workflow_context();
        let work = workflow.claim_pending_work();
        if !work.any() {
            return Ok(false);
        }

        if work.run_post_flush {
            let sweep = self.sweep_background_persisted_segments_for_retention_page();
            let more_pages = match sweep {
                Ok((_expired, more_pages)) => more_pages,
                Err(err) => {
                    workflow.restore_post_flush_pending();
                    if work.run_metadata_reconcile {
                        workflow.restore_startup_metadata_reconcile_pending();
                    }
                    return Err(err);
                }
            };
            if more_pages {
                workflow.restore_post_flush_pending();
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

        while self.run_post_flush_maintenance_if_pending()?
            && self
                .coordination
                .post_flush_maintenance_pending
                .load(Ordering::Acquire)
        {
            self.sync_persisted_segments_from_disk_if_dirty()?;
        }
        Ok(())
    }

    pub(in super::super) fn schedule_startup_maintenance(&self) {
        self.post_flush_workflow_context()
            .schedule_startup_maintenance();
    }

    pub(in super::super) fn sweep_expired_persisted_segments(&self) -> Result<usize> {
        self.sweep_persisted_segments_for_retention(false)
    }

    pub(in super::super) fn sweep_fully_expired_persisted_segments(&self) -> Result<usize> {
        self.sweep_persisted_segments_for_retention(true)
    }

    fn publish_staged_post_flush_maintenance(
        &self,
        retention: RetentionMaintenanceContext<'_>,
        data_path: &Path,
        staged: StagedPostFlushMaintenance,
    ) -> Result<usize> {
        let StagedPostFlushMaintenance {
            publication,
            promotions,
            staging_cleanup_paths,
            loaded_segments,
            removed_roots,
            retired_roots,
            tier_moves,
        } = staged;
        let transition = match publication {
            StagedPostFlushPublication::CompleteInventory(final_inventory) => retention
                .plan_inventory_publication_transition(
                    final_inventory,
                    loaded_segments,
                    removed_roots,
                ),
            StagedPostFlushPublication::PersistedStateDelta { published_roots } => {
                let registry_catalog_delta = self
                    .persisted_registry_catalog_delta_for_root_changes(
                        &published_roots,
                        &removed_roots,
                    )?;
                PersistedCatalogTransition {
                    visibility_fence: None,
                    loaded_segments,
                    removed_roots,
                    publication: PersistedCatalogPublication::PersistedState {
                        published_segment_roots: published_roots,
                        refresh_tombstones: false,
                    },
                    registry_catalog_update: Some(
                        registry_catalog::PersistedRegistryCatalogUpdate::Delta(
                            registry_catalog_delta,
                        ),
                    ),
                }
            }
        };

        let mut replacement = match publish_prepared_replacement(
            data_path,
            self.post_flush_segment_path_resolver(),
            &retired_roots,
            &promotions,
            tier_moves,
            self.persisted.local_disk_budget.as_ref(),
        ) {
            Ok(replacement) => replacement,
            Err(err) => {
                let cleanup =
                    retention.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                return Err(post_flush_failure_with_recovery(err, Ok(()), cleanup));
            }
        };

        retention.with_post_flush_promotion_reservation(&promotions, || {
            for promotion in &promotions {
                if let Err(err) = retention.promote_staged_segment_for_publish_unbudgeted(promotion)
                {
                    let rollback = replacement.rollback_prepared(None);
                    let cleanup = retention.cleanup_staged_post_flush_paths_unbudgeted(
                        &promotions,
                        &staging_cleanup_paths,
                    );
                    return Err(post_flush_failure_with_recovery(err, rollback, cleanup));
                }
            }
            Ok(())
        })?;

        replacement.mark_committing(
            data_path,
            self.post_flush_segment_path_resolver(),
            self.persisted.local_disk_budget.as_ref(),
        )?;

        {
            let publication = self.begin_persisted_catalog_publication();
            if let Err(err) = publication.publish_transition(transition) {
                drop(publication);
                self.persisted
                    .persisted_index_dirty
                    .store(true, Ordering::SeqCst);
                let cleanup =
                    retention.cleanup_staged_post_flush_paths(&[], &staging_cleanup_paths);
                return Err(post_flush_failure_with_recovery(err, Ok(()), cleanup));
            }
        }

        let metadata_reconciliation = self.reconcile_live_metadata_indexes();
        let staging_cleanup =
            retention.cleanup_staged_post_flush_paths(&[], &staging_cleanup_paths);
        let retirement = replacement.finish_committing(self.persisted.local_disk_budget.as_ref());
        match retirement {
            Ok(removed) => {
                retention.record_expired_segments(removed);
                retention.record_tier_moves(replacement.tier_moves());
                if let Err(err) = metadata_reconciliation {
                    tracing::warn!(
                        error = %err,
                        "committed post-flush replacement retained its catalog result after metadata reconciliation failure"
                    );
                }
                if let Err(err) = staging_cleanup {
                    tracing::warn!(
                        error = %err,
                        "committed post-flush replacement retained its catalog result after staging cleanup failure"
                    );
                }
                Ok(removed)
            }
            Err(retirement) => {
                let mut errors = Vec::new();
                if let Err(err) = metadata_reconciliation {
                    errors.push(format!("metadata reconciliation failed: {err}"));
                }
                if let Err(err) = staging_cleanup {
                    errors.push(format!("staging cleanup failed: {err}"));
                }
                errors.push(format!("retired-root cleanup failed: {retirement}"));
                Err(TsinkError::Other(format!(
                    "post-flush finalization failed: {}",
                    errors.join("; ")
                )))
            }
        }
    }

    fn sweep_background_persisted_segments_for_retention_page(&self) -> Result<(usize, bool)> {
        let retention = self.retention_maintenance_context();
        if !retention.has_persisted_lane_paths() {
            retention.reset_background_maintenance_cursor();
            return Ok((0, false));
        }

        self.validate_shared_object_store_writer_lock()?;
        let recency_reference = self.retention_recency_reference_timestamp();
        let Some(cutoff) = retention.active_retention_cutoff(recency_reference) else {
            retention.reset_background_maintenance_cursor();
            return Ok((0, false));
        };
        let policy = retention.retention_tier_policy(cutoff, recency_reference);

        let _compaction_guard = self.compaction_gate();
        let data_path = self.post_flush_replacement_data_path()?;
        let recovered_expired =
            self.recover_pending_post_flush_replacements(retention, &data_path)?;
        if retention.catalog_requires_refresh() {
            self.apply_known_dirty_persisted_refresh_if_pending()?;
            if retention.catalog_requires_refresh() {
                // Unknown dirty state needs the catalog-refresh phase to establish a complete,
                // validated visible inventory. Keep the page pending without claiming that the
                // current cursor reached the end; the same worker runs refresh after this method.
                return Ok((recovered_expired, true));
            }
        }

        let page = retention.select_background_maintenance_page(policy)?;
        if page.plan.is_empty() {
            let more_pages = !page.cycle_complete;
            retention.commit_background_maintenance_page(&page);
            return Ok((recovered_expired, more_pages));
        }

        let staged = retention.stage_post_flush_maintenance_page(page.plan.clone(), policy)?;
        let removed = self.publish_staged_post_flush_maintenance(retention, &data_path, staged)?;
        let more_pages = !page.cycle_complete;
        retention.commit_background_maintenance_page(&page);
        Ok((recovered_expired.saturating_add(removed), more_pages))
    }

    fn sweep_persisted_segments_for_retention(&self, fully_expired_only: bool) -> Result<usize> {
        let retention = self.retention_maintenance_context();
        if !retention.has_persisted_lane_paths() {
            return Ok(0);
        }

        // Recovery, staging, promotion, retirement, and catalog publication below may all touch
        // the shared tier namespace. Fence the complete maintenance operation if the durable
        // lease pathname was replaced while this process retained only the old locked inode.
        self.validate_shared_object_store_writer_lock()?;

        let recency_reference = self.retention_recency_reference_timestamp();
        let Some(cutoff) = retention.active_retention_cutoff(recency_reference) else {
            return Ok(0);
        };
        let policy = retention.retention_tier_policy(cutoff, recency_reference);

        let _compaction_guard = self.compaction_gate();
        let data_path = self.post_flush_replacement_data_path()?;
        let recovered_expired =
            self.recover_pending_post_flush_replacements(retention, &data_path)?;
        if retention.catalog_requires_refresh() {
            // Apply any known root diffs before maintenance decides whether it can reuse the
            // published catalog or must fall back to a full inventory scan.
            self.apply_known_dirty_persisted_refresh_if_pending()?;
        }
        let (inventory, inventory_source) = retention.post_flush_maintenance_inventory()?;
        let mut plan = policy.post_flush_maintenance_plan(&inventory);
        if fully_expired_only {
            plan.rewrite_actions.clear();
            plan.move_actions.clear();
        }
        if plan.is_empty() {
            if fully_expired_only {
                return Ok(recovered_expired);
            }
            if inventory_source == MaintenanceInventorySource::PersistedState {
                // Flush/catalog publication already installed this exact persisted view. With no
                // dirty diff, recovery transition, retention expiry, rewrite, or tier move,
                // republishing would only rescan the complete in-memory index and rewrite
                // monolithic catalogs on every idle wake.
                return Ok(recovered_expired);
            }
            let publication = self.begin_persisted_catalog_publication();
            match publication.publish_transition(
                retention.plan_noop_inventory_transition(&inventory, inventory_source),
            )? {
                PersistedCatalogRefreshApply::Applied => return Ok(recovered_expired),
                PersistedCatalogRefreshApply::SkippedStaleVisibleState => unreachable!(
                    "post-flush maintenance no-op publication should not use a visibility fence"
                ),
            }
        }

        let inventory = if inventory_source == MaintenanceInventorySource::PersistedState {
            let loaded = self.load_scanned_catalog_refresh()?;
            let scanned_inventory = loaded.inventory;
            plan = policy.post_flush_maintenance_plan(&scanned_inventory);
            if fully_expired_only {
                plan.rewrite_actions.clear();
                plan.move_actions.clear();
            }
            if plan.is_empty() {
                if fully_expired_only {
                    return Ok(recovered_expired);
                }
                let publication = self.begin_persisted_catalog_publication();
                match publication.publish_transition(retention.plan_noop_inventory_transition(
                    &scanned_inventory,
                    MaintenanceInventorySource::Scanned,
                ))? {
                    PersistedCatalogRefreshApply::Applied => return Ok(recovered_expired),
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
        let removed = self.publish_staged_post_flush_maintenance(retention, &data_path, staged)?;
        Ok(recovered_expired.saturating_add(removed))
    }
}
