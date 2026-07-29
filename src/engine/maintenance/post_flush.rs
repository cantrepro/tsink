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

fn after_invalidating_background_post_flush_clean_fence<T>(
    cursor: &parking_lot::Mutex<BackgroundPostFlushCleanFenceCursor>,
    marker_generation: &AtomicU64,
    publish_marker: impl FnOnce() -> Result<T>,
) -> Result<T> {
    // Keep marker creation inside this callback boundary: a retained ReadDir must be dropped and
    // its generation advanced before the marker can enter the namespace behind that cursor.
    invalidate_background_post_flush_clean_fence(cursor, marker_generation)?;
    publish_marker()
}

enum PostFlushCatalogPublication {
    Published(usize),
    Deferred,
}

enum BackgroundRetentionSweepOutcome {
    EnvelopeConsumed { more_pages: bool },
    CatalogRefreshRequired,
}

enum PostFlushMaintenanceRunOutcome {
    NoWork,
    EnvelopeConsumed,
    CatalogRefreshRequired,
}

impl ChunkStorage {
    const CACHED_METADATA_RECONCILIATION_ITEM_BYTES: usize = 512;

    #[cfg(test)]
    pub(in super::super) fn modeled_metadata_reconciliation_item_bytes(
        &self,
        series_id: SeriesId,
    ) -> u64 {
        let summary_cached =
            self.with_series_visibility_summaries(|summaries| summaries.contains_key(&series_id));
        self.modeled_metadata_reconciliation_item_bytes_with_cache_state(series_id, summary_cached)
    }

    fn modeled_metadata_reconciliation_item_bytes_with_cache_state(
        &self,
        series_id: SeriesId,
        summary_cached: bool,
    ) -> u64 {
        let modeled = if summary_cached {
            // Cached reconciliation reads the summary in place. This allowance covers the
            // one-ID materialized/runtime-delta/shard removal handoffs and allocator slack.
            Self::CACHED_METADATA_RECONCILIATION_ITEM_BYTES
        } else {
            // A missing summary is the indivisible dependency window: rebuilding it may need to
            // normalize every active, sealed, and persisted range before publishing the cap.
            self.series_visibility_refresh_staging_upper_bound(std::iter::once(&series_id))
        };
        u64::try_from(
            modeled
                .saturating_add(std::mem::size_of::<SeriesId>())
                .saturating_add(64),
        )
        .unwrap_or(u64::MAX)
    }

    fn reconcile_live_metadata_series_locked(
        &self,
        series_id: SeriesId,
        summary_cached: bool,
    ) -> Result<()> {
        if !summary_cached {
            self.refresh_series_visible_timestamp_cache_locked(std::iter::once(series_id))?;
        }

        let generation_before = self.live_series_pruning_generation();
        let retention_cutoff = self.active_retention_cutoff().unwrap_or(i64::MIN);
        let is_live = self.with_series_visibility_summaries(|summaries| {
            summaries
                .get(&series_id)
                .and_then(|summary| summary.latest_visible_timestamp)
                .is_some_and(|latest| latest >= retention_cutoff)
        });
        if !is_live {
            self.prune_dead_materialized_series_ids_if_stable(
                vec![series_id],
                Some(generation_before),
            );
        }
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn reset_background_metadata_reconciliation_cursor(
        &self,
    ) {
        *self
            .coordination
            .background_metadata_reconciliation_cursor
            .lock() = BackgroundMetadataReconciliationCursor::default();
    }

    pub(in crate::engine::storage_engine) fn reset_background_post_flush_recovery_cursor(&self) {
        self.coordination
            .background_post_flush_recovery_cursor
            .lock()
            .reset();
    }

    pub(in crate::engine::storage_engine) fn reset_background_post_flush_clean_fence_cursor(&self) {
        self.coordination
            .background_post_flush_clean_fence_cursor
            .lock()
            .reset();
    }

    pub(in super::super) fn run_live_metadata_reconciliation_page(&self) -> Result<bool> {
        let max_items = self.runtime.maintenance_max_items_per_pass;
        let max_bytes = self.runtime.maintenance_max_bytes_per_pass;
        let mut selected_items = 0usize;
        let mut selected_bytes = 0u64;
        // Retain only this scalar state across wakes and hold its lock for one page so an explicit
        // foreground reconciliation cannot race a background wake over the same cursor.
        let mut cursor = self
            .coordination
            .background_metadata_reconciliation_cursor
            .lock();

        let current_generation = self.live_series_pruning_generation();
        if !cursor.cycle_started {
            cursor.cycle_started = true;
            cursor.observed_generation = current_generation;
        } else if cursor.observed_generation != current_generation {
            cursor.cycle_generation_changed = true;
            cursor.observed_generation = current_generation;
        }

        loop {
            let after_series_id = cursor.after_series_id;
            let Some(series_id) = self
                .materialized_series_page_after(after_series_id, 1)
                .into_iter()
                .next()
            else {
                let current_generation = self.live_series_pruning_generation();
                if cursor.observed_generation != current_generation {
                    cursor.cycle_generation_changed = true;
                }
                if cursor.cycle_generation_changed {
                    let phase = match cursor.phase {
                        BackgroundMetadataReconciliationPhase::Sweep
                        | BackgroundMetadataReconciliationPhase::Verify => {
                            BackgroundMetadataReconciliationPhase::Verify
                        }
                    };
                    *cursor = BackgroundMetadataReconciliationCursor {
                        phase,
                        observed_generation: current_generation,
                        ..BackgroundMetadataReconciliationCursor::default()
                    };
                    return Ok(true);
                }
                *cursor = BackgroundMetadataReconciliationCursor::default();
                return Ok(false);
            };

            if selected_items >= max_items {
                if selected_items == 0 {
                    return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                        operation: "live metadata reconciliation series",
                        item_limit: max_items,
                        byte_limit: max_bytes,
                        selected_items,
                        selected_bytes,
                    });
                }
                return Ok(true);
            }
            if max_bytes == 0 {
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation: "live metadata reconciliation series",
                    item_limit: max_items,
                    byte_limit: max_bytes,
                    selected_items,
                    selected_bytes,
                });
            }
            if selected_bytes >= max_bytes {
                return Ok(true);
            }

            // Fence cache invalidation between the cheap cached-item model and its scalar
            // retention decision. Missing-summary rebuilds use the existing locked refresh path.
            let _visibility_guard = self.visibility_read_fence();
            let summary_cached = self
                .with_series_visibility_summaries(|summaries| summaries.contains_key(&series_id));
            let candidate_bytes = self.modeled_metadata_reconciliation_item_bytes_with_cache_state(
                series_id,
                summary_cached,
            );
            if candidate_bytes > max_bytes {
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation: "live metadata reconciliation series",
                    item_limit: max_items,
                    byte_limit: max_bytes,
                    selected_items,
                    selected_bytes,
                });
            }
            if candidate_bytes > max_bytes.saturating_sub(selected_bytes) {
                return Ok(true);
            }

            // Commit the scalar cursor only after the complete per-series refresh and stable
            // pruning operation succeeds. An error therefore retries this exact item without
            // retaining a page-sized snapshot or partially published staging collection.
            let generation_before = self.live_series_pruning_generation();
            self.reconcile_live_metadata_series_locked(series_id, summary_cached)?;
            let generation_after = self.live_series_pruning_generation();

            selected_items = selected_items.saturating_add(1);
            selected_bytes = selected_bytes.saturating_add(candidate_bytes);
            debug_assert_eq!(cursor.after_series_id, after_series_id);
            cursor.after_series_id = Some(series_id);
            if cursor.observed_generation != generation_before
                || generation_before != generation_after
            {
                cursor.cycle_generation_changed = true;
            }
            cursor.observed_generation = generation_after;
        }
    }

    pub(in crate::engine::storage_engine) fn drain_live_metadata_reconciliation_pages(
        &self,
    ) -> Result<()> {
        while self.run_live_metadata_reconciliation_page()? {}
        // Do not clear the at-least-once pending bit here. A catalog publication can schedule a
        // newer reconciliation concurrently after this terminal generation check; its later
        // claimed no-op page is cheaper and safer than erasing that wake.
        Ok(())
    }

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
        drain_catalog: bool,
        bounded_recovery: Option<&BoundedRuntimeReplacement>,
    ) -> Result<PostFlushCatalogPublication> {
        debug_assert_eq!(bounded_recovery.is_none(), drain_catalog);
        let publication = self.begin_persisted_catalog_publication();
        let resumed_catalog_completed = if self.finite_tiered_catalog_publication_enabled()
            && self.bounded_tiered_catalog_publication_is_pending()
        {
            let completed = match self.advance_bounded_tiered_catalog_publication(false) {
                Ok(completed) => completed,
                Err(err) => {
                    self.persisted
                        .persisted_index_dirty
                        .store(true, Ordering::SeqCst);
                    return Err(err);
                }
            };
            if !completed {
                self.persisted
                    .persisted_index_dirty
                    .store(true, Ordering::SeqCst);
                return Ok(PostFlushCatalogPublication::Deferred);
            }
            if !drain_catalog {
                // This finite writer page consumed the background wake's complete maintenance
                // envelope. Resume the marker transition on a later wake even when this page
                // happened to finish the writer cursor.
                self.persisted
                    .persisted_index_dirty
                    .store(true, Ordering::SeqCst);
                return Ok(PostFlushCatalogPublication::Deferred);
            }

            // The finite writer cursor now owns both the segment catalog and the paged complete
            // registry sidecar. Its terminal result is exact for this visibility generation, so
            // no unbounded inventory materialization is needed here.
            self.synchronize_persisted_index_dirty_with_pending();
            true
        } else {
            false
        };
        let catalog_snapshot_is_current =
            self.bounded_tiered_catalog_publication_matches_current_visibility();
        if catalog_snapshot_is_current && !resumed_catalog_completed {
            // The generic dirty-refresh caller may have completed this marker's segment catalog
            // and registry sidecar between marker wakes.
            self.synchronize_persisted_index_dirty_with_pending();
        }

        // Rebuild only the marker-owned delta from the currently visible catalog. A prior
        // publication attempt may have inserted some outputs or removed some sources before a
        // later catalog/registry persistence error. Skipping already-visible outputs prevents
        // duplicate chunk refs while this exact transition converges on retry.
        let output_entries = match bounded_recovery {
            Some(bounded) => bounded.output_entries().to_vec(),
            None => replacement.output_entries()?,
        };
        let source_roots = match bounded_recovery {
            Some(bounded) => bounded.source_roots().to_vec(),
            None => replacement.source_roots(),
        };
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
        // When the cursor that just completed already contains this marker's installed outputs
        // and removals, applying an empty transition would create a brand-new publication cycle.
        // Otherwise apply the missing marker-owned mutation now; its visibility bump correctly
        // starts a fresh latest-snapshot cursor.
        if !catalog_snapshot_is_current
            || !transition.loaded_segments.is_empty()
            || !transition.removed_roots.is_empty()
        {
            let mut transition_reservation = None;
            let result = if drain_catalog {
                publication.publish_transition(transition)
            } else {
                let bounded = bounded_recovery.expect(
                    "finite post-flush recovery must carry its admitted marker work envelope",
                );
                let item_limit = self.runtime.maintenance_max_items_per_pass;
                let byte_limit = self.runtime.maintenance_max_bytes_per_pass;
                let staging_bytes = self.modeled_finite_transition_staging_bytes(&transition);
                let staging_work = u64::try_from(staging_bytes).unwrap_or(u64::MAX);
                let transition_peak = bounded
                    .retained_memory_bytes()
                    .saturating_add(staging_bytes);
                let selected_work = bounded
                    .selected_bytes()
                    .max(u64::try_from(transition_peak).unwrap_or(u64::MAX))
                    .max(staging_work);
                if bounded.selected_items() > item_limit {
                    return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                        operation: "bounded post-flush replacement catalog publication",
                        item_limit,
                        byte_limit,
                        selected_items: bounded.selected_items(),
                        selected_bytes: selected_work,
                    });
                }
                if selected_work > byte_limit {
                    return Err(TsinkError::MaintenanceWorkItemTooLarge {
                        operation: "bounded post-flush replacement catalog publication",
                        limit: byte_limit,
                        required: selected_work,
                    });
                }
                transition_reservation =
                    Some(self.remote_catalog_memory_reservation(staging_bytes)?);
                publication.publish_transition_with_finite_recovery_budget(
                    transition,
                    item_limit.saturating_sub(bounded.selected_items()),
                    byte_limit.saturating_sub(selected_work),
                    true,
                )
            };
            drop(transition_reservation);
            match result {
                Ok(PersistedCatalogRefreshApply::Applied) => {}
                Ok(PersistedCatalogRefreshApply::Deferred) => {
                    self.persisted
                        .persisted_index_dirty
                        .store(true, Ordering::SeqCst);
                    return Ok(PostFlushCatalogPublication::Deferred);
                }
                Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState) => {
                    unreachable!("post-flush replacement recovery does not use a visibility fence")
                }
                Err(err) => {
                    self.persisted
                        .persisted_index_dirty
                        .store(true, Ordering::SeqCst);
                    return Err(err);
                }
            }
        }
        drop(publication);

        // Recovery publication changes the same persisted-series visibility metadata as the
        // direct post-flush path. Preserve the at-least-once reconciliation wake before source
        // retirement so a later cleanup error cannot erase that ownership.
        self.post_flush_workflow_context()
            .restore_startup_metadata_reconcile_pending();
        let expired = match bounded_recovery {
            Some(bounded) => {
                bounded.finish_committing(self.persisted.local_disk_budget.as_ref())?
            }
            None => replacement.finish_committing(self.persisted.local_disk_budget.as_ref())?,
        };
        retention.record_expired_segments(expired);
        retention.record_tier_moves(replacement.tier_moves());
        Ok(PostFlushCatalogPublication::Published(expired))
    }

    fn recover_pending_post_flush_replacements(
        &self,
        retention: RetentionMaintenanceContext<'_>,
        data_path: &Path,
        drain_catalog: bool,
    ) -> Result<(usize, bool, bool)> {
        if !drain_catalog {
            // A prior marker transition may already own the finite tiered writer cursor. Advancing
            // that cursor consumes this wake's complete maintenance envelope; do not also scan or
            // parse another marker even when the page happens to complete the writer.
            if self.finite_tiered_catalog_publication_enabled()
                && self.bounded_tiered_catalog_publication_is_pending()
            {
                let completed = self
                    .advance_bounded_tiered_catalog_publication(false)
                    .inspect_err(|_| {
                        self.persisted
                            .persisted_index_dirty
                            .store(true, Ordering::SeqCst);
                    })?;
                if completed {
                    self.synchronize_persisted_index_dirty_with_pending();
                } else {
                    self.persisted
                        .persisted_index_dirty
                        .store(true, Ordering::SeqCst);
                }
                return Ok((0, true, true));
            }

            let step = {
                let mut cursor = self
                    .coordination
                    .background_post_flush_recovery_cursor
                    .lock();
                next_runtime_replacement_bounded(
                    &mut cursor,
                    data_path,
                    self.post_flush_segment_path_resolver(),
                    self.persisted.local_disk_budget.as_ref(),
                    self.runtime.maintenance_max_items_per_pass,
                    self.runtime.maintenance_max_bytes_per_pass,
                    |bytes| self.remote_catalog_memory_reservation(bytes),
                )
            }?;
            return match step {
                BoundedRuntimeReplacementStep::NoPending => Ok((0, false, false)),
                BoundedRuntimeReplacementStep::NamespaceEntryConsumed
                | BoundedRuntimeReplacementStep::PreparedRolledBack => Ok((0, false, true)),
                BoundedRuntimeReplacementStep::Committing(bounded) => {
                    let replacement = bounded.replacement();
                    match self.apply_committing_post_flush_replacement(
                        retention,
                        replacement,
                        false,
                        Some(&bounded),
                    ) {
                        Ok(PostFlushCatalogPublication::Published(removed)) => {
                            Ok((removed, false, true))
                        }
                        Ok(PostFlushCatalogPublication::Deferred) => {
                            self.reset_background_post_flush_recovery_cursor();
                            Ok((0, true, true))
                        }
                        Err(err) => {
                            self.reset_background_post_flush_recovery_cursor();
                            Err(err)
                        }
                    }
                }
            };
        }

        // Foreground/lifecycle drains intentionally retain the strict complete namespace scan and
        // may finish every Prepared and Committing marker in one call.
        self.reset_background_post_flush_recovery_cursor();
        let mut expired = 0usize;
        let mut recovered_any = false;
        loop {
            let Some(replacement) = next_runtime_replacement(
                data_path,
                self.post_flush_segment_path_resolver(),
                self.persisted.local_disk_budget.as_ref(),
            )?
            else {
                return Ok((expired, false, recovered_any));
            };
            recovered_any = true;
            match self.apply_committing_post_flush_replacement(
                retention,
                &replacement,
                drain_catalog,
                None,
            )? {
                PostFlushCatalogPublication::Published(removed) => {
                    expired = expired.saturating_add(removed);
                }
                PostFlushCatalogPublication::Deferred => {
                    continue;
                }
            }
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

    fn run_post_flush_maintenance_if_pending_outcome(
        &self,
    ) -> Result<PostFlushMaintenanceRunOutcome> {
        let workflow = self.post_flush_workflow_context();
        let work = workflow.claim_pending_work();
        if !work.any() {
            return Ok(PostFlushMaintenanceRunOutcome::NoWork);
        }

        if work.run_post_flush {
            let sweep = self.sweep_background_persisted_segments_for_retention_page();
            let envelope_consumed = match sweep {
                Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages }) => {
                    if more_pages {
                        workflow.restore_post_flush_pending();
                    }
                    true
                }
                Ok(BackgroundRetentionSweepOutcome::CatalogRefreshRequired) => {
                    workflow.restore_post_flush_pending();
                    false
                }
                Err(err) => {
                    workflow.restore_post_flush_pending();
                    if work.run_metadata_reconcile {
                        workflow.restore_startup_metadata_reconcile_pending();
                    }
                    return Err(err);
                }
            };
            if !envelope_consumed {
                return Ok(PostFlushMaintenanceRunOutcome::CatalogRefreshRequired);
            }
        }

        if work.run_metadata_reconcile {
            match self.run_live_metadata_reconciliation_page() {
                Ok(more_pages) => {
                    if more_pages {
                        workflow.restore_startup_metadata_reconcile_pending();
                    }
                }
                Err(err) => {
                    workflow.restore_startup_metadata_reconcile_pending();
                    return Err(err);
                }
            }
        }

        Ok(PostFlushMaintenanceRunOutcome::EnvelopeConsumed)
    }

    #[cfg(test)]
    pub(in super::super) fn run_post_flush_maintenance_if_pending(&self) -> Result<bool> {
        Ok(!matches!(
            self.run_post_flush_maintenance_if_pending_outcome()?,
            PostFlushMaintenanceRunOutcome::NoWork
        ))
    }

    pub(in super::super) fn run_post_flush_maintenance_envelope_if_pending(&self) -> Result<bool> {
        Ok(matches!(
            self.run_post_flush_maintenance_if_pending_outcome()?,
            PostFlushMaintenanceRunOutcome::EnvelopeConsumed
        ))
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

        loop {
            let envelope_consumed = self.run_post_flush_maintenance_envelope_if_pending()?;
            if !envelope_consumed && self.persisted.persisted_index_dirty.load(Ordering::Acquire) {
                self.sync_persisted_segments_from_disk_if_dirty()?;
            }
            if !(self
                .coordination
                .post_flush_maintenance_pending
                .load(Ordering::Acquire)
                || self
                    .coordination
                    .startup_metadata_reconcile_pending
                    .load(Ordering::Acquire))
            {
                break;
            }
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
        drain_catalog: bool,
        finite_page_work: Option<(usize, u64)>,
    ) -> Result<PostFlushCatalogPublication> {
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
        let mut transition_reservation = None;
        let finite_recovery_budget =
            if let Some((selected_items, selected_bytes)) = finite_page_work {
                let item_limit = self.runtime.maintenance_max_items_per_pass;
                let byte_limit = self.runtime.maintenance_max_bytes_per_pass;
                if selected_items > item_limit {
                    let primary = TsinkError::MaintenanceDependencyWindowExceeded {
                        operation: "bounded retention catalog publication",
                        item_limit,
                        byte_limit,
                        selected_items,
                        selected_bytes,
                    };
                    let cleanup = retention
                        .cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                    return Err(post_flush_failure_with_recovery(primary, Ok(()), cleanup));
                }
                let staging_bytes = self.modeled_finite_transition_staging_bytes(&transition);
                let staging_work = u64::try_from(staging_bytes).unwrap_or(u64::MAX);
                let selected_work = selected_bytes.max(staging_work);
                if selected_work > byte_limit {
                    let primary = TsinkError::MaintenanceWorkItemTooLarge {
                        operation: "bounded retention catalog publication",
                        limit: byte_limit,
                        required: selected_work,
                    };
                    let cleanup = retention
                        .cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                    return Err(post_flush_failure_with_recovery(primary, Ok(()), cleanup));
                }
                match self.remote_catalog_memory_reservation(staging_bytes) {
                    Ok(reservation) => transition_reservation = Some(reservation),
                    Err(primary) => {
                        let cleanup = retention
                            .cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                        return Err(post_flush_failure_with_recovery(primary, Ok(()), cleanup));
                    }
                }
                Some((
                    item_limit.saturating_sub(selected_items),
                    byte_limit.saturating_sub(selected_work),
                ))
            } else {
                None
            };

        // The caller holds the shared compaction gate, so clean-fence reset, generation advance,
        // marker publication, and any immediate recovery are serialized with the compactor.
        let mut replacement = match after_invalidating_background_post_flush_clean_fence(
            self.coordination
                .background_post_flush_clean_fence_cursor
                .as_ref(),
            self.coordination.post_flush_marker_generation.as_ref(),
            || {
                publish_prepared_replacement(
                    data_path,
                    self.post_flush_segment_path_resolver(),
                    &retired_roots,
                    &promotions,
                    tier_moves,
                    self.persisted.local_disk_budget.as_ref(),
                )
            },
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

        let catalog_deferred = {
            let publication = self.begin_persisted_catalog_publication();
            let result = match finite_recovery_budget {
                Some((remaining_items, remaining_bytes)) => publication
                    .publish_transition_with_finite_recovery_budget(
                        transition,
                        remaining_items,
                        remaining_bytes,
                        true,
                    ),
                None => publication.publish_transition(transition),
            };
            match result {
                Ok(PersistedCatalogRefreshApply::Applied) => false,
                Ok(PersistedCatalogRefreshApply::Deferred) => true,
                Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState) => unreachable!(
                    "post-flush maintenance publication does not use a visibility fence"
                ),
                Err(err) => {
                    drop(publication);
                    drop(transition_reservation);
                    self.persisted
                        .persisted_index_dirty
                        .store(true, Ordering::SeqCst);
                    let cleanup =
                        retention.cleanup_staged_post_flush_paths(&[], &staging_cleanup_paths);
                    return Err(post_flush_failure_with_recovery(err, Ok(()), cleanup));
                }
            }
        };
        drop(transition_reservation);
        if catalog_deferred {
            self.persisted
                .persisted_index_dirty
                .store(true, Ordering::SeqCst);
            retention.cleanup_staged_post_flush_paths(&[], &staging_cleanup_paths)?;
            if drain_catalog {
                loop {
                    match self.apply_committing_post_flush_replacement(
                        retention,
                        &replacement,
                        true,
                        None,
                    )? {
                        PostFlushCatalogPublication::Published(removed) => {
                            return Ok(PostFlushCatalogPublication::Published(removed));
                        }
                        PostFlushCatalogPublication::Deferred => {}
                    }
                }
            }
            return Ok(PostFlushCatalogPublication::Deferred);
        }

        // Catalog publication already refreshed the directly affected visibility summaries.
        // Global dead-series cleanup is resumable and bounded separately; never rebuild a
        // root-sized materialized-series snapshot in this publication/finalization wake.
        self.post_flush_workflow_context()
            .restore_startup_metadata_reconcile_pending();
        let staging_cleanup =
            retention.cleanup_staged_post_flush_paths(&[], &staging_cleanup_paths);
        let retirement = replacement.finish_committing(self.persisted.local_disk_budget.as_ref());
        match retirement {
            Ok(removed) => {
                retention.record_expired_segments(removed);
                retention.record_tier_moves(replacement.tier_moves());
                if let Err(err) = staging_cleanup {
                    tracing::warn!(
                        error = %err,
                        "committed post-flush replacement retained its catalog result after staging cleanup failure"
                    );
                }
                Ok(PostFlushCatalogPublication::Published(removed))
            }
            Err(retirement) => {
                let mut errors = Vec::new();
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

    fn sweep_background_persisted_segments_for_retention_page(
        &self,
    ) -> Result<BackgroundRetentionSweepOutcome> {
        let retention = self.retention_maintenance_context();
        if !retention.has_persisted_lane_paths() {
            retention.reset_background_maintenance_cursor();
            return Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages: false });
        }

        self.validate_shared_object_store_writer_lock()?;
        let recency_reference = self.retention_recency_reference_timestamp();
        let Some(cutoff) = retention.active_retention_cutoff(recency_reference) else {
            retention.reset_background_maintenance_cursor();
            return Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages: false });
        };
        let policy = retention.retention_tier_policy(cutoff, recency_reference);

        let _compaction_guard = self.compaction_gate();
        let data_path = self.post_flush_replacement_data_path()?;
        let (_recovered_expired, catalog_publication_pending, recovered_replacement) =
            self.recover_pending_post_flush_replacements(retention, &data_path, false)?;
        if catalog_publication_pending || recovered_replacement {
            return Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages: true });
        }
        if retention.catalog_requires_refresh() {
            // Any catalog mutation can insert a lexically earlier root behind the retained
            // inventory cursor. The compaction gate fences producers here, so restart before
            // consuming a known-dirty page and never reuse the prior after-root boundary.
            retention.reset_background_maintenance_cursor();
            if self.apply_known_dirty_persisted_refresh_if_pending()? {
                return Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages: true });
            }
            // Unknown dirty state needs the catalog-refresh phase to establish a complete,
            // validated visible inventory. No retention envelope was spent, so let this worker
            // wake dispatch exactly one catalog-refresh page before retrying retention later.
            return Ok(BackgroundRetentionSweepOutcome::CatalogRefreshRequired);
        }

        let page = retention.select_background_maintenance_page(policy)?;
        if page.plan.is_empty() {
            let more_pages = !page.cycle_complete;
            retention.commit_background_maintenance_page(&page);
            return Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages });
        }

        let staged = retention.stage_post_flush_maintenance_page(page.plan.clone(), policy)?;
        match self.publish_staged_post_flush_maintenance(
            retention,
            &data_path,
            staged,
            false,
            Some((page.selected_items, page.selected_bytes)),
        )? {
            PostFlushCatalogPublication::Published(_) => {}
            PostFlushCatalogPublication::Deferred => {
                return Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages: true });
            }
        }
        let more_pages = !page.cycle_complete;
        retention.commit_background_maintenance_page(&page);
        Ok(BackgroundRetentionSweepOutcome::EnvelopeConsumed { more_pages })
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
        let (recovered_expired, catalog_publication_pending, _recovered_replacement) =
            self.recover_pending_post_flush_replacements(retention, &data_path, true)?;
        debug_assert!(
            !catalog_publication_pending,
            "draining post-flush recovery must finish catalog publication"
        );
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
                PersistedCatalogRefreshApply::Deferred => {
                    self.advance_bounded_tiered_catalog_publication(true)?;
                    self.synchronize_persisted_index_dirty_with_pending();
                    return Ok(recovered_expired);
                }
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
                    PersistedCatalogRefreshApply::Deferred => {
                        self.advance_bounded_tiered_catalog_publication(true)?;
                        self.synchronize_persisted_index_dirty_with_pending();
                        return Ok(recovered_expired);
                    }
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
        let removed = match self
            .publish_staged_post_flush_maintenance(retention, &data_path, staged, true, None)?
        {
            PostFlushCatalogPublication::Published(removed) => removed,
            PostFlushCatalogPublication::Deferred => {
                unreachable!("draining post-flush publication must complete")
            }
        };
        self.drain_live_metadata_reconciliation_pages()?;
        Ok(recovered_expired.saturating_add(removed))
    }
}

#[cfg(test)]
mod publication_order_tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn marker_publication_callback_runs_only_after_clean_fence_invalidation() {
        let cursor = parking_lot::Mutex::new(BackgroundPostFlushCleanFenceCursor::default());
        let marker_generation = AtomicU64::new(41);
        after_invalidating_background_post_flush_clean_fence(&cursor, &marker_generation, || {
            assert_eq!(marker_generation.load(Ordering::Acquire), 42);
            Ok(())
        })
        .unwrap();

        let overflow_generation = AtomicU64::new(u64::MAX);
        let publication_ran = AtomicBool::new(false);
        after_invalidating_background_post_flush_clean_fence(&cursor, &overflow_generation, || {
            publication_ran.store(true, Ordering::Release);
            Ok(())
        })
        .expect_err("generation overflow must stop before marker publication");
        assert!(!publication_ran.load(Ordering::Acquire));
    }
}
