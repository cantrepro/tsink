use super::super::tiering::{self, PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry};
use super::super::{ChunkStorage, HashSet, PathBuf, Result, StorageRuntimeMode};
use super::*;

mod bounded_remote;
mod bounded_scan;
mod bounded_tombstones;
mod context;
mod pipeline;
mod publication;

pub(in crate::engine::storage_engine) use self::bounded_scan::BackgroundCatalogRefreshCursor;
use self::context::CatalogRefreshContext;

impl ChunkStorage {
    pub(in super::super) fn reset_bounded_catalog_refresh_continuations(&self) {
        self.reset_bounded_unknown_dirty_catalog_refresh();
        self.reset_bounded_remote_catalog_refresh();
        self.reset_bounded_tiered_catalog_publication();
    }

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
                    lane: entry.lane,
                    tier: entry.tier,
                    manifest: entry.manifest.clone(),
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
                refresh_tombstones: false,
            },
            registry_catalog_update: Some(
                registry_catalog::PersistedRegistryCatalogUpdate::Complete(
                    registry_catalog::inventory_sources(&inventory),
                ),
            ),
        };
        let publication = self.begin_persisted_catalog_publication();
        match publication.publish_transition(transition)? {
            PersistedCatalogRefreshApply::Applied => Ok(()),
            PersistedCatalogRefreshApply::Deferred => {
                self.catalog_refresh_context()
                    .set_persisted_index_dirty(true);
                Ok(())
            }
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
        let finite_maintenance = self.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.runtime.maintenance_max_bytes_per_pass != u64::MAX;
        if finite_maintenance && !ctx.has_known_persisted_segment_changes() {
            return Ok(None);
        }
        // `take_one` moves the root PathBuf payload but allocates one destination B-tree node.
        // Install its fixed selection lease before taking the pending-diff mutex.
        let mut finite_selection_reservation = if finite_maintenance {
            Some(self.remote_catalog_memory_reservation(4096)?)
        } else {
            None
        };
        let diff = if finite_maintenance {
            ctx.take_one_known_persisted_segment_change()
        } else {
            ctx.take_known_persisted_segment_changes()
        };
        if diff.is_empty() {
            return Ok(None);
        }

        if finite_maintenance {
            let selected = diff;

            let item_limit = self.runtime.maintenance_max_items_per_pass;
            let byte_limit = self.runtime.maintenance_max_bytes_per_pass;
            if item_limit == 0 {
                ctx.restore_known_persisted_segment_change_if_unmodified(selected);
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation: "finite known-dirty catalog transition",
                    item_limit,
                    byte_limit,
                    selected_items: 1,
                    selected_bytes: 0,
                });
            }

            let selected_root = selected
                .added_roots
                .first()
                .or_else(|| selected.removed_roots.first())
                .expect("finite known-dirty selection contains one root");
            let root_path_bytes = selected_root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .saturating_mul(12)
                .saturating_add(64 * 1024);
            let initial_staging_bytes = root_path_bytes.saturating_add(
                bounded_scan::CATALOG_SCAN_MANIFEST_INSPECTION_BYTES.min(usize::MAX as u64)
                    as usize,
            );
            let mut staging_reservation = finite_selection_reservation
                .take()
                .expect("finite selection reservation created above");
            if let Err(err) = self.resize_remote_catalog_memory_reservation(
                &mut staging_reservation,
                initial_staging_bytes.max(4096),
            ) {
                ctx.restore_known_persisted_segment_change_if_unmodified(selected);
                return Err(err);
            }

            let prepared = (|| -> Result<(Vec<IndexedSegment>, u64)> {
                if selected.added_roots.contains(selected_root) {
                    let runtime = tiering::preflight_segment_runtime_refresh_memory(selected_root)?;
                    let descriptor_bytes = u64::try_from(
                        selected_root
                            .as_os_str()
                            .as_encoded_bytes()
                            .len()
                            .saturating_mul(2)
                            .saturating_add(
                                bounded_scan::CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD as usize,
                            ),
                    )
                    .unwrap_or(u64::MAX);
                    let predecode_staging_bytes =
                        bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(
                            selected_root,
                        )
                        .saturating_add(
                            bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_transition_descriptor_bytes(
                                selected_root,
                            ),
                        )
                        .saturating_add(descriptor_bytes.min(usize::MAX as u64) as usize)
                        .saturating_add(runtime.reservation_bytes);
                    self.resize_remote_catalog_memory_reservation(
                        &mut staging_reservation,
                        predecode_staging_bytes,
                    )?;
                    let manifest = crate::engine::segment::read_segment_manifest(selected_root)?;
                    let mutation_bytes =
                        bounded_scan::modeled_removal_bytes(selected_root, &manifest)
                            .min(usize::MAX as u64) as usize;
                    let staging_bytes =
                        bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(
                            selected_root,
                        )
                        .saturating_add(
                            bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_transition_descriptor_bytes(
                                selected_root,
                            ),
                        )
                        .saturating_add(descriptor_bytes.min(usize::MAX as u64) as usize)
                        .saturating_add(runtime.reservation_bytes)
                        .saturating_add(mutation_bytes);
                    let selected_work_bytes = 4096u64
                        .saturating_add(descriptor_bytes)
                        .saturating_add(bounded_scan::CATALOG_SCAN_MANIFEST_INSPECTION_BYTES)
                        .saturating_add(runtime.source_bytes)
                        .max(u64::try_from(staging_bytes).unwrap_or(u64::MAX));
                    if selected_work_bytes > byte_limit {
                        return Err(TsinkError::MaintenanceWorkItemTooLarge {
                            operation: "finite known-dirty catalog transition",
                            limit: byte_limit,
                            required: selected_work_bytes,
                        });
                    }
                    self.resize_remote_catalog_memory_reservation(
                        &mut staging_reservation,
                        staging_bytes,
                    )?;
                    let loaded =
                        ChunkStorage::load_segment_index_for_runtime_refresh(selected_root)?;
                    Ok((vec![loaded], selected_work_bytes))
                } else {
                    let manifest = self
                        .persisted
                        .persisted_index
                        .read()
                        .segments_by_root
                        .get(selected_root)
                        .map(|state| state.manifest.clone());
                    let mutation_bytes = manifest.as_ref().map_or(
                        bounded_scan::CATALOG_SCAN_ENTRY_RETAINED_OVERHEAD,
                        |manifest| bounded_scan::modeled_removal_bytes(selected_root, manifest),
                    );
                    let staging_bytes =
                        bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_auxiliary_bytes(
                            selected_root,
                        )
                        .saturating_add(
                            bounded_remote::BoundedRemoteCatalogRefreshCycle::modeled_apply_transition_descriptor_bytes(
                                selected_root,
                            ),
                        )
                        .saturating_add(mutation_bytes.min(usize::MAX as u64) as usize);
                    let selected_work_bytes = 4096u64.saturating_add(
                        mutation_bytes.max(u64::try_from(staging_bytes).unwrap_or(u64::MAX)),
                    );
                    if selected_work_bytes > byte_limit {
                        return Err(TsinkError::MaintenanceWorkItemTooLarge {
                            operation: "finite known-dirty catalog transition",
                            limit: byte_limit,
                            required: selected_work_bytes,
                        });
                    }
                    self.resize_remote_catalog_memory_reservation(
                        &mut staging_reservation,
                        staging_bytes,
                    )?;
                    Ok((Vec::new(), selected_work_bytes))
                }
            })();
            let (loaded_segments, selected_work_bytes) = match prepared {
                Ok(prepared) => prepared,
                Err(err) => {
                    ctx.restore_known_persisted_segment_change_if_unmodified(selected);
                    return Err(err);
                }
            };
            return Ok(Some(PlannedPersistedCatalogRefresh::KnownDirty(
                PlannedKnownDirtyCatalogRefresh {
                    diff: selected,
                    loaded_segments,
                    finite_staging_reservation: Some(staging_reservation),
                    finite_recovery_items: Some(item_limit.saturating_sub(1)),
                    finite_recovery_bytes: Some(byte_limit.saturating_sub(selected_work_bytes)),
                },
            )));
        }

        match ctx.load_known_dirty_catalog_refresh_segments_phase(&diff) {
            Ok(loaded_segments) => Ok(Some(PlannedPersistedCatalogRefresh::KnownDirty(
                PlannedKnownDirtyCatalogRefresh {
                    diff,
                    loaded_segments,
                    finite_staging_reservation: None,
                    finite_recovery_items: None,
                    finite_recovery_bytes: None,
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
        if ctx.has_known_persisted_segment_changes() {
            self.reset_bounded_unknown_dirty_catalog_refresh();
        }
        let Some(planned) = self.plan_known_dirty_catalog_refresh()? else {
            return Ok(false);
        };

        let restore_diff = planned.restore_known_dirty_diff();
        let restore_diff_conditionally = planned.restore_known_dirty_diff_conditionally();
        let publication = self.begin_persisted_catalog_publication();
        let apply_result = match publication.apply_planned_refresh(planned) {
            Ok(result) => result,
            Err(err) => {
                if let Some(restore_diff) = restore_diff {
                    if restore_diff_conditionally {
                        ctx.restore_known_persisted_segment_change_if_unmodified(restore_diff);
                    } else {
                        ctx.restore_known_persisted_segment_changes(restore_diff);
                    }
                }
                ctx.set_persisted_index_dirty(true);
                return Err(err);
            }
        };
        if apply_result.is_deferred() {
            // The transition's visible-state mutation has already committed and the retained
            // writer cursor now owns publication of the complete resulting snapshot. Restoring
            // this consumed diff would reapply the same loaded roots on every wake, bump the
            // visibility generation, and starve the cursor forever. A terminal cursor pass
            // persists a complete registry catalog; only an actual error restores the diff.
            ctx.set_persisted_index_dirty(true);
            return Ok(true);
        }
        if !apply_result.is_applied() {
            unreachable!("known dirty catalog refresh should not use a visibility fence");
        }

        ctx.synchronize_persisted_index_dirty_with_pending();
        Ok(true)
    }

    pub(in super::super) fn drain_known_dirty_persisted_refresh_if_pending(&self) -> Result<bool> {
        let ctx = self.catalog_refresh_context();
        let mut progressed = false;
        // Snapshot only the number of exact root intents, never their path payloads. Manual flush
        // holds the compaction gate and close has stopped lifecycle producers, so this is a hard
        // termination bound as well as a guard against an unexpected producer keeping this
        // synchronous loop alive forever.
        let root_pages = {
            let pending = self.persisted.pending_persisted_segment_diff.lock();
            pending
                .added_roots
                .len()
                .checked_add(pending.removed_roots.len())
                .ok_or_else(|| {
                    TsinkError::Other(
                        "known-dirty catalog drain page count exceeds the supported range"
                            .to_string(),
                    )
                })?
        };
        for _ in 0..root_pages {
            if !ctx.has_known_persisted_segment_changes() {
                break;
            }
            if !self.apply_known_dirty_persisted_refresh_if_pending()? {
                return Err(TsinkError::Other(
                    "known-dirty catalog drain made no progress while work remained".to_string(),
                ));
            }
            progressed = true;
        }
        if ctx.has_known_persisted_segment_changes() {
            return Err(TsinkError::Other(
                "known-dirty catalog drain observed new work after its fenced page snapshot"
                    .to_string(),
            ));
        }

        // A finite tiered transition can commit the in-memory root mutation while retaining a
        // cursor for the complete shared catalog image. Drain the final cursor only after all
        // one-root pages have applied: an intervening root mutation legitimately invalidates an
        // older cursor, whereas the terminal cursor represents the complete resulting snapshot.
        if self.finite_tiered_catalog_publication_enabled()
            && self.bounded_tiered_catalog_publication_is_pending()
        {
            let publication = self.begin_persisted_catalog_publication();
            let completed = self.advance_bounded_tiered_catalog_publication(true)?;
            if !completed {
                return Err(TsinkError::Other(
                    "bounded tiered catalog drain returned without completing its cursor"
                        .to_string(),
                ));
            }
            ctx.synchronize_persisted_index_dirty_with_pending();
            drop(publication);
            progressed = true;
        }

        if ctx.has_known_persisted_segment_changes() {
            return Err(TsinkError::Other(
                "known-dirty catalog drain observed new work while completing its terminal catalog"
                    .to_string(),
            ));
        }

        Ok(progressed)
    }

    pub(in super::super) fn refresh_dirty_persisted_segments_claimed(&self) -> Result<()> {
        let ctx = self.catalog_refresh_context();
        if self.coordination.lifecycle.load(Ordering::Acquire) != STORAGE_OPEN {
            self.drain_known_dirty_persisted_refresh_if_pending()?;
            if !ctx.persisted_index_dirty() {
                return Ok(());
            }
        }

        if self.apply_known_dirty_persisted_refresh_if_pending()? {
            return Ok(());
        }

        if self.finite_tiered_catalog_publication_enabled()
            && self.bounded_tiered_catalog_publication_is_pending()
        {
            let publication = self.begin_persisted_catalog_publication();
            let completed = self.advance_bounded_tiered_catalog_publication(false)?;
            if completed {
                self.synchronize_persisted_index_dirty_with_pending();
            }
            drop(publication);
            return Ok(());
        }

        let _compaction_guard = self.compaction_gate();
        if let Some(data_path) = self
            .persisted
            .series_index_path
            .as_deref()
            .and_then(Path::parent)
        {
            super::ensure_no_pending_post_flush_replacement(data_path)?;
        }
        if self.apply_known_dirty_persisted_refresh_if_pending()? {
            return Ok(());
        }

        let finite_maintenance = self.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.runtime.maintenance_max_bytes_per_pass != u64::MAX;
        if finite_maintenance
            && self.persisted.tiered_storage.is_none()
            && self.coordination.lifecycle.load(Ordering::Acquire) == STORAGE_OPEN
        {
            if self.refresh_unknown_dirty_catalog_bounded()? {
                let ctx = self.catalog_refresh_context();
                ctx.synchronize_persisted_index_dirty_with_pending();
            }
            return Ok(());
        }

        if finite_maintenance && self.persisted.tiered_storage.is_some() {
            self.reset_bounded_unknown_dirty_catalog_refresh();
            let drain_catalog = self.coordination.lifecycle.load(Ordering::Acquire) != STORAGE_OPEN;
            match self.runtime.runtime_mode {
                StorageRuntimeMode::ReadWrite => {
                    // A finite writer owns both its local data path and the shared tier lease.
                    // Every ordinary root mutation carries an exact diff; an otherwise unknown
                    // dirty bit therefore represents catalog/registry publication debt, not
                    // permission to materialize every physical tier into one legacy transition.
                    // Reconcile the authoritative visible state through the resumable complete
                    // writer instead.
                    self.validate_shared_object_store_writer_lock()?;
                    self.coordination
                        .bounded_registry_reconciliation_required
                        .store(true, Ordering::Release);
                    let publication = self.begin_persisted_catalog_publication();
                    let completed =
                        self.advance_bounded_tiered_catalog_publication(drain_catalog)?;
                    drop(publication);
                    if completed {
                        ctx.synchronize_persisted_index_dirty_with_pending();
                    } else {
                        ctx.set_persisted_index_dirty(true);
                    }
                }
                StorageRuntimeMode::ComputeOnly => {
                    if drain_catalog {
                        self.drain_remote_catalog_bounded()?;
                        ctx.mark_remote_catalog_refresh_success();
                        ctx.synchronize_persisted_index_dirty_with_pending();
                    } else {
                        // Compute-only visibility is defined by the remote catalog. Leave this
                        // debt for the scheduled remote path above so its configured interval and
                        // failure backoff remain authoritative; never fall through to the local
                        // full scan.
                        ctx.set_persisted_index_dirty(true);
                    }
                }
            }
            return Ok(());
        }

        // ExpertUnlimited preserves the explicit complete-snapshot behavior.
        self.reset_bounded_unknown_dirty_catalog_refresh();
        let loaded = self.load_scanned_catalog_refresh()?;
        let planned = self
            .catalog_refresh_context()
            .plan_loaded_inventory_catalog_refresh_phase(loaded)?;
        let publication = self.begin_persisted_catalog_publication();
        if publication.apply_planned_refresh(planned)?.is_applied() {
            self.synchronize_persisted_index_dirty_with_pending();
        }
        Ok(())
    }

    fn drain_remote_catalog_bounded(&self) -> Result<()> {
        let config = self.persisted.tiered_storage.as_ref().ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "finite remote catalog drain requires tiered storage".to_string(),
            )
        })?;
        let pinned = tiering::require_shared_segment_catalog_pointer(config)?;
        loop {
            let completed = self.refresh_remote_catalog_bounded()?;
            let observed = tiering::require_shared_segment_catalog_pointer(config)?;
            if observed != pinned {
                self.reset_bounded_remote_catalog_refresh();
                return Err(TsinkError::Other(
                    "remote segment catalog changed during close drain".to_string(),
                ));
            }
            if completed {
                return Ok(());
            }
        }
    }

    fn refresh_remote_catalog_claimed(&self) -> Result<()> {
        let finite_maintenance = self.runtime.maintenance_max_items_per_pass != usize::MAX
            || self.runtime.maintenance_max_bytes_per_pass != u64::MAX;
        if finite_maintenance {
            if self.refresh_remote_catalog_bounded()? {
                let ctx = self.catalog_refresh_context();
                ctx.mark_remote_catalog_refresh_success();
                ctx.synchronize_persisted_index_dirty_with_pending();
            }
            return Ok(());
        }

        self.reset_bounded_remote_catalog_refresh();
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
