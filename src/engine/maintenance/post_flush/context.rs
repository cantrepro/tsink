use super::super::super::tiering::{
    self, PersistedSegmentTier, PostFlushMaintenancePolicyPlan, RetentionTierPolicy,
    SegmentInventory, SegmentInventoryEntry, SegmentPathResolver,
};
use super::super::super::*;
use super::super::*;
use crate::engine::segment::{
    verify_segment_fingerprint, IndexedSegment, SegmentValidationContext,
};

type RetentionRewriteStage = (
    Vec<SegmentInventoryEntry>,
    Vec<StagedSegmentPromotion>,
    Vec<PathBuf>,
    usize,
);

#[derive(Clone, Copy)]
pub(super) struct RetentionMaintenanceContext<'a> {
    persisted_index: &'a RwLock<PersistedIndexState>,
    persisted_index_dirty: &'a AtomicBool,
    pending_persisted_segment_diff: &'a Mutex<PendingPersistedSegmentDiff>,
    numeric_lane_path: Option<&'a Path>,
    blob_lane_path: Option<&'a Path>,
    tiered_storage: Option<&'a super::super::super::config::TieredStorageConfig>,
    next_segment_id: &'a Arc<AtomicU64>,
    chunk_point_cap: usize,
    retention_enforced: bool,
    retention_window: i64,
    observability: &'a StorageObservabilityCounters,
    #[cfg(test)]
    persist_test_hooks: &'a PersistTestHooks,
}

#[derive(Clone, Copy)]
pub(super) struct PostFlushWorkflowContext<'a> {
    retention: RetentionMaintenanceContext<'a>,
    post_flush_maintenance_pending: &'a AtomicBool,
    startup_metadata_reconcile_pending: &'a AtomicBool,
}

pub(super) struct ClaimedPostFlushMaintenanceWork {
    pub(super) run_post_flush: bool,
    pub(super) run_metadata_reconcile: bool,
}

impl ClaimedPostFlushMaintenanceWork {
    pub(super) fn any(&self) -> bool {
        self.run_post_flush || self.run_metadata_reconcile
    }
}

impl<'a> RetentionMaintenanceContext<'a> {
    pub(super) fn enabled(self) -> bool {
        self.retention_enforced && self.has_persisted_lane_paths()
    }

    pub(super) fn has_persisted_lane_paths(self) -> bool {
        self.numeric_lane_path.is_some() || self.blob_lane_path.is_some()
    }

    pub(super) fn active_retention_cutoff(self, recency_reference: Option<i64>) -> Option<i64> {
        if !self.retention_enforced {
            return None;
        }
        recency_reference.map(|reference| reference.saturating_sub(self.retention_window))
    }

    pub(super) fn apply_retention_filter(
        self,
        points: &mut Vec<DataPoint>,
        recency_reference: Option<i64>,
    ) {
        let Some(cutoff) = self.active_retention_cutoff(recency_reference) else {
            return;
        };
        points.retain(|point| point.timestamp >= cutoff);
    }

    pub(super) fn retention_tier_policy(
        self,
        retention_cutoff: i64,
        recency_reference: Option<i64>,
    ) -> RetentionTierPolicy {
        RetentionTierPolicy::new(retention_cutoff, recency_reference, self.tiered_storage)
    }

    pub(super) fn catalog_requires_refresh(self) -> bool {
        self.persisted_index_dirty() || self.has_known_persisted_segment_changes()
    }

    fn persisted_index_dirty(self) -> bool {
        self.persisted_index_dirty.load(Ordering::SeqCst)
    }

    fn has_known_persisted_segment_changes(self) -> bool {
        !self.pending_persisted_segment_diff.lock().is_empty()
    }

    fn segment_inventory(self) -> Result<SegmentInventory> {
        tiering::build_segment_inventory_fail_on_invalid(
            self.numeric_lane_path,
            self.blob_lane_path,
            self.tiered_storage,
            SegmentValidationContext::Maintenance,
        )
    }

    fn persisted_segment_inventory(self) -> SegmentInventory {
        let entries = self
            .persisted_index
            .read()
            .segments_by_root
            .iter()
            .map(|(root, state)| SegmentInventoryEntry {
                lane: state.lane,
                tier: state.tier,
                root: root.clone(),
                manifest: state.manifest.clone(),
            })
            .collect::<Vec<_>>();
        SegmentInventory::from_entries(entries)
    }

    pub(super) fn post_flush_maintenance_inventory(
        self,
    ) -> Result<(SegmentInventory, MaintenanceInventorySource)> {
        if self.catalog_requires_refresh() {
            return Ok((
                self.segment_inventory()?,
                MaintenanceInventorySource::Scanned,
            ));
        }

        Ok((
            self.persisted_segment_inventory(),
            MaintenanceInventorySource::PersistedState,
        ))
    }

    fn persisted_registry_catalog_sources(
        self,
    ) -> Vec<registry_catalog::PersistedRegistryCatalogSource> {
        self.persisted_index
            .read()
            .segments_by_root
            .iter()
            .map(
                |(root, segment)| registry_catalog::PersistedRegistryCatalogSource {
                    lane: segment.lane,
                    root: root.clone(),
                },
            )
            .collect()
    }

    pub(super) fn plan_noop_inventory_transition(
        self,
        inventory: &SegmentInventory,
        source: MaintenanceInventorySource,
    ) -> PersistedCatalogTransition {
        match source {
            // Flush publication already mirrored any newly published hot roots, so steady-state
            // no-op maintenance only needs to refresh the catalog, counters, and registry sidecar.
            MaintenanceInventorySource::PersistedState => PersistedCatalogTransition {
                visibility_fence: None,
                loaded_segments: Vec::new(),
                removed_roots: Vec::new(),
                publication: PersistedCatalogPublication::PersistedState {
                    published_segment_roots: Vec::new(),
                },
                registry_catalog_sources: Some(self.persisted_registry_catalog_sources()),
            },
            MaintenanceInventorySource::Scanned => PersistedCatalogTransition {
                visibility_fence: None,
                loaded_segments: Vec::new(),
                removed_roots: Vec::new(),
                publication: PersistedCatalogPublication::Inventory {
                    inventory: inventory.clone(),
                    tombstones: None,
                },
                registry_catalog_sources: Some(registry_catalog::inventory_sources(inventory)),
            },
        }
    }

    pub(super) fn plan_inventory_publication_transition(
        self,
        inventory: SegmentInventory,
        loaded_segments: Vec<IndexedSegment>,
        removed_roots: Vec<PathBuf>,
    ) -> PersistedCatalogTransition {
        let registry_catalog_sources = registry_catalog::inventory_sources(&inventory);
        PersistedCatalogTransition {
            visibility_fence: None,
            loaded_segments,
            removed_roots,
            publication: PersistedCatalogPublication::Inventory {
                inventory,
                tombstones: None,
            },
            registry_catalog_sources: Some(registry_catalog_sources),
        }
    }

    fn segment_path_resolver(self) -> SegmentPathResolver<'a> {
        SegmentPathResolver::new(
            self.numeric_lane_path,
            self.blob_lane_path,
            self.tiered_storage,
        )
    }

    fn stage_segment_copy_for_publish(
        self,
        source_root: &Path,
        final_root: &Path,
    ) -> Result<StagedSegmentPromotion> {
        let source_fingerprint = verify_segment_fingerprint(source_root)?;
        let staging_root =
            crate::engine::fs_utils::stage_dir_path(final_root, POST_FLUSH_COPY_STAGE_PURPOSE)?;
        crate::engine::fs_utils::copy_dir_recursive(source_root, &staging_root)?;
        crate::engine::fs_utils::sync_dir(&staging_root)?;
        let staged_fingerprint = verify_segment_fingerprint(&staging_root)?;
        if staged_fingerprint != source_fingerprint {
            let _ = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&staging_root);
            return Err(TsinkError::Other(format!(
                "staged maintenance copy {} did not match source {}",
                staging_root.display(),
                source_root.display()
            )));
        }

        Ok(StagedSegmentPromotion {
            staging_root,
            final_root: final_root.to_path_buf(),
        })
    }

    pub(super) fn promote_staged_segment_for_publish(
        self,
        promotion: &StagedSegmentPromotion,
    ) -> Result<()> {
        if crate::engine::fs_utils::path_exists_no_follow(&promotion.final_root)? {
            let staged_fingerprint = verify_segment_fingerprint(&promotion.staging_root)?;
            let final_fingerprint = verify_segment_fingerprint(&promotion.final_root)?;
            if staged_fingerprint != final_fingerprint {
                return Err(TsinkError::Other(format!(
                    "post-flush publish destination {} already exists with different contents",
                    promotion.final_root.display()
                )));
            }
            crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(
                &promotion.staging_root,
            )?;
            return Ok(());
        }

        crate::engine::fs_utils::rename_and_sync_parents(
            &promotion.staging_root,
            &promotion.final_root,
        )
    }

    pub(super) fn cleanup_staged_post_flush_paths(
        self,
        promotions: &[StagedSegmentPromotion],
        staging_cleanup_paths: &[PathBuf],
    ) {
        for promotion in promotions {
            let _ = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(
                &promotion.staging_root,
            );
        }
        for path in staging_cleanup_paths {
            let _ = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(path);
        }
    }

    fn record_expired_segment_if_removed(self, path: &Path) -> Result<bool> {
        let existed = crate::engine::fs_utils::path_exists_no_follow(path)?;
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(path)?;
        if existed {
            self.observability
                .flush
                .expired_segments_total
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(existed)
    }

    fn stage_rewrite_persisted_segment_for_retention(
        self,
        entry: &SegmentInventoryEntry,
        policy: RetentionTierPolicy,
        paths: SegmentPathResolver<'_>,
    ) -> Result<RetentionRewriteStage> {
        let source_base = paths.lane_root(entry.lane, entry.tier)?;
        let staging_base = crate::engine::fs_utils::stage_dir_path(
            &source_base,
            POST_FLUSH_REWRITE_STAGE_PURPOSE,
        )?;
        std::fs::create_dir_all(&staging_base)?;
        let rewriter = Compactor::new_with_segment_id_allocator(
            &staging_base,
            self.chunk_point_cap,
            Arc::clone(self.next_segment_id),
        );
        let loaded_segment = crate::engine::segment::load_segment(&entry.root)?;
        let outcome = rewriter
            .stage_segment_rewrite_with_retention(&loaded_segment, policy.retention_cutoff())?;
        let mut final_entries = Vec::new();
        let mut promotions = Vec::new();
        let mut tier_moves = 0usize;

        for output_root in outcome.output_roots {
            let manifest = crate::engine::segment::read_segment_manifest(&output_root)?;
            let Some(desired_tier) = policy.desired_tier_for_manifest(&manifest) else {
                self.record_expired_segment_if_removed(&output_root)?;
                continue;
            };

            let final_tier =
                if desired_tier == PersistedSegmentTier::Hot || desired_tier <= entry.tier {
                    entry.tier
                } else {
                    tier_moves = tier_moves.saturating_add(1);
                    desired_tier
                };
            let final_root = paths.segment_root(entry.lane, final_tier, &manifest)?;
            let promotion = if final_tier == entry.tier {
                StagedSegmentPromotion {
                    staging_root: output_root,
                    final_root: final_root.clone(),
                }
            } else {
                let promotion = self.stage_segment_copy_for_publish(&output_root, &final_root)?;
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&output_root)?;
                promotion
            };
            promotions.push(promotion);
            final_entries.push(SegmentInventoryEntry {
                lane: entry.lane,
                tier: final_tier,
                root: final_root,
                manifest,
            });
        }

        Ok((final_entries, promotions, vec![staging_base], tier_moves))
    }

    #[cfg(test)]
    fn invoke_post_flush_maintenance_stage_hook(self) {
        let hook = self
            .persist_test_hooks
            .post_flush_maintenance_stage_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(super) fn stage_post_flush_maintenance(
        self,
        inventory: &SegmentInventory,
        plan: PostFlushMaintenancePolicyPlan,
        policy: RetentionTierPolicy,
    ) -> Result<StagedPostFlushMaintenance> {
        #[cfg(test)]
        self.invoke_post_flush_maintenance_stage_hook();

        let mut final_entries = inventory
            .entries()
            .iter()
            .cloned()
            .map(|entry| (entry.root.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut promotions = Vec::new();
        let mut staging_cleanup_paths = Vec::new();
        let mut retired_roots = Vec::new();
        let mut tier_moves = 0usize;
        let paths = self.segment_path_resolver();

        let stage_result = (|| -> Result<()> {
            for entry in &plan.rewrite_actions {
                final_entries.remove(&entry.root);
                let (
                    rewritten_entries,
                    mut rewritten_promotions,
                    mut rewritten_cleanup_paths,
                    moved,
                ) = self.stage_rewrite_persisted_segment_for_retention(entry, policy, paths)?;
                promotions.append(&mut rewritten_promotions);
                staging_cleanup_paths.append(&mut rewritten_cleanup_paths);
                tier_moves = tier_moves.saturating_add(moved);
                retired_roots.push(RetiredPostFlushRoot {
                    root: entry.root.clone(),
                    counts_as_expired: false,
                });
                for rewritten_entry in rewritten_entries {
                    final_entries.insert(rewritten_entry.root.clone(), rewritten_entry);
                }
            }

            for move_action in &plan.move_actions {
                let entry = &move_action.entry;
                final_entries.remove(&entry.root);
                let final_root =
                    paths.segment_root(entry.lane, move_action.target_tier, &entry.manifest)?;
                promotions.push(self.stage_segment_copy_for_publish(&entry.root, &final_root)?);
                final_entries.insert(
                    final_root.clone(),
                    SegmentInventoryEntry {
                        lane: entry.lane,
                        tier: move_action.target_tier,
                        root: final_root,
                        manifest: entry.manifest.clone(),
                    },
                );
                retired_roots.push(RetiredPostFlushRoot {
                    root: entry.root.clone(),
                    counts_as_expired: false,
                });
                tier_moves = tier_moves.saturating_add(1);
            }

            for entry in &plan.expired_actions {
                final_entries.remove(&entry.root);
                retired_roots.push(RetiredPostFlushRoot {
                    root: entry.root.clone(),
                    counts_as_expired: true,
                });
            }

            Ok(())
        })();
        if let Err(err) = stage_result {
            self.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
            return Err(err);
        }

        let final_inventory = SegmentInventory::from_entries(final_entries.into_values().collect());
        let final_roots = final_inventory
            .entries()
            .iter()
            .map(|entry| entry.root.clone())
            .collect::<BTreeSet<_>>();
        let known_roots = self
            .persisted_index
            .read()
            .segments_by_root
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_roots = known_roots
            .difference(&final_roots)
            .cloned()
            .collect::<Vec<_>>();
        let staged_root_by_final_root = promotions
            .iter()
            .map(|promotion| (promotion.final_root.clone(), promotion.staging_root.clone()))
            .collect::<HashMap<_, _>>();
        let load_result = (|| -> Result<Vec<IndexedSegment>> {
            let mut loaded_segments =
                Vec::with_capacity(final_roots.difference(&known_roots).count());
            for root in final_roots.difference(&known_roots) {
                let load_root = staged_root_by_final_root
                    .get(root)
                    .map(PathBuf::as_path)
                    .unwrap_or(root.as_path());
                let mut segment = ChunkStorage::load_segment_index_for_runtime_refresh(load_root)?;
                segment.root = root.clone();
                loaded_segments.push(segment);
            }
            Ok(loaded_segments)
        })();
        let loaded_segments = match load_result {
            Ok(loaded_segments) => loaded_segments,
            Err(err) => {
                self.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                return Err(err);
            }
        };

        Ok(StagedPostFlushMaintenance {
            final_inventory,
            promotions,
            staging_cleanup_paths,
            loaded_segments,
            removed_roots,
            retired_roots,
            tier_moves,
        })
    }

    pub(super) fn record_tier_moves(self, tier_moves: usize) {
        self.observability
            .flush
            .tier_moves_total
            .fetch_add(saturating_u64_from_usize(tier_moves), Ordering::Relaxed);
    }

    pub(super) fn finalize_retired_roots(
        self,
        retired_roots: &[RetiredPostFlushRoot],
    ) -> Result<usize> {
        let mut removed = 0usize;
        for retired_root in retired_roots {
            let existed = crate::engine::fs_utils::path_exists_no_follow(&retired_root.root)?;
            if let Err(err) =
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&retired_root.root)
            {
                if !retired_root.counts_as_expired {
                    self.observability
                        .flush
                        .tier_move_errors_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(err);
            }

            if existed && retired_root.counts_as_expired {
                removed = removed.saturating_add(1);
                self.observability
                    .flush
                    .expired_segments_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(removed)
    }
}

impl<'a> PostFlushWorkflowContext<'a> {
    pub(super) fn retention(self) -> RetentionMaintenanceContext<'a> {
        self.retention
    }

    pub(super) fn claim_pending_work(self) -> ClaimedPostFlushMaintenanceWork {
        ClaimedPostFlushMaintenanceWork {
            run_post_flush: self
                .post_flush_maintenance_pending
                .swap(false, Ordering::AcqRel),
            run_metadata_reconcile: self
                .startup_metadata_reconcile_pending
                .swap(false, Ordering::AcqRel),
        }
    }

    pub(super) fn restore_post_flush_pending(self) {
        self.post_flush_maintenance_pending
            .store(true, Ordering::Release);
    }

    pub(super) fn restore_startup_metadata_reconcile_pending(self) {
        self.startup_metadata_reconcile_pending
            .store(true, Ordering::Release);
    }

    pub(super) fn mark_post_flush_pending(self) {
        self.post_flush_maintenance_pending
            .store(true, Ordering::Release);
    }

    pub(super) fn schedule_startup_maintenance(self) {
        self.restore_startup_metadata_reconcile_pending();
        if self.retention.enabled() {
            self.mark_post_flush_pending();
        }
    }
}

impl ChunkStorage {
    pub(super) fn retention_maintenance_context(&self) -> RetentionMaintenanceContext<'_> {
        RetentionMaintenanceContext {
            persisted_index: &self.persisted.persisted_index,
            persisted_index_dirty: self.persisted.persisted_index_dirty.as_ref(),
            pending_persisted_segment_diff: &self.persisted.pending_persisted_segment_diff,
            numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
            blob_lane_path: self.persisted.blob_lane_path.as_deref(),
            tiered_storage: self.persisted.tiered_storage.as_ref(),
            next_segment_id: &self.persisted.next_segment_id,
            chunk_point_cap: self.chunks.chunk_point_cap,
            retention_enforced: self.runtime.retention_enforced,
            retention_window: self.runtime.retention_window,
            observability: self.observability.as_ref(),
            #[cfg(test)]
            persist_test_hooks: &self.persist_test_hooks,
        }
    }

    pub(super) fn post_flush_workflow_context(&self) -> PostFlushWorkflowContext<'_> {
        PostFlushWorkflowContext {
            retention: self.retention_maintenance_context(),
            post_flush_maintenance_pending: &self.coordination.post_flush_maintenance_pending,
            startup_metadata_reconcile_pending: &self
                .coordination
                .startup_metadata_reconcile_pending,
        }
    }
}
