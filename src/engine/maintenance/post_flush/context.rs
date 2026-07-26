use super::super::super::tiering::{
    self, PersistedSegmentTier, PostFlushMaintenanceAction, PostFlushMaintenancePolicyPlan,
    RetentionTierPolicy, SegmentInventory, SegmentInventoryEntry, SegmentPathResolver,
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
    Vec<SegmentCopyPlan>,
    usize,
);

#[derive(Debug)]
struct SegmentCopyPlan {
    source_root: PathBuf,
    final_root: PathBuf,
    remove_source_after_copy: bool,
}

#[derive(Debug)]
struct PreparedSegmentCopy {
    source_root: PathBuf,
    final_root: PathBuf,
    staging_root: PathBuf,
    source_fingerprint: crate::engine::segment::SegmentContentFingerprint,
    measurement: crate::engine::fs_utils::RestoreDirectoryMeasurement,
    destination_governed: bool,
    remove_source_after_copy: bool,
}

fn combine_post_flush_cleanup_error(
    context: &str,
    primary: TsinkError,
    cleanup: Result<()>,
) -> TsinkError {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup_err) => TsinkError::Other(format!(
            "{context} failed: {primary}; cleanup failed: {cleanup_err}"
        )),
    }
}

fn cleanup_owned_segment_copy_staging(owned_staging_roots: &[PathBuf]) -> Result<()> {
    let mut errors = Vec::new();
    for staging_root in owned_staging_roots.iter().rev() {
        if let Err(err) =
            crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(staging_root)
        {
            errors.push(format!("{}: {err}", staging_root.display()));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::Other(format!(
            "failed to clean owned post-flush copy staging: {}",
            errors.join("; ")
        )))
    }
}

fn execute_prepared_segment_copies(
    prepared: &[PreparedSegmentCopy],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<Vec<StagedSegmentPromotion>> {
    let mut owned_staging_roots = Vec::with_capacity(prepared.len());
    let mut promotions = Vec::with_capacity(prepared.len());
    let operation = (|| -> Result<()> {
        for copy in prepared {
            let parent = copy.staging_root.parent().ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "post-flush copy staging path has no parent: {}",
                    copy.staging_root.display()
                ))
            })?;
            if copy.destination_governed {
                local_disk_budget
                    .expect("a governed post-flush copy requires a local disk budget")
                    .create_dir_all_and_sync_parents(parent)?;
            } else {
                crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;
            }

            std::fs::create_dir(&copy.staging_root).map_err(|source| TsinkError::IoWithPath {
                path: copy.staging_root.clone(),
                source,
            })?;
            owned_staging_roots.push(copy.staging_root.clone());
            crate::engine::fs_utils::sync_parent_dir(&copy.staging_root)?;

            // The bounded copy flushes and syncs every regular file and recursively syncs every
            // destination directory. It also rechecks the pre-admitted byte/entry/depth bounds so
            // a changed source cannot grow the copy beyond the aggregate reservation.
            crate::engine::fs_utils::copy_dir_contents_bounded(
                &copy.source_root,
                &copy.staging_root,
                copy.measurement,
            )?;
            let staged_fingerprint = verify_segment_fingerprint(&copy.staging_root)?;
            if staged_fingerprint != copy.source_fingerprint {
                return Err(TsinkError::Other(format!(
                    "staged maintenance copy {} did not match source {}",
                    copy.staging_root.display(),
                    copy.source_root.display()
                )));
            }

            if copy.remove_source_after_copy {
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&copy.source_root)?;
            }
            promotions.push(StagedSegmentPromotion {
                staging_root: copy.staging_root.clone(),
                final_root: copy.final_root.clone(),
            });
        }
        Ok(())
    })();

    match operation {
        Ok(()) => Ok(promotions),
        Err(primary) => Err(combine_post_flush_cleanup_error(
            "post-flush aggregate segment copy",
            primary,
            cleanup_owned_segment_copy_staging(&owned_staging_roots),
        )),
    }
}

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
    maintenance_max_items_per_pass: usize,
    maintenance_max_bytes_per_pass: u64,
    background_cursor: &'a Mutex<BackgroundRetentionMaintenanceCursor>,
    observability: &'a StorageObservabilityCounters,
    local_disk_budget: Option<&'a Arc<crate::LocalDiskBudget>>,
    #[cfg(test)]
    persist_test_hooks: &'a PersistTestHooks,
}

pub(super) struct BackgroundRetentionMaintenancePage {
    pub(super) plan: PostFlushMaintenancePolicyPlan,
    start_after_root: Option<PathBuf>,
    next_after_root: Option<PathBuf>,
    pub(super) cycle_complete: bool,
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

    fn modeled_inventory_entry_bytes(entry: &SegmentInventoryEntry) -> u64 {
        let retained = std::mem::size_of::<SegmentInventoryEntry>()
            .saturating_add(entry.root.as_os_str().as_encoded_bytes().len());
        u64::try_from(retained).unwrap_or(u64::MAX)
    }

    fn modeled_segment_source_bytes(entry: &SegmentInventoryEntry) -> Result<u64> {
        let fingerprint = crate::engine::segment::read_segment_manifest_fingerprint(&entry.root)
            .map_err(|err| {
                crate::engine::segment::segment_validation_error(
                    &entry.root,
                    SegmentValidationContext::Maintenance,
                    &err.to_string(),
                )
            })?;
        if fingerprint.manifest != entry.manifest {
            return Err(crate::engine::segment::segment_validation_error(
                &entry.root,
                SegmentValidationContext::Maintenance,
                "manifest changed after persisted-index publication",
            ));
        }
        let manifest_len = std::fs::symlink_metadata(entry.root.join("manifest.bin"))
            .map_err(|source| TsinkError::IoWithPath {
                path: entry.root.join("manifest.bin"),
                source,
            })?
            .len();
        fingerprint
            .files
            .iter()
            .try_fold(manifest_len, |total, file| {
                total.checked_add(file.file_len).ok_or_else(|| {
                    TsinkError::Other(format!(
                        "modeled retention source bytes exceed the supported range: {}",
                        entry.root.display()
                    ))
                })
            })
    }

    fn modeled_background_candidate_bytes(
        entry: &SegmentInventoryEntry,
        action: Option<PostFlushMaintenanceAction>,
    ) -> Result<u64> {
        let descriptor = Self::modeled_inventory_entry_bytes(entry);
        match action {
            Some(_) => descriptor
                .checked_add(Self::modeled_segment_source_bytes(entry)?)
                .ok_or_else(|| {
                    TsinkError::Other(format!(
                        "modeled retention candidate bytes exceed the supported range: {}",
                        entry.root.display()
                    ))
                }),
            None => Ok(descriptor),
        }
    }

    #[cfg(test)]
    fn invoke_background_retention_inspect_hook(self) {
        let hook = self
            .persist_test_hooks
            .background_retention_inspect_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(super) fn select_background_maintenance_page(
        self,
        policy: RetentionTierPolicy,
    ) -> Result<BackgroundRetentionMaintenancePage> {
        let start_after_root = self.background_cursor.lock().after_root.clone();
        let mut plan = PostFlushMaintenancePolicyPlan::default();
        let mut next_after_root = start_after_root.clone();
        let mut inspected_items = 0usize;
        let mut modeled_bytes = 0u64;
        let mut cycle_complete = false;
        let index = self.persisted_index.read();

        if self.maintenance_max_items_per_pass == 0 || self.maintenance_max_bytes_per_pass == 0 {
            if index.segments_by_root.is_empty() {
                cycle_complete = true;
            } else {
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation: "retention/tiering inventory page",
                    item_limit: self.maintenance_max_items_per_pass,
                    byte_limit: self.maintenance_max_bytes_per_pass,
                    selected_items: 0,
                    selected_bytes: 0,
                });
            }
        } else {
            let start_bound = start_after_root
                .as_ref()
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let mut entries = index
                .segments_by_root
                .range::<PathBuf, _>((start_bound, std::ops::Bound::Unbounded));

            let effective_item_limit = self
                .maintenance_max_items_per_pass
                .min(super::recovery::MAX_POST_FLUSH_REPLACEMENT_RECORDS / 2);
            while inspected_items < effective_item_limit {
                let Some((root, state)) = entries.next() else {
                    cycle_complete = true;
                    break;
                };
                #[cfg(test)]
                self.invoke_background_retention_inspect_hook();
                inspected_items = inspected_items.saturating_add(1);
                let entry = SegmentInventoryEntry {
                    lane: state.lane,
                    tier: state.tier,
                    root: root.clone(),
                    manifest: state.manifest.clone(),
                };
                let action = policy.post_flush_maintenance_action(&entry);
                let candidate_bytes = Self::modeled_background_candidate_bytes(&entry, action)?;
                if candidate_bytes > self.maintenance_max_bytes_per_pass {
                    return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                        operation: "retention/tiering inventory page",
                        item_limit: self.maintenance_max_items_per_pass,
                        byte_limit: self.maintenance_max_bytes_per_pass,
                        selected_items: plan.action_count(),
                        selected_bytes: modeled_bytes,
                    });
                }
                if candidate_bytes
                    > self
                        .maintenance_max_bytes_per_pass
                        .saturating_sub(modeled_bytes)
                {
                    break;
                }
                modeled_bytes = modeled_bytes.saturating_add(candidate_bytes);
                next_after_root = Some(root.clone());
                if let Some(action) = action {
                    plan.push(&entry, action);
                }
            }
        }

        Ok(BackgroundRetentionMaintenancePage {
            plan,
            start_after_root,
            next_after_root,
            cycle_complete,
        })
    }

    pub(super) fn commit_background_maintenance_page(
        self,
        page: &BackgroundRetentionMaintenancePage,
    ) {
        let mut cursor = self.background_cursor.lock();
        debug_assert_eq!(cursor.after_root, page.start_after_root);
        cursor.after_root = if page.cycle_complete {
            None
        } else {
            page.next_after_root.clone()
        };
    }

    pub(super) fn reset_background_maintenance_cursor(self) {
        self.background_cursor.lock().after_root = None;
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

    pub(super) fn plan_noop_inventory_transition(
        self,
        inventory: &SegmentInventory,
        source: MaintenanceInventorySource,
    ) -> PersistedCatalogTransition {
        match source {
            // Flush publication already mirrored any newly published hot roots, so steady-state
            // no-op maintenance only needs to refresh catalog-derived counters. With no root
            // delta, the per-segment registry sidecar already describes the visible set.
            MaintenanceInventorySource::PersistedState => PersistedCatalogTransition {
                visibility_fence: None,
                loaded_segments: Vec::new(),
                removed_roots: Vec::new(),
                publication: PersistedCatalogPublication::PersistedState {
                    published_segment_roots: Vec::new(),
                    refresh_tombstones: false,
                },
                registry_catalog_update: None,
            },
            MaintenanceInventorySource::Scanned => PersistedCatalogTransition {
                visibility_fence: None,
                loaded_segments: Vec::new(),
                removed_roots: Vec::new(),
                publication: PersistedCatalogPublication::Inventory {
                    inventory: inventory.clone(),
                    refresh_tombstones: false,
                },
                registry_catalog_update: Some(
                    registry_catalog::PersistedRegistryCatalogUpdate::Complete(
                        registry_catalog::inventory_sources(inventory),
                    ),
                ),
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
                refresh_tombstones: false,
            },
            registry_catalog_update: Some(
                registry_catalog::PersistedRegistryCatalogUpdate::Complete(
                    registry_catalog_sources,
                ),
            ),
        }
    }

    fn segment_path_resolver(self) -> SegmentPathResolver<'a> {
        SegmentPathResolver::new(
            self.numeric_lane_path,
            self.blob_lane_path,
            self.tiered_storage,
        )
    }

    fn create_retention_rewrite_staging_base(self, staging_base: &Path) -> Result<()> {
        Self::create_retention_rewrite_staging_base_with_budget(
            staging_base,
            self.local_disk_budget,
        )
    }

    fn create_retention_rewrite_staging_base_with_budget(
        staging_base: &Path,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        let parent = staging_base.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "retention rewrite staging path has no parent: {}",
                staging_base.display()
            ))
        })?;
        let governed = local_disk_budget
            .map(|budget| budget.governs_entry(staging_base))
            .transpose()?
            .unwrap_or(false);
        let create = || -> Result<()> {
            if governed {
                local_disk_budget
                    .expect("a governed staging path requires a local disk budget")
                    .create_dir_all_and_sync_parents(parent)?;
            } else {
                crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;
            }
            crate::engine::fs_utils::create_staging_dir_exclusive(staging_base)?;
            crate::engine::fs_utils::sync_parent_dir(staging_base)
        };
        let Some(local_disk_budget) = local_disk_budget.filter(|_| governed) else {
            return create();
        };

        let staging_paths = [staging_base.to_path_buf()];
        let missing_parents =
            local_disk_budget.missing_managed_parent_directory_count(&staging_paths)?;
        let entry_count = missing_parents.checked_add(1).ok_or_else(|| {
            TsinkError::Other(
                "retention rewrite staging entry count exceeds the supported range".to_string(),
            )
        })?;
        let peak_bytes = entry_count
            .checked_mul(local_disk_budget.snapshot_restore_entry_staging_allowance_bytes()?)
            .ok_or_else(|| {
                TsinkError::Other(
                    "retention rewrite staging allowance exceeds the supported byte range"
                        .to_string(),
                )
            })?;

        // The unique rewrite namespace is a small transaction of its own. Its entry and any
        // missing ancestors are admitted before creation, then reconciled before Compactor starts
        // the separate immutable-output rewrite phase.
        local_disk_budget.with_reconciled_maintenance_reservation(
            crate::DiskCategory::Temporary,
            peak_bytes,
            create,
        )
    }

    fn stage_segment_copies_for_publish(
        self,
        plans: &[SegmentCopyPlan],
    ) -> Result<Vec<StagedSegmentPromotion>> {
        Self::stage_segment_copy_group(plans, self.local_disk_budget)
    }

    fn stage_segment_copy_group(
        plans: &[SegmentCopyPlan],
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<Vec<StagedSegmentPromotion>> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        // All source trees are immutable segment directories. Measure and fingerprint the whole
        // group before the first destination parent or staging entry can be created.
        let mut prepared = Vec::with_capacity(plans.len());
        let mut governed_staging_paths = Vec::new();
        let mut governed_staging_peak = 0u64;
        let mut aggregate_budget_needed = false;
        let mut entry_allowance_bytes = None;
        for plan in plans {
            let measurement =
                crate::engine::fs_utils::measure_restore_directory(&plan.source_root)?;
            let source_fingerprint = verify_segment_fingerprint(&plan.source_root)?;
            let staging_root = crate::engine::fs_utils::stage_dir_path(
                &plan.final_root,
                POST_FLUSH_COPY_STAGE_PURPOSE,
            )?;
            let destination_governed = local_disk_budget
                .map(|budget| budget.governs_entry(&staging_root))
                .transpose()?
                .unwrap_or(false);
            let source_cleanup_governed = if plan.remove_source_after_copy {
                local_disk_budget
                    .map(|budget| budget.governs_entry(&plan.source_root))
                    .transpose()?
                    .unwrap_or(false)
            } else {
                false
            };

            if destination_governed {
                let budget = local_disk_budget
                    .expect("a governed post-flush copy requires a local disk budget");
                budget.validate_managed_directory_path(&staging_root)?;
                let allowance = match entry_allowance_bytes {
                    Some(allowance) => allowance,
                    None => {
                        let allowance = budget.snapshot_restore_entry_staging_allowance_bytes()?;
                        entry_allowance_bytes = Some(allowance);
                        allowance
                    }
                };
                governed_staging_peak = governed_staging_peak
                    .checked_add(measurement.staging_admission_bytes(allowance)?)
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "post-flush copy group peak exceeds the supported byte range"
                                .to_string(),
                        )
                    })?;
                governed_staging_paths.push(staging_root.clone());
            }
            if source_cleanup_governed {
                local_disk_budget
                    .expect("a governed source cleanup requires a local disk budget")
                    .validate_managed_directory_path(&plan.source_root)?;
            }
            aggregate_budget_needed |= destination_governed || source_cleanup_governed;
            prepared.push(PreparedSegmentCopy {
                source_root: plan.source_root.clone(),
                final_root: plan.final_root.clone(),
                staging_root,
                source_fingerprint,
                measurement,
                destination_governed,
                remove_source_after_copy: plan.remove_source_after_copy,
            });
        }

        let execute = || execute_prepared_segment_copies(&prepared, local_disk_budget);
        let Some(local_disk_budget) = local_disk_budget.filter(|_| aggregate_budget_needed) else {
            return execute();
        };
        let missing_parent_count =
            local_disk_budget.missing_managed_parent_directory_count(&governed_staging_paths)?;
        let missing_parent_peak = missing_parent_count
            .checked_mul(entry_allowance_bytes.unwrap_or(0))
            .ok_or_else(|| {
                TsinkError::Other(
                    "post-flush copy parent allowance exceeds the supported byte range".to_string(),
                )
            })?;
        let aggregate_peak = governed_staging_peak
            .checked_add(missing_parent_peak)
            .ok_or_else(|| {
                TsinkError::Other(
                    "post-flush aggregate copy peak exceeds the supported byte range".to_string(),
                )
            })?;

        // This is deliberately separate from both the preceding local retention rewrite and the
        // later rename/catalog publication. One aggregate Maintenance reservation covers every
        // governed copy in the group and remains live through file/directory synchronization,
        // fingerprint verification, source cleanup, and failure cleanup. The closure therefore
        // uses only unbudgeted filesystem primitives; final accounting is installed by the outer
        // reconciled reservation. Configured object-store tiers are intentionally outside the
        // local quota: their copies still use the same bounded, fully-synchronized group and
        // rollback path, but require a separate destination coordinator for byte admission.
        local_disk_budget.with_reconciled_maintenance_reservation(
            crate::DiskCategory::Temporary,
            aggregate_peak,
            execute,
        )
    }

    fn promote_staged_segment_for_publish_with_budget(
        self,
        promotion: &StagedSegmentPromotion,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
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
            crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
                &promotion.staging_root,
                local_disk_budget,
                crate::DiskCategory::Temporary,
            )?;
            return Ok(());
        }

        crate::engine::fs_utils::rename_and_sync_parents_budgeted_reclassify(
            &promotion.staging_root,
            &promotion.final_root,
            local_disk_budget,
            crate::DiskCategory::Temporary,
            crate::DiskCategory::Segments,
        )
    }

    pub(super) fn promote_staged_segment_for_publish_unbudgeted(
        self,
        promotion: &StagedSegmentPromotion,
    ) -> Result<()> {
        self.promote_staged_segment_for_publish_with_budget(promotion, None)
    }

    pub(super) fn with_post_flush_promotion_reservation<T, F>(
        self,
        promotions: &[StagedSegmentPromotion],
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        let prepare_parents = || -> Result<T> {
            for promotion in promotions {
                let parent = promotion.final_root.parent().ok_or_else(|| {
                    TsinkError::InvalidConfiguration(format!(
                        "post-flush final segment path has no parent: {}",
                        promotion.final_root.display()
                    ))
                })?;
                if let Some(budget) = self.local_disk_budget {
                    if budget.governs_entry(parent)? {
                        budget.create_dir_all_and_sync_parents(parent)?;
                        continue;
                    }
                }
                crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;
            }
            operation()
        };
        let Some(budget) = self.local_disk_budget else {
            return prepare_parents();
        };

        let mut governed_final_roots = Vec::new();
        let mut governed_promotions = 0u64;
        for promotion in promotions {
            let staging_governed = budget.governs_entry(&promotion.staging_root)?;
            let final_governed = budget.governs_entry(&promotion.final_root)?;
            if staging_governed != final_governed {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "post-flush promotion crosses the managed disk boundary: staging={}, final={}",
                    promotion.staging_root.display(),
                    promotion.final_root.display()
                )));
            }
            if final_governed {
                governed_promotions = governed_promotions.checked_add(1).ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush promotion count exceeds the supported range".to_string(),
                    )
                })?;
                governed_final_roots.push(promotion.final_root.clone());
            }
        }
        let missing_parents =
            budget.missing_managed_parent_directory_count(&governed_final_roots)?;
        let entry_count = governed_promotions
            .checked_add(missing_parents)
            .ok_or_else(|| {
                TsinkError::Other(
                    "post-flush promotion entry count exceeds the supported range".to_string(),
                )
            })?;
        let peak_bytes = entry_count
            .checked_mul(budget.snapshot_restore_entry_staging_allowance_bytes()?)
            .ok_or_else(|| {
                TsinkError::Other(
                    "post-flush promotion allowance exceeds the supported range".to_string(),
                )
            })?;
        // Keep a zero-byte reconciled operation even when every promotion is external. The
        // operation's failure path can still remove the governed local Prepared marker, and that
        // unlink must settle the budget's previously-accounted marker charge.
        budget.with_reconciled_maintenance_reservation(
            crate::DiskCategory::Temporary,
            peak_bytes,
            prepare_parents,
        )
    }

    fn cleanup_staged_post_flush_paths_with_budget(
        self,
        promotions: &[StagedSegmentPromotion],
        staging_cleanup_paths: &[PathBuf],
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    ) -> Result<()> {
        let mut cleanup_errors = Vec::new();
        for promotion in promotions {
            if let Err(err) =
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
                    &promotion.staging_root,
                    local_disk_budget,
                    crate::DiskCategory::Temporary,
                )
            {
                cleanup_errors.push(format!("{}: {err}", promotion.staging_root.display()));
            }
        }
        for path in staging_cleanup_paths {
            if let Err(err) =
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
                    path,
                    local_disk_budget,
                    crate::DiskCategory::Temporary,
                )
            {
                cleanup_errors.push(format!("{}: {err}", path.display()));
            }
        }
        if !cleanup_errors.is_empty() {
            return Err(TsinkError::Other(format!(
                "failed to clean staged post-flush paths: {}",
                cleanup_errors.join("; ")
            )));
        }
        Ok(())
    }

    pub(super) fn cleanup_staged_post_flush_paths(
        self,
        promotions: &[StagedSegmentPromotion],
        staging_cleanup_paths: &[PathBuf],
    ) -> Result<()> {
        self.cleanup_staged_post_flush_paths_with_budget(
            promotions,
            staging_cleanup_paths,
            self.local_disk_budget,
        )
    }

    pub(super) fn cleanup_staged_post_flush_paths_unbudgeted(
        self,
        promotions: &[StagedSegmentPromotion],
        staging_cleanup_paths: &[PathBuf],
    ) -> Result<()> {
        self.cleanup_staged_post_flush_paths_with_budget(promotions, staging_cleanup_paths, None)
    }

    fn record_expired_segment_if_removed(self, path: &Path) -> Result<bool> {
        let existed = crate::engine::fs_utils::path_exists_no_follow(path)?;
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent_budgeted(
            path,
            self.local_disk_budget,
            crate::DiskCategory::Temporary,
        )?;
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
        self.create_retention_rewrite_staging_base(&staging_base)?;
        let rewriter = Compactor::new_with_segment_id_allocator_and_disk_budget(
            &staging_base,
            self.chunk_point_cap,
            Arc::clone(self.next_segment_id),
            self.local_disk_budget.cloned(),
        )
        .with_output_disk_category(crate::DiskCategory::Temporary);
        let mut final_entries = Vec::new();
        let mut promotions = Vec::new();
        let mut copy_plans = Vec::new();
        let mut tier_moves = 0usize;
        let rewrite_result = (|| -> Result<()> {
            let loaded_segment = crate::engine::segment::load_segment(&entry.root)?;
            let outcome = rewriter
                .stage_segment_rewrite_with_retention(&loaded_segment, policy.retention_cutoff())?;

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
                if final_tier == entry.tier {
                    promotions.push(StagedSegmentPromotion {
                        staging_root: output_root,
                        final_root: final_root.clone(),
                    });
                } else {
                    copy_plans.push(SegmentCopyPlan {
                        source_root: output_root,
                        final_root: final_root.clone(),
                        remove_source_after_copy: true,
                    });
                }
                final_entries.push(SegmentInventoryEntry {
                    lane: entry.lane,
                    tier: final_tier,
                    root: final_root,
                    manifest,
                });
            }
            Ok(())
        })();
        if let Err(primary) = rewrite_result {
            let cleanup = self
                .cleanup_staged_post_flush_paths(&promotions, std::slice::from_ref(&staging_base));
            return Err(combine_post_flush_cleanup_error(
                "post-flush retention rewrite",
                primary,
                cleanup,
            ));
        }

        Ok((
            final_entries,
            promotions,
            vec![staging_base],
            copy_plans,
            tier_moves,
        ))
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

    fn stage_post_flush_maintenance_with_scope(
        self,
        inventory: &SegmentInventory,
        plan: PostFlushMaintenancePolicyPlan,
        policy: RetentionTierPolicy,
        scope: PostFlushMaintenanceStageScope,
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
        let mut copy_plans = Vec::new();
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
                    mut rewritten_copy_plans,
                    moved,
                ) = self.stage_rewrite_persisted_segment_for_retention(entry, policy, paths)?;
                promotions.append(&mut rewritten_promotions);
                staging_cleanup_paths.append(&mut rewritten_cleanup_paths);
                copy_plans.append(&mut rewritten_copy_plans);
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
                copy_plans.push(SegmentCopyPlan {
                    source_root: entry.root.clone(),
                    final_root: final_root.clone(),
                    remove_source_after_copy: false,
                });
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

            // Every rewrite has completed and reconciled its own output reservation. No final
            // segment name, catalog entry, or source retirement has been published yet, so this
            // is the transaction boundary for one aggregate governed-capacity decision. External
            // tier bytes remain outside the local-disk quota, but are still measured, bounded,
            // synchronized, verified, and rolled back as a group.
            let mut copy_promotions = self.stage_segment_copies_for_publish(&copy_plans)?;
            promotions.append(&mut copy_promotions);

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
            let cleanup = self.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
            return Err(combine_post_flush_cleanup_error(
                "post-flush staging",
                err,
                cleanup,
            ));
        }

        let final_inventory = SegmentInventory::from_entries(final_entries.into_values().collect());
        let final_roots = final_inventory
            .entries()
            .iter()
            .map(|entry| entry.root.clone())
            .collect::<BTreeSet<_>>();
        let known_roots = match scope {
            PostFlushMaintenanceStageScope::CompleteInventory => self
                .persisted_index
                .read()
                .segments_by_root
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            PostFlushMaintenanceStageScope::SelectedPage => {
                let persisted = self.persisted_index.read();
                final_roots
                    .iter()
                    .filter(|root| persisted.segments_by_root.contains_key(*root))
                    .cloned()
                    .collect::<BTreeSet<_>>()
            }
        };
        let removed_roots = match scope {
            PostFlushMaintenanceStageScope::CompleteInventory => known_roots
                .difference(&final_roots)
                .cloned()
                .collect::<Vec<_>>(),
            PostFlushMaintenanceStageScope::SelectedPage => retired_roots
                .iter()
                .map(|retired| retired.root.clone())
                .collect::<Vec<_>>(),
        };
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
                let cleanup =
                    self.cleanup_staged_post_flush_paths(&promotions, &staging_cleanup_paths);
                return Err(combine_post_flush_cleanup_error(
                    "post-flush staged index load",
                    err,
                    cleanup,
                ));
            }
        };

        let publication = match scope {
            PostFlushMaintenanceStageScope::CompleteInventory => {
                StagedPostFlushPublication::CompleteInventory(final_inventory)
            }
            PostFlushMaintenanceStageScope::SelectedPage => {
                StagedPostFlushPublication::PersistedStateDelta {
                    published_roots: final_roots.into_iter().collect(),
                }
            }
        };

        Ok(StagedPostFlushMaintenance {
            publication,
            promotions,
            staging_cleanup_paths,
            loaded_segments,
            removed_roots,
            retired_roots,
            tier_moves,
        })
    }

    pub(super) fn stage_post_flush_maintenance(
        self,
        inventory: &SegmentInventory,
        plan: PostFlushMaintenancePolicyPlan,
        policy: RetentionTierPolicy,
    ) -> Result<StagedPostFlushMaintenance> {
        self.stage_post_flush_maintenance_with_scope(
            inventory,
            plan,
            policy,
            PostFlushMaintenanceStageScope::CompleteInventory,
        )
    }

    pub(super) fn stage_post_flush_maintenance_page(
        self,
        plan: PostFlushMaintenancePolicyPlan,
        policy: RetentionTierPolicy,
    ) -> Result<StagedPostFlushMaintenance> {
        let inventory = SegmentInventory::from_entries(plan.source_entries());
        self.stage_post_flush_maintenance_with_scope(
            &inventory,
            plan,
            policy,
            PostFlushMaintenanceStageScope::SelectedPage,
        )
    }

    pub(super) fn record_tier_moves(self, tier_moves: usize) {
        self.observability
            .flush
            .tier_moves_total
            .fetch_add(saturating_u64_from_usize(tier_moves), Ordering::Relaxed);
    }

    pub(super) fn record_expired_segments(self, expired_segments: usize) {
        self.observability.flush.expired_segments_total.fetch_add(
            saturating_u64_from_usize(expired_segments),
            Ordering::Relaxed,
        );
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
            maintenance_max_items_per_pass: self.runtime.maintenance_max_items_per_pass,
            maintenance_max_bytes_per_pass: self.runtime.maintenance_max_bytes_per_pass,
            background_cursor: &self.coordination.background_retention_maintenance_cursor,
            observability: self.observability.as_ref(),
            local_disk_budget: self.persisted.local_disk_budget.as_ref(),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::{SegmentWriter, WalHighWatermark};
    use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
    use crate::{Label, LocalDiskLimits, Value};

    struct TestRetentionContextState {
        persisted_index: RwLock<PersistedIndexState>,
        persisted_index_dirty: AtomicBool,
        pending_persisted_segment_diff: Mutex<PendingPersistedSegmentDiff>,
        next_segment_id: Arc<AtomicU64>,
        background_cursor: Mutex<BackgroundRetentionMaintenanceCursor>,
        observability: StorageObservabilityCounters,
        persist_test_hooks: PersistTestHooks,
    }

    impl Default for TestRetentionContextState {
        fn default() -> Self {
            Self {
                persisted_index: RwLock::new(PersistedIndexState::default()),
                persisted_index_dirty: AtomicBool::new(false),
                pending_persisted_segment_diff: Mutex::new(PendingPersistedSegmentDiff::default()),
                next_segment_id: Arc::new(AtomicU64::new(1)),
                background_cursor: Mutex::new(BackgroundRetentionMaintenanceCursor::default()),
                observability: StorageObservabilityCounters::default(),
                persist_test_hooks: PersistTestHooks::default(),
            }
        }
    }

    impl TestRetentionContextState {
        fn context<'a>(
            &'a self,
            local_disk_budget: Option<&'a Arc<crate::LocalDiskBudget>>,
        ) -> RetentionMaintenanceContext<'a> {
            RetentionMaintenanceContext {
                persisted_index: &self.persisted_index,
                persisted_index_dirty: &self.persisted_index_dirty,
                pending_persisted_segment_diff: &self.pending_persisted_segment_diff,
                numeric_lane_path: None,
                blob_lane_path: None,
                tiered_storage: None,
                next_segment_id: &self.next_segment_id,
                chunk_point_cap: 1,
                retention_enforced: true,
                retention_window: 1,
                maintenance_max_items_per_pass: 1_024,
                maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
                background_cursor: &self.background_cursor,
                observability: &self.observability,
                local_disk_budget,
                persist_test_hooks: &self.persist_test_hooks,
            }
        }
    }

    fn write_test_segment(source_lane: &Path) -> PathBuf {
        let registry = SeriesRegistry::new();
        let series = registry
            .resolve_or_insert("post_flush_copy", &[Label::new("host", "a")])
            .unwrap();
        let points = vec![
            ChunkPoint {
                ts: 10,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 20,
                value: Value::F64(2.0),
            },
        ];
        let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
        let chunk = Chunk {
            header: ChunkHeader {
                series_id: series.series_id,
                lane: ValueLane::Numeric,
                value_family: Some(SeriesValueFamily::F64),
                point_count: points.len() as u16,
                min_ts: 10,
                max_ts: 20,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points,
            encoded_payload: encoded.payload,
            wal_lowwater: WalHighWatermark::default(),
            wal_highwater: WalHighWatermark::default(),
        };
        let chunks = HashMap::from([(series.series_id, vec![chunk])]);
        let writer = SegmentWriter::new(source_lane, 0, 1).unwrap();
        writer.write_segment(&registry, &chunks).unwrap();
        writer.layout().root.clone()
    }

    fn copy_plans(source_root: &Path, budget_root: &Path) -> Vec<SegmentCopyPlan> {
        vec![
            SegmentCopyPlan {
                source_root: source_root.to_path_buf(),
                final_root: budget_root.join("warm-a").join("seg-0000000000000001"),
                remove_source_after_copy: false,
            },
            SegmentCopyPlan {
                source_root: source_root.to_path_buf(),
                final_root: budget_root.join("warm-b").join("seg-0000000000000001"),
                remove_source_after_copy: false,
            },
        ]
    }

    fn aggregate_copy_peak(budget: &crate::LocalDiskBudget, plans: &[SegmentCopyPlan]) -> u64 {
        let allowance = budget
            .snapshot_restore_entry_staging_allowance_bytes()
            .unwrap();
        let mut peak = 0u64;
        for plan in plans {
            peak = peak
                .checked_add(
                    crate::engine::fs_utils::measure_restore_directory(&plan.source_root)
                        .unwrap()
                        .staging_admission_bytes(allowance)
                        .unwrap(),
                )
                .unwrap();
        }
        let targets = plans
            .iter()
            .map(|plan| plan.final_root.clone())
            .collect::<Vec<_>>();
        peak.checked_add(
            budget
                .missing_managed_parent_directory_count(&targets)
                .unwrap()
                .checked_mul(allowance)
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn aggregate_segment_copy_rejects_before_first_destination_entry() {
        let temp = TempDir::new().unwrap();
        let source_root = write_test_segment(&temp.path().join("source"));
        let budget_root = temp.path().join("data");
        let probe = crate::LocalDiskBudget::open(&budget_root, LocalDiskLimits::default()).unwrap();
        let plans = copy_plans(&source_root, &budget_root);
        let aggregate_peak = aggregate_copy_peak(&probe, &plans);
        let single_peak = aggregate_copy_peak(&probe, &plans[..1]);
        assert!(aggregate_peak > single_peak);
        drop(probe);

        let budget = crate::LocalDiskBudget::open(
            &budget_root,
            LocalDiskLimits {
                max_bytes: Some(aggregate_peak - 1),
                ..LocalDiskLimits::default()
            },
        )
        .unwrap();
        let error =
            RetentionMaintenanceContext::<'static>::stage_segment_copy_group(&plans, Some(&budget))
                .expect_err("the complete copy group must be rejected before staging");
        assert!(matches!(
            error,
            TsinkError::InsufficientCompactionHeadroom {
                used: 0,
                reserved: 0,
                requested,
                ..
            } if requested == aggregate_peak
        ));
        assert!(plans
            .iter()
            .all(|plan| !plan.final_root.parent().unwrap().exists()));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn aggregate_segment_copy_reconciles_exact_successful_bytes() {
        let temp = TempDir::new().unwrap();
        let source_root = write_test_segment(&temp.path().join("source"));
        let measurement = crate::engine::fs_utils::measure_restore_directory(&source_root).unwrap();
        let budget_root = temp.path().join("data");
        let budget =
            crate::LocalDiskBudget::open(&budget_root, LocalDiskLimits::default()).unwrap();
        let plans = copy_plans(&source_root, &budget_root);

        let promotions =
            RetentionMaintenanceContext::<'static>::stage_segment_copy_group(&plans, Some(&budget))
                .unwrap();
        assert_eq!(promotions.len(), 2);
        assert!(promotions
            .iter()
            .all(|promotion| verify_segment_fingerprint(&promotion.staging_root).is_ok()));
        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.accounted_bytes,
            measurement.logical_bytes.checked_mul(2).unwrap()
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn aggregate_segment_copy_failure_cleans_staging_and_releases_reservation() {
        let temp = TempDir::new().unwrap();
        let source_root = write_test_segment(&temp.path().join("source"));
        let budget_root = temp.path().join("data");
        let budget =
            crate::LocalDiskBudget::open(&budget_root, LocalDiskLimits::default()).unwrap();
        let plans = copy_plans(&source_root, &budget_root);
        let failure_parent = plans[0].final_root.parent().unwrap().to_path_buf();
        let sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
            move |candidate| {
                candidate.parent() == Some(failure_parent.as_path())
                    && candidate
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(".tmp-tsink-post-flush-stage-copy-"))
            },
            "injected post-flush copied-directory sync failure",
        );

        let error =
            RetentionMaintenanceContext::<'static>::stage_segment_copy_group(&plans, Some(&budget))
                .expect_err("a copied-directory durability failure must reject the copy group");
        drop(sync_failure);
        assert!(error
            .to_string()
            .contains("injected post-flush copied-directory sync failure"));
        for plan in &plans {
            let parent = plan.final_root.parent().unwrap();
            assert_eq!(
                std::fs::read_dir(parent)
                    .map(|entries| entries.count())
                    .unwrap_or(0),
                0
            );
        }
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    }

    #[test]
    fn rewrite_staging_root_is_admitted_before_parent_creation() {
        let temp = TempDir::new().unwrap();
        let budget_root = temp.path().join("data");
        let staging_root = budget_root
            .join("missing")
            .join(".tmp-tsink-post-flush-retention-rewrite-lane_numeric-0000000000000001");
        let probe = crate::LocalDiskBudget::open(&budget_root, LocalDiskLimits::default()).unwrap();
        let allowance = probe
            .snapshot_restore_entry_staging_allowance_bytes()
            .unwrap();
        let peak = allowance.checked_mul(2).unwrap();
        drop(probe);
        let budget = crate::LocalDiskBudget::open(
            &budget_root,
            LocalDiskLimits {
                max_bytes: Some(peak - 1),
                ..LocalDiskLimits::default()
            },
        )
        .unwrap();

        let error =
            RetentionMaintenanceContext::<'static>::create_retention_rewrite_staging_base_with_budget(
                &staging_root,
                Some(&budget),
            )
            .expect_err("rewrite staging ancestry must be admitted before creation");
        assert!(matches!(
            error,
            TsinkError::InsufficientCompactionHeadroom { requested, .. } if requested == peak
        ));
        assert!(!budget_root.join("missing").exists());
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn rewrite_staging_root_never_reuses_a_raced_directory() {
        let temp = TempDir::new().unwrap();
        let staging_root = temp
            .path()
            .join(".tmp-tsink-post-flush-retention-rewrite-lane_numeric-raced");
        std::fs::create_dir(&staging_root).unwrap();
        std::fs::write(staging_root.join("foreign"), b"foreign").unwrap();

        let error =
            RetentionMaintenanceContext::<'static>::create_retention_rewrite_staging_base_with_budget(
                &staging_root,
                None,
            )
            .expect_err("rewrite staging must use create-exclusive semantics");

        assert!(matches!(
            error,
            TsinkError::IoWithPath { ref source, .. }
                if source.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(
            std::fs::read(staging_root.join("foreign")).unwrap(),
            b"foreign"
        );
    }

    #[test]
    fn aggregate_promotion_zero_headroom_rejects_before_parent_creation_or_operation() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let budget = crate::LocalDiskBudget::open_with_available_space_for_test(
            &data_path,
            LocalDiskLimits::default(),
            0,
        )
        .unwrap();
        let promotion = StagedSegmentPromotion {
            staging_root: data_path.join("staging").join("seg-0000000000000001"),
            final_root: data_path
                .join("missing-final-parent")
                .join("seg-0000000000000001"),
        };
        let allowance = budget
            .snapshot_restore_entry_staging_allowance_bytes()
            .unwrap();
        let operation_called = AtomicBool::new(false);
        let state = TestRetentionContextState::default();

        let result = state
            .context(Some(&budget))
            .with_post_flush_promotion_reservation(&[promotion], || {
                operation_called.store(true, Ordering::SeqCst);
                Ok(())
            });

        assert!(matches!(
            result,
            Err(TsinkError::InsufficientDiskSpace {
                required,
                available: 0
            }) if required == allowance * 2
        ));
        assert!(!operation_called.load(Ordering::SeqCst));
        assert!(!data_path.join("missing-final-parent").exists());
    }

    #[test]
    fn all_external_promotion_failure_reconciles_local_marker_cleanup() {
        let temp = TempDir::new().unwrap();
        let data_path = temp.path().join("data");
        let marker_dir = data_path.join(".post-flush-replacements");
        let marker_path = marker_dir.join("transaction-0000000000000001-0000000000000002.json");
        std::fs::create_dir_all(&marker_dir).unwrap();
        std::fs::write(&marker_path, b"accounted-prepared-marker").unwrap();
        let budget = crate::LocalDiskBudget::open(&data_path, LocalDiskLimits::default()).unwrap();
        let before = budget.snapshot();
        assert!(before.accounted_bytes > 0);

        let external_root = temp.path().join("external");
        let promotion = StagedSegmentPromotion {
            staging_root: external_root.join("stage").join("seg-0000000000000001"),
            final_root: external_root.join("final").join("seg-0000000000000001"),
        };
        let state = TestRetentionContextState::default();
        let result: Result<()> = state
            .context(Some(&budget))
            .with_post_flush_promotion_reservation(&[promotion], || {
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&marker_path)?;
                Err(TsinkError::Other(
                    "injected external promotion failure".to_string(),
                ))
            });

        assert!(result
            .expect_err("the injected promotion error must be retained")
            .to_string()
            .contains("injected external promotion failure"));
        assert!(!marker_path.exists());
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, 0);
        assert_eq!(after.active_reservations, 0);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(
            after.reconciliations_total,
            before.reconciliations_total + 1
        );
    }
}
