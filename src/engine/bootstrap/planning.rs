use super::*;

pub(super) struct StartupPlan {
    wal_enabled: bool,
    startup_memory_budget: usize,
    storage_options: ChunkStorageOptions,
    paths: config::StoragePathLayout,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    data_directory_manifest: Option<data_directory_manifest::OpenedDataDirectoryManifest>,
    runtime_inputs: StartupRuntimeInputs,
}

pub(super) struct StartupRuntimeInputs {
    pub(super) background_threads_enabled: bool,
    pub(super) background_fail_fast: bool,
    pub(super) data_path_process_lock: Option<DataPathProcessLock>,
    pub(super) shared_object_store_process_lock: Option<SharedObjectStoreProcessLock>,
}

pub(super) struct StartupPlanningPhase;

impl StartupPlanningPhase {
    pub(super) fn prepare(builder: &StorageBuilder) -> Result<StartupPlan> {
        builder
            .query_budget_limits()
            .validate()
            .map_err(crate::QueryBudgetError::from)?;
        validate_tiered_storage_config(builder)?;

        // Startup recovery runs before the engine's live accounting state exists. Derive the
        // same configured modeled-memory ceiling up front so even the initial disk reconciliation
        // is admitted rather than temporarily escaping the configured bound.
        let startup_memory_budget = builder.memory_limit_bytes();
        // Reject corrupt, newer, and unknown manifestless directories before acquiring a process
        // lock can create `.tsink.lock`. The same inspection is repeated while that lock is held
        // before any recovery cleanup may mutate durable state.
        data_directory_manifest::preflight_before_process_lock(builder)?;
        let data_path_process_lock = acquire_startup_data_path_process_lock(builder)?;
        let paths = config::StoragePathLayout::from(builder);
        let local_disk_budget = resolve_local_disk_budget(builder, startup_memory_budget)?;
        let data_directory_manifest =
            data_directory_manifest::prepare_before_recovery(builder, local_disk_budget.as_ref())?;
        validate_tiered_storage_disk_scope(builder, local_disk_budget.as_deref())?;
        validate_disk_limit_relationships(builder, local_disk_budget.as_deref())?;
        validate_owned_local_storage_namespaces(
            &paths,
            builder.data_path(),
            local_disk_budget.as_deref(),
        )?;
        // Validate the complete local/remote scope before creating a lease file in the shared
        // root. The lease must nevertheless be held before any tier recovery or publication.
        let shared_object_store_process_lock =
            acquire_startup_shared_object_store_process_lock(builder)?;
        let storage_options = ChunkStorageOptions::from(builder);
        let tombstone_lanes = configured_tombstone_transaction_lanes(builder, &storage_options);
        // Every operation below can rename or delete durable state, including tier roots outside
        // `data_path`. Only the read-write startup that successfully acquired and still holds the
        // process lease may run them. Compute-only builders intentionally accept a data path for
        // remote catalog configuration but never lease it, so they must remain read-only here.
        if builder.runtime_mode() == StorageRuntimeMode::ReadWrite {
            if let Some(data_path) = builder.data_path() {
                let _held_data_path_lease = data_path_process_lock.as_ref().ok_or_else(|| {
                    TsinkError::Other(
                        "read-write startup recovery requires the held data-path process lease"
                            .to_string(),
                    )
                })?;
                if storage_options.tiered_storage.is_some() {
                    shared_object_store_process_lock
                        .as_ref()
                        .ok_or_else(|| {
                            TsinkError::Other(
                                "tiered startup recovery requires the held shared writer lease"
                                    .to_string(),
                            )
                        })?
                        .validate()?;
                }
                cleanup_post_flush_recovery_blockers(
                    data_path,
                    &paths,
                    storage_options.tiered_storage.as_ref(),
                    local_disk_budget.as_ref(),
                    startup_memory_budget,
                )?;
                super::super::maintenance::finalize_pending_post_flush_replacements_for_startup(
                    data_path,
                    paths.numeric_lane_path.as_deref(),
                    paths.blob_lane_path.as_deref(),
                    storage_options.tiered_storage.as_ref(),
                    local_disk_budget.as_ref(),
                    startup_memory_budget,
                )?;
                crate::engine::tombstone::recover_tombstone_transaction_with_memory_admission(
                    data_path,
                    &tombstone_lanes,
                    local_disk_budget.as_ref(),
                    |required| {
                        if startup_memory_budget != usize::MAX && required > startup_memory_budget {
                            return Err(TsinkError::MemoryBudgetExceeded {
                                budget: startup_memory_budget,
                                required,
                            });
                        }
                        Ok(())
                    },
                )?;
                cleanup_owned_local_storage_orphans(
                    &paths,
                    &tombstone_lanes,
                    local_disk_budget.as_ref(),
                    startup_memory_budget,
                )?;
            }
        }
        Ok(StartupPlan {
            wal_enabled: builder.wal_enabled(),
            startup_memory_budget,
            paths,
            local_disk_budget,
            data_directory_manifest,
            runtime_inputs: StartupRuntimeInputs {
                background_threads_enabled: storage_options.background_threads_enabled,
                background_fail_fast: storage_options.background_fail_fast,
                data_path_process_lock,
                shared_object_store_process_lock,
            },
            storage_options,
        })
    }
}

fn validate_owned_local_storage_namespaces(
    paths: &config::StoragePathLayout,
    data_path: Option<&Path>,
    local_disk_budget: Option<&crate::LocalDiskBudget>,
) -> Result<()> {
    let Some(budget) = local_disk_budget else {
        return Ok(());
    };

    if let Some(data_path) = data_path {
        budget.validate_managed_file_path(
            &data_path.join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME),
        )?;
        let coordinator_dir =
            data_path.join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_DIR_NAME);
        budget.validate_managed_directory_path(&coordinator_dir)?;
        budget.validate_managed_file_path(
            &coordinator_dir.join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_FILE_NAME),
        )?;
    }

    if let Some(wal_path) = paths.wal_path.as_deref() {
        budget.validate_managed_directory_path(wal_path)?;
        for file_path in [
            wal_path.join("wal.published"),
            wal_path.join("wal.published.tmp"),
        ] {
            budget.validate_managed_file_path(&file_path)?;
        }
    }
    if let Some(series_index_path) = paths.series_index_path.as_deref() {
        let data_path = series_index_path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "series registry path has no parent directory: {}",
                series_index_path.display()
            ))
        })?;
        for directory in [
            SeriesRegistry::incremental_dir(series_index_path),
            registry_catalog::catalog_store_path(series_index_path),
            data_path.join(rollups::ROLLUP_DIR_NAME),
            data_path.join(super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME),
        ] {
            budget.validate_managed_directory_path(&directory)?;
        }
        for file_path in [
            series_index_path.to_path_buf(),
            SeriesRegistry::incremental_path(series_index_path),
            registry_catalog::catalog_path(series_index_path),
            data_path.join(tiering::SEGMENT_CATALOG_FILE_NAME),
            data_path
                .join(rollups::ROLLUP_DIR_NAME)
                .join("policies.json"),
            data_path.join(rollups::ROLLUP_DIR_NAME).join("state.json"),
        ] {
            budget.validate_managed_file_path(&file_path)?;
        }
    }

    for lane_path in [
        paths.numeric_lane_path.as_deref(),
        paths.blob_lane_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let segments_path = lane_path.join("segments");
        let tombstone_store_path = lane_path.join(format!(
            "{}.store",
            crate::engine::tombstone::TOMBSTONES_FILE_NAME
        ));
        for directory in [
            lane_path.to_path_buf(),
            segments_path.clone(),
            segments_path.join("L0"),
            segments_path.join("L1"),
            segments_path.join("L2"),
            lane_path.join(".compaction-replacements"),
            tombstone_store_path.clone(),
            tombstone_store_path.join("shards"),
        ] {
            budget.validate_managed_directory_path(&directory)?;
        }
        budget.validate_managed_file_path(
            &lane_path.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
        )?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OwnedLocalStorageOrphanCleanupOrder {
    phase: u8,
    lane_index: usize,
    position: u8,
}

enum OwnedLocalStorageOrphanCleanupOperation {
    Temporary {
        order: OwnedLocalStorageOrphanCleanupOrder,
        plan: crate::disk_budget::OwnedTemporaryEntryCleanupPlan,
    },
    TombstoneFinal {
        order: OwnedLocalStorageOrphanCleanupOrder,
        plan: crate::engine::tombstone::TombstoneFinalOrphanCleanupPlan,
    },
}

impl OwnedLocalStorageOrphanCleanupOperation {
    fn order(&self) -> OwnedLocalStorageOrphanCleanupOrder {
        match self {
            Self::Temporary { order, .. } | Self::TombstoneFinal { order, .. } => *order,
        }
    }

    fn modeled_heap_bytes(&self) -> Result<usize> {
        match self {
            Self::Temporary { plan, .. } => plan.modeled_heap_bytes(),
            Self::TombstoneFinal { plan, .. } => plan.modeled_heap_bytes(),
        }
    }

    fn execution_scratch_bytes(&self) -> Result<usize> {
        match self {
            Self::Temporary { .. } => Ok(0),
            Self::TombstoneFinal { plan, .. } => plan.execution_scratch_bytes(),
        }
    }

    fn execute_raw(self) -> Result<()> {
        match self {
            Self::Temporary { plan, .. } => plan.execute_locked().map(|_| ()),
            Self::TombstoneFinal { plan, .. } => plan.execute_raw().map(|_| ()),
        }
    }
}

struct OwnedLocalStorageOrphanCleanupPlan {
    operations: Vec<OwnedLocalStorageOrphanCleanupOperation>,
    has_governed_entries: bool,
}

const OWNED_ORPHAN_DISCOVERY_PATH_SLOTS: usize = 3;
const OWNED_ORPHAN_MANAGED_RESOLUTION_PATH_SLOTS: usize = 4;
// A recursive raw child can retain a primary 256 KiB path error plus both its surviving-parent
// and outer-directory sync errors. One MiB therefore bounds each native terminal error without
// truncating the single-error contract; the aggregate multi-error rendering below is capped to the
// same admitted slot instead of allocating duplicate `to_string` copies.
const OWNED_ORPHAN_TERMINAL_ERROR_BYTES: usize = 1024 * 1024;
const OWNED_ORPHAN_TERMINAL_ERROR_SLOTS: usize = 4;
const OWNED_ORPHAN_TERMINAL_ERROR_RETAINED_BYTES: usize =
    OWNED_ORPHAN_TERMINAL_ERROR_SLOTS * OWNED_ORPHAN_TERMINAL_ERROR_BYTES;

impl OwnedLocalStorageOrphanCleanupPlan {
    fn modeled_retained_bytes(&self) -> Result<usize> {
        self.operations
            .capacity()
            .checked_mul(std::mem::size_of::<OwnedLocalStorageOrphanCleanupOperation>())
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<OwnedLocalStorageOrphanCleanupPlan>())
            })
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup plan capacity overflow".to_string())
            })?
            .checked_add(
                self.operations
                    .iter()
                    .try_fold(0usize, |total, operation| {
                        total
                            .checked_add(operation.modeled_heap_bytes()?)
                            .ok_or_else(|| {
                                TsinkError::Other(
                                    "startup orphan-cleanup retained-memory overflow".to_string(),
                                )
                            })
                    })?,
            )
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup retained-memory overflow".to_string())
            })
    }

    fn build(
        paths: &config::StoragePathLayout,
        tombstone_lanes: &[crate::engine::tombstone::TombstoneLane],
        budget: &Arc<crate::LocalDiskBudget>,
        memory_budget_bytes: usize,
        max_namespace_entries: usize,
    ) -> Result<Self> {
        let series_index_path = paths.series_index_path.as_deref().ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "startup orphan cleanup requires a configured series registry".to_string(),
            )
        })?;
        let data_path = series_index_path.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "series registry path has no parent directory: {}",
                series_index_path.display()
            ))
        })?;
        let mut builder = OwnedLocalStorageOrphanCleanupPlanBuilder {
            plan: Self {
                operations: Vec::new(),
                has_governed_entries: false,
            },
            namespace_budget: crate::engine::fs_utils::RecoveryNamespaceBudget::new(
                max_namespace_entries,
            ),
            budget,
            memory_budget_bytes,
        };
        builder.admit_current_retained()?;

        // Discovery order is deliberately independent from execution order. Repeated scans of a
        // shared parent are real work and therefore each consume the one global namespace budget.
        builder.discover_fixed_atomic_target(0, || {
            data_path.join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        })?;
        builder.discover_fixed_atomic_target(1, || series_index_path.to_path_buf())?;
        builder.discover_fixed_atomic_target(2, || {
            SeriesRegistry::incremental_path(series_index_path)
        })?;
        builder.discover_fixed_atomic_target(3, || {
            registry_catalog::catalog_path(series_index_path)
        })?;
        builder.discover_fixed_atomic_target(4, || {
            data_path.join(tiering::SEGMENT_CATALOG_FILE_NAME)
        })?;
        builder.discover_fixed_atomic_target(5, || {
            data_path
                .join(rollups::ROLLUP_DIR_NAME)
                .join("policies.json")
        })?;
        builder.discover_fixed_atomic_target(6, || {
            data_path.join(rollups::ROLLUP_DIR_NAME).join("state.json")
        })?;

        let registry_directory = builder
            .allocate_discovery_path(|| SeriesRegistry::incremental_dir(series_index_path))?;
        builder.discover_matching_directory(
            registry_directory,
            false,
            |candidate| {
                crate::disk_budget::atomic_write_temp_target_name(candidate)
                    .is_some_and(is_registry_incremental_file_name)
            },
            OwnedLocalStorageOrphanCleanupOrder {
                phase: 1,
                lane_index: 0,
                position: 0,
            },
            "startup registry atomic temporary cleanup",
        )?;

        let coordinator = builder.allocate_discovery_path(|| {
            data_path
                .join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
                .join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_FILE_NAME)
        })?;
        builder.discover_atomic_target(
            coordinator,
            OwnedLocalStorageOrphanCleanupOrder {
                phase: 2,
                lane_index: 0,
                position: 0,
            },
            "startup tombstone coordinator atomic temporary cleanup",
        )?;

        for (lane_index, lane) in tombstone_lanes.iter().enumerate() {
            if budget.governs_entry(&lane.manifest_path)? {
                let manifest = builder.allocate_discovery_path(|| lane.manifest_path.clone())?;
                builder.discover_atomic_target(
                    manifest,
                    OwnedLocalStorageOrphanCleanupOrder {
                        phase: 3,
                        lane_index,
                        position: 0,
                    },
                    "startup tombstone manifest atomic temporary cleanup",
                )?;
                let shards_directory = builder.allocate_discovery_path(|| {
                    lane.manifest_path
                        .with_file_name(format!(
                            "{}.store",
                            crate::engine::tombstone::TOMBSTONES_FILE_NAME
                        ))
                        .join("shards")
                })?;
                builder.discover_matching_directory(
                    shards_directory,
                    false,
                    |candidate| {
                        crate::disk_budget::atomic_write_temp_target_name(candidate)
                            .is_some_and(is_tombstone_shard_name)
                    },
                    OwnedLocalStorageOrphanCleanupOrder {
                        phase: 3,
                        lane_index,
                        position: 1,
                    },
                    "startup tombstone shard atomic temporary cleanup",
                )?;
            }
        }

        for (lane_index, lane_path) in [
            paths.numeric_lane_path.as_deref(),
            paths.blob_lane_path.as_deref(),
        ]
        .into_iter()
        .flatten()
        .enumerate()
        {
            let replacement_directory =
                builder.allocate_discovery_path(|| lane_path.join(".compaction-replacements"))?;
            builder.discover_matching_directory(
                replacement_directory,
                false,
                |candidate| {
                    crate::disk_budget::atomic_write_temp_target_name(candidate)
                        .is_some_and(is_compaction_replacement_marker_name)
                },
                OwnedLocalStorageOrphanCleanupOrder {
                    phase: 4,
                    lane_index,
                    position: 0,
                },
                "startup local compaction-marker temporary cleanup",
            )?;
            for level in 0..=2u8 {
                let staging_directory = builder.allocate_discovery_path(|| {
                    lane_path.join("segments").join(format!("L{level}"))
                })?;
                builder.discover_matching_directory(
                    staging_directory,
                    true,
                    is_segment_staging_name,
                    OwnedLocalStorageOrphanCleanupOrder {
                        phase: 4,
                        lane_index,
                        position: level + 1,
                    },
                    "startup local segment-staging cleanup",
                )?;
            }
        }

        // Final-orphan scans intentionally run after every generic discovery scan, while their
        // explicit order metadata places each raw execution after that lane's manifest/shard temp
        // plans and before local compaction/staging execution.
        for (lane_index, lane) in tombstone_lanes.iter().enumerate() {
            builder.discover_tombstone_final(
                lane,
                OwnedLocalStorageOrphanCleanupOrder {
                    phase: 3,
                    lane_index,
                    position: 2,
                },
            )?;
        }

        builder
            .plan
            .operations
            .sort_unstable_by_key(OwnedLocalStorageOrphanCleanupOperation::order);
        if builder.plan.operations.is_empty() {
            return Ok(builder.plan);
        }
        let retained = builder.plan.modeled_retained_bytes()?;
        let execution_scratch = builder
            .plan
            .operations
            .iter()
            .try_fold(0usize, |peak, operation| {
                Ok::<_, TsinkError>(peak.max(operation.execution_scratch_bytes()?))
            })?;
        let execution_peak = retained
            .checked_add(execution_scratch)
            .and_then(|bytes| bytes.checked_add(OWNED_ORPHAN_TERMINAL_ERROR_BYTES))
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup execution memory overflow".to_string())
            })?;
        let required_peak = if builder.plan.has_governed_entries {
            let reconciliation_peak = budget
                .reconciliation_memory_peak_bytes()
                .checked_add(OWNED_ORPHAN_TERMINAL_ERROR_RETAINED_BYTES)
                .ok_or_else(|| {
                    TsinkError::Other("startup orphan-cleanup reconciliation overflow".to_string())
                })?;
            execution_peak.max(reconciliation_peak)
        } else {
            execution_peak
        };
        // Child plans are consumed and dropped before terminal reconciliation, so these peaks are
        // alternatives rather than additive. The initial exact scan covers a superset of the
        // namespace after successful deletion and supplies the scan's modeled memory threshold.
        crate::disk_budget::admit_startup_memory(memory_budget_bytes, required_peak)?;
        Ok(builder.plan)
    }

    fn execute_with_before_execute(
        self,
        budget: &Arc<crate::LocalDiskBudget>,
        memory_budget_bytes: usize,
        before_execute: impl FnOnce(),
    ) -> Result<()> {
        let Self {
            operations,
            has_governed_entries,
        } = self;
        if operations.is_empty() {
            return Ok(());
        }
        before_execute();
        let reservation = if has_governed_entries {
            Some(budget.reserve(
                crate::DiskCategory::Temporary,
                0,
                crate::DiskReservationKind::Recovery,
            )?)
        } else {
            None
        };

        let mut operations = operations.into_iter();
        let cleanup_result = loop {
            let Some(operation) = operations.next() else {
                break Ok(());
            };
            if let Err(err) = operation.execute_raw() {
                break Err(err);
            }
        };
        // Release every unexecuted retained child plan before settlement and the terminal scan.
        drop(operations);
        let settlement_result = reservation
            .map(|reservation| reservation.commit(0, 0))
            .transpose()
            .map(|_| ());
        let reconciliation_result = if has_governed_entries {
            match memory_budget_bytes.checked_sub(OWNED_ORPHAN_TERMINAL_ERROR_RETAINED_BYTES) {
                Some(reconciliation_memory_limit) => budget
                    .reconcile_when_idle_with_memory_limit(reconciliation_memory_limit)
                    .map(|_| ()),
                None => Err(TsinkError::MemoryBudgetExceeded {
                    budget: memory_budget_bytes,
                    required: OWNED_ORPHAN_TERMINAL_ERROR_RETAINED_BYTES,
                }),
            }
        } else {
            Ok(())
        };
        combine_owned_local_storage_orphan_cleanup_results(
            cleanup_result,
            settlement_result,
            reconciliation_result,
        )
    }
}

struct OwnedLocalStorageOrphanCleanupPlanBuilder<'a> {
    plan: OwnedLocalStorageOrphanCleanupPlan,
    namespace_budget: crate::engine::fs_utils::RecoveryNamespaceBudget,
    budget: &'a Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
}

impl OwnedLocalStorageOrphanCleanupPlanBuilder<'_> {
    fn admit_current_retained(&self) -> Result<()> {
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            self.plan.modeled_retained_bytes()?,
        )
    }

    fn allocate_discovery_path(&self, build: impl FnOnce() -> PathBuf) -> Result<PathBuf> {
        let baseline = self.plan.modeled_retained_bytes()?;
        let path_slot = OWNED_ORPHAN_DISCOVERY_PATH_SLOTS
            .checked_mul(
                std::mem::size_of::<PathBuf>()
                    .checked_add(crate::engine::tombstone::MAX_TOMBSTONE_TRANSACTION_PATH_BYTES)
                    .ok_or_else(|| {
                        TsinkError::Other("startup orphan-cleanup path memory overflow".to_string())
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(4 * 1024))
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup path memory overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            baseline.checked_add(path_slot).ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup path memory overflow".to_string())
            })?,
        )?;
        let path = build();
        if path.as_os_str().as_encoded_bytes().len()
            > crate::engine::tombstone::MAX_TOMBSTONE_TRANSACTION_PATH_BYTES
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "startup orphan-cleanup path exceeds the {}-byte bound: {}",
                crate::engine::tombstone::MAX_TOMBSTONE_TRANSACTION_PATH_BYTES,
                path.display()
            )));
        }
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            baseline
                .checked_add(std::mem::size_of::<PathBuf>())
                .and_then(|bytes| bytes.checked_add(path.capacity()))
                .ok_or_else(|| {
                    TsinkError::Other("startup orphan-cleanup path memory overflow".to_string())
                })?,
        )?;
        Ok(path)
    }

    fn discover_fixed_atomic_target(
        &mut self,
        index: usize,
        build: impl FnOnce() -> PathBuf,
    ) -> Result<()> {
        let target = self.allocate_discovery_path(build)?;
        self.discover_atomic_target(
            target,
            OwnedLocalStorageOrphanCleanupOrder {
                phase: 0,
                lane_index: index,
                position: 0,
            },
            "startup fixed-target atomic temporary cleanup",
        )
    }

    fn discover_atomic_target(
        &mut self,
        target: PathBuf,
        order: OwnedLocalStorageOrphanCleanupOrder,
        operation: &'static str,
    ) -> Result<()> {
        let parent = target.parent().ok_or_else(|| {
            TsinkError::InvalidConfiguration(format!(
                "managed file has no parent directory: {}",
                target.display()
            ))
        })?;
        let target_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "managed file name is not valid UTF-8: {}",
                    target.display()
                ))
            })?;
        let discovery_retained = std::mem::size_of::<PathBuf>()
            .checked_add(target.capacity())
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup target memory overflow".to_string())
            })?;
        self.discover_temporary(
            parent,
            discovery_retained,
            false,
            |candidate| {
                crate::disk_budget::atomic_write_temp_target_name(candidate) == Some(target_name)
            },
            order,
            operation,
        )
    }

    fn discover_matching_directory<F>(
        &mut self,
        directory: PathBuf,
        allow_directories: bool,
        owns_name: F,
        order: OwnedLocalStorageOrphanCleanupOrder,
        operation: &'static str,
    ) -> Result<()>
    where
        F: Fn(&str) -> bool,
    {
        let discovery_retained = std::mem::size_of::<PathBuf>()
            .checked_add(directory.capacity())
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup directory memory overflow".to_string())
            })?;
        self.discover_temporary(
            &directory,
            discovery_retained,
            allow_directories,
            owns_name,
            order,
            operation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn discover_temporary<F>(
        &mut self,
        directory: &Path,
        discovery_retained: usize,
        allow_directories: bool,
        owns_name: F,
        order: OwnedLocalStorageOrphanCleanupOrder,
        operation: &'static str,
    ) -> Result<()>
    where
        F: Fn(&str) -> bool,
    {
        let baseline = self
            .plan
            .modeled_retained_bytes()?
            .checked_add(discovery_retained)
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup retained-memory overflow".to_string())
            })?;
        let managed_resolution_scratch = OWNED_ORPHAN_MANAGED_RESOLUTION_PATH_SLOTS
            .checked_mul(
                std::mem::size_of::<PathBuf>()
                    .checked_add(crate::engine::tombstone::MAX_TOMBSTONE_TRANSACTION_PATH_BYTES)
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "startup orphan-cleanup path-resolution overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup path-resolution overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            baseline
                .checked_add(managed_resolution_scratch)
                .ok_or_else(|| {
                    TsinkError::Other("startup orphan-cleanup path-resolution overflow".to_string())
                })?,
        )?;
        let Some(plan) = self.budget.plan_owned_temporary_entries_matching_locked(
            directory,
            allow_directories,
            owns_name,
            operation,
            &mut self.namespace_budget,
            self.memory_budget_bytes,
            baseline,
        )?
        else {
            return Ok(());
        };
        if !self.budget.governs_entry(plan.governed_path())? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "startup temporary cleanup escaped the managed disk root: {}",
                plan.governed_path().display()
            )));
        }
        self.push_operation(
            OwnedLocalStorageOrphanCleanupOperation::Temporary { order, plan },
            true,
        )
    }

    fn discover_tombstone_final(
        &mut self,
        lane: &crate::engine::tombstone::TombstoneLane,
        order: OwnedLocalStorageOrphanCleanupOrder,
    ) -> Result<()> {
        let baseline = self.plan.modeled_retained_bytes()?;
        let Some(plan) = crate::engine::tombstone::plan_unreferenced_tombstone_shard_cleanup(
            lane,
            &mut self.namespace_budget,
            baseline,
            |required| crate::disk_budget::admit_startup_memory(self.memory_budget_bytes, required),
        )?
        else {
            return Ok(());
        };
        let governed = self.budget.governs_entry(plan.governed_path())?;
        self.push_operation(
            OwnedLocalStorageOrphanCleanupOperation::TombstoneFinal { order, plan },
            governed,
        )
    }

    fn push_operation(
        &mut self,
        operation: OwnedLocalStorageOrphanCleanupOperation,
        governed: bool,
    ) -> Result<()> {
        let prospective_capacity = if self.plan.operations.len() == self.plan.operations.capacity()
        {
            self.plan.operations.len().checked_add(1).ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup operation count overflow".to_string())
            })?
        } else {
            self.plan.operations.capacity()
        };
        let existing_heap = self
            .plan
            .operations
            .iter()
            .try_fold(0usize, |total, retained| {
                total
                    .checked_add(retained.modeled_heap_bytes()?)
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "startup orphan-cleanup retained-memory overflow".to_string(),
                        )
                    })
            })?;
        let prospective = prospective_capacity
            .checked_mul(std::mem::size_of::<OwnedLocalStorageOrphanCleanupOperation>())
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<OwnedLocalStorageOrphanCleanupPlan>())
            })
            .and_then(|bytes| bytes.checked_add(existing_heap))
            .and_then(|bytes| operation.modeled_heap_bytes().ok()?.checked_add(bytes))
            .ok_or_else(|| {
                TsinkError::Other("startup orphan-cleanup retained-memory overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            prospective
                .checked_add(std::mem::size_of::<OwnedLocalStorageOrphanCleanupOperation>())
                .ok_or_else(|| {
                    TsinkError::Other(
                        "startup orphan-cleanup operation staging overflow".to_string(),
                    )
                })?,
        )?;
        if self.plan.operations.len() == self.plan.operations.capacity() {
            self.plan.operations.try_reserve_exact(1).map_err(|_| {
                TsinkError::Other(
                    "unable to allocate bounded startup orphan-cleanup plan".to_string(),
                )
            })?;
        }
        self.plan.operations.push(operation);
        self.plan.has_governed_entries |= governed;
        self.admit_current_retained()
    }
}

struct BoundedOwnedOrphanTerminalError {
    message: String,
    truncated: bool,
}

impl BoundedOwnedOrphanTerminalError {
    const TRUNCATION_MARKER: &'static str = "...[truncated]";

    fn try_new() -> Option<Self> {
        let mut message = String::new();
        message
            .try_reserve_exact(OWNED_ORPHAN_TERMINAL_ERROR_BYTES)
            .ok()?;
        Some(Self {
            message,
            truncated: false,
        })
    }

    fn finish(mut self) -> String {
        if self.truncated {
            self.message.push_str(Self::TRUNCATION_MARKER);
        }
        self.message
    }
}

impl std::fmt::Write for BoundedOwnedOrphanTerminalError {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        if self.truncated {
            return Ok(());
        }
        let content_limit =
            OWNED_ORPHAN_TERMINAL_ERROR_BYTES.saturating_sub(Self::TRUNCATION_MARKER.len());
        let remaining = content_limit.saturating_sub(self.message.len());
        if value.len() <= remaining {
            self.message.push_str(value);
            return Ok(());
        }
        let mut end = remaining.min(value.len());
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.message.push_str(&value[..end]);
        self.truncated = true;
        Ok(())
    }
}

fn combine_owned_local_storage_orphan_cleanup_results(
    cleanup_result: Result<()>,
    settlement_result: Result<()>,
    reconciliation_result: Result<()>,
) -> Result<()> {
    let error_count = usize::from(cleanup_result.is_err())
        + usize::from(settlement_result.is_err())
        + usize::from(reconciliation_result.is_err());
    match error_count {
        0 => Ok(()),
        1 => match (cleanup_result, settlement_result, reconciliation_result) {
            (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
            _ => unreachable!("one recorded startup orphan-cleanup error must have failed"),
        },
        _ => {
            let Some(mut message) = BoundedOwnedOrphanTerminalError::try_new() else {
                // The native errors are already retained. If the bounded aggregate rendering
                // cannot be allocated, return the first one without attempting another heap
                // allocation or risking an abort in the terminal error path.
                return match (cleanup_result, settlement_result, reconciliation_result) {
                    (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
                    _ => unreachable!("multiple startup orphan-cleanup errors must have failed"),
                };
            };
            let _ = std::fmt::Write::write_str(
                &mut message,
                "aggregate startup orphan cleanup failed: ",
            );
            let mut first = true;
            for (label, error) in [
                ("cleanup failed", cleanup_result.as_ref().err()),
                ("disk settlement failed", settlement_result.as_ref().err()),
                (
                    "disk reconciliation failed",
                    reconciliation_result.as_ref().err(),
                ),
            ] {
                let Some(error) = error else {
                    continue;
                };
                if !first {
                    let _ = std::fmt::Write::write_str(&mut message, "; ");
                }
                first = false;
                let _ = std::fmt::Write::write_fmt(&mut message, format_args!("{label}: {error}"));
            }
            Err(TsinkError::Other(message.finish()))
        }
    }
}

fn cleanup_owned_local_storage_orphans(
    paths: &config::StoragePathLayout,
    tombstone_lanes: &[crate::engine::tombstone::TombstoneLane],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget_bytes: usize,
) -> Result<()> {
    let Some(budget) = local_disk_budget else {
        return Ok(());
    };
    if paths.series_index_path.is_none() {
        return Ok(());
    }
    budget.with_serialized_managed_file_mutation(|| {
        OwnedLocalStorageOrphanCleanupPlan::build(
            paths,
            tombstone_lanes,
            budget,
            memory_budget_bytes,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        )?
        .execute_with_before_execute(budget, memory_budget_bytes, || {})
    })
}

#[cfg(test)]
pub(super) fn cleanup_owned_local_storage_orphans_with_limits_and_before_execute_for_test(
    paths: &config::StoragePathLayout,
    tombstone_lanes: &[crate::engine::tombstone::TombstoneLane],
    budget: &Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
    max_namespace_entries: usize,
    before_execute: impl FnOnce(),
) -> Result<()> {
    budget.with_serialized_managed_file_mutation(|| {
        OwnedLocalStorageOrphanCleanupPlan::build(
            paths,
            tombstone_lanes,
            budget,
            memory_budget_bytes,
            max_namespace_entries,
        )?
        .execute_with_before_execute(budget, memory_budget_bytes, before_execute)
    })
}

#[cfg(test)]
pub(super) fn preflight_owned_local_storage_orphans_with_limits_for_test(
    paths: &config::StoragePathLayout,
    tombstone_lanes: &[crate::engine::tombstone::TombstoneLane],
    budget: &Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
    max_namespace_entries: usize,
) -> Result<()> {
    budget.with_serialized_managed_file_mutation(|| {
        OwnedLocalStorageOrphanCleanupPlan::build(
            paths,
            tombstone_lanes,
            budget,
            memory_budget_bytes,
            max_namespace_entries,
        )
        .map(drop)
    })
}

fn cleanup_post_flush_recovery_blockers(
    data_path: &Path,
    paths: &config::StoragePathLayout,
    tiered_storage: Option<&config::TieredStorageConfig>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget_bytes: usize,
) -> Result<()> {
    // Atomic marker temporaries are never a valid transaction phase, and raw rewrite/copy stages
    // are deliberately omitted from the marker. Prepared recovery owns only published finals;
    // Committing recovery has already validated every final. Reclaim these exact owned shapes
    // before a Recovery rename reservation so crash debris cannot consume the physical headroom
    // required to roll back or retire the loader-visible names.
    let budget = local_disk_budget.ok_or_else(|| {
        TsinkError::Other(
            "persistent read-write startup cleanup requires a local disk budget".to_string(),
        )
    })?;
    budget.with_serialized_managed_file_mutation(|| {
        let plan = PostFlushRecoveryBlockerCleanupPlan::build(
            data_path,
            paths,
            tiered_storage,
            budget,
            memory_budget_bytes,
        )?;
        plan.execute(budget, memory_budget_bytes)
    })
}

struct PostFlushRecoveryBlockerCleanupPlan {
    operations: Vec<PostFlushRecoveryBlockerCleanupOperation>,
    has_governed_entries: bool,
}

struct PostFlushRecoveryBlockerCleanupOperation {
    parent: PathBuf,
    parent_identity: same_file::Handle,
    boundary: PostFlushRecoveryBlockerBoundary,
    roots: Vec<PathBuf>,
    removal_plan: crate::engine::fs_utils::RecursiveNamespaceRemovalPlan,
    kind: PostFlushRecoveryBlockerCleanupKind,
}

#[derive(Clone, Copy)]
enum PostFlushRecoveryBlockerCleanupKind {
    MarkerTemporary,
    StagingDirectory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PostFlushRecoveryBlockerBoundary {
    Managed,
    External(PathBuf),
}

#[derive(Debug, Eq, PartialEq)]
struct PostFlushLaneRoot {
    path: PathBuf,
    boundary: PostFlushRecoveryBlockerBoundary,
}

// External lane resolution simultaneously retains configured, lexical, canonical, and joined
// paths. Keep that fixed-cardinality peak finite before `PathBuf::join` or canonicalization can
// allocate. This ceiling is above the supported native path range on the production platforms and
// matches the existing 256 KiB startup path guards used by adjacent recovery namespaces.
const POST_FLUSH_EXTERNAL_PATH_MAX_BYTES: usize = 256 * 1024;
const POST_FLUSH_EXTERNAL_RESOLUTION_PATH_SLOTS: usize = 8;

impl PostFlushRecoveryBlockerCleanupPlan {
    fn build(
        data_path: &Path,
        paths: &config::StoragePathLayout,
        tiered_storage: Option<&config::TieredStorageConfig>,
        budget: &Arc<crate::LocalDiskBudget>,
        memory_budget_bytes: usize,
    ) -> Result<Self> {
        Self::build_with_namespace_limit(
            data_path,
            paths,
            tiered_storage,
            budget,
            memory_budget_bytes,
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        )
    }

    fn build_with_namespace_limit(
        data_path: &Path,
        paths: &config::StoragePathLayout,
        tiered_storage: Option<&config::TieredStorageConfig>,
        budget: &Arc<crate::LocalDiskBudget>,
        memory_budget_bytes: usize,
        max_namespace_entries: usize,
    ) -> Result<Self> {
        let operations = Vec::new();
        let retained_memory_bytes =
            std::mem::size_of::<Vec<PostFlushRecoveryBlockerCleanupOperation>>();
        crate::disk_budget::admit_startup_memory(memory_budget_bytes, retained_memory_bytes)?;
        let mut builder = PostFlushRecoveryBlockerCleanupPlanBuilder {
            budget,
            memory_budget_bytes,
            namespace_budget: crate::engine::fs_utils::RecoveryNamespaceBudget::new(
                max_namespace_entries,
            ),
            operations,
            retained_memory_bytes,
            has_governed_entries: false,
        };

        let (marker_parent, transient_marker_parent_bytes) = builder.build_transient_child_path(
            data_path,
            &[super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME],
        )?;
        builder.collect_marker_temporaries(&marker_parent, transient_marker_parent_bytes)?;
        drop(marker_parent);

        // Preserve marker ambiguity/error precedence: only after that complete preflight may lane
        // resolution or its retained-memory admission fail.
        let lane_roots = post_flush_lane_roots(
            paths,
            tiered_storage,
            memory_budget_bytes,
            builder.retained_memory_bytes,
        )?;
        builder.retain_lane_roots(&lane_roots)?;

        // The shared counter charges every raw entry before applying an owned-name predicate and
        // every recursively planned descendant. When two lane roots share a physical parent, its
        // entries are conservatively charged again because each distinct predicate performs real
        // scan work; the bound limits observations, not merely unique pathnames.
        for lane_root in lane_roots {
            if let (Some(parent), Some(target_name)) = (
                lane_root.path.parent(),
                lane_root.path.file_name().and_then(|name| name.to_str()),
            ) {
                builder.collect_staging_directories(
                    parent,
                    |name| {
                        name.strip_prefix(".tmp-tsink-post-flush-retention-rewrite-")
                            .and_then(|value| value.strip_prefix(target_name))
                            .and_then(|value| value.strip_prefix('-'))
                            .is_some_and(|nonce| is_exact_lower_hex(nonce, 16))
                    },
                    &lane_root.boundary,
                    0,
                )?;
            }
            for level in ["L0", "L1", "L2"] {
                let (parent, transient_parent_bytes) =
                    builder.build_transient_child_path(&lane_root.path, &["segments", level])?;
                builder.collect_staging_directories(
                    &parent,
                    is_post_flush_copy_staging_name,
                    &lane_root.boundary,
                    transient_parent_bytes,
                )?;
            }
        }

        Ok(Self {
            operations: builder.operations,
            has_governed_entries: builder.has_governed_entries,
        })
    }

    fn execute(
        self,
        budget: &Arc<crate::LocalDiskBudget>,
        memory_budget_bytes: usize,
    ) -> Result<()> {
        if self.operations.is_empty() {
            return Ok(());
        }
        if !self.has_governed_entries {
            return execute_post_flush_recovery_blocker_operations(self.operations, budget);
        }

        // Keep one zero-byte recovery reservation live across every governed mutation. None of
        // the exact removal plans below calls a nested budget helper, so the reservation can be
        // settled before one bounded terminal reconciliation without waiting on itself.
        // The caller holds the coordinator's aggregate mutation lock across both planning and
        // this execution. Process leases exclude other engines, while that coordinator lock also
        // excludes a same-process adapter that retained the shared LocalDiskBudget handle. The
        // reservation additionally excludes a concurrent reconciliation scan.
        let reservation = budget.reserve(
            crate::DiskCategory::Temporary,
            0,
            crate::DiskReservationKind::Recovery,
        )?;
        let operation_result =
            execute_post_flush_recovery_blocker_operations(self.operations, budget);
        let settlement_result = reservation.commit(0, 0);
        let reconciliation_result = budget
            .reconcile_when_idle_with_memory_limit(memory_budget_bytes)
            .map(|_| ());
        combine_post_flush_recovery_blocker_results(
            operation_result,
            settlement_result,
            reconciliation_result,
        )
    }
}

struct PostFlushRecoveryBlockerCleanupPlanBuilder<'a> {
    budget: &'a Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
    namespace_budget: crate::engine::fs_utils::RecoveryNamespaceBudget,
    operations: Vec<PostFlushRecoveryBlockerCleanupOperation>,
    retained_memory_bytes: usize,
    has_governed_entries: bool,
}

impl PostFlushRecoveryBlockerCleanupPlanBuilder<'_> {
    fn collect_marker_temporaries(
        &mut self,
        parent: &Path,
        transient_parent_bytes: usize,
    ) -> Result<()> {
        self.collect_owned_entries(
            parent,
            |name| {
                crate::disk_budget::atomic_write_temp_target_name(name)
                    .is_some_and(super::super::maintenance::is_post_flush_replacement_marker_name)
            },
            PostFlushRecoveryBlockerCleanupKind::MarkerTemporary,
            &PostFlushRecoveryBlockerBoundary::Managed,
            transient_parent_bytes,
        )
    }

    fn collect_staging_directories(
        &mut self,
        parent: &Path,
        matches_owned_name: impl FnMut(&str) -> bool,
        boundary: &PostFlushRecoveryBlockerBoundary,
        transient_parent_bytes: usize,
    ) -> Result<()> {
        self.collect_owned_entries(
            parent,
            matches_owned_name,
            PostFlushRecoveryBlockerCleanupKind::StagingDirectory,
            boundary,
            transient_parent_bytes,
        )
    }

    fn collect_owned_entries(
        &mut self,
        parent: &Path,
        mut matches_owned_name: impl FnMut(&str) -> bool,
        kind: PostFlushRecoveryBlockerCleanupKind,
        boundary: &PostFlushRecoveryBlockerBoundary,
        transient_parent_bytes: usize,
    ) -> Result<()> {
        validate_post_flush_cleanup_parent(self.budget, parent, boundary)?;
        let metadata = match std::fs::symlink_metadata(parent) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: parent.to_path_buf(),
                    source,
                })
            }
        };
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(match kind {
                PostFlushRecoveryBlockerCleanupKind::MarkerTemporary => {
                    TsinkError::InvalidConfiguration(format!(
                        "managed directory must be a directory, found {:?}: {}",
                        metadata.file_type(),
                        parent.display()
                    ))
                }
                PostFlushRecoveryBlockerCleanupKind::StagingDirectory => {
                    TsinkError::DataCorruption(format!(
                        "post-flush staging parent is link-like or not a directory: {}",
                        parent.display()
                    ))
                }
            });
        }
        let parent_identity = capture_post_flush_cleanup_parent_identity(parent)?;

        let mut owned_paths = Vec::new();
        self.admit_scan_memory(parent, &owned_paths, None, transient_parent_bytes)?;
        for entry in std::fs::read_dir(parent).map_err(|source| TsinkError::IoWithPath {
            path: parent.to_path_buf(),
            source,
        })? {
            self.namespace_budget
                .observe_entry(parent, "post-flush recovery blocker cleanup")?;
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: parent.to_path_buf(),
                source,
            })?;
            let entry_name = entry.file_name();
            self.admit_scan_memory(
                parent,
                &owned_paths,
                Some(entry_name.as_encoded_bytes().len()),
                transient_parent_bytes,
            )?;
            let Some(name) = entry_name.to_str() else {
                continue;
            };
            if !matches_owned_name(name) {
                continue;
            }
            let path = parent.join(&entry_name);
            let metadata =
                std::fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                    path: path.clone(),
                    source,
                })?;
            let is_link_like = crate::engine::fs_utils::is_link_or_reparse_point(&metadata);
            let valid_type = match kind {
                PostFlushRecoveryBlockerCleanupKind::MarkerTemporary => {
                    is_supported_post_flush_marker_file_type(&metadata, is_link_like)
                }
                PostFlushRecoveryBlockerCleanupKind::StagingDirectory => {
                    metadata.file_type().is_dir() && !is_link_like
                }
            };
            if !valid_type {
                return Err(match kind {
                    PostFlushRecoveryBlockerCleanupKind::MarkerTemporary => {
                        TsinkError::InvalidConfiguration(format!(
                            "refusing to remove ambiguous owned temporary entry: {}",
                            path.display()
                        ))
                    }
                    PostFlushRecoveryBlockerCleanupKind::StagingDirectory => {
                        TsinkError::DataCorruption(format!(
                            "owned post-flush staging entry is link-like or not a directory: {}",
                            path.display()
                        ))
                    }
                });
            }
            self.push_owned_path(&mut owned_paths, path, transient_parent_bytes)?;
        }
        validate_post_flush_cleanup_parent(self.budget, parent, boundary)?;
        if !post_flush_cleanup_parent_identity_matches(parent, &parent_identity)? {
            return Err(TsinkError::DataCorruption(format!(
                "post-flush cleanup parent identity changed during preflight: {}",
                parent.display()
            )));
        }
        self.add_operation(
            parent,
            parent_identity,
            boundary,
            owned_paths,
            kind,
            transient_parent_bytes,
        )
    }

    fn admit_scan_memory(
        &self,
        parent: &Path,
        owned_paths: &Vec<PathBuf>,
        component_bytes: Option<usize>,
        transient_parent_bytes: usize,
    ) -> Result<()> {
        let mut required = self
            .retained_memory_bytes
            .checked_add(transient_parent_bytes)
            .and_then(|bytes| bytes.checked_add(modeled_startup_paths_bytes(owned_paths).ok()?))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::fs::ReadDir>()))
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })?;
        if let Some(component_bytes) = component_bytes {
            let anticipated_path_bytes = parent
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .checked_add(component_bytes)
                .and_then(|bytes| bytes.checked_add(1))
                .ok_or_else(|| {
                    TsinkError::Other("post-flush blocker cleanup path model overflow".to_string())
                })?;
            required = required
                .checked_add(std::mem::size_of::<std::fs::DirEntry>())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
                .and_then(|bytes| bytes.checked_add(component_bytes))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
                .and_then(|bytes| bytes.checked_add(anticipated_path_bytes))
                .ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush blocker cleanup memory model overflow".to_string(),
                    )
                })?;
        }
        crate::disk_budget::admit_startup_memory(self.memory_budget_bytes, required)
    }

    fn build_transient_child_path(
        &self,
        root: &Path,
        components: &[&str],
    ) -> Result<(PathBuf, usize)> {
        let anticipated_capacity = components.iter().try_fold(
            root.as_os_str().as_encoded_bytes().len(),
            |bytes, component| {
                bytes
                    .checked_add(component.len())
                    // Conservatively reserve one separator before every appended component.
                    .and_then(|bytes| bytes.checked_add(1))
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "post-flush cleanup-parent path size overflow".to_string(),
                        )
                    })
            },
        )?;
        self.admit_transient_parent_memory(
            std::mem::size_of::<PathBuf>()
                .checked_add(anticipated_capacity)
                .ok_or_else(|| {
                    TsinkError::Other("post-flush cleanup-parent memory model overflow".to_string())
                })?,
        )?;
        let mut parent = PathBuf::new();
        parent
            .try_reserve_exact(anticipated_capacity)
            .map_err(|_| {
                TsinkError::Other(
                    "unable to allocate bounded post-flush cleanup-parent path".to_string(),
                )
            })?;
        parent.push(root);
        for component in components {
            parent.push(component);
        }
        let transient_parent_bytes = std::mem::size_of::<PathBuf>()
            .checked_add(parent.capacity())
            .ok_or_else(|| {
                TsinkError::Other("post-flush cleanup-parent memory model overflow".to_string())
            })?;
        self.admit_transient_parent_memory(transient_parent_bytes)?;
        Ok((parent, transient_parent_bytes))
    }

    fn admit_transient_parent_memory(&self, transient_parent_bytes: usize) -> Result<()> {
        let required = self
            .retained_memory_bytes
            .checked_add(transient_parent_bytes)
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(self.memory_budget_bytes, required)
    }

    fn retain_lane_roots(&mut self, lane_roots: &Vec<PostFlushLaneRoot>) -> Result<()> {
        self.retained_memory_bytes = self
            .retained_memory_bytes
            .checked_add(modeled_post_flush_lane_roots_bytes(lane_roots)?)
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            self.retained_memory_bytes,
        )
    }

    fn reserve_operation_slot(&mut self) -> Result<()> {
        if self.operations.len() < self.operations.capacity() {
            return Ok(());
        }
        let entry_bytes = std::mem::size_of::<PostFlushRecoveryBlockerCleanupOperation>();
        self.retained_memory_bytes = self
            .retained_memory_bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            self.retained_memory_bytes,
        )?;
        let old_capacity = self.operations.capacity();
        self.operations.try_reserve_exact(1).map_err(|_| {
            TsinkError::Other(
                "unable to allocate bounded post-flush blocker cleanup operation".to_string(),
            )
        })?;
        let additional_capacity = self
            .operations
            .capacity()
            .checked_sub(old_capacity)
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker operation capacity underflow".to_string())
            })?;
        if additional_capacity > 1 {
            self.retained_memory_bytes = self
                .retained_memory_bytes
                .checked_add(
                    (additional_capacity - 1)
                        .checked_mul(entry_bytes)
                        .ok_or_else(|| {
                            TsinkError::Other(
                                "post-flush blocker operation capacity overflow".to_string(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush blocker cleanup memory model overflow".to_string(),
                    )
                })?;
            crate::disk_budget::admit_startup_memory(
                self.memory_budget_bytes,
                self.retained_memory_bytes,
            )?;
        }
        Ok(())
    }

    fn push_owned_path(
        &self,
        paths: &mut Vec<PathBuf>,
        path: PathBuf,
        transient_parent_bytes: usize,
    ) -> Result<()> {
        let path_bytes = path.capacity();
        let prospective_capacity = paths.len().checked_add(1).ok_or_else(|| {
            TsinkError::Other("post-flush blocker path-vector length overflow".to_string())
        })?;
        let prospective = self
            .retained_memory_bytes
            .checked_add(transient_parent_bytes)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<PathBuf>>()))
            .and_then(|bytes| {
                bytes.checked_add(prospective_capacity.checked_mul(std::mem::size_of::<PathBuf>())?)
            })
            .and_then(|bytes| {
                paths.iter().try_fold(bytes, |total, existing| {
                    total.checked_add(existing.capacity())
                })
            })
            .and_then(|bytes| bytes.checked_add(path_bytes))
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(self.memory_budget_bytes, prospective)?;
        if paths.len() == paths.capacity() {
            paths.try_reserve_exact(1).map_err(|_| {
                TsinkError::Other(
                    "unable to allocate bounded post-flush blocker path plan".to_string(),
                )
            })?;
        }
        paths.push(path);
        crate::disk_budget::admit_startup_memory(
            self.memory_budget_bytes,
            self.retained_memory_bytes
                .checked_add(transient_parent_bytes)
                .and_then(|bytes| bytes.checked_add(modeled_startup_paths_bytes(paths).ok()?))
                .ok_or_else(|| {
                    TsinkError::Other(
                        "post-flush blocker cleanup memory model overflow".to_string(),
                    )
                })?,
        )
    }

    fn add_operation(
        &mut self,
        parent: &Path,
        parent_identity: same_file::Handle,
        boundary: &PostFlushRecoveryBlockerBoundary,
        roots: Vec<PathBuf>,
        kind: PostFlushRecoveryBlockerCleanupKind,
        transient_parent_bytes: usize,
    ) -> Result<()> {
        if roots.is_empty() {
            return Ok(());
        }
        self.has_governed_entries |= matches!(boundary, PostFlushRecoveryBlockerBoundary::Managed);
        let base_retained_bytes = self
            .operation_base_retained_bytes(parent, boundary, &roots)?
            .checked_add(transient_parent_bytes)
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })?;
        let mut retained_memory_bytes = base_retained_bytes;
        let memory_budget_bytes = self.memory_budget_bytes;
        let mut admit = |required| {
            retained_memory_bytes = retained_memory_bytes.max(required);
            crate::disk_budget::admit_startup_memory(memory_budget_bytes, required)
        };
        let removal_plan = match kind {
            PostFlushRecoveryBlockerCleanupKind::MarkerTemporary => {
                let mut unused_namespace_budget =
                    crate::engine::fs_utils::RecoveryNamespaceBudget::new(0);
                crate::engine::fs_utils::validate_recursive_namespace_with_admission(
                    &[],
                    &mut unused_namespace_budget,
                    crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
                    "post-flush recovery blocker cleanup",
                    base_retained_bytes,
                    &mut admit,
                )?
                .include_file_like_roots_with_admission(
                    &roots,
                    base_retained_bytes,
                    &mut admit,
                )?
            }
            PostFlushRecoveryBlockerCleanupKind::StagingDirectory => {
                crate::engine::fs_utils::validate_recursive_namespace_with_admission(
                    &roots,
                    &mut self.namespace_budget,
                    crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
                    "post-flush recovery blocker cleanup",
                    base_retained_bytes,
                    &mut admit,
                )?
            }
        };
        self.retained_memory_bytes = retained_memory_bytes;
        self.reserve_operation_slot()?;
        self.operations
            .push(PostFlushRecoveryBlockerCleanupOperation {
                parent: parent.to_path_buf(),
                parent_identity,
                boundary: boundary.clone(),
                roots,
                removal_plan,
                kind,
            });
        Ok(())
    }

    fn operation_base_retained_bytes(
        &self,
        parent: &Path,
        boundary: &PostFlushRecoveryBlockerBoundary,
        roots: &Vec<PathBuf>,
    ) -> Result<usize> {
        self.retained_memory_bytes
            .checked_add(parent.as_os_str().as_encoded_bytes().len())
            .and_then(|bytes| bytes.checked_add(modeled_post_flush_boundary_bytes(boundary)))
            .and_then(|bytes| bytes.checked_add(modeled_startup_paths_bytes(roots).ok()?))
            .ok_or_else(|| {
                TsinkError::Other("post-flush blocker cleanup memory model overflow".to_string())
            })
    }
}

fn execute_post_flush_recovery_blocker_operations(
    operations: Vec<PostFlushRecoveryBlockerCleanupOperation>,
    budget: &Arc<crate::LocalDiskBudget>,
) -> Result<()> {
    for operation in operations {
        // Recheck every planned root before touching descendants. In particular, a staging root
        // swapped to a link must be rejected before a descendant pathname could traverse it.
        let parent_validation =
            validate_post_flush_cleanup_parent(budget, &operation.parent, &operation.boundary)
                .and_then(|()| {
                    if post_flush_cleanup_parent_identity_matches(
                        &operation.parent,
                        &operation.parent_identity,
                    )? {
                        Ok(())
                    } else {
                        Err(TsinkError::DataCorruption(format!(
                            "post-flush cleanup parent identity changed before removal: {}",
                            operation.parent.display()
                        )))
                    }
                })
                .and_then(|()| {
                    validate_post_flush_recovery_blocker_roots(&operation.roots, operation.kind)
                });
        let (removal_result, sync_result) = match parent_validation {
            Err(err) => (Err(err), Ok(())),
            Ok(()) => {
                let removal_result = operation.removal_plan.remove().map(|_| ());
                // Synchronize even after a partial removal failure: every deletion already
                // committed by the exact plan must remain crash-durable before the error
                // reaches startup.
                let sync_result = crate::engine::fs_utils::sync_dir(&operation.parent);
                (removal_result, sync_result)
            }
        };
        if removal_result.is_ok() && sync_result.is_ok() {
            continue;
        }
        return Err(match operation.kind {
            PostFlushRecoveryBlockerCleanupKind::MarkerTemporary => {
                let error = removal_result
                    .err()
                    .or_else(|| sync_result.err())
                    .expect("failed marker cleanup must have a removal or synchronization error");
                TsinkError::Other(format!(
                    "owned atomic-write temporary cleanup for {} failed: cleanup failed: {error}",
                    operation.parent.display()
                ))
            }
            PostFlushRecoveryBlockerCleanupKind::StagingDirectory => {
                let mut errors = Vec::new();
                if let Err(err) = removal_result {
                    errors.push(format!("removal failed: {err}"));
                }
                if let Err(err) = sync_result {
                    errors.push(format!("parent synchronization failed: {err}"));
                }
                TsinkError::Other(format!(
                    "post-flush staging cleanup for {} failed: {}",
                    operation.parent.display(),
                    errors.join("; ")
                ))
            }
        });
    }
    Ok(())
}

fn validate_post_flush_recovery_blocker_roots(
    roots: &[PathBuf],
    kind: PostFlushRecoveryBlockerCleanupKind,
) -> Result<()> {
    for path in roots {
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: path.clone(),
                    source,
                })
            }
        };
        let is_link_like = crate::engine::fs_utils::is_link_or_reparse_point(&metadata);
        match kind {
            PostFlushRecoveryBlockerCleanupKind::MarkerTemporary
                if !is_supported_post_flush_marker_file_type(&metadata, is_link_like) =>
            {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "refusing to remove ambiguous owned temporary entry: {}",
                    path.display()
                )))
            }
            PostFlushRecoveryBlockerCleanupKind::StagingDirectory
                if is_link_like || !metadata.file_type().is_dir() =>
            {
                return Err(TsinkError::DataCorruption(format!(
                    "owned post-flush staging entry is link-like or not a directory: {}",
                    path.display()
                )))
            }
            _ => {}
        }
    }
    Ok(())
}

fn is_supported_post_flush_marker_file_type(
    metadata: &std::fs::Metadata,
    is_link_like: bool,
) -> bool {
    #[cfg(windows)]
    if is_link_like {
        // `remove_file` is not a portable no-follow unlink for directory reparse points. Reject
        // every link-like Windows marker entry rather than risk following or misclassifying it.
        return false;
    }
    metadata.file_type().is_file() || is_link_like
}

fn validate_post_flush_cleanup_parent(
    budget: &Arc<crate::LocalDiskBudget>,
    parent: &Path,
    boundary: &PostFlushRecoveryBlockerBoundary,
) -> Result<()> {
    match boundary {
        PostFlushRecoveryBlockerBoundary::Managed => budget.validate_managed_directory_path(parent),
        PostFlushRecoveryBlockerBoundary::External(root) => {
            crate::engine::fs_utils::validate_boundary_directory(
                root,
                "configured post-flush object-store root",
                false,
            )?;
            crate::engine::fs_utils::validate_owned_entry_below_alias_boundary(
                root,
                parent,
                crate::engine::fs_utils::OwnedBoundaryEntryKind::Directory,
                "owned post-flush staging",
            )
        }
    }
}

fn capture_post_flush_cleanup_parent_identity(parent: &Path) -> Result<same_file::Handle> {
    let identity =
        same_file::Handle::from_path(parent).map_err(|source| TsinkError::IoWithPath {
            path: parent.to_path_buf(),
            source,
        })?;
    if !post_flush_cleanup_parent_identity_matches(parent, &identity)? {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush cleanup parent identity changed while it was captured: {}",
            parent.display()
        )));
    }
    Ok(identity)
}

fn post_flush_cleanup_parent_identity_matches(
    parent: &Path,
    expected: &same_file::Handle,
) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(parent) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: parent.to_path_buf(),
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Ok(false);
    }
    let current =
        same_file::Handle::from_path(parent).map_err(|source| TsinkError::IoWithPath {
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(&current == expected)
}

fn modeled_post_flush_boundary_bytes(boundary: &PostFlushRecoveryBlockerBoundary) -> usize {
    match boundary {
        PostFlushRecoveryBlockerBoundary::Managed => 0,
        PostFlushRecoveryBlockerBoundary::External(root) => root.capacity(),
    }
}

fn combine_post_flush_recovery_blocker_results(
    operation_result: Result<()>,
    settlement_result: Result<()>,
    reconciliation_result: Result<()>,
) -> Result<()> {
    let failure_count = usize::from(operation_result.is_err())
        + usize::from(settlement_result.is_err())
        + usize::from(reconciliation_result.is_err());
    if failure_count == 0 {
        return Ok(());
    }
    if failure_count == 1 {
        return match (operation_result, settlement_result, reconciliation_result) {
            (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
            _ => unreachable!("one blocker-cleanup failure must have one failed result"),
        };
    }

    let mut errors = Vec::new();
    if let Err(err) = &operation_result {
        errors.push(format!("cleanup failed: {err}"));
    }
    if let Err(err) = &settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = &reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    Err(TsinkError::Other(format!(
        "post-flush recovery blocker cleanup failed: {}",
        errors.join("; ")
    )))
}

#[cfg(test)]
pub(super) fn combine_post_flush_recovery_blocker_reconciliation_for_test(
    reconciliation_result: Result<()>,
) -> Result<()> {
    combine_post_flush_recovery_blocker_results(Ok(()), Ok(()), reconciliation_result)
}

#[cfg(test)]
pub(super) fn preflight_post_flush_recovery_blockers_with_limits_for_test(
    builder: &StorageBuilder,
    budget: &Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
    max_namespace_entries: usize,
) -> Result<()> {
    let data_path = builder.data_path().ok_or_else(|| {
        TsinkError::InvalidConfiguration(
            "post-flush blocker test preflight requires a data path".to_string(),
        )
    })?;
    let paths = config::StoragePathLayout::from(builder);
    let storage_options = ChunkStorageOptions::from(builder);
    PostFlushRecoveryBlockerCleanupPlan::build_with_namespace_limit(
        data_path,
        &paths,
        storage_options.tiered_storage.as_ref(),
        budget,
        memory_budget_bytes,
        max_namespace_entries,
    )
    .map(drop)
}

#[cfg(test)]
pub(super) fn cleanup_post_flush_recovery_blockers_with_limits_for_test(
    builder: &StorageBuilder,
    budget: &Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
    max_namespace_entries: usize,
) -> Result<()> {
    cleanup_post_flush_recovery_blockers_with_before_execute_for_test(
        builder,
        budget,
        memory_budget_bytes,
        max_namespace_entries,
        || {},
    )
}

#[cfg(test)]
pub(super) fn cleanup_post_flush_recovery_blockers_with_before_execute_for_test(
    builder: &StorageBuilder,
    budget: &Arc<crate::LocalDiskBudget>,
    memory_budget_bytes: usize,
    max_namespace_entries: usize,
    before_execute: impl FnOnce(),
) -> Result<()> {
    let data_path = builder.data_path().ok_or_else(|| {
        TsinkError::InvalidConfiguration(
            "post-flush blocker test cleanup requires a data path".to_string(),
        )
    })?;
    let paths = config::StoragePathLayout::from(builder);
    let storage_options = ChunkStorageOptions::from(builder);
    budget.with_serialized_managed_file_mutation(|| {
        let plan = PostFlushRecoveryBlockerCleanupPlan::build_with_namespace_limit(
            data_path,
            &paths,
            storage_options.tiered_storage.as_ref(),
            budget,
            memory_budget_bytes,
            max_namespace_entries,
        )?;
        before_execute();
        plan.execute(budget, memory_budget_bytes)
    })
}

fn configured_tombstone_transaction_lanes(
    builder: &StorageBuilder,
    storage_options: &ChunkStorageOptions,
) -> Vec<crate::engine::tombstone::TombstoneLane> {
    let mut lanes = Vec::new();
    if let Some(data_path) = builder.data_path() {
        lanes.push(crate::engine::tombstone::TombstoneLane {
            role: crate::engine::tombstone::TombstoneLaneRole::LocalNumeric,
            namespace_root: data_path.to_path_buf(),
            manifest_path: data_path
                .join(NUMERIC_LANE_ROOT)
                .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
        });
        lanes.push(crate::engine::tombstone::TombstoneLane {
            role: crate::engine::tombstone::TombstoneLaneRole::LocalBlob,
            namespace_root: data_path.to_path_buf(),
            manifest_path: data_path
                .join(BLOB_LANE_ROOT)
                .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
        });
    }
    if let Some(config) = storage_options.tiered_storage.as_ref() {
        for (tier, numeric_role, blob_role) in [
            (
                tiering::PersistedSegmentTier::Hot,
                crate::engine::tombstone::TombstoneLaneRole::HotNumeric,
                crate::engine::tombstone::TombstoneLaneRole::HotBlob,
            ),
            (
                tiering::PersistedSegmentTier::Warm,
                crate::engine::tombstone::TombstoneLaneRole::WarmNumeric,
                crate::engine::tombstone::TombstoneLaneRole::WarmBlob,
            ),
            (
                tiering::PersistedSegmentTier::Cold,
                crate::engine::tombstone::TombstoneLaneRole::ColdNumeric,
                crate::engine::tombstone::TombstoneLaneRole::ColdBlob,
            ),
        ] {
            lanes.push(crate::engine::tombstone::TombstoneLane {
                role: numeric_role,
                namespace_root: config.object_store_root.clone(),
                manifest_path: config
                    .lane_path(tiering::SegmentLaneFamily::Numeric, tier)
                    .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
            });
            lanes.push(crate::engine::tombstone::TombstoneLane {
                role: blob_role,
                namespace_root: config.object_store_root.clone(),
                manifest_path: config
                    .lane_path(tiering::SegmentLaneFamily::Blob, tier)
                    .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
            });
        }
    }
    lanes
}

fn post_flush_lane_roots(
    paths: &config::StoragePathLayout,
    tiered_storage: Option<&config::TieredStorageConfig>,
    memory_budget_bytes: usize,
    base_retained_bytes: usize,
) -> Result<Vec<PostFlushLaneRoot>> {
    let resolver = tiering::SegmentPathResolver::new(
        paths.numeric_lane_path.as_deref(),
        paths.blob_lane_path.as_deref(),
        tiered_storage,
    );
    let mut lane_roots = Vec::new();
    for lane in [
        tiering::SegmentLaneFamily::Numeric,
        tiering::SegmentLaneFamily::Blob,
    ] {
        for tier in [
            tiering::PersistedSegmentTier::Hot,
            tiering::PersistedSegmentTier::Warm,
            tiering::PersistedSegmentTier::Cold,
        ] {
            if matches!(
                tier,
                tiering::PersistedSegmentTier::Warm | tiering::PersistedSegmentTier::Cold
            ) {
                if let Some(config) = tiered_storage {
                    admit_external_post_flush_lane_resolution(
                        &config.object_store_root,
                        lane,
                        tier,
                        &lane_roots,
                        memory_budget_bytes,
                        base_retained_bytes,
                    )?;
                }
            }
            if let Ok(root) = resolver.lane_root(lane, tier) {
                let candidate = match tier {
                    tiering::PersistedSegmentTier::Hot => PostFlushLaneRoot {
                        path: root,
                        boundary: PostFlushRecoveryBlockerBoundary::Managed,
                    },
                    tiering::PersistedSegmentTier::Warm | tiering::PersistedSegmentTier::Cold => {
                        let config = tiered_storage.ok_or_else(|| {
                            TsinkError::InvalidConfiguration(
                                "tiered post-flush lane has no object-store boundary".to_string(),
                            )
                        })?;
                        let (boundary, path) = resolve_external_post_flush_lane_root(
                            &config.object_store_root,
                            &root,
                        )?;
                        PostFlushLaneRoot {
                            path,
                            boundary: PostFlushRecoveryBlockerBoundary::External(boundary),
                        }
                    }
                };
                if !lane_roots.contains(&candidate) {
                    push_post_flush_lane_root(
                        &mut lane_roots,
                        candidate,
                        memory_budget_bytes,
                        base_retained_bytes,
                    )?;
                }
            }
        }
    }
    Ok(lane_roots)
}

fn resolve_external_post_flush_lane_root(
    configured_boundary: &Path,
    configured_lane_root: &Path,
) -> Result<(PathBuf, PathBuf)> {
    let lexical_boundary =
        crate::engine::fs_utils::absolute_path_lexically_normalized(configured_boundary)?;
    validate_post_flush_external_path_size(&lexical_boundary)?;
    let lexical_lane_root =
        crate::engine::fs_utils::absolute_path_lexically_normalized(configured_lane_root)?;
    validate_post_flush_external_path_size(&lexical_lane_root)?;
    let relative = lexical_lane_root
        .strip_prefix(&lexical_boundary)
        .map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "post-flush lane root {} escapes configured object-store boundary {}",
                configured_lane_root.display(),
                configured_boundary.display()
            ))
        })?;
    let trusted_boundary =
        crate::engine::fs_utils::resolve_trusted_namespace_root(&lexical_boundary)?;
    validate_post_flush_external_path_size(&trusted_boundary)?;
    let trusted_lane_root = trusted_boundary.join(relative);
    validate_post_flush_external_path_size(&trusted_lane_root)?;
    crate::engine::fs_utils::validate_boundary_directory(
        &trusted_boundary,
        "configured post-flush object-store root",
        false,
    )?;
    crate::engine::fs_utils::validate_owned_entry_below_alias_boundary(
        &trusted_boundary,
        &trusted_lane_root,
        crate::engine::fs_utils::OwnedBoundaryEntryKind::Directory,
        "owned post-flush staging",
    )?;
    Ok((trusted_boundary, trusted_lane_root))
}

fn admit_external_post_flush_lane_resolution(
    configured_boundary: &Path,
    lane: tiering::SegmentLaneFamily,
    tier: tiering::PersistedSegmentTier,
    lane_roots: &Vec<PostFlushLaneRoot>,
    memory_budget_bytes: usize,
    base_retained_bytes: usize,
) -> Result<()> {
    validate_post_flush_external_path_size(configured_boundary)?;
    let tier_name = match tier {
        tiering::PersistedSegmentTier::Hot => "hot",
        tiering::PersistedSegmentTier::Warm => "warm",
        tiering::PersistedSegmentTier::Cold => "cold",
    };
    let anticipated_lane_bytes = configured_boundary
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(tier_name.len()))
        .and_then(|bytes| bytes.checked_add(1))
        .and_then(|bytes| bytes.checked_add(lane.root_name().len()))
        .ok_or_else(|| {
            TsinkError::Other("post-flush external lane path size overflow".to_string())
        })?;
    if anticipated_lane_bytes > POST_FLUSH_EXTERNAL_PATH_MAX_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "post-flush external lane path requires {anticipated_lane_bytes} bytes, exceeding the {}-byte startup path limit",
            POST_FLUSH_EXTERNAL_PATH_MAX_BYTES
        )));
    }

    let per_path_bytes = std::mem::size_of::<PathBuf>()
        .checked_add(POST_FLUSH_EXTERNAL_PATH_MAX_BYTES)
        .ok_or_else(|| {
            TsinkError::Other("post-flush external path memory model overflow".to_string())
        })?;
    let resolution_bytes = POST_FLUSH_EXTERNAL_RESOLUTION_PATH_SLOTS
        .checked_mul(per_path_bytes)
        .ok_or_else(|| {
            TsinkError::Other("post-flush external path memory model overflow".to_string())
        })?;
    let required = base_retained_bytes
        .checked_add(modeled_post_flush_lane_roots_bytes(lane_roots)?)
        .and_then(|bytes| bytes.checked_add(resolution_bytes))
        .ok_or_else(|| {
            TsinkError::Other("post-flush external path memory model overflow".to_string())
        })?;
    crate::disk_budget::admit_startup_memory(memory_budget_bytes, required)
}

fn validate_post_flush_external_path_size(path: &Path) -> Result<()> {
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    if path_bytes > POST_FLUSH_EXTERNAL_PATH_MAX_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "post-flush external path requires {path_bytes} bytes, exceeding the {}-byte startup path limit: {}",
            POST_FLUSH_EXTERNAL_PATH_MAX_BYTES,
            path.display()
        )));
    }
    Ok(())
}

fn modeled_post_flush_lane_roots_bytes(lane_roots: &Vec<PostFlushLaneRoot>) -> Result<usize> {
    lane_roots.iter().try_fold(
        std::mem::size_of::<Vec<PostFlushLaneRoot>>()
            .checked_add(
                lane_roots
                    .capacity()
                    .checked_mul(std::mem::size_of::<PostFlushLaneRoot>())
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "post-flush lane-root vector capacity overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                TsinkError::Other("post-flush lane-root memory model overflow".to_string())
            })?,
        |total, lane_root| {
            total
                .checked_add(lane_root.path.capacity())
                .and_then(|bytes| {
                    bytes.checked_add(modeled_post_flush_boundary_bytes(&lane_root.boundary))
                })
                .ok_or_else(|| {
                    TsinkError::Other("post-flush lane-root memory model overflow".to_string())
                })
        },
    )
}

fn push_post_flush_lane_root(
    lane_roots: &mut Vec<PostFlushLaneRoot>,
    lane_root: PostFlushLaneRoot,
    memory_budget_bytes: usize,
    base_retained_bytes: usize,
) -> Result<()> {
    let prospective_capacity = if lane_roots.len() == lane_roots.capacity() {
        lane_roots
            .len()
            .checked_add(1)
            .ok_or_else(|| TsinkError::Other("post-flush lane-root count overflow".to_string()))?
    } else {
        lane_roots.capacity()
    };
    let prospective_paths = lane_roots.iter().try_fold(
        lane_root
            .path
            .capacity()
            .checked_add(modeled_post_flush_boundary_bytes(&lane_root.boundary))
            .ok_or_else(|| {
                TsinkError::Other("post-flush lane-root memory model overflow".to_string())
            })?,
        |total, retained| {
            total
                .checked_add(retained.path.capacity())
                .and_then(|bytes| {
                    bytes.checked_add(modeled_post_flush_boundary_bytes(&retained.boundary))
                })
                .ok_or_else(|| {
                    TsinkError::Other("post-flush lane-root memory model overflow".to_string())
                })
        },
    )?;
    let prospective = base_retained_bytes
        .checked_add(std::mem::size_of::<Vec<PostFlushLaneRoot>>())
        .and_then(|bytes| {
            bytes.checked_add(
                prospective_capacity.checked_mul(std::mem::size_of::<PostFlushLaneRoot>())?,
            )
        })
        .and_then(|bytes| bytes.checked_add(prospective_paths))
        .ok_or_else(|| {
            TsinkError::Other("post-flush lane-root memory model overflow".to_string())
        })?;
    crate::disk_budget::admit_startup_memory(memory_budget_bytes, prospective)?;
    if lane_roots.len() == lane_roots.capacity() {
        lane_roots.try_reserve_exact(1).map_err(|_| {
            TsinkError::Other("unable to allocate bounded post-flush lane-root plan".to_string())
        })?;
    }
    lane_roots.push(lane_root);
    crate::disk_budget::admit_startup_memory(
        memory_budget_bytes,
        base_retained_bytes
            .checked_add(modeled_post_flush_lane_roots_bytes(lane_roots)?)
            .ok_or_else(|| {
                TsinkError::Other("post-flush lane-root memory model overflow".to_string())
            })?,
    )
}

#[cfg(test)]
pub(super) fn preflight_exact_post_flush_stage_dirs_global(
    parent: &Path,
    mut matches_owned_name: impl FnMut(&str) -> bool,
    namespace_budget: &mut crate::engine::fs_utils::RecoveryNamespaceBudget,
    max_recursive_depth: u32,
    memory_budget_bytes: usize,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(parent) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: parent.to_path_buf(),
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush staging parent is link-like or not a directory: {}",
            parent.display()
        )));
    }
    let mut owned_paths = Vec::new();
    crate::disk_budget::admit_startup_memory(
        memory_budget_bytes,
        modeled_startup_paths_bytes(&owned_paths)?
            .checked_add(std::mem::size_of::<std::fs::ReadDir>())
            .ok_or_else(|| {
                TsinkError::Other("post-flush staging cleanup memory model overflow".to_string())
            })?,
    )?;
    let entries = std::fs::read_dir(parent).map_err(|source| TsinkError::IoWithPath {
        path: parent.to_path_buf(),
        source,
    })?;
    for entry in entries {
        namespace_budget.observe_entry(parent, "post-flush staging cleanup")?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: parent.to_path_buf(),
            source,
        })?;
        let entry_name = entry.file_name();
        let component_bytes = entry_name.as_encoded_bytes().len();
        let path_bytes = parent
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(component_bytes)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| {
                TsinkError::Other("post-flush staging cleanup path model overflow".to_string())
            })?;
        let transient = modeled_startup_paths_bytes(&owned_paths)?
            .checked_add(std::mem::size_of::<std::fs::ReadDir>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::fs::DirEntry>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
            .and_then(|bytes| bytes.checked_add(component_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
            .and_then(|bytes| bytes.checked_add(path_bytes))
            .ok_or_else(|| {
                TsinkError::Other("post-flush staging cleanup memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(memory_budget_bytes, transient)?;
        let Some(name) = entry_name.to_str() else {
            continue;
        };
        if !matches_owned_name(name) {
            continue;
        }
        let path = parent.join(&entry_name);
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(TsinkError::DataCorruption(format!(
                "owned post-flush staging entry is link-like or not a directory: {}",
                path.display()
            )));
        }
        push_startup_cleanup_path(
            &mut owned_paths,
            path,
            memory_budget_bytes,
            "post-flush staging cleanup",
        )?;
    }
    if owned_paths.is_empty() {
        return Ok(());
    }
    let retained_root_bytes = modeled_startup_paths_bytes(&owned_paths)?;
    crate::engine::fs_utils::validate_recursive_namespace_with_admission(
        &owned_paths,
        namespace_budget,
        max_recursive_depth,
        "post-flush staging cleanup",
        retained_root_bytes,
        |required| crate::disk_budget::admit_startup_memory(memory_budget_bytes, required),
    )?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn cleanup_exact_post_flush_stage_dirs_with_namespace_limits(
    parent: &Path,
    mut matches_owned_name: impl FnMut(&str) -> bool,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    max_directory_entries: usize,
    max_recursive_entries: usize,
    max_recursive_depth: u32,
    memory_budget_bytes: usize,
    execute: bool,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(parent) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: parent.to_path_buf(),
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "post-flush staging parent is link-like or not a directory: {}",
            parent.display()
        )));
    }
    let mut directory_budget =
        crate::engine::fs_utils::RecoveryNamespaceBudget::new(max_directory_entries);
    let mut owned_paths = Vec::new();
    crate::disk_budget::admit_startup_memory(
        memory_budget_bytes,
        modeled_startup_paths_bytes(&owned_paths)?
            .checked_add(std::mem::size_of::<std::fs::ReadDir>())
            .ok_or_else(|| {
                TsinkError::Other("post-flush staging cleanup memory model overflow".to_string())
            })?,
    )?;
    let entries = std::fs::read_dir(parent).map_err(|source| TsinkError::IoWithPath {
        path: parent.to_path_buf(),
        source,
    })?;
    for entry in entries {
        directory_budget.observe_entry(parent, "post-flush staging cleanup")?;
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: parent.to_path_buf(),
            source,
        })?;
        let entry_name = entry.file_name();
        let component_bytes = entry_name.as_encoded_bytes().len();
        let path_bytes = parent
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(component_bytes)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| {
                TsinkError::Other("post-flush staging cleanup path model overflow".to_string())
            })?;
        let transient = modeled_startup_paths_bytes(&owned_paths)?
            .checked_add(std::mem::size_of::<std::fs::ReadDir>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::fs::DirEntry>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::ffi::OsString>()))
            .and_then(|bytes| bytes.checked_add(component_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PathBuf>()))
            .and_then(|bytes| bytes.checked_add(path_bytes))
            .ok_or_else(|| {
                TsinkError::Other("post-flush staging cleanup memory model overflow".to_string())
            })?;
        crate::disk_budget::admit_startup_memory(memory_budget_bytes, transient)?;
        let Some(name) = entry_name.to_str() else {
            continue;
        };
        if !matches_owned_name(name) {
            continue;
        }
        let path = parent.join(&entry_name);
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_dir()
        {
            return Err(TsinkError::DataCorruption(format!(
                "owned post-flush staging entry is link-like or not a directory: {}",
                path.display()
            )));
        }
        push_startup_cleanup_path(
            &mut owned_paths,
            path,
            memory_budget_bytes,
            "post-flush staging cleanup",
        )?;
    }
    if owned_paths.is_empty() {
        return Ok(());
    }
    let retained_root_bytes = modeled_startup_paths_bytes(&owned_paths)?;
    let mut recursive_budget =
        crate::engine::fs_utils::RecoveryNamespaceBudget::new(max_recursive_entries);
    let removal_plan = crate::engine::fs_utils::validate_recursive_namespace_with_admission(
        &owned_paths,
        &mut recursive_budget,
        max_recursive_depth,
        "post-flush staging cleanup",
        retained_root_bytes,
        |required| crate::disk_budget::admit_startup_memory(memory_budget_bytes, required),
    )?;

    if !execute {
        return Ok(());
    }

    let governed = if let Some(budget) = local_disk_budget {
        let mut governed = false;
        for path in &owned_paths {
            governed |= budget.governs_entry(path)?;
        }
        governed
    } else {
        false
    };
    let reservation = if governed {
        Some(
            local_disk_budget
                .expect("governed cleanup must have a disk budget")
                .reserve(
                    crate::DiskCategory::Temporary,
                    0,
                    crate::DiskReservationKind::Recovery,
                )?,
        )
    } else {
        None
    };
    let removal_result = removal_plan.remove().map(|_| ());
    let sync_result = crate::engine::fs_utils::sync_dir(parent);
    let settlement_result = reservation.map_or(Ok(()), |reservation| reservation.commit(0, 0));
    let reconciliation_result = if governed {
        local_disk_budget
            .expect("governed cleanup must have a disk budget")
            .reconcile_when_idle_with_memory_limit(memory_budget_bytes)
            .map(|_| ())
    } else {
        Ok(())
    };
    let mut errors = Vec::new();
    if let Err(err) = removal_result {
        errors.push(format!("removal failed: {err}"));
    }
    if let Err(err) = sync_result {
        errors.push(format!("parent synchronization failed: {err}"));
    }
    if let Err(err) = settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    if !errors.is_empty() {
        return Err(TsinkError::Other(format!(
            "post-flush staging cleanup for {} failed: {}",
            parent.display(),
            errors.join("; ")
        )));
    }
    Ok(())
}

fn modeled_startup_paths_bytes(paths: &Vec<PathBuf>) -> Result<usize> {
    paths.iter().try_fold(
        std::mem::size_of::<Vec<PathBuf>>()
            .checked_add(
                paths
                    .capacity()
                    .checked_mul(std::mem::size_of::<PathBuf>())
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "startup cleanup path-vector capacity overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                TsinkError::Other("startup cleanup memory model overflow".to_string())
            })?,
        |total, path| {
            total.checked_add(path.capacity()).ok_or_else(|| {
                TsinkError::Other("startup cleanup retained-path overflow".to_string())
            })
        },
    )
}

#[cfg(test)]
fn push_startup_cleanup_path(
    paths: &mut Vec<PathBuf>,
    path: PathBuf,
    memory_budget_bytes: usize,
    operation: &str,
) -> Result<()> {
    let prospective_capacity = if paths.len() == paths.capacity() {
        paths
            .len()
            .checked_add(1)
            .ok_or_else(|| TsinkError::Other(format!("{operation} path count overflow")))?
    } else {
        paths.capacity()
    };
    let path_bytes = paths.iter().try_fold(path.capacity(), |total, retained| {
        total
            .checked_add(retained.capacity())
            .ok_or_else(|| TsinkError::Other(format!("{operation} retained-path size overflow")))
    })?;
    let prospective = std::mem::size_of::<Vec<PathBuf>>()
        .checked_add(
            prospective_capacity
                .checked_mul(std::mem::size_of::<PathBuf>())
                .ok_or_else(|| TsinkError::Other(format!("{operation} capacity overflow")))?,
        )
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .ok_or_else(|| TsinkError::Other(format!("{operation} memory model overflow")))?;
    crate::disk_budget::admit_startup_memory(memory_budget_bytes, prospective)?;
    if paths.len() == paths.capacity() {
        paths.try_reserve_exact(1).map_err(|_| {
            TsinkError::Other(format!(
                "unable to allocate bounded path plan for {operation}"
            ))
        })?;
    }
    paths.push(path);
    crate::disk_budget::admit_startup_memory(
        memory_budget_bytes,
        modeled_startup_paths_bytes(paths)?,
    )
}

fn is_post_flush_copy_staging_name(name: &str) -> bool {
    let Some(value) = name.strip_prefix(".tmp-tsink-post-flush-stage-copy-seg-") else {
        return false;
    };
    let Some((segment_id, nonce)) = value.split_once('-') else {
        return false;
    };
    is_exact_lower_hex(segment_id, 16) && is_exact_lower_hex(nonce, 16)
}

fn is_exact_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_registry_incremental_file_name(name: &str) -> bool {
    if name == "journal-active.bin" {
        return true;
    }
    ["delta-", "journal-"].into_iter().any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(".bin"))
            .is_some_and(|nonce| is_exact_lower_hex(nonce, 16))
    })
}

fn is_tombstone_shard_name(name: &str) -> bool {
    let Some(value) = name
        .strip_prefix("shard-")
        .and_then(|value| value.strip_suffix(".bin"))
    else {
        return false;
    };
    let Some((shard, nonce)) = value.split_once('-') else {
        return false;
    };
    shard.len() == 3
        && shard.bytes().all(|byte| byte.is_ascii_digit())
        && shard.parse::<u16>().is_ok_and(|index| index <= 255)
        && is_exact_lower_hex(nonce, 16)
}

fn is_compaction_replacement_marker_name(name: &str) -> bool {
    let Some(value) = name
        .strip_prefix("replace-")
        .and_then(|value| value.strip_suffix(".json"))
    else {
        return false;
    };
    let Some((timestamp, nonce)) = value.split_once('-') else {
        return false;
    };
    is_exact_lower_hex(timestamp, 16) && is_exact_lower_hex(nonce, 16)
}

fn is_segment_staging_name(name: &str) -> bool {
    name.strip_prefix(".tmp-seg-")
        .is_some_and(|segment_id| is_exact_lower_hex(segment_id, 16))
}

fn resolve_local_disk_budget(
    builder: &StorageBuilder,
    startup_memory_budget: usize,
) -> Result<Option<Arc<crate::LocalDiskBudget>>> {
    let limits = builder.local_disk_limits().validate()?;
    let disk_settings_requested = limits.max_bytes.is_some()
        || limits.filesystem_free_headroom_bytes > 0
        || limits.maintenance_temp_reserve_bytes > 0
        || builder.shared_local_disk_budget().is_some();

    let persistent_data_path = (builder.runtime_mode() == StorageRuntimeMode::ReadWrite)
        .then(|| builder.data_path())
        .flatten();
    let Some(data_path) = persistent_data_path else {
        if disk_settings_requested && builder.has_explicit_local_disk_settings() {
            return Err(TsinkError::InvalidConfiguration(
                "local disk limits require persistent read-write storage with a data path"
                    .to_string(),
            ));
        }
        return Ok(None);
    };

    if let Some(shared) = builder.shared_local_disk_budget() {
        if !shared.matches_root(data_path)? {
            return Err(TsinkError::InvalidConfiguration(format!(
                "shared local disk budget root {} does not match data path {}",
                shared.root().display(),
                data_path.display()
            )));
        }
        if shared.limits() != limits {
            return Err(TsinkError::InvalidConfiguration(
                "shared local disk budget limits do not match builder disk settings".to_string(),
            ));
        }
        // An injected coordinator may have been opened or last reconciled under an unlimited
        // modeled-memory ceiling. Prove this startup's finite ceiling before any recovery cleanup
        // can delete bytes. This shared-only pre-scan is deliberately separate from a later
        // post-mutation reconciliation; newly-opened coordinators already proved the same bound
        // during `open_with_startup_memory_limit` and retain their single initial scan.
        shared.reconcile_with_memory_limit(startup_memory_budget)?;
        return Ok(Some(Arc::clone(shared)));
    }

    crate::LocalDiskBudget::open_with_startup_memory_limit(data_path, limits, startup_memory_budget)
        .map(Some)
}

fn validate_disk_limit_relationships(
    builder: &StorageBuilder,
    local_disk_budget: Option<&crate::LocalDiskBudget>,
) -> Result<()> {
    let Some(local_disk_budget) = local_disk_budget else {
        return Ok(());
    };
    let Some(max_bytes) = local_disk_budget.limits().max_bytes else {
        return Ok(());
    };
    if builder.wal_enabled() && builder.wal_size_limit_bytes() != usize::MAX {
        let wal_bytes = builder.wal_size_limit_bytes().min(u64::MAX as usize) as u64;
        let normal_growth_limit =
            max_bytes.saturating_sub(local_disk_budget.limits().maintenance_temp_reserve_bytes);
        if wal_bytes > normal_growth_limit {
            return Err(TsinkError::InvalidConfiguration(format!(
                "WAL size limit {wal_bytes} exceeds local disk growth capacity {normal_growth_limit} after maintenance reserve"
            )));
        }
    }
    Ok(())
}

fn validate_tiered_storage_disk_scope(
    builder: &StorageBuilder,
    local_disk_budget: Option<&crate::LocalDiskBudget>,
) -> Result<()> {
    let (Some(object_store_path), Some(local_disk_budget)) =
        (builder.object_store_path(), local_disk_budget)
    else {
        return Ok(());
    };
    if local_disk_budget.overlaps(object_store_path)? {
        return Err(TsinkError::InvalidConfiguration(format!(
            "object store path {} must be outside managed local data path {}",
            object_store_path.display(),
            local_disk_budget.root().display()
        )));
    }
    Ok(())
}

impl StartupPlan {
    pub(super) fn wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    pub(super) fn startup_memory_budget(&self) -> usize {
        self.startup_memory_budget
    }

    pub(super) fn storage_options(&self) -> &ChunkStorageOptions {
        &self.storage_options
    }

    pub(super) fn paths(&self) -> &config::StoragePathLayout {
        &self.paths
    }

    pub(super) fn local_disk_budget(&self) -> Option<&Arc<crate::LocalDiskBudget>> {
        self.local_disk_budget.as_ref()
    }

    pub(super) fn data_directory_manifest(
        &self,
    ) -> Option<&data_directory_manifest::OpenedDataDirectoryManifest> {
        self.data_directory_manifest.as_ref()
    }

    pub(super) fn lane_flags(&self) -> (bool, bool) {
        (
            self.paths.numeric_lane_path.is_some(),
            self.paths.blob_lane_path.is_some(),
        )
    }

    pub(super) fn into_runtime_inputs(self) -> StartupRuntimeInputs {
        self.runtime_inputs
    }
}

fn acquire_startup_data_path_process_lock(
    builder: &StorageBuilder,
) -> Result<Option<DataPathProcessLock>> {
    if builder.runtime_mode() == StorageRuntimeMode::ComputeOnly {
        return Ok(None);
    }

    builder
        .data_path()
        .map(process_lock::DataPathProcessLock::acquire)
        .transpose()
}

fn acquire_startup_shared_object_store_process_lock(
    builder: &StorageBuilder,
) -> Result<Option<SharedObjectStoreProcessLock>> {
    if builder.runtime_mode() == StorageRuntimeMode::ComputeOnly {
        return Ok(None);
    }

    builder
        .object_store_path()
        .map(process_lock::SharedObjectStoreProcessLock::acquire)
        .transpose()
}

fn validate_tiered_storage_config(builder: &StorageBuilder) -> Result<()> {
    let Some(object_store_path) = builder.object_store_path() else {
        if builder.hot_tier_retention().is_some() || builder.warm_tier_retention().is_some() {
            return Err(TsinkError::InvalidConfiguration(
                "tiered retention policy requires an object store path".to_string(),
            ));
        }
        if builder.runtime_mode() == StorageRuntimeMode::ComputeOnly {
            return Err(TsinkError::InvalidConfiguration(
                "compute-only storage mode requires an object store path".to_string(),
            ));
        }
        return Ok(());
    };

    if builder.runtime_mode() != StorageRuntimeMode::ComputeOnly && builder.data_path().is_none() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "object store path requires persistent local data_path: {}",
            object_store_path.display()
        )));
    }

    let retention = builder.retention();
    let hot = builder.hot_tier_retention().unwrap_or(retention);
    let warm = builder.warm_tier_retention().unwrap_or(retention);
    if hot > warm {
        return Err(TsinkError::InvalidConfiguration(format!(
            "hot tier retention {:?} exceeds warm tier retention {:?}",
            hot, warm
        )));
    }
    if warm > retention {
        return Err(TsinkError::InvalidConfiguration(format!(
            "warm tier retention {:?} exceeds global retention {:?}",
            warm, retention
        )));
    }

    if hot == retention && warm == retention {
        tracing::warn!(
            path = %object_store_path.display(),
            retention = ?retention,
            "Object store path configured without an earlier hot/warm cutoff; data will remain hot until global retention expires"
        );
    }

    Ok(())
}
