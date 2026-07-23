use super::*;

pub(super) struct StartupPlan {
    wal_enabled: bool,
    startup_memory_budget: usize,
    storage_options: ChunkStorageOptions,
    paths: config::StoragePathLayout,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
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
        let data_path_process_lock = acquire_startup_data_path_process_lock(builder)?;
        let paths = config::StoragePathLayout::from(builder);
        let local_disk_budget = resolve_local_disk_budget(builder, startup_memory_budget)?;
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

fn cleanup_owned_local_storage_orphans(
    paths: &config::StoragePathLayout,
    tombstone_lanes: &[crate::engine::tombstone::TombstoneLane],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget_bytes: usize,
) -> Result<()> {
    let Some(budget) = local_disk_budget else {
        return Ok(());
    };
    let Some(series_index_path) = paths.series_index_path.as_deref() else {
        return Ok(());
    };
    let data_path = series_index_path.parent().ok_or_else(|| {
        TsinkError::InvalidConfiguration(format!(
            "series registry path has no parent directory: {}",
            series_index_path.display()
        ))
    })?;

    let fixed_targets = [
        series_index_path.to_path_buf(),
        SeriesRegistry::incremental_path(series_index_path),
        registry_catalog::catalog_path(series_index_path),
        data_path.join(tiering::SEGMENT_CATALOG_FILE_NAME),
        data_path
            .join(rollups::ROLLUP_DIR_NAME)
            .join("policies.json"),
        data_path.join(rollups::ROLLUP_DIR_NAME).join("state.json"),
    ];
    let fixed_target_bytes =
        fixed_targets
            .iter()
            .try_fold(std::mem::size_of_val(&fixed_targets), |total, path| {
                total.checked_add(path.capacity()).ok_or_else(|| {
                    TsinkError::Other("startup fixed-target memory model overflow".to_string())
                })
            })?;
    crate::disk_budget::admit_startup_memory(memory_budget_bytes, fixed_target_bytes)?;

    // Complete all generic owned-temp scans before the first deletion. The same exact-shape
    // predicates are rerun during execution while the process lease remains held.
    for target in &fixed_targets {
        budget
            .preflight_atomic_write_temps_with_startup_memory_limit(target, memory_budget_bytes)?;
    }
    budget.preflight_atomic_write_temps_matching_targets_with_startup_memory_limit(
        &SeriesRegistry::incremental_dir(series_index_path),
        is_registry_incremental_file_name,
        memory_budget_bytes,
    )?;
    budget.preflight_atomic_write_temps_with_startup_memory_limit(
        &data_path
            .join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
            .join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_FILE_NAME),
        memory_budget_bytes,
    )?;
    for lane in tombstone_lanes {
        if budget.governs_entry(&lane.manifest_path)? {
            budget.preflight_atomic_write_temps_with_startup_memory_limit(
                &lane.manifest_path,
                memory_budget_bytes,
            )?;
            budget.preflight_atomic_write_temps_matching_targets_with_startup_memory_limit(
                &lane
                    .manifest_path
                    .with_file_name(format!(
                        "{}.store",
                        crate::engine::tombstone::TOMBSTONES_FILE_NAME
                    ))
                    .join("shards"),
                is_tombstone_shard_name,
                memory_budget_bytes,
            )?;
        }
    }
    for lane_path in [
        paths.numeric_lane_path.as_deref(),
        paths.blob_lane_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        budget.preflight_atomic_write_temps_matching_targets_with_startup_memory_limit(
            &lane_path.join(".compaction-replacements"),
            is_compaction_replacement_marker_name,
            memory_budget_bytes,
        )?;
        for level in 0..=2 {
            budget.preflight_temporary_directories_matching_names_with_startup_memory_limit(
                &lane_path.join("segments").join(format!("L{level}")),
                is_segment_staging_name,
                memory_budget_bytes,
            )?;
        }
    }
    for lane in tombstone_lanes {
        crate::engine::tombstone::preflight_unreferenced_tombstone_shard_cleanup_memory(
            lane,
            |required| crate::disk_budget::admit_startup_memory(memory_budget_bytes, required),
        )?;
    }

    for target in fixed_targets {
        budget
            .cleanup_atomic_write_temps_with_startup_memory_limit(&target, memory_budget_bytes)?;
    }

    budget.cleanup_atomic_write_temps_matching_targets_with_startup_memory_limit(
        &SeriesRegistry::incremental_dir(series_index_path),
        is_registry_incremental_file_name,
        memory_budget_bytes,
    )?;
    budget.cleanup_atomic_write_temps_with_startup_memory_limit(
        &data_path
            .join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_DIR_NAME)
            .join(crate::engine::tombstone::TOMBSTONE_TRANSACTION_FILE_NAME),
        memory_budget_bytes,
    )?;

    for lane in tombstone_lanes {
        if budget.governs_entry(&lane.manifest_path)? {
            budget.cleanup_atomic_write_temps_with_startup_memory_limit(
                &lane.manifest_path,
                memory_budget_bytes,
            )?;
            budget.cleanup_atomic_write_temps_matching_targets_with_startup_memory_limit(
                &lane
                    .manifest_path
                    .with_file_name(format!(
                        "{}.store",
                        crate::engine::tombstone::TOMBSTONES_FILE_NAME
                    ))
                    .join("shards"),
                is_tombstone_shard_name,
                memory_budget_bytes,
            )?;
        }
        crate::engine::tombstone::cleanup_unreferenced_tombstone_shards_with_memory_admission(
            lane,
            Some(budget),
            |_| Ok(()),
        )?;
    }

    for lane_path in [
        paths.numeric_lane_path.as_deref(),
        paths.blob_lane_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        budget.cleanup_atomic_write_temps_matching_targets_with_startup_memory_limit(
            &lane_path.join(".compaction-replacements"),
            is_compaction_replacement_marker_name,
            memory_budget_bytes,
        )?;

        for level in 0..=2 {
            budget.cleanup_temporary_directories_matching_names_with_startup_memory_limit(
                &lane_path.join("segments").join(format!("L{level}")),
                is_segment_staging_name,
                memory_budget_bytes,
            )?;
        }
    }

    Ok(())
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
    budget.preflight_atomic_write_temps_matching_targets_with_startup_memory_limit(
        &data_path.join(super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME),
        super::super::maintenance::is_post_flush_replacement_marker_name,
        memory_budget_bytes,
    )?;
    cleanup_post_flush_staging_orphans(
        paths,
        tiered_storage,
        Some(budget),
        memory_budget_bytes,
        false,
    )?;
    budget.cleanup_atomic_write_temps_matching_targets_with_startup_memory_limit(
        &data_path.join(super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME),
        super::super::maintenance::is_post_flush_replacement_marker_name,
        memory_budget_bytes,
    )?;
    cleanup_post_flush_staging_orphans(
        paths,
        tiered_storage,
        Some(budget),
        memory_budget_bytes,
        true,
    )
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

fn cleanup_post_flush_staging_orphans(
    paths: &config::StoragePathLayout,
    tiered_storage: Option<&config::TieredStorageConfig>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget_bytes: usize,
    execute: bool,
) -> Result<()> {
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
            if let Ok(root) = resolver.lane_root(lane, tier) {
                if !lane_roots.contains(&root) {
                    push_startup_cleanup_path(
                        &mut lane_roots,
                        root,
                        memory_budget_bytes,
                        "post-flush lane-root planning",
                    )?;
                }
            }
        }
    }

    if !execute {
        let mut namespace_budget = crate::engine::fs_utils::RecoveryNamespaceBudget::new(
            crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        );
        for lane_root in &lane_roots {
            if let (Some(parent), Some(target_name)) = (
                lane_root.parent(),
                lane_root.file_name().and_then(|name| name.to_str()),
            ) {
                let prefix = format!(".tmp-tsink-post-flush-retention-rewrite-{target_name}-");
                preflight_exact_post_flush_stage_dirs_global(
                    parent,
                    |name| {
                        name.strip_prefix(&prefix)
                            .is_some_and(|nonce| is_exact_lower_hex(nonce, 16))
                    },
                    &mut namespace_budget,
                    crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
                    memory_budget_bytes,
                )?;
            }
            for level in 0..=2 {
                preflight_exact_post_flush_stage_dirs_global(
                    &lane_root.join("segments").join(format!("L{level}")),
                    is_post_flush_copy_staging_name,
                    &mut namespace_budget,
                    crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
                    memory_budget_bytes,
                )?;
            }
        }
        return Ok(());
    }

    for lane_root in lane_roots {
        if let (Some(parent), Some(target_name)) = (
            lane_root.parent(),
            lane_root.file_name().and_then(|name| name.to_str()),
        ) {
            let prefix = format!(".tmp-tsink-post-flush-retention-rewrite-{target_name}-");
            cleanup_exact_post_flush_stage_dirs(
                parent,
                |name| {
                    name.strip_prefix(&prefix)
                        .is_some_and(|nonce| is_exact_lower_hex(nonce, 16))
                },
                local_disk_budget,
                memory_budget_bytes,
                execute,
            )?;
        }
        for level in 0..=2 {
            cleanup_exact_post_flush_stage_dirs(
                &lane_root.join("segments").join(format!("L{level}")),
                is_post_flush_copy_staging_name,
                local_disk_budget,
                memory_budget_bytes,
                execute,
            )?;
        }
    }
    Ok(())
}

fn cleanup_exact_post_flush_stage_dirs(
    parent: &Path,
    matches_owned_name: impl FnMut(&str) -> bool,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_budget_bytes: usize,
    execute: bool,
) -> Result<()> {
    cleanup_exact_post_flush_stage_dirs_with_namespace_limits(
        parent,
        matches_owned_name,
        local_disk_budget,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_ENTRIES,
        crate::engine::fs_utils::MAX_RECOVERY_NAMESPACE_DEPTH,
        memory_budget_bytes,
        execute,
    )
}

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
