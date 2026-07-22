use super::*;

pub(super) struct StartupPlan {
    wal_enabled: bool,
    storage_options: ChunkStorageOptions,
    paths: config::StoragePathLayout,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    runtime_inputs: StartupRuntimeInputs,
}

pub(super) struct StartupRuntimeInputs {
    pub(super) background_threads_enabled: bool,
    pub(super) background_fail_fast: bool,
    pub(super) data_path_process_lock: Option<DataPathProcessLock>,
}

pub(super) struct StartupPlanningPhase;

impl StartupPlanningPhase {
    pub(super) fn prepare(builder: &StorageBuilder) -> Result<StartupPlan> {
        validate_tiered_storage_config(builder)?;

        let data_path_process_lock = acquire_startup_data_path_process_lock(builder)?;
        let paths = config::StoragePathLayout::from(builder);
        let local_disk_budget = resolve_local_disk_budget(builder)?;
        validate_tiered_storage_disk_scope(builder, local_disk_budget.as_deref())?;
        validate_disk_limit_relationships(builder, local_disk_budget.as_deref())?;
        validate_owned_local_storage_namespaces(&paths, local_disk_budget.as_deref())?;
        cleanup_owned_local_storage_orphans(&paths, local_disk_budget.as_ref())?;
        let storage_options = ChunkStorageOptions::from(builder);
        Ok(StartupPlan {
            wal_enabled: builder.wal_enabled(),
            paths,
            local_disk_budget,
            runtime_inputs: StartupRuntimeInputs {
                background_threads_enabled: storage_options.background_threads_enabled,
                background_fail_fast: storage_options.background_fail_fast,
                data_path_process_lock,
            },
            storage_options,
        })
    }
}

fn validate_owned_local_storage_namespaces(
    paths: &config::StoragePathLayout,
    local_disk_budget: Option<&crate::LocalDiskBudget>,
) -> Result<()> {
    let Some(budget) = local_disk_budget else {
        return Ok(());
    };

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
            data_path.join(rollups::ROLLUP_DIR_NAME),
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
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
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
    for target in fixed_targets {
        budget.cleanup_atomic_write_temps(&target)?;
    }

    budget.cleanup_atomic_write_temps_matching_targets(
        &SeriesRegistry::incremental_dir(series_index_path),
        is_registry_delta_segment_name,
    )?;

    for lane_path in [
        paths.numeric_lane_path.as_deref(),
        paths.blob_lane_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let tombstone_path = lane_path.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME);
        budget.cleanup_atomic_write_temps(&tombstone_path)?;
        budget.cleanup_atomic_write_temps_matching_targets(
            &lane_path
                .join(format!(
                    "{}.store",
                    crate::engine::tombstone::TOMBSTONES_FILE_NAME
                ))
                .join("shards"),
            is_tombstone_shard_name,
        )?;
        crate::engine::tombstone::cleanup_unreferenced_tombstone_shards(
            &tombstone_path,
            Some(budget),
        )?;
        budget.cleanup_atomic_write_temps_matching_targets(
            &lane_path.join(".compaction-replacements"),
            is_compaction_replacement_marker_name,
        )?;

        for level in 0..=2 {
            budget.cleanup_temporary_directories_matching_names(
                &lane_path.join("segments").join(format!("L{level}")),
                is_segment_staging_name,
            )?;
        }
    }

    Ok(())
}

fn is_exact_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_registry_delta_segment_name(name: &str) -> bool {
    name.strip_prefix("delta-")
        .and_then(|value| value.strip_suffix(".bin"))
        .is_some_and(|nonce| is_exact_lower_hex(nonce, 16))
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
        if disk_settings_requested {
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

    crate::LocalDiskBudget::open(data_path, limits).map(Some)
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
