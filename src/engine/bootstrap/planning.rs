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
        let local_disk_budget = resolve_local_disk_budget(builder)?;
        validate_tiered_storage_disk_scope(builder, local_disk_budget.as_deref())?;
        validate_disk_limit_relationships(builder, local_disk_budget.as_deref())?;
        let storage_options = ChunkStorageOptions::from(builder);
        Ok(StartupPlan {
            wal_enabled: builder.wal_enabled(),
            paths: config::StoragePathLayout::from(builder),
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
    if local_disk_budget.governs(object_store_path)? {
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
