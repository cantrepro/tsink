use super::*;

pub(super) struct StartupPlan {
    wal_enabled: bool,
    storage_options: ChunkStorageOptions,
    paths: config::StoragePathLayout,
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

        let storage_options = ChunkStorageOptions::from(builder);
        Ok(StartupPlan {
            wal_enabled: builder.wal_enabled(),
            paths: config::StoragePathLayout::from(builder),
            runtime_inputs: StartupRuntimeInputs {
                background_threads_enabled: storage_options.background_threads_enabled,
                background_fail_fast: storage_options.background_fail_fast,
                data_path_process_lock: acquire_startup_data_path_process_lock(builder)?,
            },
            storage_options,
        })
    }
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
