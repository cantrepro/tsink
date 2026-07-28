use super::*;
use crate::engine::fs_utils::{
    admit_secure_snapshot_operation_retained_bytes, create_dir_all_and_sync_parents,
    is_link_or_reparse_point, path_exists_no_follow, paths_overlap_resolved, stage_dir_path,
    SecureSnapshotSourceTree, SecureSnapshotStagingDirectory,
};
use crate::engine::segment::{LoadedSegmentIndexes, WalHighWatermark};

#[path = "bootstrap/discovery.rs"]
mod discovery;
#[path = "bootstrap/finalize.rs"]
mod finalize;
#[path = "bootstrap/hydrate.rs"]
mod hydrate;
#[path = "bootstrap/memory.rs"]
mod memory;
#[path = "bootstrap/planning.rs"]
mod planning;
#[path = "bootstrap/recovery.rs"]
mod recovery;
#[path = "bootstrap/wal_open.rs"]
mod wal_open;

#[cfg(test)]
#[path = "bootstrap/tests.rs"]
mod tests;

use discovery::StartupDiscoveryPhase;
use finalize::StartupFinalizePhase;
use hydrate::StartupHydrationPhase;
use memory::StartupMemoryAdmission;
use planning::StartupPlanningPhase;
use recovery::StartupRecoveryPhase;
use wal_open::StartupWalOpenPhase;

pub(super) fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    match build_storage_inner(builder, false)? {
        BuiltStorage::Normal(storage) => Ok(storage as Arc<dyn Storage>),
        BuiltStorage::SnapshotValidation(_) => {
            unreachable!("normal storage build returned a snapshot validation guard")
        }
    }
}

pub(super) fn build_storage_for_snapshot_validation(
    builder: StorageBuilder,
) -> Result<SnapshotValidationStorage> {
    match build_storage_inner(builder, true)? {
        BuiltStorage::SnapshotValidation(storage) => Ok(storage),
        BuiltStorage::Normal(_) => {
            unreachable!("snapshot validation build returned a normal storage lifecycle")
        }
    }
}

enum BuiltStorage {
    Normal(Arc<ChunkStorage>),
    SnapshotValidation(SnapshotValidationStorage),
}

fn build_storage_inner(builder: StorageBuilder, snapshot_validation: bool) -> Result<BuiltStorage> {
    let plan = StartupPlanningPhase::prepare(&builder)?;
    let discovered = StartupDiscoveryPhase::discover(&builder, &plan)?;
    let recovered = StartupRecoveryPhase::recover(&plan, discovered)?;
    let finalize_state = recovered.finalize_state();
    let replay_highwater = recovered.loaded_segments().wal_replay_highwater;
    let next_segment_id = recovered.loaded_segments().next_segment_id;

    let wal = StartupWalOpenPhase::open(&builder, &plan, replay_highwater, snapshot_validation)?;
    let storage_options = plan.storage_options().clone();
    let paths = plan.paths().clone();
    let local_disk_budget = plan.local_disk_budget().cloned();
    let data_directory_manifest = plan.data_directory_manifest().cloned();
    let runtime_inputs = plan.into_runtime_inputs();

    let storage = StartupHydrationPhase::create_storage(
        builder.chunk_points(),
        storage_options,
        &paths,
        next_segment_id,
        wal,
        local_disk_budget.clone(),
        builder.query_budget_limits(),
    )?;
    let mut validation_guard =
        snapshot_validation.then(|| SnapshotValidationStorage::new(Arc::clone(&storage)));
    *storage.resource_configuration.write() = builder.resource_configuration_snapshot();
    if let Err(hydration_error) = StartupHydrationPhase::hydrate(
        storage.as_ref(),
        &builder,
        recovered,
        runtime_inputs.data_path_process_lock,
        runtime_inputs.shared_object_store_process_lock,
    ) {
        if let Some(validation_guard) = validation_guard.take() {
            return match validation_guard.finish() {
                Ok(()) => Err(hydration_error),
                Err(shutdown_error) => Err(TsinkError::Other(format!(
                    "snapshot validation hydration failed: {hydration_error}; non-persisting validation shutdown also failed: {shutdown_error}"
                ))),
            };
        }
        return Err(hydration_error);
    }

    if snapshot_validation {
        StartupFinalizePhase::run_snapshot_validation(&storage)?;
        drop(storage);
        return Ok(BuiltStorage::SnapshotValidation(
            validation_guard
                .take()
                .expect("snapshot validation lifecycle guard was installed"),
        ));
    } else {
        StartupFinalizePhase::run(
            &storage,
            finalize_state,
            runtime_inputs.background_threads_enabled,
            runtime_inputs.background_fail_fast,
        )?;
        if let Some(data_directory_manifest) = data_directory_manifest {
            data_directory_manifest.record_successful_open(local_disk_budget.as_ref())?;
        }
    }

    Ok(BuiltStorage::Normal(storage))
}

pub(super) fn restore_storage_from_snapshot(snapshot_path: &Path, data_path: &Path) -> Result<()> {
    if snapshot_path == data_path {
        return Err(TsinkError::InvalidConfiguration(
            "snapshot and restore paths must differ".to_string(),
        ));
    }

    let snapshot_meta =
        std::fs::symlink_metadata(snapshot_path).map_err(|err| TsinkError::IoWithPath {
            path: snapshot_path.to_path_buf(),
            source: err,
        })?;
    if is_link_or_reparse_point(&snapshot_meta) || !snapshot_meta.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a plain directory: {}",
            snapshot_path.display()
        )));
    }
    if paths_overlap_resolved(snapshot_path, data_path)? {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot and restore target paths must not overlap: {} and {}",
            snapshot_path.display(),
            data_path.display()
        )));
    }
    let snapshot_source = SecureSnapshotSourceTree::open_and_measure(snapshot_path)?;
    let validation_configuration = validate_secure_restore_source(&snapshot_source, snapshot_path)?;
    let validation_wal_enabled =
        snapshot_source.measured_directory_exists(Path::new(WAL_DIR_NAME))?;

    let Some(parent) = data_path.parent() else {
        return Err(TsinkError::InvalidConfiguration(format!(
            "restore target has no parent directory: {}",
            data_path.display()
        )));
    };
    create_dir_all_and_sync_parents(parent)?;

    let mut validation_staging = SecureSnapshotStagingDirectory::create_unique_replacement_sibling(
        data_path,
        "restore-validation",
    )?;
    let validation_staging_path = validation_staging.path().to_path_buf();
    validation_staging.set_operation_baseline_retained_bytes(
        snapshot_source.retained_memory_bytes(),
        "secure snapshot restore validation-copy state",
    )?;
    if let Err(copy_err) = snapshot_source.copy_to(&mut validation_staging, Path::new("")) {
        return Err(TsinkError::Other(format!(
            "restore validation copy failed: {copy_err}; retaining {} because descendant ownership was not completely captured before the failure",
            validation_staging_path.display()
        )));
    }
    validation_staging.ensure_declared_regular_file(
        Path::new(super::process_lock::DATA_PATH_LOCK_FILE_NAME),
        b"",
    )?;
    if let Err(sync_err) = validation_staging.sync_root() {
        return Err(TsinkError::Other(format!(
            "restore validation-copy synchronization failed: {sync_err}; retaining the handle-attested staging tree at {}",
            validation_staging_path.display()
        )));
    }
    let validation_live_retained_bytes = admit_secure_snapshot_operation_retained_bytes(
        &[
            snapshot_source.retained_memory_bytes(),
            validation_staging.retained_memory_bytes(),
        ],
        "secure snapshot restore validation namespace re-attestation",
        snapshot_path,
    )?;
    snapshot_source.verify_requested_namespace_unchanged(validation_live_retained_bytes)?;
    validate_and_remove_secure_restore_copy(
        validation_staging,
        validation_configuration,
        validation_wal_enabled,
    )?;
    snapshot_source.verify_unchanged(snapshot_source.retained_memory_bytes())?;
    snapshot_source
        .verify_requested_namespace_unchanged(snapshot_source.retained_memory_bytes())?;

    let original_target = if path_exists_no_follow(data_path)? {
        Some(
            SecureSnapshotSourceTree::open_and_measure_with_operation_baseline(
                data_path,
                snapshot_source.retained_memory_bytes(),
            )?,
        )
    } else {
        None
    };
    let restore_source_retained_bytes = admit_secure_snapshot_operation_retained_bytes(
        &[
            snapshot_source.retained_memory_bytes(),
            original_target
                .as_ref()
                .map_or(0, SecureSnapshotSourceTree::retained_memory_bytes),
        ],
        "secure snapshot restore source sessions",
        data_path,
    )?;
    let mut staging = SecureSnapshotStagingDirectory::create_unique_replacement_sibling(
        data_path,
        "restore-staging",
    )?;
    let staging_path = staging.path().to_path_buf();
    staging.set_operation_baseline_retained_bytes(
        restore_source_retained_bytes,
        "secure snapshot restore aggregate state",
    )?;
    if let Err(copy_err) = snapshot_source.copy_to(&mut staging, Path::new("")) {
        return Err(TsinkError::Other(format!(
            "restore staging copy failed: {copy_err}; retaining {} because descendant ownership was not completely captured before the failure",
            staging_path.display()
        )));
    }
    if let Err(sync_err) = staging.sync_root() {
        return Err(TsinkError::Other(format!(
            "restore staging synchronization failed: {sync_err}; retaining the handle-attested staging tree at {}",
            staging_path.display()
        )));
    }
    let restore_live_retained_bytes = admit_secure_snapshot_operation_retained_bytes(
        &[
            restore_source_retained_bytes,
            staging.retained_memory_bytes(),
        ],
        "secure snapshot restore publication namespace re-attestation",
        snapshot_path,
    )?;
    snapshot_source.verify_requested_namespace_unchanged(restore_live_retained_bytes)?;

    let backup = original_target
        .is_some()
        .then(|| stage_dir_path(data_path, "restore-backup"))
        .transpose()?;
    activate_secure_staged_restore(
        staging,
        data_path,
        backup.as_deref(),
        original_target,
        &staging_path,
    )
}

pub(super) fn restore_storage_from_snapshot_with_disk_budget(
    snapshot_path: &Path,
    data_path: &Path,
    disk_budget: Arc<crate::LocalDiskBudget>,
) -> Result<()> {
    let snapshot_meta =
        std::fs::symlink_metadata(snapshot_path).map_err(|source| TsinkError::IoWithPath {
            path: snapshot_path.to_path_buf(),
            source,
        })?;
    if is_link_or_reparse_point(&snapshot_meta) || !snapshot_meta.file_type().is_dir() {
        return Err(TsinkError::InvalidConfiguration(format!(
            "snapshot path is not a plain directory: {}",
            snapshot_path.display()
        )));
    }
    if disk_budget.overlaps(snapshot_path)? {
        return Err(TsinkError::InvalidConfiguration(format!(
            "budgeted restore requires an external snapshot that does not overlap local disk root {}: {}",
            disk_budget.root().display(),
            snapshot_path.display()
        )));
    }

    // Complete source validation and bounded byte/entry measurement before quota admission can
    // create a destination parent or staging directory.
    let snapshot_source = SecureSnapshotSourceTree::open_and_measure(snapshot_path)?;
    let validation_configuration = validate_secure_restore_source(&snapshot_source, snapshot_path)?;
    let validation_wal_enabled =
        snapshot_source.measured_directory_exists(Path::new(WAL_DIR_NAME))?;
    let snapshot_measurement = snapshot_source.measurement();
    let entry_allowance_bytes = disk_budget.snapshot_restore_entry_staging_allowance_bytes()?;
    let staging_admission_bytes =
        snapshot_measurement.staging_admission_bytes(entry_allowance_bytes)?;
    // Validation predeclares one operation-owned process-lock file that public snapshots omit.
    // Recovery may also need one source-logical copy plus one entry allowance for atomic-rewrite
    // scratch while the copied source remains live. The validation and publication staging trees
    // are sequential, so they share this one reservation rather than summing two complete staging
    // images.
    let validation_owned_entry_allowance_bytes =
        entry_allowance_bytes.checked_mul(2).ok_or_else(|| {
            TsinkError::Other(
                "snapshot restore validation-owned entry allowance exceeds the supported byte range"
                    .to_string(),
            )
        })?;
    let restore_operation_admission_bytes = staging_admission_bytes
        .checked_add(validation_owned_entry_allowance_bytes)
        .and_then(|bytes| bytes.checked_add(snapshot_measurement.logical_bytes))
        .ok_or_else(|| {
            TsinkError::Other(
                "snapshot restore semantic-validation staging admission exceeds the supported byte range"
                    .to_string(),
            )
        })?;
    let staging = stage_dir_path(data_path, "restore-staging")?;
    let backup = stage_dir_path(data_path, "restore-backup")?;

    disk_budget.with_managed_directory_replacement(
        data_path,
        &staging,
        &backup,
        restore_operation_admission_bytes,
        entry_allowance_bytes,
        |target, staging, backup| {
            let parent = target.parent().ok_or_else(|| {
                TsinkError::InvalidConfiguration(format!(
                    "restore target has no parent directory: {}",
                    target.display()
                ))
            })?;
            disk_budget.create_dir_all_and_sync_parents(parent)?;

            // Conservatively revalidate the final entries immediately before creating staging.
            // The budget coordinator serializes cooperating writers; these checks also reject a
            // static symlink or unexpected entry introduced since the initial preflight.
            disk_budget.validate_managed_directory_path(target)?;
            if path_exists_no_follow(staging)? || path_exists_no_follow(backup)? {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "budgeted restore staging or backup path appeared after preflight: {}, {}",
                    staging.display(),
                    backup.display()
                )));
            }

            let mut validation_staging = SecureSnapshotStagingDirectory::create_exact(
                staging,
                "budgeted restore validation staging",
            )?;
            validation_staging.set_operation_baseline_retained_bytes(
                snapshot_source.retained_memory_bytes(),
                "budgeted secure snapshot restore validation-copy state",
            )?;
            if let Err(copy_err) =
                snapshot_source.copy_to(&mut validation_staging, Path::new(""))
            {
                return Err(TsinkError::Other(format!(
                    "budgeted restore validation copy failed: {copy_err}; retaining {} because descendant ownership was not completely captured before the failure",
                    staging.display()
                )));
            }
            validation_staging.ensure_declared_regular_file(
                Path::new(super::process_lock::DATA_PATH_LOCK_FILE_NAME),
                b"",
            )?;
            if let Err(sync_err) = validation_staging.sync_root() {
                return Err(TsinkError::Other(format!(
                    "budgeted restore validation-copy synchronization failed: {sync_err}; retaining the handle-attested staging tree at {}",
                    staging.display()
                )));
            }
            let validation_live_retained_bytes =
                admit_secure_snapshot_operation_retained_bytes(
                    &[
                        snapshot_source.retained_memory_bytes(),
                        validation_staging.retained_memory_bytes(),
                    ],
                    "budgeted secure snapshot restore validation namespace re-attestation",
                    snapshot_path,
                )?;
            snapshot_source
                .verify_requested_namespace_unchanged(validation_live_retained_bytes)?;
            validate_and_remove_secure_restore_copy(
                validation_staging,
                validation_configuration,
                validation_wal_enabled,
            )?;
            snapshot_source.verify_unchanged(snapshot_source.retained_memory_bytes())?;
            snapshot_source
                .verify_requested_namespace_unchanged(snapshot_source.retained_memory_bytes())?;

            disk_budget.validate_managed_directory_path(target)?;
            if path_exists_no_follow(staging)? || path_exists_no_follow(backup)? {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "budgeted restore staging or backup path appeared after semantic validation: {}, {}",
                    staging.display(),
                    backup.display()
                )));
            }

            let original_target = match path_exists_no_follow(target) {
                Ok(true) => Some(
                    SecureSnapshotSourceTree::open_and_measure_with_operation_baseline(
                        target,
                        snapshot_source.retained_memory_bytes(),
                    )?,
                ),
                Ok(false) => None,
                Err(inspect_err) => {
                    return Err(TsinkError::Other(format!(
                        "budgeted target inspection failed: {inspect_err}; no staging directory was created"
                    )));
                }
            };
            let restore_source_retained_bytes =
                admit_secure_snapshot_operation_retained_bytes(
                    &[
                        snapshot_source.retained_memory_bytes(),
                        original_target
                            .as_ref()
                            .map_or(0, SecureSnapshotSourceTree::retained_memory_bytes),
                    ],
                    "budgeted secure snapshot restore source sessions",
                    target,
                )?;
            let mut secure_staging =
                SecureSnapshotStagingDirectory::create_exact(staging, "budgeted restore staging")?;
            secure_staging.set_operation_baseline_retained_bytes(
                restore_source_retained_bytes,
                "budgeted secure snapshot restore aggregate state",
            )?;
            if let Err(copy_err) = snapshot_source.copy_to(&mut secure_staging, Path::new("")) {
                return Err(TsinkError::Other(format!(
                    "budgeted restore staging copy failed: {copy_err}; retaining {} because descendant ownership was not completely captured before the failure",
                    staging.display()
                )));
            }
            if let Err(sync_err) = secure_staging.sync_root() {
                return Err(TsinkError::Other(format!(
                    "budgeted restore staging synchronization failed: {sync_err}; retaining the handle-attested staging tree at {}",
                    staging.display()
                )));
            }
            let restore_live_retained_bytes =
                admit_secure_snapshot_operation_retained_bytes(
                    &[
                        restore_source_retained_bytes,
                        secure_staging.retained_memory_bytes(),
                    ],
                    "budgeted secure snapshot restore publication namespace re-attestation",
                    snapshot_path,
                )?;
            snapshot_source
                .verify_requested_namespace_unchanged(restore_live_retained_bytes)?;

            if let Err(validate_err) = disk_budget.validate_managed_directory_path(target) {
                return Err(TsinkError::Other(format!(
                    "budgeted restore validation failed: {validate_err}; retaining {}",
                    staging.display()
                )));
            }
            if let Err(validate_err) = disk_budget.validate_managed_directory_path(staging) {
                return Err(TsinkError::Other(format!(
                    "budgeted staging validation failed: {validate_err}; retaining {}",
                    staging.display()
                )));
            }
            let backup_exists = match path_exists_no_follow(backup) {
                Ok(exists) => exists,
                Err(inspect_err) => {
                    return Err(TsinkError::Other(format!(
                        "budgeted backup inspection failed: {inspect_err}; retaining {}",
                        staging.display()
                    )));
                }
            };
            if backup_exists {
                return Err(TsinkError::InvalidConfiguration(format!(
                        "budgeted restore backup path appeared after staging: {}",
                        backup.display()
                    )));
            }

            activate_secure_staged_restore(
                secure_staging,
                target,
                original_target.as_ref().map(|_| backup),
                original_target,
                staging,
            )
        },
    )
}

fn validate_secure_restore_source(
    snapshot_source: &SecureSnapshotSourceTree,
    snapshot_path: &Path,
) -> Result<data_directory_manifest::SnapshotValidationConfiguration> {
    let relative = Path::new(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME);
    let manifest_path = snapshot_path.join(relative);
    let bytes = snapshot_source
        .read_measured_file_bounded(relative, data_directory_manifest::MAX_MANIFEST_FILE_BYTES)?;
    let validation_configuration =
        data_directory_manifest::validate_snapshot_manifest_bytes(&bytes, &manifest_path)?;
    drop(bytes);
    validate_retained_legacy_registry_catalog(snapshot_source, snapshot_path)?;
    snapshot_source.verify_unchanged(snapshot_source.retained_memory_bytes())?;
    snapshot_source
        .verify_requested_namespace_unchanged(snapshot_source.retained_memory_bytes())?;
    Ok(validation_configuration)
}

fn validate_retained_legacy_registry_catalog(
    snapshot_source: &SecureSnapshotSourceTree,
    snapshot_path: &Path,
) -> Result<()> {
    const LEGACY_REGISTRY_CATALOG_PATH: &str = "series_index.catalog.json";
    const LEGACY_REGISTRY_CATALOG_MAX_BYTES: usize = 64 * 1024 * 1024;

    #[derive(serde::Deserialize)]
    struct VersionProbe {
        version: u32,
    }

    let relative = Path::new(LEGACY_REGISTRY_CATALOG_PATH);
    if !snapshot_source.measured_regular_file_exists(relative)? {
        return Ok(());
    }
    let path = snapshot_path.join(relative);
    let bytes =
        snapshot_source.read_measured_file_bounded(relative, LEGACY_REGISTRY_CATALOG_MAX_BYTES)?;
    let probe = serde_json::from_slice::<VersionProbe>(&bytes).map_err(|err| {
        TsinkError::DataCorruption(format!(
            "catalog.local_corrupt: local registry catalog JSON cannot be decoded at {}: {err}",
            path.display()
        ))
    })?;
    if probe.version != 2 {
        return Err(TsinkError::DataCorruption(format!(
            "catalog.local_version_unsupported: local registry catalog version {} is unsupported at {}",
            probe.version,
            path.display()
        )));
    }
    Ok(())
}

fn validate_and_remove_secure_restore_copy(
    staging: SecureSnapshotStagingDirectory,
    configuration: data_directory_manifest::SnapshotValidationConfiguration,
    wal_enabled: bool,
) -> Result<()> {
    let staging_path = staging.path().to_path_buf();
    let validation_result = (|| -> Result<()> {
        let numeric_lane = staging_path.join(NUMERIC_LANE_ROOT);
        let blob_lane = staging_path.join(BLOB_LANE_ROOT);
        let numeric_lane = path_exists_no_follow(&numeric_lane)?.then_some(numeric_lane);
        let blob_lane = path_exists_no_follow(&blob_lane)?.then_some(blob_lane);
        let segment_inventory = tiering::build_segment_inventory_fail_on_invalid(
            numeric_lane.as_deref(),
            blob_lane.as_deref(),
            None,
            crate::engine::segment::SegmentValidationContext::RuntimeRefresh,
        )?;
        let validation_memory_limit =
            usize::try_from(crate::ResourceLimits::server().accounted_memory_bytes)
                .unwrap_or(usize::MAX);
        for entry in segment_inventory.entries() {
            crate::engine::segment::validate_segment_payloads_for_restore(
                &entry.root,
                validation_memory_limit,
            )?;
        }
        tiering::validate_restore_segment_catalog(
            &staging_path,
            numeric_lane.as_deref(),
            blob_lane.as_deref(),
        )?;

        let storage = StorageBuilder::new()
            .with_resource_profile(crate::ResourceProfile::Server)
            .with_timestamp_precision(configuration.timestamp_precision)
            .with_chunk_points(configuration.chunk_point_capacity)
            .with_partition_duration(configuration.partition_duration)
            .with_wal_enabled(wal_enabled)
            .with_filesystem_free_headroom(0)
            .with_maintenance_temp_reserve(0)
            .with_data_path(&staging_path)
            .with_background_threads_enabled_for_validation(false)
            .build_for_snapshot_validation()?;
        let health = storage.health();
        let close_result = storage.finish();
        if health.degraded {
            return Err(TsinkError::DataCorruption(format!(
                "snapshot production-open validation entered degraded health: background_errors={}, maintenance_errors={}, last_background_error={:?}, last_maintenance_error={:?}",
                health.background_errors_total,
                health.maintenance_errors_total,
                health.last_background_error,
                health.last_maintenance_error
            )));
        }
        close_result
    })();

    let cleanup_result = staging.remove_after_exact_manifest_refresh();
    match (validation_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(validation), Ok(())) => Err(validation),
        (Ok(()), Err(cleanup)) => Err(TsinkError::Other(format!(
            "snapshot passed production-open validation but exact validation-copy cleanup failed: {cleanup}; any surviving staging content is retained at {}",
            staging_path.display()
        ))),
        (Err(validation), Err(cleanup)) => Err(TsinkError::Other(format!(
            "snapshot failed production-open validation: {validation}; exact validation-copy cleanup also failed: {cleanup}; any surviving staging content is retained at {}",
            staging_path.display()
        ))),
    }
}

fn publish_secure_restore(
    staging: SecureSnapshotStagingDirectory,
    target: &Path,
    staging_path: &Path,
) -> Result<()> {
    staging.publish_noreplace(target).map_err(|publication| {
        if publication.published {
            TsinkError::Other(format!(
                "restore reached visible target {} but post-publication attestation or parent synchronization failed: {}; the visible target is retained",
                target.display(),
                publication.error
            ))
        } else {
            TsinkError::Other(format!(
                "restore publication failed before activation: {}; retaining handle-attested staging tree at {}",
                publication.error,
                staging_path.display()
            ))
        }
    })
}

fn activate_secure_staged_restore(
    staging: SecureSnapshotStagingDirectory,
    target: &Path,
    backup: Option<&Path>,
    original_target: Option<SecureSnapshotSourceTree>,
    staging_path: &Path,
) -> Result<()> {
    let (backup, original_target) = match (backup, original_target) {
        (None, None) => return publish_secure_restore(staging, target, staging_path),
        (Some(backup), Some(original_target)) => (backup, original_target),
        _ => {
            return Err(TsinkError::Other(
                "secure restore replacement requires both a backup path and the pre-move target identity manifest"
                    .to_string(),
            ))
        }
    };

    let published_backup = staging
        .publish_replacing(target, backup, original_target)
        .map_err(|publication| {
            if publication.published {
                TsinkError::Other(format!(
                    "restore reached visible target {} but post-publication attestation or parent synchronization failed: {}; visible target and backup {} are retained",
                    target.display(),
                    publication.error,
                    backup.display()
                ))
            } else {
                TsinkError::Other(format!(
                    "restore replacement failed before staging publication: {}; retaining staging {} and any surviving backup {}",
                    publication.error,
                    staging_path.display(),
                    backup.display()
                ))
            }
        })?;
    published_backup.remove_and_sync_parent().map_err(|cleanup| {
        TsinkError::Other(format!(
            "restore replacement published the visible target {} but exact pre-move-identity backup cleanup failed at {}: {}; the visible target and any surviving backup are retained",
            target.display(),
            backup.display(),
            cleanup
        ))
    })
}

pub(super) fn merge_loaded_segment_indexes(
    mut numeric: LoadedSegmentIndexes,
    mut blob: LoadedSegmentIndexes,
    numeric_lane_enabled: bool,
    blob_lane_enabled: bool,
) -> Result<LoadedSegmentIndexes> {
    let mut series_by_id = BTreeMap::new();
    for series in numeric.series.drain(..) {
        series_by_id.insert(series.series_id, series);
    }

    for series in blob.series.drain(..) {
        match series_by_id.get(&series.series_id) {
            Some(existing)
                if existing.metric == series.metric && existing.labels == series.labels => {}
            Some(_) => {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} conflicts across lane segment families",
                    series.series_id
                )));
            }
            None => {
                series_by_id.insert(series.series_id, series);
            }
        }
    }

    let numeric_has_segments = !numeric.indexed_segments.is_empty();
    let blob_has_segments = !blob.indexed_segments.is_empty();

    let mut indexed_segments = numeric.indexed_segments;
    indexed_segments.append(&mut blob.indexed_segments);
    indexed_segments.sort_by_key(|segment| (segment.manifest.level, segment.manifest.segment_id));

    let replay_highwater = match (numeric_lane_enabled, blob_lane_enabled) {
        (true, true) => match (numeric_has_segments, blob_has_segments) {
            (true, true) => numeric.wal_replay_highwater.min(blob.wal_replay_highwater),
            _ => WalHighWatermark::default(),
        },
        (true, false) => numeric.wal_replay_highwater,
        (false, true) => blob.wal_replay_highwater,
        (false, false) => WalHighWatermark::default(),
    };

    Ok(LoadedSegmentIndexes {
        next_segment_id: numeric.next_segment_id.max(blob.next_segment_id).max(1),
        series: series_by_id.into_values().collect(),
        indexed_segments,
        wal_replay_highwater: replay_highwater,
    })
}
