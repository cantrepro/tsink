use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use super::super::tiering::{
    PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily,
};
use super::discovery::{StartupDiscoveryPhase, StartupInventoryState};
use super::finalize::{RegistryPersistenceAction, StartupFinalizeState};
use super::planning::StartupPlanningPhase;
use super::recovery::StartupRecoveryPhase;
use super::wal_open::StartupWalOpenPhase;
use super::*;
use crate::engine::segment::{QuarantinedSegmentRoot, SegmentManifest, StartupQuarantinedSegment};

fn startup_builder(data_path: &Path) -> StorageBuilder {
    StorageBuilder::new()
        .with_resource_profile(crate::ResourceProfile::ExpertUnlimited)
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
}

fn planning_error(builder: &StorageBuilder) -> TsinkError {
    match StartupPlanningPhase::prepare(builder) {
        Ok(_) => panic!("expected startup planning to fail"),
        Err(err) => err,
    }
}

fn inventory_entry(root: impl Into<PathBuf>, segment_id: u64) -> SegmentInventoryEntry {
    SegmentInventoryEntry {
        lane: SegmentLaneFamily::Numeric,
        tier: PersistedSegmentTier::Hot,
        root: root.into(),
        manifest: SegmentManifest {
            segment_id,
            level: 0,
            chunk_count: 1,
            point_count: 1,
            series_count: 1,
            min_ts: Some(1),
            max_ts: Some(1),
            wal_highwater: WalHighWatermark::default(),
        },
    }
}

#[test]
fn try_new_with_data_path_succeeds_for_empty_data_path() {
    let temp_dir = TempDir::new().unwrap();
    let storage = ChunkStorage::try_new_with_data_path(
        2,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(temp_dir.path().join(BLOB_LANE_ROOT)),
        1,
    )
    .unwrap();

    assert!(storage
        .list_metrics()
        .expect("empty startup should succeed")
        .is_empty());

    storage.close().unwrap();
}

#[test]
fn build_storage_recovers_rows_from_preexisting_data_path() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];

    {
        let storage = startup_builder(temp_dir.path()).build().unwrap();
        storage
            .insert_rows(&[Row::with_labels(
                "startup_builder_reopen",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = startup_builder(temp_dir.path()).build().unwrap();
    assert_eq!(
        reopened
            .select("startup_builder_reopen", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn try_new_with_data_path_rejects_preexisting_data_path() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = startup_builder(temp_dir.path()).build().unwrap();
        storage
            .insert_rows(&[Row::new("startup_builder_reopen", DataPoint::new(1, 1.0))])
            .unwrap();
        storage.close().unwrap();
    }

    let err = match ChunkStorage::try_new_with_data_path(
        2,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(temp_dir.path().join(BLOB_LANE_ROOT)),
        1,
    ) {
        Ok(_) => panic!("expected constructor to reject existing on-disk state"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message)
            if message.contains("StorageBuilder::build()")
    ));
}

#[test]
fn build_storage_returns_structured_error_for_invalid_data_path() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_path = temp_dir.path().join("not-a-directory");
    std::fs::write(&invalid_path, b"lock me").unwrap();

    let err = match startup_builder(&invalid_path).build() {
        Ok(_) => panic!("expected invalid data path to return an initialization error"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        TsinkError::Io(_) | TsinkError::IoWithPath { .. } | TsinkError::InvalidConfiguration(_)
    ));
}

#[test]
fn planning_rejects_invalid_local_disk_limit_relationships() {
    let temp_dir = TempDir::new().unwrap();
    let cases = [
        (0, 0, "local disk limit must be greater than zero"),
        (
            100,
            100,
            "maintenance temporary reserve 100 must be smaller than local disk limit 100",
        ),
        (
            100,
            101,
            "maintenance temporary reserve 101 must be smaller than local disk limit 100",
        ),
    ];

    for (index, (limit, reserve, expected)) in cases.into_iter().enumerate() {
        let builder = startup_builder(&temp_dir.path().join(format!("case-{index}")))
            .with_local_disk_limit(limit)
            .with_maintenance_temp_reserve(reserve);
        let err = planning_error(&builder);
        assert!(
            matches!(err, TsinkError::InvalidConfiguration(ref message) if message.contains(expected)),
            "unexpected planning error: {err:?}"
        );
    }
}

#[test]
fn planning_rejects_shared_disk_budget_for_a_different_data_root() {
    let temp_dir = TempDir::new().unwrap();
    let limits = crate::LocalDiskLimits {
        max_bytes: Some(1_000),
        ..crate::LocalDiskLimits::default()
    };
    let shared = crate::LocalDiskBudget::open(temp_dir.path().join("shared"), limits).unwrap();
    let builder =
        startup_builder(&temp_dir.path().join("different")).with_shared_local_disk_budget(shared);

    let err = planning_error(&builder);
    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message)
            if message.contains("shared local disk budget root")
                && message.contains("does not match data path")
    ));
}

#[test]
fn budgeted_restore_rolls_back_and_reconciles_after_activation_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    let target_path = budget_root.join("target");
    std::fs::create_dir_all(&snapshot_path).unwrap();
    std::fs::write(snapshot_path.join("new"), b"new-state").unwrap();
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();
    let budget =
        crate::LocalDiskBudget::open(&budget_root, crate::LocalDiskLimits::default()).unwrap();

    let synchronized_root = budget.root().to_path_buf();
    let root_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&root_syncs);
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path == synchronized_root && observed_syncs.fetch_add(1, Ordering::SeqCst) + 1 == 3
        },
        "injected restore activation sync failure",
    );

    let err = restore_storage_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(err
        .to_string()
        .contains("restore activation parent sync failed"));
    assert_eq!(root_syncs.load(Ordering::SeqCst), 5);
    assert_eq!(
        std::fs::read(target_path.join("old")).unwrap(),
        b"old-state"
    );
    assert!(!target_path.join("new").exists());
    assert!(std::fs::read_dir(&budget_root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".tmp-tsink-restore-")));
    let accounting = budget.snapshot();
    assert_eq!(accounting.accounted_bytes, 9);
    assert_eq!(accounting.active_reservations, 0);
    assert_eq!(accounting.reserved_bytes, 0);
}

#[test]
fn unbudgeted_restore_rolls_back_after_activation_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    std::fs::create_dir_all(&snapshot_path).unwrap();
    std::fs::write(snapshot_path.join("new"), b"new-state").unwrap();
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let synchronized_root = temp_dir.path().to_path_buf();
    let root_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&root_syncs);
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path == synchronized_root && observed_syncs.fetch_add(1, Ordering::SeqCst) + 1 == 2
        },
        "injected unbudgeted restore activation sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err
        .to_string()
        .contains("restore activation parent sync failed"));
    assert_eq!(root_syncs.load(Ordering::SeqCst), 4);
    assert_eq!(
        std::fs::read(target_path.join("old")).unwrap(),
        b"old-state"
    );
    assert!(!target_path.join("new").exists());
    assert!(std::fs::read_dir(temp_dir.path())
        .unwrap()
        .all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tmp-tsink-restore-")));
}

#[test]
fn unbudgeted_restore_removes_staging_after_staging_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    std::fs::create_dir_all(&snapshot_path).unwrap();
    std::fs::write(snapshot_path.join("payload"), b"new-state").unwrap();

    let staging_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&staging_syncs);
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-staging-")
            }) && observed_syncs.fetch_add(1, Ordering::SeqCst) + 1 == 2
        },
        "injected unbudgeted restore staging sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err
        .to_string()
        .contains("injected unbudgeted restore staging sync failure"));
    assert_eq!(staging_syncs.load(Ordering::SeqCst), 2);
    assert!(!target_path.exists());
    assert!(std::fs::read_dir(temp_dir.path())
        .unwrap()
        .all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tmp-tsink-restore-")));
}

#[test]
fn unbudgeted_restore_removes_staging_after_copy_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    std::fs::create_dir_all(&snapshot_path).unwrap();
    std::fs::write(snapshot_path.join("payload"), b"new-state").unwrap();

    let staging_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&staging_syncs);
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-staging-")
            }) && observed_syncs.fetch_add(1, Ordering::SeqCst) == 0
        },
        "injected unbudgeted restore copy sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err
        .to_string()
        .contains("injected unbudgeted restore copy sync failure"));
    assert_eq!(staging_syncs.load(Ordering::SeqCst), 1);
    assert!(!target_path.exists());
    assert!(std::fs::read_dir(temp_dir.path())
        .unwrap()
        .all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tmp-tsink-restore-")));
}

#[test]
fn unbudgeted_restore_synchronizes_missing_target_ancestors_before_staging() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let restore_parent = temp_dir.path().join("restore-parent");
    let target_path = restore_parent.join("nested/target");
    std::fs::create_dir_all(&snapshot_path).unwrap();
    std::fs::write(snapshot_path.join("payload"), b"state").unwrap();

    let synchronized_parent = restore_parent.clone();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| path == synchronized_parent,
        "injected missing restore ancestor sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err
        .to_string()
        .contains("injected missing restore ancestor sync failure"));
    assert!(!target_path.exists());
    assert!(std::fs::read_dir(restore_parent.join("nested"))
        .unwrap()
        .all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tmp-tsink-restore-")));
}

#[test]
fn post_flush_stage_cleanup_preflights_every_candidate_tree_before_removal() {
    let temp_dir = TempDir::new().unwrap();
    let parent = temp_dir.path().join("segments/L0");
    let exact = parent.join("owned-exact");
    let first = parent.join("owned-first");
    let second = parent.join("owned-second");
    std::fs::create_dir_all(&exact).unwrap();
    std::fs::write(exact.join("one"), b"one").unwrap();
    std::fs::write(exact.join("two"), b"two").unwrap();
    std::fs::create_dir_all(&first).unwrap();
    std::fs::write(first.join("one"), b"one").unwrap();
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(second.join("two"), b"two").unwrap();
    std::fs::write(second.join("three"), b"three").unwrap();

    super::planning::cleanup_exact_post_flush_stage_dirs_with_namespace_limits(
        &parent,
        |name| name == "owned-exact",
        None,
        8,
        2,
        4,
        usize::MAX,
        true,
    )
    .expect("the exact descendant cap must be accepted");
    assert!(!exact.exists());

    let err = super::planning::cleanup_exact_post_flush_stage_dirs_with_namespace_limits(
        &parent,
        |name| matches!(name, "owned-first" | "owned-second"),
        None,
        8,
        2,
        4,
        usize::MAX,
        true,
    )
    .expect_err("cap plus one across candidate trees must fail closed");
    assert!(err.to_string().contains("2-entry global work bound"));
    assert_eq!(std::fs::read(first.join("one")).unwrap(), b"one");
    assert_eq!(std::fs::read(second.join("two")).unwrap(), b"two");
    assert_eq!(std::fs::read(second.join("three")).unwrap(), b"three");
}

#[test]
fn post_flush_stage_preflight_shares_one_namespace_cap_across_parents() {
    let temp_dir = TempDir::new().unwrap();
    let first_parent = temp_dir.path().join("first");
    let second_parent = temp_dir.path().join("second");
    let first = first_parent.join("owned");
    let second = second_parent.join("owned");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(first.join("one"), b"one").unwrap();
    std::fs::write(second.join("two"), b"two").unwrap();

    let mut too_small = crate::engine::fs_utils::RecoveryNamespaceBudget::new(3);
    super::planning::preflight_exact_post_flush_stage_dirs_global(
        &first_parent,
        |name| name == "owned",
        &mut too_small,
        4,
        usize::MAX,
    )
    .unwrap();
    let error = super::planning::preflight_exact_post_flush_stage_dirs_global(
        &second_parent,
        |name| name == "owned",
        &mut too_small,
        4,
        usize::MAX,
    )
    .expect_err("the second parent's descendant must consume the same global cap");
    assert!(error.to_string().contains("3-entry global work bound"));
    assert_eq!(std::fs::read(first.join("one")).unwrap(), b"one");
    assert_eq!(std::fs::read(second.join("two")).unwrap(), b"two");

    let mut exact = crate::engine::fs_utils::RecoveryNamespaceBudget::new(4);
    super::planning::preflight_exact_post_flush_stage_dirs_global(
        &first_parent,
        |name| name == "owned",
        &mut exact,
        4,
        usize::MAX,
    )
    .unwrap();
    super::planning::preflight_exact_post_flush_stage_dirs_global(
        &second_parent,
        |name| name == "owned",
        &mut exact,
        4,
        usize::MAX,
    )
    .expect("the exact shared namespace cap must succeed");
}

#[cfg(unix)]
#[test]
fn planning_rejects_symlinked_owned_directory_namespaces() {
    use std::os::unix::fs::symlink;

    type OwnedDirectoryPath = fn(&Path) -> PathBuf;
    let cases: [(&str, OwnedDirectoryPath); 5] = [
        ("numeric lane", |data_path| {
            data_path.join(NUMERIC_LANE_ROOT)
        }),
        ("WAL", |data_path| data_path.join(WAL_DIR_NAME)),
        ("rollups", |data_path| {
            data_path.join(rollups::ROLLUP_DIR_NAME)
        }),
        ("registry catalog", |data_path| {
            registry_catalog::catalog_store_path(&data_path.join(SERIES_INDEX_FILE_NAME))
        }),
        ("tombstones", |data_path| {
            data_path.join(NUMERIC_LANE_ROOT).join(format!(
                "{}.store",
                crate::engine::tombstone::TOMBSTONES_FILE_NAME
            ))
        }),
    ];

    for (case_name, owned_path) in cases {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().join("data");
        let external_path = temp_dir.path().join("external");
        std::fs::create_dir_all(&external_path).unwrap();
        std::fs::write(external_path.join("sentinel"), b"outside").unwrap();

        let symlink_path = owned_path(&data_path);
        std::fs::create_dir_all(symlink_path.parent().unwrap()).unwrap();
        symlink(&external_path, &symlink_path).unwrap();

        let err = planning_error(&startup_builder(&data_path));
        assert!(
            matches!(err, TsinkError::InvalidConfiguration(ref message)
                if message.contains("managed directory") && message.contains("directory")),
            "unexpected {case_name} planning error: {err:?}"
        );
        assert_eq!(
            std::fs::read(external_path.join("sentinel")).unwrap(),
            b"outside",
            "{case_name} validation must not mutate the symlink target"
        );
    }
}

#[cfg(unix)]
#[test]
fn planning_rejects_symlinked_owned_files_before_loading_them() {
    use std::os::unix::fs::symlink;

    type OwnedFilePath = fn(&Path) -> PathBuf;
    let cases: [(&str, OwnedFilePath); 8] = [
        ("series snapshot", |data_path| {
            data_path.join(SERIES_INDEX_FILE_NAME)
        }),
        ("legacy series delta", |data_path| {
            SeriesRegistry::incremental_path(&data_path.join(SERIES_INDEX_FILE_NAME))
        }),
        ("registry catalog", |data_path| {
            registry_catalog::catalog_path(&data_path.join(SERIES_INDEX_FILE_NAME))
        }),
        ("segment catalog", |data_path| {
            data_path.join(tiering::SEGMENT_CATALOG_FILE_NAME)
        }),
        ("rollup policies", |data_path| {
            data_path
                .join(rollups::ROLLUP_DIR_NAME)
                .join("policies.json")
        }),
        ("rollup state", |data_path| {
            data_path.join(rollups::ROLLUP_DIR_NAME).join("state.json")
        }),
        ("tombstone manifest", |data_path| {
            data_path
                .join(NUMERIC_LANE_ROOT)
                .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME)
        }),
        ("WAL publication marker", |data_path| {
            data_path.join(WAL_DIR_NAME).join("wal.published")
        }),
    ];

    for (case_name, owned_path) in cases {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().join("data");
        let external_file = temp_dir.path().join("external-file");
        std::fs::write(&external_file, b"outside").unwrap();

        let symlink_path = owned_path(&data_path);
        std::fs::create_dir_all(symlink_path.parent().unwrap()).unwrap();
        symlink(&external_file, &symlink_path).unwrap();

        let err = planning_error(&startup_builder(&data_path));
        assert!(
            matches!(err, TsinkError::InvalidConfiguration(ref message)
                if message.contains("managed file") && message.contains(&symlink_path.display().to_string())),
            "unexpected {case_name} planning error: {err:?}"
        );
        assert_eq!(
            std::fs::read(&external_file).unwrap(),
            b"outside",
            "{case_name} validation must not read-modify-write the symlink target"
        );
    }
}

#[cfg(unix)]
#[test]
fn planning_rejects_a_symlinked_data_path_lock_file() {
    use std::os::unix::fs::symlink;

    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let external_file = temp_dir.path().join("external-lock-target");
    std::fs::create_dir_all(&data_path).unwrap();
    std::fs::write(&external_file, b"outside").unwrap();
    symlink(&external_file, data_path.join(".tsink.lock")).unwrap();

    let err = planning_error(&startup_builder(&data_path));
    assert!(matches!(err, TsinkError::InvalidConfiguration(message)
        if message.contains("data path lock must be a regular file")));
    assert_eq!(std::fs::read(&external_file).unwrap(), b"outside");
}

#[test]
fn planning_without_orphans_keeps_the_initial_single_reconciliation() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");

    let plan = StartupPlanningPhase::prepare(&startup_builder(&data_path)).unwrap();
    assert_eq!(
        plan.local_disk_budget()
            .expect("persistent startup should own a disk budget")
            .snapshot()
            .reconciliations_total,
        1
    );
}

#[test]
fn compute_only_planning_does_not_recover_or_clean_an_unleased_writer_path() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("writer-data");
    let object_store_path = temp_dir.path().join("object-store");
    let marker_dir = data_path.join(super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME);
    let marker_path = marker_dir.join("transaction-0000000000000001-0000000000000002.json");
    std::fs::create_dir_all(&marker_dir).unwrap();
    std::fs::write(&marker_path, b"writer-owned-pending-marker").unwrap();

    let staged = object_store_path
        .join("hot")
        .join("numeric")
        .join("segments")
        .join("L0")
        .join(".tmp-tsink-post-flush-stage-copy-seg-0000000000000003-0000000000000004");
    std::fs::create_dir_all(&staged).unwrap();
    std::fs::write(staged.join("writer-owned"), b"keep").unwrap();

    let builder = startup_builder(&data_path)
        .with_object_store_path(&object_store_path)
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly);
    let _plan = StartupPlanningPhase::prepare(&builder)
        .expect("compute-only planning must not parse or mutate an unleased writer path");

    assert_eq!(
        std::fs::read(&marker_path).unwrap(),
        b"writer-owned-pending-marker"
    );
    assert_eq!(std::fs::read(staged.join("writer-owned")).unwrap(), b"keep");
}

#[test]
fn planning_reclaims_exact_post_flush_recovery_blockers_before_parsing_a_marker() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let marker_dir = data_path.join(super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME);
    let marker_name = "transaction-0000000000000001-0000000000000002.json";
    let marker_path = marker_dir.join(marker_name);
    std::fs::create_dir_all(&marker_dir).unwrap();
    std::fs::write(&marker_path, b"intentionally-invalid-marker").unwrap();

    let atomic_temp = marker_dir.join(format!(".{marker_name}.tmp-123-0000000000000003"));
    std::fs::write(&atomic_temp, vec![b'x'; 1024 * 1024]).unwrap();
    let atomic_lookalike = marker_dir.join(format!(".{marker_name}.tmp-123-000000000000000A"));
    std::fs::write(&atomic_lookalike, b"keep-atomic-lookalike").unwrap();

    let numeric_lane = data_path.join(NUMERIC_LANE_ROOT);
    let copy_stage = numeric_lane
        .join("segments")
        .join("L0")
        .join(".tmp-tsink-post-flush-stage-copy-seg-0000000000000004-0000000000000005");
    std::fs::create_dir_all(&copy_stage).unwrap();
    std::fs::write(
        copy_stage.join("large-crash-debris"),
        vec![b'y'; 1024 * 1024],
    )
    .unwrap();
    let copy_lookalike = numeric_lane
        .join("segments")
        .join("L0")
        .join(".tmp-tsink-post-flush-stage-copy-seg-0000000000000004-000000000000000A");
    std::fs::create_dir_all(&copy_lookalike).unwrap();
    std::fs::write(copy_lookalike.join("keep"), b"keep-copy-lookalike").unwrap();

    let rewrite_stage =
        data_path.join(".tmp-tsink-post-flush-retention-rewrite-lane_numeric-0000000000000006");
    std::fs::create_dir_all(&rewrite_stage).unwrap();
    std::fs::write(
        rewrite_stage.join("large-crash-debris"),
        vec![b'z'; 1024 * 1024],
    )
    .unwrap();
    let rewrite_lookalike =
        data_path.join(".tmp-tsink-post-flush-retention-rewrite-lane_numeric-000000000000000A");
    std::fs::create_dir_all(&rewrite_lookalike).unwrap();
    std::fs::write(rewrite_lookalike.join("keep"), b"keep-rewrite-lookalike").unwrap();

    let error = planning_error(&startup_builder(&data_path));
    assert!(matches!(error, TsinkError::Json(_)));
    assert!(!atomic_temp.exists());
    assert!(!copy_stage.exists());
    assert!(!rewrite_stage.exists());
    assert_eq!(
        std::fs::read(&atomic_lookalike).unwrap(),
        b"keep-atomic-lookalike"
    );
    assert_eq!(
        std::fs::read(copy_lookalike.join("keep")).unwrap(),
        b"keep-copy-lookalike"
    );
    assert_eq!(
        std::fs::read(rewrite_lookalike.join("keep")).unwrap(),
        b"keep-rewrite-lookalike"
    );
    assert_eq!(
        std::fs::read(&marker_path).unwrap(),
        b"intentionally-invalid-marker"
    );
}

#[test]
fn full_startup_memory_rejection_preserves_all_blockers_until_exact_threshold() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let marker_dir = data_path.join(super::super::maintenance::POST_FLUSH_REPLACEMENT_DIR_NAME);
    let marker_name = "transaction-0000000000000001-0000000000000002.json";
    std::fs::create_dir_all(&marker_dir).unwrap();
    let atomic_temp = marker_dir.join(format!(".{marker_name}.tmp-123-0000000000000003"));
    std::fs::write(&atomic_temp, b"atomic-temp").unwrap();

    let stage = data_path
        .join(NUMERIC_LANE_ROOT)
        .join("segments/L0")
        .join(".tmp-tsink-post-flush-stage-copy-seg-0000000000000004-0000000000000005");
    std::fs::create_dir_all(&stage).unwrap();
    for index in 0..512u32 {
        std::fs::write(stage.join(format!("owned-{index:04x}")), b"stage").unwrap();
    }
    let unknown_dir = data_path.join("operator-tree");
    std::fs::create_dir_all(&unknown_dir).unwrap();
    for index in 0..512u32 {
        std::fs::write(unknown_dir.join(format!("opaque-{index:04x}")), b"keep").unwrap();
    }

    let mut reconcile_limit = 0usize;
    loop {
        match crate::LocalDiskBudget::open_with_startup_memory_limit(
            &data_path,
            crate::LocalDiskLimits::default(),
            reconcile_limit,
        ) {
            Ok(_) => break,
            Err(TsinkError::MemoryBudgetExceeded { required, .. }) => {
                assert!(required > reconcile_limit);
                reconcile_limit = required;
            }
            Err(err) => panic!("unexpected reconciliation preflight error: {err}"),
        }
    }

    let mut exact_startup_limit = reconcile_limit;
    loop {
        let builder = startup_builder(&data_path).with_memory_limit(exact_startup_limit);
        match builder.build() {
            Ok(storage) => {
                storage.close().unwrap();
                break;
            }
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, exact_startup_limit);
                assert!(required > exact_startup_limit);
                assert!(atomic_temp.is_file());
                assert!(stage.is_dir());
                assert_eq!(std::fs::read(stage.join("owned-0000")).unwrap(), b"stage");
                exact_startup_limit = required;
            }
            Err(err) => panic!("unexpected bounded full-startup error: {err}"),
        }
    }
    assert!(exact_startup_limit > reconcile_limit);
    assert!(!atomic_temp.exists());
    assert!(!stage.exists());
    for index in 0..512u32 {
        assert_eq!(
            std::fs::read(unknown_dir.join(format!("opaque-{index:04x}"))).unwrap(),
            b"keep"
        );
    }
}

#[test]
fn full_startup_memory_rejection_preserves_generic_orphans_across_categories() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();
    let atomic_temp = data_path.join(".series_index.bin.tmp-123-0000000000000001");
    std::fs::write(&atomic_temp, b"atomic").unwrap();
    let segment_stage = data_path
        .join(NUMERIC_LANE_ROOT)
        .join("segments/L0/.tmp-seg-0000000000000002");
    std::fs::create_dir_all(&segment_stage).unwrap();
    for index in 0..256u32 {
        std::fs::write(segment_stage.join(format!("entry-{index:04x}")), b"stage").unwrap();
    }

    let mut memory_limit = 0usize;
    loop {
        let builder = startup_builder(&data_path).with_memory_limit(memory_limit);
        match builder.build() {
            Ok(storage) => {
                storage.close().unwrap();
                break;
            }
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, memory_limit);
                assert!(required > memory_limit);
                assert!(atomic_temp.is_file());
                assert!(segment_stage.is_dir());
                assert_eq!(
                    std::fs::read(segment_stage.join("entry-0000")).unwrap(),
                    b"stage"
                );
                memory_limit = required;
            }
            Err(err) => panic!("unexpected generic startup-cleanup error: {err}"),
        }
    }
    assert!(!atomic_temp.exists());
    assert!(!segment_stage.exists());
}

#[test]
fn planning_cleans_exact_owned_orphans_before_enforcing_future_growth() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let delta_dir = data_path.join("series_index.delta.d");
    let rollup_dir = data_path.join(rollups::ROLLUP_DIR_NAME);
    let tombstone_shards = lane_path.join("tombstones.json.store").join("shards");
    let replacement_dir = lane_path.join(".compaction-replacements");
    let segment_level = lane_path.join("segments").join("L0");
    for directory in [
        &data_path,
        &delta_dir,
        &rollup_dir,
        &tombstone_shards,
        &replacement_dir,
        &segment_level,
    ] {
        std::fs::create_dir_all(directory).unwrap();
    }

    let owned_files = [
        data_path.join(".series_index.bin.tmp-123-0000000000000001"),
        data_path.join(".series_index.catalog.json.tmp-123-0000000000000002"),
        data_path.join(".segment_catalog.json.tmp-123-0000000000000003"),
        rollup_dir.join(".policies.json.tmp-123-0000000000000004"),
        rollup_dir.join(".state.json.tmp-123-0000000000000005"),
        lane_path.join(".tombstones.json.tmp-123-0000000000000006"),
        delta_dir.join(".delta-0000000000000007.bin.tmp-123-0000000000000008"),
        delta_dir.join(".journal-active.bin.tmp-123-0000000000000015"),
        tombstone_shards.join(".shard-007-0000000000000009.bin.tmp-123-000000000000000a"),
        tombstone_shards.join("shard-007-000000000000000f.bin"),
        replacement_dir
            .join(".replace-000000000000000b-000000000000000c.json.tmp-123-000000000000000d"),
    ];
    for path in &owned_files {
        std::fs::write(path, b"stale-owned-bytes").unwrap();
    }
    let stale_segment = segment_level.join(".tmp-seg-000000000000000e");
    std::fs::create_dir_all(&stale_segment).unwrap();
    std::fs::write(stale_segment.join("chunks.bin"), b"stale-segment-bytes").unwrap();

    let unknown = data_path.join("operator-note.bin");
    let atomic_lookalike = data_path.join(".series_index.bin.tmp-manual");
    let marker_lookalike =
        replacement_dir.join(".replace-not-a-marker.json.tmp-123-000000000000000f");
    let pending_marker = replacement_dir.join("replace-0000000000000010-0000000000000011.json");
    let segment_lookalike = segment_level.join(".tmp-seg-not-a-segment-id");
    let uppercase_atomic_nonce = data_path.join(".series_index.bin.tmp-123-000000000000000A");
    let uppercase_registry_target =
        delta_dir.join(".delta-000000000000000A.bin.tmp-123-0000000000000012");
    let out_of_range_shard =
        tombstone_shards.join(".shard-256-0000000000000013.bin.tmp-123-0000000000000014");
    let uppercase_segment_staging = segment_level.join(".tmp-seg-000000000000000A");
    std::fs::write(&unknown, b"unknown").unwrap();
    std::fs::write(&atomic_lookalike, b"keep-atomic-lookalike").unwrap();
    std::fs::write(&marker_lookalike, b"keep-marker-lookalike").unwrap();
    std::fs::write(&pending_marker, b"pending-recovery-marker").unwrap();
    std::fs::write(&uppercase_atomic_nonce, b"keep-uppercase-nonce").unwrap();
    std::fs::write(&uppercase_registry_target, b"keep-uppercase-target").unwrap();
    std::fs::write(&out_of_range_shard, b"keep-shard-256").unwrap();
    std::fs::create_dir_all(&segment_lookalike).unwrap();
    std::fs::write(segment_lookalike.join("note"), b"keep-segment-lookalike").unwrap();
    std::fs::create_dir_all(&uppercase_segment_staging).unwrap();
    std::fs::write(
        uppercase_segment_staging.join("note"),
        b"keep-uppercase-segment",
    )
    .unwrap();
    let expected_remaining = [
        std::fs::metadata(&unknown).unwrap().len(),
        std::fs::metadata(&atomic_lookalike).unwrap().len(),
        std::fs::metadata(&marker_lookalike).unwrap().len(),
        std::fs::metadata(&pending_marker).unwrap().len(),
        std::fs::metadata(&uppercase_atomic_nonce).unwrap().len(),
        std::fs::metadata(&uppercase_registry_target).unwrap().len(),
        std::fs::metadata(&out_of_range_shard).unwrap().len(),
        std::fs::metadata(segment_lookalike.join("note"))
            .unwrap()
            .len(),
        std::fs::metadata(uppercase_segment_staging.join("note"))
            .unwrap()
            .len(),
    ]
    .into_iter()
    .sum::<u64>();

    let builder = startup_builder(&data_path).with_local_disk_limit(expected_remaining);
    let plan = StartupPlanningPhase::prepare(&builder)
        .expect("recovery cleanup must run even when stale bytes initially exceed the quota");

    for path in &owned_files {
        assert!(!path.exists(), "owned orphan survived: {}", path.display());
    }
    assert!(!stale_segment.exists());
    assert!(unknown.is_file());
    assert!(atomic_lookalike.is_file());
    assert!(marker_lookalike.is_file());
    assert!(pending_marker.is_file());
    assert!(uppercase_atomic_nonce.is_file());
    assert!(uppercase_registry_target.is_file());
    assert!(out_of_range_shard.is_file());
    assert!(segment_lookalike.is_dir());
    assert!(uppercase_segment_staging.is_dir());

    let snapshot = plan.local_disk_budget().unwrap().snapshot();
    assert_eq!(snapshot.accounted_bytes, expected_remaining);
    assert_eq!(snapshot.limits.max_bytes, Some(expected_remaining));
    assert!(!snapshot.over_limit);
}

#[test]
fn planning_removes_only_unreferenced_owned_tombstone_shards() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let tombstones_path = lane_path.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME);
    let expected = HashMap::from([(
        1,
        vec![crate::engine::tombstone::TombstoneRange { start: 10, end: 20 }],
    )]);
    crate::engine::tombstone::persist_tombstone_updates(&tombstones_path, &expected).unwrap();
    let referenced = crate::engine::tombstone::referenced_tombstone_shard_files(&tombstones_path)
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(referenced.len(), 1);

    let shards_dir =
        crate::engine::tombstone::tombstone_store_sidecar_path(&tombstones_path).join("shards");
    let orphan = shards_dir.join("shard-007-ffffffffffffffff.bin");
    let reserved_lookalike = shards_dir.join("shard-007-operator-note.bin");
    std::fs::write(&orphan, b"fully-published-but-unreferenced").unwrap();
    std::fs::write(&reserved_lookalike, b"not-an-owned-shard-name").unwrap();

    let plan = StartupPlanningPhase::prepare(&startup_builder(&data_path))
        .expect("startup should safely reclaim only exact unreferenced shard generations");
    assert!(!orphan.exists());
    assert_eq!(
        std::fs::read(&reserved_lookalike).unwrap(),
        b"not-an-owned-shard-name"
    );
    for file_name in referenced {
        assert!(shards_dir.join(file_name).is_file());
    }
    assert_eq!(
        crate::engine::tombstone::load_tombstones(&tombstones_path).unwrap(),
        expected
    );
    let snapshot = plan.local_disk_budget().unwrap().snapshot();
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.active_reservations, 0);
}

#[test]
fn planning_rejects_wal_sublimit_above_normal_disk_growth_capacity() {
    let temp_dir = TempDir::new().unwrap();
    let valid_builder = startup_builder(&temp_dir.path().join("valid"))
        .with_local_disk_limit(1_000)
        .with_maintenance_temp_reserve(200)
        .with_wal_size_limit(800);
    StartupPlanningPhase::prepare(&valid_builder).unwrap();

    let invalid_builder = startup_builder(&temp_dir.path().join("invalid"))
        .with_local_disk_limit(1_000)
        .with_maintenance_temp_reserve(200)
        .with_wal_size_limit(801);
    let err = planning_error(&invalid_builder);
    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message)
            if message.contains("WAL size limit 801 exceeds local disk growth capacity 800")
    ));
}

#[test]
fn planning_rejects_object_store_nested_under_the_managed_data_root() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let builder = startup_builder(&data_path).with_object_store_path(data_path.join("remote"));

    let err = planning_error(&builder);
    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message)
            if message.contains("object store path")
                && message.contains("must be outside managed local data path")
    ));
}

#[cfg(unix)]
#[test]
fn planning_rejects_lexically_nested_object_store_symlink_even_when_target_is_external() {
    use std::os::unix::fs::symlink;

    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let external_path = temp_dir.path().join("external-object-store");
    std::fs::create_dir_all(&data_path).unwrap();
    std::fs::create_dir_all(&external_path).unwrap();
    std::fs::write(external_path.join("sentinel"), b"outside").unwrap();
    let nested_alias = data_path.join("remote");
    symlink(&external_path, &nested_alias).unwrap();

    let builder = startup_builder(&data_path).with_object_store_path(&nested_alias);
    let err = planning_error(&builder);
    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message)
            if message.contains("object store path")
                && message.contains("must be outside managed local data path")
    ));
    assert_eq!(
        std::fs::read(external_path.join("sentinel")).unwrap(),
        b"outside"
    );
}

#[test]
fn recovery_phase_rebuilds_registry_from_segments_when_checkpoint_load_fails() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "startup_phase_registry_rebuild";
    let labels = vec![Label::new("host", "startup")];

    {
        let storage = startup_builder(temp_dir.path()).build().unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels(metric, labels, DataPoint::new(2, 2.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let mut checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    checkpoint_bytes[0] ^= 0xff;
    std::fs::write(&checkpoint_path, checkpoint_bytes).unwrap();

    let builder = startup_builder(temp_dir.path());
    let plan = StartupPlanningPhase::prepare(&builder).unwrap();
    let discovered = StartupDiscoveryPhase::discover(&builder, &plan).unwrap();
    let recovered = StartupRecoveryPhase::recover(&plan, discovered).unwrap();
    let finalize_state = recovered.finalize_state();

    assert!(finalize_state.force_registry_checkpoint_after_startup);
    assert!(!finalize_state.reconcile_registry_with_persisted);
    assert!(recovered
        .loaded_segments()
        .series
        .iter()
        .any(|series| series.metric == metric
            && series.labels == vec![Label::new("host", "startup")]));
}

#[test]
fn hydration_memory_rejection_precedes_corrupt_segment_quarantine() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let mut random_state = 0x9e37_79b9_u32;
    let payload = (0..4 * 1024 * 1024)
        .map(|_| {
            random_state ^= random_state << 13;
            random_state ^= random_state >> 17;
            random_state ^= random_state << 5;
            random_state as u8
        })
        .collect::<Vec<_>>();
    {
        let storage = startup_builder(&data_path).build().unwrap();
        storage
            .insert_rows(&[Row::new(
                "startup_hydration_blob",
                DataPoint::new(1, payload),
            )])
            .unwrap();
        storage.close().unwrap();
    }
    assert!(
        !crate::engine::segment::list_segment_dirs(data_path.join(BLOB_LANE_ROOT))
            .unwrap()
            .is_empty()
    );

    let corrupt_root = data_path
        .join(NUMERIC_LANE_ROOT)
        .join("segments/L0/seg-fffffffffffffffe");
    std::fs::create_dir_all(&corrupt_root).unwrap();
    let corrupt_manifest = corrupt_root.join("manifest.bin");
    std::fs::write(&corrupt_manifest, b"invalid-manifest-sentinel").unwrap();

    let mut memory_limit = 1usize;
    let mut saw_hydration_rejection = false;
    for _ in 0..32 {
        let builder = startup_builder(&data_path).with_memory_limit(memory_limit);
        let plan = match StartupPlanningPhase::prepare(&builder) {
            Ok(plan) => plan,
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, memory_limit);
                assert!(required > memory_limit);
                memory_limit = required;
                continue;
            }
            Err(err) => panic!("unexpected bounded startup planning error: {err}"),
        };
        match StartupDiscoveryPhase::discover(&builder, &plan) {
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, memory_limit);
                assert!(required > memory_limit);
                saw_hydration_rejection = true;
                break;
            }
            Err(err) => panic!("unexpected bounded startup discovery error: {err}"),
            Ok(_) => panic!("expected persisted hydration to exceed the planning-only threshold"),
        }
    }

    assert!(saw_hydration_rejection);
    assert!(corrupt_root.is_dir());
    assert_eq!(
        std::fs::read(&corrupt_manifest).unwrap(),
        b"invalid-manifest-sentinel"
    );
}

#[test]
fn inventory_state_apply_quarantines_removes_roots_from_visible_inventory() {
    let keep_root = PathBuf::from("/tmp/bootstrap-keep");
    let bad_root = PathBuf::from("/tmp/bootstrap-bad");
    let mut inventory = StartupInventoryState {
        segment_inventory: SegmentInventory::from_entries(vec![
            inventory_entry(keep_root.clone(), 1),
            inventory_entry(bad_root.clone(), 2),
        ]),
        startup_quarantined: Vec::new(),
    };

    let changed = inventory.apply_quarantines(vec![StartupQuarantinedSegment {
        original_root: bad_root.clone(),
        quarantined: QuarantinedSegmentRoot {
            path: PathBuf::from("/tmp/bootstrap-quarantine"),
            sync_failed: false,
        },
        details: "manifest crc32 mismatch".to_string(),
    }]);

    assert!(changed);
    assert_eq!(
        inventory.segment_inventory.root_set(),
        BTreeSet::from([keep_root])
    );
    assert_eq!(inventory.startup_quarantined.len(), 1);
    assert_eq!(inventory.startup_quarantined[0].original_root, bad_root);
}

#[test]
fn wal_open_phase_applies_replay_highwater_floor() {
    let temp_dir = TempDir::new().unwrap();
    let builder = startup_builder(temp_dir.path());
    let plan = StartupPlanningPhase::prepare(&builder).unwrap();

    let wal = StartupWalOpenPhase::open(
        &builder,
        &plan,
        WalHighWatermark {
            segment: 4,
            frame: 9,
        },
    )
    .unwrap()
    .expect("startup plan should include a WAL path");

    assert_eq!(
        wal.current_highwater(),
        WalHighWatermark {
            segment: 4,
            frame: 9,
        }
    );
    assert_eq!(
        wal.current_durable_highwater(),
        WalHighWatermark {
            segment: 4,
            frame: 9,
        }
    );
    assert_eq!(
        wal.current_published_highwater(),
        WalHighWatermark {
            segment: 4,
            frame: 9,
        }
    );
}

#[test]
fn finalize_state_actions_cover_checkpoint_persist_and_maintenance_modes() {
    let rebuild_actions = StartupFinalizeState {
        force_registry_checkpoint_after_startup: true,
        reconcile_registry_with_persisted: false,
    }
    .actions(false, false);
    assert_eq!(
        rebuild_actions.registry_persistence,
        RegistryPersistenceAction::Checkpoint
    );
    assert!(rebuild_actions.run_sync_maintenance);
    assert!(!rebuild_actions.start_background_threads);
    assert!(!rebuild_actions.schedule_startup_maintenance);

    let reconciled_actions = StartupFinalizeState {
        force_registry_checkpoint_after_startup: false,
        reconcile_registry_with_persisted: true,
    }
    .actions(true, false);
    assert_eq!(
        reconciled_actions.registry_persistence,
        RegistryPersistenceAction::Persist
    );
    assert!(!reconciled_actions.run_sync_maintenance);
    assert!(reconciled_actions.start_background_threads);
    assert!(reconciled_actions.schedule_startup_maintenance);

    let fail_fast_actions = StartupFinalizeState {
        force_registry_checkpoint_after_startup: false,
        reconcile_registry_with_persisted: true,
    }
    .actions(true, true);
    assert!(fail_fast_actions.run_sync_maintenance);
    assert!(fail_fast_actions.start_background_threads);
    assert!(!fail_fast_actions.schedule_startup_maintenance);
}
