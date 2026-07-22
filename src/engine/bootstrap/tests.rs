use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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
        TsinkError::Io(_) | TsinkError::IoWithPath { .. }
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

#[cfg(unix)]
#[test]
fn planning_rejects_symlinked_owned_directory_namespaces() {
    use std::os::unix::fs::symlink;

    type OwnedDirectoryPath = fn(&Path) -> PathBuf;
    let cases: [(&str, OwnedDirectoryPath); 4] = [
        ("numeric lane", |data_path| {
            data_path.join(NUMERIC_LANE_ROOT)
        }),
        ("WAL", |data_path| data_path.join(WAL_DIR_NAME)),
        ("rollups", |data_path| {
            data_path.join(rollups::ROLLUP_DIR_NAME)
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
