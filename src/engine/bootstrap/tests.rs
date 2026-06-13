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
