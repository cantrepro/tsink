use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

fn create_valid_restore_snapshot(path: &Path) {
    static FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(1);
    let source = path.parent().unwrap().join(format!(
        ".restore-fixture-source-{:016x}",
        FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let storage = startup_builder(&source)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new("restore_fixture_metric", DataPoint::new(1, 1.0))])
        .unwrap();
    storage.snapshot(path).unwrap();
    storage.close().unwrap();
    std::fs::remove_dir_all(source).unwrap();
}

fn create_semantic_restore_snapshot(path: &Path) {
    let source = path.parent().unwrap().join(".semantic-restore-source");
    let storage = startup_builder(&source)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[
            Row::new("semantic_restore_metric", DataPoint::new(1, 1.0)),
            Row::new("semantic_restore_metric", DataPoint::new(2, 2.0)),
            Row::new("semantic_restore_metric", DataPoint::new(3, 3.0)),
        ])
        .unwrap();
    storage
        .apply_rollup_policies(vec![crate::RollupPolicy {
            id: "semantic-restore-rollup".to_string(),
            metric: "semantic_restore_metric".to_string(),
            match_labels: Vec::new(),
            interval: 2,
            aggregation: crate::Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();
    storage
        .delete_series(
            &crate::SeriesSelection::new()
                .with_metric("semantic_restore_metric")
                .with_time_range(2, 3),
        )
        .unwrap();
    storage.snapshot(path).unwrap();
    storage.close().unwrap();
    std::fs::remove_dir_all(source).unwrap();
}

fn create_persisted_segment_restore_snapshot(path: &Path) {
    let source = path.parent().unwrap().join(".segment-restore-source");
    {
        let storage = startup_builder(&source)
            .with_background_threads_enabled_for_tests(false)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::new("segment_restore_metric", DataPoint::new(1, 1.0)),
                Row::new("segment_restore_metric", DataPoint::new(2, 2.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }
    let reopened = startup_builder(&source)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    reopened.snapshot(path).unwrap();
    reopened.close().unwrap();
    std::fs::remove_dir_all(source).unwrap();
}

fn directory_image(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, directory: &Path, image: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.file_type().is_dir() {
                image.insert(relative, None);
                visit(root, &path, image);
            } else if metadata.file_type().is_file() {
                image.insert(relative, Some(std::fs::read(path).unwrap()));
            } else {
                panic!("test tree contains a non-plain entry: {}", path.display());
            }
        }
    }

    let mut image = BTreeMap::new();
    visit(root, root, &mut image);
    image
}

fn seed_existing_restore_target(target: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    std::fs::create_dir_all(target.join("nested")).unwrap();
    std::fs::write(target.join("old"), b"old-state").unwrap();
    std::fs::write(target.join("nested/more"), b"more-old-state").unwrap();
    directory_image(target)
}

fn find_descendant_named(root: &Path, name: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(root).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if entry.file_name() == std::ffi::OsStr::new(name) {
            return Some(path);
        }
        if entry.file_type().ok()?.is_dir() {
            if let Some(found) = find_descendant_named(&path, name) {
                return Some(found);
            }
        }
    }
    None
}

fn replace_first_chunk_payload_with_invalid_encoded_payload(segment_root: &Path) {
    const CHUNKS_FILE_HEADER_LEN: usize = 16;
    const CHUNK_RECORD_FIXED_HEADER_LEN: usize = 4 + 4 + 34;
    const CHUNK_FLAGS_OFFSET: usize = CHUNKS_FILE_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1;
    const PAYLOAD_LEN_OFFSET: usize =
        CHUNKS_FILE_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1 + 1 + 2 + 8 + 8;
    const MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET: usize = 96 + 12;

    let chunks_path = segment_root.join("chunks.bin");
    let mut chunks_bytes = std::fs::read(&chunks_path).unwrap();
    assert_eq!(chunks_bytes[CHUNK_FLAGS_OFFSET], 0);
    let payload_len = u32::from_le_bytes(
        chunks_bytes[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let payload_offset = CHUNKS_FILE_HEADER_LEN + CHUNK_RECORD_FIXED_HEADER_LEN;
    let payload_end = payload_offset + payload_len;
    let invalid_ts_len = u32::try_from(payload_len).unwrap().saturating_add(1);
    chunks_bytes[payload_offset..payload_offset + 4].copy_from_slice(&invalid_ts_len.to_le_bytes());
    let payload_crc = crate::engine::binio::checksum32(&chunks_bytes[payload_offset..payload_end]);
    chunks_bytes[payload_end..payload_end + 4].copy_from_slice(&payload_crc.to_le_bytes());
    std::fs::write(&chunks_path, &chunks_bytes).unwrap();

    let manifest_path = segment_root.join("manifest.bin");
    let mut manifest_bytes = std::fs::read(&manifest_path).unwrap();
    let chunks_hash = xxhash_rust::xxh64::xxh64(&chunks_bytes, 0);
    manifest_bytes
        [MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET..MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET + 8]
        .copy_from_slice(&chunks_hash.to_le_bytes());
    let crc_offset = manifest_bytes.len() - 4;
    let manifest_crc = crate::engine::binio::checksum32(&manifest_bytes[..crc_offset]);
    manifest_bytes[crc_offset..].copy_from_slice(&manifest_crc.to_le_bytes());
    std::fs::write(manifest_path, manifest_bytes).unwrap();
}

fn replace_first_samples_series_id_with_unknown(snapshot: &Path) {
    let wal_file = std::fs::read_dir(snapshot.join(WAL_DIR_NAME))
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "wal.log" || name.ends_with(".log"))
        })
        .unwrap();
    let mut bytes = std::fs::read(&wal_file).unwrap();
    let mut offset = 0usize;
    while offset + 24 <= bytes.len() {
        assert_eq!(&bytes[offset..offset + 4], b"TSFR");
        let payload_len =
            u32::from_le_bytes(bytes[offset + 16..offset + 20].try_into().unwrap()) as usize;
        let payload_start = offset + 24;
        let payload_end = payload_start + payload_len;
        assert!(payload_end <= bytes.len());
        if bytes[offset + 4] == 2 {
            assert!(payload_len >= 10);
            bytes[payload_start + 2..payload_start + 10].copy_from_slice(&u64::MAX.to_le_bytes());
            let checksum = crate::engine::binio::checksum32(&bytes[payload_start..payload_end]);
            bytes[offset + 20..offset + 24].copy_from_slice(&checksum.to_le_bytes());
            std::fs::write(wal_file, bytes).unwrap();
            return;
        }
        offset = payload_end;
    }
    panic!("fixture WAL contains no samples frame");
}

fn restore_backup_under(parent: &Path) -> Option<PathBuf> {
    std::fs::read_dir(parent)
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-backup-")
            })
        })
}

fn restore_target_is_published(parent: &Path, target: &Path) -> bool {
    target
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file()
        && restore_backup_under(parent).is_some()
}

fn restore_target_is_between_replacement_moves(parent: &Path, target: &Path) -> bool {
    !target.exists() && restore_backup_under(parent).is_some()
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
fn unbudgeted_restore_replaces_nonempty_target_and_cleans_exact_backup() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap();

    assert!(target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    assert!(!target_path.join("old").exists());
    assert!(std::fs::read_dir(temp_dir.path()).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        !name.to_string_lossy().starts_with(".tmp-tsink-restore-")
    }));
    let restored = startup_builder(&target_path)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    assert_eq!(
        restored
            .select("restore_fixture_metric", &[], 0, 2)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    restored.close().unwrap();
}

#[test]
fn production_validation_accepts_snapshot_with_tombstones_and_rollup_state() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_semantic_restore_snapshot(&snapshot_path);

    restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap();

    assert!(target_path
        .join(NUMERIC_LANE_ROOT)
        .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME)
        .is_file());
    assert!(target_path
        .join(rollups::ROLLUP_DIR_NAME)
        .join("state.json")
        .is_file());
    let restored = startup_builder(&target_path)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    assert_eq!(
        restored
            .select("semantic_restore_metric", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(3, 3.0)]
    );
    restored.close().unwrap();
}

#[test]
fn corrupt_wal_snapshot_is_rejected_before_existing_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    let wal_file = std::fs::read_dir(snapshot_path.join(WAL_DIR_NAME))
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "wal.log" || name.ends_with(".log"))
        })
        .expect("populated snapshot must contain a canonical WAL segment");
    let mut wal_bytes = std::fs::read(&wal_file).unwrap();
    assert!(!wal_bytes.is_empty());
    *wal_bytes.last_mut().unwrap() ^= 0x5a;
    std::fs::write(&wal_file, wal_bytes).unwrap();
    std::fs::create_dir(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err.to_string().contains("checksum mismatch"), "{err}");
    assert_eq!(
        std::fs::read(target_path.join("old")).unwrap(),
        b"old-state"
    );
    assert_eq!(std::fs::read_dir(&target_path).unwrap().count(), 1);
    assert!(std::fs::read_dir(temp_dir.path()).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        !name.to_string_lossy().starts_with(".tmp-tsink-restore-")
    }));
}

#[test]
fn checksum_valid_semantically_invalid_wal_is_rejected_with_target_and_source_unchanged() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    replace_first_samples_series_id_with_unknown(&snapshot_path);
    let source_before = directory_image(&snapshot_path);
    let target_before = seed_existing_restore_target(&target_path);

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err.to_string().contains("missing from registry"), "{err}");
    assert_eq!(directory_image(&snapshot_path), source_before);
    assert_eq!(directory_image(&target_path), target_before);
}

#[test]
fn budgeted_checksum_valid_semantically_invalid_wal_uses_the_same_prepublication_gate() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("budget-root");
    let target_path = budget_root.join("target");
    create_valid_restore_snapshot(&snapshot_path);
    replace_first_samples_series_id_with_unknown(&snapshot_path);
    let source_before = directory_image(&snapshot_path);
    let target_before = seed_existing_restore_target(&target_path);
    let budget =
        crate::LocalDiskBudget::open(&budget_root, crate::LocalDiskLimits::default()).unwrap();

    let err = restore_storage_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(err.to_string().contains("missing from registry"), "{err}");
    assert_eq!(directory_image(&snapshot_path), source_before);
    assert_eq!(directory_image(&target_path), target_before);
    assert_eq!(budget.snapshot().active_reservations, 0);
}

#[test]
fn production_invalid_persisted_segment_is_rejected_before_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_persisted_segment_restore_snapshot(&snapshot_path);
    let segment_root = find_descendant_named(&snapshot_path, "chunks.bin")
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    replace_first_chunk_payload_with_invalid_encoded_payload(&segment_root);
    let source_before = directory_image(&snapshot_path);
    let target_before = seed_existing_restore_target(&target_path);

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(
        err.to_string()
            .contains("persisted segment chunk payload cannot be decoded"),
        "{err}"
    );
    assert_eq!(directory_image(&snapshot_path), source_before);
    assert_eq!(directory_image(&target_path), target_before);
}

#[test]
fn corrupt_tombstone_shard_is_rejected_before_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_semantic_restore_snapshot(&snapshot_path);
    let shard_dir = snapshot_path
        .join(NUMERIC_LANE_ROOT)
        .join("tombstones.json.store")
        .join("shards");
    let shard = std::fs::read_dir(&shard_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.is_file())
        .expect("semantic fixture must contain a tombstone shard");
    let mut bytes = std::fs::read(&shard).unwrap();
    bytes[0] ^= 0x5a;
    std::fs::write(shard, bytes).unwrap();
    let target_before = seed_existing_restore_target(&target_path);

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err.to_string().contains("tombstone"), "{err}");
    assert_eq!(directory_image(&target_path), target_before);
}

#[test]
fn corrupt_rollup_state_is_rejected_before_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_semantic_restore_snapshot(&snapshot_path);
    let state_path = snapshot_path
        .join(rollups::ROLLUP_DIR_NAME)
        .join("state.json");
    let mut state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    state["magic"] = serde_json::json!("not-tsink-rollup-state");
    std::fs::write(state_path, serde_json::to_vec(&state).unwrap()).unwrap();
    let target_before = seed_existing_restore_target(&target_path);

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err.to_string().contains("rollup"), "{err}");
    assert_eq!(directory_image(&target_path), target_before);
}

#[test]
fn segment_catalog_parent_traversal_is_rejected_before_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::write(
        snapshot_path.join(tiering::SEGMENT_CATALOG_FILE_NAME),
        serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "entries": [{
                "lane": "numeric",
                "tier": "hot",
                "level": 0,
                "segment_id": 1,
                "chunk_count": 1,
                "point_count": 1,
                "series_count": 1,
                "min_ts": 1,
                "max_ts": 1,
                "wal_highwater_segment": 0,
                "wal_highwater_frame": 0,
                "relative_path": "../../escape"
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let target_before = seed_existing_restore_target(&target_path);

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err.to_string().contains("must stay within"), "{err}");
    assert_eq!(directory_image(&target_path), target_before);
}

#[test]
fn unsupported_catalog_snapshot_is_rejected_before_existing_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::write(
        snapshot_path.join("series_index.catalog.json"),
        br#"{"version":999,"segments":[]}"#,
    )
    .unwrap();
    std::fs::create_dir(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(
        err.to_string()
            .contains("catalog.local_version_unsupported"),
        "{err}"
    );
    assert_eq!(
        std::fs::read(target_path.join("old")).unwrap(),
        b"old-state"
    );
    assert_eq!(std::fs::read_dir(&target_path).unwrap().count(), 1);
}

#[test]
fn missing_manifest_snapshot_is_rejected_before_destination_ancestry_creation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("missing-parent/nested/target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::remove_file(
        snapshot_path.join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME),
    )
    .unwrap();

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(
        err.to_string().contains("missing required regular file"),
        "{err}"
    );
    assert!(!temp_dir.path().join("missing-parent").exists());
}

#[test]
fn valid_checksum_minimum_reader_too_new_is_rejected_without_target_mutation() {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct ManifestPayload {
        storage_format_version: u16,
        minimum_reader_storage_format_version: u16,
        creating_tsink_version: Option<String>,
        last_successfully_opened_tsink_version: Option<String>,
        format_affecting_features: Vec<String>,
        timestamp_precision: String,
        chunk_point_capacity: u32,
        partition_window_timestamp_units: i64,
    }

    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    let manifest_path =
        snapshot_path.join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME);
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let mut payload: ManifestPayload = serde_json::from_value(envelope["payload"].clone()).unwrap();
    payload.minimum_reader_storage_format_version = crate::engine::STORAGE_FORMAT_VERSION + 1;
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    envelope["payload"] = serde_json::to_value(payload).unwrap();
    envelope["payload_crc32"] = serde_json::json!(crate::engine::binio::checksum32(&payload_bytes));
    std::fs::write(&manifest_path, serde_json::to_vec(&envelope).unwrap()).unwrap();
    std::fs::create_dir(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err.to_string().contains("minimum reader format"), "{err}");
    assert_eq!(
        std::fs::read(target_path.join("old")).unwrap(),
        b"old-state"
    );
    assert_eq!(std::fs::read_dir(&target_path).unwrap().count(), 1);
}

#[test]
fn budgeted_corrupt_manifest_is_rejected_without_reservation_or_target_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let budget_root = temp_dir.path().join("budget");
    let target_path = budget_root.join("target");
    create_valid_restore_snapshot(&snapshot_path);
    let manifest_path =
        snapshot_path.join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME);
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let crc = envelope["payload_crc32"].as_u64().unwrap();
    envelope["payload_crc32"] = serde_json::json!(crc ^ 1);
    std::fs::write(&manifest_path, serde_json::to_vec(&envelope).unwrap()).unwrap();
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();
    let budget =
        crate::LocalDiskBudget::open(&budget_root, crate::LocalDiskLimits::default()).unwrap();

    let err = restore_storage_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(err.to_string().contains("checksum mismatch"), "{err}");
    assert_eq!(
        std::fs::read(target_path.join("old")).unwrap(),
        b"old-state"
    );
    assert_eq!(std::fs::read_dir(&target_path).unwrap().count(), 1);
    let accounting = budget.snapshot();
    assert_eq!(accounting.accounted_bytes, 9);
    assert_eq!(accounting.active_reservations, 0);
    assert_eq!(accounting.reserved_bytes, 0);
}

#[test]
fn budgeted_restore_admits_the_exact_semantic_validation_peak_and_rejects_one_byte_less() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    create_valid_restore_snapshot(&snapshot_path);
    let source_before = directory_image(&snapshot_path);
    let measurement = crate::engine::fs_utils::measure_restore_directory(&snapshot_path).unwrap();

    let probe = crate::LocalDiskBudget::open(
        temp_dir.path().join("allowance-probe"),
        crate::LocalDiskLimits::default(),
    )
    .unwrap();
    let entry_allowance = probe
        .snapshot_restore_entry_staging_allowance_bytes()
        .unwrap();
    let required_peak = measurement
        .staging_admission_bytes(entry_allowance)
        .unwrap()
        .checked_add(entry_allowance.checked_mul(2).unwrap())
        .and_then(|bytes| bytes.checked_add(measurement.logical_bytes))
        .unwrap();
    drop(probe);

    let below_root = temp_dir.path().join("below-root");
    let below_target = below_root.join("target");
    let below_budget = crate::LocalDiskBudget::open(
        &below_root,
        crate::LocalDiskLimits {
            max_bytes: Some(required_peak - 1),
            ..crate::LocalDiskLimits::default()
        },
    )
    .unwrap();
    let err = restore_storage_from_snapshot_with_disk_budget(
        &snapshot_path,
        &below_target,
        Arc::clone(&below_budget),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            TsinkError::DiskQuotaExceeded {
                limit,
                used: 0,
                reserved: 0,
                requested,
            } if limit == required_peak - 1 && requested == required_peak
        ),
        "unexpected one-byte-below admission error: {err}"
    );
    assert!(!below_target.exists());
    let below_accounting = below_budget.snapshot();
    assert_eq!(below_accounting.accounted_bytes, 0);
    assert_eq!(below_accounting.active_reservations, 0);
    assert_eq!(below_accounting.reserved_bytes, 0);
    assert_eq!(directory_image(&snapshot_path), source_before);

    let exact_root = temp_dir.path().join("exact-root");
    let exact_target = exact_root.join("target");
    let exact_budget = crate::LocalDiskBudget::open(
        &exact_root,
        crate::LocalDiskLimits {
            max_bytes: Some(required_peak),
            ..crate::LocalDiskLimits::default()
        },
    )
    .unwrap();
    restore_storage_from_snapshot_with_disk_budget(
        &snapshot_path,
        &exact_target,
        Arc::clone(&exact_budget),
    )
    .unwrap();
    assert!(exact_target
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    assert_eq!(directory_image(&snapshot_path), source_before);
    let exact_accounting = exact_budget.snapshot();
    assert_eq!(exact_accounting.accounted_bytes, measurement.logical_bytes);
    assert_eq!(exact_accounting.active_reservations, 0);
    assert_eq!(exact_accounting.reserved_bytes, 0);
}

#[test]
fn budgeted_restore_reconciles_visible_target_and_backup_after_postpublication_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    let target_path = budget_root.join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();
    let budget =
        crate::LocalDiskBudget::open(&budget_root, crate::LocalDiskLimits::default()).unwrap();

    let synchronized_root = budget.root().to_path_buf();
    let activation_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&activation_syncs);
    let activated_target = target_path.clone();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path != synchronized_root
                || !restore_target_is_published(&synchronized_root, &activated_target)
            {
                return false;
            }
            observed_syncs.fetch_add(1, Ordering::SeqCst);
            true
        },
        "injected restore activation sync failure",
    );

    let err = restore_storage_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("injected restore activation sync failure")
            && err
                .to_string()
                .contains("post-publication attestation or parent synchronization failed"),
        "unexpected restore error: {err}"
    );
    assert_eq!(activation_syncs.load(Ordering::SeqCst), 1);
    assert!(target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    let backup = std::fs::read_dir(&budget_root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-backup-")
            })
        })
        .expect("the original target backup must remain after post-publication failure");
    assert_eq!(std::fs::read(backup.join("old")).unwrap(), b"old-state");
    let accounting = budget.snapshot();
    let measured = crate::engine::fs_utils::measure_restore_directory(&budget_root).unwrap();
    assert_eq!(accounting.accounted_bytes, measured.logical_bytes);
    assert_eq!(accounting.active_reservations, 0);
    assert_eq!(accounting.reserved_bytes, 0);
}

#[test]
fn unbudgeted_restore_rolls_back_and_preserves_a_post_activation_consumer_write() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let synchronized_root = temp_dir.path().to_path_buf();
    let activation_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&activation_syncs);
    let activated_target = target_path.clone();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path != synchronized_root
                || !restore_target_is_published(&synchronized_root, &activated_target)
            {
                return false;
            }
            observed_syncs.fetch_add(1, Ordering::SeqCst);
            std::fs::write(activated_target.join("consumer-created"), b"consumer-state").unwrap();
            true
        },
        "injected unbudgeted restore activation sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(
        err.to_string()
            .contains("post-publication attestation or parent synchronization failed"),
        "{err}"
    );
    assert_eq!(activation_syncs.load(Ordering::SeqCst), 1);
    assert!(target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    assert_eq!(
        std::fs::read(target_path.join("consumer-created")).unwrap(),
        b"consumer-state"
    );
    let retained_backup = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-backup-")
            })
        })
        .expect("the pre-publication target backup must be retained");
    assert_eq!(
        std::fs::read(retained_backup.join("old")).unwrap(),
        b"old-state"
    );
}

#[test]
fn unbudgeted_restore_preserves_a_raced_staging_path_when_rollback_cannot_reclaim_it() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let synchronized_parent = temp_dir.path().to_path_buf();
    let observed_staging = Arc::new(Mutex::new(None::<PathBuf>));
    let staging_for_hook = Arc::clone(&observed_staging);
    let activated_target = target_path.clone();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path.parent() == Some(synchronized_parent.as_path())
                && path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with(".tmp-tsink-restore-staging-")
                })
            {
                *staging_for_hook
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()) = Some(path.to_path_buf());
                return false;
            }
            if path != synchronized_parent
                || !restore_target_is_published(&synchronized_parent, &activated_target)
            {
                return false;
            }

            let raced_staging = staging_for_hook
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
                .expect("the staged restore must have been synchronized before activation");
            std::fs::create_dir(&raced_staging).unwrap();
            std::fs::write(raced_staging.join("foreign"), b"foreign-state").unwrap();
            true
        },
        "injected activation sync failure after installing a raced staging path",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path)
        .expect_err("the injected activation synchronization must fail");

    assert!(
        err.to_string()
            .contains("post-publication attestation or parent synchronization failed"),
        "{err}"
    );
    assert!(target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    let raced_staging = observed_staging
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
        .unwrap();
    assert_eq!(
        std::fs::read(raced_staging.join("foreign")).unwrap(),
        b"foreign-state"
    );

    let backups = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-backup-target-")
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        backups.len(),
        1,
        "the original target must remain recoverable"
    );
    assert_eq!(std::fs::read(backups[0].join("old")).unwrap(), b"old-state");
}

#[test]
fn unbudgeted_restore_never_cleans_a_replacement_at_the_backup_path() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    let preserved_original = temp_dir.path().join("preserved-original");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let synchronized_parent = temp_dir.path().to_path_buf();
    let preserved_for_hook = preserved_original.clone();
    let observed_backup = Arc::new(Mutex::new(None::<PathBuf>));
    let backup_for_hook = Arc::clone(&observed_backup);
    let activated_target = target_path.clone();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path != synchronized_parent
                || !restore_target_is_published(&synchronized_parent, &activated_target)
            {
                return false;
            }
            let backup = std::fs::read_dir(&synchronized_parent)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .find(|candidate| {
                    candidate.file_name().is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with(".tmp-tsink-restore-backup-target-")
                    })
                })
                .expect("the original target must be present at its backup path");
            std::fs::rename(&backup, &preserved_for_hook).unwrap();
            std::fs::create_dir(&backup).unwrap();
            std::fs::write(backup.join("foreign"), b"foreign-state").unwrap();
            *backup_for_hook
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(backup);
            false
        },
        "the backup replacement hook observes without injecting a sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path)
        .expect_err("identity-checked backup cleanup must refuse the replacement");

    assert!(
        err.to_string()
            .contains("exact pre-move-identity backup cleanup failed"),
        "{err}"
    );
    assert!(target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    assert_eq!(
        std::fs::read(preserved_original.join("old")).unwrap(),
        b"old-state"
    );
    let raced_backup = observed_backup
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
        .unwrap();
    assert_eq!(
        std::fs::read(raced_backup.join("foreign")).unwrap(),
        b"foreign-state"
    );
}

#[test]
fn unbudgeted_restore_preserves_an_unknown_file_added_to_the_owned_backup() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let synchronized_parent = temp_dir.path().to_path_buf();
    let observed_backup = Arc::new(Mutex::new(None::<PathBuf>));
    let backup_for_hook = Arc::clone(&observed_backup);
    let activated_target = target_path.clone();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path != synchronized_parent
                || !restore_target_is_published(&synchronized_parent, &activated_target)
            {
                return false;
            }
            let backup = std::fs::read_dir(&synchronized_parent)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .find(|candidate| {
                    candidate.file_name().is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with(".tmp-tsink-restore-backup-target-")
                    })
                })
                .expect("the original target must still be present at its backup path");
            std::fs::write(backup.join("consumer-created"), b"consumer-state").unwrap();
            *backup_for_hook
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(backup);
            false
        },
        "the backup consumer hook observes without injecting a sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path)
        .expect_err("exact cleanup must preserve the unknown backup descendant");

    assert!(
        err.to_string()
            .contains("exact pre-move-identity backup cleanup failed"),
        "{err}"
    );
    assert!(
        err.to_string().contains("pre-move identity manifest"),
        "{err}"
    );
    assert!(target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
    let backup = observed_backup
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
        .unwrap();
    assert_eq!(std::fs::read(backup.join("old")).unwrap(), b"old-state");
    assert_eq!(
        std::fs::read(backup.join("consumer-created")).unwrap(),
        b"consumer-state"
    );
}

#[test]
fn unbudgeted_restore_never_replaces_a_target_created_during_activation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);
    std::fs::create_dir_all(&target_path).unwrap();
    std::fs::write(target_path.join("old"), b"old-state").unwrap();

    let synchronized_parent = temp_dir.path().to_path_buf();
    let raced_target = target_path.clone();
    let installed = Arc::new(AtomicBool::new(false));
    let observed_installed = Arc::clone(&installed);
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path == synchronized_parent
                && restore_target_is_between_replacement_moves(&synchronized_parent, &raced_target)
                && !observed_installed.swap(true, Ordering::SeqCst)
            {
                // The original target has just moved to its restore backup. Install a new target
                // before activation returns from the parent synchronization boundary.
                std::fs::create_dir(&raced_target).unwrap();
                std::fs::write(raced_target.join("foreign"), b"foreign-state").unwrap();
            }
            false
        },
        "the race hook observes without injecting a sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path)
        .expect_err("activation must not replace the raced target");

    assert!(installed.load(Ordering::SeqCst));
    assert!(
        err.to_string().contains("restore activation failed"),
        "{err}"
    );
    assert!(err.to_string().contains("rollback failed"), "{err}");
    assert_eq!(
        std::fs::read(target_path.join("foreign")).unwrap(),
        b"foreign-state"
    );
    assert!(!target_path
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .exists());

    let backups = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-backup-target-")
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        backups.len(),
        1,
        "the original target must remain recoverable"
    );
    assert_eq!(std::fs::read(backups[0].join("old")).unwrap(), b"old-state");
    let retained_staging = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-staging-")
            })
        })
        .expect("failed pre-publication activation must retain staging");
    assert!(retained_staging
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
}

#[test]
fn unbudgeted_restore_retains_staging_after_staging_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);

    let staging_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&staging_syncs);
    let expected_parent = temp_dir.path().to_path_buf();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path.parent() == Some(expected_parent.as_path())
                && path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with(".tmp-tsink-restore-staging-")
                })
                && observed_syncs.fetch_add(1, Ordering::SeqCst) + 1 == 2
        },
        "injected unbudgeted restore staging sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err
        .to_string()
        .contains("injected unbudgeted restore staging sync failure"));
    assert_eq!(staging_syncs.load(Ordering::SeqCst), 2);
    assert!(!target_path.exists());
    let retained_staging = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-staging-")
            })
        })
        .expect("a staging tree whose final sync failed must be retained");
    assert!(retained_staging
        .join(data_directory_manifest::DATA_DIRECTORY_MANIFEST_FILE_NAME)
        .is_file());
}

#[test]
fn unbudgeted_restore_retains_unverified_staging_after_copy_sync_failure() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");
    create_valid_restore_snapshot(&snapshot_path);

    let staging_syncs = Arc::new(AtomicUsize::new(0));
    let observed_syncs = Arc::clone(&staging_syncs);
    let expected_parent = temp_dir.path().to_path_buf();
    let _sync_guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path.parent() == Some(expected_parent.as_path())
                && path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with(".tmp-tsink-restore-staging-")
                })
                && observed_syncs.fetch_add(1, Ordering::SeqCst) == 0
        },
        "injected unbudgeted restore copy sync failure",
    );

    let err = restore_storage_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(err
        .to_string()
        .contains("injected unbudgeted restore copy sync failure"));
    assert_eq!(staging_syncs.load(Ordering::SeqCst), 1);
    assert!(!target_path.exists());
    let staging = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".tmp-tsink-restore-staging-")
            })
        })
        .expect("copy failure before identity capture must retain staging");
    assert!(staging.is_dir());
}

#[test]
fn unbudgeted_restore_synchronizes_missing_target_ancestors_before_staging() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");
    let restore_parent = temp_dir.path().join("restore-parent");
    let target_path = restore_parent.join("nested/target");
    create_valid_restore_snapshot(&snapshot_path);

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

        let builder = startup_builder(&data_path);
        data_directory_manifest::install_current_manifest_for_test(&builder).unwrap();
        let err = planning_error(&builder);
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

        let builder = startup_builder(&data_path);
        data_directory_manifest::install_current_manifest_for_test(&builder).unwrap();
        let err = planning_error(&builder);
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

    let builder = startup_builder(&data_path);
    data_directory_manifest::install_current_manifest_for_test(&builder).unwrap();
    let error = planning_error(&builder);
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
    data_directory_manifest::install_current_manifest_for_test(&startup_builder(&data_path))
        .unwrap();

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
    data_directory_manifest::install_current_manifest_for_test(&startup_builder(&data_path))
        .unwrap();

    // Keep the fixture above the indivisible live WAL writer-buffer capacity so this test reaches
    // the startup cleanup-memory boundary it is intended to exercise.
    let mut memory_limit = startup_builder(&data_path).wal_buffer_size().max(1);
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
    let manifest_bytes =
        data_directory_manifest::install_current_manifest_for_test(&startup_builder(&data_path))
            .unwrap();
    let expected_remaining = [
        manifest_bytes,
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
    let expected = crate::engine::tombstone::TombstoneMap::from([(
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

    let builder = startup_builder(&data_path);
    data_directory_manifest::install_current_manifest_for_test(&builder).unwrap();
    let plan = StartupPlanningPhase::prepare(&builder)
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
    data_directory_manifest::install_current_manifest_for_test(&builder).unwrap();
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
        false,
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
