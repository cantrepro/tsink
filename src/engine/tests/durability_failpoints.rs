use super::*;

use crate::engine::wal::WalDurabilityFailpoint;
use crate::WriteAcknowledgement;

fn wal_only_storage(data_path: &Path, chunk_points: usize) -> ChunkStorage {
    let wal = FramedWal::open(data_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    ChunkStorage::new_with_data_path_and_options(
        chunk_points,
        Some(wal),
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn reopen_wal_only_storage(data_path: &Path, chunk_points: usize) -> ChunkStorage {
    let storage = wal_only_storage(data_path, chunk_points);
    storage
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap();
    storage
}

#[test]
fn series_definition_wal_append_failpoint_rolls_back_identity_and_reopen() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let metric = "failpoint_series_definition_append";
    let storage = wal_only_storage(&data_path, 2);
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_durability_failpoint_hook(|point| {
            if point == WalDurabilityFailpoint::SeriesDefinitionAppend {
                return Err(TsinkError::Other(
                    "injected series-definition WAL append failure".to_string(),
                ));
            }
            Ok(())
        });

    let err = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(1, 1.0))])
        .unwrap_err();
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_durability_failpoint_hook();
    assert!(
        err.to_string()
            .contains("injected series-definition WAL append failure"),
        "unexpected append error: {err}"
    );
    assert!(storage.select(metric, &[], 0, 10).unwrap().is_empty());
    assert!(storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .replay_frames()
        .unwrap()
        .is_empty());
    assert!(!storage
        .list_metrics_with_wal()
        .unwrap()
        .iter()
        .any(|series| series.name == metric));

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);
    let reopened = reopen_wal_only_storage(&data_path, 2);
    assert!(reopened.select(metric, &[], 0, 10).unwrap().is_empty());
    assert!(!reopened
        .list_metrics_with_wal()
        .unwrap()
        .iter()
        .any(|series| series.name == metric));
    reopened.close().unwrap();
}

#[test]
fn sample_wal_append_failpoint_rolls_back_staged_definition_and_reopen() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let metric = "failpoint_sample_append";
    let storage = wal_only_storage(&data_path, 2);
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_durability_failpoint_hook(|point| {
            if point == WalDurabilityFailpoint::SamplesAppend {
                return Err(TsinkError::Other(
                    "injected sample WAL append failure".to_string(),
                ));
            }
            Ok(())
        });

    let err = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(1, 2.0))])
        .unwrap_err();
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_durability_failpoint_hook();
    assert!(
        err.to_string()
            .contains("injected sample WAL append failure"),
        "unexpected sample-append error: {err}"
    );
    assert!(storage.select(metric, &[], 0, 10).unwrap().is_empty());
    assert!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .replay_frames()
            .unwrap()
            .is_empty(),
        "the series-definition frame staged before the sample failure must be rolled back"
    );

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);
    let reopened = reopen_wal_only_storage(&data_path, 2);
    assert!(reopened.select(metric, &[], 0, 10).unwrap().is_empty());
    assert!(!reopened
        .list_metrics_with_wal()
        .unwrap()
        .iter()
        .any(|series| series.name == metric));
    reopened.close().unwrap();
}

#[test]
fn wal_flush_failpoint_rolls_back_only_the_unacknowledged_write() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let metric = "failpoint_wal_flush";
    let storage = wal_only_storage(&data_path, 4);
    let acknowledged = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(1, 3.0))])
        .unwrap();
    assert_eq!(acknowledged.acknowledgement, WriteAcknowledgement::Durable);
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_durability_failpoint_hook(|point| {
            if point == WalDurabilityFailpoint::Flush {
                return Err(TsinkError::Other("injected WAL flush failure".to_string()));
            }
            Ok(())
        });

    let err = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(2, 4.0))])
        .unwrap_err();
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_durability_failpoint_hook();
    assert!(
        err.to_string().contains("injected WAL flush failure"),
        "unexpected flush error: {err}"
    );
    assert_eq!(
        storage.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 3.0)]
    );

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);
    let reopened = reopen_wal_only_storage(&data_path, 4);
    assert_eq!(
        reopened.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 3.0)],
        "recovery must retain the prior Durable acknowledgement and exclude the failed write"
    );
    reopened.close().unwrap();
}

#[test]
fn chunk_seal_failpoint_aborts_wal_and_reopens_without_partial_rotation() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let metric = "failpoint_chunk_seal";
    let storage = wal_only_storage(&data_path, 2);
    let acknowledged = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(1, 5.0))])
        .unwrap();
    assert_eq!(acknowledged.acknowledgement, WriteAcknowledgement::Durable);
    storage.set_ingest_post_chunk_seal_hook(|| {
        Err(TsinkError::Other(
            "injected post-seal pre-publication failure".to_string(),
        ))
    });

    let err = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(2, 6.0))])
        .unwrap_err();
    storage.clear_ingest_post_chunk_seal_hook();
    assert!(
        err.to_string()
            .contains("injected post-seal pre-publication failure"),
        "unexpected chunk-seal error: {err}"
    );
    assert_eq!(
        storage.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 5.0)]
    );

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);
    let reopened = reopen_wal_only_storage(&data_path, 2);
    assert_eq!(
        reopened.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 5.0)],
        "recovery must not publish either the staged WAL sample or a partial active-to-sealed handoff"
    );
    reopened.close().unwrap();
}

#[test]
fn wal_reset_after_truncate_failpoint_reopens_from_published_segment() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let metric = "failpoint_wal_reset";
    let manifest_storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .with_partition_duration(Duration::MAX)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    manifest_storage.close().unwrap();

    let wal = FramedWal::open(data_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(data_path.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            partition_window: i64::MAX,
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let acknowledged = storage
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(1, 7.0))])
        .unwrap();
    assert_eq!(acknowledged.acknowledgement, WriteAcknowledgement::Durable);
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_durability_failpoint_hook(|point| {
            if point == WalDurabilityFailpoint::ResetAfterTruncate {
                return Err(TsinkError::Other(
                    "injected WAL reset failure after active-segment truncate".to_string(),
                ));
            }
            Ok(())
        });

    let err = storage.persist_segment_with_outcome().unwrap_err();
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_durability_failpoint_hook();
    assert!(
        err.to_string()
            .contains("injected WAL reset failure after active-segment truncate"),
        "unexpected reset error: {err}"
    );
    assert_eq!(
        storage.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 7.0)],
        "the already-published segment must remain query-visible after reset failure"
    );

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);
    let reopened = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .with_partition_duration(Duration::MAX)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 7.0)],
        "the Durable acknowledgement must recover from the published segment after WAL reset"
    );
    let second = reopened
        .insert_rows_with_result(&[Row::new(metric, DataPoint::new(2, 8.0))])
        .unwrap();
    assert_eq!(second.acknowledgement, WriteAcknowledgement::Durable);
    reopened.close().unwrap();

    let reopened_again = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .with_partition_duration(Duration::MAX)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened_again.select(metric, &[], 0, 10).unwrap(),
        vec![DataPoint::new(1, 7.0), DataPoint::new(2, 8.0)]
    );
    reopened_again.close().unwrap();
}

#[test]
fn snapshot_pre_final_rename_failpoint_cleans_staging_and_reopens_source() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let snapshot_path = temp.path().join("snapshot");
    let manifest_builder = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(4)
        .with_partition_duration(Duration::MAX);
    super::super::data_directory_manifest::install_current_manifest_for_test(&manifest_builder)
        .unwrap();
    let wal = FramedWal::open(data_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        4,
        Some(wal),
        Some(data_path.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            partition_window: i64::MAX,
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let acknowledged = storage
        .insert_rows_with_result(&[Row::new(
            "failpoint_snapshot_pre_rename",
            DataPoint::new(1, 10.0),
        )])
        .unwrap();
    assert_eq!(acknowledged.acknowledgement, WriteAcknowledgement::Durable);
    storage.set_snapshot_pre_publication_hook(|| {
        Err(TsinkError::Other(
            "injected snapshot pre-final-rename failure".to_string(),
        ))
    });

    let err = storage.snapshot(&snapshot_path).unwrap_err();
    storage.clear_snapshot_pre_publication_hook();
    assert!(
        err.to_string()
            .contains("injected snapshot pre-final-rename failure"),
        "unexpected snapshot pre-publication error: {err}"
    );
    assert!(!snapshot_path.exists());
    assert!(
        std::fs::read_dir(temp.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tmp-tsink-snapshot-")
        }),
        "pre-final-rename failure must clean the fully synchronized owned staging tree"
    );
    assert_eq!(
        storage
            .select("failpoint_snapshot_pre_rename", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 10.0)]
    );
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(4)
        .with_partition_duration(Duration::MAX)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened
            .select("failpoint_snapshot_pre_rename", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 10.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn snapshot_copy_file_sync_failpoint_retains_unverified_staging_and_preserves_source() {
    let temp = TempDir::new().unwrap();
    let data_path = temp.path().join("data");
    let snapshot_path = temp.path().join("snapshot");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(4)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    let acknowledged = storage
        .insert_rows_with_result(&[Row::new("failpoint_snapshot_copy", DataPoint::new(1, 9.0))])
        .unwrap();
    assert_eq!(acknowledged.acknowledgement, WriteAcknowledgement::Durable);
    let snapshot_parent = temp.path().to_path_buf();
    let failure = crate::engine::fs_utils::fail_file_sync_matching_once(
        move |candidate| {
            candidate.starts_with(&snapshot_parent)
                && candidate.to_string_lossy().contains(".tmp-tsink-snapshot-")
                && candidate
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == WAL_DIR_NAME)
                && candidate.file_name().is_some_and(|name| {
                    let name = name.to_string_lossy();
                    name.starts_with("wal-") && name.ends_with(".log")
                })
        },
        "injected snapshot WAL-copy file sync failure",
    );

    let err = storage.snapshot(&snapshot_path).unwrap_err();
    drop(failure);
    assert!(
        err.to_string()
            .contains("injected snapshot WAL-copy file sync failure"),
        "unexpected snapshot-copy error: {err}"
    );
    assert!(!snapshot_path.exists());
    let staging = std::fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".tmp-tsink-snapshot-"))
        })
        .expect("copy failure before descendant identity capture must retain staging");
    assert!(staging.join(WAL_DIR_NAME).exists());
    assert_eq!(
        storage
            .select("failpoint_snapshot_copy", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 9.0)]
    );
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(4)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened
            .select("failpoint_snapshot_copy", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 9.0)]
    );
    reopened.close().unwrap();
}
