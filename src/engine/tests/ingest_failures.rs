use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn replace_numeric_lane_with_file(root: &Path) {
    if let Err(err) = std::fs::remove_dir_all(root) {
        if err.kind() != std::io::ErrorKind::NotFound {
            panic!(
                "failed to remove numeric lane root {}: {err}",
                root.display()
            );
        }
    }
    if let Err(err) = std::fs::remove_file(root) {
        if err.kind() != std::io::ErrorKind::NotFound {
            panic!(
                "failed to remove numeric lane file {}: {err}",
                root.display()
            );
        }
    }
    std::fs::write(root, b"not-a-directory").unwrap();
}

#[test]
fn rejects_mixed_numeric_insert_when_wal_is_disabled() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_chunk_points(8)
        .build()
        .unwrap();
    let labels = vec![Label::new("host", "a")];

    let err = storage
        .insert_rows(&[
            Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(2, 2_i64)),
        ])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let points = storage.select("mixed_no_wal", &labels, 0, 10).unwrap();
    assert!(points.is_empty());
}

#[test]
fn rejects_mixed_numeric_insert_across_calls_when_wal_is_disabled() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_chunk_points(8)
        .build()
        .unwrap();
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            "mixed_no_wal_across_calls",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "mixed_no_wal_across_calls",
            labels.clone(),
            DataPoint::new(2, 2_i64),
        )])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let points = storage
        .select("mixed_no_wal_across_calls", &labels, 0, 10)
        .unwrap();
    assert_eq!(points, vec![DataPoint::new(1, 1.0)]);
}

#[test]
fn failed_insert_rolls_back_new_series_metadata_immediately() {
    let storage = ChunkStorage::new(4, None);
    let labels = vec![Label::new("host", "a")];

    let err = storage
        .insert_rows(&[
            Row::with_labels("phantom_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("phantom_metric", labels.clone(), DataPoint::new(2, 2_i64)),
        ])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let has_phantom_metric = storage
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == "phantom_metric" && series.labels == labels);
    assert!(!has_phantom_metric);
}

#[test]
fn repeated_failed_high_cardinality_inserts_keep_registry_memory_accounting_aligned() {
    let storage = ChunkStorage::new(4, None);

    for series_idx in 0..256 {
        let labels = vec![
            Label::new("host", format!("host-{series_idx}")),
            Label::new("rack", format!("rack-{}", series_idx % 16)),
        ];
        let err = storage
            .insert_rows(&[
                Row::with_labels(
                    "rollback_churn_metric",
                    labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels("rollback_churn_metric", labels, DataPoint::new(2, 2_i64)),
            ])
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        if series_idx % 32 == 0 {
            let snapshot = storage.observability_snapshot();
            assert_eq!(
                snapshot.memory.registry_bytes,
                storage.catalog.registry.read().memory_usage_bytes(),
                "series-churn rollback path should keep exposed registry memory in sync",
            );
            assert_eq!(
                storage.catalog.registry.read().series_count(),
                0,
                "failed writes should not leave phantom series behind during churn",
            );
        }
    }

    let snapshot = storage.observability_snapshot();
    assert_eq!(
        snapshot.memory.registry_bytes,
        storage.catalog.registry.read().memory_usage_bytes(),
    );
    assert!(
        storage.list_metrics().unwrap().is_empty(),
        "repeated rollback churn should not materialize any committed series",
    );
}

#[test]
fn failed_mixed_numeric_insert_does_not_resurrect_series_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let err = storage
            .insert_rows(&[
                Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(2, 2_i64)),
            ])
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let has_mixed_metric = reopened
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == "mixed_metric" && series.labels == labels);
    assert!(!has_mixed_metric);

    reopened.close().unwrap();
}

#[test]
fn wal_append_failure_does_not_ingest_points_or_survive_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "wal_append_atomicity";
    let oversized_value = vec![0xAB; (64 * 1024 * 1024) + 1];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new(metric, DataPoint::new(1, oversized_value))])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::InvalidConfiguration(message)
                if message.contains("WAL frame payload too large")
        ));

        let points = storage.select(metric, &[], 0, 10).unwrap();
        assert!(
            points.is_empty(),
            "failed WAL append must not expose ingested points in memory"
        );

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select(metric, &[], 0, 10).unwrap();
    assert!(
        points.is_empty(),
        "failed WAL append must not leave points durable after reopen"
    );

    reopened.close().unwrap();
}

#[test]
fn wal_sync_failure_does_not_ingest_points_or_survive_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "wal_sync_atomicity";

    {
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
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
        .unwrap();
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failed_hook = std::sync::Arc::clone(&failed);
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .set_append_sync_hook(move || {
                if failed_hook.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return Ok(());
                }

                Err(TsinkError::Other("injected WAL sync failure".to_string()))
            });

        let err = storage
            .insert_rows(&[Row::new(metric, DataPoint::new(1, 1.0))])
            .unwrap_err();
        assert!(failed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            err.to_string().contains("injected WAL sync failure"),
            "unexpected WAL sync error: {err:?}"
        );
        assert!(
            storage.select(metric, &[], 0, 10).unwrap().is_empty(),
            "failed WAL sync must not expose points in memory"
        );
        assert!(
            storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_frames()
                .unwrap()
                .is_empty(),
            "failed WAL sync must roll back flushed frames"
        );

        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .clear_append_sync_hook();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();

    assert!(
        reopened.select(metric, &[], 0, 10).unwrap().is_empty(),
        "failed WAL sync must not leave replayable points after reopen"
    );

    reopened.close().unwrap();
}

#[test]
fn wal_observability_distinguishes_appended_and_durable_progress_for_periodic_mode() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(
        temp_dir.path().join(WAL_DIR_NAME),
        WalSyncMode::Periodic(std::time::Duration::from_secs(3600)),
    )
    .unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
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
    .unwrap();

    storage
        .insert_rows(&[Row::new("wal_observability_gap", DataPoint::new(1, 1.0))])
        .unwrap();

    let obs = storage.observability_snapshot();
    assert_eq!(obs.wal.sync_mode, "periodic");
    assert!(!obs.wal.acknowledged_writes_durable);
    assert_eq!(obs.wal.durable_highwater_segment, 0);
    assert_eq!(obs.wal.durable_highwater_frame, 0);
    assert!(
        obs.wal.highwater_frame > obs.wal.durable_highwater_frame,
        "periodic mode should expose appended WAL progress ahead of durable progress"
    );
}

#[test]
fn committed_write_survives_post_commit_memory_pressure_failure() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_root = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(numeric_root.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                memory_budget_bytes: 1_000_000,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    let labels = vec![Label::new("host", "a")];

    let weak_storage = Arc::downgrade(&storage);
    storage.set_ingest_post_samples_hook({
        let numeric_root = numeric_root.clone();
        move || {
            replace_numeric_lane_with_file(&numeric_root);
            if let Some(storage) = weak_storage.upgrade() {
                storage.memory.budget_bytes.store(1, Ordering::SeqCst);
            }
        }
    });

    storage
        .insert_rows(&[Row::with_labels(
            "post_commit_memory_pressure",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.clear_ingest_post_samples_hook();

    assert_eq!(
        storage
            .select("post_commit_memory_pressure", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );

    let health = storage.observability_snapshot().health;
    assert_eq!(health.background_errors_total, 0);
    assert_eq!(health.maintenance_errors_total, 1);
    assert!(health.degraded);
    assert!(health
        .last_maintenance_error
        .as_deref()
        .is_some_and(|message| message.contains("post-commit memory budget enforcement")));

    drop(storage);

    std::fs::remove_file(&numeric_root).unwrap();
    std::fs::create_dir_all(&numeric_root).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .with_retention_enforced(false)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("post_commit_memory_pressure", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn post_wal_append_failure_aborts_staged_frames_and_does_not_survive_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let metric = "post_wal_append_failure";

    {
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                2,
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
            .unwrap(),
        );

        let weak_storage = Arc::downgrade(&storage);
        storage.set_ingest_post_samples_hook({
            let labels = labels.clone();
            move || {
                let Some(storage) = weak_storage.upgrade() else {
                    return;
                };
                let series_id = storage
                    .catalog
                    .registry
                    .read()
                    .resolve_existing(metric, &labels)
                    .expect("series definition must exist before the post-samples hook runs")
                    .series_id;
                let mut active = storage.active_shard(series_id).write();
                active
                    .get_mut(&series_id)
                    .expect("reserved series lane must exist before ingest")
                    .lane = ValueLane::Blob;
            }
        });

        let err = storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap_err();
        storage.clear_ingest_post_samples_hook();

        assert!(matches!(
            err,
            TsinkError::ValueTypeMismatch { expected, actual }
                if expected == "blob" && actual == "numeric"
        ));
        assert!(storage.select(metric, &labels, 0, 10).unwrap().is_empty());
        assert!(
            storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_frames()
                .unwrap()
                .is_empty(),
            "a rejected write must truncate staged WAL frames before returning the error"
        );
        assert!(
            !storage
                .list_metrics_with_wal()
                .unwrap()
                .into_iter()
                .any(|series| series.name == metric && series.labels == labels),
            "staged WAL metadata must stay hidden when the write aborts"
        );

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();

    assert!(reopened.select(metric, &labels, 0, 10).unwrap().is_empty());
    assert!(
        !reopened
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels),
        "recovery must not resurrect a write that failed after WAL append"
    );

    reopened.close().unwrap();
}

#[test]
fn crash_after_wal_persist_before_memory_ingest_does_not_resurrect_write() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let metric = "persist_before_ingest_crash";

    {
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                2,
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
            .unwrap(),
        );

        storage.set_ingest_crash_after_wal_persist_before_ingest();

        let panic_result = catch_unwind(AssertUnwindSafe({
            let storage = Arc::clone(&storage);
            let labels = labels.clone();
            move || {
                storage
                    .insert_rows(&[Row::with_labels(metric, labels, DataPoint::new(1, 1.0))])
                    .unwrap();
            }
        }));
        assert!(panic_result.is_err());
        assert!(storage.select(metric, &labels, 0, 10).unwrap().is_empty());
        assert!(
            !storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_frames()
                .unwrap()
                .is_empty(),
            "the crash window should leave persisted WAL bytes behind"
        );
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();

    assert!(reopened.select(metric, &labels, 0, 10).unwrap().is_empty());
    assert!(
        !reopened
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels),
        "restart recovery must ignore a write that crashed before memory ingest"
    );

    reopened.close().unwrap();
}

#[test]
fn crash_after_memory_ingest_before_wal_publish_does_not_resurrect_write() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let metric = "ingest_before_publish_crash";

    {
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                2,
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
            .unwrap(),
        );

        storage.set_ingest_crash_after_memory_ingest_before_publish();

        let panic_result = catch_unwind(AssertUnwindSafe({
            let storage = Arc::clone(&storage);
            let labels = labels.clone();
            move || {
                storage
                    .insert_rows(&[Row::with_labels(metric, labels, DataPoint::new(1, 1.0))])
                    .unwrap();
            }
        }));
        assert!(panic_result.is_err());
        assert_eq!(
            storage.select(metric, &labels, 0, 10).unwrap(),
            vec![DataPoint::new(1, 1.0)],
            "the write should already be live in memory before the publish step crashes"
        );
        assert!(
            !storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_frames()
                .unwrap()
                .is_empty(),
            "the crash window should leave persisted WAL bytes behind"
        );
        assert!(
            storage
                .persisted
                .wal
                .as_ref()
                .unwrap()
                .replay_committed_writes()
                .unwrap()
                .is_empty(),
            "the unpublished write must stay behind the WAL publish boundary"
        );
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();

    assert!(reopened.select(metric, &labels, 0, 10).unwrap().is_empty());
    assert!(
        !reopened
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels),
        "restart recovery must ignore a write that crashed before publish"
    );

    reopened.close().unwrap();
}

#[test]
fn failed_ingest_does_not_append_wal_samples() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new(2, Some(wal));
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            "wal_ingest_guard",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let sample_batches_before: usize = storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .replay_frames()
        .unwrap()
        .into_iter()
        .map(|frame| match frame {
            ReplayFrame::Samples(batches) => batches.len(),
            ReplayFrame::SeriesDefinition(_) => 0,
        })
        .sum();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("wal_ingest_guard", &labels)
        .unwrap()
        .series_id;
    {
        let mut active = storage.active_shard(series_id).write();
        let state = active.get_mut(&series_id).unwrap();
        state
            .rotate_partition_if_needed(
                2,
                storage.runtime.partition_window,
                storage.runtime.max_active_partition_heads_per_series,
            )
            .unwrap();
        state.append_point(2, Value::I64(2), WalHighWatermark::default());
    }

    let err = storage
        .insert_rows(&[Row::with_labels(
            "wal_ingest_guard",
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let sample_batches_after: usize = storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .replay_frames()
        .unwrap()
        .into_iter()
        .map(|frame| match frame {
            ReplayFrame::Samples(batches) => batches.len(),
            ReplayFrame::SeriesDefinition(_) => 0,
        })
        .sum();
    assert_eq!(sample_batches_after, sample_batches_before);
}

#[test]
fn rejects_family_drift_after_chunk_seal_without_persist() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels(
                "sealed_family_guard",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "sealed_family_guard",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "sealed_family_guard",
            labels,
            DataPoint::new(3, 3_i64),
        )])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
}

#[test]
fn rejects_family_drift_after_persisting_segment() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(numeric_lane_path),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "persisted_family_guard",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "persisted_family_guard",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush().unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "persisted_family_guard",
            labels,
            DataPoint::new(3, 3_i64),
        )])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
}

#[test]
fn wal_replay_rejects_family_drift_after_persisted_history() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap()),
        Some(numeric_lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "replay_family_guard",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "replay_family_guard",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("replay_family_guard", &labels)
        .unwrap()
        .series_id;
    storage.close().unwrap();

    let replay_points = vec![ChunkPoint {
        ts: 3,
        value: Value::I64(3),
    }];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    wal.append_samples(&[SamplesBatchFrame::from_points(
        series_id,
        ValueLane::Numeric,
        &replay_points,
    )
    .unwrap()])
        .unwrap();

    let reopened = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(numeric_lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    reopened
        .apply_loaded_segment_indexes(load_segment_indexes(&numeric_lane_path).unwrap(), false)
        .unwrap();

    let err = reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::ValueTypeMismatch { expected, actual }
            if expected == "f64" && actual == "i64"
    ));
}

#[test]
fn incremental_persisted_segment_adoption_rejects_family_drift() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(numeric_lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("adopt_family_guard", &labels)
        .unwrap()
        .series_id;

    let mut initial_chunks = HashMap::new();
    initial_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, &[(1, 1.0)])],
    );
    SegmentWriter::new(&numeric_lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &initial_chunks)
        .unwrap();

    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&numeric_lane_path).unwrap(), false)
        .unwrap();

    let conflicting_points = vec![ChunkPoint {
        ts: 2,
        value: Value::I64(2),
    }];
    let encoded = Encoder::encode_chunk_points(&conflicting_points, ValueLane::Numeric).unwrap();
    let conflicting_chunk = Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::I64),
            point_count: conflicting_points.len() as u16,
            min_ts: 2,
            max_ts: 2,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points: conflicting_points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    };
    let mut conflicting_chunks = HashMap::new();
    conflicting_chunks.insert(series_id, vec![conflicting_chunk]);

    let conflicting_writer = SegmentWriter::new(&numeric_lane_path, 0, 2).unwrap();
    conflicting_writer
        .write_segment(&registry, &conflicting_chunks)
        .unwrap();

    let err = storage
        .add_persisted_segments_from_loaded(vec![load_segment_index(
            conflicting_writer.layout().root.clone(),
        )
        .unwrap()])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::DataCorruption(message) if message.contains("mixes value families")
    ));
}
