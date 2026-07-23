//! Integration tests for tsink.

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tsink::engine::wal::{FramedWal, SeriesDefinitionFrame};
use tsink::{
    DataPoint, HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, LocalDiskBudget,
    LocalDiskLimits, MetricSeries, NativeHistogram, QueryOptions, Row, RowWriteStatus,
    SeriesMatcher, SeriesSelection, Storage, StorageBuilder, TimestampPrecision, TsinkError, Value,
    WalSyncMode, WriteAcknowledgement, WriteMode, WriteRejection, WriteRejectionCategory,
    MAX_WRITE_REJECTION_MESSAGE_BYTES,
};

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(9)),
        sum: 12.5,
        schema: 2,
        zero_threshold: 0.0,
        zero_count: Some(HistogramCount::Int(1)),
        negative_spans: vec![],
        negative_deltas: vec![],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 1,
            length: 2,
        }],
        positive_deltas: vec![4, 2],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::Gauge,
        custom_values: vec![0.25, 0.5],
    }
}

fn restore_entry_allowance_for(root: &std::path::Path) -> u64 {
    let budget = LocalDiskBudget::open(root, LocalDiskLimits::default()).unwrap();
    budget
        .snapshot_restore_entry_staging_allowance_bytes()
        .unwrap()
}

#[test]
fn effective_storage_limits_distinguish_unbounded_defaults() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let limits = storage.effective_storage_limits();
    assert!(limits.reported_by_backend);
    assert!(!limits.persistent);
    assert!(!limits.wal_enabled);
    assert_eq!(limits.accounted_memory_bytes, None);
    assert_eq!(limits.cardinality, None);
    assert_eq!(
        limits.max_labels_per_series,
        Some(tsink::DEFAULT_MAX_LABELS_PER_SERIES as u64)
    );
    assert_eq!(
        limits.max_series_identity_bytes,
        Some(tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES as u64)
    );
    assert_eq!(limits.max_new_series_per_window, None);
    assert_eq!(limits.new_series_window_nanos, None);
    assert_eq!(limits.wal_bytes, None);
    assert_eq!(limits.local_disk_bytes, None);
    assert_eq!(limits.filesystem_free_headroom_bytes, None);
    assert_eq!(limits.maintenance_temp_reserve_bytes, None);
    assert!(limits.max_concurrent_writers.is_some_and(|value| value > 0));
    assert_eq!(limits.write_timeout_nanos, Some(30_000_000_000));
    assert_eq!(limits.max_active_partition_heads_per_series, Some(8));
    let observability = storage.observability_snapshot();
    assert_eq!(observability.limits, limits);
    assert!(observability.local_disk.is_none());

    storage.close().unwrap();
}

#[test]
fn effective_storage_limits_report_configured_persistent_controls() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_memory_limit(16 * 1024 * 1024)
        .with_cardinality_limit(1_024)
        .with_max_labels_per_series(7)
        .with_max_series_identity_bytes(4_096)
        .with_series_creation_rate_limit(11, Duration::from_secs(2))
        .with_wal_size_limit(4 * 1024 * 1024)
        .with_local_disk_limit(64 * 1024 * 1024)
        .with_filesystem_free_headroom(1024 * 1024)
        .with_maintenance_temp_reserve(2 * 1024 * 1024)
        .with_max_writers(3)
        .with_write_timeout(Duration::from_nanos(17))
        .with_max_active_partition_heads_per_series(4)
        .build()
        .unwrap();

    assert_eq!(
        storage.effective_storage_limits(),
        tsink::EffectiveStorageLimits {
            reported_by_backend: true,
            persistent: true,
            wal_enabled: true,
            accounted_memory_bytes: Some(16 * 1024 * 1024),
            cardinality: Some(1_024),
            max_labels_per_series: Some(7),
            max_series_identity_bytes: Some(4_096),
            max_new_series_per_window: Some(11),
            new_series_window_nanos: Some(2_000_000_000),
            max_write_batch_rows: Some(100_000),
            max_write_batch_input_bytes: Some(64 * 1024 * 1024),
            wal_bytes: Some(4 * 1024 * 1024),
            wal_write_buffer_bytes: Some(4 * 1024),
            local_disk_bytes: Some(64 * 1024 * 1024),
            filesystem_free_headroom_bytes: Some(1024 * 1024),
            maintenance_temp_reserve_bytes: Some(2 * 1024 * 1024),
            max_concurrent_writers: Some(3),
            write_timeout_nanos: Some(17),
            max_background_threads: Some(4),
            max_flush_concurrency: Some(1),
            max_compaction_concurrency: Some(1),
            max_retention_tiering_concurrency: Some(0),
            max_remote_catalog_refresh_concurrency: Some(0),
            max_remote_tier_fetch_concurrency: Some(0),
            max_rollup_concurrency: Some(1),
            flush_interval_nanos: Some(250_000_000),
            compaction_interval_nanos: Some(5_000_000_000),
            persisted_refresh_poll_interval_nanos: Some(250_000_000),
            rollup_interval_nanos: Some(5_000_000_000),
            max_active_partition_heads_per_series: Some(4),
        }
    );
    let local_disk = storage
        .observability_snapshot()
        .local_disk
        .expect("persistent built-in storage should report local disk accounting");
    assert_eq!(local_disk.limits.max_bytes, Some(64 * 1024 * 1024));
    assert_eq!(
        local_disk.limits.filesystem_free_headroom_bytes,
        1024 * 1024
    );
    assert_eq!(
        local_disk.limits.maintenance_temp_reserve_bytes,
        2 * 1024 * 1024
    );

    let background = storage.observability_snapshot().background;
    assert_eq!(background.max_threads, 4);
    assert_eq!(background.installed_threads, 4);
    assert_eq!(background.flush.interval_nanos, Some(250_000_000));
    assert_eq!(background.compaction.interval_nanos, Some(5_000_000_000));
    assert_eq!(
        background.persisted_refresh.interval_nanos,
        Some(250_000_000)
    );
    assert_eq!(background.rollup.interval_nanos, Some(5_000_000_000));

    storage.close().unwrap();

    let background = storage.observability_snapshot().background;
    assert_eq!(background.installed_threads, 0);
    assert_eq!(background.running_threads, 0);
    for worker in [
        background.flush,
        background.compaction,
        background.persisted_refresh,
        background.rollup,
    ] {
        assert_eq!(worker.starts_total, worker.exits_total);
        assert_eq!(worker.passes_started_total, worker.passes_completed_total);
        assert_eq!(worker.shutdown_joins_total, 1);
    }
}

#[test]
fn creation_rate_window_reports_timestamp_precision_rounding() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_series_creation_rate_limit(1, Duration::from_nanos(1))
        .build()
        .unwrap();

    let limits = storage.effective_storage_limits();
    assert_eq!(limits.max_new_series_per_window, Some(1));
    assert_eq!(limits.new_series_window_nanos, Some(1_000_000_000));

    storage.close().unwrap();
}

#[test]
fn test_basic_insert_and_select() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let rows = vec![
        Row::new("metric1", DataPoint::new(1000, 1.0)),
        Row::new("metric1", DataPoint::new(1001, 2.0)),
        Row::new("metric1", DataPoint::new(1002, 3.0)),
    ];

    storage.insert_rows(&rows).unwrap();

    let points = storage.select("metric1", &[], 1000, 1003).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0].value_as_f64().unwrap_or(f64::NAN), 1.0);
    assert_eq!(points[1].value_as_f64().unwrap_or(f64::NAN), 2.0);
    assert_eq!(points[2].value_as_f64().unwrap_or(f64::NAN), 3.0);
}

#[test]
fn test_labeled_metrics() {
    let storage = StorageBuilder::new().build().unwrap();

    let labels1 = vec![Label::new("host", "server1")];
    let labels2 = vec![Label::new("host", "server2")];

    let rows = vec![
        Row::with_labels("cpu", labels1.clone(), DataPoint::new(1000, 10.0)),
        Row::with_labels("cpu", labels2.clone(), DataPoint::new(1000, 20.0)),
    ];

    storage.insert_rows(&rows).unwrap();

    let points1 = storage.select("cpu", &labels1, 999, 1001).unwrap();
    assert_eq!(points1.len(), 1);
    assert_eq!(points1[0].value_as_f64().unwrap_or(f64::NAN), 10.0);

    let points2 = storage.select("cpu", &labels2, 999, 1001).unwrap();
    assert_eq!(points2.len(), 1);
    assert_eq!(points2[0].value_as_f64().unwrap_or(f64::NAN), 20.0);
}

#[test]
fn test_no_data_points_error() {
    let storage = StorageBuilder::new().build().unwrap();

    let result = storage.select("nonexistent", &[], 1000, 2000);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[test]
fn test_invalid_time_range() {
    let storage = StorageBuilder::new().build().unwrap();

    let result = storage.select("metric", &[], 2000, 1000);
    assert!(matches!(result, Err(TsinkError::InvalidTimeRange { .. })));
}

#[test]
fn test_empty_metric_name() {
    let storage = StorageBuilder::new().build().unwrap();

    let result = storage.select("", &[], 1000, 2000);
    assert!(matches!(result, Err(TsinkError::MetricRequired)));
}

#[test]
fn test_insert_rejects_empty_metric_name() {
    let storage = StorageBuilder::new().build().unwrap();

    let result = storage.insert_rows(&[Row::new("", DataPoint::new(1000, 1.0))]);
    assert!(matches!(result, Err(TsinkError::MetricRequired)));
}

#[test]
fn test_insert_rejects_overlong_metric_name() {
    let storage = StorageBuilder::new().build().unwrap();
    let metric = "m".repeat(u16::MAX as usize + 1);

    let result = storage.insert_rows(&[Row::new(metric.clone(), DataPoint::new(1000, 1.0))]);
    assert!(matches!(result, Err(TsinkError::InvalidMetricName(_))));

    let result = storage.select(&metric, &[], 0, 2000);
    assert!(matches!(result, Err(TsinkError::InvalidMetricName(_))));
}

#[test]
fn wal_backed_pre_apply_batch_rejection_commits_none_across_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let rows = vec![
        Row::new("atomic_batch_first", DataPoint::new(1, 1.0)),
        Row::new("", DataPoint::new(2, 2.0)),
        Row::new("atomic_batch_last", DataPoint::new(3, 3.0)),
    ];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(true)
            .with_wal_sync_mode(WalSyncMode::PerAppend)
            .build()
            .unwrap();

        let err = storage.insert_rows(&rows).unwrap_err();
        assert!(matches!(err, TsinkError::MetricRequired));
        assert!(storage
            .select("atomic_batch_first", &[], 0, 10)
            .unwrap()
            .is_empty());
        assert!(storage
            .select("atomic_batch_last", &[], 0, 10)
            .unwrap()
            .is_empty());
        assert!(storage.list_metrics().unwrap().is_empty());
        assert!(storage.list_metrics_with_wal().unwrap().is_empty());

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();

    assert!(reopened
        .select("atomic_batch_first", &[], 0, 10)
        .unwrap()
        .is_empty());
    assert!(reopened
        .select("atomic_batch_last", &[], 0, 10)
        .unwrap()
        .is_empty());
    assert!(reopened.list_metrics().unwrap().is_empty());
    assert!(reopened.list_metrics_with_wal().unwrap().is_empty());
    reopened.close().unwrap();
}

#[test]
fn test_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();

    {
        let storage = StorageBuilder::new()
            .with_data_path(data_path)
            .build()
            .unwrap();

        let rows = vec![
            Row::new("persistent_metric", DataPoint::new(1000, 100.0)),
            Row::new("persistent_metric", DataPoint::new(1001, 101.0)),
        ];

        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    {
        let storage = StorageBuilder::new()
            .with_data_path(data_path)
            .build()
            .unwrap();

        let points = storage.select("persistent_metric", &[], 999, 1002).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].value_as_f64().unwrap_or(f64::NAN), 100.0);
        assert_eq!(points[1].value_as_f64().unwrap_or(f64::NAN), 101.0);
    }
}

#[test]
fn test_snapshot_and_restore_recover_live_data() {
    let temp_dir = TempDir::new().unwrap();
    let source_path = temp_dir.path().join("source");
    let snapshot_path = temp_dir.path().join("snapshot");
    let restore_path = temp_dir.path().join("restore");

    {
        let storage = StorageBuilder::new()
            .with_data_path(&source_path)
            .with_chunk_points(4096)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::new("snapshot_metric", DataPoint::new(1, 11.0)),
                Row::new("snapshot_metric", DataPoint::new(2, 22.0)),
            ])
            .unwrap();

        storage.snapshot(&snapshot_path).unwrap();
        storage.close().unwrap();
    }

    StorageBuilder::restore_from_snapshot(&snapshot_path, &restore_path).unwrap();

    let restored = StorageBuilder::new()
        .with_data_path(&restore_path)
        .with_chunk_points(4096)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let points = restored.select("snapshot_metric", &[], 0, 10).unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(1, 11.0), DataPoint::new(2, 22.0)]
    );
    restored.close().unwrap();
}

#[test]
fn test_restore_from_snapshot_replaces_existing_target_contents() {
    let temp_dir = TempDir::new().unwrap();
    let source_path = temp_dir.path().join("source");
    let snapshot_path = temp_dir.path().join("snapshot");
    let target_path = temp_dir.path().join("target");

    {
        let storage = StorageBuilder::new()
            .with_data_path(&source_path)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();
        storage
            .insert_rows(&[Row::new("restored_metric", DataPoint::new(1, 1.0))])
            .unwrap();
        storage.snapshot(&snapshot_path).unwrap();
        storage.close().unwrap();
    }

    {
        let storage = StorageBuilder::new()
            .with_data_path(&target_path)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();
        storage
            .insert_rows(&[Row::new("old_metric", DataPoint::new(1, 9.0))])
            .unwrap();
        storage.close().unwrap();
    }

    StorageBuilder::restore_from_snapshot(&snapshot_path, &target_path).unwrap();

    let restored = StorageBuilder::new()
        .with_data_path(&target_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let restored_points = restored.select("restored_metric", &[], 0, 10).unwrap();
    assert_eq!(restored_points, vec![DataPoint::new(1, 1.0)]);
    let old_points = restored.select("old_metric", &[], 0, 10).unwrap();
    assert!(old_points.is_empty());

    restored.close().unwrap();
}

#[test]
fn test_snapshot_and_restore_preserve_histogram_values() {
    let temp_dir = TempDir::new().unwrap();
    let source_path = temp_dir.path().join("source");
    let snapshot_path = temp_dir.path().join("snapshot");
    let restore_path = temp_dir.path().join("restore");

    {
        let storage = StorageBuilder::new()
            .with_data_path(&source_path)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::new(
                    "snapshot_histogram",
                    DataPoint::new(1, Value::from(sample_histogram())),
                ),
                Row::new(
                    "snapshot_histogram",
                    DataPoint::new(2, Value::from(sample_histogram())),
                ),
            ])
            .unwrap();

        storage.snapshot(&snapshot_path).unwrap();
        storage.close().unwrap();
    }

    StorageBuilder::restore_from_snapshot(&snapshot_path, &restore_path).unwrap();

    let restored = StorageBuilder::new()
        .with_data_path(&restore_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let points = restored.select("snapshot_histogram", &[], 0, 10).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, Value::from(sample_histogram())),
            DataPoint::new(2, Value::from(sample_histogram()))
        ]
    );
    restored.close().unwrap();
}

#[test]
fn test_snapshot_requires_persistent_storage() {
    let storage = StorageBuilder::new().build().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");

    let err = storage.snapshot(&snapshot_path).unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
}

#[test]
fn test_snapshot_rejects_managed_descendants_before_creating_staging_paths() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .build()
        .unwrap();

    let direct = data_path.join("snapshot");
    let err = storage.snapshot(&direct).unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
    assert!(!direct.exists());

    let dotdot = data_path.join("not-created/../snapshot-through-dotdot");
    let err = storage.snapshot(&dotdot).unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
    assert!(!data_path.join("not-created").exists());

    storage.close().unwrap();
}

#[cfg(unix)]
#[test]
fn test_snapshot_rejects_managed_destination_through_symlinked_parent() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let data_alias = temp_dir.path().join("data-alias");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .build()
        .unwrap();
    symlink(&data_path, &data_alias).unwrap();

    let destination = data_alias.join("snapshot");
    let err = storage.snapshot(&destination).unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
    assert!(!data_path.join("snapshot").exists());

    storage.close().unwrap();
}

#[cfg(unix)]
#[test]
fn test_snapshot_rejects_dangling_symlink_destination() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let snapshot_path = temp_dir.path().join("snapshot-link");
    symlink(
        temp_dir.path().join("missing-snapshot-target"),
        &snapshot_path,
    )
    .unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .build()
        .unwrap();
    let err = storage.snapshot(&snapshot_path).unwrap_err();

    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
}

#[cfg(unix)]
#[test]
fn test_restore_rejects_symlink_snapshot_path() {
    let temp_dir = TempDir::new().unwrap();
    let real_snapshot = temp_dir.path().join("real-snapshot");
    let snapshot_link = temp_dir.path().join("snapshot-link");
    let restore_path = temp_dir.path().join("restore");

    fs::create_dir_all(&real_snapshot).unwrap();
    symlink(&real_snapshot, &snapshot_link).unwrap();

    let err = StorageBuilder::restore_from_snapshot(&snapshot_link, &restore_path).unwrap_err();

    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
}

#[test]
fn unbudgeted_restore_rejects_resolved_overlap_in_both_directions_before_staging() {
    let temp_dir = TempDir::new().unwrap();

    let source_ancestor = temp_dir.path().join("source-ancestor");
    fs::create_dir_all(&source_ancestor).unwrap();
    fs::write(source_ancestor.join("payload"), b"source").unwrap();
    let descendant_target = source_ancestor.join("missing/target");
    let descendant_err =
        StorageBuilder::restore_from_snapshot(&source_ancestor, &descendant_target).unwrap_err();
    assert!(matches!(
        descendant_err,
        TsinkError::InvalidConfiguration(_)
    ));
    assert!(!source_ancestor.join("missing").exists());

    let target_ancestor = temp_dir.path().join("target-ancestor");
    let nested_source = target_ancestor.join("snapshot");
    fs::create_dir_all(&nested_source).unwrap();
    fs::write(target_ancestor.join("sentinel"), b"keep").unwrap();
    fs::write(nested_source.join("payload"), b"nested").unwrap();
    let ancestor_err =
        StorageBuilder::restore_from_snapshot(&nested_source, &target_ancestor).unwrap_err();
    assert!(matches!(ancestor_err, TsinkError::InvalidConfiguration(_)));
    assert_eq!(fs::read(target_ancestor.join("sentinel")).unwrap(), b"keep");
    assert_eq!(fs::read(nested_source.join("payload")).unwrap(), b"nested");
    assert!(fs::read_dir(temp_dir.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".tmp-tsink-restore-")));
}

#[test]
fn unbudgeted_restore_rejects_excessive_snapshot_depth_before_destination_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("deep-snapshot");
    let target_path = temp_dir.path().join("missing-parent/target");
    let mut deepest = snapshot_path.clone();
    for depth in 0..=tsink::MAX_SNAPSHOT_RESTORE_DEPTH {
        deepest.push(format!("d{depth}"));
    }
    fs::create_dir_all(&deepest).unwrap();

    let err = StorageBuilder::restore_from_snapshot(&snapshot_path, &target_path).unwrap_err();

    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message)
            if message.contains("directory depth") && message.contains("exceeds limit")
    ));
    assert!(!temp_dir.path().join("missing-parent").exists());
    assert!(fs::read_dir(temp_dir.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".tmp-tsink-restore-")));
}

#[cfg(unix)]
#[test]
fn unbudgeted_restore_rejects_overlap_through_intermediate_symlink_aliases() {
    let temp_dir = TempDir::new().unwrap();
    let real_root = temp_dir.path().join("real-root");
    let root_alias = temp_dir.path().join("root-alias");
    let source = real_root.join("container/snapshot");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload"), b"source").unwrap();
    symlink(&real_root, &root_alias).unwrap();

    let descendant_alias = root_alias.join("container/snapshot/missing/target");
    let descendant_err =
        StorageBuilder::restore_from_snapshot(&source, &descendant_alias).unwrap_err();
    assert!(matches!(
        descendant_err,
        TsinkError::InvalidConfiguration(_)
    ));
    assert!(!source.join("missing").exists());

    let aliased_source = root_alias.join("container/snapshot");
    let ancestor_target = real_root.join("container");
    let ancestor_err =
        StorageBuilder::restore_from_snapshot(&aliased_source, &ancestor_target).unwrap_err();
    assert!(matches!(ancestor_err, TsinkError::InvalidConfiguration(_)));
    assert_eq!(fs::read(source.join("payload")).unwrap(), b"source");
    assert!(fs::read_dir(&real_root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".tmp-tsink-restore-")));
}

#[test]
fn budgeted_restore_rejects_quota_before_destination_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    let target_path = budget_root.join("missing-parent/target");
    fs::create_dir_all(&snapshot_path).unwrap();
    fs::write(snapshot_path.join("payload"), b"12345").unwrap();
    let entry_allowance = restore_entry_allowance_for(&budget_root);
    let budget = LocalDiskBudget::open(
        &budget_root,
        LocalDiskLimits {
            max_bytes: Some(4),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();

    let err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        TsinkError::DiskQuotaExceeded { requested, .. }
            if requested
                == 5 + 3 * entry_allowance
    ));
    assert!(!budget_root.join("missing-parent").exists());
    assert_eq!(fs::read(snapshot_path.join("payload")).unwrap(), b"12345");
    let accounting = budget.snapshot();
    assert_eq!(accounting.accounted_bytes, 0);
    assert_eq!(accounting.active_reservations, 0);
    assert_eq!(accounting.reserved_bytes, 0);
    assert_eq!(accounting.rejections_total, 1);
}

#[test]
fn budgeted_restore_reserves_entry_allowance_for_empty_files() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    let target_path = budget_root.join("target");
    fs::create_dir_all(&snapshot_path).unwrap();
    fs::write(snapshot_path.join("empty"), b"").unwrap();
    let required = 2 * restore_entry_allowance_for(&budget_root);
    let budget = LocalDiskBudget::open(
        &budget_root,
        LocalDiskLimits {
            max_bytes: Some(required - 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();

    let err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        TsinkError::DiskQuotaExceeded {
            requested,
            limit,
            used: 0,
            reserved: 0,
            ..
        } if requested == required && limit == required - 1
    ));
    assert!(!target_path.exists());
    assert_eq!(budget.snapshot().accounted_bytes, 0);
    assert_eq!(budget.snapshot().active_reservations, 0);
}

#[test]
fn budgeted_restore_reserves_allowance_for_every_missing_target_parent() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    let target_path = budget_root.join("one/two/three/target");
    fs::create_dir_all(&snapshot_path).unwrap();
    let entry_allowance = restore_entry_allowance_for(&budget_root);
    let required = 4 * entry_allowance; // snapshot root plus three missing target parents
    let budget = LocalDiskBudget::open(
        &budget_root,
        LocalDiskLimits {
            max_bytes: Some(required - 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();

    let err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        TsinkError::DiskQuotaExceeded {
            requested,
            limit,
            used: 0,
            reserved: 0,
        } if requested == required && limit == required - 1
    ));
    assert!(!budget_root.join("one").exists());
    assert_eq!(budget.snapshot().active_reservations, 0);
}

#[test]
fn budgeted_restore_replaces_target_preserves_siblings_and_reopens_exactly() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    let target_path = budget_root.join("target");
    fs::create_dir_all(snapshot_path.join("nested")).unwrap();
    fs::write(snapshot_path.join("payload"), b"fresh").unwrap();
    fs::write(snapshot_path.join("nested/more"), b"xy").unwrap();
    fs::create_dir_all(&target_path).unwrap();
    fs::write(target_path.join("old"), b"oldold").unwrap();
    fs::write(budget_root.join("host-owned.bin"), b"host").unwrap();

    // Existing bytes (10) plus 7 logical staging bytes and four entry allowances fit exactly.
    let staging_admission = 7 + 4 * restore_entry_allowance_for(&budget_root);
    let limits = LocalDiskLimits {
        max_bytes: Some(10 + staging_admission),
        ..LocalDiskLimits::default()
    };
    let budget = LocalDiskBudget::open(&budget_root, limits).unwrap();
    StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &target_path,
        Arc::clone(&budget),
    )
    .unwrap();

    assert_eq!(fs::read(target_path.join("payload")).unwrap(), b"fresh");
    assert_eq!(fs::read(target_path.join("nested/more")).unwrap(), b"xy");
    assert!(!target_path.join("old").exists());
    assert_eq!(
        fs::read(budget_root.join("host-owned.bin")).unwrap(),
        b"host"
    );
    assert!(fs::read_dir(&budget_root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".tmp-tsink-restore-")));

    let accounting = budget.snapshot();
    assert_eq!(accounting.accounted_bytes, 11);
    assert_eq!(accounting.unknown_bytes, 11);
    assert_eq!(accounting.active_reservations, 0);
    assert_eq!(accounting.reserved_bytes, 0);
    drop(budget);

    let reopened = LocalDiskBudget::open(&budget_root, limits).unwrap();
    let reopened_accounting = reopened.snapshot();
    assert_eq!(reopened_accounting.accounted_bytes, 11);
    assert_eq!(reopened_accounting.unknown_bytes, 11);
    assert_eq!(reopened_accounting.active_reservations, 0);
}

#[test]
fn budgeted_restore_rejects_target_escape_root_and_snapshot_overlap() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    fs::create_dir_all(&snapshot_path).unwrap();
    fs::write(snapshot_path.join("payload"), b"data").unwrap();
    fs::create_dir_all(budget_root.join("in-tree-snapshot")).unwrap();
    fs::write(budget_root.join("in-tree-snapshot/payload"), b"managed").unwrap();
    fs::write(budget_root.join("sentinel"), b"keep").unwrap();
    let budget = LocalDiskBudget::open(&budget_root, LocalDiskLimits::default()).unwrap();

    let outside_target = temp_dir.path().join("outside-target");
    let outside_err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &outside_target,
        Arc::clone(&budget),
    )
    .unwrap_err();
    assert!(matches!(outside_err, TsinkError::InvalidConfiguration(_)));
    assert!(!outside_target.exists());

    let root_err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &budget_root,
        Arc::clone(&budget),
    )
    .unwrap_err();
    assert!(matches!(root_err, TsinkError::InvalidConfiguration(_)));

    let overlap_target = budget_root.join("overlap-target");
    let overlap_err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        budget_root.join("in-tree-snapshot"),
        &overlap_target,
        Arc::clone(&budget),
    )
    .unwrap_err();
    assert!(matches!(overlap_err, TsinkError::InvalidConfiguration(_)));
    assert!(!overlap_target.exists());
    assert_eq!(fs::read(budget_root.join("sentinel")).unwrap(), b"keep");
    assert_eq!(budget.snapshot().active_reservations, 0);
}

#[cfg(unix)]
#[test]
fn budgeted_restore_rejects_snapshot_and_target_symlinks_before_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("external-snapshot");
    let budget_root = temp_dir.path().join("restore-envelope");
    fs::create_dir_all(&snapshot_path).unwrap();
    fs::write(snapshot_path.join("payload"), b"data").unwrap();
    symlink(snapshot_path.join("payload"), snapshot_path.join("linked")).unwrap();
    fs::create_dir_all(budget_root.join("real-parent")).unwrap();
    symlink(
        budget_root.join("real-parent"),
        budget_root.join("linked-parent"),
    )
    .unwrap();
    let budget = LocalDiskBudget::open(&budget_root, LocalDiskLimits::default()).unwrap();

    let source_err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        budget_root.join("source-rejected/target"),
        Arc::clone(&budget),
    )
    .unwrap_err();
    assert!(matches!(source_err, TsinkError::InvalidConfiguration(_)));
    assert!(!budget_root.join("source-rejected").exists());

    fs::remove_file(snapshot_path.join("linked")).unwrap();
    let linked_target = budget_root.join("linked-parent/target");
    let target_err = StorageBuilder::restore_from_snapshot_with_disk_budget(
        &snapshot_path,
        &linked_target,
        Arc::clone(&budget),
    )
    .unwrap_err();
    assert!(matches!(target_err, TsinkError::InvalidConfiguration(_)));
    assert!(!budget_root.join("real-parent/target").exists());
    assert_eq!(budget.snapshot().active_reservations, 0);
}

#[test]
fn concurrent_budgeted_restores_admit_only_the_final_available_bytes() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_a = temp_dir.path().join("snapshot-a");
    let snapshot_b = temp_dir.path().join("snapshot-b");
    let budget_root = temp_dir.path().join("restore-envelope");
    fs::create_dir_all(&snapshot_a).unwrap();
    fs::create_dir_all(&snapshot_b).unwrap();
    fs::write(snapshot_a.join("payload"), b"aaaa").unwrap();
    fs::write(snapshot_b.join("payload"), b"bbbb").unwrap();
    let entry_allowance = restore_entry_allowance_for(&budget_root);
    let budget = LocalDiskBudget::open(
        &budget_root,
        LocalDiskLimits {
            max_bytes: Some(4 + 2 * entry_allowance),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let handles = [
        (snapshot_a, budget_root.join("target-a")),
        (snapshot_b, budget_root.join("target-b")),
    ]
    .into_iter()
    .map(|(snapshot, target)| {
        let budget = Arc::clone(&budget);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            StorageBuilder::restore_from_snapshot_with_disk_budget(snapshot, target, budget)
        })
    })
    .collect::<Vec<_>>();
    barrier.wait();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(TsinkError::DiskQuotaExceeded { .. })))
            .count(),
        1
    );
    assert_eq!(
        [budget_root.join("target-a"), budget_root.join("target-b")]
            .iter()
            .filter(|target| target.exists())
            .count(),
        1
    );
    let accounting = budget.snapshot();
    assert_eq!(accounting.accounted_bytes, 4);
    assert_eq!(accounting.active_reservations, 0);
    assert_eq!(accounting.reserved_bytes, 0);
    assert_eq!(accounting.rejections_total, 1);
}

#[test]
fn test_out_of_order_inserts() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let rows = vec![
        Row::new("metric", DataPoint::new(1002, 3.0)),
        Row::new("metric", DataPoint::new(1000, 1.0)),
        Row::new("metric", DataPoint::new(1001, 2.0)),
    ];

    storage.insert_rows(&rows).unwrap();

    let points = storage.select("metric", &[], 999, 1003).unwrap();
    assert_eq!(points.len(), 3);

    assert_eq!(points[0].timestamp, 1000);
    assert_eq!(points[1].timestamp, 1001);
    assert_eq!(points[2].timestamp, 1002);
}

#[test]
fn test_future_data_is_not_expired_by_partition_age() {
    let storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(1))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let future_ts = now + 24 * 3600;
    storage
        .insert_rows(&[Row::new("future_metric", DataPoint::new(future_ts, 1.0))])
        .unwrap();

    thread::sleep(Duration::from_secs(2));

    let points = storage.select("future_metric", &[], 0, i64::MAX).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, future_ts);
}

#[test]
fn test_far_future_write_does_not_reject_current_data_or_hide_history() {
    let storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(12 * 3600))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let current = now;
    let older = now - 60;
    let future = now + 30 * 24 * 3600;

    storage
        .insert_rows(&[Row::new("future_skew_metric", DataPoint::new(future, 3.0))])
        .unwrap();
    storage
        .insert_rows(&[
            Row::new("future_skew_metric", DataPoint::new(older, 1.0)),
            Row::new("future_skew_metric", DataPoint::new(current, 2.0)),
        ])
        .unwrap();

    assert_eq!(
        storage
            .select("future_skew_metric", &[], older - 1, future + 1)
            .unwrap(),
        vec![
            DataPoint::new(older, 1.0),
            DataPoint::new(current, 2.0),
            DataPoint::new(future, 3.0),
        ]
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.retention.max_observed_timestamp, Some(future));
    assert_eq!(
        snapshot.retention.recency_reference_timestamp,
        Some(current)
    );
    assert_eq!(snapshot.retention.future_skew_points_total, 1);
    assert_eq!(snapshot.retention.future_skew_max_timestamp, Some(future));
}

#[test]
fn test_full_delete_recomputes_retention_reference_across_series() {
    let storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(60))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let hidden = now - 30;
    let anchor = now + 31;
    let hidden_labels = vec![Label::new("host", "hidden")];
    let anchor_labels = vec![Label::new("host", "anchor")];

    storage
        .insert_rows(&[Row::with_labels(
            "delete_retention_metric",
            hidden_labels.clone(),
            DataPoint::new(hidden, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "delete_retention_metric",
            anchor_labels.clone(),
            DataPoint::new(anchor, 2.0),
        )])
        .unwrap();

    assert!(storage
        .select(
            "delete_retention_metric",
            &hidden_labels,
            hidden - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        storage
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(anchor)
    );

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("delete_retention_metric")
                .with_matcher(SeriesMatcher::equal("host", "anchor")),
        )
        .unwrap();

    assert!(storage
        .observability_snapshot()
        .retention
        .recency_reference_timestamp
        .is_some_and(|ts| ts < anchor));
    assert_eq!(
        storage
            .select(
                "delete_retention_metric",
                &hidden_labels,
                hidden - 1,
                anchor + 1
            )
            .unwrap(),
        vec![DataPoint::new(hidden, 1.0)]
    );
    assert!(storage
        .select(
            "delete_retention_metric",
            &anchor_labels,
            hidden - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
}

#[test]
fn test_concurrent_writes() {
    let storage = Arc::new(
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap(),
    );

    let test_timestamp = 1_000_000;
    let test_row = vec![Row::new(
        "test_metric",
        DataPoint::new(test_timestamp, 42.0),
    )];
    storage.insert_rows(&test_row).unwrap();

    let test_points = storage
        .select("test_metric", &[], test_timestamp - 1, test_timestamp + 1)
        .unwrap();
    assert_eq!(test_points.len(), 1);
    assert_eq!(test_points[0].value_as_f64().unwrap_or(f64::NAN), 42.0);

    let mut handles = vec![];
    let base_timestamp = 2_000_000;

    for i in 0..10 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            let rows = vec![Row::new(
                "concurrent_metric",
                DataPoint::new(base_timestamp + i as i64, i as f64),
            )];
            storage.insert_rows(&rows).unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let points = storage
        .select(
            "concurrent_metric",
            &[],
            base_timestamp - 1,
            base_timestamp + 20,
        )
        .unwrap_or_else(|e| {
            panic!("Failed to find data for concurrent_metric: {:?}", e);
        });

    assert_eq!(points.len(), 10, "Expected 10 points for concurrent_metric");

    let mut values: Vec<f64> = points
        .iter()
        .map(|p| p.value_as_f64().unwrap_or(f64::NAN))
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let expected: Vec<f64> = (0..10).map(|i| i as f64).collect();
    assert_eq!(values, expected);
}

#[test]
fn test_with_max_writers_zero_allows_writes() {
    let storage = StorageBuilder::new()
        .with_max_writers(0)
        .with_write_timeout(Duration::from_millis(5))
        .build()
        .unwrap();

    let result = storage.insert_rows(&[Row::new("auto_workers", DataPoint::new(1, 1.0))]);
    assert!(
        result.is_ok(),
        "with_max_writers(0) should auto-detect workers instead of timing out"
    );
}

#[test]
fn test_operations_after_close_return_storage_closed() {
    let storage = StorageBuilder::new().build().unwrap();
    storage
        .insert_rows(&[Row::new("closed_metric", DataPoint::new(1, 1.0))])
        .unwrap();
    storage.close().unwrap();

    assert!(matches!(
        storage.insert_rows(&[Row::new("closed_metric", DataPoint::new(2, 2.0))]),
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.select("closed_metric", &[], 0, 10),
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.select_all("closed_metric", 0, 10),
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.select_with_options("closed_metric", QueryOptions::new(0, 10)),
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.list_metrics(),
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(storage.close(), Err(TsinkError::StorageClosed)));
}

#[test]
fn test_select_returns_sorted_points() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(2))
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let rows = vec![
        Row::new("sorted_metric", DataPoint::new(5, 1.0)),
        Row::new("sorted_metric", DataPoint::new(1, 2.0)),
        Row::new("sorted_metric", DataPoint::new(3, 3.0)),
    ];

    storage.insert_rows(&rows).unwrap();

    let points = storage
        .select("sorted_metric", &[], 0, 10)
        .expect("select should succeed");

    assert_eq!(points.len(), 3);
    assert!(points.windows(2).all(|w| w[0].timestamp <= w[1].timestamp));
}

#[test]
fn test_persistence_with_existing_partitions_still_allows_writes() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_secs(1))
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::new("persist", DataPoint::new(0, 1.0)),
                Row::new("persist", DataPoint::new(2, 2.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(true)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new("persist", DataPoint::new(3, 3.0))])
        .unwrap();

    let mut live_points = storage.select("persist", &[], 0, 10).unwrap();
    live_points.sort_by_key(|p| p.timestamp);
    assert!(
        live_points
            .iter()
            .any(|p| p.timestamp == 3 && (p.value_as_f64().unwrap_or(f64::NAN) - 3.0).abs() < 1e-12),
        "newly inserted point should be present before close"
    );
    storage.close().unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let points = storage.select("persist", &[], 0, 10).unwrap();
    assert!(
        points
            .iter()
            .any(|p| p.timestamp == 3 && (p.value_as_f64().unwrap_or(f64::NAN) - 3.0).abs() < 1e-12),
        "newly inserted point should survive close/reopen even with existing disk partitions"
    );
}

#[test]
fn test_list_metrics_deduplicates_across_disk_memory_and_wal() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_secs(1))
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::new("cpu", DataPoint::new(10, 1.0)),
                Row::with_labels(
                    "http_requests",
                    vec![Label::new("status", "200"), Label::new("method", "GET")],
                    DataPoint::new(11, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(true)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::new("cpu", DataPoint::new(12, 3.0)),
            Row::with_labels(
                "queue_depth",
                vec![Label::new("queue", "critical")],
                DataPoint::new(13, 4.0),
            ),
        ])
        .unwrap();

    let metrics = storage.list_metrics().unwrap();
    let expected = vec![
        MetricSeries {
            name: "cpu".to_string(),
            labels: Vec::new(),
        },
        MetricSeries {
            name: "http_requests".to_string(),
            labels: vec![Label::new("method", "GET"), Label::new("status", "200")],
        },
        MetricSeries {
            name: "queue_depth".to_string(),
            labels: vec![Label::new("queue", "critical")],
        },
    ];

    assert_eq!(metrics, expected);
}

#[test]
fn test_list_metrics_ignores_runtime_wal_only_series() {
    let temp_dir = TempDir::new().unwrap();
    let wal_only_metric = "wal_only_metric";
    let wal_only_labels = vec![Label::new("source", "wal")];

    {
        let wal = FramedWal::open(temp_dir.path().join("wal"), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 4242,
            metric: wal_only_metric.to_string(),
            labels: wal_only_labels.clone(),
        })
        .unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let metrics = storage.list_metrics().unwrap();
    assert!(metrics.is_empty());

    storage.close().unwrap();
}

#[test]
fn test_list_metrics_with_wal_ignores_uncommitted_series_definitions() {
    let temp_dir = TempDir::new().unwrap();
    let wal_only_metric = "wal_only_metric";
    let wal_only_labels = vec![Label::new("source", "wal")];

    {
        let wal = FramedWal::open(temp_dir.path().join("wal"), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 4242,
            metric: wal_only_metric.to_string(),
            labels: wal_only_labels.clone(),
        })
        .unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let metrics = storage.list_metrics_with_wal().unwrap();
    assert!(metrics.is_empty());

    storage.close().unwrap();
}

#[test]
fn test_build_with_new_data_path_and_wal_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("fresh-data-path");

    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new("fresh_metric", DataPoint::new(1, 1.0))])
        .unwrap();
    let points = storage.select("fresh_metric", &[], 0, 10).unwrap();
    assert_eq!(points.len(), 1);
}

#[test]
fn test_wal_disabled_does_not_replay_stale_segments() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();
    fs::write(wal_dir.join("wal.log"), [0xFF, 0x00, 0x13, 0x37]).unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert!(storage
        .select("stale_metric", &[], 0, 10)
        .unwrap()
        .is_empty());
    assert!(storage.list_metrics().unwrap().is_empty());
    storage.close().unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert!(storage
        .select("stale_metric", &[], 0, 10)
        .unwrap()
        .is_empty());
    assert!(storage.list_metrics().unwrap().is_empty());
}

#[test]
fn test_wal_disabled_cleans_stale_segments_before_reenable() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    fs::create_dir_all(&wal_dir).unwrap();
    fs::write(wal_dir.join("wal.log"), [0xAA, 0xBB, 0xCC]).unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new("fresh_metric", DataPoint::new(6, 6.0))])
        .unwrap();
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    assert!(reopened
        .select("stale_metric", &[], 0, 10)
        .unwrap()
        .is_empty());
    let fresh = reopened.select("fresh_metric", &[], 0, 10).unwrap();
    assert_eq!(fresh.len(), 1);
    assert_eq!(fresh[0].timestamp, 6);
    assert!((fresh[0].value_as_f64().unwrap_or(f64::NAN) - 6.0).abs() < 1e-12);
}

#[test]
fn test_insert_rejects_oversized_labels_even_with_struct_literal() {
    let storage = StorageBuilder::new().build().unwrap();
    let oversized = Label {
        name: "k".to_string(),
        value: "x".repeat(tsink::label::MAX_LABEL_VALUE_LEN + 1),
    };

    let err = storage
        .insert_rows(&[Row::with_labels(
            "oversized_label_metric",
            vec![oversized],
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::InvalidLabel(_)));
}

#[test]
fn test_wal_buffer_size_zero_still_recovers() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(true)
            .with_wal_buffer_size(0)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("zero_buf_wal", DataPoint::new(1, 1.0))])
            .unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_wal_buffer_size(0)
        .build()
        .unwrap();

    let points = storage.select("zero_buf_wal", &[], 0, 10).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 1);
    assert!((points[0].value_as_f64().unwrap_or(f64::NAN) - 1.0).abs() < 1e-12);
}

#[test]
fn test_drop_without_close_persists_when_wal_disabled() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("drop_persist_metric", DataPoint::new(1, 1.0))])
            .unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let points = storage.select("drop_persist_metric", &[], 0, 10).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 1);
    assert!((points[0].value_as_f64().unwrap_or(f64::NAN) - 1.0).abs() < 1e-12);
    storage.close().unwrap();
}

#[test]
fn test_wal_sync_mode_can_be_switched() {
    for mode in [
        WalSyncMode::Periodic(Duration::from_millis(250)),
        WalSyncMode::PerAppend,
    ] {
        let temp_dir = TempDir::new().unwrap();

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(true)
                .with_wal_sync_mode(mode)
                .build()
                .unwrap();

            storage
                .insert_rows(&[Row::new("sync_mode_metric", DataPoint::new(1, 1.0))])
                .unwrap();
            storage.close().unwrap();
        }

        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(true)
            .with_wal_sync_mode(mode)
            .build()
            .unwrap();

        let points = storage.select("sync_mode_metric", &[], 0, 10).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, 1);
        assert!((points[0].value_as_f64().unwrap_or(f64::NAN) - 1.0).abs() < 1e-12);
    }
}

#[test]
fn write_result_reports_volatile_without_wal() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let result = storage
        .insert_rows_with_result(&[Row::new("volatile_write", DataPoint::new(1, 1.0))])
        .unwrap();

    assert_eq!(result.acknowledgement, WriteAcknowledgement::Volatile);
    assert!(!result.is_durable());
}

#[test]
fn write_result_reports_wal_acknowledgement_level() {
    for (mode, expected) in [
        (WalSyncMode::PerAppend, WriteAcknowledgement::Durable),
        (
            WalSyncMode::Periodic(Duration::from_secs(3600)),
            WriteAcknowledgement::Appended,
        ),
    ] {
        let temp_dir = TempDir::new().unwrap();
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(true)
            .with_wal_sync_mode(mode)
            .build()
            .unwrap();

        let result = storage
            .insert_rows_with_result(&[Row::new("ack_write", DataPoint::new(1, 1.0))])
            .unwrap();

        assert_eq!(result.acknowledgement, expected);
        assert_eq!(result.is_durable(), expected.is_durable());
    }
}

#[test]
fn canonical_atomic_batch_reports_all_rows_accepted() {
    let storage = StorageBuilder::new().build().unwrap();
    let rows = [
        Row::new("atomic_batch", DataPoint::new(1, 1_i64)),
        Row::new("atomic_batch", DataPoint::new(2, 2_i64)),
    ];

    let result = storage.write_batch(&rows, WriteMode::Atomic).unwrap();

    assert_eq!(result.submitted, 2);
    assert_eq!(result.accepted, 2);
    assert_eq!(result.rejected, 0);
    assert_eq!(result.acknowledgement, Some(WriteAcknowledgement::Volatile));
    assert_eq!(result.outcomes.len(), 2);
    for (index, outcome) in result.outcomes.iter().enumerate() {
        assert_eq!(outcome.index, index);
        assert_eq!(outcome.status, RowWriteStatus::Accepted);
    }
    assert_eq!(storage.select("atomic_batch", &[], 0, 3).unwrap().len(), 2);
}

#[test]
fn canonical_atomic_batch_rejects_every_row_when_one_is_invalid() {
    let storage = StorageBuilder::new().build().unwrap();
    let rows = [
        Row::new("atomic_valid_before", DataPoint::new(1, 1_i64)),
        Row::new("", DataPoint::new(2, 2_i64)),
        Row::new("atomic_valid_after", DataPoint::new(3, 3_i64)),
    ];

    let result = storage.write_batch(&rows, WriteMode::Atomic).unwrap();

    assert_eq!(result.submitted, 3);
    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 3);
    assert_eq!(result.acknowledgement, None);
    assert_eq!(result.outcomes.len(), 3);
    for (index, outcome) in result.outcomes.iter().enumerate() {
        assert_eq!(outcome.index, index);
        let RowWriteStatus::Rejected(rejection) = &outcome.status else {
            panic!("atomic rejection must reject every input row");
        };
        assert_eq!(rejection.category, WriteRejectionCategory::InvalidMetric);
        assert_eq!(rejection.cause_index, None);
    }
    assert!(storage
        .select("atomic_valid_before", &[], 0, 4)
        .unwrap()
        .is_empty());
    assert!(storage
        .select("atomic_valid_after", &[], 0, 4)
        .unwrap()
        .is_empty());
}

#[test]
fn canonical_best_effort_batch_reports_middle_rejection_by_index() {
    let storage = StorageBuilder::new().build().unwrap();
    let rows = [
        Row::new("best_effort", DataPoint::new(1, 1_i64)),
        Row::new("", DataPoint::new(2, 2_i64)),
        Row::new("best_effort", DataPoint::new(3, 3_i64)),
    ];

    let result = storage.write_batch(&rows, WriteMode::BestEffort).unwrap();

    assert_eq!(result.submitted, 3);
    assert_eq!(result.accepted, 2);
    assert_eq!(result.rejected, 1);
    assert_eq!(result.acknowledgement, Some(WriteAcknowledgement::Volatile));
    assert_eq!(result.outcomes[0].index, 0);
    assert_eq!(result.outcomes[0].status, RowWriteStatus::Accepted);
    assert_eq!(result.outcomes[1].index, 1);
    let RowWriteStatus::Rejected(rejection) = &result.outcomes[1].status else {
        panic!("invalid middle row must be rejected");
    };
    assert_eq!(rejection.category, WriteRejectionCategory::InvalidMetric);
    assert_eq!(rejection.cause_index, Some(1));
    assert_eq!(result.outcomes[2].index, 2);
    assert_eq!(result.outcomes[2].status, RowWriteStatus::Accepted);
    assert_eq!(storage.select("best_effort", &[], 0, 4).unwrap().len(), 2);
}

#[test]
fn canonical_atomic_batch_reports_cardinality_limit_without_partial_commit() {
    let storage = StorageBuilder::new()
        .with_cardinality_limit(1)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new("existing_series", DataPoint::new(1, 1_i64))])
        .unwrap();

    let result = storage
        .write_batch(
            &[
                Row::new("existing_series", DataPoint::new(2, 2_i64)),
                Row::new("new_series", DataPoint::new(2, 3_i64)),
            ],
            WriteMode::Atomic,
        )
        .unwrap();

    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 2);
    assert_eq!(result.acknowledgement, None);
    for outcome in &result.outcomes {
        let RowWriteStatus::Rejected(rejection) = &outcome.status else {
            panic!("cardinality-limited atomic batch must reject every row");
        };
        assert_eq!(
            rejection.category,
            WriteRejectionCategory::CardinalityLimitExceeded
        );
    }
    assert_eq!(
        storage.select("existing_series", &[], 0, 3).unwrap(),
        vec![DataPoint::new(1, 1_i64)]
    );
    assert!(storage.select("new_series", &[], 0, 3).unwrap().is_empty());
}

#[test]
fn canonical_atomic_batch_reports_memory_pressure_without_visibility() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_memory_limit(1)
        .with_write_timeout(Duration::ZERO)
        .build()
        .unwrap();

    let result = storage
        .write_batch(
            &[Row::new("memory_pressure", DataPoint::new(1, 1_i64))],
            WriteMode::Atomic,
        )
        .unwrap();

    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 1);
    let RowWriteStatus::Rejected(rejection) = &result.outcomes[0].status else {
        panic!("memory-limited write must be rejected");
    };
    assert_eq!(rejection.category, WriteRejectionCategory::MemoryPressure);
    assert!(storage
        .select("memory_pressure", &[], 0, 2)
        .unwrap()
        .is_empty());
}

#[test]
fn canonical_atomic_batch_reports_wal_quota_without_publishing_series() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_wal_size_limit(1)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_write_timeout(Duration::ZERO)
        .build()
        .unwrap();

    let result = storage
        .write_batch(
            &[Row::new("wal_quota", DataPoint::new(1, 1_i64))],
            WriteMode::Atomic,
        )
        .unwrap();

    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 1);
    let RowWriteStatus::Rejected(rejection) = &result.outcomes[0].status else {
        panic!("WAL-limited write must be rejected");
    };
    assert_eq!(rejection.category, WriteRejectionCategory::WalQuotaExceeded);
    assert!(storage.list_metrics_with_wal().unwrap().is_empty());
    assert!(storage.select("wal_quota", &[], 0, 2).unwrap().is_empty());
}

#[test]
fn canonical_atomic_batch_reports_disk_quota_after_over_limit_reopen() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_sync_mode(WalSyncMode::PerAppend)
            .build()
            .unwrap();
        storage
            .write_batch(
                &[Row::new("disk_quota_reopen", DataPoint::new(1, 1_i64))],
                WriteMode::Atomic,
            )
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_local_disk_limit(1)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .unwrap();
    let disk = reopened
        .observability_snapshot()
        .local_disk
        .expect("persistent storage must report disk accounting");
    assert!(disk.over_limit);
    assert!(disk.accounted_bytes > 1);
    assert_eq!(
        reopened.select("disk_quota_reopen", &[], 0, 3).unwrap(),
        vec![DataPoint::new(1, 1_i64)]
    );

    let result = reopened
        .write_batch(
            &[Row::new("disk_quota_reopen", DataPoint::new(2, 2_i64))],
            WriteMode::Atomic,
        )
        .unwrap();
    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 1);
    let RowWriteStatus::Rejected(rejection) = &result.outcomes[0].status else {
        panic!("over-limit write must be rejected");
    };
    assert_eq!(
        rejection.category,
        WriteRejectionCategory::DiskQuotaExceeded
    );
    assert_eq!(
        reopened.select("disk_quota_reopen", &[], 0, 3).unwrap(),
        vec![DataPoint::new(1, 1_i64)]
    );
    reopened.close().unwrap();
}

#[test]
fn persistent_reopen_reconciles_disk_categories_and_unknown_files() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_sync_mode(WalSyncMode::PerAppend)
            .build()
            .unwrap();
        storage
            .insert_rows(&[Row::new("disk_category_reopen", DataPoint::new(1, 1_i64))])
            .unwrap();
        storage.close().unwrap();
    }
    let host_file = temp_dir.path().join("host-owned.bin");
    fs::write(&host_file, vec![7u8; 13]).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_local_disk_limit(64 * 1024 * 1024)
        .build()
        .unwrap();
    let disk = reopened
        .observability_snapshot()
        .local_disk
        .expect("persistent storage must report disk accounting");
    let category_bytes = |category| {
        disk.categories
            .iter()
            .find(|usage| usage.category == category)
            .map(|usage| usage.bytes)
            .unwrap_or(0)
    };
    assert!(category_bytes(tsink::DiskCategory::Wal) > 0);
    assert!(category_bytes(tsink::DiskCategory::Segments) > 0);
    assert!(category_bytes(tsink::DiskCategory::Registry) > 0);
    assert!(category_bytes(tsink::DiskCategory::Unknown) >= 13);
    assert_eq!(
        disk.categories.iter().map(|usage| usage.bytes).sum::<u64>(),
        disk.accounted_bytes
    );
    assert_eq!(disk.active_reservations, 0);
    assert_eq!(disk.reserved_bytes, 0);
    assert!(disk.reconciliations_total >= 2);
    assert!(host_file.exists());
    assert_eq!(
        reopened.select("disk_category_reopen", &[], 0, 2).unwrap(),
        vec![DataPoint::new(1, 1_i64)]
    );
    reopened.close().unwrap();
}

#[test]
fn canonical_atomic_batch_reports_retention_floor_without_visibility() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(60))
        .build()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let timestamp = now - 120;

    let result = storage
        .write_batch(
            &[Row::new(
                "below_retention_floor",
                DataPoint::new(timestamp, 1_i64),
            )],
            WriteMode::Atomic,
        )
        .unwrap();

    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 1);
    let RowWriteStatus::Rejected(rejection) = &result.outcomes[0].status else {
        panic!("expired write must be rejected");
    };
    assert_eq!(
        rejection.category,
        WriteRejectionCategory::BelowRetentionFloor
    );
    assert!(storage
        .select("below_retention_floor", &[], timestamp - 1, now + 1)
        .unwrap()
        .is_empty());
}

#[test]
fn canonical_empty_and_all_rejected_batches_have_no_acknowledgement() {
    let storage = StorageBuilder::new().build().unwrap();

    for mode in [WriteMode::Atomic, WriteMode::BestEffort] {
        let result = storage.write_batch(&[], mode).unwrap();
        assert_eq!(result.submitted, 0);
        assert_eq!(result.accepted, 0);
        assert_eq!(result.rejected, 0);
        assert_eq!(result.acknowledgement, None);
        assert!(result.outcomes.is_empty());
    }

    let result = storage
        .write_batch(
            &[
                Row::new("", DataPoint::new(1, 1_i64)),
                Row::new("", DataPoint::new(2, 2_i64)),
            ],
            WriteMode::BestEffort,
        )
        .unwrap();
    assert_eq!(result.submitted, 2);
    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 2);
    assert_eq!(result.acknowledgement, None);
    assert_eq!(result.outcomes.len(), 2);
    for (index, outcome) in result.outcomes.iter().enumerate() {
        assert_eq!(outcome.index, index);
        let RowWriteStatus::Rejected(rejection) = &outcome.status else {
            panic!("all-invalid best-effort batch must reject every row");
        };
        assert_eq!(rejection.category, WriteRejectionCategory::InvalidMetric);
        assert_eq!(rejection.cause_index, Some(index));
    }

    storage.close().unwrap();
    for mode in [WriteMode::Atomic, WriteMode::BestEffort] {
        assert!(matches!(
            storage.write_batch(&[], mode),
            Err(TsinkError::StorageClosed)
        ));
    }
}

#[test]
fn write_acknowledgement_weakest_uses_durability_order() {
    use WriteAcknowledgement::{Appended, Durable, Volatile};

    assert_eq!(Durable.weakest(Durable), Durable);
    assert_eq!(Durable.weakest(Appended), Appended);
    assert_eq!(Appended.weakest(Durable), Appended);
    assert_eq!(Appended.weakest(Volatile), Volatile);
    assert_eq!(Volatile.weakest(Durable), Volatile);
}

#[test]
fn canonical_rejection_messages_are_bounded_at_utf8_boundaries() {
    let rejection = WriteRejection::new(
        WriteRejectionCategory::Internal,
        Some(7),
        "é".repeat(MAX_WRITE_REJECTION_MESSAGE_BYTES),
    );

    assert!(rejection.message.len() <= MAX_WRITE_REJECTION_MESSAGE_BYTES);
    assert!(rejection.message.is_char_boundary(rejection.message.len()));
    assert_eq!(rejection.cause_index, Some(7));
}

#[derive(Default)]
struct LegacyStorage {
    limits: tsink::EffectiveStorageLimits,
}

impl Storage for LegacyStorage {
    fn insert_rows(&self, _rows: &[Row]) -> tsink::Result<()> {
        Ok(())
    }

    fn select(
        &self,
        _metric: &str,
        _labels: &[Label],
        _start: i64,
        _end: i64,
    ) -> tsink::Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_with_options(
        &self,
        _metric: &str,
        _opts: QueryOptions,
    ) -> tsink::Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_all(
        &self,
        _metric: &str,
        _start: i64,
        _end: i64,
    ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        Ok(Vec::new())
    }

    fn close(&self) -> tsink::Result<()> {
        Ok(())
    }

    fn effective_storage_limits(&self) -> tsink::EffectiveStorageLimits {
        self.limits
    }
}

#[test]
fn legacy_storage_limits_are_explicitly_unreported() {
    let storage = LegacyStorage::default();
    assert_eq!(
        storage.effective_storage_limits(),
        tsink::EffectiveStorageLimits::default()
    );
    assert_eq!(storage.observability_snapshot().limits, storage.limits);
    assert!(storage.observability_snapshot().local_disk.is_none());
}

#[test]
fn default_observability_preserves_third_party_reported_limits() {
    let storage = LegacyStorage {
        limits: tsink::EffectiveStorageLimits {
            reported_by_backend: true,
            max_concurrent_writers: Some(1),
            ..tsink::EffectiveStorageLimits::default()
        },
    };

    assert_eq!(
        storage.observability_snapshot().limits,
        storage.effective_storage_limits()
    );
}

#[test]
fn canonical_batch_is_unsupported_for_legacy_storage_backends() {
    let error = LegacyStorage::default()
        .write_batch(
            &[Row::new("legacy", DataPoint::new(1, 1_i64))],
            WriteMode::Atomic,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: "write_batch",
            ..
        }
    ));
}

#[test]
fn test_close_handles_partition_name_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new("close_flush", DataPoint::new(1, 1.0))])
        .unwrap();

    fs::write(temp_dir.path().join("p-1-1"), b"conflict").unwrap();

    storage.close().unwrap();
}

#[test]
fn test_close_failure_can_be_retried_on_same_handle() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new("close_retry", DataPoint::new(1, 1.0))])
        .unwrap();

    let numeric_lane_root = temp_dir.path().join("lane_numeric");
    if numeric_lane_root.exists() {
        if numeric_lane_root.is_dir() {
            fs::remove_dir_all(&numeric_lane_root).unwrap();
        } else {
            fs::remove_file(&numeric_lane_root).unwrap();
        }
    }
    fs::write(&numeric_lane_root, b"conflict").unwrap();

    let first_close_err = storage.close().unwrap_err();
    assert!(
        matches!(
            &first_close_err,
            TsinkError::Io(_) | TsinkError::IoWithPath { .. }
        ),
        "unexpected first close error: {first_close_err:?}"
    );

    fs::remove_file(&numeric_lane_root).unwrap();
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    let points = reopened.select("close_retry", &[], 0, 10).unwrap();
    assert_eq!(points, vec![DataPoint::new(1, 1.0)]);
}

#[test]
fn test_select_across_multiple_partitions_persistent() {
    let temp_dir = TempDir::new().unwrap();

    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(2))
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("multi_part", DataPoint::new(10, 1.0)),
        Row::new("multi_part", DataPoint::new(13, 2.0)),
        Row::new("multi_part", DataPoint::new(11, 3.0)),
    ];
    storage.insert_rows(&rows).unwrap();
    storage.close().unwrap();

    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(2))
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let points = storage.select("multi_part", &[], 0, 20).unwrap();
    assert_eq!(
        points.len(),
        3,
        "should read all points across partitions, got {:?}",
        points
    );
    assert!(points.windows(2).all(|w| w[0].timestamp <= w[1].timestamp));
}

#[test]
fn test_close_persists_partitions_with_same_time_bounds_without_overwrite() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_secs(1))
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("collision_metric", DataPoint::new(10, 1.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("collision_metric", DataPoint::new(10, 2.0))])
            .unwrap();
        storage.close().unwrap();
    }

    let count_segments_in_level = |level: &str| -> usize {
        fs::read_dir(
            temp_dir
                .path()
                .join("lane_numeric")
                .join("segments")
                .join(level),
        )
        .ok()
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .map(|name| name.starts_with("seg-"))
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
    };
    let l0_segment_dirs = count_segments_in_level("L0");
    let l1_segment_dirs = count_segments_in_level("L1");
    let l2_segment_dirs = count_segments_in_level("L2");
    let segment_dirs = l0_segment_dirs + l1_segment_dirs + l2_segment_dirs;
    assert!(
        segment_dirs >= 1,
        "expected at least one segment directory across levels, got {segment_dirs} (L0={l0_segment_dirs}, L1={l1_segment_dirs}, L2={l2_segment_dirs})"
    );

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let points = storage.select("collision_metric", &[], 0, 20).unwrap();
    assert_eq!(points.len(), 2);
    assert!(points
        .iter()
        .any(|p| (p.value_as_f64().unwrap_or(f64::NAN) - 1.0).abs() < 1e-12));
    assert!(points
        .iter()
        .any(|p| (p.value_as_f64().unwrap_or(f64::NAN) - 2.0).abs() < 1e-12));
}
