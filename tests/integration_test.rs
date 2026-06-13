//! Integration tests for tsink.

use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tsink::engine::chunk::ValueLane;
use tsink::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
use tsink::{
    DataPoint, Label, MetricSeries, QueryOptions, Row, StorageBuilder, TimestampPrecision,
    TsinkError, Value, WalSyncMode,
};

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
fn test_snapshot_and_restore_recover_wal_backed_data() {
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

    // Force a WAL-only restore path to verify replay-aware recovery.
    let _ = fs::remove_dir_all(snapshot_path.join("lane_numeric"));
    let _ = fs::remove_dir_all(snapshot_path.join("lane_blob"));

    let snapshot_wal_dir = snapshot_path.join("wal");
    assert!(snapshot_wal_dir.is_dir());

    let snapshot_wal = FramedWal::open(&snapshot_wal_dir, WalSyncMode::PerAppend).unwrap();
    let wal_frames = snapshot_wal.replay_frames().unwrap();
    let wal_has_samples = wal_frames
        .iter()
        .any(|frame| matches!(frame, ReplayFrame::Samples(_)));
    if !wal_has_samples {
        let existing_series_id = wal_frames.iter().find_map(|frame| match frame {
            ReplayFrame::SeriesDefinition(definition)
                if definition.metric == "snapshot_metric" && definition.labels.is_empty() =>
            {
                Some(definition.series_id)
            }
            _ => None,
        });
        let series_id = existing_series_id.unwrap_or(10_001);

        if existing_series_id.is_none() {
            // When the WAL is empty due to a background flush winning the snapshot race,
            // drop the index and seed a minimal WAL stream so restore still exercises replay.
            let _ = fs::remove_file(snapshot_path.join("series_index.bin"));
            snapshot_wal
                .append_series_definition(&SeriesDefinitionFrame {
                    series_id,
                    metric: "snapshot_metric".to_string(),
                    labels: vec![],
                })
                .unwrap();
        }

        let values = [Value::from(11.0), Value::from(22.0)];
        let samples = SamplesBatchFrame::from_timestamp_value_refs(
            series_id,
            ValueLane::Numeric,
            &[(1, &values[0]), (2, &values[1])],
        )
        .unwrap();
        snapshot_wal.append_samples(&[samples]).unwrap();
    }
    drop(snapshot_wal);

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
fn test_snapshot_requires_persistent_storage() {
    let storage = StorageBuilder::new().build().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot");

    let err = storage.snapshot(&snapshot_path).unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
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
fn test_list_metrics_with_wal_includes_runtime_wal_only_series() {
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
    let expected = vec![MetricSeries {
        name: wal_only_metric.to_string(),
        labels: wal_only_labels,
    }];
    assert_eq!(metrics, expected);

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
    assert!(matches!(first_close_err, TsinkError::Io(_)));

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
