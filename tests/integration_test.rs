//! Integration tests for tsink.

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision, TsinkError};

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
    assert_eq!(points[0].value, 1.0);
    assert_eq!(points[1].value, 2.0);
    assert_eq!(points[2].value, 3.0);
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
    assert_eq!(points1[0].value, 10.0);

    let points2 = storage.select("cpu", &labels2, 999, 1001).unwrap();
    assert_eq!(points2.len(), 1);
    assert_eq!(points2[0].value, 20.0);
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
fn test_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();

    // Insert data and close
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

    // Reopen and verify data
    {
        let storage = StorageBuilder::new()
            .with_data_path(data_path)
            .build()
            .unwrap();

        let points = storage.select("persistent_metric", &[], 999, 1002).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].value, 100.0);
        assert_eq!(points[1].value, 101.0);
    }
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

    // Should be returned in order
    assert_eq!(points[0].timestamp, 1000);
    assert_eq!(points[1].timestamp, 1001);
    assert_eq!(points[2].timestamp, 1002);
}

#[test]
fn test_concurrent_writes() {
    // Use in-memory storage for more reliable testing
    let storage = Arc::new(
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap(),
    );

    // First, test that single-threaded writes work
    let test_timestamp = 1_000_000;
    let test_row = vec![Row::new(
        "test_metric",
        DataPoint::new(test_timestamp, 42.0),
    )];
    storage.insert_rows(&test_row).unwrap();

    // Verify the test write worked
    let test_points = storage
        .select("test_metric", &[], test_timestamp - 1, test_timestamp + 1)
        .unwrap();
    assert_eq!(test_points.len(), 1);
    assert_eq!(test_points[0].value, 42.0);

    // Now test concurrent writes with a shared metric name
    let mut handles = vec![];
    let base_timestamp = 2_000_000;

    for i in 0..10 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            let rows = vec![Row::new(
                "concurrent_metric", // Use same metric name for all
                DataPoint::new(base_timestamp + i as i64, i as f64),
            )];
            storage.insert_rows(&rows).unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all data points were inserted for the shared metric
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

    // Check that all values are present
    let mut values: Vec<f64> = points.iter().map(|p| p.value).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let expected: Vec<f64> = (0..10).map(|i| i as f64).collect();
    assert_eq!(values, expected);
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

    // Create initial on-disk partitions
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

    // Reopen; there will be disk partitions already loaded
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(1))
        .with_wal_enabled(true)
        .build()
        .unwrap();

    // Should still accept new writes
    storage
        .insert_rows(&[Row::new("persist", DataPoint::new(3, 3.0))])
        .unwrap();

    // Verify the new point is visible before close
    let mut live_points = storage.select("persist", &[], 0, 10).unwrap();
    live_points.sort_by_key(|p| p.timestamp);
    assert!(
        live_points
            .iter()
            .any(|p| p.timestamp == 3 && (p.value - 3.0).abs() < 1e-12),
        "newly inserted point should be present before close"
    );
    storage.close().unwrap();

    // Reopen and ensure the new point is still present
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
            .any(|p| p.timestamp == 3 && (p.value - 3.0).abs() < 1e-12),
        "newly inserted point should survive close/reopen even with existing disk partitions"
    );
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

    // Insert out-of-order but spanning multiple partitions
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
