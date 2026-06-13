use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tempfile::TempDir;
use tsink::memory::{MemoryPartition, flush_memory_partition_to_disk};
use tsink::partition::{Partition, SharedPartition};
use tsink::wal::{NopWal, Wal};
use tsink::{DataPoint, Label, Row, TimestampPrecision};

fn new_memory_partition(duration: Duration, precision: TimestampPrecision) -> SharedPartition {
    let wal: Arc<dyn Wal> = Arc::new(NopWal);
    Arc::new(MemoryPartition::new(
        wal,
        duration,
        precision,
        Duration::from_secs(24 * 3600),
    ))
}

#[test]
fn memory_partition_handles_zero_timestamp_and_out_of_order() {
    let partition =
        new_memory_partition(Duration::from_secs(3600), TimestampPrecision::Nanoseconds);

    let baseline = Utc::now().timestamp_nanos_opt().unwrap();
    let rows = vec![
        Row::new("test_metric", DataPoint::new(0, 1.0)),
        Row::new("test_metric", DataPoint::new(baseline + 10, 2.0)),
        Row::new("test_metric", DataPoint::new(baseline - 10, 3.0)),
    ];

    let outdated = partition.insert_rows(&rows).unwrap();
    assert!(outdated.is_empty(), "no rows should be treated as outdated");

    let points = partition
        .select_data_points("test_metric", &[], 0, i64::MAX)
        .unwrap();
    assert_eq!(points.len(), 3);
    assert!(points.iter().all(|p| p.timestamp != 0));
    assert!(points.windows(2).all(|w| w[0].timestamp <= w[1].timestamp));

    let values: Vec<f64> = points.iter().map(|p| p.value).collect();
    assert!(values.contains(&1.0));
    assert!(values.contains(&2.0));
    assert!(values.contains(&3.0));

    assert_eq!(partition.size(), 3);
}

#[test]
fn memory_partition_returns_outdated_rows() {
    let partition = new_memory_partition(Duration::from_secs(60), TimestampPrecision::Seconds);

    let newer = Row::new("metric", DataPoint::new(1000, 1.0));
    let older = Row::new("metric", DataPoint::new(880, 2.0));
    let rows = vec![newer.clone(), older.clone()];

    let outdated = partition.insert_rows(&rows).unwrap();

    assert_eq!(outdated.len(), 1);
    assert_eq!(
        outdated[0].data_point().timestamp,
        older.data_point().timestamp
    );
    assert_eq!(partition.size(), 1);
}

#[test]
fn memory_partition_filters_rows_far_behind_newest_in_fresh_batch() {
    let partition = new_memory_partition(Duration::from_secs(60), TimestampPrecision::Seconds);

    let rows = vec![
        Row::new("metric", DataPoint::new(1000, 1.0)),
        Row::new("metric", DataPoint::new(100, 2.0)),
    ];

    let outdated = partition.insert_rows(&rows).unwrap();
    assert_eq!(outdated.len(), 1);
    assert_eq!(outdated[0].data_point().timestamp, 100);

    let stored = partition
        .select_data_points("metric", &[], 0, 2000)
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].timestamp, 1000);
    assert!((stored[0].value - 1.0).abs() < 1e-12);
}

#[test]
fn memory_partition_tracks_negative_max_timestamp() {
    let partition = new_memory_partition(Duration::from_secs(60), TimestampPrecision::Seconds);

    let rows = vec![
        Row::new("metric", DataPoint::new(-50, 1.0)),
        Row::new("metric", DataPoint::new(-10, 2.0)),
    ];

    let outdated = partition.insert_rows(&rows).unwrap();
    assert!(outdated.is_empty());
    assert_eq!(partition.max_timestamp(), -10);

    let stored = partition
        .select_data_points("metric", &[], -200, 0)
        .unwrap();
    assert_eq!(stored.len(), 2);
}

#[test]
fn flush_memory_partition_round_trip_to_disk() {
    let partition = new_memory_partition(Duration::from_secs(600), TimestampPrecision::Seconds);

    let rows = vec![
        Row::with_labels(
            "disk_metric",
            vec![Label::new("host", "alpha")],
            DataPoint::new(10, 5.0),
        ),
        Row::with_labels(
            "disk_metric",
            vec![Label::new("host", "alpha")],
            DataPoint::new(12, 7.5),
        ),
    ];

    partition.insert_rows(&rows).unwrap();

    let temp_dir = TempDir::new().unwrap();
    let disk_partition = flush_memory_partition_to_disk(
        partition.clone(),
        temp_dir.path(),
        Duration::from_secs(3600),
    )
    .unwrap();

    let disk_points = disk_partition
        .select_data_points("disk_metric", &[Label::new("host", "alpha")], 0, 100)
        .unwrap();

    assert_eq!(disk_points.len(), 2);
    assert_eq!(disk_points[0].timestamp, 10);
    assert_eq!(disk_points[1].timestamp, 12);
    assert_eq!(disk_points[0].value, 5.0);
    assert_eq!(disk_points[1].value, 7.5);

    // Selecting via select_all should surface the same series
    let grouped = disk_partition
        .select_all_labels("disk_metric", 0, 100)
        .unwrap();
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].0, vec![Label::new("host", "alpha")]);
    assert_eq!(grouped[0].1.len(), 2);
}
