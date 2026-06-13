use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use tempfile::TempDir;
use tsink::memory::{MemoryPartition, flush_memory_partition_to_disk};
use tsink::partition::{Partition, SharedPartition};
use tsink::wal::{NopWal, Wal};
use tsink::{DataPoint, Label, Row, TimestampPrecision};

struct BlockingFlushWal {
    flush_started_tx: mpsc::Sender<()>,
    unblock_flush_rx: Mutex<mpsc::Receiver<()>>,
}

impl Wal for BlockingFlushWal {
    fn append_rows(&self, _rows: &[Row]) -> tsink::Result<()> {
        Ok(())
    }

    fn flush(&self) -> tsink::Result<()> {
        self.flush_started_tx.send(()).unwrap();
        self.unblock_flush_rx.lock().unwrap().recv().unwrap();
        Ok(())
    }

    fn punctuate(&self) -> tsink::Result<()> {
        Ok(())
    }

    fn remove_oldest(&self) -> tsink::Result<()> {
        Ok(())
    }

    fn remove_all(&self) -> tsink::Result<()> {
        Ok(())
    }

    fn refresh(&self) -> tsink::Result<()> {
        Ok(())
    }
}

fn new_memory_partition_with_wal(
    wal: Arc<dyn Wal>,
    duration: Duration,
    precision: TimestampPrecision,
) -> SharedPartition {
    Arc::new(MemoryPartition::new(
        wal,
        duration,
        precision,
        Duration::from_secs(24 * 3600),
    ))
}

fn new_memory_partition(duration: Duration, precision: TimestampPrecision) -> SharedPartition {
    new_memory_partition_with_wal(Arc::new(NopWal), duration, precision)
}

#[test]
fn memory_partition_empty_insert_is_noop() {
    let partition =
        new_memory_partition(Duration::from_secs(3600), TimestampPrecision::Nanoseconds);

    let outdated = partition.insert_rows(&[]).unwrap();
    assert!(outdated.is_empty());
    assert_eq!(partition.size(), 0);
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

    let grouped = disk_partition
        .select_all_labels("disk_metric", 0, 100)
        .unwrap();
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].0, vec![Label::new("host", "alpha")]);
    assert_eq!(grouped[0].1.len(), 2);
}

#[test]
fn public_flush_helper_seals_partition_while_flushing() {
    let (flush_started_tx, flush_started_rx) = mpsc::channel();
    let (unblock_flush_tx, unblock_flush_rx) = mpsc::channel();
    let wal: Arc<dyn Wal> = Arc::new(BlockingFlushWal {
        flush_started_tx,
        unblock_flush_rx: Mutex::new(unblock_flush_rx),
    });
    let partition =
        new_memory_partition_with_wal(wal, Duration::from_secs(600), TimestampPrecision::Seconds);
    partition
        .insert_rows(&[Row::new("metric", DataPoint::new(1, 1.0))])
        .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let flush_path = temp_dir.path().to_path_buf();
    let partition_for_flush = partition.clone();
    let flush_thread = thread::spawn(move || {
        flush_memory_partition_to_disk(partition_for_flush, flush_path, Duration::from_secs(3600))
    });

    flush_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not start in time");

    let pending_row = Row::new("metric", DataPoint::new(2, 2.0));
    let rejected = partition
        .insert_rows(std::slice::from_ref(&pending_row))
        .unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].metric(), pending_row.metric());
    assert_eq!(
        rejected[0].data_point().timestamp,
        pending_row.data_point().timestamp
    );
    assert_eq!(
        rejected[0].data_point().value,
        pending_row.data_point().value
    );
    assert_eq!(partition.size(), 1);

    unblock_flush_tx.send(()).unwrap();
    let disk_partition = flush_thread
        .join()
        .expect("flush thread panicked")
        .expect("flush helper failed");

    let disk_points = disk_partition
        .select_data_points("metric", &[], 0, 10)
        .unwrap();
    assert_eq!(disk_points.len(), 1);
    assert_eq!(disk_points[0].timestamp, 1);
    assert_eq!(disk_points[0].value, 1.0);

    let accepted = partition.insert_rows(&[pending_row]).unwrap();
    assert!(accepted.is_empty());
    assert_eq!(partition.size(), 2);
}
