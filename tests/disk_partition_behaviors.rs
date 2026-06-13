use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tempfile::TempDir;
use tsink::disk::{DiskMetric, DiskPartition, PartitionMeta};
use tsink::partition::Partition;
use tsink::{DataPoint, Row, TsinkError};

fn make_meta(metrics: HashMap<String, DiskMetric>) -> PartitionMeta {
    PartitionMeta {
        min_timestamp: 0,
        max_timestamp: 100,
        num_data_points: metrics.values().map(|m| m.num_data_points).sum(),
        metrics,
        created_at: SystemTime::now(),
    }
}

#[test]
fn disk_partition_reports_invalid_offsets() {
    let temp_dir = TempDir::new().unwrap();

    let mut metrics = HashMap::new();
    metrics.insert(
        "bad_metric".to_string(),
        DiskMetric {
            name: "bad_metric".to_string(),
            offset: 1024,
            encoded_size: 16,
            min_timestamp: 0,
            max_timestamp: 10,
            num_data_points: 1,
        },
    );

    let partition = DiskPartition::create(
        temp_dir.path(),
        make_meta(metrics),
        vec![0u8; 16],
        Duration::from_secs(60),
    )
    .unwrap();

    let err = partition
        .select_data_points("bad_metric", &[], 0, 100)
        .unwrap_err();

    match err {
        TsinkError::InvalidOffset { offset, .. } => assert_eq!(offset, 1024),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn disk_partition_expires_and_prevents_reads() {
    let temp_dir = TempDir::new().unwrap();

    let mut metrics = HashMap::new();
    metrics.insert(
        "expired_metric".to_string(),
        DiskMetric {
            name: "expired_metric".to_string(),
            offset: 0,
            encoded_size: 16,
            min_timestamp: 0,
            max_timestamp: 10,
            num_data_points: 0,
        },
    );

    let meta = PartitionMeta {
        created_at: SystemTime::now() - Duration::from_secs(600),
        ..make_meta(metrics)
    };

    let partition = DiskPartition::create(
        temp_dir.path(),
        meta,
        vec![0u8; 16],
        Duration::from_secs(60),
    )
    .unwrap();

    assert!(partition.expired());
    let err = partition
        .select_data_points("expired_metric", &[], 0, 100)
        .unwrap_err();
    assert!(matches!(err, TsinkError::NoDataPoints { .. }));
}

#[test]
fn disk_partition_clean_removes_files() {
    let temp_dir = TempDir::new().unwrap();

    let mut metrics = HashMap::new();
    metrics.insert(
        "clean_metric".to_string(),
        DiskMetric {
            name: "clean_metric".to_string(),
            offset: 0,
            encoded_size: 16,
            min_timestamp: 0,
            max_timestamp: 10,
            num_data_points: 1,
        },
    );

    let data = {
        // Minimal encoded payload: flush a single point through MemoryPartition encoding
        let rows = vec![Row::new("clean_metric", DataPoint::new(0, 1.0))];
        let temp_mem = TempDir::new().unwrap();
        let wal: Arc<dyn tsink::wal::Wal> = Arc::new(tsink::wal::NopWal);
        let partition: tsink::partition::SharedPartition =
            Arc::new(tsink::memory::MemoryPartition::new(
                wal,
                Duration::from_secs(60),
                tsink::TimestampPrecision::Seconds,
            ));
        partition.insert_rows(&rows).unwrap();
        tsink::memory::flush_memory_partition_to_disk(
            partition.clone(),
            temp_mem.path(),
            Duration::from_secs(60),
        )
        .unwrap();
        std::fs::read(temp_mem.path().join(tsink::disk::DATA_FILE_NAME)).unwrap()
    };

    let partition = DiskPartition::create(
        temp_dir.path(),
        make_meta(metrics),
        data,
        Duration::from_secs(60),
    )
    .unwrap();

    assert!(temp_dir.path().join(tsink::disk::DATA_FILE_NAME).exists());
    partition.clean().unwrap();
    assert!(!temp_dir.path().exists());
}
