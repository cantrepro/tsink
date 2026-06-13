use super::*;

#[test]
fn timestamp_precision_changes_retention_unit_conversion() {
    let seconds_storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(1))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    seconds_storage
        .insert_rows(&[Row::new("seconds", DataPoint::new(0, 1.0))])
        .unwrap();
    seconds_storage
        .insert_rows(&[Row::new("seconds", DataPoint::new(2, 2.0))])
        .unwrap();
    let seconds_points = seconds_storage.select("seconds", &[], 0, 10).unwrap();
    assert_eq!(seconds_points, vec![DataPoint::new(2, 2.0)]);

    let millis_storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(1))
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    millis_storage
        .insert_rows(&[Row::new("millis", DataPoint::new(0, 1.0))])
        .unwrap();
    millis_storage
        .insert_rows(&[Row::new("millis", DataPoint::new(2, 2.0))])
        .unwrap();
    let millis_points = millis_storage.select("millis", &[], 0, 10).unwrap();
    assert_eq!(millis_points.len(), 2);
}

#[test]
fn write_limiter_respects_configured_timeout() {
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_window: i64::MAX,
            retention_enforced: false,
            partition_window: i64::MAX,
            max_writers: 1,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: true,
        },
    )
    .unwrap();

    let _held_permit = storage.write_limiter.acquire();
    let err = storage
        .insert_rows(&[Row::new("write_timeout_metric", DataPoint::new(1, 1.0))])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WriteTimeout {
            timeout_ms: 0,
            workers: 1
        }
    ));
}

#[test]
fn admission_pressure_drain_times_out_when_another_writer_is_in_flight() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            retention_window: i64::MAX,
            retention_enforced: false,
            partition_window: i64::MAX,
            max_writers: 2,
            write_timeout: Duration::from_millis(100),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: 1,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: true,
        },
    )
    .unwrap();

    let _held_permit = storage.write_limiter.acquire();
    let err = storage
        .insert_rows(&[Row::new(
            "wal_pressure_drain_metric",
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::WriteTimeout { workers: 2, .. }));
}

#[test]
fn memory_pressure_drain_times_out_when_another_writer_is_in_flight() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            retention_window: i64::MAX,
            retention_enforced: false,
            partition_window: i64::MAX,
            max_writers: 2,
            write_timeout: Duration::from_millis(100),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: true,
        },
    )
    .unwrap();

    storage
        .insert_rows(&[
            Row::new("memory_pressure_drain_metric", DataPoint::new(1, 1.0)),
            Row::new("memory_pressure_drain_metric", DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    storage
        .memory_budget_bytes
        .store(1, std::sync::atomic::Ordering::Release);
    storage.refresh_memory_usage();

    let _held_permit = storage.write_limiter.acquire();
    let err = storage.enforce_memory_budget_if_needed().unwrap_err();
    assert!(matches!(err, TsinkError::WriteTimeout { workers: 2, .. }));
}

#[test]
fn close_blocks_until_in_flight_writer_releases_permit() {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 1,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
            },
        )
        .unwrap(),
    );
    let labels = vec![Label::new("host", "a")];

    let held_permit = storage.write_limiter.acquire();

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let result = writer_storage.insert_rows(&[Row::with_labels(
            "close_race_metric",
            writer_labels,
            DataPoint::new(1, 1.0),
        )]);
        writer_tx.send(result).unwrap();
    });

    assert!(writer_rx.recv_timeout(Duration::from_millis(100)).is_err());

    let close_storage = Arc::clone(&storage);
    let (close_tx, close_rx) = mpsc::channel();
    let closer = thread::spawn(move || {
        let result = close_storage.close();
        close_tx.send(result).unwrap();
    });

    assert!(close_rx.recv_timeout(Duration::from_millis(100)).is_err());

    drop(held_permit);

    let close_result = close_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(close_result.is_ok());

    let writer_result = writer_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(writer_result, Err(TsinkError::StorageClosed)));

    writer.join().unwrap();
    closer.join().unwrap();
}
