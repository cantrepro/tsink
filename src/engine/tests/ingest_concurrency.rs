use super::*;

#[test]
fn list_metrics_remains_available_while_writer_waits_on_active_lock() {
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
                max_writers: 2,
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

    let active_read_guards = storage
        .active_builders
        .iter()
        .map(|shard| shard.read())
        .collect::<Vec<_>>();

    let writer_storage = Arc::clone(&storage);
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let result = writer_storage
            .insert_rows(&[Row::new("read_concurrency_metric", DataPoint::new(1, 1.0))]);
        writer_tx.send(result).unwrap();
    });

    thread::sleep(Duration::from_millis(75));

    let reader_storage = Arc::clone(&storage);
    let (reader_tx, reader_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let result = reader_storage.list_metrics();
        reader_tx.send(result).unwrap();
    });

    let reader_result = reader_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("list_metrics should not block on in-flight WAL/ingest work");
    assert!(reader_result.is_ok());

    drop(active_read_guards);

    let writer_result = writer_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(writer_result.is_ok());

    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn writer_waiting_on_one_metric_shard_does_not_block_other_metric_shards() {
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
                max_writers: 2,
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
    let metric_a = "registry_shard_metric_a";
    let metric_a_shard = ChunkStorage::registry_metric_shard_idx(metric_a);
    let metric_b = (0..1024)
        .map(|idx| format!("registry_shard_metric_b_{idx}"))
        .find(|candidate| ChunkStorage::registry_metric_shard_idx(candidate) != metric_a_shard)
        .expect("expected to find a metric mapped to a different registry shard");

    storage
        .insert_rows(&[Row::with_labels(
            metric_a,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            metric_b.as_str(),
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_a = storage
        .registry
        .read()
        .resolve_existing(metric_a, &labels)
        .unwrap()
        .series_id;
    let active_shard_a = ChunkStorage::series_shard_idx(series_a);
    let active_read_guard = storage.active_builders[active_shard_a].read();

    let writer_a_storage = Arc::clone(&storage);
    let writer_a_labels = labels.clone();
    let writer_a_metric = metric_a.to_string();
    let (writer_a_tx, writer_a_rx) = mpsc::channel();
    let writer_a = thread::spawn(move || {
        let result = writer_a_storage.insert_rows(&[Row::with_labels(
            writer_a_metric.as_str(),
            writer_a_labels,
            DataPoint::new(2, 2.0),
        )]);
        writer_a_tx.send(result).unwrap();
    });

    thread::sleep(Duration::from_millis(75));

    let writer_b_storage = Arc::clone(&storage);
    let writer_b_labels = labels.clone();
    let writer_b_metric = metric_b.clone();
    let (writer_b_tx, writer_b_rx) = mpsc::channel();
    let writer_b = thread::spawn(move || {
        let result = writer_b_storage.insert_rows(&[Row::with_labels(
            writer_b_metric.as_str(),
            writer_b_labels,
            DataPoint::new(2, 2.0),
        )]);
        writer_b_tx.send(result).unwrap();
    });

    let writer_b_result = writer_b_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("writer on a different metric shard should not block");
    assert!(writer_b_result.is_ok());
    assert!(writer_a_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    drop(active_read_guard);

    let writer_a_result = writer_a_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(writer_a_result.is_ok());

    writer_a.join().unwrap();
    writer_b.join().unwrap();
}

#[test]
fn concurrent_lane_mismatch_does_not_log_failed_write_to_wal() {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 2,
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
    let start = Arc::new(Barrier::new(3));
    let (tx, rx) = mpsc::channel();

    let active_read_guards = storage
        .active_builders
        .iter()
        .map(|shard| shard.read())
        .collect::<Vec<_>>();

    let thread_storage = Arc::clone(&storage);
    let thread_labels = labels.clone();
    let thread_start = Arc::clone(&start);
    let thread_tx = tx.clone();
    let numeric_writer = thread::spawn(move || {
        thread_start.wait();
        let result = thread_storage.insert_rows(&[Row::with_labels(
            "lane_race_metric",
            thread_labels,
            DataPoint::new(1, 1.0),
        )]);
        thread_tx.send(result).unwrap();
    });

    let thread_storage = Arc::clone(&storage);
    let thread_labels = labels.clone();
    let thread_start = Arc::clone(&start);
    let blob_writer = thread::spawn(move || {
        thread_start.wait();
        let result = thread_storage.insert_rows(&[Row::with_labels(
            "lane_race_metric",
            thread_labels,
            DataPoint::new(2, "blob"),
        )]);
        tx.send(result).unwrap();
    });

    start.wait();

    let mut pre_release_sample_batches = 0usize;
    let deadline = Instant::now() + Duration::from_millis(250);
    while Instant::now() < deadline {
        pre_release_sample_batches = storage
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
        if pre_release_sample_batches > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(pre_release_sample_batches, 0);

    drop(active_read_guards);

    let first = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let second = rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let mut ok_count = 0usize;
    let mut mismatch_count = 0usize;
    for result in [first, second] {
        match result {
            Ok(()) => ok_count += 1,
            Err(TsinkError::ValueTypeMismatch { .. }) => mismatch_count += 1,
            Err(other) => panic!("unexpected insert result: {other}"),
        }
    }
    assert_eq!(ok_count, 1);
    assert_eq!(mismatch_count, 1);

    numeric_writer.join().unwrap();
    blob_writer.join().unwrap();

    let final_sample_batches: usize = storage
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
    assert_eq!(final_sample_batches, 1);
}

#[test]
fn close_clears_metadata_only_wal_when_no_chunks_are_sealed() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let metric = "phantom_metric";

    {
        let wal = FramedWal::open(temp_dir.path().join("wal"), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 42,
            metric: metric.to_string(),
            labels: labels.clone(),
        })
        .unwrap();
    }

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let has_phantom_without_wal = storage
            .list_metrics()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels);
        assert!(!has_phantom_without_wal);

        let has_phantom_with_wal = storage
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels);
        assert!(has_phantom_with_wal);

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let has_phantom = reopened
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_phantom);

    let has_phantom = reopened
        .list_metrics_with_wal()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_phantom);

    reopened.close().unwrap();
}
