use super::*;

#[test]
fn list_metrics_remains_available_while_writer_waits_on_active_lock() {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            128,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let active_read_guards = storage
        .chunks
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
fn writer_waiting_on_one_series_shard_does_not_block_other_series_shards_for_same_metric() {
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
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let metric = "registry_shard_metric";
    let labels_a = vec![Label::new("host", "a")];
    let series_a_registry_shard = ChunkStorage::registry_series_shard_idx(metric, &labels_a);
    let labels_b = (0..1024usize)
        .map(|idx| vec![Label::new("host", format!("b-{idx}"))])
        .find(|candidate| {
            ChunkStorage::registry_series_shard_idx(metric, candidate.as_slice())
                != series_a_registry_shard
        })
        .expect("expected to find a labelset mapped to a different registry write shard");

    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels_a.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels_b.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_a = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels_a)
        .unwrap()
        .series_id;
    let series_b = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels_b)
        .unwrap()
        .series_id;
    let active_shard_a = ChunkStorage::series_shard_idx(series_a);
    assert_ne!(active_shard_a, ChunkStorage::series_shard_idx(series_b));
    let active_read_guard = storage.chunks.active_builders[active_shard_a].read();

    let writer_a_storage = Arc::clone(&storage);
    let writer_a_labels = labels_a.clone();
    let writer_metric = metric.to_string();
    let (writer_a_tx, writer_a_rx) = mpsc::channel();
    let writer_a = thread::spawn(move || {
        let result = writer_a_storage.insert_rows(&[Row::with_labels(
            writer_metric.as_str(),
            writer_a_labels,
            DataPoint::new(2, 2.0),
        )]);
        writer_a_tx.send(result).unwrap();
    });

    thread::sleep(Duration::from_millis(75));

    let writer_b_storage = Arc::clone(&storage);
    let writer_b_labels = labels_b.clone();
    let writer_b_metric = metric.to_string();
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
        .expect("writer on a different series shard should not block");
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
fn concurrent_same_metric_new_series_writers_create_one_series() {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let metric = "concurrent_same_metric_new_series";
    let labels = vec![Label::new("host", "a"), Label::new("series", "0")];
    let start = Arc::new(Barrier::new(3));
    let (tx, rx) = mpsc::channel();
    let mut handles = Vec::new();

    for ts in [1i64, 2i64] {
        let writer_storage = Arc::clone(&storage);
        let writer_labels = labels.clone();
        let writer_start = Arc::clone(&start);
        let writer_tx = tx.clone();
        handles.push(thread::spawn(move || {
            writer_start.wait();
            let result = writer_storage.insert_rows(&[Row::with_labels(
                metric,
                writer_labels,
                DataPoint::new(ts, ts as f64),
            )]);
            writer_tx.send(result).unwrap();
        }));
    }

    start.wait();

    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());

    for handle in handles {
        handle.join().unwrap();
    }

    let series_ids = storage
        .catalog
        .registry
        .read()
        .series_ids_for_metric(metric);
    assert_eq!(series_ids.len(), 1);
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
}

#[test]
fn concurrent_distinct_new_series_writers_same_metric_create_all_series() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    const WRITERS: usize = 8;
    const SERIES_PER_WRITER: usize = 8;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: WRITERS,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let metric = "concurrent_distinct_new_series_same_metric";
    let start = Arc::new(Barrier::new(WRITERS + 1));
    let mut handles = Vec::with_capacity(WRITERS);

    for writer_id in 0..WRITERS {
        let writer_storage = Arc::clone(&storage);
        let writer_start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let rows = (0..SERIES_PER_WRITER)
                .map(|series_idx| {
                    Row::with_labels(
                        metric,
                        vec![
                            Label::new("writer", format!("w{writer_id}")),
                            Label::new("series", format!("s{series_idx}")),
                        ],
                        DataPoint::new(1, writer_id as f64),
                    )
                })
                .collect::<Vec<_>>();
            writer_start.wait();
            writer_storage.insert_rows(&rows)
        }));
    }

    start.wait();

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    assert_eq!(
        storage
            .catalog
            .registry
            .read()
            .series_ids_for_metric(metric)
            .len(),
        WRITERS * SERIES_PER_WRITER
    );
    for writer_id in 0..WRITERS {
        for series_idx in 0..SERIES_PER_WRITER {
            let labels = vec![
                Label::new("writer", format!("w{writer_id}")),
                Label::new("series", format!("s{series_idx}")),
            ];
            assert_eq!(
                storage.select(metric, &labels, 0, 10).unwrap(),
                vec![DataPoint::new(1, writer_id as f64)]
            );
        }
    }
}

#[test]
fn concurrent_distinct_new_series_writers_across_metrics_create_all_series() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    const WRITERS: usize = 8;
    const SERIES_PER_WRITER: usize = 8;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: WRITERS,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let start = Arc::new(Barrier::new(WRITERS + 1));
    let mut handles = Vec::with_capacity(WRITERS);

    for writer_id in 0..WRITERS {
        let writer_storage = Arc::clone(&storage);
        let writer_start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let rows = (0..SERIES_PER_WRITER)
                .map(|series_idx| {
                    let metric = format!("concurrent_distinct_metric_{writer_id}");
                    Row::with_labels(
                        metric.as_str(),
                        vec![Label::new("series", format!("s{series_idx}"))],
                        DataPoint::new(1, series_idx as f64),
                    )
                })
                .collect::<Vec<_>>();
            writer_start.wait();
            writer_storage.insert_rows(&rows)
        }));
    }

    start.wait();

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    for writer_id in 0..WRITERS {
        let metric = format!("concurrent_distinct_metric_{writer_id}");
        assert_eq!(
            storage
                .catalog
                .registry
                .read()
                .series_ids_for_metric(metric.as_str())
                .len(),
            SERIES_PER_WRITER
        );
        for series_idx in 0..SERIES_PER_WRITER {
            let labels = vec![Label::new("series", format!("s{series_idx}"))];
            assert_eq!(
                storage.select(metric.as_str(), &labels, 0, 10).unwrap(),
                vec![DataPoint::new(1, series_idx as f64)]
            );
        }
    }
}

#[test]
fn cached_missing_label_selector_sees_concurrent_new_series() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    const WRITERS: usize = 6;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: WRITERS,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let metric = "cached_missing_label_selector";
    storage
        .insert_rows(&[
            Row::with_labels(
                metric,
                vec![Label::new("host", "seed-a"), Label::new("job", "api")],
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                metric,
                vec![Label::new("host", "seed-b")],
                DataPoint::new(1, 1.0),
            ),
        ])
        .unwrap();

    let missing_selection = SeriesSelection::new()
        .with_metric(metric)
        .with_matcher(SeriesMatcher::equal("job", ""));
    let missing_before = storage.select_series(&missing_selection).unwrap();
    assert_eq!(missing_before.len(), 1);

    let start = Arc::new(Barrier::new(WRITERS + 1));
    let mut handles = Vec::with_capacity(WRITERS);
    let mut expected_missing = 1usize;
    for writer_id in 0..WRITERS {
        let writer_storage = Arc::clone(&storage);
        let writer_start = Arc::clone(&start);
        let without_job = writer_id % 2 == 0;
        if without_job {
            expected_missing += 1;
        }
        handles.push(thread::spawn(move || {
            let mut labels = vec![Label::new("host", format!("writer-{writer_id}"))];
            if !without_job {
                labels.push(Label::new("job", format!("worker-{writer_id}")));
            }
            writer_start.wait();
            writer_storage.insert_rows(&[Row::with_labels(
                metric,
                labels,
                DataPoint::new(2 + writer_id as i64, writer_id as f64),
            )])
        }));
    }

    start.wait();
    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let missing_after = storage.select_series(&missing_selection).unwrap();
    assert_eq!(missing_after.len(), expected_missing);
    assert!(missing_after.iter().all(
        |series| series.name == metric && series.labels.iter().all(|label| label.name != "job")
    ));
}

#[test]
fn existing_series_fanout_writers_do_not_block_on_registry_readers() {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    const WRITERS: usize = 8;
    const SERIES_PER_WRITER: usize = 8;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: WRITERS,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let writer_rows = (0..WRITERS)
        .map(|writer_id| {
            (0..SERIES_PER_WRITER)
                .map(|series_idx| {
                    let metric = format!("registry_reader_fanout_metric_{}", writer_id % 2);
                    let labels = vec![
                        Label::new("writer", format!("w{writer_id}")),
                        Label::new("series", format!("s{series_idx}")),
                    ];
                    storage
                        .insert_rows(&[Row::with_labels(
                            metric.as_str(),
                            labels.clone(),
                            DataPoint::new(1, 1.0),
                        )])
                        .unwrap();
                    (metric, labels)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let registry_reader = storage.catalog.registry.read();
    let start = Arc::new(Barrier::new(WRITERS + 1));
    let (tx, rx) = mpsc::channel();
    let mut handles = Vec::with_capacity(WRITERS);

    for (writer_id, rows_for_writer) in writer_rows.into_iter().enumerate() {
        let writer_storage = Arc::clone(&storage);
        let writer_start = Arc::clone(&start);
        let writer_tx = tx.clone();
        handles.push(thread::spawn(move || {
            let rows = rows_for_writer
                .into_iter()
                .enumerate()
                .map(|(series_idx, (metric, labels))| {
                    Row::with_labels(
                        metric.as_str(),
                        labels,
                        DataPoint::new(10 + series_idx as i64, writer_id as f64),
                    )
                })
                .collect::<Vec<_>>();
            writer_start.wait();
            let result = writer_storage.insert_rows(&rows);
            writer_tx.send(result).unwrap();
        }));
    }

    start.wait();

    for _ in 0..WRITERS {
        assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());
    }

    drop(registry_reader);

    for handle in handles {
        handle.join().unwrap();
    }

    for writer_id in 0..WRITERS {
        for series_idx in 0..SERIES_PER_WRITER {
            let metric = format!("registry_reader_fanout_metric_{}", writer_id % 2);
            let labels = vec![
                Label::new("writer", format!("w{writer_id}")),
                Label::new("series", format!("s{series_idx}")),
            ];
            assert_eq!(
                storage.select(metric.as_str(), &labels, 0, 20).unwrap(),
                vec![
                    DataPoint::new(1, 1.0),
                    DataPoint::new(10 + series_idx as i64, writer_id as f64),
                ]
            );
        }
    }
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
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let labels = vec![Label::new("host", "a")];
    let start = Arc::new(Barrier::new(3));
    let (tx, rx) = mpsc::channel();

    let active_read_guards = storage
        .chunks
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
    assert_eq!(final_sample_batches, 1);
}

#[test]
fn slow_merge_query_releases_same_shard_writes_and_flushes() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let query_metric = "merge_query_lock_query_metric";
    let query_labels = vec![Label::new("host", "query")];
    storage
        .insert_rows(&[
            Row::with_labels(query_metric, query_labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels(query_metric, query_labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels(query_metric, query_labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            query_metric,
            vec![Label::new("host", "gone")],
            DataPoint::new(4, 4.0),
        )])
        .unwrap();
    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric(query_metric)
                .with_matcher(SeriesMatcher::equal("host", "gone"))
                .with_time_range(0, 10),
        )
        .unwrap();

    let query_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(query_metric, &query_labels)
        .unwrap()
        .series_id;
    let target_shard = ChunkStorage::series_shard_idx(query_series_id);

    let writer_metric = "merge_query_lock_writer_metric";
    let writer_labels = (0..256usize)
        .find_map(|idx| {
            let labels = vec![Label::new("host", format!("writer-{idx}"))];
            storage
                .insert_rows(&[Row::with_labels(
                    writer_metric,
                    labels.clone(),
                    DataPoint::new(10, 10.0),
                )])
                .unwrap();
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(writer_metric, &labels)
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id) == target_shard).then_some(labels)
        })
        .expect("expected to find a same-shard writer series");
    storage.flush_all_active().unwrap();

    let writer_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(writer_metric, &writer_labels)
        .unwrap()
        .series_id;
    assert_eq!(
        ChunkStorage::series_shard_idx(writer_series_id),
        target_shard
    );

    let (query_entered_tx, query_entered_rx) = mpsc::channel();
    let (query_release_tx, query_release_rx) = mpsc::channel();
    let query_release_rx = Arc::new(Mutex::new(query_release_rx));
    storage.set_query_merge_in_memory_source_snapshot_hook({
        let query_entered_tx = query_entered_tx.clone();
        let query_release_rx = Arc::clone(&query_release_rx);
        move || {
            query_entered_tx.send(()).unwrap();
            query_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let query_storage = Arc::clone(&storage);
    let query_thread =
        thread::spawn(move || query_storage.select(query_metric, &query_labels, 0, 10));

    query_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("query did not reach merge in-memory snapshot hook");

    let writer_storage = Arc::clone(&storage);
    let writer_labels_for_thread = writer_labels.clone();
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_thread = thread::spawn(move || {
        let result = writer_storage.insert_rows(&[Row::with_labels(
            writer_metric,
            writer_labels_for_thread,
            DataPoint::new(11, 11.0),
        )]);
        writer_tx.send(result).unwrap();
    });

    let writer_result = writer_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("same-shard write should not block on merge query snapshot expansion");
    assert!(writer_result.is_ok());

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        let result = flush_storage.flush_all_active();
        flush_tx.send(result).unwrap();
    });

    let flush_result = flush_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("same-shard flush should not block on merge query snapshot expansion");
    assert!(flush_result.is_ok());

    query_release_tx.send(()).unwrap();

    let query_points = query_thread.join().unwrap().unwrap();
    assert_eq!(
        query_points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
        ]
    );

    writer_thread.join().unwrap();
    flush_thread.join().unwrap();
    storage.clear_query_merge_in_memory_source_snapshot_hook();

    let sealed = storage.sealed_shard(writer_series_id).read();
    assert!(sealed.get(&writer_series_id).is_some());

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.merge_path_queries_total, 1);
    assert_eq!(snapshot.query.merge_path_shard_snapshots_total, 1);
}

#[test]
fn hot_active_merge_query_releases_same_shard_writes_during_snapshot_expansion() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            512,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let query_metric = "hot_active_merge_query_metric";
    let query_labels = vec![Label::new("host", "query")];
    let expected_points = (0..192i64)
        .map(|ts| DataPoint::new(ts, ts as f64))
        .collect::<Vec<_>>();
    let query_rows = expected_points
        .iter()
        .map(|point| Row::with_labels(query_metric, query_labels.clone(), point.clone()))
        .collect::<Vec<_>>();
    storage.insert_rows(&query_rows).unwrap();

    let query_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(query_metric, &query_labels)
        .unwrap()
        .series_id;
    let target_shard = ChunkStorage::series_shard_idx(query_series_id);

    let writer_metric = "hot_active_merge_writer_metric";
    let writer_labels = (0..1024usize)
        .find_map(|idx| {
            let labels = vec![Label::new("host", format!("writer-{idx}"))];
            storage
                .insert_rows(&[Row::with_labels(
                    writer_metric,
                    labels.clone(),
                    DataPoint::new(10_000, 10_000.0),
                )])
                .unwrap();
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(writer_metric, &labels)
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id) == target_shard).then_some(labels)
        })
        .expect("expected to find a same-shard writer series");

    let (query_entered_tx, query_entered_rx) = mpsc::channel();
    let (query_release_tx, query_release_rx) = mpsc::channel();
    let query_release_rx = Arc::new(Mutex::new(query_release_rx));
    storage.set_query_merge_in_memory_source_snapshot_hook({
        let query_entered_tx = query_entered_tx.clone();
        let query_release_rx = Arc::clone(&query_release_rx);
        move || {
            query_entered_tx.send(()).unwrap();
            query_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let query_storage = Arc::clone(&storage);
    let query_labels_for_thread = query_labels.clone();
    let query_thread =
        thread::spawn(move || query_storage.select(query_metric, &query_labels_for_thread, 0, 256));

    query_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("merge query did not reach in-memory snapshot hook");

    let writer_storage = Arc::clone(&storage);
    let writer_labels_for_thread = writer_labels.clone();
    let writer_metric_for_thread = writer_metric.to_string();
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_thread = thread::spawn(move || {
        let result = (|| -> crate::Result<()> {
            for offset in 1..=32i64 {
                writer_storage.insert_rows(&[Row::with_labels(
                    writer_metric_for_thread.as_str(),
                    writer_labels_for_thread.clone(),
                    DataPoint::new(10_000 + offset, (10_000 + offset) as f64),
                )])?;
            }
            Ok(())
        })();
        writer_tx.send(result).unwrap();
    });

    let writer_result = writer_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("same-shard writer should progress while merge query expands active snapshot");
    assert!(writer_result.is_ok());

    query_release_tx.send(()).unwrap();

    let query_points = query_thread.join().unwrap().unwrap();
    assert_eq!(query_points, expected_points);

    writer_thread.join().unwrap();
    storage.clear_query_merge_in_memory_source_snapshot_hook();

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.merge_path_queries_total, 1);
    assert_eq!(snapshot.query.append_sort_path_queries_total, 0);
    assert_eq!(snapshot.query.merge_path_shard_snapshots_total, 1);

    let writer_points = storage
        .select(writer_metric, &writer_labels, 0, 20_000)
        .unwrap();
    assert_eq!(writer_points.len(), 33);
}

#[test]
fn query_snapshot_waits_for_active_to_sealed_publication() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let storage = Arc::new(ChunkStorage::new(2, None));
    let metric = "active_to_sealed_query_visibility";
    let labels = vec![Label::new("host", "query")];

    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let (publish_entered_tx, publish_entered_rx) = mpsc::channel();
    let (publish_release_tx, publish_release_rx) = mpsc::channel();
    let publish_release_rx = Arc::new(Mutex::new(publish_release_rx));
    storage.set_ingest_pre_sealed_chunk_publish_hook({
        let publish_entered_tx = publish_entered_tx.clone();
        let publish_release_rx = Arc::clone(&publish_release_rx);
        move || {
            publish_entered_tx.send(()).unwrap();
            publish_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer_thread = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(2, 2.0),
        )])
    });

    publish_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("write did not reach the active-to-sealed publish boundary");

    let query_storage = Arc::clone(&storage);
    let query_labels = labels.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let query_thread = thread::spawn(move || {
        query_tx
            .send(query_storage.select(metric, &query_labels, 0, 10))
            .unwrap();
    });

    assert!(
        query_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "query snapshot should wait while the finalized chunk is between active mutation and sealed publication",
    );

    publish_release_tx.send(()).unwrap();

    writer_thread.join().unwrap().unwrap();
    let points = query_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    query_thread.join().unwrap();
    storage.clear_ingest_pre_sealed_chunk_publish_hook();

    assert_eq!(points, vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]);

    storage.close().unwrap();
}

#[test]
fn merge_query_does_not_flatten_large_active_snapshot() {
    use std::sync::Arc;

    use super::super::state::{
        active_series_snapshot_flatten_count, active_series_snapshot_point_count,
        reset_active_series_snapshot_flatten_count, reset_active_series_snapshot_point_count,
    };

    const ACTIVE_POINTS: usize = 4_096;
    const WINDOW_POINTS: usize = 64;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            ACTIVE_POINTS + 1,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let metric = "merge_query_no_flatten_metric";
    let labels = vec![Label::new("host", "query")];
    let rows = (0..ACTIVE_POINTS)
        .map(|idx| {
            Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    reset_active_series_snapshot_flatten_count();
    reset_active_series_snapshot_point_count();

    let start = (ACTIVE_POINTS - WINDOW_POINTS) as i64;
    let end = ACTIVE_POINTS as i64;
    let points = storage.select(metric, &labels, start, end).unwrap();

    let expected = (start..end)
        .map(|ts| DataPoint::new(ts, ts as f64))
        .collect::<Vec<_>>();
    assert_eq!(points, expected);
    assert_eq!(active_series_snapshot_flatten_count(), 0);
    assert_eq!(active_series_snapshot_point_count(), WINDOW_POINTS);

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.merge_path_queries_total, 1);
    assert_eq!(snapshot.query.append_sort_path_queries_total, 0);
}

#[test]
fn append_sort_query_snapshots_only_requested_active_window() {
    use super::super::state::{
        active_series_snapshot_point_count, reset_active_series_snapshot_point_count,
    };

    const ACTIVE_POINTS: usize = 4_096;
    const WINDOW_POINTS: usize = 64;

    let storage = ChunkStorage::new_with_data_path_and_options(
        ACTIVE_POINTS + 1,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(2),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();

    let metric = "append_sort_windowed_active_snapshot_metric";
    let labels = vec![Label::new("host", "query")];
    let start = (ACTIVE_POINTS - WINDOW_POINTS) as i64;
    let end = ACTIVE_POINTS as i64;

    let mut rows = (0..start)
        .map(|ts| Row::with_labels(metric, labels.clone(), DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    rows.extend(
        (start..end)
            .rev()
            .map(|ts| Row::with_labels(metric, labels.clone(), DataPoint::new(ts, ts as f64))),
    );
    storage.insert_rows(&rows).unwrap();

    reset_active_series_snapshot_point_count();

    let points = storage.select(metric, &labels, start, end).unwrap();
    let expected = (start..end)
        .map(|ts| DataPoint::new(ts, ts as f64))
        .collect::<Vec<_>>();
    assert_eq!(points, expected);
    // Append/sort reuses the bounded snapshot gathered for the merge probe.
    assert_eq!(active_series_snapshot_point_count(), WINDOW_POINTS);

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.merge_path_queries_total, 0);
    assert_eq!(snapshot.query.append_sort_path_queries_total, 1);
}

#[test]
fn select_all_wide_live_metric_does_not_flatten_active_snapshots() {
    use std::sync::Arc;

    use super::super::state::{
        active_series_snapshot_flatten_count, reset_active_series_snapshot_flatten_count,
    };

    const SERIES_COUNT: usize = 32;
    const ACTIVE_POINTS_PER_SERIES: usize = 1_024;
    const WINDOW_POINTS: usize = 32;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            ACTIVE_POINTS_PER_SERIES + 1,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 4,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let metric = "select_all_no_flatten_metric";
    for series_idx in 0..SERIES_COUNT {
        let labels = vec![Label::new("host", format!("series-{series_idx:03}"))];
        let rows = (0..ACTIVE_POINTS_PER_SERIES)
            .map(|point_idx| {
                Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(
                        point_idx as i64,
                        (series_idx * ACTIVE_POINTS_PER_SERIES + point_idx) as f64,
                    ),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    reset_active_series_snapshot_flatten_count();

    let start = (ACTIVE_POINTS_PER_SERIES - WINDOW_POINTS) as i64;
    let end = ACTIVE_POINTS_PER_SERIES as i64;
    let selected = storage.select_all(metric, start, end).unwrap();

    assert_eq!(selected.len(), SERIES_COUNT);
    assert!(selected
        .iter()
        .all(|(_, points)| points.len() == WINDOW_POINTS));
    assert!(selected.iter().all(|(_, points)| {
        points.first().is_some_and(|point| point.timestamp == start)
            && points
                .last()
                .is_some_and(|point| point.timestamp == end - 1)
    }));
    assert_eq!(active_series_snapshot_flatten_count(), 0);

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.merge_path_queries_total, SERIES_COUNT as u64);
    assert_eq!(snapshot.query.append_sort_path_queries_total, 0);
}

#[test]
fn hot_active_append_sort_query_releases_same_shard_writes_during_snapshot_expansion() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            512,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let query_metric = "hot_active_append_sort_query_metric";
    let query_labels = vec![Label::new("host", "query")];
    let mut expected_points = Vec::new();
    let mut query_rows = Vec::new();
    for idx in 0..192i64 {
        let ts = if idx == 96 { 32 } else { idx + 100 };
        expected_points.push(DataPoint::new(ts, idx as f64));
        query_rows.push(Row::with_labels(
            query_metric,
            query_labels.clone(),
            DataPoint::new(ts, idx as f64),
        ));
    }
    expected_points.sort_by_key(|point| point.timestamp);
    storage.insert_rows(&query_rows).unwrap();

    let query_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(query_metric, &query_labels)
        .unwrap()
        .series_id;
    let target_shard = ChunkStorage::series_shard_idx(query_series_id);

    let writer_metric = "hot_active_append_sort_writer_metric";
    let writer_labels = (0..1024usize)
        .find_map(|idx| {
            let labels = vec![Label::new("host", format!("writer-{idx}"))];
            storage
                .insert_rows(&[Row::with_labels(
                    writer_metric,
                    labels.clone(),
                    DataPoint::new(20_000, 20_000.0),
                )])
                .unwrap();
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(writer_metric, &labels)
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id) == target_shard).then_some(labels)
        })
        .expect("expected to find a same-shard writer series");

    let (query_entered_tx, query_entered_rx) = mpsc::channel();
    let (query_release_tx, query_release_rx) = mpsc::channel();
    let query_release_rx = Arc::new(Mutex::new(query_release_rx));
    storage.set_query_append_sort_in_memory_source_snapshot_hook({
        let query_entered_tx = query_entered_tx.clone();
        let query_release_rx = Arc::clone(&query_release_rx);
        move || {
            query_entered_tx.send(()).unwrap();
            query_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let query_storage = Arc::clone(&storage);
    let query_labels_for_thread = query_labels.clone();
    let query_thread =
        thread::spawn(move || query_storage.select(query_metric, &query_labels_for_thread, 0, 512));

    query_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("append-sort query did not reach in-memory snapshot hook");

    let writer_storage = Arc::clone(&storage);
    let writer_labels_for_thread = writer_labels.clone();
    let writer_metric_for_thread = writer_metric.to_string();
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_thread = thread::spawn(move || {
        let result = (|| -> crate::Result<()> {
            for offset in 1..=32i64 {
                writer_storage.insert_rows(&[Row::with_labels(
                    writer_metric_for_thread.as_str(),
                    writer_labels_for_thread.clone(),
                    DataPoint::new(20_000 + offset, (20_000 + offset) as f64),
                )])?;
            }
            Ok(())
        })();
        writer_tx.send(result).unwrap();
    });

    let writer_result = writer_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("same-shard writer should progress while append-sort query expands snapshots");
    assert!(writer_result.is_ok());

    query_release_tx.send(()).unwrap();

    let query_points = query_thread.join().unwrap().unwrap();
    assert_eq!(query_points, expected_points);

    writer_thread.join().unwrap();
    storage.clear_query_append_sort_in_memory_source_snapshot_hook();

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.merge_path_queries_total, 0);
    assert_eq!(snapshot.query.append_sort_path_queries_total, 1);
    assert_eq!(snapshot.query.merge_path_shard_snapshots_total, 1);

    let writer_points = storage
        .select(writer_metric, &writer_labels, 0, 30_000)
        .unwrap();
    assert_eq!(writer_points.len(), 33);
}

#[test]
fn slow_metadata_time_range_query_releases_same_shard_writes_and_flushes() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );

    let query_metric = "metadata_query_lock_query_metric";
    let query_labels = vec![Label::new("host", "query")];
    storage
        .insert_rows(&[
            Row::with_labels(query_metric, query_labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels(query_metric, query_labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels(query_metric, query_labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();

    let query_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(query_metric, &query_labels)
        .unwrap()
        .series_id;
    let target_shard = ChunkStorage::series_shard_idx(query_series_id);

    let writer_metric = "metadata_query_lock_writer_metric";
    let writer_labels = (0..256usize)
        .find_map(|idx| {
            let labels = vec![Label::new("host", format!("writer-{idx}"))];
            storage
                .insert_rows(&[Row::with_labels(
                    writer_metric,
                    labels.clone(),
                    DataPoint::new(10, 10.0),
                )])
                .unwrap();
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(writer_metric, &labels)
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id) == target_shard).then_some(labels)
        })
        .expect("expected to find a same-shard writer series");
    storage.flush_all_active().unwrap();

    let writer_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(writer_metric, &writer_labels)
        .unwrap()
        .series_id;
    assert_eq!(
        ChunkStorage::series_shard_idx(writer_series_id),
        target_shard
    );

    let (query_entered_tx, query_entered_rx) = mpsc::channel();
    let (query_release_tx, query_release_rx) = mpsc::channel();
    let query_release_rx = Arc::new(Mutex::new(query_release_rx));
    storage.set_metadata_time_range_summary_hook({
        let query_entered_tx = query_entered_tx.clone();
        let query_release_rx = Arc::clone(&query_release_rx);
        move || {
            query_entered_tx.send(()).unwrap();
            query_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let controller_storage = Arc::clone(&storage);
    let controller_writer_labels = writer_labels.clone();
    let controller = thread::spawn(move || {
        query_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("metadata query did not reach time-range summary hook");

        let writer_storage = Arc::clone(&controller_storage);
        let writer_labels_for_thread = controller_writer_labels.clone();
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer_thread = thread::spawn(move || {
            let result = writer_storage.insert_rows(&[Row::with_labels(
                writer_metric,
                writer_labels_for_thread,
                DataPoint::new(11, 11.0),
            )]);
            writer_tx.send(result).unwrap();
        });

        let writer_result = writer_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("same-shard write should not block on metadata time-range summary");
        assert!(writer_result.is_ok());

        let flush_storage = Arc::clone(&controller_storage);
        let (flush_tx, flush_rx) = mpsc::channel();
        let flush_thread = thread::spawn(move || {
            let result = flush_storage.flush_all_active();
            flush_tx.send(result).unwrap();
        });

        let flush_result = flush_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("same-shard flush should not block on metadata time-range summary");
        assert!(flush_result.is_ok());

        query_release_tx.send(()).unwrap();
        writer_thread.join().unwrap();
        flush_thread.join().unwrap();
    });

    let selected = storage
        .as_ref()
        .filter_series_ids_in_time_range_for_tests(vec![query_series_id], 2, 3)
        .unwrap();
    assert_eq!(selected, vec![query_series_id]);

    controller.join().unwrap();
    storage.clear_metadata_time_range_summary_hook();

    let sealed = storage.sealed_shard(writer_series_id).read();
    assert!(sealed.get(&writer_series_id).is_some());
}

#[test]
fn list_metrics_with_wal_hides_new_series_until_samples_are_committed() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(ChunkStorage::new(2, Some(wal)));
    let labels = vec![Label::new("host", "a")];
    let metric = "hooked_metric_pre_commit";
    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));

    storage.set_ingest_post_series_definitions_hook({
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            entered_hook.wait();
            release_hook.wait();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(1, 1.0),
        )])
    });

    entered_hook.wait();

    let has_metric = storage
        .list_metrics_with_wal()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_metric);

    release_hook.wait();
    writer.join().unwrap().unwrap();
    storage.clear_ingest_post_series_definitions_hook();
}

#[test]
fn list_metrics_with_wal_hides_staged_series_until_memory_ingest_finishes() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(ChunkStorage::new(2, Some(wal)));
    let labels = vec![Label::new("host", "a")];
    let metric = "hooked_metric_pre_publish";
    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));

    storage.set_ingest_post_samples_hook({
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            entered_hook.wait();
            release_hook.wait();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(1, 1.0),
        )])
    });

    entered_hook.wait();

    let has_metric_without_wal = storage
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_metric_without_wal);

    let has_metric_with_wal = storage
        .list_metrics_with_wal()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_metric_with_wal);

    release_hook.wait();
    writer.join().unwrap().unwrap();
    storage.clear_ingest_post_samples_hook();
}

#[test]
fn list_metrics_with_wal_uses_primed_storage_wal_cache_without_rebuild() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(ChunkStorage::new(2, Some(wal)));
    let labels = vec![Label::new("host", "a")];
    let metric = "hooked_metric_cold_cache";
    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));

    storage.set_ingest_post_samples_hook({
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            entered_hook.wait();
            release_hook.wait();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(1, 1.0),
        )])
    });

    entered_hook.wait();

    let rebuild_called = Arc::new(AtomicBool::new(false));
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_cached_series_definition_rebuild_hook({
            let rebuild_called = Arc::clone(&rebuild_called);
            move || {
                rebuild_called.store(true, Ordering::SeqCst);
            }
        });

    let first_reader_storage = Arc::clone(&storage);
    let first_reader = thread::spawn(move || first_reader_storage.list_metrics_with_wal());

    let second_reader_storage = Arc::clone(&storage);
    let second_reader = thread::spawn(move || second_reader_storage.list_metrics_with_wal());

    let first_reader_metrics = first_reader.join().unwrap().unwrap();
    let second_reader_metrics = second_reader.join().unwrap().unwrap();
    assert!(!first_reader_metrics
        .iter()
        .any(|series| series.name == metric && series.labels == labels));
    assert!(!second_reader_metrics
        .iter()
        .any(|series| series.name == metric && series.labels == labels));
    assert!(
        !rebuild_called.load(Ordering::SeqCst),
        "storage-owned WAL metadata listing should use the primed snapshot cache instead of replaying the WAL"
    );

    release_hook.wait();
    writer.join().unwrap().unwrap();
    storage.clear_ingest_post_samples_hook();
    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_cached_series_definition_rebuild_hook();
}

#[test]
fn list_metrics_with_wal_uses_cached_series_definitions_across_multi_segment_wal() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let baseline_definition = SeriesDefinitionFrame {
        series_id: 1,
        metric: "baseline_metric".to_string(),
        labels: vec![Label::new("host", "seed")],
    };
    let baseline_batch = SamplesBatchFrame::from_points(
        baseline_definition.series_id,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 1,
            value: Value::F64(1.0),
        }],
    )
    .unwrap();
    let segment_max_bytes = FramedWal::estimate_series_definition_frame_bytes(&baseline_definition)
        .unwrap()
        .saturating_add(FramedWal::estimate_samples_frame_bytes(&[baseline_batch]).unwrap())
        .saturating_sub(1)
        .max(1);
    let wal = FramedWal::open_with_options(
        temp_dir.path().join(WAL_DIR_NAME),
        WalSyncMode::PerAppend,
        128,
        segment_max_bytes,
    )
    .unwrap();
    let storage = Arc::new(ChunkStorage::new(2, Some(wal)));

    for idx in 0..4 {
        storage
            .insert_rows(&[Row::with_labels(
                format!("baseline_metric_{idx}"),
                vec![Label::new("host", format!("baseline-{idx}"))],
                DataPoint::new(idx as i64 + 1, idx as f64 + 1.0),
            )])
            .unwrap();
    }

    let warmed = storage.list_metrics_with_wal().unwrap();
    assert!(
        warmed.len() >= 4,
        "expected list_metrics_with_wal to warm the WAL cache with baseline series"
    );

    let mut wal_segments = std::fs::read_dir(temp_dir.path().join(WAL_DIR_NAME))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .collect::<Vec<_>>();
    wal_segments.sort();
    assert!(
        wal_segments.len() >= 2,
        "expected baseline writes to span multiple WAL segments, got {wal_segments:?}"
    );
    let oldest_segment = wal_segments.first().unwrap().clone();
    let mut oldest_bytes = std::fs::read(&oldest_segment).unwrap();
    oldest_bytes[0] ^= 0xff;
    std::fs::write(&oldest_segment, oldest_bytes).unwrap();

    let labels = vec![Label::new("host", "late")];
    let metric = "cached_after_corruption";
    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));
    storage.set_ingest_post_samples_hook({
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            entered_hook.wait();
            release_hook.wait();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(10, 10.0),
        )])
    });

    entered_hook.wait();

    let has_metric_without_wal = storage
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_metric_without_wal);

    let has_metric_with_wal = storage
        .list_metrics_with_wal()
        .unwrap()
        .into_iter()
        .any(|series| series.name == metric && series.labels == labels);
    assert!(!has_metric_with_wal);

    release_hook.wait();
    writer.join().unwrap().unwrap();
    storage.clear_ingest_post_samples_hook();
}

#[test]
fn list_metrics_racing_revival_write_does_not_prune_materialized_series() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 5,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: Some(20),
            },
        )
        .unwrap(),
    );
    let metric = "list_metrics_revival_race";
    let labels = vec![Label::new("host", "a")];
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(20, 1.0),
        )])
        .unwrap();
    storage.set_current_time_override(30);

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    assert_eq!(storage.materialized_series_snapshot(), vec![series_id]);

    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));
    storage.set_metadata_live_series_pre_prune_hook({
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            entered_hook.wait();
            release_hook.wait();
        }
    });

    let reader_storage = Arc::clone(&storage);
    let reader = thread::spawn(move || reader_storage.list_metrics());

    entered_hook.wait();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(30, 2.0),
        )])
        .unwrap();
    release_hook.wait();

    let reader_result = reader.join().unwrap();
    assert!(reader_result.is_ok());
    storage.clear_metadata_live_series_pre_prune_hook();

    assert_eq!(storage.materialized_series_snapshot(), vec![series_id]);
    assert_eq!(
        storage.list_metrics().unwrap(),
        vec![MetricSeries {
            name: metric.to_string(),
            labels,
        }],
    );
}

#[test]
fn concurrent_new_series_writers_wait_for_logical_wal_commit_before_reopen() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            Some(temp_dir.path().join(BLOB_LANE_ROOT)),
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let labels = vec![Label::new("host", "a")];
    let metric_a = "logical_wal_metric_a";
    let metric_a_shard = ChunkStorage::registry_series_shard_idx(metric_a, &labels);
    let metric_b = (0..1024)
        .map(|idx| format!("logical_wal_metric_b_{idx}"))
        .find(|candidate| {
            ChunkStorage::registry_series_shard_idx(candidate, &labels) != metric_a_shard
        })
        .expect("expected to find a metric on a different registry shard");

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));
    storage.set_ingest_post_series_definitions_hook({
        let hook_calls = Arc::clone(&hook_calls);
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            if hook_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                entered_hook.wait();
                release_hook.wait();
            }
        }
    });

    let writer_a_storage = Arc::clone(&storage);
    let writer_a_labels = labels.clone();
    let (writer_a_tx, writer_a_rx) = mpsc::channel();
    let writer_a = thread::spawn(move || {
        let result = writer_a_storage.insert_rows(&[Row::with_labels(
            metric_a,
            writer_a_labels,
            DataPoint::new(1, 1.0),
        )]);
        writer_a_tx.send(result).unwrap();
    });

    entered_hook.wait();

    let writer_b_storage = Arc::clone(&storage);
    let writer_b_labels = labels.clone();
    let writer_b_metric = metric_b.clone();
    let (writer_b_tx, writer_b_rx) = mpsc::channel();
    let writer_b = thread::spawn(move || {
        let result = writer_b_storage.insert_rows(&[Row::with_labels(
            writer_b_metric.as_str(),
            writer_b_labels,
            DataPoint::new(1, 2.0),
        )]);
        writer_b_tx.send(result).unwrap();
    });

    let writer_b_early_result = writer_b_rx.recv_timeout(Duration::from_millis(200));

    release_hook.wait();

    assert!(
        writer_b_early_result.is_err(),
        "unrelated writer should wait until the first logical WAL write commits"
    );
    assert!(writer_a_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    let writer_b_result = match writer_b_early_result {
        Ok(result) => result,
        Err(_) => writer_b_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
    };
    assert!(writer_b_result.is_ok());

    writer_a.join().unwrap();
    writer_b.join().unwrap();
    storage.clear_ingest_post_series_definitions_hook();
    storage.close().unwrap();
    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(8)
        .with_retention_enforced(false)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(metric_a, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert_eq!(
        reopened.select(metric_b.as_str(), &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 2.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn concurrent_current_and_late_writers_reject_late_overflow_without_churning_current_head() {
    use std::sync::atomic::{AtomicUsize, Ordering};
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
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: 100,
                max_active_partition_heads_per_series: 4,
                max_writers: 2,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    let metric = "mixed_partition_fanout";
    let labels = vec![Label::new("host", "a")];
    let current_partition_timestamp = 5_000i64;

    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(current_partition_timestamp, 1.0),
        )])
        .unwrap();

    let current_writer_storage = Arc::clone(&storage);
    let current_writer_labels = labels.clone();
    let current_writer = thread::spawn(move || {
        for offset in 1..=50i64 {
            current_writer_storage
                .insert_rows(&[Row::with_labels(
                    metric,
                    current_writer_labels.clone(),
                    DataPoint::new(current_partition_timestamp + offset, offset as f64),
                )])
                .unwrap();
        }
    });

    let late_writer_storage = Arc::clone(&storage);
    let late_writer_labels = labels.clone();
    let late_rejections = Arc::new(AtomicUsize::new(0));
    let late_rejections_for_thread = Arc::clone(&late_rejections);
    let late_writer = thread::spawn(move || {
        for partition in (0..50i64).rev() {
            match late_writer_storage.insert_rows(&[Row::with_labels(
                metric,
                late_writer_labels.clone(),
                DataPoint::new(partition * 100, (partition + 100) as f64),
            )]) {
                Ok(()) => {}
                Err(TsinkError::LateWritePartitionFanoutExceeded { .. }) => {
                    late_rejections_for_thread.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => panic!("unexpected late-writer error: {err}"),
            }
        }
    });

    current_writer.join().unwrap();
    late_writer.join().unwrap();
    assert!(late_rejections.load(Ordering::Relaxed) > 0);

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert!(state.partition_head_count() <= 4);
    assert!(state.contains_partition_head(50));
    drop(active);

    let points = storage.select(metric, &labels, 0, 5_100).unwrap();
    assert_eq!(points.len(), 54);
    assert!(points
        .windows(2)
        .all(|window| window[0].timestamp <= window[1].timestamp));
}

#[test]
fn def_only_wal_series_stays_hidden_across_reopen() {
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
        assert!(!has_phantom_with_wal);

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
