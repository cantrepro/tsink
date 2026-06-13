use super::*;

fn wait_for_condition<F>(timeout: Duration, poll_interval: Duration, condition: F) -> bool
where
    F: Fn() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(poll_interval);
    }
    condition()
}

#[test]
fn timestamp_precision_changes_retention_unit_conversion() {
    let seconds_storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(1))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_current_time_override_for_tests(0)
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
        .with_current_time_override_for_tests(0)
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
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::ZERO,
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
    .unwrap();

    let _held_permit = storage.runtime.write_limiter.acquire();
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
fn wal_pressure_with_busy_writer_permit_returns_limit_error_not_timeout() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
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
            write_timeout: Duration::from_millis(100),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: 1,
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
    .unwrap();

    let _held_permit = storage.runtime.write_limiter.acquire();
    let err = storage
        .insert_rows(&[Row::new(
            "wal_pressure_drain_metric",
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::WalSizeLimitExceeded { .. }));
}

#[test]
fn memory_pressure_relief_completes_with_busy_writer_permit() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(lane_path.clone()),
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
            write_timeout: Duration::from_millis(100),
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
    .unwrap();

    storage
        .insert_rows(&[
            Row::new("memory_pressure_drain_metric", DataPoint::new(1, 1.0)),
            Row::new("memory_pressure_drain_metric", DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    storage
        .memory
        .budget_bytes
        .store(1, std::sync::atomic::Ordering::Release);
    storage.refresh_memory_usage();

    let _held_permit = storage.runtime.write_limiter.acquire();
    storage.enforce_memory_budget_if_needed().unwrap();
    assert!(
        !load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
        "memory-pressure relief should still persist data while another writer permit is held",
    );
}

#[test]
fn memory_admission_backpressure_uses_background_flush_without_sealing_current_head() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    let first_blob = "a".repeat(4096);
    let second_blob = "b".repeat(4096);
    let third_blob = "c".repeat(4096);
    let fourth_blob = "d".repeat(4096);
    let build_storage = |root: &TempDir| {
        let wal = FramedWal::open(root.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            None,
            Some(root.path().join(BLOB_LANE_ROOT)),
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: 10,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 1,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: 1_000_000,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: Duration::from_millis(5),
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
        .unwrap()
    };
    let calibration_dir = TempDir::new().unwrap();
    let calibration = build_storage(&calibration_dir);
    calibration
        .insert_rows(&[
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(1, first_blob.clone()),
            ),
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(11, second_blob.clone()),
            ),
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(21, third_blob.clone()),
            ),
        ])
        .unwrap();
    calibration.flush_background_eligible_active().unwrap();
    calibration.persist_segment_with_outcome().unwrap();
    calibration
        .insert_rows(&[Row::new(
            "memory_backpressure_head_guard",
            DataPoint::new(22, fourth_blob.clone()),
        )])
        .unwrap();
    let target_budget = calibration.memory_used().saturating_add(512) as u64;

    let storage = std::sync::Arc::new(build_storage(&temp_dir));

    storage
        .insert_rows(&[
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(1, first_blob.clone()),
            ),
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(11, second_blob.clone()),
            ),
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(21, third_blob.clone()),
            ),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("memory_backpressure_head_guard", &[])
        .unwrap()
        .series_id;
    let before = storage.observability_snapshot();
    {
        let active = storage.active_shard(series_id).read();
        let state = active.get(&series_id).unwrap();
        assert_eq!(state.partition_head_count(), 3);
        assert_eq!(state.point_count(), 3);
    }

    storage
        .start_background_flush_thread(Duration::from_secs(60))
        .unwrap();
    storage
        .memory
        .budget_bytes
        .store(target_budget, std::sync::atomic::Ordering::Release);

    storage
        .insert_rows(&[Row::new(
            "memory_backpressure_head_guard",
            DataPoint::new(22, fourth_blob.clone()),
        )])
        .unwrap();

    assert!(
        wait_for_condition(Duration::from_secs(1), Duration::from_millis(10), || {
            !load_segments_for_level(&lane_path, 0).unwrap().is_empty()
        }),
        "background flush should publish a persisted segment while relieving admission pressure",
    );

    let after = storage.observability_snapshot();
    assert!(
        after.flush.admission_backpressure_delays_total
            > before.flush.admission_backpressure_delays_total
    );
    assert!(
        after.flush.admission_pressure_relief_requests_total
            > before.flush.admission_pressure_relief_requests_total
    );
    assert!(
        after.flush.admission_pressure_relief_observed_total
            > before.flush.admission_pressure_relief_observed_total
    );
    assert!(
        after.flush.persisted_segments_total > before.flush.persisted_segments_total,
        "background flush should persist the non-current head instead of fragmenting the live head",
    );
    {
        let active = storage.active_shard(series_id).read();
        let state = active.get(&series_id).unwrap();
        assert_eq!(state.partition_head_count(), 1);
        assert_eq!(
            state.point_count(),
            2,
            "admission pressure must keep the current partial head intact while appending the new point",
        );
    }
    assert_eq!(
        storage
            .select("memory_backpressure_head_guard", &[], 0, 30)
            .unwrap(),
        vec![
            DataPoint::new(1, first_blob),
            DataPoint::new(11, second_blob),
            DataPoint::new(21, third_blob),
            DataPoint::new(22, fourth_blob),
        ]
    );
}

#[test]
fn wal_admission_backpressure_rejects_without_flushing_current_head() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(lane_path.clone()),
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
            max_writers: 1,
            write_timeout: Duration::from_millis(25),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: Duration::from_millis(5),
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

    storage
        .insert_rows(&[
            Row::new("wal_backpressure_head_guard", DataPoint::new(1, 1.0)),
            Row::new("wal_backpressure_head_guard", DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("wal_backpressure_head_guard", &[])
        .unwrap()
        .series_id;
    let before = storage.observability_snapshot();
    let wal_size_limit = storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .total_size_bytes()
        .unwrap();
    storage.runtime.wal_size_limit_bytes = wal_size_limit;

    let err = storage
        .insert_rows(&[Row::new(
            "wal_backpressure_head_guard",
            DataPoint::new(3, 3.0),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WalSizeLimitExceeded { limit, required }
            if limit == wal_size_limit && required > limit
    ));

    let after = storage.observability_snapshot();
    assert!(
        after.flush.admission_backpressure_delays_total
            > before.flush.admission_backpressure_delays_total
    );
    assert!(
        after.flush.admission_pressure_relief_requests_total
            > before.flush.admission_pressure_relief_requests_total
    );
    assert_eq!(
        after.flush.admission_pressure_relief_observed_total,
        before.flush.admission_pressure_relief_observed_total,
        "no asynchronous relief should be observed when WAL pressure cannot clear without sealing the live head",
    );
    assert_eq!(
        after.flush.persisted_segments_total, before.flush.persisted_segments_total,
        "WAL backpressure must not persist the current partial head on the foreground write path",
    );
    assert!(
        load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
        "rejected WAL-pressure writes must not publish persisted segments",
    );
    {
        let active = storage.active_shard(series_id).read();
        let state = active.get(&series_id).unwrap();
        assert_eq!(state.partition_head_count(), 1);
        assert_eq!(
            state.point_count(),
            2,
            "rejected WAL-pressure writes must leave the current partial head untouched",
        );
    }
}

#[test]
fn wal_size_limit_rejects_writes_against_many_segment_wals() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let template_definition = SeriesDefinitionFrame {
        series_id: 0,
        metric: "cpu_0000".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let segment_max_bytes =
        FramedWal::estimate_series_definition_frame_bytes(&template_definition).unwrap();
    let wal =
        FramedWal::open_with_options(&wal_dir, WalSyncMode::PerAppend, 128, segment_max_bytes)
            .unwrap();

    for series_id in 0..64 {
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id,
            metric: format!("cpu_{series_id:04}"),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();
    }

    let wal_size_limit = wal.total_size_bytes().unwrap();
    let wal_segment_count = wal.segment_count().unwrap();
    assert!(wal_segment_count >= 32);

    let storage = ChunkStorage::new_with_data_path_and_options(
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
            max_writers: 1,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: wal_size_limit,
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

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.wal.size_bytes, wal_size_limit);
    assert_eq!(snapshot.wal.segment_count, wal_segment_count);

    let err = storage
        .insert_rows(&[Row::new(
            "wal_many_segment_guard_metric",
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WalSizeLimitExceeded { limit, required }
            if limit == wal_size_limit && required > limit
    ));
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
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 1,
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

    let held_permit = storage.runtime.write_limiter.acquire();

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
