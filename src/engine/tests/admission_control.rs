use super::*;
use crate::{RowWriteStatus, WriteMode, WriteRejectionCategory};
use std::sync::atomic::Ordering;

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
fn opt_in_future_skew_limit_accepts_boundary_and_rejects_before_publication() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_max_future_skew(Duration::from_secs(10))
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_background_threads_enabled_for_tests(false)
        .with_current_time_override_for_tests(100)
        .build()
        .unwrap();

    let boundary = storage
        .write_batch(
            &[Row::new("future_skew_boundary", DataPoint::new(110, 1.0))],
            WriteMode::Atomic,
        )
        .unwrap();
    assert_eq!(boundary.accepted, 1);
    assert_eq!(boundary.rejected, 0);

    let metrics_before = storage.list_metrics().unwrap();
    let wal_before = storage.observability_snapshot().wal;
    let rejected = storage
        .write_batch(
            &[
                Row::new("future_skew_atomic_safe", DataPoint::new(109, 2.0)),
                Row::new("future_skew_atomic_too_far", DataPoint::new(111, 3.0)),
            ],
            WriteMode::Atomic,
        )
        .unwrap();

    assert_eq!(rejected.accepted, 0);
    assert_eq!(rejected.rejected, 2);
    assert_eq!(rejected.acknowledgement, None);
    assert!(rejected.outcomes.iter().all(|outcome| {
        matches!(
            &outcome.status,
            RowWriteStatus::Rejected(rejection)
                if rejection.category == WriteRejectionCategory::FutureSkewExceeded
        )
    }));
    assert_eq!(storage.list_metrics().unwrap(), metrics_before);

    let wal_after = storage.observability_snapshot().wal;
    assert_eq!(
        (
            wal_after.append_series_definitions_total,
            wal_after.append_sample_batches_total,
            wal_after.append_points_total,
            wal_after.append_bytes_total,
        ),
        (
            wal_before.append_series_definitions_total,
            wal_before.append_sample_batches_total,
            wal_before.append_points_total,
            wal_before.append_bytes_total,
        ),
    );
    assert!(storage
        .select("future_skew_atomic_safe", &[], 0, 200)
        .unwrap()
        .is_empty());
    assert!(storage
        .select("future_skew_atomic_too_far", &[], 0, 200)
        .unwrap()
        .is_empty());

    let error = storage
        .insert_rows(&[Row::new(
            "future_skew_specific_error",
            DataPoint::new(111, 4.0),
        )])
        .unwrap_err();
    assert!(matches!(
        error,
        TsinkError::FutureSkewExceeded {
            timestamp: 111,
            cutoff: 110,
        }
    ));

    storage.close().unwrap();
}

#[test]
fn future_skew_limit_is_unset_by_default() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_current_time_override_for_tests(100)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new(
            "default_future_skew_acceptance",
            DataPoint::new(10_000, 1.0),
        )])
        .unwrap();
    assert_eq!(
        storage
            .select("default_future_skew_acceptance", &[], 0, 10_001)
            .unwrap(),
        vec![DataPoint::new(10_000, 1.0)]
    );
    let retention = storage.observability_snapshot().retention;
    assert_eq!(retention.future_skew_points_total, 1);
    assert_eq!(retention.future_skew_max_timestamp, Some(10_000));

    storage.close().unwrap();
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
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window_units: 1,
            new_series_window_nanos: 60_000_000_000,
            write_batch_limits: Default::default(),
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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

    let canonical = storage
        .write_batch(
            &[Row::new(
                "canonical_write_timeout_metric",
                DataPoint::new(1, 1.0),
            )],
            WriteMode::Atomic,
        )
        .expect("a safe timeout should be represented as a canonical rejection");
    assert_eq!(canonical.accepted, 0);
    assert_eq!(canonical.rejected, 1);
    assert_eq!(canonical.acknowledgement, None);
    let RowWriteStatus::Rejected(rejection) = &canonical.outcomes[0].status else {
        panic!("the held writer permit must reject the canonical write");
    };
    assert_eq!(rejection.category, WriteRejectionCategory::WriteTimeout);
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
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_millis(100),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window_units: 1,
            new_series_window_nanos: 60_000_000_000,
            write_batch_limits: Default::default(),
            wal_size_limit_bytes: 1,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
fn close_cancels_writer_waiting_for_admission_pressure() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                max_future_skew_window: None,
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 1,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
                max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
                max_new_series_per_window: None,
                new_series_window_units: 1,
                new_series_window_nanos: 60_000_000_000,
                write_batch_limits: Default::default(),
                wal_size_limit_bytes: 1,
                admission_poll_interval: Duration::from_millis(5),
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                maintenance_max_items_per_pass: 1_024,
                maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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

    let before = storage.observability_snapshot();
    let blocked_relief = storage.memory.admission_backpressure_lock.lock();
    let writer_storage = Arc::clone(&storage);
    let writer = std::thread::spawn(move || {
        writer_storage.insert_rows(&[Row::new(
            "close_admission_pressure_metric",
            DataPoint::new(1, 1.0),
        )])
    });
    assert!(wait_for_condition(
        Duration::from_secs(1),
        Duration::from_millis(5),
        || {
            storage
                .observability_snapshot()
                .flush
                .admission_backpressure_delays_total
                > before.flush.admission_backpressure_delays_total
        },
    ));
    assert_eq!(
        storage
            .observability_snapshot()
            .flush
            .admission_pressure_relief_requests_total,
        before.flush.admission_pressure_relief_requests_total,
        "a failed serialized relief attempt must not be counted as a request",
    );
    drop(blocked_relief);
    assert!(wait_for_condition(
        Duration::from_secs(1),
        Duration::from_millis(5),
        || {
            storage
                .observability_snapshot()
                .flush
                .admission_pressure_relief_requests_total
                >= before
                    .flush
                    .admission_pressure_relief_requests_total
                    .saturating_add(2)
        },
    ));

    let close_started = std::time::Instant::now();
    storage.close().unwrap();
    assert!(
        close_started.elapsed() < Duration::from_secs(1),
        "close should cancel admission backpressure instead of waiting for the write timeout",
    );
    assert!(matches!(
        writer.join().unwrap(),
        Err(TsinkError::StorageClosed)
    ));
}

#[test]
fn memory_pressure_relief_rejects_safely_with_busy_writer_permit() {
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
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_millis(100),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window_units: 1,
            new_series_window_nanos: 60_000_000_000,
            write_batch_limits: Default::default(),
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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

    let expected = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    storage
        .insert_rows(&[
            Row::new("memory_pressure_drain_metric", expected[0].clone()),
            Row::new("memory_pressure_drain_metric", expected[1].clone()),
        ])
        .unwrap();
    storage
        .memory
        .budget_bytes
        .store(1, std::sync::atomic::Ordering::Release);
    storage.refresh_memory_usage();

    let held_permit = storage.runtime.write_limiter.acquire();
    let error = storage
        .enforce_memory_budget_if_needed()
        .expect_err("a one-byte budget cannot admit the finite persistence peak");
    assert!(matches!(
        error,
        TsinkError::MemoryBudgetExceeded {
            budget: 1,
            required,
        } if required > 1
    ));
    assert!(
        load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
        "rejected memory-pressure relief must roll back its staged segment",
    );
    assert_eq!(
        storage
            .select("memory_pressure_drain_metric", &[], 0, 3)
            .unwrap(),
        expected,
        "rejected memory-pressure relief must preserve accepted data",
    );
    drop(held_permit);
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, std::sync::atomic::Ordering::Release);
    storage.close().unwrap();
    assert!(
        !load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
        "an admitted close retry must persist the accepted data",
    );
}

#[test]
fn memory_admission_backpressure_repeats_bounded_flush_until_relief() {
    const ESTIMATED_GROWTH_BYTES: usize = 4096;

    let temp_dir = TempDir::new().unwrap();
    // Keep two background-eligible old heads substantially larger than the current head. The
    // calibrated budget fits only after two item-bounded worker passes, so one lost wake cannot
    // accidentally satisfy this test.
    let first_blob = "a".repeat(32 * 1024);
    let second_blob = "b".repeat(24 * 1024);
    let third_blob = "c".repeat(4096);
    let build_storage = |root: &TempDir, write_timeout: Duration| {
        let wal = FramedWal::open(root.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        ChunkStorage::new_with_data_path_and_options(
            64,
            Some(wal),
            None,
            Some(root.path().join(BLOB_LANE_ROOT)),
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                max_future_skew_window: None,
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: 10,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 1,
                write_timeout,
                memory_budget_bytes: 8_000_000,
                cardinality_limit: usize::MAX,
                max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
                max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
                max_new_series_per_window: None,
                new_series_window_units: 1,
                new_series_window_nanos: 60_000_000_000,
                write_batch_limits: Default::default(),
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: Duration::from_millis(5),
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                // One active-series inspection plus the two chunks that share this write
                // batch's WAL interval. WAL ordering safely defers the first attempt until the
                // second old head has also been finalized.
                maintenance_max_items_per_pass: 3,
                maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
    let initial_rows = || {
        let mut rows = (1..=8)
            .map(|timestamp| {
                Row::new(
                    "memory_backpressure_head_guard",
                    DataPoint::new(timestamp, first_blob.clone()),
                )
            })
            .collect::<Vec<_>>();
        rows.extend((11..=18).map(|timestamp| {
            Row::new(
                "memory_backpressure_head_guard",
                DataPoint::new(timestamp, second_blob.clone()),
            )
        }));
        rows.push(Row::new(
            "memory_backpressure_head_guard",
            DataPoint::new(21, third_blob.clone()),
        ));
        rows
    };
    let run_relief_pass = |storage: &ChunkStorage| {
        let selected = storage
            .flush_background_eligible_active_with_selection()
            .unwrap();
        storage
            .persist_segment_background_bounded_with_limits(
                storage
                    .runtime
                    .maintenance_max_items_per_pass
                    .saturating_sub(selected.inspected_items),
                storage
                    .runtime
                    .maintenance_max_bytes_per_pass
                    .saturating_sub(selected.input_bytes),
            )
            .unwrap()
    };
    let calibration_dir = TempDir::new().unwrap();
    let calibration = build_storage(&calibration_dir, Duration::ZERO);
    calibration.insert_rows(&initial_rows()).unwrap();
    assert_engine_memory_usage_reconciled(&calibration);
    assert!(
        !run_relief_pass(&calibration).persisted,
        "the first pass must defer the open WAL dependency window",
    );
    assert_engine_memory_usage_reconciled(&calibration);
    let after_first_relief_bytes = calibration.memory.used_bytes.load(Ordering::Acquire);
    assert!(
        !run_relief_pass(&calibration).persisted,
        "the current head shares the initial WAL frame, so persistence must remain deferred",
    );
    assert_engine_memory_usage_reconciled(&calibration);
    let after_second_relief_bytes = calibration.memory.used_bytes.load(Ordering::Acquire);
    assert!(
        after_second_relief_bytes < after_first_relief_bytes,
        "the second bounded pass must reclaim additional retained memory",
    );
    let estimated_growth_bytes = u64::try_from(ESTIMATED_GROWTH_BYTES).unwrap();
    let target_budget = after_second_relief_bytes.saturating_add(estimated_growth_bytes);
    assert!(
        after_first_relief_bytes.saturating_add(estimated_growth_bytes) > target_budget,
        "one bounded pass must remain above the exact admission threshold",
    );

    let storage = std::sync::Arc::new(build_storage(&temp_dir, Duration::from_secs(1)));

    storage.insert_rows(&initial_rows()).unwrap();

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
        assert_eq!(state.point_count(), 17);
    }

    storage
        .start_background_flush_thread(Duration::from_secs(60))
        .unwrap();
    storage
        .memory
        .budget_bytes
        .store(target_budget, std::sync::atomic::Ordering::Release);

    let prepare = storage.write_prepare_context();
    prepare
        .admission
        .enforce_admission_controls(
            prepare.memory_budget,
            prepare.wal,
            ESTIMATED_GROWTH_BYTES,
            0,
        )
        .unwrap();

    assert!(
        wait_for_condition(Duration::from_secs(1), Duration::from_millis(10), || {
            storage
                .observability_snapshot()
                .flush
                .active_flushed_chunks_total
                >= before.flush.active_flushed_chunks_total.saturating_add(2)
        }),
        "repeated pressure wakes should finalize both older heads",
    );

    let after = storage.observability_snapshot();
    assert!(
        after.flush.admission_backpressure_delays_total
            > before.flush.admission_backpressure_delays_total
    );
    assert!(
        after.flush.admission_pressure_relief_requests_total
            >= before
                .flush
                .admission_pressure_relief_requests_total
                .saturating_add(2)
    );
    assert!(
        after.flush.admission_pressure_relief_observed_total
            > before.flush.admission_pressure_relief_observed_total
    );
    assert!(
        after.memory.pressure.backpressure_events_total
            > before.memory.pressure.backpressure_events_total
    );
    assert_eq!(after.memory.pressure.active_backpressured_writers, 0);
    let rejection_delta = after
        .memory
        .pressure
        .rejections_total
        .saturating_sub(before.memory.pressure.rejections_total);
    let pipeline_error_delta = after
        .flush
        .pipeline_errors_total
        .saturating_sub(before.flush.pipeline_errors_total);
    let persist_error_delta = after
        .flush
        .persist_errors_total
        .saturating_sub(before.flush.persist_errors_total);
    assert_eq!(rejection_delta, pipeline_error_delta);
    assert_eq!(pipeline_error_delta, persist_error_delta);
    assert!(
        after.flush.active_flushed_chunks_total
            >= before.flush.active_flushed_chunks_total.saturating_add(2),
        "each pressure wake must remain one bounded pass while the writer makes progress",
    );
    {
        let active = storage.active_shard(series_id).read();
        let state = active.get(&series_id).unwrap();
        assert_eq!(state.partition_head_count(), 1);
        let current_partition_id = state.current_partition_id.unwrap();
        assert_eq!(
            state.partition_heads[&current_partition_id].builder.len(),
            1,
            "admission pressure must keep the current partial head intact",
        );
    }
    assert_eq!(
        storage
            .select("memory_backpressure_head_guard", &[], 0, 30)
            .unwrap(),
        (1..=8)
            .map(|timestamp| DataPoint::new(timestamp, first_blob.clone()))
            .chain((11..=18).map(|timestamp| { DataPoint::new(timestamp, second_blob.clone()) }))
            .chain([DataPoint::new(21, third_blob)])
            .collect::<Vec<_>>()
    );
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    storage.close().unwrap();
}

#[test]
fn memory_admission_evicts_persisted_sealed_overlap_to_exact_residual_budget() {
    const STAGED_BYTES: usize = 113;
    const TRANSIENT_BYTES: usize = 127;
    const ESTIMATED_GROWTH_BYTES: usize = 139;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path.clone()),
        None,
        2,
        ChunkStorageOptions {
            retention_enforced: false,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: 64 * 1024 * 1024,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .insert_rows(&[Row::new(
            "admission_residual_eviction",
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("admission_residual_eviction", &[])
        .unwrap()
        .series_id;
    let (sealed_key, sealed_chunk) = storage.chunks.sealed_chunks
        [ChunkStorage::series_shard_idx(series_id)]
    .read()
    .get(&series_id)
    .unwrap()
    .first_key_value()
    .map(|(key, chunk)| (*key, Arc::clone(chunk)))
    .unwrap();
    let overlap_bytes = ChunkStorage::chunk_memory_usage_bytes(&sealed_chunk);

    let writer = SegmentWriter::new(&lane_path, 0, 1).unwrap();
    writer
        .write_segment(
            &storage.catalog.registry.read(),
            &HashMap::from([(series_id, vec![Arc::clone(&sealed_chunk)])]),
        )
        .unwrap();
    storage
        .add_persisted_segments_from_loaded(
            vec![load_segment_index(&writer.layout().root).unwrap()],
        )
        .unwrap();
    storage.mark_persisted_chunk_watermarks(&HashMap::from([(series_id, sealed_key.sequence)]));

    let used_before = storage.memory.used_bytes.load(Ordering::Acquire) as usize;
    storage
        .memory
        .tombstone_staged_bytes
        .store(STAGED_BYTES as u64, Ordering::Release);
    let transient = storage
        .reserve_write_transient_memory(TRANSIENT_BYTES)
        .unwrap();
    let exact_residual_budget = used_before
        .saturating_sub(overlap_bytes)
        .saturating_add(STAGED_BYTES)
        .saturating_add(TRANSIENT_BYTES)
        .saturating_add(ESTIMATED_GROWTH_BYTES);
    storage
        .memory
        .budget_bytes
        .store(exact_residual_budget as u64, Ordering::Release);

    let prepare = storage.write_prepare_context();
    prepare
        .admission
        .enforce_admission_controls(
            prepare.memory_budget,
            prepare.wal,
            ESTIMATED_GROWTH_BYTES,
            0,
        )
        .expect("one persisted sealed overlap should make the exact residual boundary fit");
    let used_after = storage.memory.used_bytes.load(Ordering::Acquire) as usize;
    assert_eq!(used_after, used_before - overlap_bytes);
    assert!(
        storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)]
            .read()
            .get(&series_id)
            .is_none()
    );

    storage
        .memory
        .budget_bytes
        .store((exact_residual_budget - 1) as u64, Ordering::Release);
    let error = prepare
        .admission
        .enforce_admission_controls(
            prepare.memory_budget,
            prepare.wal,
            ESTIMATED_GROWTH_BYTES,
            0,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        TsinkError::MemoryBudgetExceeded { budget, required }
            if budget == exact_residual_budget - 1 && required == exact_residual_budget
    ));

    storage
        .memory
        .tombstone_staged_bytes
        .store(0, Ordering::Release);
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    drop(transient);
    storage.close().unwrap();
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
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::from_millis(25),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window_units: 1,
            new_series_window_nanos: 60_000_000_000,
            write_batch_limits: Default::default(),
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: Duration::from_millis(5),
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window_units: 1,
            new_series_window_nanos: 60_000_000_000,
            write_batch_limits: Default::default(),
            wal_size_limit_bytes: wal_size_limit,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
                max_future_skew_window: None,
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 1,
                write_timeout: Duration::from_secs(2),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
                max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
                max_new_series_per_window: None,
                new_series_window_units: 1,
                new_series_window_nanos: 60_000_000_000,
                write_batch_limits: Default::default(),
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                maintenance_max_items_per_pass: 1_024,
                maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
