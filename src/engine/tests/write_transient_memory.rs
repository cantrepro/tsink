use super::*;
use crate::engine::series::SeriesKey;
use crate::engine::storage_engine::{ActiveSeriesState, SeriesId, WalHighWatermark};
use crate::{
    HistogramBucketSpan, HistogramCount, HistogramResetHint, MemoryPressureLevel, NativeHistogram,
    WriteBatchLimits, WriteMode,
};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Barrier};
use std::thread;

fn sample_histogram(custom_values: usize) -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(9)),
        sum: 12.5,
        schema: 2,
        zero_threshold: 0.0,
        zero_count: Some(HistogramCount::Int(1)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -2,
            length: 1,
        }],
        negative_deltas: vec![3],
        negative_counts: vec![1.5],
        positive_spans: vec![HistogramBucketSpan {
            offset: 1,
            length: 2,
        }],
        positive_deltas: vec![4, 2],
        positive_counts: vec![2.5, 3.5],
        reset_hint: HistogramResetHint::Gauge,
        custom_values: vec![0.25; custom_values],
    }
}

fn in_memory_storage_with_limits(limits: WriteBatchLimits) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            write_batch_limits: limits,
            write_timeout: Duration::ZERO,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn populated_active_series_storage() -> (ChunkStorage, SeriesId) {
    const ACTIVE_POINT_CAP: usize = 4_096;
    const SEEDED_POINTS: usize = 1_024;

    let storage = ChunkStorage::new_with_data_path_and_options(
        ACTIVE_POINT_CAP,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            memory_budget_bytes: 64 * 1024 * 1024,
            write_batch_limits: WriteBatchLimits::default(),
            write_timeout: Duration::ZERO,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let series_id = {
        let registry = storage.catalog.registry.read();
        let series_id = registry
            .resolve_or_insert("active_staging_boundary", &[])
            .unwrap()
            .series_id;
        registry
            .assign_series_value_family_if_missing(series_id, SeriesValueFamily::F64)
            .unwrap();
        series_id
    };
    let mut state = ActiveSeriesState::new(series_id, ValueLane::Numeric, ACTIVE_POINT_CAP);
    for timestamp in 0..SEEDED_POINTS {
        let timestamp = i64::try_from(timestamp).unwrap();
        assert!(state
            .rotate_partition_if_needed(
                timestamp,
                storage.runtime.partition_window,
                storage.runtime.max_active_partition_heads_per_series,
            )
            .unwrap()
            .is_none());
        state.append_point(
            timestamp,
            Value::F64(timestamp as f64),
            WalHighWatermark::default(),
        );
        assert!(state.rotate_full_if_needed().unwrap().is_none());
    }
    {
        let mut active_wal_index = storage.chunks.active_wal_index.lock();
        for lowwater in state.wal_lowwaters() {
            active_wal_index.add(lowwater);
        }
    }
    let shard_idx = ChunkStorage::series_shard_idx(series_id);
    storage.chunks.active_builders[shard_idx]
        .write()
        .insert(series_id, state);
    storage.mark_materialized_series_ids(std::iter::once(series_id));
    storage.refresh_memory_usage();
    (storage, series_id)
}

fn assert_memory_snapshot_component_sum(snapshot: &crate::MemoryObservabilitySnapshot) {
    let component_sum = snapshot
        .active_and_sealed_bytes
        .saturating_add(snapshot.registry_bytes)
        .saturating_add(snapshot.metadata_cache_bytes)
        .saturating_add(snapshot.persisted_index_bytes)
        .saturating_add(snapshot.persisted_mmap_bytes)
        .saturating_add(snapshot.tombstone_bytes)
        .saturating_add(snapshot.remote_catalog_staging_bytes)
        .saturating_add(snapshot.wal_writer_buffer_bytes)
        .saturating_add(snapshot.wal_series_definition_cache_bytes)
        .saturating_add(snapshot.write_transient_bytes);
    assert_eq!(snapshot.accounted_bytes, component_sum);
}

#[test]
fn finite_profile_accounts_live_wal_writer_buffer_at_exact_n_and_rejects_n_minus_one() {
    const WAL_BUFFER_BYTES: usize = 1024 * 1024;

    let mut exact_build_limit = WAL_BUFFER_BYTES;
    loop {
        let calibration_dir = TempDir::new().unwrap();
        match StorageBuilder::new()
            .with_resource_profile(crate::ResourceProfile::Test)
            .with_data_path(calibration_dir.path())
            .with_wal_buffer_size(WAL_BUFFER_BYTES)
            .with_memory_limit(exact_build_limit)
            .with_background_threads_enabled_for_tests(false)
            .build()
        {
            Ok(calibration) => {
                let snapshot = calibration.observability_snapshot();
                assert_eq!(snapshot.memory.wal_writer_buffer_bytes, WAL_BUFFER_BYTES);
                assert!(snapshot.memory.accounted_bytes <= exact_build_limit);
                calibration.close().unwrap();
                break;
            }
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, exact_build_limit);
                assert!(required > exact_build_limit);
                exact_build_limit = required;
            }
            Err(error) => panic!("unexpected calibration failure: {error}"),
        }
    }

    let exact_dir = TempDir::new().unwrap();
    let exact = StorageBuilder::new()
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_data_path(exact_dir.path())
        .with_wal_buffer_size(WAL_BUFFER_BYTES)
        .with_memory_limit(exact_build_limit)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .expect("the exact accounted total must fit");
    let exact_snapshot = exact.observability_snapshot();
    assert_eq!(
        exact_snapshot.memory.wal_writer_buffer_bytes,
        WAL_BUFFER_BYTES
    );
    assert_eq!(
        exact_snapshot.wal.write_buffer_capacity_bytes,
        WAL_BUFFER_BYTES as u64
    );
    assert!(exact_snapshot.memory.accounted_bytes <= exact_build_limit);
    assert_memory_snapshot_component_sum(&exact_snapshot.memory);
    exact.close().unwrap();

    let rejected_parent = TempDir::new().unwrap();
    let rejected_path = rejected_parent.path().join("data");
    let error = StorageBuilder::new()
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_data_path(&rejected_path)
        .with_wal_buffer_size(WAL_BUFFER_BYTES)
        .with_memory_limit(exact_build_limit - 1)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .err()
        .expect("one byte below the complete WAL-backed startup threshold must fail");
    assert!(matches!(
        error,
        TsinkError::MemoryBudgetExceeded {
            budget,
            required
        } if budget == exact_build_limit - 1 && required == exact_build_limit
    ));
}

#[test]
fn finite_limit_smaller_than_wal_writer_buffer_rejects_before_data_path_mutation() {
    const WAL_BUFFER_BYTES: usize = 1024 * 1024;

    let parent = TempDir::new().unwrap();
    let data_path = parent.path().join("data");
    let error = StorageBuilder::new()
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_data_path(&data_path)
        .with_wal_buffer_size(WAL_BUFFER_BYTES)
        .with_memory_limit(WAL_BUFFER_BYTES - 1)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .err()
        .expect("an indivisible WAL buffer larger than the memory limit must be rejected");

    assert!(
        matches!(
            error,
            TsinkError::InvalidConfiguration(ref message)
                if message.contains("smaller than the configured WAL writer-buffer capacity")
        ),
        "unexpected configuration error: {error}"
    );
    assert!(
        !data_path.exists(),
        "configuration validation must run before the persistent data path is created"
    );
}

#[test]
fn wal_writer_buffer_accounting_is_zero_when_no_live_wal_exists() {
    let volatile = StorageBuilder::new()
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_wal_buffer_size(1024 * 1024)
        .build()
        .expect("volatile storage");
    let volatile_snapshot = volatile.observability_snapshot();
    assert!(!volatile_snapshot.wal.enabled);
    assert_eq!(volatile_snapshot.limits.wal_write_buffer_bytes, None);
    assert_eq!(volatile_snapshot.memory.wal_writer_buffer_bytes, 0);
    assert_memory_snapshot_component_sum(&volatile_snapshot.memory);
    volatile.close().unwrap();

    let persistent_dir = TempDir::new().unwrap();
    let wal_disabled = StorageBuilder::new()
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_data_path(persistent_dir.path())
        .with_wal_enabled(false)
        .with_wal_buffer_size(1024 * 1024)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .expect("persistent storage without a WAL");
    let disabled_snapshot = wal_disabled.observability_snapshot();
    assert!(!disabled_snapshot.wal.enabled);
    assert_eq!(disabled_snapshot.limits.wal_write_buffer_bytes, None);
    assert_eq!(disabled_snapshot.memory.wal_writer_buffer_bytes, 0);
    assert_memory_snapshot_component_sum(&disabled_snapshot.memory);
    wal_disabled.close().unwrap();
}

#[test]
fn modeled_write_input_bytes_counts_identity_blob_string_and_histogram_storage() {
    let histogram = sample_histogram(3);
    let rows = vec![
        Row::new("numeric", DataPoint::new(1, 1.0)),
        Row::with_labels(
            "bytes",
            vec![Label::new("host", "a")],
            DataPoint::new(2, Value::Bytes(vec![1; 7])),
        ),
        Row::new("string", DataPoint::new(3, Value::String("éé".to_string()))),
        Row::new("histogram", DataPoint::new(4, histogram.clone())),
    ];

    let row_and_identity_bytes = rows.len() * std::mem::size_of::<Row>()
        + rows.iter().map(|row| row.metric().len()).sum::<usize>()
        + std::mem::size_of::<Label>()
        + "host".len()
        + "a".len();
    let histogram_bytes = std::mem::size_of::<NativeHistogram>()
        + histogram.negative_spans.len() * std::mem::size_of::<HistogramBucketSpan>()
        + histogram.negative_deltas.len() * std::mem::size_of::<i64>()
        + histogram.negative_counts.len() * std::mem::size_of::<f64>()
        + histogram.positive_spans.len() * std::mem::size_of::<HistogramBucketSpan>()
        + histogram.positive_deltas.len() * std::mem::size_of::<i64>()
        + histogram.positive_counts.len() * std::mem::size_of::<f64>()
        + histogram.custom_values.len() * std::mem::size_of::<f64>();
    let expected = row_and_identity_bytes + 7 + "éé".len() + histogram_bytes;

    assert_eq!(
        crate::modeled_write_batch_input_bytes(&rows).unwrap(),
        expected
    );
}

#[test]
fn write_batch_row_and_modeled_byte_limits_are_exact_and_preallocation_safe() {
    let exact_rows = vec![
        Row::new("row_limit", DataPoint::new(1, 1.0)),
        Row::new("row_limit", DataPoint::new(2, 2.0)),
    ];
    let exact = in_memory_storage_with_limits(WriteBatchLimits {
        max_rows: Some(exact_rows.len()),
        max_modeled_input_bytes: None,
    });
    exact.insert_rows(&exact_rows).unwrap();
    assert_eq!(exact.select("row_limit", &[], 0, 3).unwrap().len(), 2);
    exact.close().unwrap();

    let over = in_memory_storage_with_limits(WriteBatchLimits {
        max_rows: Some(exact_rows.len()),
        max_modeled_input_bytes: None,
    });
    let over_rows = vec![
        Row::new("best_effort_limit", DataPoint::new(1, 1.0)),
        Row::new("best_effort_limit", DataPoint::new(2, 2.0)),
        Row::new("best_effort_limit", DataPoint::new(3, 3.0)),
    ];
    let err = over
        .write_batch(&over_rows, WriteMode::BestEffort)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WriteBatchRowLimitExceeded {
            limit: 2,
            submitted: 3
        }
    ));
    let rejected_snapshot = over.memory_observability_snapshot();
    assert_eq!(rejected_snapshot.write_transient_bytes, 0);
    assert_eq!(rejected_snapshot.write_transient_reservations_total, 0);
    assert!(over.list_metrics().unwrap().is_empty());
    over.close().unwrap();

    let byte_row = Row::with_labels(
        "byte_limit",
        vec![Label::new("site", "one")],
        DataPoint::new(1, Value::String("payload".to_string())),
    );
    let modeled = crate::modeled_write_batch_input_bytes(std::slice::from_ref(&byte_row)).unwrap();
    let exact = in_memory_storage_with_limits(WriteBatchLimits {
        max_rows: None,
        max_modeled_input_bytes: Some(modeled),
    });
    exact.insert_rows(std::slice::from_ref(&byte_row)).unwrap();
    exact.close().unwrap();

    let over = in_memory_storage_with_limits(WriteBatchLimits {
        max_rows: None,
        max_modeled_input_bytes: Some(modeled - 1),
    });
    let err = over.insert_rows(&[byte_row]).unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WriteBatchInputLimitExceeded {
            limit,
            submitted
        } if limit == modeled - 1 && submitted == modeled
    ));
    assert_eq!(
        over.memory_observability_snapshot()
            .write_transient_reservations_total,
        0
    );
    over.close().unwrap();
}

#[test]
fn huge_blob_string_and_histogram_values_reject_before_transient_reservation() {
    let huge_values = vec![
        Value::Bytes(vec![7; 2 * 1024 * 1024]),
        Value::String("s".repeat(2 * 1024 * 1024)),
        Value::Histogram(Box::new(sample_histogram(256 * 1024))),
    ];

    for (index, value) in huge_values.into_iter().enumerate() {
        let storage = in_memory_storage_with_limits(WriteBatchLimits {
            max_rows: Some(1),
            max_modeled_input_bytes: Some(4 * 1024),
        });
        let err = storage
            .insert_rows(&[Row::new(
                format!("huge_value_{index}"),
                DataPoint::new(1, value),
            )])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::WriteBatchInputLimitExceeded { limit: 4096, .. }
        ));
        let snapshot = storage.memory_observability_snapshot();
        assert_eq!(snapshot.write_transient_bytes, 0);
        assert_eq!(snapshot.write_transient_reservations_total, 0);
        storage.close().unwrap();
    }
}

#[test]
fn transient_leases_release_on_success_rollback_error_and_panic() {
    let storage = in_memory_storage_with_limits(WriteBatchLimits::default());

    storage
        .insert_rows(&[Row::new("success", DataPoint::new(1, 1.0))])
        .unwrap();
    let success = storage.memory_observability_snapshot();
    assert_eq!(success.write_transient_bytes, 0);
    assert!(success.peak_write_transient_bytes > 0);
    assert_eq!(success.write_transient_reservations_total, 1);

    let err = storage
        .insert_rows(&[
            Row::new("rolled_back", DataPoint::new(1, 1.0)),
            Row::new(
                "rolled_back",
                DataPoint::new(2, Value::String("wrong family".to_string())),
            ),
        ])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    assert!(
        storage
            .catalog
            .registry
            .read()
            .resolve_existing("rolled_back", &[])
            .is_none(),
        "prepare failure must roll back the newly allocated series"
    );
    let rolled_back = storage.memory_observability_snapshot();
    assert_eq!(rolled_back.write_transient_bytes, 0);
    assert_eq!(rolled_back.write_transient_reservations_total, 2);

    let panic_result = catch_unwind(AssertUnwindSafe(|| {
        let _lease = storage.reserve_write_transient_memory(8 * 1024).unwrap();
        panic!("exercise transient reservation drop during unwind");
    }));
    assert!(panic_result.is_err());
    let after_panic = storage.memory_observability_snapshot();
    assert_eq!(after_panic.write_transient_bytes, 0);
    assert_eq!(after_panic.write_transient_reservations_total, 3);
    assert!(after_panic.peak_write_transient_bytes >= 8 * 1024);
    assert!(after_panic.write_transient_bytes_estimated);
    storage.close().unwrap();
}

#[test]
fn transient_reservation_budget_has_exact_coexistence_boundary_and_counters() {
    let storage = in_memory_storage_with_limits(WriteBatchLimits::default());
    storage.refresh_memory_usage();
    let retained = storage.memory.used_bytes.load(Ordering::Acquire);
    let allowance = 16 * 1024_u64;
    storage
        .memory
        .budget_bytes
        .store(retained + allowance, Ordering::Release);

    let lease = storage
        .reserve_write_transient_memory(allowance as usize)
        .unwrap();
    let at_limit = storage.memory_observability_snapshot();
    assert_eq!(at_limit.write_transient_bytes, allowance as usize);
    assert_eq!(at_limit.write_transient_reservations_total, 1);
    assert_eq!(at_limit.write_transient_rejections_total, 0);

    let err = storage.reserve_write_transient_memory(1).unwrap_err();
    assert!(matches!(
        err,
        TsinkError::MemoryBudgetExceeded { budget, required }
            if budget == (retained + allowance) as usize
                && required == (retained + allowance + 1) as usize
    ));
    let rejected = storage.memory_observability_snapshot();
    assert_eq!(rejected.write_transient_bytes, allowance as usize);
    assert_eq!(rejected.write_transient_reservations_total, 1);
    assert_eq!(rejected.write_transient_rejections_total, 1);
    assert_eq!(rejected.pressure.rejections_total, 1);
    assert_eq!(
        rejected.pressure.level,
        Some(MemoryPressureLevel::Rejecting)
    );

    drop(lease);
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .write_transient_bytes,
        0
    );
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    storage.close().unwrap();
}

#[test]
fn canonical_memory_rejection_result_has_an_exact_admission_boundary() {
    let limits = WriteBatchLimits {
        max_rows: Some(1),
        max_modeled_input_bytes: Some(1024 * 1024),
    };
    let row = Row::new(
        "canonical_response_memory",
        DataPoint::new(1, Value::String("payload".repeat(256))),
    );
    let response_bytes = ChunkStorage::modeled_write_rejection_result_bytes_for_tests(1).unwrap();

    let exact = in_memory_storage_with_limits(limits);
    exact.refresh_memory_usage();
    let exact_retained = exact.memory.used_bytes.load(Ordering::Acquire);
    exact.memory.budget_bytes.store(
        exact_retained + u64::try_from(response_bytes).unwrap(),
        Ordering::Release,
    );
    let result = exact
        .write_batch(std::slice::from_ref(&row), WriteMode::Atomic)
        .expect("the separately admitted rejection result should fit exactly");
    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 1);
    let crate::RowWriteStatus::Rejected(rejection) = &result.outcomes[0].status else {
        panic!("memory admission must reject the row");
    };
    assert_eq!(
        rejection.category,
        crate::WriteRejectionCategory::MemoryPressure
    );
    let exact_snapshot = exact.memory_observability_snapshot();
    assert_eq!(exact_snapshot.write_transient_bytes, 0);
    assert_eq!(exact_snapshot.write_transient_reservations_total, 1);
    exact.memory.budget_bytes.store(u64::MAX, Ordering::Release);
    exact.close().unwrap();

    let below = in_memory_storage_with_limits(limits);
    below.refresh_memory_usage();
    let below_retained = below.memory.used_bytes.load(Ordering::Acquire);
    below.memory.budget_bytes.store(
        below_retained + u64::try_from(response_bytes - 1).unwrap(),
        Ordering::Release,
    );
    let error = below
        .write_batch(&[row], WriteMode::Atomic)
        .expect_err("one byte below the response envelope must remain an outer error");
    assert!(matches!(error, TsinkError::MemoryBudgetExceeded { .. }));
    let below_snapshot = below.memory_observability_snapshot();
    assert_eq!(below_snapshot.write_transient_bytes, 0);
    assert_eq!(below_snapshot.write_transient_reservations_total, 0);
    below.memory.budget_bytes.store(u64::MAX, Ordering::Release);
    below.close().unwrap();
}

#[test]
fn reused_lease_resets_retained_overlap_without_double_reservation() {
    let storage = in_memory_storage_with_limits(WriteBatchLimits::default());
    storage.refresh_memory_usage();
    let retained = storage.memory.used_bytes.load(Ordering::Acquire);
    let base = 8 * 1024;
    let overlap = 4 * 1024;
    storage.memory.budget_bytes.store(
        retained + u64::try_from(base + overlap).unwrap(),
        Ordering::Release,
    );

    let lease = storage.reserve_write_transient_memory(base).unwrap();
    storage
        .ensure_write_transient_memory(&lease, base + overlap)
        .unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .write_transient_bytes,
        base + overlap
    );

    lease.reset_to_base();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .write_transient_bytes,
        base
    );
    storage
        .ensure_write_transient_memory(&lease, base + overlap)
        .unwrap();
    let reused = storage.memory_observability_snapshot();
    assert_eq!(reused.write_transient_bytes, base + overlap);
    assert_eq!(reused.write_transient_reservations_total, 1);
    assert_eq!(reused.write_transient_rejections_total, 0);

    drop(lease);
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .write_transient_bytes,
        0
    );
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    storage.close().unwrap();
}

#[test]
fn concurrent_writers_cannot_overcommit_coexisting_transient_memory() {
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
                retention_enforced: false,
                max_writers: 2,
                write_timeout: Duration::ZERO,
                memory_budget_bytes: u64::MAX,
                background_threads_enabled: false,
                background_fail_fast: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let hook_calls = Arc::new(AtomicUsize::new(0));
    storage.set_ingest_post_samples_hook({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        let hook_calls = Arc::clone(&hook_calls);
        move || {
            if hook_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                entered.wait();
                release.wait();
            }
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::new("first_writer", DataPoint::new(1, 1.0))])
    });
    entered.wait();

    let during = storage.memory_observability_snapshot();
    assert!(during.write_transient_bytes > 0);
    assert_eq!(during.write_transient_reservations_total, 1);
    let exact_budget = storage.memory.used_bytes.load(Ordering::Acquire)
        + u64::try_from(during.write_transient_bytes).unwrap();
    storage
        .memory
        .budget_bytes
        .store(exact_budget, Ordering::Release);

    let second_result = storage.insert_rows(&[Row::new("second_writer", DataPoint::new(1, 2.0))]);
    let err = match second_result {
        Err(error) => error,
        Ok(()) => {
            storage
                .memory
                .budget_bytes
                .store(u64::MAX, Ordering::Release);
            release.wait();
            writer.join().unwrap().unwrap();
            storage.clear_ingest_post_samples_hook();
            panic!("the second writer bypassed the coexisting transient-memory envelope");
        }
    };
    assert!(matches!(err, TsinkError::MemoryBudgetExceeded { .. }));
    let rejected = storage.memory_observability_snapshot();
    assert_eq!(
        rejected.write_transient_bytes, during.write_transient_bytes,
        "the rejected writer must not install a partial scratch lease"
    );
    assert_eq!(rejected.write_transient_reservations_total, 1);
    assert_eq!(rejected.pressure.rejections_total, 1);

    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    release.wait();
    writer.join().unwrap().unwrap();
    storage.clear_ingest_post_samples_hook();
    let finished = storage.memory_observability_snapshot();
    assert_eq!(finished.write_transient_bytes, 0);
    assert!(finished.peak_write_transient_bytes >= during.write_transient_bytes);
    assert!(storage
        .list_metrics()
        .unwrap()
        .iter()
        .all(|series| { series.name != "second_writer" }));
    storage.close().unwrap();
}

#[test]
fn postings_estimator_clone_lease_blocks_concurrent_storage_memory_admission() {
    let storage = Arc::new(in_memory_storage_with_limits(WriteBatchLimits::default()));
    {
        let registry = storage.catalog.registry.read();
        for host in 0..512 {
            registry
                .resolve_or_insert(
                    "cpu",
                    &[
                        Label::new("host", host.to_string()),
                        Label::new("job", "api"),
                    ],
                )
                .unwrap();
        }
    }
    storage.refresh_memory_usage();
    let planned = Arc::new(vec![SeriesKey {
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "new"), Label::new("job", "api")],
    }]);
    let mut clone_peak = 0usize;
    storage
        .catalog
        .registry
        .read()
        .estimate_new_series_memory_growth_bytes_with_transient_admission(&planned, |required| {
            clone_peak = clone_peak.max(required);
            Ok(())
        })
        .unwrap();
    assert!(clone_peak > 0);

    let base = 1024usize;
    let used = storage.memory.used_bytes.load(Ordering::Acquire);
    storage.memory.budget_bytes.store(
        used.saturating_add(u64::try_from(base + clone_peak).unwrap()),
        Ordering::Release,
    );
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let estimator = {
        let storage = Arc::clone(&storage);
        let planned = Arc::clone(&planned);
        thread::spawn(move || {
            let lease = storage.reserve_write_transient_memory(base)?;
            let registry = storage.catalog.registry.read();
            let mut blocked = false;
            let result = registry.estimate_new_series_memory_growth_bytes_with_transient_admission(
                &planned,
                |required| {
                    storage.ensure_write_transient_memory(&lease, base.saturating_add(required))?;
                    if required == clone_peak && !blocked {
                        blocked = true;
                        admitted_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    Ok(())
                },
            );
            drop(registry);
            drop(lease);
            result.map(|_| ())
        })
    };

    admitted_rx.recv().unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .write_transient_bytes,
        base + clone_peak
    );
    assert!(matches!(
        storage.reserve_write_transient_memory(1),
        Err(TsinkError::MemoryBudgetExceeded { .. })
    ));
    release_tx.send(()).unwrap();
    estimator.join().unwrap().unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .write_transient_bytes,
        0
    );
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    storage.close().unwrap();
}

#[test]
fn postings_estimation_and_retained_transfer_have_exact_global_memory_boundary() {
    fn seeded_storage() -> ChunkStorage {
        let storage = in_memory_storage_with_limits(WriteBatchLimits::default());
        {
            let registry = storage.catalog.registry.read();
            for host in 0..512 {
                registry
                    .resolve_or_insert(
                        "cpu",
                        &[
                            Label::new("host", host.to_string()),
                            Label::new("job", "api"),
                        ],
                    )
                    .unwrap();
            }
        }
        storage.refresh_memory_usage();
        storage
    }

    let row = Row::with_labels(
        "cpu",
        vec![Label::new("host", "new"), Label::new("job", "api")],
        DataPoint::new(1, 1.0),
    );
    let probe = seeded_storage();
    probe.insert_rows(std::slice::from_ref(&row)).unwrap();
    let exact = probe
        .memory_observability_snapshot()
        .peak_write_transient_bytes;
    assert!(exact > 1);
    probe.close().unwrap();

    let one_under = seeded_storage();
    let one_under_used = one_under.memory.used_bytes.load(Ordering::Acquire);
    one_under.memory.budget_bytes.store(
        one_under_used.saturating_add(u64::try_from(exact - 1).unwrap()),
        Ordering::Release,
    );
    assert!(matches!(
        one_under.insert_rows(std::slice::from_ref(&row)),
        Err(TsinkError::MemoryBudgetExceeded { .. })
    ));
    let rejected = one_under.memory_observability_snapshot();
    assert_eq!(rejected.write_transient_bytes, 0);
    assert!(one_under
        .catalog
        .registry
        .read()
        .resolve_existing(
            "cpu",
            &[Label::new("host", "new"), Label::new("job", "api")]
        )
        .is_none());
    one_under
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    one_under.close().unwrap();

    let admitted = seeded_storage();
    let admitted_used = admitted.memory.used_bytes.load(Ordering::Acquire);
    admitted.memory.budget_bytes.store(
        admitted_used.saturating_add(u64::try_from(exact).unwrap()),
        Ordering::Release,
    );
    admitted.insert_rows(&[row]).unwrap();
    let snapshot = admitted.memory_observability_snapshot();
    assert_eq!(snapshot.write_transient_bytes, 0);
    assert!(snapshot.accounted_bytes <= snapshot.budgeted_bytes);
    assert_memory_snapshot_component_sum(&snapshot);
    admitted
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    admitted.close().unwrap();
}

#[test]
fn active_state_atomic_staging_has_exact_global_memory_boundary() {
    const SEEDED_POINTS: usize = 1_024;
    let next_timestamp = i64::try_from(SEEDED_POINTS).unwrap();

    let (probe, probe_series_id) = populated_active_series_storage();
    let seed_peak = probe
        .memory_observability_snapshot()
        .peak_write_transient_bytes;
    assert_eq!(seed_peak, 0);
    probe
        .append_point_to_series(
            probe_series_id,
            ValueLane::Numeric,
            next_timestamp,
            Value::F64(7.0),
        )
        .unwrap();
    let exact_transient = probe
        .memory_observability_snapshot()
        .peak_write_transient_bytes;
    assert!(exact_transient > 1);
    probe.close().unwrap();

    let (exact, exact_series_id) = populated_active_series_storage();
    let exact_used = exact.memory.used_bytes.load(Ordering::Acquire);
    let exact_budget = exact_used + u64::try_from(exact_transient).unwrap();
    exact
        .memory
        .budget_bytes
        .store(exact_budget, Ordering::Release);
    exact
        .append_point_to_series(
            exact_series_id,
            ValueLane::Numeric,
            next_timestamp,
            Value::F64(7.0),
        )
        .expect("the exact populated-state staging envelope must be admitted");
    let admitted = exact.memory_observability_snapshot();
    assert_eq!(admitted.write_transient_bytes, 0);
    assert_eq!(admitted.peak_write_transient_bytes, exact_transient);
    assert!(admitted.accounted_bytes <= admitted.budgeted_bytes);
    assert_eq!(
        exact
            .select(
                "active_staging_boundary",
                &[],
                0,
                i64::try_from(SEEDED_POINTS + 1).unwrap(),
            )
            .unwrap()
            .len(),
        SEEDED_POINTS + 1
    );
    exact.memory.budget_bytes.store(u64::MAX, Ordering::Release);
    exact.close().unwrap();

    let (one_under, one_under_series_id) = populated_active_series_storage();
    let one_under_used = one_under.memory.used_bytes.load(Ordering::Acquire);
    assert_eq!(one_under_used, exact_used);
    one_under.memory.budget_bytes.store(
        one_under_used + u64::try_from(exact_transient - 1).unwrap(),
        Ordering::Release,
    );
    let err = one_under
        .append_point_to_series(
            one_under_series_id,
            ValueLane::Numeric,
            next_timestamp,
            Value::F64(7.0),
        )
        .expect_err("one byte below populated-state staging must reject before publication");
    assert!(matches!(
        err,
        TsinkError::MemoryBudgetExceeded { budget, required }
            if budget == (one_under_used + u64::try_from(exact_transient - 1).unwrap()) as usize
                && required == (one_under_used + u64::try_from(exact_transient).unwrap()) as usize
    ));
    let rejected = one_under.memory_observability_snapshot();
    assert_eq!(rejected.write_transient_bytes, 0);
    assert_eq!(rejected.write_transient_reservations_total, 1);
    assert_eq!(rejected.write_transient_rejections_total, 1);
    assert_eq!(
        one_under
            .select(
                "active_staging_boundary",
                &[],
                0,
                i64::try_from(SEEDED_POINTS + 1).unwrap(),
            )
            .unwrap()
            .len(),
        SEEDED_POINTS,
        "rejected staging must not publish the pending point"
    );
    one_under
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    one_under.close().unwrap();
}

#[test]
fn startup_wal_replay_is_streamed_admitted_and_reports_fixed_writer_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let limits = WriteBatchLimits {
        max_rows: Some(16),
        max_modeled_input_bytes: Some(1024 * 1024),
    };
    let histogram = sample_histogram(4);
    let rows = vec![
        Row::new("replay_numeric", DataPoint::new(1, 1.0)),
        Row::new(
            "replay_string",
            DataPoint::new(2, Value::String("replayed".to_string())),
        ),
        Row::new("replay_histogram", DataPoint::new(3, histogram.clone())),
    ];

    let writer = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_wal_buffer_size(12_345)
        .with_write_batch_limits(limits)
        .with_memory_limit(32 * 1024 * 1024)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    writer.insert_rows(&rows).unwrap();
    let live_snapshot = writer.observability_snapshot().memory;
    assert!(live_snapshot.wal_series_definition_cache_bytes > 0);
    assert!(!live_snapshot
        .excluded_categories
        .iter()
        .any(|category| category == "wal_series_definition_cache"));
    assert_memory_snapshot_component_sum(&live_snapshot);
    writer.abandon_without_close_for_tests().unwrap();
    drop(writer);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_wal_buffer_size(12_345)
        .with_write_batch_limits(limits)
        .with_memory_limit(32 * 1024 * 1024)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    let snapshot = reopened.observability_snapshot();
    assert_eq!(snapshot.limits.max_write_batch_rows, Some(16));
    assert_eq!(
        snapshot.limits.max_write_batch_input_bytes,
        Some(1024 * 1024)
    );
    assert_eq!(snapshot.limits.wal_write_buffer_bytes, Some(12_345));
    assert_eq!(snapshot.wal.write_buffer_capacity_bytes, 12_345);
    assert_eq!(snapshot.memory.wal_writer_buffer_bytes, 12_345);
    assert!(snapshot.wal.replay_frames_total >= 4);
    assert_eq!(snapshot.wal.replay_points_total, 3);
    assert_eq!(snapshot.memory.write_transient_bytes, 0);
    assert!(snapshot.memory.wal_series_definition_cache_bytes > 0);
    assert!(snapshot.memory.peak_write_transient_bytes > 0);
    assert!(snapshot.memory.write_transient_reservations_total >= 4);
    assert!(!snapshot
        .memory
        .excluded_categories
        .iter()
        .any(|category| category == "wal_writer_buffer_finite_unbudgeted"));
    assert!(!snapshot
        .memory
        .excluded_categories
        .iter()
        .any(|category| category == "wal_series_definition_cache"));
    assert_memory_snapshot_component_sum(&snapshot.memory);

    assert_eq!(
        reopened.select("replay_numeric", &[], 0, 4).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert_eq!(
        reopened.select("replay_string", &[], 0, 4).unwrap(),
        vec![DataPoint::new(2, Value::String("replayed".to_string()))]
    );
    assert_eq!(
        reopened.select("replay_histogram", &[], 0, 4).unwrap(),
        vec![DataPoint::new(3, histogram)]
    );
    reopened.close().unwrap();
}

#[test]
fn retained_wal_cache_charge_releases_after_successful_flush_reset() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            memory_budget_bytes: 32 * 1024 * 1024,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::new("cache_flush_reset", DataPoint::new(1, 1.0))])
        .unwrap();
    let before = storage.memory_observability_snapshot();
    assert!(before.wal_series_definition_cache_bytes > 0);
    assert_memory_snapshot_component_sum(&before);

    storage.flush().unwrap();
    let after = storage.memory_observability_snapshot();
    assert_eq!(after.wal_series_definition_cache_bytes, 0);
    assert_memory_snapshot_component_sum(&after);
    assert!(after.accounted_bytes < before.accounted_bytes);
    storage.close().unwrap();
}

#[test]
fn startup_wal_replay_enforces_configured_batch_rows_before_decode() {
    let temp_dir = TempDir::new().unwrap();
    let writer = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    writer
        .insert_rows(&[
            Row::new("replay_limit_a", DataPoint::new(1, 1.0)),
            Row::new("replay_limit_b", DataPoint::new(1, 2.0)),
            Row::new("replay_limit_c", DataPoint::new(1, 3.0)),
        ])
        .unwrap();
    writer.abandon_without_close_for_tests().unwrap();
    drop(writer);

    let err = match StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_write_batch_limits(WriteBatchLimits {
            max_rows: Some(2),
            max_modeled_input_bytes: None,
        })
        .with_background_threads_enabled_for_tests(false)
        .build()
    {
        Ok(_) => panic!("replay must enforce the configured row bound"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        TsinkError::WriteBatchRowLimitExceeded {
            limit: 2,
            submitted: 3
        }
    ));
}
