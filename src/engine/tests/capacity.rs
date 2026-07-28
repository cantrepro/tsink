use super::super::maintenance::{PersistedCatalogPublication, PersistedCatalogTransition};
use super::*;
use crate::engine::series::SeriesKey;
use crate::{
    MemoryPressureLevel, RowWriteStatus, WriteMode, WriteRejectionCategory,
    MAX_SUPPORTED_LABELS_PER_SERIES,
};
use std::sync::atomic::Ordering;

fn new_cardinality_test_storage(
    max_labels_per_series: usize,
    max_series_identity_bytes: usize,
    max_new_series_per_window: Option<usize>,
    new_series_window_units: i64,
    current_time: i64,
) -> Arc<ChunkStorage> {
    Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            4,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                max_writers: 32,
                write_timeout: Duration::from_secs(2),
                max_labels_per_series,
                max_series_identity_bytes,
                max_new_series_per_window,
                new_series_window_units,
                new_series_window_nanos: u64::try_from(new_series_window_units.max(1))
                    .unwrap_or(u64::MAX)
                    .saturating_mul(1_000_000_000),
                background_threads_enabled: false,
                background_fail_fast: false,
                #[cfg(test)]
                current_time_override: Some(current_time),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    )
}

fn new_memory_budget_test_storage(temp_dir: &TempDir, memory_budget_bytes: u64) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
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
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes,
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
    .unwrap()
}

fn registry_growth_estimate_for_rows(storage: &ChunkStorage, rows: &[Row]) -> usize {
    let planned_series = rows
        .iter()
        .map(|row| SeriesKey {
            metric: row.metric().to_string(),
            labels: row.labels().to_vec(),
        })
        .collect::<Vec<_>>();
    storage
        .catalog
        .registry
        .read()
        .estimate_new_series_memory_growth_bytes(&planned_series)
        .unwrap()
}

#[test]
fn memory_budget_spills_to_l0_and_preserves_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = new_memory_budget_test_storage(&temp_dir, 64 * 1024);

    storage
        .insert_rows(&[
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(4, 4.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(5, 5.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(6, 6.0)),
        ])
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let snapshot = storage.observability_snapshot();
    assert!(
        snapshot.memory.active_and_sealed_bytes > 0,
        "test workload should include budgeted hot chunk state"
    );
    let tightened_budget = snapshot
        .memory
        .budgeted_bytes
        .saturating_sub(snapshot.memory.active_and_sealed_bytes)
        .saturating_add(1);
    storage
        .memory
        .budget_bytes
        .store(tightened_budget as u64, std::sync::atomic::Ordering::SeqCst);
    let mut admitted_budget = match storage.enforce_memory_budget_if_needed() {
        Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
            assert_eq!(budget, tightened_budget);
            assert!(required > budget);
            required
        }
        result => panic!("expected finite spill-publication rejection, got {result:?}"),
    };
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());
    assert_eq!(
        storage.select("budget_metric", &labels, 0, 10).unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
            DataPoint::new(5, 5.0),
            DataPoint::new(6, 6.0),
        ],
        "a rejected spill must preserve every accepted point",
    );

    let mut persisted = false;
    for _ in 0..8 {
        storage
            .memory
            .budget_bytes
            .store(admitted_budget as u64, std::sync::atomic::Ordering::SeqCst);
        match storage.persist_segment_with_outcome() {
            Ok(outcome) => {
                assert!(outcome.persisted);
                persisted = true;
                break;
            }
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, admitted_budget);
                assert!(required > admitted_budget);
                admitted_budget = required;
            }
            Err(error) => panic!("unexpected admitted spill retry failure: {error}"),
        }
    }
    assert!(
        persisted,
        "spill retry did not converge after eight structured memory-boundary admissions",
    );
    assert_engine_memory_usage_reconciled(&storage);

    let l0_segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(
        !l0_segments.is_empty(),
        "budget pressure should flush sealed chunks to L0 before close"
    );

    let points = storage.select("budget_metric", &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
            DataPoint::new(5, 5.0),
            DataPoint::new(6, 6.0),
        ]
    );
    assert_eq!(storage.memory_budget(), admitted_budget);
    let post_snapshot = storage.observability_snapshot();
    assert!(
        post_snapshot.memory.active_and_sealed_bytes < snapshot.memory.active_and_sealed_bytes,
        "spill pressure should reduce budgeted hot chunk state even when shared metadata remains"
    );

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("budget_metric", &labels)
        .unwrap()
        .series_id;
    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let sealed_count = sealed
        .get(&series_id)
        .map(|chunks| chunks.len())
        .unwrap_or(0);
    assert_eq!(
        sealed_count, 0,
        "the persisted sealed chunk should be evicted after spill"
    );

    storage.close().unwrap();
}

#[test]
fn reduced_hot_chunk_state_fits_previous_spill_budget() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = new_memory_budget_test_storage(&temp_dir, 64 * 1024);

    storage
        .insert_rows(&[
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(4, 4.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(5, 5.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(6, 6.0)),
        ])
        .unwrap();

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let l0_segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(
        l0_segments.is_empty(),
        "encoded-only sealed chunks should keep this workload under the previous spill budget"
    );

    assert_eq!(
        storage.select("budget_metric", &labels, 0, 10).unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
            DataPoint::new(5, 5.0),
            DataPoint::new(6, 6.0),
        ]
    );
    assert!(storage.memory_used() <= storage.memory_budget());

    let mut admitted_budget = 64 * 1024;
    let mut rejections = 0usize;
    let mut closed = false;
    for _ in 0..8 {
        match storage.close() {
            Ok(()) => {
                closed = true;
                break;
            }
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, admitted_budget);
                assert!(required > budget);
                rejections = rejections.saturating_add(1);
                assert_eq!(
                    storage.select("budget_metric", &labels, 0, 10).unwrap(),
                    vec![
                        DataPoint::new(1, 1.0),
                        DataPoint::new(2, 2.0),
                        DataPoint::new(3, 3.0),
                        DataPoint::new(4, 4.0),
                        DataPoint::new(5, 5.0),
                        DataPoint::new(6, 6.0),
                    ],
                    "a rejected close must leave the accepted rows queryable for an admitted retry",
                );
                admitted_budget = required;
                storage
                    .memory
                    .budget_bytes
                    .store(admitted_budget as u64, Ordering::Release);
            }
            Err(error) => panic!("unexpected bounded close error: {error}"),
        }
    }
    assert!(
        closed,
        "close retry did not converge after eight structured memory-boundary admissions",
    );
    assert!(
        rejections > 0,
        "the retained state fits, but the initial finite close-publication peak must reject",
    );
}

#[test]
fn memory_budget_stats_reflect_builder_configuration() {
    let storage = StorageBuilder::new()
        .with_memory_limit(1234)
        .build()
        .unwrap();
    let snapshot = storage.observability_snapshot();

    assert_eq!(storage.memory_budget(), 1234);
    assert_eq!(
        snapshot.memory.accounted_bytes,
        snapshot.memory.budgeted_bytes
    );
    assert_eq!(
        snapshot
            .memory
            .estimated_accounted_bytes
            .saturating_add(snapshot.memory.persisted_mmap_bytes),
        snapshot.memory.accounted_bytes
    );
    assert_eq!(storage.memory_used(), snapshot.memory.budgeted_bytes);
    assert_eq!(snapshot.memory.excluded_bytes, 0);
    assert!(!snapshot.memory.excluded_bytes_known);
    assert!(!snapshot.memory.excluded_categories.is_empty());
    assert!(
        snapshot.memory.accounted_bytes >= storage.memory_budget(),
        "the configured budget is smaller than the instance's already-accounted retained state",
    );
    assert_eq!(
        snapshot.memory.pressure.level,
        Some(MemoryPressureLevel::Rejecting)
    );
    assert_eq!(
        snapshot.memory.pressure.approaching_limit_basis_points,
        Some(9_000)
    );
    assert_eq!(
        snapshot.memory.pressure.approaching_limit_bytes,
        Some(1_111)
    );
}

#[test]
fn tombstone_staging_is_observable_and_released_with_unlimited_accounting() {
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, u64::MAX);
    storage.refresh_memory_usage();
    let baseline_used = storage.memory_used();
    let baseline_tombstones = storage.observability_snapshot().memory.tombstone_bytes;

    {
        let mut reservation = storage.tombstone_memory_reservation();
        reservation.resize(8 * 1024).unwrap();
        assert_eq!(storage.memory_used(), baseline_used + 8 * 1024);
        assert_eq!(
            storage.observability_snapshot().memory.tombstone_bytes,
            baseline_tombstones + 8 * 1024
        );
    }

    assert_eq!(storage.memory_used(), baseline_used);
    assert_eq!(
        storage.observability_snapshot().memory.tombstone_bytes,
        baseline_tombstones
    );
    storage.close().unwrap();
}

#[test]
fn memory_pressure_levels_have_deterministic_precedence() {
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, 1_000);

    storage.memory.used_bytes.store(899, Ordering::Release);
    let normal = storage.memory_observability_snapshot();
    assert_eq!(normal.pressure.level, Some(MemoryPressureLevel::Normal));
    assert_eq!(normal.pressure.approaching_limit_bytes, Some(900));

    storage.memory.used_bytes.store(900, Ordering::Release);
    assert_eq!(
        storage.memory_observability_snapshot().pressure.level,
        Some(MemoryPressureLevel::ApproachingLimit)
    );

    storage
        .memory
        .active_backpressured_writers
        .store(1, Ordering::Release);
    assert_eq!(
        storage.memory_observability_snapshot().pressure.level,
        Some(MemoryPressureLevel::Backpressured)
    );

    storage
        .memory
        .active_backpressured_writers
        .store(0, Ordering::Release);
    storage.memory.used_bytes.store(1_000, Ordering::Release);
    assert_eq!(
        storage.memory_observability_snapshot().pressure.level,
        Some(MemoryPressureLevel::Rejecting)
    );

    storage
        .observability
        .health
        .maintenance_errors_total
        .store(1, Ordering::Release);
    assert_eq!(
        storage.memory_observability_snapshot().pressure.level,
        Some(MemoryPressureLevel::Degraded)
    );
}

#[test]
fn memory_budget_guard_rejects_writes_when_in_memory_budget_cannot_be_relaxed() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_memory_limit(1)
        .with_write_timeout(Duration::ZERO)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();

    let err = storage
        .insert_rows(&[Row::new("memory_guard_metric", DataPoint::new(1, 1.0))])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::MemoryBudgetExceeded { budget: 1, .. }
    ));
    assert!(
        storage
            .select("memory_guard_metric", &[], 0, 10)
            .unwrap()
            .is_empty(),
        "rejected writes must not mutate in-memory state"
    );
    let pressure = storage.observability_snapshot().memory.pressure;
    assert_eq!(pressure.active_backpressured_writers, 0);
    assert_eq!(pressure.backpressure_events_total, 0);
    assert_eq!(pressure.rejections_total, 1);
}

#[test]
fn memory_budget_rejects_registry_heavy_new_series_before_mutating_registry() {
    let temp_dir = TempDir::new().unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
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
            write_timeout: Duration::ZERO,
            memory_budget_bytes: 2_048,
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
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    let large_label_value = "x".repeat(4_096);
    let labels = vec![Label::new("host", large_label_value.clone())];
    let rows = vec![Row::with_labels(
        "registry_budget_metric",
        labels.clone(),
        DataPoint::new(1, 1.0),
    )];
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(estimated_registry_growth > 0);
    storage.memory.budget_bytes.store(
        estimated_registry_growth.saturating_sub(1) as u64,
        std::sync::atomic::Ordering::Release,
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    assert!(
        matches!(
            err,
            TsinkError::MemoryBudgetExceeded {
                budget,
                required
            } if budget + 1 == estimated_registry_growth && required > budget
        ),
        "unexpected error: {err:?}"
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.memory.registry_bytes, 0);
    assert_eq!(
        snapshot.memory.active_and_sealed_bytes, 0,
        "admission should reject before active or sealed chunk state is created"
    );
    assert!(storage.catalog.registry.read().is_empty());
    assert!(
        storage
            .select("registry_budget_metric", &labels, 0, 10)
            .unwrap()
            .is_empty(),
        "rejected registry-heavy writes must not ingest points"
    );

    storage.close().unwrap();
}

#[test]
fn memory_budget_rejects_many_new_metrics_before_mutating_registry() {
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, usize::MAX as u64);

    let rows = (0..32)
        .map(|idx| {
            Row::with_labels(
                format!("metric_{idx:02}"),
                vec![Label::new("host", "shared")],
                DataPoint::new(1, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(estimated_registry_growth > 0);
    storage.memory.budget_bytes.store(
        estimated_registry_growth.saturating_sub(1) as u64,
        std::sync::atomic::Ordering::Release,
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    assert!(
        matches!(err, TsinkError::MemoryBudgetExceeded { .. }),
        "unexpected error: {err:?}"
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.memory.registry_bytes, 0);
    assert_eq!(snapshot.memory.active_and_sealed_bytes, 0);
    assert!(storage.catalog.registry.read().is_empty());
    assert!(storage.materialized_series_snapshot().is_empty());

    storage.close().unwrap();
}

#[test]
fn memory_budget_rejects_many_new_label_pairs_before_mutating_registry() {
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, usize::MAX as u64);

    let rows = (0..32)
        .map(|idx| {
            Row::with_labels(
                "label_heavy_metric",
                vec![
                    Label::new("host", format!("node-{idx:02}")),
                    Label::new("rack", format!("rack-{}", idx % 8)),
                    Label::new(format!("dynamic_label_{idx:02}"), format!("value_{idx:02}")),
                ],
                DataPoint::new(1, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(estimated_registry_growth > 0);
    storage.memory.budget_bytes.store(
        estimated_registry_growth.saturating_sub(1) as u64,
        std::sync::atomic::Ordering::Release,
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    assert!(
        matches!(err, TsinkError::MemoryBudgetExceeded { .. }),
        "unexpected error: {err:?}"
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.memory.registry_bytes, 0);
    assert_eq!(snapshot.memory.active_and_sealed_bytes, 0);
    assert!(storage.catalog.registry.read().is_empty());
    assert!(storage.materialized_series_snapshot().is_empty());

    storage.close().unwrap();
}

#[test]
fn restart_memory_reconciliation_counts_persisted_registry_and_index_state() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();

        for series_idx in 0..64 {
            storage
                .insert_rows(&[Row::with_labels(
                    "reopen_budget_metric",
                    vec![
                        Label::new("host", format!("host-{series_idx}")),
                        Label::new("rack", format!("rack-{}", series_idx % 8)),
                    ],
                    DataPoint::new(1, series_idx as f64),
                )])
                .unwrap();
        }

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let snapshot = reopened.observability_snapshot();
    let reconciled_used = reopened.memory_used();

    assert_eq!(snapshot.memory.budgeted_bytes, reconciled_used);
    assert!(
        snapshot.memory.registry_bytes > 0,
        "reopened storage should account for persisted registry state"
    );
    assert!(
        snapshot.memory.persisted_index_bytes > 0,
        "reopened storage should account for persisted chunk refs and timestamp indexes"
    );
    assert!(
        snapshot.memory.persisted_mmap_bytes > 0,
        "reopened storage should budget persisted mmap-backed segment state"
    );
    assert_eq!(snapshot.memory.excluded_bytes, 0);
    assert_eq!(snapshot.memory.excluded_persisted_mmap_bytes, 0);

    reopened.close().unwrap();

    let tightened_budget = 1;
    let startup_error = match StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_wal_enabled(false)
        .with_memory_limit(tightened_budget)
        .with_current_time_override_for_tests(0)
        .build()
    {
        Ok(_) => panic!("tiny startup budget must reject before persistent-state loading"),
        Err(err) => err,
    };
    assert!(matches!(
        startup_error,
        TsinkError::MemoryBudgetExceeded { budget, required }
            if budget == tightened_budget && required > budget
    ));

    let verified = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
        .with_current_time_override_for_tests(0)
        .build()
        .expect("a fully-admitted retry must preserve the rejected startup's durable state");
    assert_eq!(
        verified
            .select(
                "reopen_budget_metric",
                &[Label::new("host", "host-0"), Label::new("rack", "rack-0")],
                0,
                10,
            )
            .unwrap(),
        vec![DataPoint::new(1, 0.0)]
    );
    verified.close().unwrap();
}

#[test]
fn restart_query_budget_counts_persisted_mmap_and_rejects_new_writes() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "restart_query_budget_metric",
                    labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "restart_query_budget_metric",
                    labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();

        storage.close().unwrap();
    }

    let baseline = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
        .with_background_threads_enabled_for_tests(false)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let baseline_snapshot = baseline.observability_snapshot();
    assert!(
        baseline_snapshot.memory.persisted_mmap_bytes > 0,
        "reopened storage should include persisted mmap bytes in the budgeted footprint"
    );
    let tightened_budget = baseline.memory_used().saturating_sub(1).max(1);
    baseline.close().unwrap();

    let mut admitted_budget = tightened_budget;
    let reopened = loop {
        match StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_memory_limit(admitted_budget)
            .with_write_timeout(Duration::ZERO)
            .with_background_threads_enabled_for_tests(false)
            .with_current_time_override_for_tests(0)
            .build()
        {
            Ok(storage) => break storage,
            Err(TsinkError::MemoryBudgetExceeded { budget, required }) => {
                assert_eq!(budget, admitted_budget);
                assert!(required > admitted_budget);
                admitted_budget = required;
            }
            Err(err) => panic!("unexpected bounded restart error: {err}"),
        }
    };

    let available_after_startup = reopened
        .memory_budget()
        .saturating_sub(reopened.memory_used());
    assert_eq!(
        reopened
            .select("restart_query_budget_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );

    // Startup reconciliation itself has a larger bounded scratch peak than the retained runtime
    // state, so the smallest admitted startup budget can leave write headroom. Make the attempted
    // registry growth deterministically larger than that headroom instead of relying on the old
    // behavior where startup was allowed to finish already over budget.
    let rejected_rows = (0..256)
        .map(|index| {
            Row::with_labels(
                format!("restart_rejected_metric_{index:02}"),
                vec![Label::new("host", format!("rejected-{index:02}"))],
                DataPoint::new(3, index as f64),
            )
        })
        .collect::<Vec<_>>();
    assert!(available_after_startup < admitted_budget);
    let err = reopened.insert_rows(&rejected_rows).unwrap_err();
    assert!(
        matches!(
            err,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == admitted_budget && required > budget
        ),
        "unexpected error: {err:?}"
    );
    assert_eq!(
        reopened
            .select("restart_query_budget_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
        "budget rejection after restart/query must not mutate persisted data",
    );
    assert!(
        reopened
            .select(
                "restart_rejected_metric_00",
                &[Label::new("host", "rejected-00")],
                0,
                10,
            )
            .unwrap()
            .is_empty(),
        "rejected registry growth must not publish a new series",
    );

    reopened.close().unwrap();
}

#[test]
fn runtime_persisted_segment_load_updates_memory_budget_accounting() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();

        for series_idx in 0..16 {
            storage
                .insert_rows(&[Row::with_labels(
                    "refresh_budget_metric",
                    vec![Label::new("host", format!("node-{series_idx}"))],
                    DataPoint::new(1, series_idx as f64),
                )])
                .unwrap();
        }

        storage.close().unwrap();
    }

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let loaded = load_segment_indexes(&lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path),
        None,
        loaded.next_segment_id,
        ChunkStorageOptions {
            memory_budget_bytes: 1_000_000,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .add_persisted_segments_from_loaded(loaded.indexed_segments)
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let snapshot = storage.observability_snapshot();
    assert!(
        snapshot.memory.persisted_index_bytes > 0,
        "runtime persisted refresh should update budgeted persisted-index bytes"
    );
    assert!(
        snapshot.memory.persisted_mmap_bytes > 0,
        "runtime persisted refresh should budget mmap-backed bytes"
    );
    assert_eq!(snapshot.memory.excluded_bytes, 0);
    assert_eq!(snapshot.memory.excluded_persisted_mmap_bytes, 0);

    let tightened_budget = storage.memory_used().saturating_sub(1).max(1);
    storage.memory.budget_bytes.store(
        tightened_budget as u64,
        std::sync::atomic::Ordering::Release,
    );
    storage.enforce_memory_budget_if_needed().unwrap();
    assert_engine_memory_usage_reconciled(&storage);
    assert!(
        storage.memory_used() > storage.memory_budget(),
        "persisted-index accounting should remain visible after runtime segment adoption"
    );

    storage.close().unwrap();
}

#[test]
fn catalog_tombstone_admission_failure_precedes_segment_publication() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();
        storage
            .insert_rows(&[Row::new(
                "catalog_tombstone_budget_metric",
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        storage.close().unwrap();
    }

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    crate::engine::tombstone::persist_tombstone_updates(
        &lane_path.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
        &crate::engine::tombstone::TombstoneMap::from([(
            1,
            vec![crate::engine::tombstone::TombstoneRange { start: 0, end: 2 }],
        )]),
    )
    .unwrap();
    let loaded = load_segment_indexes(&lane_path).unwrap();
    let inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path),
        None,
        loaded.next_segment_id,
        ChunkStorageOptions {
            memory_budget_bytes: 1_000_000,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage.memory.budget_bytes.store(1, Ordering::Release);
    let transition = PersistedCatalogTransition {
        visibility_fence: None,
        loaded_segments: loaded.indexed_segments,
        removed_roots: Vec::new(),
        publication: PersistedCatalogPublication::Inventory {
            inventory,
            refresh_tombstones: true,
        },
        registry_catalog_update: None,
    };

    let err = match storage
        .begin_persisted_catalog_publication()
        .publish_transition(transition)
    {
        Err(err) => err,
        Ok(_) => panic!("tiny memory budget must reject before catalog mutation"),
    };
    assert!(matches!(err, TsinkError::MemoryBudgetExceeded { .. }));
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .is_empty());
    assert!(storage.visibility.tombstones.read().is_empty());
    storage
        .memory
        .budget_bytes
        .store(1_000_000, Ordering::Release);
    storage.close().unwrap();
}

#[test]
fn delete_series_tombstone_accounting_reconciles_with_full_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = new_memory_budget_test_storage(&temp_dir, 1_000_000);

    storage
        .insert_rows(&[
            Row::with_labels(
                "delete_budget_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "delete_budget_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let deleted = storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("delete_budget_metric")
                .with_matcher(SeriesMatcher::equal("host", "a")),
        )
        .unwrap();
    assert_eq!(deleted.matched_series, 1);
    assert_eq!(deleted.tombstones_applied, 1);
    assert!(
        storage.observability_snapshot().memory.tombstone_bytes > 0,
        "delete should budget the persisted tombstone footprint",
    );
    assert_engine_memory_usage_reconciled(&storage);

    storage.close().unwrap();
}

#[test]
fn cardinality_limit_rejects_new_series_beyond_limit() {
    let storage = StorageBuilder::new()
        .with_cardinality_limit(1)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let labels_a = vec![Label::new("host", "a")];
    let labels_b = vec![Label::new("host", "b")];

    storage
        .insert_rows(&[Row::with_labels(
            "cardinality_guard_metric",
            labels_a.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "cardinality_guard_metric",
            labels_b.clone(),
            DataPoint::new(1, 2.0),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::CardinalityLimitExceeded { limit: 1, .. }
    ));

    storage
        .insert_rows(&[Row::with_labels(
            "cardinality_guard_metric",
            labels_a.clone(),
            DataPoint::new(2, 3.0),
        )])
        .unwrap();
    let points_a = storage
        .select("cardinality_guard_metric", &labels_a, 0, 10)
        .unwrap();
    assert_eq!(points_a.len(), 2);
    assert!(storage
        .select("cardinality_guard_metric", &labels_b, 0, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn cardinality_limit_rejection_does_not_grow_string_dictionaries() {
    let storage = ChunkStorage::new_with_data_path_and_options(
        4,
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
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: 1,
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
    let baseline_labels = vec![Label::new("host", "baseline")];

    storage
        .insert_rows(&[Row::with_labels(
            "cardinality_dict_guard_metric",
            baseline_labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let (baseline_metric_len, baseline_label_name_len, baseline_label_value_len) = {
        let registry = storage.catalog.registry.read();
        (
            registry.metric_dictionary_len(),
            registry.label_name_dictionary_len(),
            registry.label_value_dictionary_len(),
        )
    };

    for attempt in 0..16 {
        let err = storage
            .insert_rows(&[Row::with_labels(
                format!("cardinality_dict_leak_metric_{attempt}"),
                vec![Label::new(
                    format!("dict_name_{attempt}"),
                    format!("dict_value_{attempt}"),
                )],
                DataPoint::new(2, attempt as f64),
            )])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::CardinalityLimitExceeded { limit: 1, .. }
        ));
    }

    let (metric_len, label_name_len, label_value_len) = {
        let registry = storage.catalog.registry.read();
        (
            registry.metric_dictionary_len(),
            registry.label_name_dictionary_len(),
            registry.label_value_dictionary_len(),
        )
    };

    assert_eq!(metric_len, baseline_metric_len);
    assert_eq!(label_name_len, baseline_label_name_len);
    assert_eq!(label_value_len, baseline_label_value_len);

    storage.close().unwrap();
}

#[test]
fn series_label_count_limit_accepts_boundary_and_rejects_before_registry_growth() {
    let storage = new_cardinality_test_storage(2, usize::MAX, None, 10, 100);
    let boundary_labels = vec![Label::new("a", "1"), Label::new("b", "2")];

    storage
        .insert_rows(&[Row::with_labels(
            "shape_boundary",
            boundary_labels,
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    let before = storage.catalog.registry.read().series_count();

    let rejected_labels = vec![
        Label::new("a", "1"),
        Label::new("b", "2"),
        Label::new("c", "3"),
    ];
    let err = storage
        .insert_rows(&[Row::with_labels(
            "shape_rejected",
            rejected_labels,
            DataPoint::new(1, 2.0),
        )])
        .unwrap_err();
    assert!(
        matches!(err, TsinkError::InvalidLabel(message) if message.contains("configured limit 2"))
    );
    assert_eq!(storage.catalog.registry.read().series_count(), before);
    assert!(storage
        .list_metrics()
        .unwrap()
        .iter()
        .all(|series| series.name != "shape_rejected"));

    storage.close().unwrap();
}

#[test]
fn series_identity_byte_limit_accepts_exact_boundary_and_rejects_next_byte() {
    let storage = new_cardinality_test_storage(8, 4, None, 10, 100);

    // "m" + "a" + "bc" is exactly four UTF-8 bytes.
    storage
        .insert_rows(&[Row::with_labels(
            "m",
            vec![Label::new("a", "bc")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    // The second byte in the metric pushes this otherwise identical identity to five bytes.
    let err = storage
        .insert_rows(&[Row::with_labels(
            "mm",
            vec![Label::new("a", "bc")],
            DataPoint::new(1, 2.0),
        )])
        .unwrap_err();
    assert!(
        matches!(err, TsinkError::InvalidLabel(message) if message.contains("5 bytes") && message.contains("limit 4"))
    );
    assert_eq!(storage.catalog.registry.read().series_count(), 1);

    storage.close().unwrap();
}

#[test]
fn cardinality_shape_and_rate_builder_configuration_is_validated() {
    let too_many_labels = StorageBuilder::new()
        .with_max_labels_per_series(MAX_SUPPORTED_LABELS_PER_SERIES.saturating_add(1))
        .build();
    assert!(matches!(
        too_many_labels,
        Err(TsinkError::InvalidConfiguration(message))
            if message.contains("storage-format limit")
    ));

    let zero_window = StorageBuilder::new()
        .with_series_creation_rate_limit(1, Duration::ZERO)
        .build();
    assert!(matches!(
        zero_window,
        Err(TsinkError::InvalidConfiguration(message))
            if message.contains("greater than zero")
    ));
}

#[test]
fn series_creation_rate_limit_counts_only_newly_published_series() {
    let storage = new_cardinality_test_storage(8, 1024, Some(2), 10, 100);
    storage
        .insert_rows(&[
            Row::new("rate_a", DataPoint::new(1, 1.0)),
            Row::new("rate_b", DataPoint::new(1, 2.0)),
        ])
        .unwrap();

    let rejected = storage
        .write_batch(
            &[Row::new("rate_c", DataPoint::new(1, 3.0))],
            WriteMode::Atomic,
        )
        .unwrap();
    assert_eq!(rejected.accepted, 0);
    assert_eq!(rejected.rejected, 1);
    assert!(matches!(
        &rejected.outcomes[0].status,
        RowWriteStatus::Rejected(rejection)
            if rejection.category == WriteRejectionCategory::CardinalityCreationRateExceeded
    ));

    storage
        .insert_rows(&[Row::new("rate_a", DataPoint::new(2, 4.0))])
        .unwrap();
    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.cardinality.series_count, 2);
    assert_eq!(snapshot.cardinality.pending_new_series, 0);
    assert_eq!(snapshot.cardinality.committed_in_window, 2);
    assert_eq!(snapshot.cardinality.current_window_start, Some(100));
    assert_eq!(snapshot.cardinality.admitted_new_series_total, 2);
    assert_eq!(snapshot.cardinality.committed_new_series_total, 2);
    assert_eq!(snapshot.cardinality.creation_rate_rejections_total, 1);
    assert_eq!(snapshot.limits.max_labels_per_series, Some(8));
    assert_eq!(snapshot.limits.max_series_identity_bytes, Some(1024));
    assert_eq!(snapshot.limits.max_new_series_per_window, Some(2));
    assert_eq!(
        snapshot.limits.new_series_window_nanos,
        Some(10_000_000_000)
    );

    storage.set_current_time_override(110);
    storage
        .insert_rows(&[Row::new("rate_c", DataPoint::new(2, 5.0))])
        .unwrap();
    let advanced = storage.observability_snapshot().cardinality;
    assert_eq!(advanced.series_count, 3);
    assert_eq!(advanced.committed_in_window, 1);
    assert_eq!(advanced.current_window_start, Some(110));
    assert_eq!(advanced.admitted_new_series_total, 3);
    assert_eq!(advanced.committed_new_series_total, 3);

    storage.close().unwrap();
}

#[test]
fn failed_new_series_write_releases_creation_rate_reservation() {
    let storage = new_cardinality_test_storage(8, 1024, Some(1), 10, 100);

    let err = storage
        .insert_rows(&[
            Row::new("rate_rollback", DataPoint::new(1, 1.0)),
            Row::new("rate_rollback", DataPoint::new(2, 2_i64)),
        ])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    let after_failure = storage.observability_snapshot().cardinality;
    assert_eq!(after_failure.series_count, 0);
    assert_eq!(after_failure.pending_new_series, 0);
    assert_eq!(after_failure.committed_in_window, 0);
    assert_eq!(after_failure.admitted_new_series_total, 1);
    assert_eq!(after_failure.committed_new_series_total, 0);

    storage
        .insert_rows(&[Row::new("rate_after_rollback", DataPoint::new(3, 3.0))])
        .unwrap();
    let after_success = storage.observability_snapshot().cardinality;
    assert_eq!(after_success.series_count, 1);
    assert_eq!(after_success.pending_new_series, 0);
    assert_eq!(after_success.committed_in_window, 1);
    assert_eq!(after_success.admitted_new_series_total, 2);
    assert_eq!(after_success.committed_new_series_total, 1);

    storage.close().unwrap();
}

#[test]
fn concurrent_writers_cannot_collectively_bypass_series_creation_rate_limit() {
    use std::sync::Barrier;
    use std::thread;

    const WRITERS: usize = 32;
    const LIMIT: usize = 8;

    let storage = new_cardinality_test_storage(8, 1024, Some(LIMIT), 10, 100);
    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let mut writers = Vec::with_capacity(WRITERS);
    for writer_idx in 0..WRITERS {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        writers.push(thread::spawn(move || {
            barrier.wait();
            storage.insert_rows(&[Row::new(
                format!("concurrent_rate_{writer_idx}"),
                DataPoint::new(1, writer_idx as f64),
            )])
        }));
    }
    barrier.wait();

    let mut accepted = 0;
    let mut rejected = 0;
    for writer in writers {
        match writer.join().unwrap() {
            Ok(()) => accepted += 1,
            Err(TsinkError::CardinalityCreationRateExceeded { limit: LIMIT, .. }) => rejected += 1,
            Err(err) => panic!("unexpected concurrent write error: {err:?}"),
        }
    }

    assert_eq!(accepted, LIMIT);
    assert_eq!(rejected, WRITERS - LIMIT);
    let snapshot = storage.observability_snapshot().cardinality;
    assert_eq!(snapshot.series_count, LIMIT as u64);
    assert_eq!(snapshot.pending_new_series, 0);
    assert_eq!(snapshot.committed_in_window, LIMIT as u64);
    assert_eq!(
        snapshot.creation_rate_rejections_total,
        (WRITERS - LIMIT) as u64
    );

    storage.close().unwrap();
}
