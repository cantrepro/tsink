use super::*;
use crate::engine::series::SeriesKey;

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
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes,
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
    storage.enforce_memory_budget_if_needed().unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
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
    assert_eq!(storage.memory_budget(), tightened_budget);
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
    assert!(
        sealed_count < 3,
        "oldest sealed chunks should be evicted after spill"
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

    storage.close().unwrap();
}

#[test]
fn memory_budget_stats_reflect_builder_configuration() {
    let storage = StorageBuilder::new()
        .with_memory_limit(1234)
        .build()
        .unwrap();
    let snapshot = storage.observability_snapshot();

    assert_eq!(storage.memory_budget(), 1234);
    assert_eq!(storage.memory_used(), snapshot.memory.budgeted_bytes);
    assert_eq!(snapshot.memory.excluded_bytes, 0);
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
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: 2_048,
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

    let tightened_budget = 1;
    reopened.close().unwrap();

    let over_budget = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(tightened_budget)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    assert!(
        over_budget.memory_used() > over_budget.memory_budget(),
        "restart reconciliation should expose the full persisted footprint even under a tiny budget"
    );
    over_budget.close().unwrap();
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
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
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

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(tightened_budget)
        .with_write_timeout(Duration::ZERO)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();

    assert!(
        reopened.memory_used() > reopened.memory_budget(),
        "persisted mmap bytes should keep the reopened storage over its tightened budget"
    );
    assert_eq!(
        reopened
            .select("restart_query_budget_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );

    let err = reopened
        .insert_rows(&[Row::with_labels(
            "restart_query_budget_metric",
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap_err();
    assert!(
        matches!(
            err,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == tightened_budget && required > budget
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
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: 1,
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
