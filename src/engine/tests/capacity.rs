use super::*;

#[test]
fn memory_budget_spills_to_l0_and_preserves_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            retention_window: i64::MAX,
            retention_enforced: false,
            partition_window: i64::MAX,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: 512,
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
    assert_eq!(storage.memory_budget(), 512);
    assert!(
        storage.memory_used() <= storage.memory_budget(),
        "memory usage should fall under budget after spill"
    );

    let series_id = storage
        .registry
        .read()
        .resolve_existing("budget_metric", &labels)
        .unwrap()
        .series_id;
    let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
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
fn memory_budget_stats_reflect_builder_configuration() {
    let storage = StorageBuilder::new()
        .with_memory_limit(1234)
        .build()
        .unwrap();

    assert_eq!(storage.memory_budget(), 1234);
    assert_eq!(storage.memory_used(), 0);
}

#[test]
fn memory_budget_guard_rejects_writes_when_in_memory_budget_cannot_be_relaxed() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_memory_limit(1)
        .with_write_timeout(Duration::ZERO)
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
fn cardinality_limit_rejects_new_series_beyond_limit() {
    let storage = StorageBuilder::new()
        .with_cardinality_limit(1)
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
            cardinality_limit: 1,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
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
        let registry = storage.registry.read();
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
        let registry = storage.registry.read();
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
