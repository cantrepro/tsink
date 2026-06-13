use super::*;

#[test]
fn wal_size_limit_rejects_writes_that_cannot_fit_new_frames() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_write_timeout(Duration::ZERO)
        .with_wal_size_limit(1)
        .build()
        .unwrap();

    let err = storage
        .insert_rows(&[Row::new("wal_guard_metric", DataPoint::new(1, 1.0))])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WalSizeLimitExceeded { limit: 1, .. }
    ));
    assert!(
        storage
            .select("wal_guard_metric", &[], 0, 10)
            .unwrap()
            .is_empty(),
        "WAL admission failure must not ingest points"
    );
}

#[test]
fn chunk_storage_default_retention_enforcement_matches_builder_default() {
    let chunk_defaults = ChunkStorageOptions::default();
    let builder_default = StorageBuilder::new();
    assert_eq!(
        chunk_defaults.retention_enforced,
        builder_default.retention_enforced()
    );
}

#[test]
fn retention_window_hides_points_older_than_latest_minus_window() {
    let storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(1))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new("retention_metric", DataPoint::new(100, 1.0))])
        .unwrap();
    storage
        .insert_rows(&[Row::new("retention_metric", DataPoint::new(102, 2.0))])
        .unwrap();

    let points = storage.select("retention_metric", &[], 0, 200).unwrap();
    assert_eq!(points, vec![DataPoint::new(102, 2.0)]);
}

#[test]
fn default_retention_rejects_out_of_window_writes() {
    let retention_secs = Duration::from_secs(14 * 24 * 3600).as_secs() as i64;
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new(
            "default_retention_metric",
            DataPoint::new(retention_secs + 1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::new("default_retention_metric", DataPoint::new(0, 0.0))])
        .unwrap_err();
    assert!(matches!(err, TsinkError::OutOfRetention { timestamp: 0 }));
}

#[test]
fn explicitly_setting_default_retention_rejects_out_of_window_writes() {
    let retention = Duration::from_secs(14 * 24 * 3600);
    let storage = StorageBuilder::new()
        .with_retention(retention)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new(
            "explicit_default_retention_metric",
            DataPoint::new(retention.as_secs() as i64 + 1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::new(
            "explicit_default_retention_metric",
            DataPoint::new(0, 0.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::OutOfRetention { timestamp: 0 }));
}

#[test]
fn disabling_retention_enforcement_never_expires_or_rejects_points() {
    let storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(1))
        .with_retention_enforced(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(100, 1.0))])
        .unwrap();
    storage
        .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(102, 2.0))])
        .unwrap();
    storage
        .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(0, 0.0))])
        .unwrap();

    let points = storage.select("no_retention_metric", &[], 0, 200).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(0, 0.0),
            DataPoint::new(100, 1.0),
            DataPoint::new(102, 2.0)
        ]
    );
}

#[test]
fn retention_sweeper_deletes_expired_persisted_segments_across_levels() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let mut registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("retention_swept_metric", &labels)
        .unwrap()
        .series_id;

    let mut expired_chunks = HashMap::new();
    expired_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &expired_chunks)
        .unwrap();
    SegmentWriter::new(&lane_path, 1, 2)
        .unwrap()
        .write_segment(&registry, &expired_chunks)
        .unwrap();

    let mut retained_chunks = HashMap::new();
    retained_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(100, 100.0), (101, 101.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 2, 3)
        .unwrap()
        .write_segment(&registry, &retained_chunks)
        .unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_retention(Duration::from_secs(10))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());
    assert!(load_segments_for_level(&lane_path, 1).unwrap().is_empty());
    let l2 = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(l2.len(), 1);
    assert_eq!(l2[0].manifest.segment_id, 3);

    let points = storage
        .select("retention_swept_metric", &labels, 0, 200)
        .unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
    );

    storage.close().unwrap();
}

#[test]
fn retention_sweep_reload_failure_keeps_existing_persisted_data_visible() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let mut registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("retention_reload_visibility_metric", &labels)
        .unwrap()
        .series_id;

    let mut expired_chunks = HashMap::new();
    expired_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &expired_chunks)
        .unwrap();

    let mut retained_chunks = HashMap::new();
    retained_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(100, 100.0), (101, 101.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 2, 2)
        .unwrap()
        .write_segment(&registry, &retained_chunks)
        .unwrap();

    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(lane_path.clone()),
        None,
        3,
        ChunkStorageOptions {
            retention_window: 10,
            retention_enforced: true,
            partition_window: i64::MAX,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let before = storage
        .select("retention_reload_visibility_metric", &labels, 0, 200)
        .unwrap();
    assert_eq!(
        before,
        vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
    );

    // Introduce an on-disk conflict that makes future index reloads fail.
    let mut conflicting_registry = SeriesRegistry::new();
    conflicting_registry
        .register_series_with_id(series_id, "retention_reload_conflict_metric", &labels)
        .unwrap();
    let mut conflicting_chunks = HashMap::new();
    conflicting_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, &[(150, 150.0)])],
    );
    SegmentWriter::new(&lane_path, 1, 10)
        .unwrap()
        .write_segment(&conflicting_registry, &conflicting_chunks)
        .unwrap();

    let err = storage.sweep_expired_persisted_segments().unwrap_err();
    assert!(matches!(err, TsinkError::DataCorruption(_)));

    let after = storage
        .select("retention_reload_visibility_metric", &labels, 0, 200)
        .unwrap();
    assert_eq!(
        after,
        vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
    );
}

#[test]
fn retention_sweeper_is_disabled_when_retention_enforcement_is_off() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let mut registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("retention_unswept_metric", &labels)
        .unwrap()
        .series_id;

    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &chunks)
        .unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_retention(Duration::from_secs(1))
        .with_retention_enforced(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let l0 = load_segments_for_level(&lane_path, 0).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l0[0].manifest.segment_id, 1);

    let points = storage
        .select("retention_unswept_metric", &labels, 0, 10)
        .unwrap();
    assert_eq!(points, vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]);

    storage.close().unwrap();
}
