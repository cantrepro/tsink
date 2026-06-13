use super::super::tiering::SEGMENT_CATALOG_FILE_NAME;
use super::*;

fn current_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

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

fn chunk_storage_at_time(now: i64, retention_window: i64) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
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
            current_time_override: Some(now),
        },
    )
    .unwrap()
}

#[test]
fn wal_size_limit_rejects_writes_that_cannot_fit_new_frames() {
    let temp_dir = TempDir::new().unwrap();
    let storage = builder_at_time(1)
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
    let storage = chunk_storage_at_time(100, 1);

    storage
        .insert_rows(&[Row::new("retention_metric", DataPoint::new(100, 1.0))])
        .unwrap();
    storage.set_current_time_override(102);
    storage
        .insert_rows(&[Row::new("retention_metric", DataPoint::new(102, 2.0))])
        .unwrap();

    let points = storage.select("retention_metric", &[], 0, 200).unwrap();
    assert_eq!(points, vec![DataPoint::new(102, 2.0)]);
}

#[test]
fn default_builder_accepts_historical_backfill_points() {
    let retention_secs = Duration::from_secs(14 * 24 * 3600).as_secs() as i64;
    let storage = builder_at_time(retention_secs + 1)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new(
            "default_retention_metric",
            DataPoint::new(retention_secs + 1, 1.0),
        )])
        .unwrap();

    storage
        .insert_rows(&[Row::new("default_retention_metric", DataPoint::new(0, 0.0))])
        .unwrap();

    let points = storage
        .select("default_retention_metric", &[], 0, retention_secs + 2)
        .unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(0, 0.0),
            DataPoint::new(retention_secs + 1, 1.0),
        ]
    );
}

#[test]
fn enabling_default_retention_rejects_out_of_window_writes() {
    let retention_secs = Duration::from_secs(14 * 24 * 3600).as_secs() as i64;
    let storage = builder_at_time(retention_secs + 1)
        .with_retention_enforced(true)
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
    let storage = builder_at_time(retention.as_secs() as i64 + 1)
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
    let registry = SeriesRegistry::new();
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

    let storage = builder_at_time(110)
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
    let registry = SeriesRegistry::new();
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
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: 10,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
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
            current_time_override: Some(110),
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
    let conflicting_registry = SeriesRegistry::new();
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
    let registry = SeriesRegistry::new();
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

#[test]
fn retention_sweeper_rewrites_mixed_age_segments_and_persists_pruned_state_across_restart() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("retention_rewrite_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0), (100, 100.0), (101, 101.0)],
    );

    let storage = builder_at_time(110)
        .with_data_path(temp_dir.path())
        .with_retention(Duration::from_secs(10))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("retention_rewrite_metric", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
    );

    let persisted = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].manifest.min_ts, Some(100));
    assert_eq!(persisted[0].manifest.max_ts, Some(101));
    assert_eq!(
        persisted_timestamps_for_series(&persisted, series_id),
        vec![100, 101]
    );

    storage.close().unwrap();

    let reopened = builder_at_time(110)
        .with_data_path(temp_dir.path())
        .with_retention(Duration::from_secs(10))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("retention_rewrite_metric", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
    );

    let persisted = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].manifest.min_ts, Some(100));
    assert_eq!(persisted[0].manifest.max_ts, Some(101));
    assert_eq!(
        persisted_timestamps_for_series(&persisted, series_id),
        vec![100, 101]
    );

    reopened.close().unwrap();
}

#[test]
fn retention_pruned_restart_does_not_reuse_tombstoned_series_ids() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let deleted_labels = vec![Label::new("host", "deleted")];
    let replacement_labels = vec![Label::new("host", "replacement")];

    {
        let storage = builder_at_time(10)
            .with_data_path(data_path)
            .with_retention(Duration::from_secs(5))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "retention_deleted_metric",
                deleted_labels.clone(),
                DataPoint::new(10, 1.0),
            )])
            .unwrap();
        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("retention_deleted_metric")
                    .with_matcher(SeriesMatcher::equal("host", "deleted")),
            )
            .unwrap();
        storage.close().unwrap();
    }

    {
        let storage = builder_at_time(30)
            .with_data_path(data_path)
            .with_retention(Duration::from_secs(5))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        assert!(data_path
            .join(NUMERIC_LANE_ROOT)
            .join(crate::engine::tombstone::TOMBSTONES_FILE_NAME)
            .exists());
        storage.close().unwrap();
    }

    let reopened = builder_at_time(30)
        .with_data_path(data_path)
        .with_retention(Duration::from_secs(5))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    reopened
        .insert_rows(&[Row::with_labels(
            "retention_replacement_metric",
            replacement_labels.clone(),
            DataPoint::new(30, 2.0),
        )])
        .unwrap();

    assert_eq!(
        reopened
            .select("retention_replacement_metric", &replacement_labels, 0, 40)
            .unwrap(),
        vec![DataPoint::new(30, 2.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn retention_sweeper_uses_repaired_recency_after_deleting_anchor() {
    let temp_dir = TempDir::new().unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 15,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
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
    let now = current_unix_seconds();
    let oldest = now - 30;
    let fallback = now - 20;
    let anchor = now;
    let oldest_labels = vec![Label::new("host", "oldest")];
    let fallback_labels = vec![Label::new("host", "fallback")];
    let anchor_labels = vec![Label::new("host", "anchor")];

    for (labels, timestamp, value) in [
        (oldest_labels.clone(), oldest, 1.0),
        (fallback_labels.clone(), fallback, 2.0),
        (anchor_labels.clone(), anchor, 3.0),
    ] {
        storage.set_current_time_override(timestamp);
        storage
            .insert_rows(&[Row::with_labels(
                "retention_repair_metric",
                labels,
                DataPoint::new(timestamp, value),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }
    storage.set_current_time_override(anchor);

    assert!(storage
        .select(
            "retention_repair_metric",
            &oldest_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert!(storage
        .select(
            "retention_repair_metric",
            &fallback_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        storage
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(anchor)
    );

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("retention_repair_metric")
                .with_matcher(SeriesMatcher::equal("host", "anchor")),
        )
        .unwrap();

    assert_eq!(
        storage
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(anchor)
    );
    assert!(storage
        .select(
            "retention_repair_metric",
            &oldest_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert!(storage
        .select(
            "retention_repair_metric",
            &fallback_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert!(storage
        .select(
            "retention_repair_metric",
            &anchor_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());

    assert_eq!(storage.sweep_expired_persisted_segments().unwrap(), 2);
    storage.close().unwrap();

    let reopened = builder_at_time(anchor)
        .with_data_path(temp_dir.path())
        .with_retention(Duration::from_secs(15))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(anchor)
    );
    assert!(reopened
        .select(
            "retention_repair_metric",
            &oldest_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert!(reopened
        .select(
            "retention_repair_metric",
            &fallback_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    assert!(reopened
        .select(
            "retention_repair_metric",
            &anchor_labels,
            oldest - 1,
            anchor + 1
        )
        .unwrap()
        .is_empty());
    reopened.close().unwrap();
}

#[test]
fn tiered_retention_uses_repaired_recency_after_deleting_anchor() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 30,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
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
            tiered_storage: Some(super::super::config::TieredStorageConfig {
                object_store_root: object_store_dir.path().to_path_buf(),
                segment_catalog_path: Some(data_dir.path().join(SEGMENT_CATALOG_FILE_NAME)),
                mirror_hot_segments: false,
                hot_retention_window: 10,
                warm_retention_window: 20,
            }),
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    let now = current_unix_seconds();
    storage.set_current_time_override(now);
    let warm_candidate = now - 12;
    let fallback = now - 5;
    let anchor = now;
    let warm_labels = vec![Label::new("host", "warm-candidate")];
    let fallback_labels = vec![Label::new("host", "fallback")];
    let anchor_labels = vec![Label::new("host", "anchor")];

    for (labels, timestamp, value) in [
        (warm_labels.clone(), warm_candidate, 1.0),
        (fallback_labels.clone(), fallback, 2.0),
        (anchor_labels.clone(), anchor, 3.0),
    ] {
        storage
            .insert_rows(&[Row::with_labels(
                "tier_repair_metric",
                labels,
                DataPoint::new(timestamp, value),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("tier_repair_metric")
                .with_matcher(SeriesMatcher::equal("host", "anchor")),
        )
        .unwrap();

    assert_eq!(
        storage
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(anchor)
    );
    assert_eq!(storage.sweep_expired_persisted_segments().unwrap(), 0);

    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let hot_l0 = load_segments_for_level(&hot_lane, 0).unwrap();
    assert_eq!(hot_l0.len(), 2);
    let warm_lane = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    let warm_l0 = load_segments_for_level(&warm_lane, 0).unwrap();
    assert_eq!(warm_l0.len(), 1);
    assert_eq!(warm_l0[0].manifest.segment_id, 1);
    let cold_lane = object_store_dir.path().join("cold").join(NUMERIC_LANE_ROOT);
    assert!(load_segments_for_level(&cold_lane, 0).unwrap().is_empty());

    storage.close().unwrap();
}

#[test]
fn retention_sweeper_prunes_expired_points_before_tiering_to_cold_storage() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("retention_tier_prune_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0), (60, 60.0), (61, 61.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        2,
        &[(120, 120.0), (121, 121.0)],
    );

    let storage = builder_at_time(131)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("retention_tier_prune_metric", &labels, 0, 200)
            .unwrap(),
        vec![
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(120, 120.0),
            DataPoint::new(121, 121.0),
        ]
    );
    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.flush.tier_moves_total, 1);
    assert_eq!(snapshot.flush.expired_segments_total, 0);

    let local_l2 = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(local_l2.len(), 1);
    assert_eq!(
        persisted_timestamps_for_series(&local_l2, series_id),
        vec![120, 121]
    );

    let cold_lane = object_store_dir.path().join("cold").join(NUMERIC_LANE_ROOT);
    let cold_l2 = load_segments_for_level(&cold_lane, 2).unwrap();
    assert_eq!(cold_l2.len(), 1);
    assert_eq!(cold_l2[0].manifest.min_ts, Some(60));
    assert_eq!(cold_l2[0].manifest.max_ts, Some(61));
    assert_eq!(
        persisted_timestamps_for_series(&cold_l2, series_id),
        vec![60, 61]
    );

    storage.close().unwrap();

    let reopened = builder_at_time(131)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("retention_tier_prune_metric", &labels, 0, 200)
            .unwrap(),
        vec![
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(120, 120.0),
            DataPoint::new(121, 121.0),
        ]
    );
    let cold_l2 = load_segments_for_level(&cold_lane, 2).unwrap();
    assert_eq!(
        persisted_timestamps_for_series(&cold_l2, series_id),
        vec![60, 61]
    );

    reopened.close().unwrap();
}

#[test]
fn future_skew_rebuilds_bounded_recency_across_restart() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "future_skew_restart_metric";
    let now = current_unix_seconds();
    let recent = now - 60;
    let current = now;
    let future = now + 30 * 24 * 3600;

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_retention(Duration::from_secs(12 * 3600))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::new(metric, DataPoint::new(future, 3.0)),
                Row::new(metric, DataPoint::new(recent, 1.0)),
                Row::new(metric, DataPoint::new(current, 2.0)),
            ])
            .unwrap();

        let snapshot = storage.observability_snapshot();
        assert_eq!(snapshot.retention.max_observed_timestamp, Some(future));
        assert_eq!(snapshot.retention.recency_reference_timestamp, Some(now));
        assert_eq!(snapshot.retention.future_skew_points_total, 1);
        assert_eq!(snapshot.retention.future_skew_max_timestamp, Some(future));

        storage.close().unwrap();
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_retention(Duration::from_secs(12 * 3600))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select(metric, &[], recent - 1, future + 1)
            .unwrap(),
        vec![
            DataPoint::new(recent, 1.0),
            DataPoint::new(current, 2.0),
            DataPoint::new(future, 3.0),
        ]
    );
    let snapshot = reopened.observability_snapshot();
    assert_eq!(snapshot.retention.max_observed_timestamp, Some(future));
    assert_eq!(snapshot.retention.recency_reference_timestamp, Some(now));

    reopened.close().unwrap();
}

#[test]
fn future_only_writes_still_enforce_retention_against_wall_clock() {
    let now = current_unix_seconds();
    let retention = Duration::from_secs(5 * 60);
    let future = future_timestamp(now);
    let too_old = now - retention.as_secs() as i64 - 1;

    let storage = builder_at_time(now)
        .with_retention(retention)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[Row::new(
            "future_only_retention_metric",
            DataPoint::new(future, 1.0),
        )])
        .unwrap();

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.retention.max_observed_timestamp, Some(future));
    assert_eq!(snapshot.retention.recency_reference_timestamp, Some(now));

    let err = storage
        .insert_rows(&[Row::new(
            "future_only_retention_metric",
            DataPoint::new(too_old, 0.0),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::OutOfRetention { timestamp } if timestamp == too_old
    ));

    assert_eq!(
        storage
            .select("future_only_retention_metric", &[], too_old, future + 1)
            .unwrap(),
        vec![DataPoint::new(future, 1.0)]
    );

    storage.close().unwrap();
}

#[test]
fn future_skew_does_not_expire_or_cold_move_retained_segments() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("future_skew_tiered_metric", &labels)
        .unwrap()
        .series_id;
    let now = current_unix_seconds();

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(now - 2 * 3600, 1.0), (now - 2 * 3600 + 1, 2.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        2,
        &[(now - 300, 3.0), (now - 299, 4.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        3,
        &[
            (future_timestamp(now), 5.0),
            (future_timestamp(now) + 1, 6.0),
        ],
    );

    let storage = builder_at_time(now)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(3600), Duration::from_secs(12 * 3600))
        .with_retention(Duration::from_secs(7 * 24 * 3600))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.flush.tier_moves_total, 1);
    assert_eq!(snapshot.flush.expired_segments_total, 0);
    assert_eq!(snapshot.flush.hot_segments_visible, 2);
    assert_eq!(snapshot.flush.warm_segments_visible, 1);
    assert_eq!(snapshot.flush.cold_segments_visible, 0);
    assert_eq!(snapshot.retention.recency_reference_timestamp, Some(now),);

    let local_l2 = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(local_l2.len(), 2);
    assert!(local_l2
        .iter()
        .any(|segment| segment.manifest.segment_id == 2));
    assert!(local_l2
        .iter()
        .any(|segment| segment.manifest.segment_id == 3));

    let warm_l2 = load_segments_for_level(
        object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT),
        2,
    )
    .unwrap();
    assert_eq!(warm_l2.len(), 1);
    assert_eq!(warm_l2[0].manifest.segment_id, 1);
    assert!(load_segments_for_level(
        object_store_dir.path().join("cold").join(NUMERIC_LANE_ROOT),
        2
    )
    .unwrap()
    .is_empty());

    assert_eq!(
        storage
            .select(
                "future_skew_tiered_metric",
                &labels,
                now - 10_000,
                future_timestamp(now) + 2
            )
            .unwrap(),
        vec![
            DataPoint::new(now - 2 * 3600, 1.0),
            DataPoint::new(now - 2 * 3600 + 1, 2.0),
            DataPoint::new(now - 300, 3.0),
            DataPoint::new(now - 299, 4.0),
            DataPoint::new(future_timestamp(now), 5.0),
            DataPoint::new(future_timestamp(now) + 1, 6.0),
        ]
    );

    storage.close().unwrap();
}

fn future_timestamp(now: i64) -> i64 {
    now + 30 * 24 * 3600
}

#[test]
fn tiered_retention_moves_segments_to_warm_and_cold_storage_and_survives_restart() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("tiered_retention_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        2,
        &[(60, 60.0), (61, 61.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        3,
        &[(95, 95.0), (96, 96.0)],
    );

    let storage = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.flush.tier_moves_total, 2);
    assert_eq!(snapshot.flush.hot_segments_visible, 1);
    assert_eq!(snapshot.flush.warm_segments_visible, 1);
    assert_eq!(snapshot.flush.cold_segments_visible, 1);
    assert!(data_dir.path().join(SEGMENT_CATALOG_FILE_NAME).exists());

    let local_l2 = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(local_l2.len(), 1);
    assert_eq!(local_l2[0].manifest.segment_id, 3);

    let warm_l2 = load_segments_for_level(
        object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT),
        2,
    )
    .unwrap();
    assert_eq!(warm_l2.len(), 1);
    assert_eq!(warm_l2[0].manifest.segment_id, 2);

    let cold_l2 = load_segments_for_level(
        object_store_dir.path().join("cold").join(NUMERIC_LANE_ROOT),
        2,
    )
    .unwrap();
    assert_eq!(cold_l2.len(), 1);
    assert_eq!(cold_l2[0].manifest.segment_id, 1);

    let points = storage
        .select("tiered_retention_metric", &labels, 0, 200)
        .unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(95, 95.0),
            DataPoint::new(96, 96.0),
        ]
    );

    storage.close().unwrap();

    let reopened = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened
            .select("tiered_retention_metric", &labels, 0, 200)
            .unwrap(),
        points
    );
    reopened.close().unwrap();
}

#[test]
fn tiered_retention_rejects_preexisting_corrupted_destination_and_keeps_source_segment() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_lane = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let mismatched_labels = vec![Label::new("host", "b")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("tiered_retention_corrupt_destination_metric", &labels)
        .unwrap()
        .series_id;
    let mismatched_registry = SeriesRegistry::new();
    let mismatched_series_id = mismatched_registry
        .resolve_or_insert(
            "tiered_retention_corrupt_destination_metric",
            &mismatched_labels,
        )
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(60, 60.0), (61, 61.0)],
    );
    write_numeric_segment(
        &warm_lane,
        &mismatched_registry,
        mismatched_series_id,
        2,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let source_root = lane_path
        .join("segments")
        .join("L2")
        .join("seg-0000000000000001");
    let warm_root = warm_lane
        .join("segments")
        .join("L2")
        .join("seg-0000000000000001");
    let err = super::super::tiering::move_segment_to_tier(&source_root, &warm_root).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("tier move destination"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&warm_root.display().to_string()),
        "unexpected error: {message}"
    );

    let local_l2 = load_segments_for_level(&lane_path, 2).unwrap();
    assert_eq!(local_l2.len(), 1);
    assert_eq!(local_l2[0].manifest.segment_id, 1);
    assert!(source_root.exists(), "source segment should remain visible");
    assert!(
        warm_root.exists(),
        "mismatched destination should remain visible"
    );
}

#[test]
fn idle_tiered_retention_advances_across_restarts_without_new_ingest() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "idle")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("idle_tiering_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(95, 95.0), (96, 96.0)],
    );

    let storage = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert_eq!(
        storage
            .select("idle_tiering_metric", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(95, 95.0), DataPoint::new(96, 96.0)]
    );
    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.flush.hot_segments_visible, 1);
    assert_eq!(snapshot.flush.warm_segments_visible, 0);
    assert_eq!(snapshot.flush.cold_segments_visible, 0);
    storage.close().unwrap();

    let reopened_warm = builder_at_time(110)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened_warm
            .select("idle_tiering_metric", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(95, 95.0), DataPoint::new(96, 96.0)]
    );
    let snapshot = reopened_warm.observability_snapshot();
    assert_eq!(snapshot.flush.hot_segments_visible, 0);
    assert_eq!(snapshot.flush.warm_segments_visible, 1);
    assert_eq!(snapshot.flush.cold_segments_visible, 0);
    reopened_warm.close().unwrap();

    let reopened_cold = builder_at_time(160)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened_cold
            .select("idle_tiering_metric", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(95, 95.0), DataPoint::new(96, 96.0)]
    );
    let snapshot = reopened_cold.observability_snapshot();
    assert_eq!(snapshot.flush.hot_segments_visible, 0);
    assert_eq!(snapshot.flush.warm_segments_visible, 0);
    assert_eq!(snapshot.flush.cold_segments_visible, 1);
    reopened_cold.close().unwrap();

    let reopened_expired = builder_at_time(200)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert!(reopened_expired
        .select("idle_tiering_metric", &labels, 0, 200)
        .unwrap()
        .is_empty());
    let snapshot = reopened_expired.observability_snapshot();
    assert_eq!(snapshot.flush.hot_segments_visible, 0);
    assert_eq!(snapshot.flush.warm_segments_visible, 0);
    assert_eq!(snapshot.flush.cold_segments_visible, 0);
    reopened_expired.close().unwrap();
}

#[test]
fn tiered_storage_initializes_when_data_path_is_brand_new() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("fresh-data");
    let object_store_path = temp_dir.path().join("object-store");

    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_object_store_path(&object_store_path)
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert!(data_path.exists());
    assert!(data_path.join(SEGMENT_CATALOG_FILE_NAME).exists());

    storage.close().unwrap();
}

#[test]
fn tiered_query_planner_routes_recent_queries_without_touching_cold_tiers() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("tiered_query_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        2,
        &[(60, 60.0), (61, 61.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        3,
        &[(95, 95.0), (96, 96.0)],
    );

    let storage = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("tiered_query_metric", &labels, 90, 100)
            .unwrap(),
        vec![DataPoint::new(95, 95.0), DataPoint::new(96, 96.0)]
    );
    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.hot_only_query_plans_total, 1);
    assert_eq!(snapshot.query.warm_tier_query_plans_total, 0);
    assert_eq!(snapshot.query.cold_tier_query_plans_total, 0);
    assert_eq!(snapshot.query.hot_tier_persisted_chunks_read_total, 1);
    assert_eq!(snapshot.query.warm_tier_persisted_chunks_read_total, 0);
    assert_eq!(snapshot.query.cold_tier_persisted_chunks_read_total, 0);

    assert_eq!(
        storage
            .select("tiered_query_metric", &labels, 50, 100)
            .unwrap(),
        vec![
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(95, 95.0),
            DataPoint::new(96, 96.0),
        ]
    );
    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.hot_only_query_plans_total, 1);
    assert_eq!(snapshot.query.warm_tier_query_plans_total, 1);
    assert_eq!(snapshot.query.cold_tier_query_plans_total, 0);
    assert_eq!(snapshot.query.hot_tier_persisted_chunks_read_total, 2);
    assert_eq!(snapshot.query.warm_tier_persisted_chunks_read_total, 1);
    assert_eq!(snapshot.query.cold_tier_persisted_chunks_read_total, 0);
    assert!(snapshot.query.warm_tier_fetch_duration_nanos_total > 0);

    assert_eq!(
        storage
            .select("tiered_query_metric", &labels, 0, 100)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(95, 95.0),
            DataPoint::new(96, 96.0),
        ]
    );
    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.hot_only_query_plans_total, 1);
    assert_eq!(snapshot.query.warm_tier_query_plans_total, 2);
    assert_eq!(snapshot.query.cold_tier_query_plans_total, 1);
    assert_eq!(snapshot.query.hot_tier_persisted_chunks_read_total, 3);
    assert_eq!(snapshot.query.warm_tier_persisted_chunks_read_total, 2);
    assert_eq!(snapshot.query.cold_tier_persisted_chunks_read_total, 1);
    assert!(snapshot.query.cold_tier_fetch_duration_nanos_total > 0);

    storage.close().unwrap();
}

#[test]
fn snapshot_restore_preserves_tier_catalog_for_offloaded_segments() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("tiered_snapshot_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        &lane_path,
        &registry,
        series_id,
        2,
        2,
        &[(95, 95.0), (96, 96.0)],
    );

    let storage = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    let snapshot_dir = TempDir::new().unwrap();
    let snapshot_path = snapshot_dir.path().join("snapshot");
    storage.snapshot(&snapshot_path).unwrap();
    assert!(snapshot_path.join(SEGMENT_CATALOG_FILE_NAME).exists());
    storage.close().unwrap();

    let restore_dir = TempDir::new().unwrap();
    let restored_data_path = restore_dir.path().join("restored-data");
    StorageBuilder::restore_from_snapshot(&snapshot_path, &restored_data_path).unwrap();

    let restored = builder_at_time(100)
        .with_data_path(&restored_data_path)
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        restored
            .select("tiered_snapshot_metric", &labels, 0, 200)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(95, 95.0),
            DataPoint::new(96, 96.0),
        ]
    );
    restored.close().unwrap();
}

#[test]
fn compute_only_storage_reads_remote_hot_warm_and_cold_segments_and_rejects_writes() {
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("compute_only_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment(
        &object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT),
        &registry,
        series_id,
        2,
        3,
        &[(95, 95.0), (96, 96.0)],
    );
    write_numeric_segment(
        &object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT),
        &registry,
        series_id,
        2,
        2,
        &[(60, 60.0), (61, 61.0)],
    );
    write_numeric_segment(
        &object_store_dir.path().join("cold").join(NUMERIC_LANE_ROOT),
        &registry,
        series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0)],
    );

    let storage = builder_at_time(100)
        .with_object_store_path(object_store_dir.path())
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("compute_only_metric", &labels, 0, 200)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(95, 95.0),
            DataPoint::new(96, 96.0),
        ]
    );

    let snapshot = storage.observability_snapshot();
    assert!(snapshot.remote.enabled);
    assert_eq!(
        snapshot.remote.runtime_mode,
        StorageRuntimeMode::ComputeOnly
    );
    assert_eq!(
        snapshot.remote.cache_policy,
        RemoteSegmentCachePolicy::MetadataOnly
    );
    assert!(snapshot.remote.accessible);
    assert!(snapshot.remote.catalog_refreshes_total >= 1);
    assert!(snapshot.remote.last_successful_refresh_unix_ms.is_some());

    let err = storage
        .insert_rows(&[Row::with_labels(
            "compute_only_metric",
            labels.clone(),
            DataPoint::new(100, 100.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));

    storage.close().unwrap();
}

#[test]
fn compute_only_storage_refreshes_remote_tombstones_without_segment_inventory_changes() {
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let expected_series = MetricSeries {
        name: "compute_only_tombstone_refresh_metric".to_string(),
        labels: labels.clone(),
    };
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("compute_only_tombstone_refresh_metric", &labels)
        .unwrap()
        .series_id;

    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    write_numeric_segment(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);

    let storage = builder_at_time(10)
        .with_object_store_path(object_store_dir.path())
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
        .with_remote_segment_refresh_interval(Duration::from_millis(1))
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("compute_only_tombstone_refresh_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_eq!(
        storage.list_metrics().unwrap(),
        vec![expected_series.clone()]
    );

    let mut tombstones = std::collections::HashMap::new();
    tombstones.insert(
        series_id,
        vec![crate::engine::tombstone::TombstoneRange { start: 0, end: 10 }],
    );
    crate::engine::tombstone::persist_tombstones(
        &hot_lane.join(crate::engine::tombstone::TOMBSTONES_FILE_NAME),
        &tombstones,
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(5));

    assert!(storage
        .select("compute_only_tombstone_refresh_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());
    assert!(storage.list_metrics().unwrap().is_empty());
    assert!(
        storage
            .select_series(
                &SeriesSelection::new().with_metric("compute_only_tombstone_refresh_metric"),
            )
            .unwrap()
            .is_empty()
    );

    storage.close().unwrap();
}

#[test]
fn compute_only_storage_serves_stale_catalog_during_remote_refresh_backoff_and_recovers() {
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("compute_only_refresh_stale_metric", &labels)
        .unwrap()
        .series_id;

    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    write_numeric_segment(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);

    let storage = builder_at_time(31)
        .with_object_store_path(object_store_dir.path())
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
        .with_remote_segment_refresh_interval(Duration::from_millis(1))
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("compute_only_refresh_stale_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    let invalid_segment_root = object_store_dir
        .path()
        .join("cold")
        .join(BLOB_LANE_ROOT)
        .join("segments")
        .join("L0")
        .join("seg-00000000000000ff");
    // A segment-shaped directory without a manifest forces the inventory scan to fail
    // deterministically, which keeps the visible catalog stale until the retry succeeds.
    std::fs::create_dir_all(&invalid_segment_root).unwrap();

    let warm_lane = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    write_numeric_segment(
        &warm_lane,
        &registry,
        series_id,
        0,
        2,
        &[(30, 30.0), (31, 31.0)],
    );

    let expected_stale_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    assert!(
        wait_for_condition(Duration::from_secs(1), Duration::from_millis(5), || {
            let selected = storage
                .select("compute_only_refresh_stale_metric", &labels, 0, 200)
                .unwrap();
            let snapshot = storage.observability_snapshot();
            selected == expected_stale_points
                && !snapshot.remote.accessible
                && snapshot.remote.catalog_refresh_errors_total >= 1
                && snapshot.remote.consecutive_refresh_failures == 1
                && snapshot.remote.backoff_active
        }),
        "timed out waiting for remote refresh backoff while the stale catalog stayed visible"
    );
    assert_eq!(
        storage
            .select("compute_only_refresh_stale_metric", &labels, 0, 200)
            .unwrap(),
        expected_stale_points
    );

    let outage_snapshot = storage.observability_snapshot();
    assert!(!outage_snapshot.remote.accessible);
    assert!(outage_snapshot.remote.catalog_refresh_errors_total >= 1);
    assert_eq!(outage_snapshot.remote.consecutive_refresh_failures, 1);
    assert!(outage_snapshot.remote.backoff_active);
    assert!(outage_snapshot
        .remote
        .last_refresh_attempt_unix_ms
        .is_some());
    assert!(outage_snapshot.remote.next_refresh_retry_unix_ms.is_some());
    assert!(outage_snapshot
        .remote
        .last_refresh_error
        .as_deref()
        .is_some_and(|message| message.contains("remote catalog refresh")));
    assert!(outage_snapshot.health.degraded);

    let refresh_errors_before_retry = outage_snapshot.remote.catalog_refresh_errors_total;
    assert_eq!(
        storage
            .select("compute_only_refresh_stale_metric", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    let repeated_snapshot = storage.observability_snapshot();
    assert_eq!(
        repeated_snapshot.remote.catalog_refresh_errors_total,
        refresh_errors_before_retry
    );
    assert_eq!(repeated_snapshot.remote.consecutive_refresh_failures, 1);
    assert!(repeated_snapshot.remote.backoff_active);

    std::fs::remove_dir_all(&invalid_segment_root).unwrap();

    let expected_recovered_points = vec![
        DataPoint::new(1, 1.0),
        DataPoint::new(2, 2.0),
        DataPoint::new(30, 30.0),
        DataPoint::new(31, 31.0),
    ];
    assert!(
        wait_for_condition(Duration::from_secs(2), Duration::from_millis(10), || {
            let selected = storage
                .select("compute_only_refresh_stale_metric", &labels, 0, 200)
                .unwrap();
            let snapshot = storage.observability_snapshot();
            selected == expected_recovered_points
                && snapshot.remote.accessible
                && !snapshot.remote.backoff_active
                && snapshot.remote.consecutive_refresh_failures == 0
                && snapshot.remote.last_refresh_error.is_none()
                && !snapshot.health.degraded
        }),
        "timed out waiting for remote catalog refresh recovery"
    );
    assert_eq!(
        storage
            .select("compute_only_refresh_stale_metric", &labels, 0, 200)
            .unwrap(),
        expected_recovered_points
    );
    let recovered_snapshot = storage.observability_snapshot();
    assert!(recovered_snapshot.remote.accessible);
    assert!(!recovered_snapshot.remote.backoff_active);
    assert_eq!(recovered_snapshot.remote.consecutive_refresh_failures, 0);
    assert!(recovered_snapshot.remote.last_refresh_error.is_none());
    assert!(!recovered_snapshot.health.degraded);
    assert_eq!(
        storage
            .select("compute_only_refresh_stale_metric", &labels, 0, 200)
            .unwrap(),
        expected_recovered_points
    );

    storage.close().unwrap();
}

#[cfg(unix)]
#[test]
fn compute_only_storage_suppresses_repeated_refresh_attempts_while_cached_segments_remain_readable()
{
    use std::os::unix::fs::PermissionsExt;

    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("compute_only_refresh_failure_metric", &labels)
        .unwrap()
        .series_id;

    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    write_numeric_segment(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);

    let storage = builder_at_time(4)
        .with_object_store_path(object_store_dir.path())
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
        .with_remote_segment_refresh_interval(Duration::from_millis(1))
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("compute_only_refresh_failure_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    let level_root = hot_lane.join("segments").join("L0");
    let original_permissions = std::fs::metadata(&level_root).unwrap().permissions();
    let mut denied_permissions = original_permissions.clone();
    denied_permissions.set_mode(0o000);
    std::fs::set_permissions(&level_root, denied_permissions).unwrap();

    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(
        storage
            .select("compute_only_refresh_failure_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    let outage_snapshot = storage.observability_snapshot();
    assert!(!outage_snapshot.remote.accessible);
    assert!(outage_snapshot.remote.catalog_refresh_errors_total >= 1);
    assert_eq!(outage_snapshot.remote.consecutive_refresh_failures, 1);
    assert!(outage_snapshot.remote.backoff_active);
    assert!(outage_snapshot.health.degraded);

    let refresh_errors_before_retry = outage_snapshot.remote.catalog_refresh_errors_total;
    assert_eq!(
        storage
            .select("compute_only_refresh_failure_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    let repeated_snapshot = storage.observability_snapshot();
    assert_eq!(
        repeated_snapshot.remote.catalog_refresh_errors_total,
        refresh_errors_before_retry
    );
    assert_eq!(repeated_snapshot.remote.consecutive_refresh_failures, 1);
    assert!(repeated_snapshot.remote.backoff_active);

    std::fs::set_permissions(&level_root, original_permissions).unwrap();
    write_numeric_segment(&hot_lane, &registry, series_id, 0, 2, &[(3, 3.0), (4, 4.0)]);

    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        storage
            .select("compute_only_refresh_failure_metric", &labels, 0, 10)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
        ]
    );

    let recovered_snapshot = storage.observability_snapshot();
    assert!(recovered_snapshot.remote.accessible);
    assert!(!recovered_snapshot.remote.backoff_active);
    assert_eq!(recovered_snapshot.remote.consecutive_refresh_failures, 0);
    assert!(recovered_snapshot.remote.last_refresh_error.is_none());
    assert!(!recovered_snapshot.health.degraded);

    storage.close().unwrap();
}

fn write_numeric_segment(
    lane_path: &std::path::Path,
    registry: &SeriesRegistry,
    series_id: u64,
    level: u8,
    segment_id: u64,
    points: &[(i64, f64)],
) {
    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, points)],
    );
    SegmentWriter::new(lane_path, level, segment_id)
        .unwrap()
        .write_segment(registry, &chunks)
        .unwrap();
}

fn persisted_timestamps_for_series(
    segments: &[crate::engine::segment::LoadedSegment],
    series_id: u64,
) -> Vec<i64> {
    segments
        .iter()
        .filter_map(|segment| segment.chunks_by_series.get(&series_id))
        .flat_map(|chunks| chunks.iter())
        .flat_map(|chunk| {
            if !chunk.points.is_empty() {
                return chunk
                    .points
                    .iter()
                    .map(|point| point.ts)
                    .collect::<Vec<_>>();
            }

            Encoder::decode_chunk_points_from_payload(
                chunk.header.lane,
                chunk.header.ts_codec,
                chunk.header.value_codec,
                chunk.header.point_count as usize,
                &chunk.encoded_payload,
            )
            .unwrap()
            .into_iter()
            .map(|point| point.ts)
            .collect::<Vec<_>>()
        })
        .collect()
}
