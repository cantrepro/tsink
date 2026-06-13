use super::*;

#[test]
fn reenable_wal_after_wal_disabled_run_ignores_stale_wal_generation() {
    let temp_dir = TempDir::new().unwrap();
    let stale_labels = vec![Label::new("host", "stale")];
    let fresh_labels = vec![Label::new("host", "fresh")];

    let stale_series_id = 1;
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: stale_series_id,
        metric: "metric".to_string(),
        labels: stale_labels.clone(),
    })
    .unwrap();
    let stale_batch = SamplesBatchFrame::from_points(
        stale_series_id,
        ValueLane::Numeric,
        &[
            ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            },
        ],
    )
    .unwrap();
    wal.append_samples(&[stale_batch]).unwrap();
    drop(wal);

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "metric",
                fresh_labels.clone(),
                DataPoint::new(10, 10.0),
            )])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let fresh = reopened.select("metric", &fresh_labels, 0, 20).unwrap();
    assert_eq!(fresh, vec![DataPoint::new(10, 10.0)]);
    let stale = reopened.select("metric", &stale_labels, 0, 20).unwrap();
    assert!(stale.is_empty());

    reopened.close().unwrap();
}

#[test]
fn reopens_from_segment_files_without_wal() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("seg", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("seg", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("seg", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select("seg", &labels, 0, 10).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0], DataPoint::new(1, 1.0));
    assert_eq!(points[2], DataPoint::new(3, 3.0));

    let segments_root = temp_dir
        .path()
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0");
    assert!(
        segments_root.exists(),
        "numeric lane segments should exist at {:?}",
        segments_root
    );
    let mut found_segment = false;
    for entry in std::fs::read_dir(segments_root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found_segment = true;
            assert!(path.join("manifest.bin").exists());
            assert!(path.join("chunks.bin").exists());
            assert!(path.join("chunk_index.bin").exists());
            assert!(path.join("series.bin").exists());
            assert!(path.join("postings.bin").exists());
        }
    }
    assert!(found_segment);

    reopened.close().unwrap();
}

#[test]
fn reopen_rejects_registry_snapshot_that_conflicts_with_segment_series_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let labels = vec![Label::new("host", "a")];
    let series_id = {
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("seg", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("seg", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let series_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing("seg", &labels)
            .unwrap()
            .series_id;
        storage.close().unwrap();
        series_id
    };

    let conflicting_registry = SeriesRegistry::new();
    conflicting_registry
        .register_series_with_id(series_id, "wrong_metric", &[Label::new("host", "wrong")])
        .unwrap();
    conflicting_registry
        .persist_to_path(&checkpoint_path)
        .unwrap();
    let conflicting_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select("seg", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        conflicting_checkpoint_bytes
    );
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed checkpoint should reload after reopen");
    assert!(loaded_registry
        .registry
        .resolve_existing("seg", &labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("wrong_metric", &[Label::new("host", "wrong")])
        .is_none());
    reopened.close().unwrap();
}

#[test]
fn isolates_numeric_and_blob_segments_and_merges_in_queries() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_labels = vec![Label::new("kind", "numeric")];
    let blob_labels = vec![Label::new("kind", "blob")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("mix", blob_labels.clone(), DataPoint::new(1, "a")),
                Row::with_labels("mix", blob_labels.clone(), DataPoint::new(2, "b")),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let numeric = reopened.select("mix", &numeric_labels, 0, 10).unwrap();
    let blob = reopened.select("mix", &blob_labels, 0, 10).unwrap();
    assert_eq!(numeric.len(), 2);
    assert_eq!(blob.len(), 2);
    assert_eq!(blob[0], DataPoint::new(1, "a"));
    assert_eq!(blob[1], DataPoint::new(2, "b"));

    let mut all = reopened.select_all("mix", 0, 10).unwrap();
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].1.len(), 2);
    assert_eq!(all[1].1.len(), 2);

    let numeric_root = temp_dir
        .path()
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0");
    let blob_root = temp_dir
        .path()
        .join(BLOB_LANE_ROOT)
        .join("segments")
        .join("L0");
    assert!(numeric_root.exists());
    assert!(blob_root.exists());

    reopened.close().unwrap();
}

#[test]
fn replay_highwater_is_conservative_when_one_configured_lane_has_no_segments() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    let labels = vec![Label::new("kind", "numeric")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("watermark_gap", &labels)
        .unwrap()
        .series_id;

    let mut numeric_chunks = HashMap::new();
    numeric_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, &[(1, 1.0)])],
    );
    SegmentWriter::new(&numeric_lane_path, 0, 1)
        .unwrap()
        .write_segment_with_wal_highwater(
            &registry,
            &numeric_chunks,
            WalHighWatermark {
                segment: 3,
                frame: 7,
            },
        )
        .unwrap();

    let loaded_numeric = load_segment_indexes(&numeric_lane_path).unwrap();
    let loaded_blob = load_segment_indexes(&blob_lane_path).unwrap();
    let merged = merge_loaded_segment_indexes(loaded_numeric, loaded_blob, true, true).unwrap();
    assert_eq!(merged.wal_replay_highwater, WalHighWatermark::default());
}

#[test]
fn persist_segment_rolls_back_published_lane_when_other_lane_fails() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    std::fs::write(&blob_lane_path, b"not-a-directory").unwrap();

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(numeric_lane_path.clone()),
        Some(blob_lane_path),
        1,
        ChunkStorageOptions::default(),
    )
    .unwrap();
    let numeric_labels = vec![Label::new("kind", "numeric")];
    let blob_labels = vec![Label::new("kind", "blob")];
    storage
        .insert_rows(&[
            Row::with_labels(
                "lane_atomicity",
                numeric_labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "lane_atomicity",
                blob_labels.clone(),
                DataPoint::new(1, "a"),
            ),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();

    assert!(storage.persist_segment().is_err());
    assert!(load_segments_for_level(&numeric_lane_path, 0)
        .unwrap()
        .is_empty());
}

#[test]
fn persist_segment_stamps_wal_highwater_when_wal_is_enabled() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "wal_highwater_persist",
            labels,
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_all_active().unwrap();
    storage.persist_segment().unwrap();

    let segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(!segments.is_empty());
    assert!(
        segments
            .iter()
            .any(|segment| segment.manifest.wal_highwater > WalHighWatermark::default()),
        "persisted segment must include WAL replay highwater when WAL is enabled"
    );

    storage.close().unwrap();
}

#[test]
fn close_reconciles_compacted_segments_before_checkpointing_registry() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let labels = vec![Label::new("host", "bench")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    for offset in 0..64 {
        storage
            .insert_rows(&[Row::with_labels(
                "close_compaction_checkpoint_metric",
                labels.clone(),
                DataPoint::new(1_000_000_000 + offset, offset as f64),
            )])
            .unwrap();
        storage.flush().unwrap();
    }

    storage
        .insert_rows(&[Row::with_labels(
            "close_compaction_checkpoint_metric",
            labels.clone(),
            DataPoint::new(1_000_000_064, 64.0),
        )])
        .unwrap();
    storage.flush().unwrap();
    storage.close().unwrap();

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("close should checkpoint the reconciled registry");
    assert!(loaded_registry
        .registry
        .resolve_existing("close_compaction_checkpoint_metric", &labels)
        .is_some());
}

#[test]
fn background_flush_seals_rotated_partition_before_reaching_chunk_cap() {
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
            partition_window: 1,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
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

    let labels = vec![Label::new("host", "a")];
    storage
        .insert_rows(&[
            Row::with_labels("partitioned", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("partitioned", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();
    storage.flush_background_eligible_active().unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("partitioned", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(
        chunks.len(),
        1,
        "background flush should seal the non-current partition head before chunk cap"
    );
    assert_eq!(chunks[0].header.min_ts, 1);
    assert_eq!(chunks[0].header.max_ts, 1);

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().point_count(), 1);
}
