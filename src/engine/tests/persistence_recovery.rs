use super::*;

#[test]
fn recovers_rows_from_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("recover", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("recover", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("recover", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select("recover", &labels, 0, 10).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0], DataPoint::new(1, 1.0));
    assert_eq!(points[2], DataPoint::new(3, 3.0));

    reopened.close().unwrap();
}

#[test]
fn stale_wal_with_already_persisted_points_does_not_duplicate_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);

    let stale_frames = {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();

        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        let frames = wal.replay_frames().unwrap();
        assert!(
            !frames.is_empty(),
            "expected WAL frames before close so we can simulate crash window replay"
        );

        storage.close().unwrap();
        frames
    };

    {
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        for frame in stale_frames {
            match frame {
                ReplayFrame::SeriesDefinition(definition) => {
                    wal.append_series_definition(&definition).unwrap();
                }
                ReplayFrame::Samples(batches) => {
                    wal.append_samples(&batches).unwrap();
                }
            }
        }
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select("dupe_recovery", &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0)
        ]
    );

    reopened.close().unwrap();
}

#[test]
fn stale_wal_with_already_persisted_nan_points_does_not_duplicate_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);

    let stale_frames = {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "dupe_recovery_nan",
                    labels.clone(),
                    DataPoint::new(1, f64::NAN),
                ),
                Row::with_labels("dupe_recovery_nan", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        let frames = wal.replay_frames().unwrap();
        assert!(
            !frames.is_empty(),
            "expected WAL frames before close so we can simulate crash window replay"
        );

        storage.close().unwrap();
        frames
    };

    {
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        for frame in stale_frames {
            match frame {
                ReplayFrame::SeriesDefinition(definition) => {
                    wal.append_series_definition(&definition).unwrap();
                }
                ReplayFrame::Samples(batches) => {
                    wal.append_samples(&batches).unwrap();
                }
            }
        }
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened
        .select("dupe_recovery_nan", &labels, 0, 10)
        .unwrap();
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].timestamp, 1);
    assert!(points[0].value_as_f64().is_some_and(f64::is_nan));
    assert_eq!(points[1], DataPoint::new(2, 2.0));

    reopened.close().unwrap();
}

#[test]
fn query_prefers_compacted_generation_when_compaction_crash_leaves_both_generations() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "compaction_crash_dupe";

    let mut registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    let mut stale_l0 = HashMap::new();
    stale_l0.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &stale_l0)
        .unwrap();

    let mut compacted_l1 = HashMap::new();
    compacted_l1.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 10.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 1, 2)
        .unwrap()
        .write_segment(&registry, &compacted_l1)
        .unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select(metric, &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(1, 10.0), DataPoint::new(2, 2.0)]
    );

    reopened.close().unwrap();
}
