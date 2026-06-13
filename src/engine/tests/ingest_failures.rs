use super::*;

#[test]
fn rejects_mixed_numeric_insert_when_wal_is_disabled() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_chunk_points(8)
        .build()
        .unwrap();
    let labels = vec![Label::new("host", "a")];

    let err = storage
        .insert_rows(&[
            Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(2, 2_i64)),
        ])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let points = storage.select("mixed_no_wal", &labels, 0, 10).unwrap();
    assert!(points.is_empty());
}

#[test]
fn rejects_mixed_numeric_insert_across_calls_when_wal_is_disabled() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_chunk_points(8)
        .build()
        .unwrap();
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            "mixed_no_wal_across_calls",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "mixed_no_wal_across_calls",
            labels.clone(),
            DataPoint::new(2, 2_i64),
        )])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let points = storage
        .select("mixed_no_wal_across_calls", &labels, 0, 10)
        .unwrap();
    assert_eq!(points, vec![DataPoint::new(1, 1.0)]);
}

#[test]
fn failed_insert_rolls_back_new_series_metadata_immediately() {
    let storage = ChunkStorage::new(4, None);
    let labels = vec![Label::new("host", "a")];

    let err = storage
        .insert_rows(&[
            Row::with_labels("phantom_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("phantom_metric", labels.clone(), DataPoint::new(2, 2_i64)),
        ])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let has_phantom_metric = storage
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == "phantom_metric" && series.labels == labels);
    assert!(!has_phantom_metric);
}

#[test]
fn failed_mixed_numeric_insert_does_not_resurrect_series_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let err = storage
            .insert_rows(&[
                Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(2, 2_i64)),
            ])
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let has_mixed_metric = reopened
        .list_metrics()
        .unwrap()
        .into_iter()
        .any(|series| series.name == "mixed_metric" && series.labels == labels);
    assert!(!has_mixed_metric);

    reopened.close().unwrap();
}

#[test]
fn wal_append_failure_does_not_ingest_points_or_survive_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "wal_append_atomicity";
    let oversized_value = vec![0xAB; (64 * 1024 * 1024) + 1];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new(metric, DataPoint::new(1, oversized_value))])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::InvalidConfiguration(message)
                if message.contains("WAL frame payload too large")
        ));

        let points = storage.select(metric, &[], 0, 10).unwrap();
        assert!(
            points.is_empty(),
            "failed WAL append must not expose ingested points in memory"
        );

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select(metric, &[], 0, 10).unwrap();
    assert!(
        points.is_empty(),
        "failed WAL append must not leave points durable after reopen"
    );

    reopened.close().unwrap();
}

#[test]
fn failed_ingest_does_not_append_wal_samples() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new(2, Some(wal));
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            "wal_ingest_guard",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let sample_batches_before: usize = storage
        .wal
        .as_ref()
        .unwrap()
        .replay_frames()
        .unwrap()
        .into_iter()
        .map(|frame| match frame {
            ReplayFrame::Samples(batches) => batches.len(),
            ReplayFrame::SeriesDefinition(_) => 0,
        })
        .sum();

    let series_id = storage
        .registry
        .read()
        .resolve_existing("wal_ingest_guard", &labels)
        .unwrap()
        .series_id;
    {
        let mut active = storage.active_shard(series_id).write();
        active
            .get_mut(&series_id)
            .unwrap()
            .builder
            .append(2, Value::I64(2));
    }

    let err = storage
        .insert_rows(&[Row::with_labels(
            "wal_ingest_guard",
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

    let sample_batches_after: usize = storage
        .wal
        .as_ref()
        .unwrap()
        .replay_frames()
        .unwrap()
        .into_iter()
        .map(|frame| match frame {
            ReplayFrame::Samples(batches) => batches.len(),
            ReplayFrame::SeriesDefinition(_) => 0,
        })
        .sum();
    assert_eq!(sample_batches_after, sample_batches_before);
}
