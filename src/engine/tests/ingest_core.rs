use super::*;

#[test]
fn rotates_chunks_at_configured_cap() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "a")];

    let rows = vec![
        Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
        Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
        Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
        Row::with_labels("cpu", labels.clone(), DataPoint::new(4, 4.0)),
        Row::with_labels("cpu", labels.clone(), DataPoint::new(5, 5.0)),
    ];

    storage.insert_rows(&rows).unwrap();

    let series_id = storage
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].header.point_count, 2);
    assert_eq!(chunks[1].header.point_count, 2);

    let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.builder.len(), 1);
}

#[test]
fn finalize_sorts_out_of_order_chunk_points() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
        ])
        .unwrap();

    let series_id = storage
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    let timestamps = chunks[0]
        .points
        .iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2]);
}

#[test]
fn select_reads_active_points_without_flushing_and_sorts_points() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    let points = storage.select("latency", &labels, 0, 10).unwrap();
    let timestamps = points
        .iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2, 3]);
    assert_eq!(storage.select("latency", &labels, 0, 10).unwrap(), points);

    let series_id = storage
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().builder.len(), 1);
    drop(active);

    let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(sealed.get(&series_id).unwrap().len(), 1);
}

#[test]
fn select_sorts_unsorted_active_points_without_flushing() {
    let storage = ChunkStorage::new(8, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    let points = storage.select("latency", &labels, 0, 10).unwrap();
    let timestamps = points
        .iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2, 3]);

    let series_id = storage
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().builder.len(), 3);
    drop(active);

    let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    assert!(sealed.get(&series_id).is_none());
}

#[test]
fn select_sorts_overlapping_sealed_chunks() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(4, 4.0)),
        ])
        .unwrap();

    let points = storage.select("latency", &labels, 0, 10).unwrap();
    let timestamps = points
        .iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2, 3, 4]);
}

#[test]
fn select_preserves_duplicate_timestamps_across_overlapping_chunks() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 30.0)),
        ])
        .unwrap();

    let points = storage.select("latency", &labels, 0, 10).unwrap();
    let timestamps = points
        .iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2, 3, 3]);

    let mut values_at_timestamp_three = points
        .iter()
        .filter(|point| point.timestamp == 3)
        .filter_map(|point| point.value_as_f64())
        .collect::<Vec<_>>();
    values_at_timestamp_three.sort_by(f64::total_cmp);
    assert_eq!(values_at_timestamp_three, vec![3.0, 30.0]);
}

#[test]
fn select_sorts_manual_unsorted_chunk_without_payload() {
    let storage = ChunkStorage::new(4, None);
    let labels = vec![Label::new("host", "a")];

    let series_id = storage
        .registry
        .write()
        .resolve_or_insert("manual", &labels)
        .unwrap()
        .series_id;

    storage.append_sealed_chunk(
        series_id,
        Chunk {
            header: ChunkHeader {
                series_id,
                lane: ValueLane::Numeric,
                point_count: 3,
                min_ts: 1,
                max_ts: 3,
                ts_codec: TimestampCodecId::DeltaVarint,
                value_codec: ValueCodecId::ConstantRle,
            },
            points: vec![
                ChunkPoint {
                    ts: 3,
                    value: Value::F64(3.0),
                },
                ChunkPoint {
                    ts: 1,
                    value: Value::F64(1.0),
                },
                ChunkPoint {
                    ts: 2,
                    value: Value::F64(2.0),
                },
            ],
            encoded_payload: Vec::new(),
        },
    );

    let points = storage.select("manual", &labels, 0, 10).unwrap();
    let timestamps = points
        .iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2, 3]);
}

#[test]
fn select_into_reuses_output_buffer() {
    let storage = ChunkStorage::new(4, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let mut out = vec![DataPoint::new(-1, -1.0)];
    storage
        .select_into("cpu", &labels, 0, 10, &mut out)
        .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].timestamp, 1);
    assert_eq!(out[1].timestamp, 2);
    assert_eq!(out[2].timestamp, 3);

    let reused_capacity = out.capacity();
    storage
        .select_into("cpu", &labels, 100, 200, &mut out)
        .unwrap();
    assert!(out.is_empty());
    assert!(out.capacity() >= reused_capacity);
}

#[test]
fn rejects_lane_mismatch_for_same_series() {
    let storage = ChunkStorage::new(4, None);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            "events",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "events",
            labels,
            DataPoint::new(2, "oops"),
        )])
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
}
