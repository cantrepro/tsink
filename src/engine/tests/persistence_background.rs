use super::*;

#[test]
fn background_compaction_reduces_l0_segments_while_storage_is_open() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let mut registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("background_compaction", &labels)
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        5,
        ChunkStorageOptions {
            retention_window: i64::MAX,
            retention_enforced: false,
            partition_window: i64::MAX,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: Duration::from_millis(25),
            background_threads_enabled: true,
        },
    )
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut compacted = false;

    while Instant::now() < deadline {
        let l0 = load_segments_for_level(&lane_path, 0).unwrap();
        let l1 = load_segments_for_level(&lane_path, 1).unwrap();
        if l0.len() < 4 && !l1.is_empty() {
            compacted = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    assert!(compacted, "background thread did not compact L0 into L1");
    storage.close().unwrap();
}

#[test]
fn background_flush_pipeline_refreshes_persisted_index_and_evicts_sealed_chunks_while_open() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions::default(),
        )
        .unwrap(),
    );
    storage
        .start_background_flush_thread(Duration::from_millis(25))
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("background_flush", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("background_flush", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("background_flush", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .registry
        .read()
        .resolve_existing("background_flush", &labels)
        .unwrap()
        .series_id;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut flushed = false;
    while Instant::now() < deadline {
        let active_len = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.builder.len());
        let sealed_len = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let persisted_len = storage
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let l0 = load_segments_for_level(&lane_path, 0).unwrap();

        if active_len == 0 && sealed_len == 0 && persisted_len >= 2 && !l0.is_empty() {
            flushed = true;
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    assert!(
            flushed,
            "background flush pipeline did not refresh persisted indexes and evict flushed sealed chunks"
        );
    assert_eq!(
        storage
            .select("background_flush", &labels, 0, 10)
            .unwrap()
            .len(),
        3
    );

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_waits_for_busy_writer_permit() {
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_window: i64::MAX,
            retention_enforced: false,
            partition_window: i64::MAX,
            max_writers: 2,
            write_timeout: Duration::from_millis(250),
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
        .insert_rows(&[Row::with_labels(
            "flush_busy_writer",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .registry
        .read()
        .resolve_existing("flush_busy_writer", &labels)
        .unwrap()
        .series_id;

    let active_before = storage
        .active_shard(series_id)
        .read()
        .get(&series_id)
        .map_or(0, |state| state.builder.len());
    assert_eq!(active_before, 1);

    let held_permit = storage.write_limiter.acquire();
    thread::scope(|scope| {
        let flush = scope.spawn(|| storage.flush_pipeline_once());
        thread::sleep(Duration::from_millis(50));
        drop(held_permit);
        flush.join().unwrap().unwrap();
    });

    let active_after = storage
        .active_shard(series_id)
        .read()
        .get(&series_id)
        .map_or(0, |state| state.builder.len());
    assert_eq!(active_after, 0);
    assert_eq!(
        storage
            .select("flush_busy_writer", &labels, 0, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(
        !load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
        "flush pipeline should persist after blocked writer permit is released"
    );

    storage.close().unwrap();
}
