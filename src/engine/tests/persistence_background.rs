use super::*;
use std::sync::mpsc;

fn new_raw_numeric_storage(lane_path: std::path::PathBuf, next_segment_id: u64) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path),
        None,
        next_segment_id,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn bounded_catalog_refresh_storage(
    lane_path: &std::path::Path,
    next_segment_id: u64,
    max_items: usize,
    max_bytes: u64,
) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.to_path_buf()),
        None,
        next_segment_id,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            maintenance_max_items_per_pass: max_items,
            maintenance_max_bytes_per_pass: max_bytes,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn finite_compute_only_remote_storage(
    tiered_storage: super::super::config::TieredStorageConfig,
    next_segment_id: u64,
    max_items: usize,
) -> ChunkStorage {
    finite_compute_only_remote_storage_with_limits(
        tiered_storage,
        next_segment_id,
        max_items,
        256 * 1024 * 1024,
    )
}

fn finite_compute_only_remote_storage_with_limits(
    tiered_storage: super::super::config::TieredStorageConfig,
    next_segment_id: u64,
    max_items: usize,
    max_bytes: u64,
) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        next_segment_id,
        ChunkStorageOptions {
            runtime_mode: StorageRuntimeMode::ComputeOnly,
            retention_enforced: false,
            maintenance_max_items_per_pass: max_items,
            maintenance_max_bytes_per_pass: max_bytes,
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(tiered_storage),
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn write_numeric_segment_to_path(
    lane_path: &std::path::Path,
    registry: &SeriesRegistry,
    series_id: crate::engine::series::SeriesId,
    level: u8,
    segment_id: u64,
    points: &[(i64, f64)],
) -> std::path::PathBuf {
    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, points)],
    );
    let writer = SegmentWriter::new(lane_path, level, segment_id).unwrap();
    writer.write_segment(registry, &chunks).unwrap();
    writer.layout().root.clone()
}

fn bounded_retention_page_storage(
    lane_path: &std::path::Path,
    next_segment_id: u64,
    max_items: usize,
) -> ChunkStorage {
    bounded_retention_page_storage_with_bytes(
        lane_path,
        next_segment_id,
        max_items,
        256 * 1024 * 1024,
    )
}

fn bounded_retention_page_storage_with_bytes(
    lane_path: &std::path::Path,
    next_segment_id: u64,
    max_items: usize,
    max_bytes: u64,
) -> ChunkStorage {
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.to_path_buf()),
        None,
        next_segment_id,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 10,
            retention_enforced: true,
            maintenance_max_items_per_pass: max_items,
            maintenance_max_bytes_per_pass: max_bytes,
            background_threads_enabled: false,
            #[cfg(test)]
            current_time_override: Some(100),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(lane_path).unwrap(), false)
        .unwrap();
    storage
}

fn modeled_retention_rewrite_candidate_bytes(root: &std::path::Path) -> u64 {
    let fingerprint = crate::engine::segment::read_segment_manifest_fingerprint(root).unwrap();
    let descriptor = std::mem::size_of::<super::super::tiering::SegmentInventoryEntry>()
        .saturating_add(root.as_os_str().as_encoded_bytes().len());
    fingerprint.files.iter().fold(
        u64::try_from(descriptor)
            .unwrap()
            .saturating_add(std::fs::metadata(root.join("manifest.bin")).unwrap().len()),
        |total, file| total.saturating_add(file.file_len),
    )
}

fn mark_post_flush_maintenance_pending(storage: &ChunkStorage) {
    storage
        .coordination
        .post_flush_maintenance_pending
        .store(true, std::sync::atomic::Ordering::Release);
}

fn incremental_segment_paths(snapshot_path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let dir_path = SeriesRegistry::incremental_dir(snapshot_path);
    let mut paths = match std::fs::read_dir(&dir_path) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                file_type.is_file().then_some(entry.path())
            })
            .collect::<Vec<_>>(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("failed to list incremental registry dir: {err}"),
    };
    paths.sort();
    paths
}

#[test]
fn background_compaction_reduces_l0_segments_while_storage_is_open() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
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
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window_units: 1,
            new_series_window_nanos: 60_000_000_000,
            write_batch_limits: Default::default(),
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: Duration::from_millis(25),
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
fn background_compaction_refreshes_persisted_index_in_background() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("background_compaction_sync", &labels)
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

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            5,
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
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
                max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
                max_new_series_per_window: None,
                new_series_window_units: 1,
                new_series_window_nanos: 60_000_000_000,
                write_batch_limits: Default::default(),
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: Duration::from_millis(25),
                maintenance_max_items_per_pass: 1_024,
                maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
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
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();
    storage.start_background_persisted_refresh_thread().unwrap();

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
    assert!(
        compacted,
        "background compaction did not produce compacted output"
    );
    storage.notify_persisted_refresh_thread();

    let refresh_deadline = Instant::now() + Duration::from_secs(2);
    let mut refreshed = false;
    while Instant::now() < refresh_deadline {
        let persisted_levels = storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .unwrap()
            .iter()
            .map(|chunk| chunk.level)
            .collect::<Vec<_>>();
        if !storage
            .persisted
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
            && persisted_levels.iter().all(|level| *level == 1)
        {
            refreshed = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        refreshed,
        "background refresh worker did not reconcile the compacted persisted index"
    );

    let points = storage
        .select("background_compaction_sync", &labels, 0, 10)
        .unwrap();
    assert_eq!(points.len(), 4);

    let persisted_levels = storage
        .persisted
        .persisted_index
        .read()
        .chunk_refs
        .get(&series_id)
        .unwrap()
        .iter()
        .map(|chunk| chunk.level)
        .collect::<Vec<_>>();
    assert!(
        persisted_levels.iter().all(|level| *level == 1),
        "persisted index should point at compacted L1 output after background refresh"
    );

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_reconciles_known_dirty_compaction_changes_before_checkpointing() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("flush_known_dirty_compaction", &labels)
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
        2,
        None,
        Some(lane_path.clone()),
        None,
        5,
        ChunkStorageOptions {
            retention_enforced: false,
            compaction_interval: Duration::from_millis(25),
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
            background_threads_enabled: true,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut compacted = false;
    while Instant::now() < deadline {
        let l1 = load_segments_for_level(&lane_path, 1).unwrap();
        if storage
            .persisted
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
            && !l1.is_empty()
        {
            compacted = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        compacted,
        "background compaction did not leave dirty persisted changes for flush reconciliation"
    );

    storage
        .insert_rows(&[Row::with_labels(
            "flush_known_dirty_compaction",
            labels.clone(),
            DataPoint::new(10, 10.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .select("flush_known_dirty_compaction", &labels, 0, 20)
            .unwrap()
            .len(),
        5
    );

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
            ChunkStorageOptions {
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
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
        .catalog
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
            .map_or(0, |state| state.point_count());
        let sealed_len = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let persisted_len = storage
            .persisted
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
    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should seal the remaining current head once it becomes the freshness bottleneck",
    );

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_waits_for_inflight_tombstone_visibility_publication() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Milliseconds,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_delete_visibility_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "flush_delete_visibility_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "flush_delete_visibility_metric",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let (flush_published_tx, flush_published_rx) = mpsc::channel();
    storage.set_persist_post_publish_hook(move |_| {
        let _ = flush_published_tx.send(());
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage
            .delete_series(&SeriesSelection::new().with_metric("flush_delete_visibility_metric"))
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        let result = flush_storage.flush_pipeline_once();
        flush_tx.send(result).unwrap();
    });

    flush_published_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not reach the segment publication hook");
    let flush_blocked = flush_rx.recv_timeout(Duration::from_millis(200)).is_err();

    delete_release_tx.send(()).unwrap();
    assert!(
        flush_blocked,
        "flush should wait for the in-flight tombstone visibility publication",
    );

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert!(flush_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    flush_thread.join().unwrap();

    assert!(storage
        .select("flush_delete_visibility_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());

    storage.clear_persist_post_publish_hook();
    storage.clear_tombstone_post_swap_pre_visibility_hook();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_adopts_new_segments_without_full_inventory_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let historical_series_id = registry
        .resolve_or_insert("historical_flush_inventory", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=64 {
        let mut chunks = HashMap::new();
        chunks.insert(
            historical_series_id,
            vec![make_persisted_numeric_chunk(
                historical_series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            65,
            ChunkStorageOptions {
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage
        .start_background_flush_thread(Duration::from_millis(25))
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "background_flush_incremental",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "background_flush_incremental",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "background_flush_incremental",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_incremental", &labels)
        .unwrap()
        .series_id;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut flushed = false;
    while Instant::now() < deadline {
        let active_len = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count());
        let sealed_len = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let persisted_len = storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());

        if active_len == 0 && sealed_len == 0 && persisted_len >= 2 {
            flushed = true;
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    assert!(
        flushed,
        "background flush did not persist the incremental series"
    );
    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "steady-state flush should not trigger a full persisted inventory scan",
    );
    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should seal the last current head once it is the only unpublished data",
    );

    storage.clear_full_inventory_scan_hook();
    drop(storage);
}

#[test]
fn background_flush_persists_current_partial_head_and_resets_wal() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_wal_guard",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_wal_guard",
            labels.clone(),
            DataPoint::new(2, 2.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_wal_guard",
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_wal_guard", &labels)
        .unwrap()
        .series_id;
    let wal = storage.persisted.wal.as_ref().unwrap();
    assert!(
        wal.current_highwater() > WalHighWatermark::default(),
        "writes should advance the WAL highwater before any flush path runs",
    );

    storage.background_flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should seal the current partial head once it is the last unpublished data",
    );
    assert_eq!(
        storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        0,
        "background flush should evict the sealed snapshot it just persisted",
    );
    assert_eq!(
        storage
            .persisted.persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        2,
        "background flush should persist both the previously sealed chunk and the current partial head",
    );
    assert_eq!(
        wal.total_size_bytes().unwrap(),
        0,
        "background flush should reset the WAL once it durably publishes the current head",
    );

    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "explicit flush should still drain the current head",
    );
    assert_eq!(
        storage
            .persisted.persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        2,
        "explicit flush should be a no-op once background flush has already published the current head",
    );
    assert_eq!(
        wal.total_size_bytes().unwrap(),
        0,
        "WAL should stay reset once the current head has already been durably published",
    );
    assert_eq!(
        storage
            .select("background_flush_wal_guard", &labels, 0, 10)
            .unwrap()
            .len(),
        3
    );

    storage.close().unwrap();
}

#[test]
fn timed_flush_defers_young_wal_backed_current_head_until_half_initial_block() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "fill-aware")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2_048,
        Some(wal),
        Some(lane_path),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            wal_size_limit_bytes: 64 * 1024 * 1024,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    let rows = (0..31)
        .map(|timestamp| {
            Row::with_labels(
                "fill_aware_background_flush",
                labels.clone(),
                DataPoint::new(timestamp, timestamp as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("fill_aware_background_flush", &labels)
        .unwrap()
        .series_id;

    storage.background_flush_pipeline_once().unwrap();
    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        31,
        "a healthy WAL-backed timed pass must not turn a very young current head into a tiny segment",
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        0,
    );

    storage
        .insert_rows(&[Row::with_labels(
            "fill_aware_background_flush",
            labels.clone(),
            DataPoint::new(31, 31.0),
        )])
        .unwrap();
    storage.background_flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        1,
    );
    assert_eq!(
        storage
            .select("fill_aware_background_flush", &labels, 0, 32)
            .unwrap()
            .len(),
        32,
    );
    assert_eq!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .total_size_bytes()
            .unwrap(),
        0,
    );
    storage.close().unwrap();
}

#[test]
fn background_flush_persists_sealed_and_live_series_together() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let target_labels = vec![Label::new("host", "sealed")];
    let blocker_labels = vec![Label::new("host", "live")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_live_blocker",
            blocker_labels.clone(),
            DataPoint::new(1, 10.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[
            Row::with_labels(
                "background_flush_sealed_target",
                target_labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "background_flush_sealed_target",
                target_labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();

    let blocker_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_live_blocker", &blocker_labels)
        .unwrap()
        .series_id;
    let target_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_sealed_target", &target_labels)
        .unwrap()
        .series_id;

    storage.background_flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(blocker_series_id)
            .read()
            .get(&blocker_series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should also publish the unrelated live head once it is the only unpublished data for that series",
    );
    assert_eq!(
        storage
            .sealed_shard(target_series_id)
            .read()
            .get(&target_series_id)
            .map_or(0, |chunks| chunks.len()),
        0,
        "background flush should evict the sealed chunk once it is published",
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&target_series_id)
            .map_or(0, |chunks| chunks.len()),
        1,
        "background flush should publish the unrelated sealed series",
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&blocker_series_id)
            .map_or(0, |chunks| chunks.len()),
        1,
        "background flush should publish the live blocker series once it seals the current head",
    );
    assert_eq!(
        load_segments_for_level(&lane_path, 0).unwrap().len(),
        1,
        "background flush should write a persisted segment without waiting for close",
    );
    assert_eq!(
        storage.persisted.wal.as_ref().unwrap().total_size_bytes().unwrap(),
        0,
        "background flush should reset the WAL once both the sealed series and the live head are durably published",
    );
    assert_eq!(
        storage
            .select("background_flush_sealed_target", &target_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );
    assert_eq!(
        storage
            .select("background_flush_live_blocker", &blocker_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 10.0)],
    );

    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_retention_enforced(false)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("background_flush_sealed_target", &target_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
        "reopen should recover the background-persisted sealed series without losing data",
    );
    assert_eq!(
        reopened
            .select("background_flush_live_blocker", &blocker_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 10.0)],
        "reopen should recover the live blocker from the background-persisted segment",
    );

    reopened.close().unwrap();
}

#[test]
fn background_flush_publishes_current_heads_to_compute_only_readers() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let metric = "background_flush_compute_only_visibility";

    let writer = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(super::super::config::TieredStorageConfig {
                object_store_root: object_store_dir.path().to_path_buf(),
                segment_catalog_path: None,
                mirror_hot_segments: true,
                hot_retention_window: i64::MAX,
                warm_retention_window: i64::MAX,
            }),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    install_shared_object_store_writer_lock_for_test(&writer, object_store_dir.path());
    let reader = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ComputeOnly,
            background_threads_enabled: false,
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(super::super::config::TieredStorageConfig {
                object_store_root: object_store_dir.path().to_path_buf(),
                segment_catalog_path: None,
                mirror_hot_segments: false,
                hot_retention_window: i64::MAX,
                warm_retention_window: i64::MAX,
            }),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    writer
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    assert!(
        reader.select(metric, &labels, 0, 10).unwrap().is_empty(),
        "compute-only readers should not see the current head before it is background-published",
    );

    writer.background_flush_pipeline_once().unwrap();

    assert_eq!(
        load_segments_for_level(
            object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT),
            0
        )
        .unwrap()
        .len(),
        1,
        "background flush should mirror the sealed current head into the object-store hot tier",
    );

    std::thread::sleep(Duration::from_millis(5));
    reader.sync_persisted_segments_from_disk_if_dirty().unwrap();

    assert_eq!(
        reader.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)],
        "compute-only readers should observe current-head writes after the bounded background flush and refresh interval",
    );

    reader.close().unwrap();
    writer.close().unwrap();
}

#[test]
fn flush_pipeline_persists_large_sealed_snapshot_without_chunk_clones() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = new_raw_numeric_storage(lane_path.clone(), 1);
    let metric = "flush_snapshot_no_clone";
    let series_count = 64usize;
    let points_per_series = 16usize;
    let total_points = series_count * points_per_series;
    let total_chunks = total_points / 2;
    let sample_labels = vec![Label::new("host", "series-0000")];

    let rows = (0..series_count)
        .flat_map(|series_idx| {
            let labels = vec![Label::new("host", format!("series-{series_idx:04}"))];
            (0..points_per_series).map(move |point_idx| {
                Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(1_000 + point_idx as i64, point_idx as f64),
                )
            })
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    crate::engine::chunk::reset_chunk_clone_count();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        crate::engine::chunk::chunk_clone_count(),
        0,
        "flush should persist sealed chunks through shared references"
    );

    let l0 = load_segments_for_level(&lane_path, 0).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l0[0].manifest.chunk_count, total_chunks);
    assert_eq!(l0[0].manifest.point_count, total_points);

    let selected = storage.select(metric, &sample_labels, 0, 10_000).unwrap();
    assert_eq!(selected.len(), points_per_series);

    storage.close().unwrap();
}

#[test]
fn bounded_background_persistence_resumes_and_restart_replays_only_the_suffix() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 2;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();
    let metric = "bounded_partial_persist_restart";
    let labels = vec![Label::new("host", "a")];

    for ts in 1..=5 {
        storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(ts, ts as f64),
            )])
            .unwrap();
    }
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 5);

    let first = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap();
    assert_eq!(first.chunks, 2);
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 3);
    let first_segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert_eq!(first_segments.len(), 1);
    assert_eq!(first_segments[0].manifest.chunk_count, 2);
    let first_deferred_floor = storage
        .chunks
        .pending_sealed_chunks
        .read()
        .by_wal
        .iter()
        .next()
        .unwrap()
        .wal_lowwater;
    assert!(first_segments[0].manifest.wal_highwater < first_deferred_floor);

    let second = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap();
    assert_eq!(second.chunks, 2);
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 1);
    assert_eq!(load_segments_for_level(&lane_path, 0).unwrap().len(), 2);

    // Model an abrupt process exit: keep the last chunk only in WAL and prove that the maximum
    // segment replay high-water mark neither skips it nor duplicates the persisted prefix.
    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        (1..=5)
            .map(|ts| DataPoint::new(ts, ts as f64))
            .collect::<Vec<_>>()
    );
    reopened.close().unwrap();
}

#[test]
fn bounded_persistence_evicts_only_its_exact_selection_from_a_large_backlog() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const BACKLOG_CHUNKS: usize = 256;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    options.memory_budget_bytes = 256 * 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(lane_path),
        None,
        1,
        options,
    )
    .unwrap();
    let metric = "bounded_exact_sealed_eviction";
    let labels = vec![Label::new("host", "a")];

    for ts in 0..=BACKLOG_CHUNKS {
        storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(ts as i64, ts as f64),
            )])
            .unwrap();
    }

    let sealed_chunk_count = || {
        storage
            .chunks
            .sealed_chunks
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .values()
                    .map(|chunks| chunks.len())
                    .sum::<usize>()
            })
            .sum::<usize>()
    };
    assert_eq!(sealed_chunk_count(), BACKLOG_CHUNKS + 1);

    let selected_location = storage
        .chunks
        .pending_sealed_chunks
        .read()
        .by_sequence
        .first_key_value()
        .map(|(_, location)| *location)
        .unwrap();
    let selected_chunk_bytes = {
        let sealed = storage.chunks.sealed_chunks[selected_location.shard_idx].read();
        let chunk = sealed
            .get(&selected_location.series_id)
            .and_then(|chunks| chunks.get(&selected_location.sealed_key))
            .unwrap();
        ChunkStorage::chunk_memory_usage_bytes(chunk)
    };
    let selected_shard_bytes_before = storage.memory.used_bytes_by_shard
        [selected_location.shard_idx]
        .load(Ordering::Acquire) as usize;
    let eviction_inspections = Arc::new(AtomicUsize::new(0));
    storage.set_exact_sealed_eviction_inspect_hook({
        let eviction_inspections = Arc::clone(&eviction_inspections);
        move || {
            eviction_inspections.fetch_add(1, Ordering::Relaxed);
        }
    });

    let outcome = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap();
    assert!(outcome.persisted);
    assert_eq!(outcome.chunks, 1);
    assert_eq!(eviction_inspections.load(Ordering::Relaxed), 1);
    assert_eq!(sealed_chunk_count(), BACKLOG_CHUNKS);
    assert_eq!(
        storage.chunks.pending_sealed_chunks.read().len(),
        BACKLOG_CHUNKS
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .flush
            .evicted_sealed_chunks_total,
        1
    );
    let selected_shard_bytes_after = storage.memory.used_bytes_by_shard[selected_location.shard_idx]
        .load(Ordering::Acquire) as usize;
    assert_eq!(
        selected_shard_bytes_before.saturating_sub(selected_shard_bytes_after),
        selected_chunk_bytes,
        "exact eviction must debit the selected chunk from its owning shard"
    );

    let expected = (0..=BACKLOG_CHUNKS)
        .map(|ts| DataPoint::new(ts as i64, ts as f64))
        .collect::<Vec<_>>();
    assert_eq!(
        storage
            .select(metric, &labels, 0, BACKLOG_CHUNKS as i64 + 1)
            .unwrap(),
        expected,
        "persisted publication must replace the evicted in-memory chunk without a query gap"
    );

    storage.clear_exact_sealed_eviction_inspect_hook();
    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened
            .select(metric, &labels, 0, BACKLOG_CHUNKS as i64 + 1)
            .unwrap(),
        expected,
        "restart must load the selected prefix once and replay every deferred chunk"
    );
    reopened.close().unwrap();
}

#[test]
fn bounded_non_tiered_flush_accounts_one_root_without_scanning_a_large_persisted_backlog() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const BACKLOG_SEGMENTS: usize = 64;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    options.memory_budget_bytes = 256 * 1024 * 1024;
    let storage =
        ChunkStorage::new_with_data_path_and_options(1, None, Some(lane_path), None, 1, options)
            .unwrap();

    for ts in 0..BACKLOG_SEGMENTS {
        storage
            .insert_rows(&[Row::new(
                "bounded_catalog_delta_backlog",
                DataPoint::new(ts as i64, ts as f64),
            )])
            .unwrap();
        let outcome = storage
            .persist_segment_background_bounded_with_outcome()
            .unwrap();
        assert!(outcome.persisted);
        assert_eq!(outcome.chunks, 1);
    }
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        BACKLOG_SEGMENTS
    );

    let accounting_inspections = Arc::new(AtomicUsize::new(0));
    storage.set_persisted_index_accounting_inspect_hook({
        let accounting_inspections = Arc::clone(&accounting_inspections);
        move || {
            accounting_inspections.fetch_add(1, Ordering::Relaxed);
        }
    });
    let catalog_inventory_inspections = Arc::new(AtomicUsize::new(0));
    storage.set_persisted_catalog_inventory_entry_hook({
        let catalog_inventory_inspections = Arc::clone(&catalog_inventory_inspections);
        move || {
            catalog_inventory_inspections.fetch_add(1, Ordering::Relaxed);
        }
    });

    storage
        .insert_rows(&[Row::new(
            "bounded_catalog_delta_backlog",
            DataPoint::new(BACKLOG_SEGMENTS as i64, BACKLOG_SEGMENTS as f64),
        )])
        .unwrap();
    let outcome = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap();
    assert!(outcome.persisted);
    assert_eq!(outcome.chunks, 1);
    assert_eq!(
        accounting_inspections.load(Ordering::Relaxed),
        2,
        "accounting should inspect the one changed root before and after publication"
    );
    assert_eq!(
        catalog_inventory_inspections.load(Ordering::Relaxed),
        0,
        "a non-tiered flush should not rebuild the persisted segment inventory"
    );
    assert_eq!(
        storage.observability_snapshot().flush.hot_segments_visible,
        (BACKLOG_SEGMENTS + 1) as u64
    );

    storage.clear_persisted_catalog_inventory_entry_hook();
    accounting_inspections.store(0, Ordering::Relaxed);
    let removed_root = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .next()
        .cloned()
        .unwrap();
    assert!(storage
        .remove_persisted_segment_roots(&[removed_root])
        .unwrap());
    assert_eq!(
        accounting_inspections.load(Ordering::Relaxed),
        2,
        "root removal accounting should inspect only the removed root before and after mutation"
    );
    storage.clear_persisted_index_accounting_inspect_hook();
    assert_engine_memory_usage_reconciled(&storage);
    storage.close().unwrap();
}

#[test]
fn bounded_persistence_requires_a_replay_closed_wal_window_before_publication() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();
    let metric = "bounded_replay_closed_window";
    let older_labels = vec![Label::new("host", "older-active")];
    let later_labels = vec![Label::new("host", "later-sealed")];

    // The older series owns the first WAL frame but remains active. The later series fills and
    // seals first, so publishing it would require a scalar WAL checkpoint that skips the active
    // head. A bounded pass must leave every segment and pending chunk untouched.
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            older_labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            later_labels.clone(),
            DataPoint::new(10, 10.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            later_labels.clone(),
            DataPoint::new(11, 11.0),
        )])
        .unwrap();
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 1);
    let blocked_by_active = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap();
    assert!(!blocked_by_active.persisted);
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 1);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());

    // Seal the older head. Sequence order remains later-sealed then older-active, while WAL
    // order is the opposite. With one item available, the sequence prefix cannot close the WAL
    // replay interval and must fail explicitly without publishing a partial segment.
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            older_labels.clone(),
            DataPoint::new(2, 2.0),
        )])
        .unwrap();
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 2);
    let dependency_error = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap_err();
    assert!(matches!(
        dependency_error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "sealed chunk persistence",
            item_limit: 1,
            selected_items: 1,
            ..
        }
    ));
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 2);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());

    let foreground = storage.persist_segment_with_outcome().unwrap();
    assert!(foreground.persisted);
    assert_eq!(foreground.chunks, 2);
    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &older_labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_eq!(
        reopened.select(metric, &later_labels, 0, 20).unwrap(),
        vec![DataPoint::new(10, 10.0), DataPoint::new(11, 11.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn bounded_persistence_rejects_a_partial_single_wal_write_without_publication() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();
    let metric = "bounded_single_wal_dependency";
    let left_labels = vec![Label::new("host", "left")];
    let right_labels = vec![Label::new("host", "right")];

    // Both chunks originate from one WAL append. Selecting only one would advance a scalar
    // replay watermark through the other chunk, so bounded persistence must not publish either.
    storage
        .insert_rows(&[
            Row::with_labels(metric, left_labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels(metric, right_labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 2);
    let dependency_error = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap_err();
    assert!(matches!(
        dependency_error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "sealed chunk persistence",
            item_limit: 1,
            selected_items: 1,
            ..
        }
    ));
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 2);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &left_labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert_eq!(
        reopened.select(metric, &right_labels, 0, 10).unwrap(),
        vec![DataPoint::new(2, 2.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn bounded_persistence_defers_a_dependency_that_only_exceeds_the_shared_remainder() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 2;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();

    storage
        .insert_rows(&[
            Row::new("bounded_shared_remainder_a", DataPoint::new(1, 1.0)),
            Row::new("bounded_shared_remainder_b", DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    // Model one of the two configured item slots having already been consumed by active
    // finalization. The same-WAL-frame pair cannot fit the remainder, but does fit the next full
    // pass, so this attempt must be a retryable no-op rather than a fail-fast policy error.
    let deferred = storage
        .persist_segment_background_bounded_with_limits(1, 1024 * 1024)
        .unwrap();
    assert!(!deferred.persisted);
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 2);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());

    let persisted = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap();
    assert!(persisted.persisted);
    assert_eq!(persisted.chunks, 2);
    assert!(storage.chunks.pending_sealed_chunks.read().is_empty());
    storage.close().unwrap();
}

#[test]
fn bounded_partial_persistence_restarts_exactly_across_numeric_and_blob_lanes() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 2;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        Some(wal),
        Some(numeric_lane_path.clone()),
        Some(blob_lane_path.clone()),
        1,
        options,
    )
    .unwrap();
    let metric = "bounded_mixed_lane_restart";
    let numeric_labels = vec![Label::new("kind", "numeric")];
    let blob_labels = vec![Label::new("kind", "blob")];

    // Separate appends give the bounded sequence prefix two replay-closed windows spanning both
    // lane families, followed by one WAL-only numeric suffix.
    for row in [
        Row::with_labels(metric, numeric_labels.clone(), DataPoint::new(1, 1.0)),
        Row::with_labels(metric, blob_labels.clone(), DataPoint::new(2, "two")),
        Row::with_labels(metric, numeric_labels.clone(), DataPoint::new(3, 3.0)),
        Row::with_labels(metric, blob_labels.clone(), DataPoint::new(4, "four")),
        Row::with_labels(metric, numeric_labels.clone(), DataPoint::new(5, 5.0)),
    ] {
        storage.insert_rows(&[row]).unwrap();
    }
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 5);

    for expected_pending in [3, 1] {
        let outcome = storage
            .persist_segment_background_bounded_with_outcome()
            .unwrap();
        assert!(outcome.persisted);
        assert_eq!(outcome.chunks, 2);
        assert_eq!(
            storage.chunks.pending_sealed_chunks.read().len(),
            expected_pending
        );

        let deferred_floor = storage
            .chunks
            .pending_sealed_chunks
            .read()
            .by_wal
            .iter()
            .next()
            .unwrap()
            .wal_lowwater;
        for lane_path in [&numeric_lane_path, &blob_lane_path] {
            let segments = load_segments_for_level(lane_path, 0).unwrap();
            assert!(
                segments
                    .iter()
                    .all(|segment| segment.manifest.wal_highwater < deferred_floor),
                "each visible lane checkpoint must remain before the deferred WAL suffix",
            );
        }
    }
    assert_eq!(
        load_segments_for_level(&numeric_lane_path, 0)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        load_segments_for_level(&blob_lane_path, 0).unwrap().len(),
        2
    );

    storage.abandon_without_close_for_tests().unwrap();
    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &numeric_labels, 0, 10).unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(5, 5.0),
        ]
    );
    assert_eq!(
        reopened.select(metric, &blob_labels, 0, 10).unwrap(),
        vec![DataPoint::new(2, "two"), DataPoint::new(4, "four")]
    );
    reopened.close().unwrap();
}

#[test]
fn bounded_background_persistence_honors_byte_limit_without_losing_foreground_progress() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 8;
    options.maintenance_max_bytes_per_pass = 1;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();

    storage
        .insert_rows(&[Row::new(
            "bounded_partial_persist_bytes",
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 1);

    let bounded_error = storage
        .persist_segment_background_bounded_with_outcome()
        .unwrap_err();
    assert!(matches!(
        bounded_error,
        TsinkError::MaintenanceWorkItemTooLarge {
            operation: "sealed chunk persistence",
            limit: 1,
            required,
        } if required > 1
    ));
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 1);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());

    let foreground = storage.persist_segment_with_outcome().unwrap();
    assert!(foreground.persisted);
    assert_eq!(foreground.chunks, 1);
    assert!(storage.chunks.pending_sealed_chunks.read().is_empty());
    storage.close().unwrap();
}

#[test]
fn bounded_background_pipeline_shares_one_item_allowance_across_flush_and_persist() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();

    storage
        .insert_rows(&[Row::new(
            "bounded_pipeline_shared_allowance",
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.background_flush_pipeline_once().unwrap();
    assert_eq!(storage.chunks.pending_sealed_chunks.read().len(), 1);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());

    storage.background_flush_pipeline_once().unwrap();
    assert!(storage.chunks.pending_sealed_chunks.read().is_empty());
    let segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].manifest.chunk_count, 1);
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_defers_unknown_dirty_reconcile_to_background_refresh() {
    use std::sync::atomic::{AtomicUsize, Ordering};
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
            ChunkStorageOptions {
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_dirty_reconcile",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "flush_dirty_reconcile",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.flush_pipeline_once().unwrap();

    assert!(
        full_scans.load(Ordering::SeqCst) == 0,
        "dirty flush should not block on a full persisted inventory scan",
    );
    assert!(
        storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "flush should leave unknown dirty reconcile work pending for the background worker",
    );
    assert_eq!(
        storage
            .select("flush_dirty_reconcile", &labels, 0, 10)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        storage.list_metrics().unwrap(),
        vec![MetricSeries {
            name: "flush_dirty_reconcile".to_string(),
            labels: labels.clone(),
        }]
    );
    assert_eq!(
        storage
            .select_series(&SeriesSelection::new().with_metric("flush_dirty_reconcile"))
            .unwrap(),
        vec![MetricSeries {
            name: "flush_dirty_reconcile".to_string(),
            labels: labels.clone(),
        }]
    );
    assert!(
        full_scans.load(Ordering::SeqCst) == 0,
        "foreground reads should keep using the last published catalog without running a full scan",
    );

    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut refreshed = false;
    while Instant::now() < deadline {
        if full_scans.load(Ordering::SeqCst) > 0
            && !storage
                .persisted
                .persisted_index_dirty
                .load(Ordering::SeqCst)
        {
            refreshed = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        refreshed,
        "background refresh worker did not run the deferred full persisted inventory scan",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_full_refresh_keeps_concurrent_queries_running() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("dirty_refresh_query_metric", &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        0,
        1,
        &[(1, 1.0), (2, 2.0)],
    );

    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 2));
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let scan_started = Arc::new(AtomicBool::new(false));
    let release_scan = Arc::new(AtomicBool::new(false));
    let scan_count = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let scan_started = Arc::clone(&scan_started);
        let release_scan = Arc::clone(&release_scan);
        let scan_count = Arc::clone(&scan_count);
        move || {
            scan_count.fetch_add(1, Ordering::SeqCst);
            scan_started.store(true, Ordering::SeqCst);
            while !release_scan.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !scan_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        scan_started.load(Ordering::SeqCst),
        "background dirty refresh did not reach the full inventory scan hook",
    );

    let concurrent_storage = Arc::clone(&storage);
    let concurrent_labels = labels.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let concurrent_query = thread::spawn(move || {
        let result =
            concurrent_storage.select("dirty_refresh_query_metric", &concurrent_labels, 0, 10);
        query_tx.send(result).unwrap();
    });

    let concurrent_points = query_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("concurrent query should keep using the last visible catalog");
    assert_eq!(
        concurrent_points.unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    release_scan.store(true, Ordering::SeqCst);

    concurrent_query.join().unwrap();
    let refresh_deadline = Instant::now() + Duration::from_secs(2);
    while storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst)
        && Instant::now() < refresh_deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        scan_count.load(Ordering::SeqCst),
        1,
        "only the refresh owner should run the full inventory scan",
    );
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "successful dirty refresh should clear the dirty flag",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_known_diff_refresh_skips_full_inventory_scans_at_large_segment_counts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("dirty_known_diff_refresh_metric", &labels)
        .unwrap()
        .series_id;

    let mut removed_root = None;
    for segment_id in 1..=256 {
        let root = write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
        if segment_id == 1 {
            removed_root = Some(root);
        }
    }

    let storage = new_raw_numeric_storage(lane_path.clone(), 257);
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();
    storage.persist_series_registry_index().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let registry_catalog_store =
        super::super::registry_catalog::catalog_store_path(&checkpoint_path);
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        257,
        "the native catalog should contain one manifest plus one file per segment"
    );

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let removed_root = removed_root.expect("expected the first root to exist");
    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&removed_root).unwrap();
    let added_root = write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        0,
        257,
        &[(10_000, 10_000.0)],
    );

    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(
            std::iter::once(added_root.clone()),
            std::iter::once(removed_root.clone()),
        );
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "known add/remove refresh should not rescan the full segment tree",
    );
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "known dirty refresh should settle the dirty flag",
    );
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "known dirty refresh should drain the pending root diff",
    );
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&added_root));
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&removed_root));
    let points = storage
        .select("dirty_known_diff_refresh_metric", &labels, 0, 20_000)
        .unwrap();
    assert_eq!(points.len(), 256);
    assert!(!points.contains(&DataPoint::new(1, 1.0)));
    assert!(points.contains(&DataPoint::new(10_000, 10_000.0)));
    assert!(
        !super::super::registry_catalog::catalog_path(&checkpoint_path).exists(),
        "bounded delta publication must retire the stale monolithic compatibility snapshot"
    );
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store)
            .unwrap()
            .count(),
        257,
        "one remove plus one add must keep the native catalog namespace proportional to live segments"
    );
    let inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    let validated = super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&inventory),
    )
    .unwrap()
    .expect("the incrementally updated registry catalog should remain exact");
    assert!(validated.incremental_store);
    assert!(
        validated.series_fingerprint.is_none(),
        "a root delta must conservatively invalidate the aggregate series fingerprint"
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_persisted_refresh_waits_for_inflight_tombstone_visibility_publication() {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 1));

    storage
        .insert_rows(&[
            Row::with_labels(
                "dirty_refresh_delete_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "dirty_refresh_delete_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let (scan_started_tx, scan_started_rx) = mpsc::channel();
    storage.set_full_inventory_scan_hook(move || {
        let _ = scan_started_tx.send(());
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage
            .delete_series(&SeriesSelection::new().with_metric("dirty_refresh_delete_metric"))
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let refresh_storage = Arc::clone(&storage);
    let (refresh_tx, refresh_rx) = mpsc::channel();
    let refresh_thread = thread::spawn(move || {
        let result = refresh_storage.sync_persisted_segments_from_disk_if_dirty();
        refresh_tx.send(result).unwrap();
    });

    scan_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("refresh did not reach the full inventory scan hook");
    let refresh_blocked = refresh_rx.recv_timeout(Duration::from_millis(200)).is_err();

    delete_release_tx.send(()).unwrap();
    assert!(
        refresh_blocked,
        "dirty refresh should wait for the in-flight tombstone visibility publication",
    );

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert!(refresh_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    refresh_thread.join().unwrap();

    if storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst)
    {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
    }

    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a follow-up refresh should reconcile any invalidated scan after delete publication",
    );
    assert!(storage
        .select("dirty_refresh_delete_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());

    storage.clear_full_inventory_scan_hook();
    storage.clear_tombstone_post_swap_pre_visibility_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_persisted_refresh_skips_stale_scanned_state_after_delete_visibility_change() {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 1));

    storage
        .insert_rows(&[
            Row::with_labels(
                "dirty_refresh_stale_visibility_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "dirty_refresh_stale_visibility_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let (scan_entered_tx, scan_entered_rx) = mpsc::channel();
    let (release_scan_tx, release_scan_rx) = mpsc::channel();
    let release_scan_rx = Arc::new(Mutex::new(release_scan_rx));
    storage.set_full_inventory_scan_hook({
        let release_scan_rx = Arc::clone(&release_scan_rx);
        move || {
            scan_entered_tx.send(()).unwrap();
            release_scan_rx.lock().unwrap().recv().unwrap();
        }
    });

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let refresh_storage = Arc::clone(&storage);
    let (refresh_tx, refresh_rx) = mpsc::channel();
    let refresh_thread = thread::spawn(move || {
        let result = refresh_storage.sync_persisted_segments_from_disk_if_dirty();
        refresh_tx.send(result).unwrap();
    });

    scan_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("refresh did not reach the full inventory scan hook");

    let delete_result = storage
        .delete_series(&SeriesSelection::new().with_metric("dirty_refresh_stale_visibility_metric"))
        .unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);

    release_scan_tx.send(()).unwrap();
    assert!(refresh_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    refresh_thread.join().unwrap();

    assert!(
        storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a scanned refresh prepared before a newer visibility publication should stay dirty for retry",
    );
    assert!(
        storage
            .select("dirty_refresh_stale_visibility_metric", &labels, 0, 10)
            .unwrap()
            .is_empty(),
        "stale scanned refresh state must not resurrect data hidden by a newer tombstone publication",
    );

    storage.clear_full_inventory_scan_hook();
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a follow-up refresh should clear the dirty flag once it snapshots the newer visibility generation",
    );
    assert!(storage
        .select("dirty_refresh_stale_visibility_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());

    storage.close().unwrap();
}

#[test]
fn finite_remote_catalog_refresh_refuses_v2_only_state_without_changing_visibility() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("remote_refresh_query_metric", &labels)
        .unwrap()
        .series_id;
    let segment_root =
        write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let legacy_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_segment_catalog(
        &super::super::tiering::shared_segment_catalog_path(&tiered_storage),
        &legacy_inventory,
    )
    .unwrap();

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        2,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ComputeOnly,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
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
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(tiered_storage),
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(
            crate::engine::segment::load_segment_indexes_from_dirs_with_series(
                vec![segment_root],
                true,
            )
            .unwrap(),
            false,
        )
        .unwrap();
    storage.mark_remote_catalog_refresh_success();
    std::thread::sleep(Duration::from_millis(5));

    let scan_count = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let scan_count = Arc::clone(&scan_count);
        move || {
            scan_count.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    let concurrent_points = storage
        .select("remote_refresh_query_metric", &labels, 0, 10)
        .unwrap();
    assert_eq!(
        concurrent_points,
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_eq!(
        scan_count.load(Ordering::SeqCst),
        0,
        "finite remote refresh must not scan object-store roots or fall back to v2",
    );
    let failed = storage.observability_snapshot().remote;
    assert_eq!(failed.catalog_refresh_errors_total, 1);
    assert_eq!(failed.consecutive_refresh_failures, 1);
    assert!(failed.backoff_active);
    assert!(failed
        .last_refresh_error
        .as_deref()
        .is_some_and(|error| error.contains("authoritative v3 pointer")));

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert_eq!(
        storage
            .observability_snapshot()
            .remote
            .catalog_refresh_errors_total,
        1,
        "the structured refusal should enter backoff instead of retrying on every query",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn remote_catalog_refresh_uses_shared_catalog_without_full_inventory_scans() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("remote_catalog_incremental_metric", &labels)
        .unwrap()
        .series_id;

    let mut initial_roots = Vec::new();
    for segment_id in 1..=256 {
        initial_roots.push(write_numeric_segment_to_path(
            &hot_lane,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        ));
    }

    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let initial_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &initial_inventory,
        None,
    )
    .unwrap();

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        257,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            max_future_skew_window: None,
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ComputeOnly,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
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
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(tiered_storage.clone()),
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(
            crate::engine::segment::load_segment_indexes_from_dirs_with_series(
                initial_roots.clone(),
                true,
            )
            .unwrap(),
            false,
        )
        .unwrap();
    storage.mark_remote_catalog_refresh_success();
    std::thread::sleep(Duration::from_millis(5));

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let removed_root = initial_roots[0].clone();
    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&removed_root).unwrap();
    let added_root = write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        257,
        &[(10_000, 10_000.0)],
    );
    let updated_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &updated_inventory,
        None,
    )
    .unwrap();

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "remote refresh should consume the shared catalog instead of rescanning the object-store tree",
    );
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&added_root));
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&removed_root));
    let points = storage
        .select("remote_catalog_incremental_metric", &labels, 0, 20_000)
        .unwrap();
    assert_eq!(points.len(), 256);
    assert!(!points.contains(&DataPoint::new(1, 1.0)));
    assert!(points.contains(&DataPoint::new(10_000, 10_000.0)));

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn finite_remote_catalog_refresh_uses_exact_item_pages_and_terminal_probe() {
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "exact")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_exact_pages", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=2 {
        write_numeric_segment_to_path(
            &hot_lane,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory,
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage, 3, 1);

    for _ in 0..2 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        let memory = storage.memory_observability_snapshot();
        assert!(
            memory.remote_catalog_staging_bytes > 0,
            "decoded catalog entries and the resumable reader must stay admitted between wakes",
        );
        assert!(memory.accounted_bytes >= memory.remote_catalog_staging_bytes);
        assert!(
            storage
                .persisted
                .persisted_index
                .read()
                .segments_by_root
                .is_empty(),
            "catalog entries must remain private until the full generation validates",
        );
        assert_eq!(
            storage
                .observability_snapshot()
                .remote
                .catalog_refreshes_total,
            0,
            "a validation page is not a completed refresh",
        );
    }

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        1,
        "one exact item page should publish one addition",
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .remote
            .catalog_refreshes_total,
        0,
    );

    for _ in 0..3 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert_eq!(
            storage
                .observability_snapshot()
                .remote
                .catalog_refreshes_total,
            0,
            "exact-boundary addition/removal pages must not report success before the terminal probe",
        );
    }
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    let completed = storage.observability_snapshot().remote;
    assert_eq!(completed.catalog_refreshes_total, 1);
    assert_eq!(completed.catalog_refresh_errors_total, 0);
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "terminal completion must release the retained catalog cursor and map",
    );
    assert_eq!(
        storage
            .select("remote_catalog_exact_pages", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    storage.close().unwrap();
}

#[test]
fn finite_remote_catalog_apply_byte_ceiling_has_exact_boundary_and_no_publication_below_it() {
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "apply-byte-boundary")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_apply_byte_boundary", &labels)
        .unwrap()
        .series_id;
    let root =
        write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory,
        None,
    )
    .unwrap();

    let probe = finite_compute_only_remote_storage(tiered_storage.clone(), 2, 1);
    let (apply_required, apply_staging) = probe
        .remote_catalog_add_apply_limits_for_test(&inventory.entries()[0])
        .unwrap();
    assert!(apply_required > 1);
    assert!(apply_staging > 0);
    probe.close().unwrap();

    let below = finite_compute_only_remote_storage_with_limits(
        tiered_storage.clone(),
        2,
        1,
        apply_required - 1,
    );
    let rejection = (0..16)
        .find_map(|_| match below.refresh_remote_catalog_bounded() {
            Ok(completed) => {
                assert!(!completed);
                None
            }
            Err(err) => Some(err),
        })
        .expect("N-1 maintenance bytes should reject the apply page");
    assert!(matches!(
        rejection,
        TsinkError::MaintenanceWorkItemTooLarge {
            operation: "finite remote segment catalog apply",
            limit,
            required,
        } if limit == apply_required - 1 && required == apply_required
    ));
    assert!(!below
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        below
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "a pre-publication maintenance rejection must discard the retained cycle lease",
    );
    below.close().unwrap();

    let exact =
        finite_compute_only_remote_storage_with_limits(tiered_storage, 2, 1, apply_required);
    for _ in 0..16 {
        if exact.refresh_remote_catalog_bounded().unwrap() {
            break;
        }
    }
    assert!(exact
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        exact
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "terminal success at the exact threshold must release the cycle lease",
    );
    exact.close().unwrap();
}

#[test]
fn finite_remote_catalog_removal_byte_ceiling_has_exact_boundary() {
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "r".repeat(8 * 1024))];
    let series_id = registry
        .resolve_or_insert("remote_catalog_remove_byte_boundary", &labels)
        .unwrap()
        .series_id;
    let root = write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &super::super::tiering::SegmentInventory::from_entries(Vec::new()),
        None,
    )
    .unwrap();
    let load_visible = |storage: &ChunkStorage| {
        storage
            .apply_loaded_segment_indexes(
                crate::engine::segment::load_segment_indexes_from_dirs_with_series(
                    vec![root.clone()],
                    true,
                )
                .unwrap(),
                false,
            )
            .unwrap();
    };

    let probe = finite_compute_only_remote_storage(tiered_storage.clone(), 2, 1);
    load_visible(&probe);
    let (remove_required, remove_staging) = probe
        .remote_catalog_remove_apply_limits_for_test(&root)
        .unwrap();
    assert!(remove_required > 1);
    assert!(remove_staging > 0);
    probe.close().unwrap();

    let below = finite_compute_only_remote_storage_with_limits(
        tiered_storage.clone(),
        2,
        1,
        remove_required - 1,
    );
    load_visible(&below);
    let rejection = (0..16)
        .find_map(|_| match below.refresh_remote_catalog_bounded() {
            Ok(completed) => {
                assert!(!completed);
                None
            }
            Err(err) => Some(err),
        })
        .expect("N-1 maintenance bytes should reject the removal page");
    assert!(matches!(
        rejection,
        TsinkError::MaintenanceWorkItemTooLarge {
            operation: "finite remote segment catalog apply",
            limit,
            required,
        } if limit == remove_required - 1 && required == remove_required
    ));
    assert!(below
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        below
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
    );
    below.close().unwrap();

    let exact =
        finite_compute_only_remote_storage_with_limits(tiered_storage, 2, 1, remove_required);
    load_visible(&exact);
    for _ in 0..16 {
        if exact.refresh_remote_catalog_bounded().unwrap() {
            break;
        }
    }
    assert!(!exact
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        exact
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
    );
    exact.close().unwrap();
}

#[test]
fn finite_remote_catalog_reader_memory_admission_has_exact_boundaries_and_close_release() {
    use std::sync::atomic::Ordering;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "memory")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_memory", &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory,
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage, 2, 1);
    let base = storage.memory_used_value();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0
    );

    storage
        .memory
        .budget_bytes
        .store(u64::try_from(base).unwrap(), Ordering::Release);
    let construction_required = match storage.refresh_remote_catalog_bounded().unwrap_err() {
        TsinkError::MemoryBudgetExceeded { required, .. } => required,
        err => panic!("expected cycle construction admission failure, got {err:?}"),
    };
    assert!(construction_required > base);
    storage.memory.budget_bytes.store(
        u64::try_from(construction_required - 1).unwrap(),
        Ordering::Release,
    );
    assert!(matches!(
        storage.refresh_remote_catalog_bounded(),
        Err(TsinkError::MemoryBudgetExceeded { required, .. })
            if required == construction_required
    ));

    storage.memory.budget_bytes.store(
        u64::try_from(construction_required).unwrap(),
        Ordering::Release,
    );
    let reader_required = match storage.refresh_remote_catalog_bounded().unwrap_err() {
        TsinkError::MemoryBudgetExceeded { required, .. } => required,
        err => panic!("expected generation page admission failure, got {err:?}"),
    };
    assert!(reader_required > construction_required);
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "a failed generation-page admission must discard its cycle lease",
    );

    storage.memory.budget_bytes.store(
        u64::try_from(reader_required - 1).unwrap(),
        Ordering::Release,
    );
    assert!(matches!(
        storage.refresh_remote_catalog_bounded(),
        Err(TsinkError::MemoryBudgetExceeded { required, .. }) if required == reader_required
    ));
    storage
        .memory
        .budget_bytes
        .store(u64::try_from(reader_required).unwrap(), Ordering::Release);
    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    assert!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes
            > 0,
        "the exact budget must admit and retain the first decoded page",
    );

    storage.close().unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "close must release an incomplete catalog continuation",
    );
}

#[test]
fn finite_remote_catalog_addition_is_memory_admitted_before_visibility_mutation() {
    use std::sync::atomic::Ordering;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "apply-memory")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_apply_memory", &labels)
        .unwrap()
        .series_id;
    let root =
        write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory,
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage, 2, 1);
    storage.refresh_memory_usage();
    let (_, apply_staging) = storage
        .remote_catalog_add_apply_limits_for_test(&inventory.entries()[0])
        .unwrap();

    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    let staged = storage.memory_observability_snapshot();
    assert!(staged.remote_catalog_staging_bytes > 0);
    storage.memory.budget_bytes.store(
        u64::try_from(staged.accounted_bytes).unwrap(),
        Ordering::Release,
    );
    match storage.refresh_remote_catalog_bounded().unwrap_err() {
        TsinkError::MemoryBudgetExceeded { .. } => {}
        err => panic!("expected remote addition admission failure, got {err:?}"),
    }
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
    );

    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    let restaged = storage.memory_observability_snapshot();
    let modeled_apply_floor = restaged.accounted_bytes.saturating_add(apply_staging);
    assert!(modeled_apply_floor > restaged.accounted_bytes);
    storage.memory.budget_bytes.store(
        u64::try_from(modeled_apply_floor - 1).unwrap(),
        Ordering::Release,
    );
    let apply_required = match storage.refresh_remote_catalog_bounded() {
        Err(TsinkError::MemoryBudgetExceeded { required, .. }) => required,
        result => panic!(
            "remote addition admission returned {result:?}; expected at least modeled floor {modeled_apply_floor}"
        ),
    };
    assert_eq!(
        apply_required, modeled_apply_floor,
        "the complete apply peak should be admitted in one exact reservation step",
    );
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));

    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    storage.memory.budget_bytes.store(
        u64::try_from(apply_required - 1).unwrap(),
        Ordering::Release,
    );
    assert!(matches!(
        storage.refresh_remote_catalog_bounded(),
        Err(TsinkError::MemoryBudgetExceeded { required, .. }) if required == apply_required
    ));
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));

    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    storage
        .memory
        .budget_bytes
        .store(u64::try_from(apply_required).unwrap(), Ordering::Release);
    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    let memory = storage.memory_observability_snapshot();
    assert!(
        memory.accounted_bytes <= apply_required,
        "admitted publication must not leave the modeled total over budget",
    );
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    storage
        .memory
        .budget_bytes
        .store(u64::MAX, Ordering::Release);
    for _ in 0..8 {
        if storage.refresh_remote_catalog_bounded().unwrap() {
            break;
        }
    }
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "terminal completion must release the exact-threshold cycle lease",
    );
    storage.close().unwrap();
}

#[test]
fn finite_remote_catalog_apply_preflight_failure_has_no_publication_or_residual_lease() {
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "apply-preflight-failure")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_apply_preflight_failure", &labels)
        .unwrap()
        .series_id;
    let root = write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory,
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage, 2, 1);

    assert!(!storage.refresh_remote_catalog_bounded().unwrap());
    assert!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes
            > 0,
        "the validated catalog page should be retained before apply",
    );
    std::fs::remove_file(root.join("series.bin")).unwrap();

    let err = (0..8)
        .find_map(|_| storage.refresh_remote_catalog_bounded().err())
        .expect("the missing pinned segment metadata must fail apply preflight");
    assert!(
        matches!(
            err,
            TsinkError::Io(_) | TsinkError::IoWithPath { .. } | TsinkError::DataCorruption(_)
        ),
        "unexpected apply-preflight error: {err:?}",
    );
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "preflight failure must release inspection, apply, and retained-cycle reservations",
    );
    storage.close().unwrap();
}

#[test]
fn concurrent_catalog_and_write_reservations_share_one_global_admission_limit() {
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc, Barrier};

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(new_raw_numeric_storage(
        temp_dir.path().join(NUMERIC_LANE_ROOT),
        1,
    ));
    let base = storage.memory_used_value();
    const RESERVATION_BYTES: usize = 32 * 1024;
    storage.memory.budget_bytes.store(
        u64::try_from(base + RESERVATION_BYTES).unwrap(),
        Ordering::Release,
    );
    let start = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let (result_tx, result_rx) = mpsc::channel();
    let catalog_worker = {
        let storage = Arc::clone(&storage);
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let result_tx = result_tx.clone();
        std::thread::spawn(move || {
            start.wait();
            let reservation = storage.remote_catalog_memory_reservation(RESERVATION_BYTES);
            result_tx.send(reservation.is_ok()).unwrap();
            release.wait();
            drop(reservation);
        })
    };
    let write_worker = {
        let storage = Arc::clone(&storage);
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let result_tx = result_tx.clone();
        std::thread::spawn(move || {
            start.wait();
            let reservation = storage.reserve_write_transient_memory(RESERVATION_BYTES);
            result_tx.send(reservation.is_ok()).unwrap();
            release.wait();
            drop(reservation);
        })
    };
    drop(result_tx);
    start.wait();
    let admitted = [result_rx.recv().unwrap(), result_rx.recv().unwrap()];
    assert_eq!(admitted.into_iter().filter(|admitted| *admitted).count(), 1);
    let active = storage.memory_observability_snapshot();
    assert_eq!(
        active
            .remote_catalog_staging_bytes
            .saturating_add(active.write_transient_bytes),
        RESERVATION_BYTES,
    );
    assert!(matches!(
        storage.reserve_write_transient_memory(1),
        Err(TsinkError::MemoryBudgetExceeded { .. }),
    ));
    let mut tombstone_reservation = storage.tombstone_memory_reservation();
    assert!(matches!(
        tombstone_reservation.ensure(1),
        Err(TsinkError::MemoryBudgetExceeded { .. }),
    ));
    drop(tombstone_reservation);
    release.wait();
    catalog_worker.join().unwrap();
    write_worker.join().unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
    );
    let released_base = storage.memory_used_value();
    storage.memory.budget_bytes.store(
        u64::try_from(released_base + RESERVATION_BYTES).unwrap(),
        Ordering::Release,
    );
    drop(
        storage
            .remote_catalog_memory_reservation(RESERVATION_BYTES)
            .unwrap(),
    );
    storage.close().unwrap();
}

#[test]
fn finite_remote_catalog_refresh_restarts_on_pointer_change_during_scan_and_apply() {
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "swap")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_pointer_swap", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=5 {
        write_numeric_segment_to_path(
            &hot_lane,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let complete_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    let inventory_for = |segment_ids: &[u64]| {
        super::super::tiering::SegmentInventory::from_entries(
            complete_inventory
                .entries()
                .iter()
                .filter(|entry| segment_ids.contains(&entry.manifest.segment_id))
                .cloned()
                .collect(),
        )
    };
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory_for(&[1, 2]),
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage.clone(), 6, 1);

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    let first_generation_staging = storage
        .memory_observability_snapshot()
        .remote_catalog_staging_bytes;
    assert!(first_generation_staging > 0);
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .is_empty());
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory_for(&[3, 4]),
        None,
    )
    .unwrap();

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        first_generation_staging,
        "pointer churn must replace, not accumulate, the retained generation lease",
    );
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .is_empty());
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        1,
        "the second pointer should have reached its first bounded addition",
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .remote
            .catalog_refreshes_total,
        0,
        "a pointer swap during apply must not report the older generation as complete",
    );

    let final_pointer = super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory_for(&[5]),
        None,
    )
    .unwrap();
    for _ in 0..10 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        if storage
            .observability_snapshot()
            .remote
            .catalog_refreshes_total
            == 1
        {
            break;
        }
    }
    let visible = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(visible.len(), 1);
    assert!(visible[0].ends_with("seg-0000000000000005"));
    assert_eq!(
        super::super::tiering::require_shared_segment_catalog_pointer(&tiered_storage)
            .unwrap()
            .generation,
        final_pointer.generation
    );
    let completed = storage.observability_snapshot().remote;
    assert_eq!(completed.catalog_refreshes_total, 1);
    assert_eq!(completed.catalog_refresh_errors_total, 0);
    storage.close().unwrap();
}

#[test]
fn repeated_remote_pointer_churn_defers_success_until_publication_quiets() {
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "churn")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_pointer_churn", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=4 {
        write_numeric_segment_to_path(
            &hot_lane,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let complete_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    let inventory_for = |segment_id| {
        super::super::tiering::SegmentInventory::from_entries(
            complete_inventory
                .entries()
                .iter()
                .filter(|entry| entry.manifest.segment_id == segment_id)
                .cloned()
                .collect(),
        )
    };
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory_for(1),
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage.clone(), 5, 1);

    for next_segment_id in 2..=4 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert_eq!(
            storage
                .observability_snapshot()
                .remote
                .catalog_refreshes_total,
            0,
            "a generation replaced before its apply phase must not report success",
        );
        assert!(storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .is_empty());
        super::super::tiering::persist_shared_segment_catalog_budgeted(
            &tiered_storage,
            &inventory_for(next_segment_id),
            None,
        )
        .unwrap();
    }

    for _ in 0..8 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        if storage
            .observability_snapshot()
            .remote
            .catalog_refreshes_total
            == 1
        {
            break;
        }
    }
    let completed = storage.observability_snapshot().remote;
    assert_eq!(completed.catalog_refreshes_total, 1);
    assert_eq!(completed.catalog_refresh_errors_total, 0);
    let visible = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(visible.len(), 1);
    assert!(visible[0].ends_with("seg-0000000000000004"));
    storage.close().unwrap();
}

#[test]
fn corrupt_v3_generation_keeps_last_remote_visibility_and_never_scans_tier_roots() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "corrupt")];
    let series_id = registry
        .resolve_or_insert("remote_catalog_corruption", &labels)
        .unwrap()
        .series_id;
    let first_root =
        write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0)]);
    write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 2, &[(2, 2.0)]);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let complete_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    let inventory_for = |segment_id| {
        super::super::tiering::SegmentInventory::from_entries(
            complete_inventory
                .entries()
                .iter()
                .filter(|entry| entry.manifest.segment_id == segment_id)
                .cloned()
                .collect(),
        )
    };
    super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory_for(1),
        None,
    )
    .unwrap();
    let storage = finite_compute_only_remote_storage(tiered_storage.clone(), 3, 1_024);
    storage
        .apply_loaded_segment_indexes(
            crate::engine::segment::load_segment_indexes_from_dirs_with_series(
                vec![first_root.clone()],
                true,
            )
            .unwrap(),
            false,
        )
        .unwrap();
    storage.mark_remote_catalog_refresh_success();
    std::thread::sleep(Duration::from_millis(5));

    let corrupt_pointer = super::super::tiering::persist_shared_segment_catalog_budgeted(
        &tiered_storage,
        &inventory_for(2),
        None,
    )
    .unwrap();
    let corrupt_path = super::super::tiering::shared_segment_catalog_generation_path(
        &tiered_storage,
        corrupt_pointer.generation,
    );
    let mut corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
    *corrupt_bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&corrupt_path, corrupt_bytes).unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert_eq!(
        storage
            .memory_observability_snapshot()
            .remote_catalog_staging_bytes,
        0,
        "a corrupt generation must release reader and staged-map memory before backoff",
    );

    let visible = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(visible, vec![first_root]);
    assert_eq!(
        storage
            .select("remote_catalog_corruption", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert_eq!(full_scans.load(Ordering::SeqCst), 0);
    let failed = storage.observability_snapshot().remote;
    assert_eq!(failed.catalog_refresh_errors_total, 1);
    assert_eq!(failed.consecutive_refresh_failures, 1);
    assert!(failed.backoff_active);
    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_writes_incremental_registry_sidecar_without_rewriting_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels = vec![Label::new("host", "delta")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "flush_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "flush_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_before = std::fs::read(&checkpoint_path).unwrap();
    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_registry_delta",
                delta_labels.clone(),
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "flush_registry_delta",
                delta_labels.clone(),
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(std::fs::read(&checkpoint_path).unwrap(), checkpoint_before);
    assert!(!delta_path.exists());
    assert!(delta_dir.exists());
    assert_eq!(incremental_segment_paths(&checkpoint_path).len(), 1);
    let delta_registry = SeriesRegistry::load_incremental_state(&checkpoint_path)
        .unwrap()
        .expect("incremental sidecar should load");
    assert_eq!(delta_registry.delta_series_count, 1);
    assert_eq!(delta_registry.registry.series_count(), 1);
    assert!(delta_registry
        .registry
        .resolve_existing("flush_registry_delta", &delta_labels)
        .is_some());

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("checkpoint plus sidecar should load");
    assert!(loaded_registry
        .registry
        .resolve_existing("flush_registry_checkpoint", &base_labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("flush_registry_delta", &delta_labels)
        .is_some());
    let visible_inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());
}

#[test]
fn dirty_reconcile_writes_incremental_registry_sidecar_without_rewriting_checkpoint() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels = vec![Label::new("host", "reconcile")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "reconcile_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "reconcile_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_before = std::fs::read(&checkpoint_path).unwrap();
    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    let delta_registry = SeriesRegistry::new();
    let delta_series_id = delta_registry
        .register_series_with_id(100, "reconcile_registry_delta", &delta_labels)
        .unwrap()
        .series_id;
    let mut chunks = HashMap::new();
    chunks.insert(
        delta_series_id,
        vec![make_persisted_numeric_chunk(delta_series_id, &[(10, 10.0)])],
    );
    SegmentWriter::new(&lane_path, 0, 100)
        .unwrap()
        .write_segment(&delta_registry, &chunks)
        .unwrap();

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert_eq!(std::fs::read(&checkpoint_path).unwrap(), checkpoint_before);
    assert!(!delta_path.exists());
    assert!(delta_dir.exists());
    assert_eq!(incremental_segment_paths(&checkpoint_path).len(), 1);
    let delta_registry = SeriesRegistry::load_incremental_state(&checkpoint_path)
        .unwrap()
        .expect("incremental sidecar should load");
    assert_eq!(delta_registry.delta_series_count, 1);
    assert_eq!(delta_registry.registry.series_count(), 1);
    assert!(delta_registry
        .registry
        .resolve_existing("reconcile_registry_delta", &delta_labels)
        .is_some());

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("checkpoint plus sidecar should load");
    assert!(loaded_registry
        .registry
        .resolve_existing("reconcile_registry_checkpoint", &base_labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("reconcile_registry_delta", &delta_labels)
        .is_some());
    let visible_inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());
}

#[test]
fn unknown_dirty_catalog_refresh_pages_without_publishing_a_false_complete_inventory() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = bounded_catalog_refresh_storage(&lane_path, 4, 1, 256 * 1024 * 1024);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "paged")];
    let series_id = registry
        .resolve_or_insert("paged_unknown_dirty_catalog", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=3 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }

    let scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let scans = Arc::clone(&scans);
        move || {
            scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .is_empty(),
        "opening the first scan level must not claim that an empty partial page is complete"
    );

    let mut calls = 1usize;
    while storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst)
    {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        calls += 1;
        assert!(
            calls < 64,
            "bounded catalog cursor failed to reach a terminal page"
        );
    }

    assert!(
        calls >= 12,
        "one-item pages should require explicit level, entry, manifest, apply, and terminal passes"
    );
    assert_eq!(scans.load(Ordering::SeqCst), 1);
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        3
    );
    assert_eq!(
        storage
            .select("paged_unknown_dirty_catalog", &labels, 0, 10)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
        ]
    );
    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn unknown_dirty_catalog_failed_page_retries_its_exact_root_delta() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = bounded_catalog_refresh_storage(&lane_path, 2, 1_024, 256 * 1024 * 1024);
    storage.persist_series_registry_index().unwrap();
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "retry")];
    let series_id = registry
        .resolve_or_insert("paged_unknown_dirty_retry", &labels)
        .unwrap()
        .series_id;
    let root = write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 1, &[(1, 1.0)]);

    let fail_once = Arc::new(AtomicBool::new(true));
    storage.set_catalog_transition_post_index_mutation_hook({
        let fail_once = Arc::clone(&fail_once);
        move || {
            if fail_once.swap(false, Ordering::SeqCst) {
                Err(TsinkError::Other(
                    "injected bounded catalog page failure".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    });
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let err = storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("injected bounded catalog page failure"));
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&root));
    assert_eq!(
        storage.observability_snapshot().flush.hot_segments_visible,
        1,
        "a post-index-mutation failure must publish the exact visible-root counter delta"
    );
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));

    for _ in 0..8 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        if !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst)
        {
            break;
        }
    }
    assert!(!storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert_eq!(
        storage
            .select("paged_unknown_dirty_retry", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert_eq!(
        storage.observability_snapshot().flush.hot_segments_visible,
        1,
        "retry must not double-apply a counter delta already reconciled on failure"
    );
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&inventory),
    )
    .unwrap()
    .is_some());

    storage.clear_catalog_transition_post_index_mutation_hook();
    storage.close().unwrap();
}

#[test]
fn unknown_dirty_catalog_restarts_after_a_retained_root_disappears() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = bounded_catalog_refresh_storage(&lane_path, 3, 1, 256 * 1024 * 1024);
    storage.persist_series_registry_index().unwrap();
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "disappeared")];
    let series_id = registry
        .resolve_or_insert("paged_unknown_dirty_disappearance", &labels)
        .unwrap()
        .series_id;
    let disappeared_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 1, &[(1, 1.0)]);
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    for _ in 0..16 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        if storage.bounded_unknown_dirty_catalog_retains_root_for_test(&disappeared_root) {
            break;
        }
    }
    assert!(
        storage.bounded_unknown_dirty_catalog_retains_root_for_test(&disappeared_root),
        "test did not reach the retained-snapshot boundary"
    );
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&disappeared_root));

    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&disappeared_root).unwrap();
    let replacement_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 2, &[(2, 2.0)]);

    for _ in 0..64 {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        if !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst)
        {
            break;
        }
    }

    assert!(!storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    let persisted_index = storage.persisted.persisted_index.read();
    assert!(!persisted_index
        .segments_by_root
        .contains_key(&disappeared_root));
    assert!(persisted_index
        .segments_by_root
        .contains_key(&replacement_root));
    drop(persisted_index);
    assert_eq!(
        storage
            .select("paged_unknown_dirty_disappearance", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(2, 2.0)]
    );

    storage.close().unwrap();
}

#[test]
fn unknown_dirty_catalog_cursor_is_discarded_on_restart_and_startup_recovers_exactly() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("host", "restart")];
    let series_id = registry
        .resolve_or_insert("paged_unknown_dirty_restart", &labels)
        .unwrap()
        .series_id;
    let storage = bounded_catalog_refresh_storage(&lane_path, 3, 1, 256 * 1024 * 1024);
    for segment_id in 1..=2 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .is_empty());
    drop(storage);

    let reopened = open_raw_numeric_storage_from_data_path(temp_dir.path());
    assert_eq!(
        reopened
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        2
    );
    assert_eq!(
        reopened
            .select("paged_unknown_dirty_restart", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn unknown_dirty_catalog_rejects_an_oversized_scan_dependency() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = bounded_catalog_refresh_storage(&lane_path, 1, 1, 4_095);
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let err = storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::MaintenanceWorkItemTooLarge {
            operation: "unknown-dirty persisted catalog scan",
            limit: 4_095,
            required: 4_096,
        }
    ));
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    drop(storage);
}

#[test]
fn repeated_small_flushes_compact_active_registry_generation_and_restart_recovers() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels_a = vec![Label::new("host", "delta-a")];
    let delta_labels_b = vec![Label::new("host", "delta-b")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "repeated_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "repeated_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_before = std::fs::read(&checkpoint_path).unwrap();
    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    storage
        .insert_rows(&[
            Row::with_labels(
                "repeated_registry_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "repeated_registry_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let first_segment_paths = incremental_segment_paths(&checkpoint_path);
    assert_eq!(first_segment_paths.len(), 1);
    let first_segment_bytes = std::fs::read(&first_segment_paths[0]).unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "repeated_registry_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(5, 5.0),
            ),
            Row::with_labels(
                "repeated_registry_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(6, 6.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(std::fs::read(&checkpoint_path).unwrap(), checkpoint_before);
    let segment_paths = incremental_segment_paths(&checkpoint_path);
    assert_eq!(segment_paths.len(), 1);
    assert_ne!(
        std::fs::read(&segment_paths[0]).unwrap(),
        first_segment_bytes,
        "the bounded active generation should be atomically replaced with the merged registry"
    );

    let incremental = SeriesRegistry::load_incremental_state(&checkpoint_path)
        .unwrap()
        .expect("incremental segments should load");
    assert_eq!(incremental.delta_series_count, 2);
    assert!(incremental
        .registry
        .resolve_existing("repeated_registry_delta_a", &delta_labels_a)
        .is_some());
    assert!(incremental
        .registry
        .resolve_existing("repeated_registry_delta_b", &delta_labels_b)
        .is_some());

    let reopened = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    assert_eq!(
        reopened
            .select("repeated_registry_checkpoint", &base_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_eq!(
        reopened
            .select("repeated_registry_delta_a", &delta_labels_a, 0, 10)
            .unwrap(),
        vec![DataPoint::new(3, 3.0), DataPoint::new(4, 4.0)]
    );
    assert_eq!(
        reopened
            .select("repeated_registry_delta_b", &delta_labels_b, 0, 10)
            .unwrap(),
        vec![DataPoint::new(5, 5.0), DataPoint::new(6, 6.0)]
    );
    reopened.close().unwrap();
    storage.close().unwrap();
}

#[test]
fn checkpoint_rollover_compacts_incremental_registry_segments() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels_a = vec![Label::new("host", "delta-a")];
    let delta_labels_b = vec![Label::new("host", "delta-b")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "checkpoint_rollover_base",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "checkpoint_rollover_base",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    storage
        .insert_rows(&[
            Row::with_labels(
                "checkpoint_rollover_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "checkpoint_rollover_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let checkpoint_before_rollover = std::fs::read(&checkpoint_path).unwrap();
    assert_eq!(incremental_segment_paths(&checkpoint_path).len(), 1);

    storage
        .catalog
        .delta_series_count
        .store(u64::MAX, Ordering::SeqCst);
    storage
        .insert_rows(&[
            Row::with_labels(
                "checkpoint_rollover_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(5, 5.0),
            ),
            Row::with_labels(
                "checkpoint_rollover_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(6, 6.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        checkpoint_before_rollover
    );
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("checkpoint should include compacted incremental state");
    assert_eq!(loaded_registry.delta_series_count, 0);
    assert!(loaded_registry
        .registry
        .resolve_existing("checkpoint_rollover_base", &base_labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("checkpoint_rollover_delta_a", &delta_labels_a)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("checkpoint_rollover_delta_b", &delta_labels_b)
        .is_some());

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_completes_while_another_writer_permit_is_held() {
    use std::sync::mpsc;
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
            memory_budget_bytes: u64::MAX,
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
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "flush_busy_writer",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("flush_busy_writer", &labels)
        .unwrap()
        .series_id;

    let active_before = storage
        .active_shard(series_id)
        .read()
        .get(&series_id)
        .map_or(0, |state| state.point_count());
    assert_eq!(active_before, 1);

    let held_permit = storage.runtime.write_limiter.acquire();
    let (flush_tx, flush_rx) = mpsc::channel();
    thread::scope(|scope| {
        scope.spawn(|| {
            flush_tx.send(storage.flush_pipeline_once()).unwrap();
        });
        let flush_result = flush_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("flush should not wait for an unrelated held writer permit");
        flush_result.unwrap();
        drop(held_permit);
    });

    let active_after = storage
        .active_shard(series_id)
        .read()
        .get(&series_id)
        .map_or(0, |state| state.point_count());
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
        "flush pipeline should persist while another writer permit is held"
    );

    storage.close().unwrap();
}

#[test]
fn flush_stage_keeps_staged_segments_and_wal_behind_visibility_publish_boundary() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[Row::with_labels(
            "flush_visibility_boundary",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "flush_visibility_boundary",
            labels,
            DataPoint::new(2, 2.0),
        )])
        .unwrap();

    let (staged_tx, staged_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_persist_post_publish_hook({
        let release_rx = Arc::clone(&release_rx);
        move |roots| {
            staged_tx.send(roots.to_vec()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        flush_tx.send(flush_storage.flush_pipeline_once()).unwrap();
    });

    // Invariant: staged flush roots and WAL bytes stay behind the visibility publish step.
    let staged_roots = staged_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not reach the staged segment hook");
    assert_eq!(staged_roots.len(), 1);
    let staged_root = staged_roots[0].clone();
    assert!(staged_root.exists());
    load_segment_index(&staged_root).expect("staged segment should already be readable");
    assert!(
        !storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(&staged_root),
        "staged flush roots must stay hidden until the visibility swap publishes them",
    );
    assert!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .total_size_bytes()
            .unwrap()
            > 0,
        "WAL reset must wait until the flush visibility publication commits",
    );
    assert!(
        flush_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "flush should still be paused at the visibility publish boundary",
    );

    release_tx.send(()).unwrap();
    assert!(flush_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    flush_thread.join().unwrap();

    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&staged_root));
    assert_eq!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .total_size_bytes()
            .unwrap(),
        0,
        "WAL reset should only happen after the new persisted view is published",
    );

    storage.clear_persist_post_publish_hook();
    storage.close().unwrap();
}

#[test]
fn flush_staging_holds_compaction_gate_until_catalog_publication() {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
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
        .unwrap(),
    );

    // Leave three durable L0 roots in place. The staged fourth root makes the concurrent
    // compaction pass eligible to consume every source, including the not-yet-visible flush root.
    for batch in 0..3_i64 {
        storage
            .insert_rows(&[
                Row::with_labels(
                    "flush_compaction_fence",
                    labels.clone(),
                    DataPoint::new(batch * 2 + 1, (batch * 2 + 1) as f64),
                ),
                Row::with_labels(
                    "flush_compaction_fence",
                    labels.clone(),
                    DataPoint::new(batch * 2 + 2, (batch * 2 + 2) as f64),
                ),
            ])
            .unwrap();
        storage.flush_pipeline_once().unwrap();
    }

    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_compaction_fence",
                labels.clone(),
                DataPoint::new(7, 7.0),
            ),
            Row::with_labels(
                "flush_compaction_fence",
                labels.clone(),
                DataPoint::new(8, 8.0),
            ),
        ])
        .unwrap();

    let (staged_tx, staged_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_persist_post_publish_hook({
        let release_rx = Arc::clone(&release_rx);
        move |roots| {
            staged_tx.send(roots.to_vec()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        flush_tx.send(flush_storage.flush_pipeline_once()).unwrap();
    });

    let staged_roots = staged_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not reach the staged segment hook");
    assert_eq!(staged_roots.len(), 1);
    assert!(staged_roots[0].exists());

    let compaction_storage = Arc::clone(&storage);
    let (compaction_attempted_tx, compaction_attempted_rx) = mpsc::channel();
    let (compaction_acquired_tx, compaction_acquired_rx) = mpsc::channel();
    let compaction_thread = thread::spawn(move || {
        compaction_attempted_tx.send(()).unwrap();
        let _compaction_guard = compaction_storage.compaction_gate();
        compaction_acquired_tx.send(()).unwrap();
        ChunkStorage::compact_compactors_with_changes(
            compaction_storage
                .persisted
                .series_index_path
                .as_deref()
                .and_then(Path::parent),
            compaction_storage.persisted.numeric_compactor.as_ref(),
            compaction_storage.persisted.blob_compactor.as_ref(),
            Some(compaction_storage.visibility.tombstones.as_ref()),
            Some(compaction_storage.observability.as_ref()),
            |changes| {
                compaction_storage
                    .persisted
                    .pending_persisted_segment_diff
                    .lock()
                    .merge(changes);
                compaction_storage
                    .persisted
                    .persisted_index_dirty
                    .store(true, Ordering::SeqCst);
            },
        )
    });

    compaction_attempted_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("compaction thread did not attempt the gate");
    let compaction_acquired_while_staged = compaction_acquired_rx
        .recv_timeout(Duration::from_millis(200))
        .is_ok();

    release_tx.send(()).unwrap();
    let flush_result = flush_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not finish after releasing the staged segment hook");
    flush_thread.join().unwrap();
    if !compaction_acquired_while_staged {
        compaction_acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("compaction did not acquire the gate after flush publication");
    }
    let compaction_result = compaction_thread.join().unwrap();

    assert!(
        !compaction_acquired_while_staged,
        "compaction must not inspect a staged flush root before catalog publication",
    );
    assert!(flush_result.is_ok());
    assert!(compaction_result.unwrap());

    storage.refresh_dirty_persisted_segments_claimed().unwrap();
    assert_eq!(
        storage
            .select("flush_compaction_fence", &labels, 0, 10)
            .unwrap()
            .len(),
        8,
    );

    storage.clear_persist_post_publish_hook();
    storage.close().unwrap();
}

#[test]
fn flush_snapshot_defers_active_to_sealed_handoff_without_advancing_wal() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path.clone()),
            None,
            1,
            options,
        )
        .unwrap(),
    );
    let metric = "active_to_sealed_flush_visibility";
    let labels = vec![Label::new("host", "flush")];

    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let (publish_entered_tx, publish_entered_rx) = mpsc::channel();
    let (publish_release_tx, publish_release_rx) = mpsc::channel();
    let publish_release_rx = Arc::new(Mutex::new(publish_release_rx));
    storage.set_ingest_pre_sealed_chunk_publish_hook({
        let publish_entered_tx = publish_entered_tx.clone();
        let publish_release_rx = Arc::clone(&publish_release_rx);
        move || {
            publish_entered_tx.send(()).unwrap();
            publish_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer_thread = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(2, 2.0),
        )])
    });

    publish_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("write did not reach the active-to-sealed publish boundary");

    let durable_before = storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .current_durable_highwater();
    let deferred = storage.persist_segment_with_outcome().unwrap();
    assert!(!deferred.persisted);
    assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());
    assert_eq!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .current_durable_highwater(),
        durable_before,
        "a handoff no-op must not advance WAL durability",
    );

    publish_release_tx.send(()).unwrap();

    writer_thread.join().unwrap().unwrap();
    storage.clear_ingest_pre_sealed_chunk_publish_hook();
    let outcome = storage.persist_segment_with_outcome().unwrap();

    assert!(outcome.persisted);
    assert_eq!(outcome.series, 1);
    assert_eq!(outcome.chunks, 1);
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn snapshot_fences_background_flush_before_copying_wal_state() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let snapshot_path = temp_dir.path().join("snapshot");
    let restore_path = temp_dir.path().join("restore");

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            4_096,
            Some(wal),
            Some(lane_path),
            None,
            1,
            options,
        )
        .unwrap(),
    );
    let fixture_manifest_builder = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(4_096)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::MAX);
    super::super::data_directory_manifest::install_current_manifest_for_test(
        &fixture_manifest_builder,
    )
    .unwrap();

    storage
        .insert_rows(&[
            Row::new("snapshot_background_flush", DataPoint::new(1, 11.0)),
            Row::new("snapshot_background_flush", DataPoint::new(2, 22.0)),
        ])
        .unwrap();

    let (flush_start_tx, flush_start_rx) = mpsc::channel();
    let (flush_attempt_tx, flush_attempt_rx) = mpsc::channel();
    let flush_attempt_rx = Arc::new(Mutex::new(flush_attempt_rx));
    let flush_storage = Arc::clone(&storage);
    let flush_thread = thread::spawn(move || {
        flush_start_rx.recv().unwrap();
        flush_attempt_tx.send(()).unwrap();
        let _background_maintenance_guard = flush_storage.background_maintenance_gate();
        flush_storage.background_flush_pipeline_once().unwrap();
    });

    storage.set_snapshot_pre_wal_copy_hook({
        let flush_attempt_rx = Arc::clone(&flush_attempt_rx);
        move || {
            flush_start_tx.send(()).unwrap();
            flush_attempt_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(1))
                .expect("background flush helper did not start");
            thread::sleep(Duration::from_millis(200));
        }
    });

    storage.snapshot(&snapshot_path).unwrap();
    storage.clear_snapshot_pre_wal_copy_hook();
    flush_thread.join().unwrap();

    let snapshot_wal =
        FramedWal::open(snapshot_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    assert!(
        snapshot_wal.total_size_bytes().unwrap() > 0,
        "snapshot should keep WAL-backed writes when a background flush races the staging copy",
    );
    drop(snapshot_wal);
    let _ = std::fs::remove_dir_all(snapshot_path.join(NUMERIC_LANE_ROOT));

    StorageBuilder::restore_from_snapshot(&snapshot_path, &restore_path).unwrap();
    let restored = StorageBuilder::new()
        .with_data_path(&restore_path)
        .with_chunk_points(4_096)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::MAX)
        .build()
        .unwrap();
    assert_eq!(
        restored
            .select("snapshot_background_flush", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 11.0), DataPoint::new(2, 22.0)],
    );

    restored.close().unwrap();
    storage.close().unwrap();
}

#[test]
fn snapshot_rejects_earlier_source_tree_mutation_before_publication() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let snapshot_path = temp_dir.path().join("snapshot");
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();
    let fixture_manifest_builder = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::MAX);
    super::super::data_directory_manifest::install_current_manifest_for_test(
        &fixture_manifest_builder,
    )
    .unwrap();

    storage.set_snapshot_pre_publication_hook({
        let lane_path = lane_path.clone();
        move || {
            std::fs::write(lane_path.join("late-source-entry"), b"late").unwrap();
            Ok(())
        }
    });
    let err = storage.snapshot(&snapshot_path).unwrap_err();
    storage.clear_snapshot_pre_publication_hook();

    assert!(
        err.to_string()
            .contains("snapshot source changed before publication"),
        "unexpected late-source mutation error: {err}"
    );
    assert!(
        !snapshot_path.exists(),
        "a snapshot with a late source mutation must not be published"
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_aggregate_entry_limit_accepts_exact_n_and_rejects_n_plus_one() {
    super::super::ensure_snapshot_aggregate_entry_limit(crate::MAX_SNAPSHOT_RESTORE_ENTRIES)
        .expect("the exact aggregate restore entry limit must be accepted");
    let err = super::super::ensure_snapshot_aggregate_entry_limit(
        crate::MAX_SNAPSHOT_RESTORE_ENTRIES + 1,
    )
    .expect_err("one aggregate entry beyond the restore limit must be rejected");
    assert!(err.to_string().contains("snapshot aggregate entry count"));
    assert!(err
        .to_string()
        .contains(&crate::MAX_SNAPSHOT_RESTORE_ENTRIES.to_string()));
}

#[test]
fn snapshot_publication_sync_failure_retains_the_visible_destination() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let snapshot_path = temp_dir.path().join("snapshot");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new(
            "snapshot_publication_failure",
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
        temp_dir.path().to_path_buf(),
        "injected snapshot publication parent sync failure",
    );
    let err = storage.snapshot(&snapshot_path).unwrap_err();
    drop(sync_failure);

    assert!(
        err.to_string()
            .contains("injected snapshot publication parent sync failure"),
        "unexpected snapshot error: {err}"
    );
    assert!(
        crate::engine::fs_utils::path_exists_no_follow(&snapshot_path).unwrap(),
        "an error after rename must retain the visible destination because its durability is indeterminate"
    );
    assert!(
        !std::fs::read_dir(temp_dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tmp-tsink-snapshot-")
        }),
        "the renamed staging pathname must remain absent"
    );
    assert_eq!(
        storage
            .select("snapshot_publication_failure", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)],
        "snapshot cleanup must not mutate the live source"
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_postrename_cleanup_preserves_a_raced_staging_path() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let snapshot_path = temp_dir.path().join("snapshot");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new(
            "snapshot_identity_cleanup",
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let synchronized_parent = temp_dir.path().to_path_buf();
    let destination_for_hook = snapshot_path.clone();
    let observed_staging = std::sync::Arc::new(std::sync::Mutex::new(None::<std::path::PathBuf>));
    let staging_for_hook = std::sync::Arc::clone(&observed_staging);
    let sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".tmp-tsink-snapshot-"))
            {
                *staging_for_hook
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()) = Some(path.to_path_buf());
                return false;
            }
            if path != synchronized_parent || !destination_for_hook.exists() {
                return false;
            }
            std::fs::write(
                destination_for_hook.join("consumer-created"),
                b"consumer-state",
            )
            .unwrap();
            let raced_staging = staging_for_hook
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
                .expect("snapshot staging must have been synchronized before publication");
            std::fs::create_dir(&raced_staging).unwrap();
            std::fs::write(raced_staging.join("foreign"), b"foreign-state").unwrap();
            true
        },
        "injected post-rename sync failure with a raced staging path",
    );
    let err = storage.snapshot(&snapshot_path).unwrap_err();
    drop(sync_failure);

    assert!(
        err.to_string()
            .contains("injected post-rename sync failure"),
        "{err}"
    );
    assert!(
        snapshot_path.exists(),
        "a visible destination must be retained after parent-sync failure"
    );
    assert_eq!(
        std::fs::read(snapshot_path.join("consumer-created")).unwrap(),
        b"consumer-state",
        "post-rename consumer data must never be removed by snapshot cleanup"
    );
    let raced_staging = observed_staging
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
        .unwrap();
    assert_eq!(
        std::fs::read(raced_staging.join("foreign")).unwrap(),
        b"foreign-state",
        "cleanup must not mistake the raced staging pathname for the published snapshot"
    );
    assert_eq!(
        storage
            .select("snapshot_identity_cleanup", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_existing_destination_is_rejected_without_mutating_it() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let snapshot_path = temp_dir.path().join("snapshot");
    std::fs::create_dir(&snapshot_path).unwrap();
    std::fs::write(snapshot_path.join("owner.txt"), b"preexisting").unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention_enforced(false)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    let err = storage.snapshot(&snapshot_path).unwrap_err();

    assert!(matches!(err, TsinkError::InvalidConfiguration(message)
        if message.contains("snapshot destination already exists")));
    assert_eq!(
        std::fs::read(snapshot_path.join("owner.txt")).unwrap(),
        b"preexisting"
    );
    assert_eq!(std::fs::read_dir(&snapshot_path).unwrap().count(), 1);
    assert!(
        !std::fs::read_dir(temp_dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tmp-tsink-snapshot-")
        }),
        "preflight rejection must not create a staging namespace"
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_destination_created_during_copy_is_not_replaced() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let snapshot_path = temp_dir.path().join("snapshot");
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    let storage = std::sync::Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path),
            None,
            1,
            options,
        )
        .unwrap(),
    );
    let fixture_manifest_builder = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::MAX);
    super::super::data_directory_manifest::install_current_manifest_for_test(
        &fixture_manifest_builder,
    )
    .unwrap();
    storage
        .insert_rows(&[Row::new("snapshot_noreplace_race", DataPoint::new(1, 1.0))])
        .unwrap();

    storage.set_snapshot_pre_wal_copy_hook({
        let snapshot_path = snapshot_path.clone();
        move || std::fs::create_dir(&snapshot_path).unwrap()
    });
    let err = storage.snapshot(&snapshot_path).unwrap_err();
    storage.clear_snapshot_pre_wal_copy_hook();

    assert!(
        err.to_string()
            .contains(&snapshot_path.display().to_string()),
        "unexpected no-replace publication error: {err}"
    );
    assert!(
        snapshot_path.is_dir(),
        "the destination created by another owner must remain"
    );
    assert_eq!(
        std::fs::read_dir(&snapshot_path).unwrap().count(),
        0,
        "snapshot publication must not add entries to the raced destination"
    );
    assert!(
        !std::fs::read_dir(temp_dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tmp-tsink-snapshot-")
        }),
        "the losing owned staging tree must be removed"
    );
    assert_eq!(
        storage
            .select("snapshot_noreplace_race", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)],
        "a destination race must not mutate the live source"
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_copied_subtree_sync_failure_retains_unverified_staging() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let snapshot_path = temp_dir.path().join("snapshot");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new(
            "snapshot_copy_sync_failure",
            DataPoint::new(1, 2.0),
        )])
        .unwrap();

    let expected_snapshot_parent = temp_dir.path().to_path_buf();
    let _sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| {
            path.file_name().is_some_and(|name| name == WAL_DIR_NAME)
                && path.starts_with(&expected_snapshot_parent)
                && path.to_string_lossy().contains(".tmp-tsink-snapshot-")
        },
        "injected copied WAL directory sync failure",
    );
    let err = storage.snapshot(&snapshot_path).unwrap_err();

    assert!(
        err.to_string()
            .contains("injected copied WAL directory sync failure"),
        "unexpected snapshot copy error: {err}"
    );
    assert!(!snapshot_path.exists());
    let staging = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".tmp-tsink-snapshot-"))
        })
        .expect("a copy-time sync failure before identity capture must retain staging");
    assert!(staging.join(WAL_DIR_NAME).exists());
    assert_eq!(
        storage
            .select("snapshot_copy_sync_failure", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 2.0)]
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_synchronizes_missing_destination_ancestors_before_staging() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let destination_root = temp_dir.path().join("snapshot-root");
    let snapshot_path = destination_root.join("nested/snapshot");
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new(
            "snapshot_ancestor_sync_failure",
            DataPoint::new(1, 3.0),
        )])
        .unwrap();

    let synchronized_parent = destination_root.clone();
    let _sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |path| path == synchronized_parent,
        "injected snapshot ancestor sync failure",
    );
    let err = storage.snapshot(&snapshot_path).unwrap_err();

    assert!(
        err.to_string()
            .contains("injected snapshot ancestor sync failure"),
        "unexpected snapshot ancestry error: {err}"
    );
    assert!(!snapshot_path.exists());
    assert!(
        std::fs::read_dir(destination_root.join("nested"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tmp-tsink-snapshot-")),
        "ancestor synchronization must fail before staging creation"
    );
    assert_eq!(
        storage
            .select("snapshot_ancestor_sync_failure", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 3.0)]
    );
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_skips_wal_reset_when_a_new_write_commits_after_publish() {
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[Row::with_labels(
            "flush_reset_race_metric",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "flush_reset_race_metric",
            labels.clone(),
            DataPoint::new(2, 2.0),
        )])
        .unwrap();

    let hook_storage = Arc::clone(&storage);
    let hook_labels = labels.clone();
    storage.set_persist_post_publish_hook(move |_| {
        hook_storage
            .insert_rows(&[Row::with_labels(
                "flush_reset_race_metric",
                hook_labels.clone(),
                DataPoint::new(3, 3.0),
            )])
            .unwrap();
    });

    storage.flush_pipeline_once().unwrap();

    let wal = storage.persisted.wal.as_ref().unwrap();
    assert!(
        wal.total_size_bytes().unwrap() > 0,
        "WAL reset should be skipped once a newer write lands after publish",
    );
    assert_eq!(
        storage
            .select("flush_reset_race_metric", &labels, 0, 10)
            .unwrap()
            .len(),
        3,
    );

    storage.clear_persist_post_publish_hook();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_runs_steady_state_post_flush_maintenance_without_full_inventory_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let historical_series_id = registry
        .resolve_or_insert("historical_post_flush_maintenance", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=64 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            historical_series_id,
            0,
            segment_id,
            &[(100, segment_id as f64)],
        );
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            65,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 10,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                max_future_skew_window: None,
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
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
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    let catalog_publications = Arc::new(AtomicUsize::new(0));
    storage.set_catalog_transition_post_catalog_publication_hook({
        let catalog_publications = Arc::clone(&catalog_publications);
        move || {
            catalog_publications.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

    storage
        .insert_rows(&[Row::with_labels(
            "steady_state_flush_maintenance",
            labels.clone(),
            DataPoint::new(100, 1.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    storage.set_current_time_override(101);
    storage
        .insert_rows(&[Row::with_labels(
            "steady_state_flush_maintenance",
            labels.clone(),
            DataPoint::new(101, 2.0),
        )])
        .unwrap();
    assert_eq!(
        storage
            .select("steady_state_flush_maintenance", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(100, 1.0), DataPoint::new(101, 2.0)],
        "foreground reads should keep serving published data while no-op maintenance settles"
    );

    assert!(
        !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst),
        "background maintenance did not settle the post-flush no-op work"
    );
    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "steady-state post-flush maintenance should reuse the persisted catalog instead of rescanning the segment tree",
    );
    assert_eq!(
        catalog_publications.load(Ordering::SeqCst),
        1,
        "the flush should publish once without a redundant no-op catalog rewrite",
    );

    storage.clear_full_inventory_scan_hook();
    storage.clear_catalog_transition_post_catalog_publication_hook();
}

#[test]
fn background_retention_page_expires_only_one_bounded_inventory_page_per_wake() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("bounded_retention_page", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    for segment_id in 1..=7 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(1, segment_id as f64)],
        );
    }
    let storage = bounded_retention_page_storage(&lane_path, 8, 3);
    storage.persist_series_registry_index().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let registry_catalog_store =
        super::super::registry_catalog::catalog_store_path(&checkpoint_path);
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        8
    );
    let inspected = Arc::new(AtomicUsize::new(0));
    storage.set_background_retention_inspect_hook({
        let inspected = Arc::clone(&inspected);
        move || {
            inspected.fetch_add(1, Ordering::SeqCst);
        }
    });

    mark_post_flush_maintenance_pending(&storage);
    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        4
    );
    assert_eq!(inspected.load(Ordering::SeqCst), 3);
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        5,
        "the first page should remove exactly three catalog entry files"
    );
    assert!(storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::Acquire));

    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        1
    );
    assert_eq!(inspected.load(Ordering::SeqCst), 6);
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        2,
        "the second page should remove exactly three more catalog entry files"
    );
    assert!(storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::Acquire));

    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .is_empty());
    assert_eq!(inspected.load(Ordering::SeqCst), 7);
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        1,
        "an empty inventory retains only the bounded catalog manifest"
    );
    assert!(
        !super::super::registry_catalog::catalog_path(&checkpoint_path).exists(),
        "the first bounded page should retire the stale complete JSON snapshot"
    );
    assert!(!storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::Acquire));

    storage.clear_background_retention_inspect_hook();
    storage.close().unwrap();
}

#[test]
fn background_retention_page_respects_modeled_source_byte_limit_for_rewrites() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("bounded_retention_bytes", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    let first_root = write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        0,
        1,
        &[(80, 1.0), (95, 2.0)],
    );
    let second_root = write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        0,
        2,
        &[(80, 3.0), (95, 4.0)],
    );
    let one_source_bytes = modeled_retention_rewrite_candidate_bytes(&first_root)
        .max(modeled_retention_rewrite_candidate_bytes(&second_root));
    let storage = bounded_retention_page_storage_with_bytes(&lane_path, 3, 10, one_source_bytes);

    mark_post_flush_maintenance_pending(&storage);
    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert!(!first_root.exists());
    assert!(second_root.exists());
    assert!(storage
        .coordination
        .post_flush_maintenance_pending
        .load(std::sync::atomic::Ordering::Acquire));

    storage.close().unwrap();
}

#[test]
fn background_retention_exact_page_multiple_needs_terminal_empty_page_before_clean_claim() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("bounded_retention_clean", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    for segment_id in 1..=6 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(95, segment_id as f64)],
        );
    }
    let storage = bounded_retention_page_storage(&lane_path, 7, 2);
    let inspected = Arc::new(AtomicUsize::new(0));
    storage.set_background_retention_inspect_hook({
        let inspected = Arc::clone(&inspected);
        move || {
            inspected.fetch_add(1, Ordering::SeqCst);
        }
    });

    mark_post_flush_maintenance_pending(&storage);
    for expected_inspections in [2, 4, 6] {
        assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
        assert_eq!(inspected.load(Ordering::SeqCst), expected_inspections);
        assert!(storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::Acquire));
    }
    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert_eq!(inspected.load(Ordering::SeqCst), 6);
    assert!(!storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::Acquire));
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        6,
        "a clean bounded cycle must not publish a partial inventory as a full no-op"
    );

    storage.clear_background_retention_inspect_hook();
    storage.close().unwrap();
}

#[test]
fn background_retention_publication_error_retries_cursor_then_reaches_later_segments() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("bounded_retention_retry", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    for segment_id in 1..=5 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(1, segment_id as f64)],
        );
    }
    let storage = bounded_retention_page_storage(&lane_path, 6, 2);
    storage.persist_series_registry_index().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let registry_catalog_store =
        super::super::registry_catalog::catalog_store_path(&checkpoint_path);
    let fail_once = Arc::new(AtomicBool::new(true));
    storage.set_catalog_transition_post_index_mutation_hook({
        let fail_once = Arc::clone(&fail_once);
        move || {
            if fail_once.swap(false, Ordering::SeqCst) {
                Err(TsinkError::Other(
                    "injected bounded retention publication failure".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    });

    mark_post_flush_maintenance_pending(&storage);
    let err = storage.run_post_flush_maintenance_if_pending().unwrap_err();
    assert!(err
        .to_string()
        .contains("injected bounded retention publication failure"));
    assert!(storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::Acquire));
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        6,
        "failure before sidecar publication must retain the original five entries"
    );
    assert!(storage
        .coordination
        .background_retention_maintenance_cursor
        .lock()
        .after_root
        .is_none());

    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        3,
        "retry must finish the durable failed page before the dirty catalog is refreshed"
    );
    assert_eq!(
        std::fs::read_dir(&registry_catalog_store).unwrap().count(),
        4,
        "retry must reconstruct and remove both source keys after index mutation"
    );
    let inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&inventory),
    )
    .unwrap()
    .is_some());
    assert!(storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::Acquire));

    let mut continuation_passes = 0usize;
    while storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .len()
        > 1
    {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
        continuation_passes += 1;
        assert!(
            continuation_passes < 64,
            "bounded catalog reconciliation failed to resume retention after publication recovery"
        );
    }
    assert!(
        continuation_passes > 1,
        "unknown-dirty catalog recovery should retain its finite continuation boundary"
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .len(),
        1,
        "the cursor retry must not starve segments after the recovered page"
    );

    storage.clear_catalog_transition_post_index_mutation_hook();
    storage.close().unwrap();
}

#[test]
fn background_post_flush_maintenance_applies_known_dirty_diff_before_inventory_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("dirty_post_flush_maintenance", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=128 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(100, segment_id as f64)],
        );
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            129,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 10,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                max_future_skew_window: None,
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
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
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let added_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 129, &[(100, 129.0)]);
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::once(added_root.clone()), std::iter::empty());
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    storage.start_background_persisted_refresh_thread().unwrap();
    storage.schedule_post_flush_maintenance().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut settled = false;
    while Instant::now() < deadline {
        if !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst)
            && !storage
                .persisted
                .persisted_index_dirty
                .load(Ordering::SeqCst)
            && !storage.has_known_persisted_segment_changes()
            && storage
                .persisted
                .persisted_index
                .read()
                .segments_by_root
                .contains_key(&added_root)
        {
            settled = true;
            break;
        }

        thread::sleep(Duration::from_millis(10));
    }

    assert!(
        settled,
        "background post-flush maintenance did not settle the known dirty diff",
    );
    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "background post-flush maintenance should apply known dirty roots before scanning the full segment tree",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn finite_tiered_known_dirty_diff_advances_without_replaying_visibility_mutation() {
    use std::sync::atomic::Ordering;

    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let local_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: Some(data_dir.path().join("local-tiered-catalog.json")),
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let shared_hot_lane = tiered_storage.lane_path(
        super::super::tiering::SegmentLaneFamily::Numeric,
        super::super::tiering::PersistedSegmentTier::Hot,
    );
    let labels = vec![Label::new("host", "known-dirty")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("finite_tiered_known_dirty", &labels)
        .unwrap()
        .series_id;
    let added_root =
        write_numeric_segment_to_path(&shared_hot_lane, &registry, series_id, 0, 1, &[(1, 1.0)]);
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(local_lane),
        None,
        2,
        ChunkStorageOptions {
            retention_enforced: false,
            maintenance_max_items_per_pass: 4,
            maintenance_max_bytes_per_pass: u64::MAX,
            tiered_storage: Some(tiered_storage.clone()),
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    install_shared_object_store_writer_lock_for_test(&storage, object_store_dir.path());
    storage.persist_series_registry_index().unwrap();
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::once(added_root.clone()), std::iter::empty());
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "the already-installed diff must transfer ownership to the retained publication cursor"
    );
    assert!(storage.bounded_tiered_catalog_publication_is_pending());
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&added_root));
    let installed_visibility_generation = storage.visibility_state_generation();

    let mut continuation_passes = 1usize;
    while storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst)
    {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
        continuation_passes = continuation_passes.saturating_add(1);
        assert!(
            continuation_passes < 64,
            "finite known-dirty catalog publication failed to converge"
        );
        assert_eq!(
            storage.visibility_state_generation(),
            installed_visibility_generation,
            "cursor wakes must not replay the already-installed root transition"
        );
    }
    assert!(continuation_passes > 1);
    assert!(!storage.bounded_tiered_catalog_publication_is_pending());
    let pointer =
        super::super::tiering::require_shared_segment_catalog_pointer(&tiered_storage).unwrap();
    assert_eq!(pointer.entry_count, 1);
    assert_eq!(
        storage
            .observability_snapshot()
            .memory
            .remote_catalog_staging_bytes,
        0
    );
    let inventory = storage.persisted_segment_inventory();
    assert_eq!(inventory.entries().len(), 1);
    assert_eq!(inventory.entries()[0].root, added_root);
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &data_dir.path().join(SERIES_INDEX_FILE_NAME),
        &super::super::registry_catalog::inventory_sources(&inventory),
    )
    .unwrap()
    .is_some());
}

#[test]
fn background_post_flush_maintenance_stage_does_not_block_queries() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_root = object_store_dir
        .path()
        .join("warm")
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let labels = vec![Label::new("host", "a")];
    let metric = "background_post_flush_query_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(hot_lane.clone()),
            None,
            2,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 100,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                max_future_skew_window: None,
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
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
                tiered_storage: Some(super::super::config::TieredStorageConfig {
                    object_store_root: object_store_dir.path().to_path_buf(),
                    segment_catalog_path: None,
                    mirror_hot_segments: false,
                    hot_retention_window: 10,
                    warm_retention_window: 50,
                }),
                #[cfg(test)]
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    install_shared_object_store_writer_lock_for_test(storage.as_ref(), object_store_dir.path());
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();

    let stage_started = Arc::new(AtomicBool::new(false));
    let release_stage = Arc::new(AtomicBool::new(false));
    storage.set_post_flush_maintenance_stage_hook({
        let stage_started = Arc::clone(&stage_started);
        let release_stage = Arc::clone(&release_stage);
        move || {
            stage_started.store(true, Ordering::SeqCst);
            while !release_stage.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });

    storage.start_background_persisted_refresh_thread().unwrap();
    storage.schedule_post_flush_maintenance().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !stage_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        stage_started.load(Ordering::SeqCst),
        "background post-flush maintenance did not reach the staging hook",
    );

    let concurrent_storage = Arc::clone(&storage);
    let concurrent_labels = labels.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let concurrent_query = thread::spawn(move || {
        let result = concurrent_storage.select(metric, &concurrent_labels, 0, 200);
        query_tx.send(result).unwrap();
    });

    let concurrent_points = query_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("concurrent query should not block on staged post-flush maintenance");
    assert_eq!(
        concurrent_points.unwrap(),
        vec![DataPoint::new(60, 60.0), DataPoint::new(61, 61.0)]
    );

    release_stage.store(true, Ordering::SeqCst);
    concurrent_query.join().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut settled = false;
    while Instant::now() < deadline {
        if !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst)
            && warm_root.exists()
            && !hot_lane
                .join("segments")
                .join("L0")
                .join("seg-0000000000000001")
                .exists()
        {
            settled = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        settled,
        "background post-flush maintenance did not finish the staged tier move"
    );

    storage.clear_post_flush_maintenance_stage_hook();
    storage.close().unwrap();
}

#[test]
fn finite_tiered_post_flush_marker_advances_existing_catalog_cursor_without_reapplying() {
    use std::sync::atomic::Ordering;

    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = data_dir.path().join(SERIES_INDEX_FILE_NAME);
    let hot_root = hot_lane
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let labels = vec![Label::new("host", "bounded-marker")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("bounded_post_flush_marker", &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: Some(data_dir.path().join("local-tiered-catalog.json")),
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(hot_lane.clone()),
        None,
        2,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 100,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            max_future_skew_window: None,
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
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
            maintenance_max_items_per_pass: 4,
            maintenance_max_bytes_per_pass: u64::MAX,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: Some(tiered_storage.clone()),
            #[cfg(test)]
            current_time_override: Some(100),
        },
    )
    .unwrap();
    install_shared_object_store_writer_lock_for_test(&storage, object_store_dir.path());
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();
    storage.checkpoint_series_registry_index().unwrap();
    storage
        .refresh_segment_catalog_and_observability_from_persisted_state(&[])
        .unwrap();
    let initial_pointer =
        super::super::tiering::require_shared_segment_catalog_pointer(&tiered_storage).unwrap();
    let initial_entry = storage.persisted_segment_inventory().entries()[0].clone();
    let warm_root = super::super::tiering::destination_segment_root(
        &tiered_storage,
        initial_entry.lane,
        super::super::tiering::PersistedSegmentTier::Warm,
        &initial_entry.manifest,
    );

    storage
        .coordination
        .post_flush_maintenance_pending
        .store(true, Ordering::SeqCst);
    let marker_dir = data_dir.path().join(".post-flush-replacements");
    let mut passes = 0usize;
    let mut observed_deferred_marker = false;
    while storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::SeqCst)
        || storage
            .coordination
            .startup_metadata_reconcile_pending
            .load(Ordering::SeqCst)
    {
        assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
        passes = passes.saturating_add(1);
        assert!(
            passes < 128,
            "finite post-flush marker/catalog publication failed to converge"
        );

        let marker_present = marker_dir
            .read_dir()
            .is_ok_and(|mut entries| entries.next().is_some());
        if marker_present {
            observed_deferred_marker = true;
            assert!(
                hot_root.exists(),
                "the source root must remain durable until pointer publication completes"
            );
            assert_eq!(
                super::super::tiering::require_shared_segment_catalog_pointer(&tiered_storage)
                    .unwrap(),
                initial_pointer,
                "a retained Committing marker must keep finite readers on the prior pointer"
            );
        }
    }

    assert!(passes > 1);
    assert!(
        observed_deferred_marker,
        "the finite publication should retain a real Committing marker across wakes"
    );
    assert!(!hot_root.exists());
    assert!(warm_root.exists());
    super::super::maintenance::ensure_no_pending_post_flush_replacement(data_dir.path()).unwrap();
    assert!(!storage.bounded_tiered_catalog_publication_is_pending());
    assert_eq!(
        storage
            .observability_snapshot()
            .memory
            .remote_catalog_staging_bytes,
        0
    );
    let replacement_pointer =
        super::super::tiering::require_shared_segment_catalog_pointer(&tiered_storage).unwrap();
    assert!(replacement_pointer.generation > initial_pointer.generation);
    assert_eq!(replacement_pointer.entry_count, 1);

    let visible_inventory = storage.persisted_segment_inventory();
    assert_eq!(visible_inventory.entries().len(), 1);
    assert_eq!(visible_inventory.entries()[0].root, warm_root);
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());
    assert!(!storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
}

#[test]
fn background_post_flush_maintenance_syncs_registry_catalog_after_tier_move() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = data_dir.path().join(SERIES_INDEX_FILE_NAME);
    let hot_root = hot_lane
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let warm_root = object_store_dir
        .path()
        .join("warm")
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let labels = vec![Label::new("host", "a")];
    let metric = "background_post_flush_catalog_sync_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(hot_lane.clone()),
        None,
        2,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 100,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            max_future_skew_window: None,
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
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
            tiered_storage: Some(tiered_storage.clone()),
            #[cfg(test)]
            current_time_override: Some(100),
        },
    )
    .unwrap();
    install_shared_object_store_writer_lock_for_test(&storage, object_store_dir.path());
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();
    storage.checkpoint_series_registry_index().unwrap();

    storage.schedule_post_flush_maintenance().unwrap();

    assert!(!hot_root.exists());
    assert!(warm_root.exists());
    assert_eq!(
        storage.select(metric, &labels, 0, 200).unwrap(),
        vec![DataPoint::new(60, 60.0), DataPoint::new(61, 61.0)],
    );

    let visible_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        Some(&hot_lane),
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    assert!(visible_inventory
        .entries()
        .iter()
        .any(|entry| entry.root == warm_root));
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_returns_while_background_post_flush_maintenance_is_staged() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "background_post_flush_flush_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(hot_lane.clone()),
            None,
            2,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 100,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                max_future_skew_window: None,
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
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
                tiered_storage: Some(super::super::config::TieredStorageConfig {
                    object_store_root: object_store_dir.path().to_path_buf(),
                    segment_catalog_path: None,
                    mirror_hot_segments: false,
                    hot_retention_window: 10,
                    warm_retention_window: 50,
                }),
                #[cfg(test)]
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    install_shared_object_store_writer_lock_for_test(storage.as_ref(), object_store_dir.path());
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();

    let stage_started = Arc::new(AtomicBool::new(false));
    let release_stage = Arc::new(AtomicBool::new(false));
    storage.set_post_flush_maintenance_stage_hook({
        let stage_started = Arc::clone(&stage_started);
        let release_stage = Arc::clone(&release_stage);
        move || {
            stage_started.store(true, Ordering::SeqCst);
            while !release_stage.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });

    storage.start_background_persisted_refresh_thread().unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(100, 100.0),
        )])
        .unwrap();

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        flush_tx.send(flush_storage.flush_pipeline_once()).unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    while !stage_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        stage_started.load(Ordering::SeqCst),
        "background post-flush maintenance did not reach the staging hook after flush",
    );

    let flush_result = flush_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("flush should return while background post-flush maintenance is staged");
    assert!(flush_result.is_ok());

    release_stage.store(true, Ordering::SeqCst);
    flush_thread.join().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::SeqCst)
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst),
        "background post-flush maintenance did not settle after the staging hook released",
    );

    storage.clear_post_flush_maintenance_stage_hook();
    storage.close().unwrap();
}

#[test]
fn wal_disabled_persistent_storage_still_runs_background_flush() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_retention_enforced(false)
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "wal_disabled_background",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "wal_disabled_background",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "wal_disabled_background",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut persisted = false;
    while Instant::now() < deadline {
        if !load_segments_for_level(&lane_path, 0).unwrap().is_empty() {
            persisted = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    assert!(
        persisted,
        "background flush should persist segments even when WAL is disabled"
    );
    storage.close().unwrap();
}

#[test]
fn close_waits_for_inflight_background_flush_before_final_persist() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
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
        .unwrap(),
    );

    storage
        .insert_rows(&[
            Row::with_labels(
                "close_background_flush",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "close_background_flush",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels("close_background_flush", labels, DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let (background_entered_tx, background_entered_rx) = mpsc::channel();
    let (close_persist_tx, close_persist_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_persist_post_publish_hook({
        let hook_calls = Arc::clone(&hook_calls);
        let release_rx = Arc::clone(&release_rx);
        move |_| match hook_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                background_entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
            1 => {
                close_persist_tx.send(()).unwrap();
            }
            _ => {}
        }
    });

    storage
        .start_background_flush_thread(Duration::from_secs(60))
        .unwrap();
    storage.notify_flush_thread();
    background_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("background flush did not reach the publish hook");

    let close_storage = Arc::clone(&storage);
    let (close_tx, close_rx) = mpsc::channel();
    let close_thread = thread::spawn(move || {
        close_tx.send(close_storage.close()).unwrap();
    });

    assert!(
        close_persist_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "close should not publish its final persisted segment while background flush is mid-pass",
    );

    release_tx.send(()).unwrap();
    close_persist_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close did not reach its final persist after the background flush finished");
    assert!(close_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    close_thread.join().unwrap();
    assert_eq!(hook_calls.load(Ordering::SeqCst), 2);
}

#[test]
fn close_waits_for_inflight_background_refresh_before_final_persist() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
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
        .unwrap(),
    );

    storage
        .insert_rows(&[
            Row::with_labels(
                "close_background_refresh",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "close_background_refresh",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "close_background_refresh",
            labels,
            DataPoint::new(3, 3.0),
        )])
        .unwrap();

    let scan_calls = Arc::new(AtomicUsize::new(0));
    let (background_entered_tx, background_entered_rx) = mpsc::channel();
    let (close_persist_tx, close_persist_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_full_inventory_scan_hook({
        let scan_calls = Arc::clone(&scan_calls);
        let release_rx = Arc::clone(&release_rx);
        move || {
            if scan_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                background_entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        }
    });
    storage.set_persist_post_publish_hook(move |_| {
        close_persist_tx.send(()).unwrap();
    });

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();
    background_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("background refresh did not reach the full inventory scan hook");

    let close_storage = Arc::clone(&storage);
    let (close_tx, close_rx) = mpsc::channel();
    let close_thread = thread::spawn(move || {
        close_tx.send(close_storage.close()).unwrap();
    });

    assert!(
        close_persist_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "close should not publish its final persisted segment while background refresh is mid-pass",
    );

    release_tx.send(()).unwrap();
    close_persist_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close did not reach its final persist after the background refresh finished");
    assert!(close_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    close_thread.join().unwrap();
    assert!(scan_calls.load(Ordering::SeqCst) >= 1);
}

fn bounded_metadata_reconciliation_storage(max_items: usize, max_bytes: u64) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            maintenance_max_items_per_pass: max_items,
            maintenance_max_bytes_per_pass: max_bytes,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

#[test]
fn live_metadata_reconciliation_obeys_exact_item_boundary_across_wakes() {
    let storage = bounded_metadata_reconciliation_storage(2, u64::MAX);
    storage.mark_materialized_series_ids(1..=5);

    assert!(storage.run_live_metadata_reconciliation_page().unwrap());
    assert_eq!(storage.materialized_series_snapshot(), vec![3, 4, 5]);
    assert_eq!(
        storage
            .coordination
            .background_metadata_reconciliation_cursor
            .lock()
            .after_series_id,
        Some(2),
    );

    assert!(storage.run_live_metadata_reconciliation_page().unwrap());
    assert_eq!(storage.materialized_series_snapshot(), vec![5]);
    assert_eq!(
        storage
            .coordination
            .background_metadata_reconciliation_cursor
            .lock()
            .after_series_id,
        Some(4),
    );

    // The fifth item is the N+1 boundary for the preceding page. Its removal changes the
    // visibility generation, so a final empty verification cycle is deliberately retained.
    assert!(storage.run_live_metadata_reconciliation_page().unwrap());
    assert!(storage.materialized_series_snapshot().is_empty());
    assert_eq!(
        storage
            .coordination
            .background_metadata_reconciliation_cursor
            .lock()
            .phase,
        super::super::BackgroundMetadataReconciliationPhase::Verify,
    );
    assert!(!storage.run_live_metadata_reconciliation_page().unwrap());
    let terminal_cursor = storage
        .coordination
        .background_metadata_reconciliation_cursor
        .lock();
    assert_eq!(terminal_cursor.after_series_id, None);
    assert!(!terminal_cursor.cycle_started);
    assert!(!terminal_cursor.cycle_generation_changed);
}

#[test]
fn live_metadata_reconciliation_exact_live_multiple_finishes_without_empty_wake() {
    let storage = bounded_metadata_reconciliation_storage(2, u64::MAX);
    storage
        .insert_rows(&[
            Row::new("metadata_reconcile_exact_live_a", DataPoint::new(1, 1.0)),
            Row::new("metadata_reconcile_exact_live_b", DataPoint::new(1, 2.0)),
        ])
        .unwrap();
    assert_eq!(storage.materialized_series_snapshot().len(), 2);

    assert!(
        !storage.run_live_metadata_reconciliation_page().unwrap(),
        "an exact live multiple should use its terminal cursor probe in the same page",
    );
    assert_eq!(storage.materialized_series_snapshot().len(), 2);
    assert_eq!(
        storage
            .coordination
            .background_metadata_reconciliation_cursor
            .lock()
            .after_series_id,
        None,
    );
}

#[test]
fn metadata_reconciliation_active_range_model_matches_full_traversal_count() {
    let storage = bounded_metadata_reconciliation_storage(8, u64::MAX);
    storage
        .insert_rows(&[
            Row::new("metadata_reconcile_active_model", DataPoint::new(1, 1.0)),
            Row::new("metadata_reconcile_active_model", DataPoint::new(2, 2.0)),
            Row::new("metadata_reconcile_active_model", DataPoint::new(3, 3.0)),
        ])
        .unwrap();
    let series_id = storage.materialized_series_snapshot()[0];
    storage.clear_series_visible_timestamp_cache(std::iter::once(series_id));

    // In test builds the production point_count() fast path asserts equality with the complete
    // partition-order traversal used by query-deadline-aware preflight.
    let modeled =
        storage.series_visibility_refresh_staging_upper_bound(std::iter::once(&series_id));
    assert!(modeled > 512);
}

#[test]
fn live_metadata_reconciliation_obeys_exact_byte_dependency_window() {
    let probe = bounded_metadata_reconciliation_storage(8, u64::MAX);
    probe.mark_materialized_series_ids(std::iter::once(1));
    let exact_bytes = probe.modeled_metadata_reconciliation_item_bytes(1);
    drop(probe);

    let exact = bounded_metadata_reconciliation_storage(8, exact_bytes);
    exact.mark_materialized_series_ids(std::iter::once(1));
    assert!(exact.run_live_metadata_reconciliation_page().unwrap());
    assert!(exact.materialized_series_snapshot().is_empty());

    let below = bounded_metadata_reconciliation_storage(8, exact_bytes.saturating_sub(1));
    below.mark_materialized_series_ids(std::iter::once(1));
    let error = below
        .run_live_metadata_reconciliation_page()
        .expect_err("N-1 bytes must reject the indivisible series dependency window");
    assert!(matches!(
        error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "live metadata reconciliation series",
            item_limit: 8,
            byte_limit,
            selected_items: 0,
            selected_bytes: 0,
        } if byte_limit == exact_bytes - 1
    ));
    assert_eq!(below.materialized_series_snapshot(), vec![1]);
    assert_eq!(
        below
            .coordination
            .background_metadata_reconciliation_cursor
            .lock()
            .after_series_id,
        None,
        "a failed dependency window must not advance or retain an owned page",
    );
    below.reset_background_metadata_reconciliation_cursor();
    let reset_cursor = below
        .coordination
        .background_metadata_reconciliation_cursor
        .lock();
    assert_eq!(reset_cursor.after_series_id, None);
    assert!(!reset_cursor.cycle_started);
    assert!(!reset_cursor.cycle_generation_changed);
}

#[test]
fn live_metadata_reconciliation_revalidates_ids_reinserted_behind_cursor() {
    let storage = bounded_metadata_reconciliation_storage(2, u64::MAX);
    storage.mark_materialized_series_ids(1..=4);

    assert!(storage.run_live_metadata_reconciliation_page().unwrap());
    assert_eq!(storage.materialized_series_snapshot(), vec![3, 4]);

    // Simulate a writer reviving an already visited identity. The insertion generation forces a
    // verification cycle, which reaches the lower ID without rescanning from the root each wake.
    storage.mark_materialized_series_ids(std::iter::once(1));
    let mut passes = 0usize;
    while storage.run_live_metadata_reconciliation_page().unwrap() {
        passes = passes.saturating_add(1);
        assert!(passes < 8, "metadata reconciliation failed to converge");
    }
    assert!(storage.materialized_series_snapshot().is_empty());
    assert!(
        passes >= 2,
        "the changed generation must require a clean verification sweep",
    );
}

#[test]
fn startup_metadata_reconciliation_pending_bit_tracks_bounded_continuations() {
    let storage = bounded_metadata_reconciliation_storage(1, u64::MAX);
    storage.mark_materialized_series_ids(1..=2);
    storage.schedule_startup_maintenance();

    assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
    assert!(storage
        .coordination
        .startup_metadata_reconcile_pending
        .load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(storage.materialized_series_snapshot(), vec![2]);

    let mut passes = 0usize;
    while storage
        .coordination
        .startup_metadata_reconcile_pending
        .load(std::sync::atomic::Ordering::Acquire)
    {
        assert!(storage.run_post_flush_maintenance_if_pending().unwrap());
        passes = passes.saturating_add(1);
        assert!(passes < 8, "pending reconciliation failed to settle");
    }
    assert!(storage.materialized_series_snapshot().is_empty());
    assert!(!storage.run_post_flush_maintenance_if_pending().unwrap());
}

#[test]
fn close_releases_retained_metadata_reconciliation_cursor() {
    let storage = bounded_metadata_reconciliation_storage(1, u64::MAX);
    storage.mark_materialized_series_ids(1..=3);
    assert!(storage.run_live_metadata_reconciliation_page().unwrap());
    assert_eq!(
        storage
            .coordination
            .background_metadata_reconciliation_cursor
            .lock()
            .after_series_id,
        Some(1),
    );

    storage.close().unwrap();

    let cursor = storage
        .coordination
        .background_metadata_reconciliation_cursor
        .lock();
    assert_eq!(cursor.after_series_id, None);
    assert!(!cursor.cycle_started);
    assert!(!cursor.cycle_generation_changed);
}
