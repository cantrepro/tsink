use super::super::ActiveSeriesState;
use super::*;

fn new_ingest_test_storage(chunk_point_cap: usize) -> ChunkStorage {
    new_ingest_test_storage_with_options(chunk_point_cap, ChunkStorageOptions::default())
}

fn runtime_metadata_delta_series_ids(storage: &ChunkStorage) -> Vec<u64> {
    storage
        .persisted
        .persisted_index
        .read()
        .runtime_metadata_delta_series_ids
        .iter()
        .collect()
}

fn pending_series_ids(storage: &ChunkStorage) -> Vec<u64> {
    storage
        .catalog
        .pending_series_ids
        .read()
        .iter()
        .copied()
        .collect()
}

fn new_ingest_test_storage_with_options(
    chunk_point_cap: usize,
    mut options: ChunkStorageOptions,
) -> ChunkStorage {
    options.retention_enforced = false;
    options.background_threads_enabled = false;
    options.background_fail_fast = false;
    ChunkStorage::new_with_data_path_and_options(chunk_point_cap, None, None, None, 1, options)
        .unwrap()
}

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
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].header.point_count, 2);
    assert_eq!(chunks[1].header.point_count, 2);

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.point_count(), 1);
}

#[test]
fn finalize_sorts_out_of_order_chunk_points() {
    let storage = new_ingest_test_storage(2);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].points.is_empty());
    let timestamps = chunks[0]
        .decode_points()
        .unwrap()
        .iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2]);
}

#[test]
fn select_reads_encoded_only_sealed_chunks() {
    let storage = new_ingest_test_storage(2);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].points.is_empty());
    drop(sealed);

    let points = storage.select("latency", &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
        ]
    );
}

#[test]
fn select_rejects_duplicate_label_names() {
    let storage = new_ingest_test_storage(2);

    let err = storage
        .select(
            "latency",
            &[Label::new("host", "a"), Label::new("host", "b")],
            0,
            10,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::TsinkError::InvalidLabel(message)
            if message.contains("duplicate label 'host'")
    ));
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
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().point_count(), 1);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
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
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().point_count(), 3);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    assert!(sealed.get(&series_id).is_none());
}

#[test]
fn cross_partition_out_of_order_writes_keep_multiple_active_heads() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(23, 23.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.partition_head_count(), 2);
    assert_eq!(state.point_count(), 6);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    assert!(sealed.get(&series_id).is_none());
    drop(sealed);

    let timestamps = storage
        .select("latency", &labels, 0, 30)
        .unwrap()
        .into_iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 2, 3, 21, 22, 23]);
}

#[test]
fn cross_partition_out_of_order_writes_fill_each_head_before_sealing() {
    let storage = new_ingest_test_storage_with_options(
        2,
        ChunkStorageOptions {
            partition_window: 10,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(23, 23.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 2);
    let ranges = chunks
        .iter()
        .map(|chunk| {
            (
                chunk.header.min_ts,
                chunk.header.max_ts,
                chunk.header.point_count,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(ranges, vec![(1, 2, 2), (21, 22, 2)]);
    drop(sealed);

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.partition_head_count(), 2);
    assert_eq!(state.point_count(), 2);
}

#[test]
fn bounded_late_writes_slide_the_active_partition_window_at_fanout_limit() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            max_active_partition_heads_per_series: 2,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
        ])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "latency",
            labels.clone(),
            DataPoint::new(11, 11.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.partition_head_count(), 2);
    assert_eq!(
        state.partition_heads.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(state.point_count(), 3);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.header.min_ts,
                    chunk.header.max_ts,
                    chunk.header.point_count,
                )
            })
            .collect::<Vec<_>>(),
        vec![(1, 1, 1)]
    );
    drop(sealed);

    let timestamps = storage
        .select("latency", &labels, 0, 30)
        .unwrap()
        .into_iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 11, 21, 22]);
}

#[test]
fn too_late_writes_reject_new_partition_heads_below_the_active_window() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            max_active_partition_heads_per_series: 2,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(11, 11.0)),
        ])
        .unwrap();
    let err = storage
        .insert_rows(&[Row::with_labels(
            "latency",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::LateWritePartitionFanoutExceeded {
            timestamp: 1,
            partition_id: 0,
            max_active_partition_heads_per_series: 2,
            oldest_active_partition_id: 1,
            newest_active_partition_id: 2,
        }
    ));

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.partition_head_count(), 2);
    assert_eq!(
        state.partition_heads.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(state.point_count(), 3);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    assert!(sealed.get(&series_id).is_none());
    drop(sealed);

    let timestamps = storage
        .select("latency", &labels, 0, 30)
        .unwrap()
        .into_iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![11, 21, 22]);
}

#[test]
fn too_late_write_overflow_rejects_entire_batch_before_materializing_series() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            max_active_partition_heads_per_series: 2,
            metadata_shard_count: Some(8),
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    let err = storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(11, 11.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::LateWritePartitionFanoutExceeded {
            timestamp: 1,
            partition_id: 0,
            max_active_partition_heads_per_series: 2,
            oldest_active_partition_id: 1,
            newest_active_partition_id: 2,
        }
    ));

    let registry = storage.catalog.registry.read();
    assert!(registry.resolve_existing("latency", &labels).is_none());
    drop(registry);

    assert!(storage
        .chunks
        .active_builders
        .iter()
        .all(|shard| shard.read().is_empty()));
    assert!(storage
        .chunks
        .sealed_chunks
        .iter()
        .all(|shard| shard.read().is_empty()));
    assert!(pending_series_ids(&storage).is_empty());
    assert!(storage.materialized_series_snapshot().is_empty());
    assert!(runtime_metadata_delta_series_ids(&storage).is_empty());
    let metadata_shard_buckets = storage
        .catalog
        .metadata_shard_index
        .as_ref()
        .unwrap()
        .series_ids_by_shard
        .read();
    assert!(metadata_shard_buckets
        .iter()
        .all(|bucket| bucket.is_empty()));
}

#[test]
fn wal_stage_failure_rolls_back_new_series_visibility_and_metadata_publication() {
    use crate::engine::storage_engine::WAL_DIR_NAME;
    use crate::engine::wal::FramedWal;
    use crate::wal::WalSyncMode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            metadata_shard_count: Some(8),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let labels = vec![Label::new("host", "a")];
    let failed = Arc::new(AtomicBool::new(false));

    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_append_sync_hook({
            let failed = Arc::clone(&failed);
            move || {
                if failed.swap(true, Ordering::SeqCst) {
                    return Ok(());
                }

                Err(TsinkError::Other("injected WAL sync failure".to_string()))
            }
        });

    let err = storage
        .insert_rows(&[Row::with_labels(
            "wal_stage_failure_metric",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(failed.load(Ordering::SeqCst));
    assert!(
        err.to_string().contains("injected WAL sync failure"),
        "unexpected WAL stage error: {err:?}"
    );

    let registry = storage.catalog.registry.read();
    assert!(registry
        .resolve_existing("wal_stage_failure_metric", &labels)
        .is_none());
    drop(registry);

    assert!(storage
        .chunks
        .active_builders
        .iter()
        .all(|shard| shard.read().is_empty()));
    assert!(storage
        .chunks
        .sealed_chunks
        .iter()
        .all(|shard| shard.read().is_empty()));
    assert!(pending_series_ids(&storage).is_empty());
    assert!(storage.materialized_series_snapshot().is_empty());
    assert!(runtime_metadata_delta_series_ids(&storage).is_empty());
    assert!(storage.list_metrics().unwrap().is_empty());

    let metadata_shard_buckets = storage
        .catalog
        .metadata_shard_index
        .as_ref()
        .unwrap()
        .series_ids_by_shard
        .read();
    assert!(metadata_shard_buckets
        .iter()
        .all(|bucket| bucket.is_empty()));

    storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_append_sync_hook();
}

#[test]
fn first_write_populates_pending_series_and_live_metadata_indexes() {
    let shard_count = 8;
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            metadata_shard_count: Some(shard_count),
            ..ChunkStorageOptions::default()
        },
    );
    let metric = "cpu";
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;

    assert_eq!(pending_series_ids(&storage), vec![series_id]);
    assert_eq!(storage.materialized_series_snapshot(), vec![series_id]);
    assert_eq!(runtime_metadata_delta_series_ids(&storage), vec![series_id]);

    let metadata_index = storage.catalog.metadata_shard_index.as_ref().unwrap();
    let target_shard = metadata_index.shard_for_series(metric, &labels);
    let shard_scope = crate::storage::MetadataShardScope::new(shard_count, vec![target_shard]);
    assert_eq!(
        storage.list_metrics_in_shards(&shard_scope).unwrap(),
        vec![MetricSeries {
            name: metric.to_string(),
            labels: labels.clone(),
        }]
    );
    assert_eq!(
        storage
            .select_series_in_shards(&SeriesSelection::new().with_metric(metric), &shard_scope)
            .unwrap(),
        vec![MetricSeries {
            name: metric.to_string(),
            labels: labels.clone(),
        }]
    );

    let metadata_shard_buckets = metadata_index.series_ids_by_shard.read();
    assert_eq!(
        metadata_shard_buckets[target_shard as usize]
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![series_id]
    );
}

#[test]
fn newer_partition_rolls_forward_by_sealing_the_oldest_active_head() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            max_active_partition_heads_per_series: 2,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
        ])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "latency",
            labels.clone(),
            DataPoint::new(31, 31.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(
        state.partition_heads.keys().copied().collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(state.point_count(), 3);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.header.min_ts,
                    chunk.header.max_ts,
                    chunk.header.point_count,
                )
            })
            .collect::<Vec<_>>(),
        vec![(1, 1, 1)]
    );
    drop(sealed);

    let timestamps = storage
        .select("latency", &labels, 0, 40)
        .unwrap()
        .into_iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 21, 22, 31]);
}

#[test]
fn late_writes_across_many_partitions_keep_partition_head_count_bounded() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            max_active_partition_heads_per_series: 3,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(11, 11.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(31, 31.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(41, 41.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(51, 51.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(state.partition_head_count(), 3);
    assert_eq!(
        state.partition_heads.keys().copied().collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 3);
    drop(sealed);

    let timestamps = storage
        .select("latency", &labels, 0, 60)
        .unwrap()
        .into_iter()
        .map(|point| point.timestamp)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![1, 11, 21, 31, 41, 51]);
}

#[test]
fn flush_all_active_drains_all_cross_partition_heads() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("latency", labels.clone(), DataPoint::new(21, 21.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(22, 22.0)),
            Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("latency", &labels)
        .unwrap()
        .series_id;

    storage.flush_all_active().unwrap();

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(
        active
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0
    );
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    let ranges = chunks
        .iter()
        .map(|chunk| {
            (
                chunk.header.min_ts,
                chunk.header.max_ts,
                chunk.header.point_count,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(ranges, vec![(1, 2, 2), (21, 22, 2)]);
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
        .catalog
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
                value_family: Some(SeriesValueFamily::F64),
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
            wal_highwater: WalHighWatermark::default(),
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

#[test]
fn legacy_append_point_to_series_respects_chunk_cap() {
    let storage = new_ingest_test_storage(2);
    let labels = vec![Label::new("host", "a")];
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_or_insert("cpu", &labels)
        .unwrap()
        .series_id;

    storage
        .append_point_to_series(series_id, ValueLane::Numeric, 1, Value::F64(1.0))
        .unwrap();
    storage
        .append_point_to_series(series_id, ValueLane::Numeric, 2, Value::F64(2.0))
        .unwrap();
    storage
        .append_point_to_series(series_id, ValueLane::Numeric, 3, Value::F64(3.0))
        .unwrap();

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].header.point_count, 2);
    drop(sealed);

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().point_count(), 1);
}

#[test]
fn legacy_append_point_to_series_respects_partition_window_and_head_limit() {
    let storage = new_ingest_test_storage_with_options(
        8,
        ChunkStorageOptions {
            partition_window: 10,
            max_active_partition_heads_per_series: 2,
            ..ChunkStorageOptions::default()
        },
    );
    let labels = vec![Label::new("host", "a")];
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_or_insert("latency", &labels)
        .unwrap()
        .series_id;

    for (ts, value) in [(21, 21.0), (22, 22.0), (1, 1.0), (11, 11.0)] {
        storage
            .append_point_to_series(series_id, ValueLane::Numeric, ts, Value::F64(value))
            .unwrap();
    }

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    let state = active.get(&series_id).unwrap();
    assert_eq!(
        state.partition_heads.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(state.point_count(), 3);
    drop(active);

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.header.min_ts,
                    chunk.header.max_ts,
                    chunk.header.point_count,
                )
            })
            .collect::<Vec<_>>(),
        vec![(1, 1, 1)]
    );
}

#[test]
fn legacy_append_point_to_series_rolls_back_family_assignment_on_apply_error() {
    let storage = new_ingest_test_storage(4);
    let labels = vec![Label::new("host", "a")];
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_or_insert("events", &labels)
        .unwrap()
        .series_id;
    let shard_idx = ChunkStorage::series_shard_idx(series_id);
    storage.chunks.active_builders[shard_idx].write().insert(
        series_id,
        ActiveSeriesState::new(series_id, ValueLane::Blob, 4),
    );

    let err = storage
        .append_point_to_series(series_id, ValueLane::Numeric, 1, Value::F64(1.0))
        .unwrap_err();

    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    assert_eq!(
        storage
            .catalog
            .registry
            .read()
            .series_value_family(series_id),
        None
    );
}
