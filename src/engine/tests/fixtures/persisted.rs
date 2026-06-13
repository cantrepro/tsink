use super::super::*;
use super::base_storage_test_options;

pub(in crate::engine::storage_engine::tests) fn persistent_numeric_storage(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
) -> ChunkStorage {
    persistent_numeric_storage_with_chunk_cap(root, timestamp_precision, chunk_point_cap)
}

pub(in crate::engine::storage_engine::tests) fn persistent_numeric_storage_with_chunk_cap(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        base_storage_test_options(timestamp_precision, None),
    )
    .unwrap()
}

pub(in crate::engine::storage_engine::tests) fn reopen_persistent_numeric_storage(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
) -> ChunkStorage {
    reopen_persistent_numeric_storage_with_chunk_cap(root, timestamp_precision, chunk_point_cap)
}

pub(in crate::engine::storage_engine::tests) fn reopen_persistent_numeric_storage_with_chunk_cap(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
) -> ChunkStorage {
    let storage =
        persistent_numeric_storage_with_chunk_cap(root, timestamp_precision, chunk_point_cap);
    storage.load_tombstones_index().unwrap();
    storage
        .apply_loaded_segment_indexes(
            load_segment_indexes(root.join(NUMERIC_LANE_ROOT)).unwrap(),
            false,
        )
        .unwrap();
    storage.reconcile_live_metadata_indexes().unwrap();
    storage
}

pub(in crate::engine::storage_engine::tests) fn persistent_numeric_storage_with_metadata_shards(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
    metadata_shard_count: u32,
) -> ChunkStorage {
    let mut options = base_storage_test_options(timestamp_precision, None);
    options.metadata_shard_count = Some(metadata_shard_count);
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap()
}

pub(in crate::engine::storage_engine::tests) fn live_numeric_storage_with_config(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
    current_time_override: i64,
    retention: Duration,
    metadata_shard_count: Option<u32>,
) -> ChunkStorage {
    let mut options = base_storage_test_options(timestamp_precision, Some(current_time_override));
    options.retention_window =
        super::super::super::duration_to_timestamp_units(retention, timestamp_precision);
    options.metadata_shard_count = metadata_shard_count;
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap()
}

pub(in crate::engine::storage_engine::tests) fn persistent_rollup_storage(
    root: &Path,
) -> Arc<ChunkStorage> {
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;

    Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(root.join(NUMERIC_LANE_ROOT)),
            Some(root.join(BLOB_LANE_ROOT)),
            1,
            options,
        )
        .unwrap(),
    )
}

pub(in crate::engine::storage_engine::tests) fn reopen_persistent_rollup_storage(
    root: &Path,
) -> Arc<ChunkStorage> {
    let storage = persistent_rollup_storage(root);
    storage.load_tombstones_index().unwrap();

    let snapshot_path = root.join(SERIES_INDEX_FILE_NAME);
    let loaded_registry = SeriesRegistry::load_persisted_state(&snapshot_path).unwrap();
    let reconcile_registry_with_persisted = loaded_registry.is_some();
    if let Some(loaded_registry) = loaded_registry {
        storage
            .replace_registry_from_persisted_state(
                loaded_registry.registry,
                loaded_registry.delta_series_count,
            )
            .unwrap();
    }

    let loaded_segments = merge_loaded_segment_indexes(
        load_segment_indexes(root.join(NUMERIC_LANE_ROOT)).unwrap(),
        load_segment_indexes(root.join(BLOB_LANE_ROOT)).unwrap(),
        true,
        true,
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(loaded_segments, reconcile_registry_with_persisted)
        .unwrap();
    storage.load_rollup_runtime_state().unwrap();
    storage.reconcile_live_metadata_indexes().unwrap();
    storage.refresh_segment_catalog_and_observability().unwrap();
    storage
}

pub(in crate::engine::storage_engine::tests) fn open_raw_numeric_storage_from_data_path(
    data_path: &Path,
) -> ChunkStorage {
    open_raw_numeric_storage_from_data_path_with_background_fail_fast(data_path, false)
}

pub(in crate::engine::storage_engine::tests) fn open_raw_numeric_storage_from_data_path_with_background_fail_fast(
    data_path: &Path,
    background_fail_fast: bool,
) -> ChunkStorage {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let loaded_segments = load_segment_indexes(&lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path),
        None,
        loaded_segments.next_segment_id,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            background_fail_fast,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(loaded_segments, false)
        .unwrap();
    storage
}

pub(in crate::engine::storage_engine::tests) fn open_raw_numeric_storage_with_registry_snapshot_from_data_path(
    data_path: &Path,
) -> ChunkStorage {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let snapshot_path = data_path.join(SERIES_INDEX_FILE_NAME);
    let loaded_registry = SeriesRegistry::load_persisted_state(&snapshot_path).unwrap();
    let reconcile_registry_with_persisted = loaded_registry.is_some();
    let loaded_segments = load_segment_indexes(&lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path),
        None,
        loaded_segments.next_segment_id,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    if let Some(loaded_registry) = loaded_registry {
        storage
            .replace_registry_from_persisted_state(
                loaded_registry.registry,
                loaded_registry.delta_series_count,
            )
            .unwrap();
    }
    storage
        .apply_loaded_segment_indexes(loaded_segments, reconcile_registry_with_persisted)
        .unwrap();
    storage
}
