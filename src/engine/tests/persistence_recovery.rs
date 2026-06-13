use super::*;
use crate::WriteAcknowledgement;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn write_startup_persisted_segment(data_path: &Path, metric: &str) -> PathBuf {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );

    let writer = SegmentWriter::new(&lane_path, 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    writer.layout().root.clone()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplaySeriesStateSnapshot {
    metric: String,
    labels: Vec<Label>,
    family: Option<crate::engine::series::SeriesValueFamily>,
    active_points: Vec<DataPoint>,
    sealed_chunks: Vec<Vec<DataPoint>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayStorageStateSnapshot {
    materialized_series_ids: Vec<u64>,
    series_by_id: BTreeMap<u64, ReplaySeriesStateSnapshot>,
}

fn replay_storage_state_snapshot(storage: &ChunkStorage) -> ReplayStorageStateSnapshot {
    let series_definitions = {
        let registry = storage.catalog.registry.read();
        registry
            .all_series_ids()
            .into_iter()
            .map(|series_id| {
                (
                    series_id,
                    registry.decode_series_key(series_id).unwrap(),
                    registry.series_value_family(series_id),
                )
            })
            .collect::<Vec<_>>()
    };

    let mut series_by_id = BTreeMap::new();
    for (series_id, series_key, family) in series_definitions {
        let active_points = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map(|state| {
                state
                    .points_in_partition_order()
                    .map(|point| DataPoint::new(point.ts, point.value.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let sealed_chunks = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map(|chunks| {
                chunks
                    .values()
                    .map(|chunk| {
                        chunk
                            .decode_points()
                            .unwrap()
                            .into_iter()
                            .map(|point| DataPoint::new(point.ts, point.value))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        series_by_id.insert(
            series_id,
            ReplaySeriesStateSnapshot {
                metric: series_key.metric,
                labels: series_key.labels,
                family,
                active_points,
                sealed_chunks,
            },
        );
    }

    let mut materialized_series_ids = storage.materialized_series_snapshot();
    materialized_series_ids.sort_unstable();

    ReplayStorageStateSnapshot {
        materialized_series_ids,
        series_by_id,
    }
}

fn select_metric_series_sorted(storage: &ChunkStorage, metric: &str) -> Vec<MetricSeries> {
    let mut selected = storage
        .select_series(&SeriesSelection::new().with_metric(metric))
        .unwrap();
    selected.sort();
    selected
}

fn rewrite_wal_to_durable_writes(wal_dir: &Path, durable_highwater: WalHighWatermark) {
    let durable_writes = FramedWal::open(wal_dir, WalSyncMode::PerAppend)
        .unwrap()
        .replay_committed_writes_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Salvage,
        )
        .unwrap()
        .into_iter()
        .take_while(|write| write.highwater <= durable_highwater)
        .collect::<Vec<_>>();

    if wal_dir.exists() {
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    let rebuilt = FramedWal::open(wal_dir, WalSyncMode::PerAppend).unwrap();
    for write in durable_writes {
        for definition in write.series_definitions {
            rebuilt.append_series_definition(&definition).unwrap();
        }
        if !write.sample_batches.is_empty() {
            rebuilt.append_samples(&write.sample_batches).unwrap();
        }
    }
}

fn new_wal_only_storage(wal_dir: &Path, sync_mode: WalSyncMode) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        Some(FramedWal::open(wal_dir, sync_mode).unwrap()),
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn flip_first_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    assert!(
        !bytes.is_empty(),
        "expected {} to be non-empty",
        path.display()
    );
    bytes[0] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn flip_last_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let last = bytes
        .len()
        .checked_sub(1)
        .unwrap_or_else(|| panic!("expected {} to be non-empty", path.display()));
    bytes[last] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn replace_first_chunk_payload_with_invalid_encoded_payload(segment_root: &Path) {
    const CHUNKS_FILE_HEADER_LEN: usize = 16;
    const CHUNK_RECORD_FIXED_HEADER_LEN: usize = 4 + 4 + 34;
    const CHUNK_FLAGS_OFFSET: usize = CHUNKS_FILE_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1;
    const PAYLOAD_LEN_OFFSET: usize =
        CHUNKS_FILE_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1 + 1 + 2 + 8 + 8;
    const MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET: usize = 96 + 12;

    let chunks_path = segment_root.join("chunks.bin");
    let mut chunks_bytes = std::fs::read(&chunks_path).unwrap();
    assert_eq!(
        chunks_bytes[CHUNK_FLAGS_OFFSET], 0,
        "test helper expects the first chunk payload to be stored uncompressed"
    );

    let payload_len = u32::from_le_bytes(
        chunks_bytes[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let payload_offset = CHUNKS_FILE_HEADER_LEN + CHUNK_RECORD_FIXED_HEADER_LEN;
    let payload_end = payload_offset + payload_len;
    assert!(
        payload_len >= 4,
        "encoded chunk payload should be large enough to corrupt"
    );

    let invalid_ts_len = u32::try_from(payload_len).unwrap().saturating_add(1);
    chunks_bytes[payload_offset..payload_offset + 4].copy_from_slice(&invalid_ts_len.to_le_bytes());
    let payload_crc = crate::engine::binio::checksum32(&chunks_bytes[payload_offset..payload_end]);
    chunks_bytes[payload_end..payload_end + 4].copy_from_slice(&payload_crc.to_le_bytes());
    std::fs::write(&chunks_path, &chunks_bytes).unwrap();

    let manifest_path = segment_root.join("manifest.bin");
    let mut manifest_bytes = std::fs::read(&manifest_path).unwrap();
    let chunks_hash = xxhash_rust::xxh64::xxh64(&chunks_bytes, 0);
    manifest_bytes
        [MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET..MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET + 8]
        .copy_from_slice(&chunks_hash.to_le_bytes());
    let crc_offset = manifest_bytes.len() - 4;
    let manifest_crc = crate::engine::binio::checksum32(&manifest_bytes[..crc_offset]);
    manifest_bytes[crc_offset..].copy_from_slice(&manifest_crc.to_le_bytes());
    std::fs::write(manifest_path, manifest_bytes).unwrap();
}

fn startup_quarantined_segment_paths(level_root: &Path) -> Vec<PathBuf> {
    let prefix = format!(
        ".tmp-tsink-{}-",
        crate::engine::segment::STARTUP_SEGMENT_QUARANTINE_PURPOSE
    );
    let mut roots = match std::fs::read_dir(level_root) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }
                let name = entry.file_name();
                let name = name.to_str()?;
                name.starts_with(&prefix).then_some(entry.path())
            })
            .collect::<Vec<_>>(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("failed to list {level_root:?}: {err}"),
    };
    roots.sort();
    roots
}

fn assert_startup_segment_is_quarantined(
    storage: &Arc<dyn Storage>,
    segment_root: &Path,
    metric: &str,
    expected_detail: &str,
) {
    let labels = vec![Label::new("host", "startup")];
    assert!(
        storage.select(metric, &labels, 0, 100).unwrap().is_empty(),
        "quarantined startup segment should not remain queryable"
    );
    let quarantined = startup_quarantined_segment_paths(
        segment_root
            .parent()
            .unwrap_or_else(|| panic!("missing parent for {}", segment_root.display())),
    );
    assert_eq!(quarantined.len(), 1);
    assert!(
        !segment_root.exists(),
        "corrupt startup segment root should be moved out of the visible inventory"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert_eq!(health.maintenance_errors_total, 1);
    let message = health
        .last_maintenance_error
        .unwrap_or_else(|| "missing startup recovery error".to_string());
    assert!(
        message.contains(&segment_root.display().to_string()),
        "expected startup recovery error to include segment path, got: {message}"
    );
    assert!(
        message.contains(&quarantined[0].display().to_string()),
        "expected startup recovery error to include quarantine path, got: {message}"
    );
    assert!(
        message.contains(expected_detail),
        "expected startup recovery error to include detail '{expected_detail}', got: {message}"
    );
}

fn assert_runtime_refresh_failure(err: TsinkError, segment_root: &Path, expected_detail: &str) {
    let segment_display = segment_root.display().to_string();
    match err {
        TsinkError::DataCorruption(message) => {
            assert!(
                message.contains("runtime refresh"),
                "expected runtime refresh failure context, got: {message}"
            );
            assert!(
                message.contains(&segment_display),
                "expected runtime refresh error to include segment path {segment_display}, got: {message}"
            );
            assert!(
                message.contains(expected_detail),
                "expected runtime refresh error to include detail '{expected_detail}', got: {message}"
            );
        }
        other => panic!("expected runtime refresh corruption error, got {other:?}"),
    }
}

fn write_runtime_refresh_segment(
    data_path: &Path,
    metric: &str,
    series_id: u64,
    segment_id: u64,
    points: &[(i64, f64)],
) -> PathBuf {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    registry
        .register_series_with_id(series_id, metric, &labels)
        .unwrap();
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, points)],
    );
    let writer = SegmentWriter::new(&lane_path, 0, segment_id).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    writer.layout().root.clone()
}

fn assert_explicit_runtime_refresh_rejects_corrupted_added_segment(
    metric: &str,
    corrupt_file: &str,
    corrupt_file_fn: fn(&Path),
    expected_detail: &str,
) {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let good_root = write_startup_persisted_segment(temp_dir.path(), metric);
    let storage = open_raw_numeric_storage_from_data_path(temp_dir.path());
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    let corrupt_root =
        write_runtime_refresh_segment(temp_dir.path(), metric, series_id, 2, &[(3, 3.0)]);
    corrupt_file_fn(&corrupt_root.join(corrupt_file));

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    let err = storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap_err();
    assert_runtime_refresh_failure(err, &corrupt_root, expected_detail);
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    let persisted_roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(persisted_roots, vec![good_root]);
}

#[test]
fn recovers_rows_from_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let now = 100i64;

    {
        let storage = builder_at_time(now)
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

    let reopened = builder_at_time(now)
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
fn per_append_acknowledged_rows_survive_simulated_crash_window() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let storage = new_wal_only_storage(&wal_dir, WalSyncMode::PerAppend);

    let write_result = storage
        .insert_rows_with_result(&[Row::new("per_append_crash_window", DataPoint::new(1, 1.0))])
        .unwrap();
    assert_eq!(write_result.acknowledgement, WriteAcknowledgement::Durable);

    let wal = storage.persisted.wal.as_ref().unwrap();
    let appended_highwater = wal.current_appended_highwater();
    let durable_highwater = wal.current_durable_highwater();
    assert_eq!(durable_highwater, appended_highwater);

    drop(storage);
    rewrite_wal_to_durable_writes(&wal_dir, durable_highwater);

    let reopened = new_wal_only_storage(&wal_dir, WalSyncMode::PerAppend);
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();
    assert_eq!(
        reopened
            .select("per_append_crash_window", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
}

#[test]
fn periodic_acknowledged_rows_can_be_lost_in_simulated_crash_window() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let storage = new_wal_only_storage(&wal_dir, WalSyncMode::Periodic(Duration::from_secs(3600)));

    let write_result = storage
        .insert_rows_with_result(&[Row::new("periodic_crash_window", DataPoint::new(1, 1.0))])
        .unwrap();
    assert_eq!(write_result.acknowledgement, WriteAcknowledgement::Appended);

    let wal = storage.persisted.wal.as_ref().unwrap();
    let appended_highwater = wal.current_appended_highwater();
    let durable_highwater = wal.current_durable_highwater();
    assert!(appended_highwater > WalHighWatermark::default());
    assert_eq!(durable_highwater, WalHighWatermark::default());

    drop(storage);
    rewrite_wal_to_durable_writes(&wal_dir, durable_highwater);

    let reopened = new_wal_only_storage(&wal_dir, WalSyncMode::Periodic(Duration::from_secs(3600)));
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();
    assert!(
        reopened
            .select("periodic_crash_window", &[], 0, 10)
            .unwrap()
            .is_empty(),
        "periodic mode should expose a crash window until WAL sync or persistence makes the write durable"
    );
}

#[test]
fn batched_wal_replay_matches_direct_ingest_state_for_mixed_series_and_lanes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let now = 100i64;
    let expected = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            background_threads_enabled: false,
            current_time_override: Some(now),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let writes = vec![
        vec![
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "a")],
                DataPoint::new(10, 1.0),
            ),
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "b")],
                DataPoint::new(10, 10.0),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "a")],
                DataPoint::new(10, Value::String("cold".to_string())),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "b")],
                DataPoint::new(10, Value::Bytes(vec![1, 2])),
            ),
        ],
        vec![
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "a")],
                DataPoint::new(11, 2.0),
            ),
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "b")],
                DataPoint::new(11, 11.0),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "a")],
                DataPoint::new(11, Value::String("warm".to_string())),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "b")],
                DataPoint::new(11, Value::Bytes(vec![3, 4])),
            ),
        ],
        vec![
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "a")],
                DataPoint::new(12, 3.0),
            ),
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "b")],
                DataPoint::new(12, 12.0),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "a")],
                DataPoint::new(12, Value::String("hot".to_string())),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "b")],
                DataPoint::new(12, Value::Bytes(vec![5, 6])),
            ),
        ],
    ];

    for rows in &writes {
        expected.insert_rows(rows).unwrap();
    }
    let expected_snapshot = replay_storage_state_snapshot(&expected);
    let expected_numeric_series = select_metric_series_sorted(&expected, "replay_batch_cpu");
    let expected_blob_series = select_metric_series_sorted(&expected, "replay_batch_log");

    {
        let wal_storage = ChunkStorage::new_with_data_path_and_options(
            2,
            Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
            None,
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                current_time_override: Some(now),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        for rows in &writes {
            wal_storage.insert_rows(rows).unwrap();
        }
    }

    let replayed = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
        None,
        None,
        1,
        ChunkStorageOptions {
            background_threads_enabled: false,
            current_time_override: Some(now),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    replayed
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();

    assert_eq!(replay_storage_state_snapshot(&replayed), expected_snapshot);
    assert_eq!(
        select_metric_series_sorted(&replayed, "replay_batch_cpu"),
        expected_numeric_series
    );
    assert_eq!(
        select_metric_series_sorted(&replayed, "replay_batch_log"),
        expected_blob_series
    );
}

#[test]
fn wal_replay_policy_strict_vs_salvage_applies_later_valid_frames_after_checksum_corruption() {
    let labels = vec![Label::new("host", "a")];
    let seed_corrupt_wal = |data_path: &Path| {
        let wal_dir = data_path.join(WAL_DIR_NAME);
        let definition = SeriesDefinitionFrame {
            series_id: 1,
            metric: "replay_policy".to_string(),
            labels: labels.clone(),
        };
        let first_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            }],
        )
        .unwrap();
        let corrupt_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            }],
        )
        .unwrap();
        let later_clean_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 3,
                value: Value::F64(3.0),
            }],
        )
        .unwrap();
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&definition).unwrap();
        wal.append_samples(std::slice::from_ref(&first_batch))
            .unwrap();
        wal.append_samples(&[corrupt_batch]).unwrap();
        wal.append_samples(&[later_clean_batch]).unwrap();

        let definition_frame_bytes =
            FramedWal::estimate_series_definition_frame_bytes(&definition).unwrap();
        let sample_frame_bytes =
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&first_batch)).unwrap();
        let checksum_offset = definition_frame_bytes + sample_frame_bytes + 20;
        let corrupt_segment_path = wal.path();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&corrupt_segment_path)
            .unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        let mut checksum = [0u8; 4];
        file.read_exact(&mut checksum).unwrap();
        checksum[0] ^= 0xff;
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        file.write_all(&checksum).unwrap();
        file.flush().unwrap();
        wal_dir
    };

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = seed_corrupt_wal(temp_dir.path());

    let strict_storage = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    let strict_err = strict_storage
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap_err();
    assert!(matches!(
        strict_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 3")
                && message.contains("checksum mismatch")
    ));

    let strict_startup_dir = TempDir::new().unwrap();
    seed_corrupt_wal(strict_startup_dir.path());
    let startup_err = match StorageBuilder::new()
        .with_data_path(strict_startup_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
    {
        Ok(_) => panic!("expected default startup replay to fail on WAL corruption"),
        Err(err) => err,
    };
    assert!(matches!(
        startup_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 3")
                && message.contains("checksum mismatch")
    ));

    let replayed_timestamps = FramedWal::open(&wal_dir, WalSyncMode::PerAppend)
        .unwrap()
        .replay_committed_writes_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Salvage,
        )
        .unwrap()
        .into_iter()
        .flat_map(|write| write.sample_batches)
        .map(|batch| batch.decode_points().unwrap()[0].ts)
        .collect::<Vec<_>>();
    assert_eq!(replayed_timestamps, vec![1, 3]);

    let reopened = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();
    let series_id = reopened
        .catalog
        .registry
        .read()
        .resolve_existing("replay_policy", &labels)
        .unwrap()
        .series_id;
    let active = reopened.active_shard(series_id).read();
    let points = active
        .get(&series_id)
        .unwrap()
        .points_in_partition_order()
        .map(|point| DataPoint::new(point.ts, point.value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(points, vec![DataPoint::new(1, 1.0), DataPoint::new(3, 3.0)]);

    let salvage_startup_dir = TempDir::new().unwrap();
    seed_corrupt_wal(salvage_startup_dir.path());
    let salvaged = StorageBuilder::new()
        .with_data_path(salvage_startup_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .with_wal_replay_mode(WalReplayMode::Salvage)
        .build()
        .unwrap();
    assert_eq!(
        salvaged.select("replay_policy", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(3, 3.0)]
    );
    salvaged
        .insert_rows(&[Row::with_labels(
            "replay_policy",
            labels.clone(),
            DataPoint::new(4, 4.0),
        )])
        .unwrap();
    salvaged.close().unwrap();

    let salvaged_reopened = StorageBuilder::new()
        .with_data_path(salvage_startup_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .with_wal_replay_mode(WalReplayMode::Salvage)
        .build()
        .unwrap();
    assert_eq!(
        salvaged_reopened
            .select("replay_policy", &labels, 0, 10)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
        ]
    );
    salvaged_reopened.close().unwrap();
}

#[test]
fn list_metrics_with_wal_rebuild_uses_configured_salvage_mode_on_corrupt_wal() {
    let labels = vec![Label::new("host", "a")];
    let metric = "replay_policy";
    let seed_corrupt_wal = |data_path: &Path| {
        let wal_dir = data_path.join(WAL_DIR_NAME);
        let definition = SeriesDefinitionFrame {
            series_id: 1,
            metric: metric.to_string(),
            labels: labels.clone(),
        };
        let first_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            }],
        )
        .unwrap();
        let corrupt_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            }],
        )
        .unwrap();
        let later_clean_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 3,
                value: Value::F64(3.0),
            }],
        )
        .unwrap();
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&definition).unwrap();
        wal.append_samples(std::slice::from_ref(&first_batch))
            .unwrap();
        wal.append_samples(&[corrupt_batch]).unwrap();
        wal.append_samples(&[later_clean_batch]).unwrap();

        let definition_frame_bytes =
            FramedWal::estimate_series_definition_frame_bytes(&definition).unwrap();
        let sample_frame_bytes =
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&first_batch)).unwrap();
        let checksum_offset = definition_frame_bytes + sample_frame_bytes + 20;
        let corrupt_segment_path = wal.path();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&corrupt_segment_path)
            .unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        let mut checksum = [0u8; 4];
        file.read_exact(&mut checksum).unwrap();
        checksum[0] ^= 0xff;
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        file.write_all(&checksum).unwrap();
        file.flush().unwrap();
        wal_dir
    };

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = seed_corrupt_wal(temp_dir.path());

    let strict_storage = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    strict_storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .invalidate_cached_series_definition_snapshot_for_test();
    let strict_err = strict_storage.list_metrics_with_wal().unwrap_err();
    assert!(matches!(
        strict_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 3")
                && message.contains("checksum mismatch")
    ));

    let salvage_storage = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    assert!(salvage_storage.list_metrics().unwrap().is_empty());
    salvage_storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_configured_replay_mode(WalReplayMode::Salvage);
    salvage_storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .invalidate_cached_series_definition_snapshot_for_test();
    assert_eq!(
        salvage_storage.list_metrics_with_wal().unwrap(),
        vec![MetricSeries {
            name: metric.to_string(),
            labels: labels.clone(),
        }]
    );

    strict_storage.close().unwrap();
    salvage_storage.close().unwrap();
}

#[test]
fn stale_wal_with_already_persisted_points_does_not_duplicate_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let now = 100i64;

    let stale_frames = {
        let storage = builder_at_time(now)
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

    let reopened = builder_at_time(now)
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
    let now = 100i64;

    let stale_frames = {
        let storage = builder_at_time(now)
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

    let reopened = builder_at_time(now)
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
fn startup_replays_only_logically_published_wal_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();

    let encode_write = |series_id: u64, metric: &str, ts: i64, value: f64| {
        let definition = SeriesDefinitionFrame {
            series_id,
            metric: metric.to_string(),
            labels: labels.clone(),
        };
        let batch = SamplesBatchFrame::from_points(
            series_id,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts,
                value: Value::F64(value),
            }],
        )
        .unwrap();
        let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&definition)
            .unwrap()
            .saturating_add(
                FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&batch)).unwrap(),
            );
        (
            estimated_bytes,
            FramedWal::encode_series_definition_frame_payload(&definition).unwrap(),
            FramedWal::encode_samples_frame_payload(std::slice::from_ref(&batch)).unwrap(),
        )
    };

    let (estimated_bytes, definition_payload, samples_payload) =
        encode_write(1, "wal_published_boundary", 1, 1.0);
    let mut published = wal.begin_logical_write(estimated_bytes).unwrap();
    published
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    published.append_samples_payload(&samples_payload).unwrap();
    let published_highwater = published.persist_pending().unwrap();
    assert_eq!(
        published.publish_persisted().unwrap(),
        published_highwater,
        "published logical writes should advance the WAL visibility marker once",
    );

    let (estimated_bytes, definition_payload, samples_payload) =
        encode_write(2, "wal_staged_boundary", 2, 2.0);
    let mut staged = wal.begin_logical_write(estimated_bytes).unwrap();
    staged
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    staged.append_samples_payload(&samples_payload).unwrap();
    let staged_highwater = staged.persist_pending().unwrap();
    assert!(
        staged_highwater > published_highwater,
        "the staged write should occupy a later WAL position than the published write",
    );
    std::mem::forget(staged);
    drop(wal);

    // Invariant: restart recovery only replays writes that crossed the logical WAL
    // publish boundary, even if later writes already persisted their bytes.
    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("wal_published_boundary", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert!(
        reopened
            .select("wal_staged_boundary", &labels, 0, 10)
            .unwrap()
            .is_empty(),
        "recovery must ignore a write that persisted WAL bytes without publishing them",
    );
    assert!(
        !reopened
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == "wal_staged_boundary" && series.labels == labels),
        "staged WAL metadata must stay hidden across restart until publication",
    );

    let reopened_wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
    assert_eq!(
        reopened_wal.current_published_highwater(),
        published_highwater
    );
    let committed = reopened_wal
        .replay_committed_writes_after(WalHighWatermark::default())
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].highwater, published_highwater);

    reopened.close().unwrap();
}

#[test]
fn flush_pipeline_post_publish_load_failure_replays_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let now = 100i64;

    {
        let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                current_time_override: Some(now),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "flush_recovery",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        storage.set_persist_post_publish_hook(|roots| {
            let manifest_path = roots[0].join("manifest.bin");
            std::fs::remove_file(manifest_path).unwrap();
        });

        let err = storage.flush_pipeline_once().unwrap_err();
        assert!(matches!(
            err,
            TsinkError::DataCorruption(message) if message.contains("missing manifest.bin")
        ));
        storage.clear_persist_post_publish_hook();
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select("flush_recovery", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn close_post_publish_load_failure_replays_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let now = 100i64;

    {
        let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                current_time_override: Some(now),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "close_recovery",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        storage.set_persist_post_publish_hook(|roots| {
            let manifest_path = roots[0].join("manifest.bin");
            std::fs::remove_file(manifest_path).unwrap();
        });

        let err = storage.close().unwrap_err();
        assert!(matches!(
            err,
            TsinkError::DataCorruption(message) if message.contains("missing manifest.bin")
        ));
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select("close_recovery", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn startup_quarantines_manifest_corruption_and_serves_healthy_data() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let healthy_metric = "startup_manifest_healthy";
    write_runtime_refresh_segment(temp_dir.path(), healthy_metric, 1, 1, &[(1, 1.0), (2, 2.0)]);
    let segment_root = write_runtime_refresh_segment(
        temp_dir.path(),
        "startup_manifest_corrupt",
        2,
        2,
        &[(3, 3.0)],
    );
    flip_first_byte(&segment_root.join("manifest.bin"));

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_startup_segment_is_quarantined(
        &storage,
        &segment_root,
        "startup_manifest_corrupt",
        "manifest crc32 mismatch",
    );
    storage.close().unwrap();
}

#[test]
fn startup_quarantines_chunk_index_corruption_and_serves_healthy_data() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let healthy_metric = "startup_index_healthy";
    write_runtime_refresh_segment(temp_dir.path(), healthy_metric, 1, 1, &[(1, 1.0), (2, 2.0)]);
    let segment_root =
        write_runtime_refresh_segment(temp_dir.path(), "startup_index_corrupt", 2, 2, &[(3, 3.0)]);
    flip_first_byte(&segment_root.join("chunk_index.bin"));

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_startup_segment_is_quarantined(
        &storage,
        &segment_root,
        "startup_index_corrupt",
        "manifest file hash mismatch for kind 2",
    );
    storage.close().unwrap();
}

#[test]
fn startup_quarantines_chunk_payload_corruption_and_serves_healthy_data() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let healthy_metric = "startup_chunk_healthy";
    write_runtime_refresh_segment(temp_dir.path(), healthy_metric, 1, 1, &[(1, 1.0), (2, 2.0)]);
    let segment_root =
        write_runtime_refresh_segment(temp_dir.path(), "startup_chunk_corrupt", 2, 2, &[(3, 3.0)]);
    flip_last_byte(&segment_root.join("chunks.bin"));

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_startup_segment_is_quarantined(
        &storage,
        &segment_root,
        "startup_chunk_corrupt",
        "manifest file hash mismatch for kind 1",
    );
    storage.close().unwrap();
}

#[test]
fn startup_loads_persisted_metadata_without_decoding_chunk_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "startup_lazy_metadata";
    let labels = vec![Label::new("host", "startup")];
    let segment_root = write_startup_persisted_segment(temp_dir.path(), metric);
    replace_first_chunk_payload_with_invalid_encoded_payload(&segment_root);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    storage
        .select_series(&SeriesSelection::new().with_metric(metric))
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(metric, labels, DataPoint::new(3, 3i64))])
        .unwrap_err();
    match err {
        TsinkError::ValueTypeMismatch { .. } => {}
        other => panic!("expected recovered value family to reject i64 writes, got {other:?}"),
    }

    storage.close().unwrap();
}

#[test]
fn startup_primes_list_metrics_with_wal_snapshot_without_replaying_wal_metadata() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let rebuild_called = Arc::new(AtomicBool::new(false));
    let metric = "startup_wal_metric_listing";
    let labels = vec![Label::new("host", "startup")];

    {
        let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(numeric_lane_path.clone()),
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
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels(metric, labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();
    }

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let reopened = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(numeric_lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    reopened
        .apply_loaded_segment_indexes(load_segment_indexes(&numeric_lane_path).unwrap(), false)
        .unwrap();
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap();

    reopened
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_cached_series_definition_rebuild_hook({
            let rebuild_called = Arc::clone(&rebuild_called);
            move || {
                rebuild_called.store(true, Ordering::SeqCst);
            }
        });

    let metrics = reopened.list_metrics_with_wal().unwrap();
    assert!(metrics
        .iter()
        .any(|series| series.name == metric && series.labels == labels));
    assert!(
        !rebuild_called.load(Ordering::SeqCst),
        "first post-restart list_metrics_with_wal call should use the startup-primed WAL metadata snapshot"
    );

    reopened
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_cached_series_definition_rebuild_hook();
    reopened.close().unwrap();
}

#[test]
fn startup_self_heals_corrupt_registry_checkpoint_after_segment_rebuild() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let metric = "startup_registry_self_heal";
    let labels = vec![Label::new("host", "startup")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    let now = 100i64;

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    flip_first_byte(&checkpoint_path);
    let corrupt_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    assert!(
        SeriesRegistry::load_persisted_state(&checkpoint_path).is_err(),
        "corrupted checkpoint should fail persisted registry loading before restart",
    );

    let rebuilt = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        rebuilt.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    let healed_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    assert_ne!(healed_checkpoint_bytes, corrupt_checkpoint_bytes);
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed checkpoint should reload without segment rebuild");
    assert_eq!(loaded_registry.delta_series_count, 0);
    assert!(loaded_registry
        .registry
        .resolve_existing(metric, &labels)
        .is_some());
    rebuilt.close().unwrap();

    let checkpoint_before_second_restart = std::fs::read(&checkpoint_path).unwrap();
    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert_eq!(
        std::fs::read(&checkpoint_path).unwrap(),
        checkpoint_before_second_restart
    );
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());
    reopened.close().unwrap();
}

#[test]
fn startup_reopens_from_registry_catalog_without_segment_series_files() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let metric = "startup_registry_catalog_fast_path";
    let labels = vec![Label::new("host", "startup")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let segment_root = load_segments_for_level(temp_dir.path().join(NUMERIC_LANE_ROOT), 0)
        .unwrap()
        .into_iter()
        .next()
        .expect("expected a persisted segment")
        .root;
    assert!(checkpoint_path.exists());
    assert!(catalog_path.exists());

    std::fs::remove_file(segment_root.join("series.bin")).unwrap();
    std::fs::remove_file(segment_root.join("postings.bin")).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert_eq!(
        reopened
            .select_series(&SeriesSelection::new().with_metric(metric))
            .unwrap()
            .len(),
        1
    );
    let selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric(metric)
                .with_time_range(0, 10),
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].name, metric);
    assert_eq!(selected[0].labels, labels);
    reopened.close().unwrap();
}

#[test]
fn startup_self_heals_conflicting_registry_snapshot_even_when_catalog_matches_segments() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let metric = "startup_registry_catalog_conflict";
    let labels = vec![Label::new("host", "startup")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    let now = 100i64;

    let series_id = {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
        SeriesRegistry::load_persisted_state(&checkpoint_path)
            .unwrap()
            .expect("initial checkpoint should exist")
            .registry
            .resolve_existing(metric, &labels)
            .unwrap()
            .series_id
    };

    assert!(checkpoint_path.exists());
    assert!(catalog_path.exists());

    let conflicting_registry = SeriesRegistry::new();
    conflicting_registry
        .register_series_with_id(
            series_id,
            "startup_registry_catalog_conflict_wrong",
            &[Label::new("host", "wrong")],
        )
        .unwrap();
    conflicting_registry
        .persist_to_path(&checkpoint_path)
        .unwrap();
    let conflicting_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();

    let rebuilt = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        rebuilt.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        conflicting_checkpoint_bytes
    );
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed checkpoint should reload after startup rebuild");
    assert!(loaded_registry
        .registry
        .resolve_existing(metric, &labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing(
            "startup_registry_catalog_conflict_wrong",
            &[Label::new("host", "wrong")]
        )
        .is_none());
    rebuilt.close().unwrap();
}

#[test]
fn startup_recovers_visible_series_when_checkpoint_and_catalog_lag_segments() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let base_metric = "startup_registry_lag_base";
    let base_labels = vec![Label::new("host", "base")];
    let lagged_metric = "startup_registry_lag_new";
    let lagged_labels = vec![Label::new("host", "lagged")];
    let now = 100i64;

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(base_metric, base_labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels(base_metric, base_labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let stale_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    let stale_catalog_bytes = std::fs::read(&catalog_path).unwrap();

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(lagged_metric, lagged_labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels(lagged_metric, lagged_labels.clone(), DataPoint::new(4, 4.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        stale_checkpoint_bytes
    );
    assert_ne!(std::fs::read(&catalog_path).unwrap(), stale_catalog_bytes);

    std::fs::write(&checkpoint_path, &stale_checkpoint_bytes).unwrap();
    std::fs::write(&catalog_path, &stale_catalog_bytes).unwrap();

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(base_metric, &base_labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );
    assert_eq!(
        reopened.select(lagged_metric, &lagged_labels, 0, 10).unwrap(),
        vec![DataPoint::new(3, 3.0), DataPoint::new(4, 4.0)],
        "startup should recover series that were visible in segments even when both registry files lag behind",
    );

    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        stale_checkpoint_bytes
    );
    assert_ne!(std::fs::read(&catalog_path).unwrap(), stale_catalog_bytes);
    let healed_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed registry checkpoint should reload after startup recovery");
    assert!(healed_registry
        .registry
        .resolve_existing(base_metric, &base_labels)
        .is_some());
    assert!(healed_registry
        .registry
        .resolve_existing(lagged_metric, &lagged_labels)
        .is_some());

    reopened.close().unwrap();
}

#[test]
fn applying_loaded_segment_indexes_reconciles_stale_registry_series_out_of_materialized_state() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "startup_registry_reconcile_metric";
    let labels = vec![Label::new("host", "persisted")];
    let ghost_metric = "startup_registry_reconcile_ghost";
    let ghost_labels = vec![Label::new("host", "ghost")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("expected persisted registry snapshot");
    loaded_registry
        .registry
        .resolve_or_insert(ghost_metric, &ghost_labels)
        .unwrap();
    loaded_registry
        .registry
        .persist_to_path(&checkpoint_path)
        .unwrap();

    let reopened = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert!(reopened
        .select_series(&SeriesSelection::new().with_metric(ghost_metric))
        .unwrap()
        .is_empty());

    let registry = reopened.catalog.registry.read();
    let actual_series_id = registry
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    assert!(registry
        .resolve_existing(ghost_metric, &ghost_labels)
        .is_none());
    drop(registry);

    assert_eq!(
        reopened.materialized_series_snapshot(),
        vec![actual_series_id]
    );
    reopened.close().unwrap();
}

#[test]
fn explicit_runtime_refresh_fails_when_visible_manifest_is_corrupted() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "runtime_refresh_manifest_corrupt";
    let labels = vec![Label::new("host", "startup")];
    let segment_root = write_startup_persisted_segment(temp_dir.path(), metric);
    let storage = open_raw_numeric_storage_from_data_path(temp_dir.path());

    flip_first_byte(&segment_root.join("manifest.bin"));

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    let err = storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap_err();
    assert_runtime_refresh_failure(err, &segment_root, "manifest crc32 mismatch");
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    let persisted_roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(persisted_roots, vec![segment_root]);
}

#[test]
fn explicit_runtime_refresh_fails_when_added_chunk_index_is_corrupted() {
    assert_explicit_runtime_refresh_rejects_corrupted_added_segment(
        "runtime_refresh_index_corrupt",
        "chunk_index.bin",
        flip_first_byte,
        "manifest file hash mismatch for kind 2",
    );
}

#[test]
fn explicit_runtime_refresh_fails_when_added_chunk_payload_is_corrupted() {
    assert_explicit_runtime_refresh_rejects_corrupted_added_segment(
        "runtime_refresh_chunk_corrupt",
        "chunks.bin",
        flip_last_byte,
        "manifest file hash mismatch for kind 1",
    );
}

#[test]
fn explicit_runtime_refresh_loads_added_segment_without_decoding_chunk_payloads() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "runtime_refresh_lazy_metadata";
    let labels = vec![Label::new("host", "startup")];
    let good_root = write_startup_persisted_segment(temp_dir.path(), metric);
    let storage = open_raw_numeric_storage_from_data_path(temp_dir.path());
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    let added_root =
        write_runtime_refresh_segment(temp_dir.path(), metric, series_id, 2, &[(3, 3.0)]);
    replace_first_chunk_payload_with_invalid_encoded_payload(&added_root);

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert!(!storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_timestamp_indexes
            .values()
            .all(|index| index.get().is_none()),
        "runtime refresh should not eagerly build persisted timestamp search indexes",
    );
    let persisted_roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(persisted_roots, vec![good_root, added_root]);
    let selected = select_metric_series_sorted(&storage, metric);
    assert_eq!(selected.len(), 1);
}

#[test]
fn query_prefers_compacted_generation_when_compaction_crash_leaves_both_generations() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "compaction_crash_dupe";
    let now = 100i64;

    let registry = SeriesRegistry::new();
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

    let reopened = builder_at_time(now)
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

#[test]
fn query_prefers_compacted_generation_when_generations_cover_same_window_with_different_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "compaction_crash_point_count_precedence";
    let now = 100i64;

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    let mut stale_l0 = HashMap::new();
    stale_l0.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0), (2, 20.0)],
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

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(3)
        .build()
        .unwrap();

    let points = reopened.select(metric, &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(1, 10.0), DataPoint::new(2, 2.0)]
    );

    reopened.close().unwrap();
}
