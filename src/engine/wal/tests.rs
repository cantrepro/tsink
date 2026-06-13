use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tempfile::TempDir;

use super::{
    checksum32, collect_wal_segment_files, encode_series_definition, replay_from_path,
    replay_from_path_with_mode, scan_last_seq, segment_path, CachedSeriesDefinitionFrame,
    CachedSeriesDefinitionIndex, FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame,
    DEFAULT_WAL_SEGMENT_MAX_BYTES, FRAME_HEADER_LEN, FRAME_MAGIC, FRAME_TYPE_SERIES_DEF,
    WAL_FILE_NAME,
};
use crate::engine::binio::{write_u32_at, write_u64_at};
use crate::engine::chunk::{ChunkPoint, ValueLane};
use crate::engine::segment::WalHighWatermark;
use crate::{
    wal::{WalReplayMode, WalSyncMode},
    HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram, TsinkError,
    Value,
};

fn series_def_frame_header(seq: u64, payload_len: usize, crc32: u32) -> [u8; FRAME_HEADER_LEN] {
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[0..4].copy_from_slice(&FRAME_MAGIC);
    header[4] = FRAME_TYPE_SERIES_DEF;
    write_u64_at(&mut header, 8, seq).unwrap();
    write_u32_at(&mut header, 16, payload_len as u32).unwrap();
    write_u32_at(&mut header, 20, crc32).unwrap();
    header
}

fn next_u64(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Float(14.5)),
        sum: 6.75,
        schema: 2,
        zero_threshold: 0.0,
        zero_count: Some(HistogramCount::Float(2.5)),
        negative_spans: vec![],
        negative_deltas: vec![],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 1,
            length: 3,
        }],
        positive_deltas: vec![],
        positive_counts: vec![1.0, 4.5, 6.5],
        reset_hint: HistogramResetHint::Gauge,
        custom_values: vec![0.25, 0.5],
    }
}

fn actual_wal_size_bytes(dir: &Path) -> u64 {
    collect_wal_segment_files(dir)
        .unwrap()
        .into_iter()
        .map(|segment| fs::metadata(segment.path).unwrap().len())
        .sum()
}

fn actual_wal_segment_count(dir: &Path) -> u64 {
    collect_wal_segment_files(dir).unwrap().len() as u64
}

fn actual_active_wal_segment_size_bytes(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

fn assert_runtime_accounting_matches_disk(wal: &FramedWal, dir: &Path) {
    assert_eq!(wal.total_size_bytes().unwrap(), actual_wal_size_bytes(dir));
    assert_eq!(wal.segment_count().unwrap(), actual_wal_segment_count(dir));
    assert_eq!(
        wal.active_segment_size_bytes(),
        actual_active_wal_segment_size_bytes(&wal.path())
    );
}

#[test]
fn samples_batch_roundtrip_preserves_points() {
    let points = vec![
        ChunkPoint {
            ts: 1,
            value: Value::I64(10),
        },
        ChunkPoint {
            ts: 5,
            value: Value::I64(11),
        },
        ChunkPoint {
            ts: 8,
            value: Value::I64(20),
        },
    ];

    let batch = SamplesBatchFrame::from_points(42, ValueLane::Numeric, &points).unwrap();
    let decoded = batch.decode_points().unwrap();
    assert_eq!(decoded.len(), points.len());
    assert_eq!(decoded[0].ts, 1);
    assert_eq!(decoded[2].value, Value::I64(20));
}

#[test]
fn samples_batch_roundtrip_preserves_histogram_points() {
    let histogram = sample_histogram();
    let points = vec![
        ChunkPoint {
            ts: 10,
            value: Value::from(histogram.clone()),
        },
        ChunkPoint {
            ts: 20,
            value: Value::from(histogram.clone()),
        },
    ];

    let batch = SamplesBatchFrame::from_points(42, ValueLane::Blob, &points).unwrap();
    let decoded = batch.decode_points().unwrap();
    assert_eq!(decoded.len(), points.len());
    for (actual, expected) in decoded.iter().zip(&points) {
        assert_eq!(actual.ts, expected.ts);
        assert_eq!(actual.value, expected.value);
    }
}

#[test]
fn wal_roundtrip_replays_series_defs_and_samples() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let batch = SamplesBatchFrame::from_points(
        7,
        ValueLane::Numeric,
        &[
            ChunkPoint {
                ts: 10,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 20,
                value: Value::F64(2.0),
            },
        ],
    )
    .unwrap();

    wal.append_samples(&[batch]).unwrap();

    let replay = wal.replay_frames().unwrap();
    assert_eq!(replay.len(), 2);

    assert!(matches!(replay[0], ReplayFrame::SeriesDefinition(_)));
    assert!(matches!(replay[1], ReplayFrame::Samples(_)));
}

#[test]
fn committed_replay_discards_unpaired_series_definitions() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "phantom".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "committed".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();
    wal.append_samples(&[SamplesBatchFrame::from_points(
        2,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap()])
        .unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 3,
        metric: "trailing".to_string(),
        labels: vec![Label::new("host", "c")],
    })
    .unwrap();

    let writes = wal.replay_committed_writes().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].series_definitions.len(), 1);
    assert_eq!(writes[0].series_definitions[0].series_id, 2);
    assert_eq!(writes[0].sample_batches.len(), 1);
    assert_eq!(writes[0].sample_batches[0].series_id, 2);

    let committed_series_defs = wal.replay_committed_series_definitions().unwrap();
    assert_eq!(committed_series_defs.len(), 1);
    assert_eq!(committed_series_defs[0].series_id, 2);
}

#[test]
fn committed_replay_ignores_persisted_writes_that_never_crossed_publish_boundary() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let definition = SeriesDefinitionFrame {
        series_id: 7,
        metric: "staged".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let batch = SamplesBatchFrame::from_points(
        7,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap();
    let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&definition)
        .unwrap()
        .saturating_add(
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&batch)).unwrap(),
        );
    let definition_payload =
        FramedWal::encode_series_definition_frame_payload(&definition).unwrap();
    let samples_payload =
        FramedWal::encode_samples_frame_payload(std::slice::from_ref(&batch)).unwrap();

    let mut logical = wal.begin_logical_write(estimated_bytes).unwrap();
    logical
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    logical.append_samples_payload(&samples_payload).unwrap();
    assert_eq!(
        logical.persist_pending().unwrap(),
        WalHighWatermark {
            segment: 0,
            frame: 2,
        }
    );
    std::mem::forget(logical);
    drop(wal);

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_eq!(
        reopened.current_published_highwater(),
        WalHighWatermark::default()
    );
    assert_eq!(reopened.replay_frames().unwrap().len(), 2);
    assert!(reopened.replay_committed_writes().unwrap().is_empty());
    assert!(reopened
        .committed_series_definitions_snapshot()
        .unwrap()
        .is_empty());
}

#[test]
fn published_highwater_marker_persists_across_reopen_after_logical_publish() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let definition = SeriesDefinitionFrame {
        series_id: 9,
        metric: "published".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let batch = SamplesBatchFrame::from_points(
        9,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap();
    let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&definition)
        .unwrap()
        .saturating_add(
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&batch)).unwrap(),
        );
    let definition_payload =
        FramedWal::encode_series_definition_frame_payload(&definition).unwrap();
    let samples_payload =
        FramedWal::encode_samples_frame_payload(std::slice::from_ref(&batch)).unwrap();

    let mut logical = wal.begin_logical_write(estimated_bytes).unwrap();
    logical
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    logical.append_samples_payload(&samples_payload).unwrap();
    let persisted_highwater = logical.persist_pending().unwrap();
    let published_highwater = logical.publish_persisted().unwrap();
    assert_eq!(published_highwater, persisted_highwater);
    assert_eq!(wal.current_published_highwater(), published_highwater);

    drop(wal);

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_eq!(reopened.current_published_highwater(), published_highwater);
    let committed = reopened.replay_committed_writes().unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].highwater, published_highwater);
}

#[test]
fn cached_committed_series_definitions_snapshot_tracks_pending_then_commit() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "phantom".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "committed".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();

    assert!(wal
        .committed_series_definitions_snapshot()
        .unwrap()
        .is_empty());

    wal.append_samples(&[SamplesBatchFrame::from_points(
        2,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap()])
        .unwrap();

    let committed_series_defs = wal.committed_series_definitions_snapshot().unwrap();
    assert_eq!(committed_series_defs.len(), 1);
    assert_eq!(committed_series_defs[0].series_id, 2);
}

#[test]
fn cached_committed_series_definitions_snapshot_waits_for_inflight_rebuild() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap());

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 9,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    wal.append_samples(&[SamplesBatchFrame::from_points(
        9,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap()])
        .unwrap();

    let rebuild_entered = Arc::new(std::sync::Barrier::new(2));
    let rebuild_release = Arc::new(std::sync::Barrier::new(2));
    wal.set_cached_series_definition_rebuild_hook({
        let rebuild_entered = Arc::clone(&rebuild_entered);
        let rebuild_release = Arc::clone(&rebuild_release);
        move || {
            rebuild_entered.wait();
            rebuild_release.wait();
        }
    });

    let first_wal = Arc::clone(&wal);
    let (first_tx, first_rx) = mpsc::channel();
    let first = thread::spawn(move || {
        first_tx
            .send(first_wal.committed_series_definitions_snapshot())
            .unwrap();
    });

    rebuild_entered.wait();

    let second_wal = Arc::clone(&wal);
    let (second_tx, second_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        second_tx
            .send(second_wal.committed_series_definitions_snapshot())
            .unwrap();
    });

    assert!(
        second_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "concurrent readers should wait for the in-flight snapshot rebuild"
    );

    rebuild_release.wait();

    let first_defs = first_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let second_defs = second_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(first_defs.len(), 1);
    assert_eq!(second_defs.len(), 1);
    assert_eq!(first_defs[0].series_id, 9);
    assert_eq!(second_defs[0].series_id, 9);

    first.join().unwrap();
    second.join().unwrap();
    wal.clear_cached_series_definition_rebuild_hook();
}

#[test]
fn cached_committed_series_definitions_snapshot_clears_after_reset() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    wal.append_samples(&[SamplesBatchFrame::from_points(
        7,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap()])
        .unwrap();

    assert_eq!(
        wal.committed_series_definitions_snapshot().unwrap().len(),
        1
    );

    wal.reset().unwrap();
    assert!(wal
        .committed_series_definitions_snapshot()
        .unwrap()
        .is_empty());

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 8,
        metric: "mem".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();
    wal.append_samples(&[SamplesBatchFrame::from_points(
        8,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 20,
            value: Value::F64(2.0),
        }],
    )
    .unwrap()])
        .unwrap();

    let committed_series_defs = wal.committed_series_definitions_snapshot().unwrap();
    assert_eq!(committed_series_defs.len(), 1);
    assert_eq!(committed_series_defs[0].series_id, 8);
}

#[test]
fn appending_series_definitions_tracks_pending_cache_before_first_snapshot_build() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 41,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let index = wal.cached_series_definition_index.lock();
    assert!(!index.initialized);
    assert!(!index.building);
    assert!(index.buffered_frames.is_empty());
    assert_eq!(index.pending.len(), 1);
    assert_eq!(index.pending.get(&41).unwrap().metric, "cpu");
}

#[test]
fn cached_series_definition_rebuild_overlays_pending_definitions_before_buffered_samples() {
    let definition = SeriesDefinitionFrame {
        series_id: 17,
        metric: "overlay".to_string(),
        labels: vec![Label::new("host", "late")],
    };
    let mut live = CachedSeriesDefinitionIndex::default();
    live.apply_frame(CachedSeriesDefinitionFrame::SeriesDefinition(
        definition.clone(),
    ));
    live.building = true;
    live.buffered_frames
        .push(CachedSeriesDefinitionFrame::Samples(BTreeSet::from([
            definition.series_id,
        ])));

    let mut rebuilt = CachedSeriesDefinitionIndex::default();
    rebuilt.overlay_uninitialized_pending_from(&live);
    for frame in live.buffered_frames.drain(..) {
        rebuilt.apply_frame(frame);
    }

    let snapshot = rebuilt.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].series_id, definition.series_id);
    assert_eq!(snapshot[0].metric, definition.metric);
    assert_eq!(snapshot[0].labels, definition.labels);
    assert!(rebuilt.pending.is_empty());
}

#[test]
fn committed_replay_helpers_default_to_strict_and_snapshot_uses_configured_mode_on_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
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
        2,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 2,
            value: Value::F64(2.0),
        }],
    )
    .unwrap();
    let later_clean_batch = SamplesBatchFrame::from_points(
        3,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 3,
            value: Value::F64(3.0),
        }],
    )
    .unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu_a".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    wal.append_samples(std::slice::from_ref(&first_batch))
        .unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "cpu_b".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();
    wal.append_samples(std::slice::from_ref(&corrupt_batch))
        .unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 3,
        metric: "cpu_c".to_string(),
        labels: vec![Label::new("host", "c")],
    })
    .unwrap();
    wal.append_samples(std::slice::from_ref(&later_clean_batch))
        .unwrap();

    let first_definition_bytes =
        FramedWal::estimate_series_definition_frame_bytes(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "cpu_a".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();
    let first_batch_bytes =
        FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&first_batch)).unwrap();
    let second_definition_bytes =
        FramedWal::estimate_series_definition_frame_bytes(&SeriesDefinitionFrame {
            series_id: 2,
            metric: "cpu_b".to_string(),
            labels: vec![Label::new("host", "b")],
        })
        .unwrap();
    let checksum_offset = first_definition_bytes + first_batch_bytes + second_definition_bytes + 20;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal.path())
        .unwrap();
    file.seek(SeekFrom::Start(checksum_offset)).unwrap();
    let mut checksum = [0u8; 4];
    file.read_exact(&mut checksum).unwrap();
    checksum[0] ^= 0xff;
    file.seek(SeekFrom::Start(checksum_offset)).unwrap();
    file.write_all(&checksum).unwrap();
    file.flush().unwrap();

    for err in [
        wal.replay_committed_writes().unwrap_err(),
        wal.replay_committed_series_definitions().unwrap_err(),
        wal.committed_series_definitions_snapshot().unwrap_err(),
    ] {
        assert!(matches!(
            err,
            TsinkError::DataCorruption(message)
                if message.contains("segment 0, frame 4")
                    && message.contains("checksum mismatch")
        ));
    }

    let replayed_timestamps = wal
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

    wal.set_configured_replay_mode(WalReplayMode::Salvage);
    let salvaged_series_ids = wal
        .committed_series_definitions_snapshot()
        .unwrap()
        .into_iter()
        .map(|definition| definition.series_id)
        .collect::<Vec<_>>();
    assert_eq!(salvaged_series_ids, vec![1, 3]);
}

#[test]
fn replay_frames_after_skips_checkpointed_frames() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let batch = SamplesBatchFrame::from_points(
        7,
        ValueLane::Numeric,
        &[
            ChunkPoint {
                ts: 10,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 20,
                value: Value::F64(2.0),
            },
        ],
    )
    .unwrap();
    wal.append_samples(&[batch]).unwrap();

    let replay = wal
        .replay_frames_after(WalHighWatermark {
            segment: 0,
            frame: 1,
        })
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert!(matches!(replay[0], ReplayFrame::Samples(_)));
    assert_eq!(
        wal.current_highwater(),
        WalHighWatermark {
            segment: 0,
            frame: 2
        }
    );
    assert_eq!(
        wal.current_durable_highwater(),
        WalHighWatermark {
            segment: 0,
            frame: 2
        }
    );
}

#[test]
fn wal_reset_clears_existing_frames() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    assert_eq!(wal.replay_frames().unwrap().len(), 1);
    wal.reset().unwrap();
    assert!(wal.replay_frames().unwrap().is_empty());
}

#[test]
fn wal_reset_preserves_monotonic_sequence() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    assert_eq!(wal.current_highwater().frame, 1);
    assert_eq!(wal.current_durable_highwater().frame, 1);

    wal.reset().unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 8,
        metric: "mem".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();

    assert_eq!(wal.current_highwater().frame, 2);
    assert_eq!(wal.current_durable_highwater().frame, 2);
}

#[test]
fn ensure_min_next_seq_sets_sequence_floor() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.ensure_min_next_seq(5);

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    assert_eq!(wal.current_highwater().frame, 5);
    assert_eq!(wal.current_durable_highwater().frame, 5);
}

#[test]
fn wal_open_with_buffer_size_configures_writer_capacity() {
    let temp_dir = TempDir::new().unwrap();
    let wal =
        FramedWal::open_with_buffer_size(temp_dir.path(), WalSyncMode::PerAppend, 128).unwrap();
    assert_eq!(wal.writer.lock().capacity(), 128);

    let wal_zero =
        FramedWal::open_with_buffer_size(temp_dir.path(), WalSyncMode::PerAppend, 0).unwrap();
    assert_eq!(wal_zero.writer.lock().capacity(), 1);
}

#[test]
fn wal_rotates_segments_and_replays_across_them() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        (FRAME_HEADER_LEN as u64) + 40,
    )
    .unwrap();

    for series_id in 0..10 {
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id,
            metric: format!("cpu_{series_id}"),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();
    }

    let segments = collect_wal_segment_files(temp_dir.path()).unwrap();
    assert!(
        segments.len() >= 2,
        "expected WAL rotation, got {segments:?}"
    );

    let replay = wal.replay_frames().unwrap();
    assert_eq!(replay.len(), 10);
}

#[test]
fn wal_runtime_accounting_tracks_rotation_and_reset() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        (FRAME_HEADER_LEN as u64) + 40,
    )
    .unwrap();

    assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
    assert_eq!(wal.total_size_bytes().unwrap(), 0);
    assert_eq!(wal.segment_count().unwrap(), 1);

    for series_id in 0..10 {
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id,
            metric: format!("cpu_{series_id}"),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();
    }

    assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
    assert!(wal.segment_count().unwrap() >= 2);
    assert!(wal.total_size_bytes().unwrap() > 0);

    wal.reset().unwrap();

    assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
    assert_eq!(wal.total_size_bytes().unwrap(), 0);
    assert_eq!(wal.segment_count().unwrap(), 1);
}

#[test]
fn logical_wal_write_runtime_accounting_tracks_commit_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let definition = SeriesDefinitionFrame {
        series_id: 7,
        metric: "logical_cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let batch = SamplesBatchFrame::from_points(
        7,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap();
    let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&definition)
        .unwrap()
        .saturating_add(
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&batch)).unwrap(),
        );
    let wal = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        estimated_bytes.saturating_sub(1).max(1),
    )
    .unwrap();

    let definition_payload =
        FramedWal::encode_series_definition_frame_payload(&definition).unwrap();
    let samples_payload =
        FramedWal::encode_samples_frame_payload(std::slice::from_ref(&batch)).unwrap();

    let mut logical = wal.begin_logical_write(estimated_bytes).unwrap();
    logical
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    logical.flush_pending().unwrap();
    logical.append_samples_payload(&samples_payload).unwrap();
    let highwater = logical.commit().unwrap();

    assert_eq!(
        highwater,
        WalHighWatermark {
            segment: 0,
            frame: 2,
        }
    );
    assert_eq!(wal.active_segment(), 1);
    assert_eq!(wal.active_segment_size_bytes(), 0);
    assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
    assert_eq!(wal.replay_frames().unwrap().len(), 2);
}

#[test]
fn logical_wal_write_abort_rolls_back_staged_frames() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let definition = SeriesDefinitionFrame {
        series_id: 7,
        metric: "logical_abort".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let batch = SamplesBatchFrame::from_points(
        7,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        }],
    )
    .unwrap();
    let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&definition)
        .unwrap()
        .saturating_add(
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&batch)).unwrap(),
        );
    let definition_payload =
        FramedWal::encode_series_definition_frame_payload(&definition).unwrap();
    let samples_payload =
        FramedWal::encode_samples_frame_payload(std::slice::from_ref(&batch)).unwrap();

    let mut logical = wal.begin_logical_write(estimated_bytes).unwrap();
    logical
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    logical.append_samples_payload(&samples_payload).unwrap();
    logical.abort().unwrap();

    assert!(wal.replay_frames().unwrap().is_empty());
    assert_eq!(wal.current_highwater(), WalHighWatermark::default());
    assert_eq!(
        wal.current_published_highwater(),
        WalHighWatermark::default()
    );
    assert_eq!(wal.current_durable_highwater(), WalHighWatermark::default());
    assert_eq!(wal.next_seq.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 0);
}

#[test]
fn logical_wal_write_sync_failure_restores_runtime_accounting() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let definition = SeriesDefinitionFrame {
        series_id: 7,
        metric: "logical_sync_failure".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let payload = FramedWal::encode_series_definition_frame_payload(&definition).unwrap();
    let estimated_bytes = FramedWal::frame_size_bytes_for_payload_len(payload.len());
    let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failed_hook = Arc::clone(&failed);
    wal.set_append_sync_hook(move || {
        if failed_hook.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        Err(TsinkError::Other(
            "injected logical WAL sync failure".to_string(),
        ))
    });

    let mut logical = wal.begin_logical_write(estimated_bytes).unwrap();
    logical.append_series_definition_payload(&payload).unwrap();
    let err = logical.commit().unwrap_err();

    assert!(matches!(
        err,
        TsinkError::Other(message) if message.contains("injected logical WAL sync failure")
    ));
    assert!(failed.load(Ordering::SeqCst));
    assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
    assert_eq!(wal.active_segment_size_bytes(), 0);
    assert!(wal.replay_frames().unwrap().is_empty());
    assert_eq!(wal.current_highwater(), WalHighWatermark::default());
    assert_eq!(wal.current_durable_highwater(), WalHighWatermark::default());
    assert_eq!(wal.next_seq.load(Ordering::SeqCst), 1);
}

#[test]
fn wal_reopen_and_highwater_floor_bootstrap_runtime_accounting() {
    let temp_dir = TempDir::new().unwrap();
    {
        let wal = FramedWal::open_with_options(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            (FRAME_HEADER_LEN as u64) + 40,
        )
        .unwrap();

        for series_id in 0..6 {
            wal.append_series_definition(&SeriesDefinitionFrame {
                series_id,
                metric: format!("cpu_{series_id}"),
                labels: vec![Label::new("host", "a")],
            })
            .unwrap();
        }

        assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
    }

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_runtime_accounting_matches_disk(&reopened, temp_dir.path());

    let size_before = reopened.total_size_bytes().unwrap();
    let count_before = reopened.segment_count().unwrap();
    reopened
        .ensure_min_highwater(WalHighWatermark {
            segment: reopened.active_segment().saturating_add(3),
            frame: 8,
        })
        .unwrap();

    assert_runtime_accounting_matches_disk(&reopened, temp_dir.path());
    assert_eq!(reopened.total_size_bytes().unwrap(), size_before);
    assert_eq!(reopened.segment_count().unwrap(), count_before + 1);

    drop(reopened);

    let reopened_again = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_runtime_accounting_matches_disk(&reopened_again, temp_dir.path());
}

#[test]
fn replay_stream_after_skips_checkpointed_frames() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "mem".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();

    let mut stream = wal
        .replay_stream_after(WalHighWatermark {
            segment: 0,
            frame: 1,
        })
        .unwrap();
    let first = stream.next_frame().unwrap();
    assert!(matches!(first, Some(ReplayFrame::SeriesDefinition(_))));
    assert!(stream.next_frame().unwrap().is_none());
}

#[test]
fn ensure_min_highwater_moves_active_segment_floor() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.ensure_min_highwater(WalHighWatermark {
        segment: 3,
        frame: 8,
    })
    .unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    assert_eq!(
        wal.current_highwater(),
        WalHighWatermark {
            segment: 3,
            frame: 9
        }
    );
}

#[test]
fn replay_default_mode_errors_at_truncated_tail_frame() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "m".to_string(),
        labels: vec![],
    })
    .unwrap();

    {
        let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
        file.write_all(b"W2FR\x02\x00\x00\x00\x00").unwrap();
        file.flush().unwrap();
    }

    let err = replay_from_path(&wal.path(), WalHighWatermark::default()).unwrap_err();
    assert!(
        matches!(err, TsinkError::DataCorruption(message) if message.contains("truncated frame header"))
    );
}

#[test]
fn replay_salvage_mode_skips_checksum_mismatch_and_continues_with_later_frames() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join(WAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&wal_path)
        .unwrap();

    let payload_a = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu_a".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    let payload_b = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "cpu_b".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();
    let payload_c = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 3,
        metric: "cpu_c".to_string(),
        labels: vec![Label::new("host", "c")],
    })
    .unwrap();

    let write_frame = |file: &mut std::fs::File, seq: u64, payload: &[u8], crc32: u32| {
        let header = series_def_frame_header(seq, payload.len(), crc32);
        file.write_all(&header).unwrap();
        file.write_all(payload).unwrap();
    };

    write_frame(&mut file, 1, &payload_a, checksum32(&payload_a));
    write_frame(
        &mut file,
        2,
        &payload_b,
        checksum32(&payload_b).wrapping_add(1),
    );
    write_frame(&mut file, 3, &payload_c, checksum32(&payload_c));
    file.flush().unwrap();

    let default_err = replay_from_path(&wal_path, WalHighWatermark::default()).unwrap_err();
    assert!(matches!(
        default_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 2")
                && message.contains("checksum mismatch")
    ));

    let replay = replay_from_path_with_mode(
        &wal_path,
        WalHighWatermark::default(),
        WalReplayMode::Salvage,
    )
    .unwrap();
    let replayed_series_ids = replay
        .into_iter()
        .map(|frame| match frame {
            ReplayFrame::SeriesDefinition(frame) => frame.series_id,
            ReplayFrame::Samples(_) => panic!("expected series definition frame"),
        })
        .collect::<Vec<_>>();
    assert_eq!(replayed_series_ids, vec![1, 3]);

    let err = replay_from_path_with_mode(
        &wal_path,
        WalHighWatermark::default(),
        WalReplayMode::Strict,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 2")
                && message.contains("checksum mismatch")
    ));
}

#[test]
fn wal_open_quarantines_corrupt_active_segment_before_new_appends() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join(WAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&wal_path)
        .unwrap();

    let payload_a = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu_a".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();
    let payload_b = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "cpu_b".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();
    let payload_c = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 3,
        metric: "cpu_c".to_string(),
        labels: vec![Label::new("host", "c")],
    })
    .unwrap();

    let write_frame = |file: &mut std::fs::File, seq: u64, payload: &[u8], crc32: u32| {
        let header = series_def_frame_header(seq, payload.len(), crc32);
        file.write_all(&header).unwrap();
        file.write_all(payload).unwrap();
    };

    write_frame(&mut file, 1, &payload_a, checksum32(&payload_a));
    write_frame(
        &mut file,
        2,
        &payload_b,
        checksum32(&payload_b).wrapping_add(1),
    );
    write_frame(&mut file, 3, &payload_c, checksum32(&payload_c));
    file.flush().unwrap();

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let recovery_segment = segment_path(temp_dir.path(), 1);
    assert_eq!(
        reopened.current_highwater(),
        WalHighWatermark {
            segment: 0,
            frame: 3,
        }
    );
    assert_eq!(reopened.active_segment(), 1);
    assert_eq!(reopened.path(), recovery_segment);
    assert_eq!(reopened.next_seq.load(Ordering::SeqCst), 1);

    reopened
        .append_series_definition(&SeriesDefinitionFrame {
            series_id: 4,
            metric: "cpu_d".to_string(),
            labels: vec![Label::new("host", "d")],
        })
        .unwrap();

    let replayed_series_ids = reopened
        .replay_frames_with_mode(WalReplayMode::Salvage)
        .unwrap()
        .into_iter()
        .map(|frame| match frame {
            ReplayFrame::SeriesDefinition(frame) => frame.series_id,
            ReplayFrame::Samples(_) => panic!("expected series definition frame"),
        })
        .collect::<Vec<_>>();
    assert_eq!(replayed_series_ids, vec![1, 3, 4]);
}

#[test]
fn replay_strict_mode_errors_at_truncated_tail_frame() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "m".to_string(),
        labels: vec![],
    })
    .unwrap();

    {
        let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
        file.write_all(b"W2FR\x02\x00\x00\x00\x00").unwrap();
        file.flush().unwrap();
    }

    let err = replay_from_path_with_mode(
        &wal.path(),
        WalHighWatermark::default(),
        WalReplayMode::Strict,
    )
    .unwrap_err();
    assert!(
        matches!(err, TsinkError::DataCorruption(message) if message.contains("truncated frame header"))
    );
}

#[test]
fn replay_salvage_mode_stops_at_frame_with_magic_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "m".to_string(),
        labels: vec![],
    })
    .unwrap();

    {
        let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
        let mut header = [0u8; FRAME_HEADER_LEN];
        header[0..4].copy_from_slice(b"BAD!");
        file.write_all(&header).unwrap();
        file.flush().unwrap();
    }

    let replay = replay_from_path_with_mode(
        &wal.path(),
        WalHighWatermark::default(),
        WalReplayMode::Salvage,
    )
    .unwrap();
    assert_eq!(replay.len(), 1);
    assert!(matches!(replay[0], ReplayFrame::SeriesDefinition(_)));
}

#[test]
fn replay_salvage_mode_handles_random_truncated_tails() {
    for seed in 0u64..32 {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();

        let mut state = seed.wrapping_mul(17).wrapping_add(3);
        let noise_len = ((next_u64(&mut state) % (FRAME_HEADER_LEN as u64 - 1)) + 1) as usize;
        let mut noise = Vec::with_capacity(noise_len);
        for _ in 0..noise_len {
            noise.push((next_u64(&mut state) & 0xff) as u8);
        }

        {
            let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
            file.write_all(&noise).unwrap();
            file.flush().unwrap();
        }

        let replay = replay_from_path_with_mode(
            &wal.path(),
            WalHighWatermark::default(),
            WalReplayMode::Salvage,
        )
        .unwrap();
        assert_eq!(
            replay.len(),
            1,
            "salvage replay should recover valid prefix for seed {seed}"
        );
        assert!(matches!(replay[0], ReplayFrame::SeriesDefinition(_)));
    }
}

#[test]
fn replay_salvage_mode_skips_corrupt_segment_tail_and_continues_to_later_segments() {
    let temp_dir = TempDir::new().unwrap();
    let definition = SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
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
    let second_batch = SamplesBatchFrame::from_points(
        1,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 2,
            value: Value::F64(2.0),
        }],
    )
    .unwrap();
    let third_batch = SamplesBatchFrame::from_points(
        1,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 3,
            value: Value::F64(3.0),
        }],
    )
    .unwrap();
    let segment_max_bytes =
        FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&first_batch)).unwrap();
    let wal = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        segment_max_bytes,
    )
    .unwrap();

    wal.append_series_definition(&definition).unwrap();
    wal.append_samples(&[first_batch]).unwrap();
    wal.append_samples(&[second_batch]).unwrap();
    wal.append_samples(&[third_batch]).unwrap();

    let segments = collect_wal_segment_files(temp_dir.path()).unwrap();
    assert!(
        segments.len() >= 4,
        "expected one segment per append, got {segments:?}"
    );
    let corrupt_segment_id = segments[2].id;
    {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&segments[2].path)
            .unwrap();
        file.write_all(b"TSFR\x02\x00\x00\x00\x00").unwrap();
        file.flush().unwrap();
    }

    let replayed_timestamps = wal
        .replay_committed_writes_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Salvage,
        )
        .unwrap()
        .into_iter()
        .flat_map(|write| write.sample_batches)
        .map(|batch| batch.decode_points().unwrap()[0].ts)
        .collect::<Vec<_>>();
    assert_eq!(replayed_timestamps, vec![1, 2, 3]);

    let err = wal
        .replay_committed_writes_after_with_mode(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::DataCorruption(message)
            if message.contains(&format!("segment {corrupt_segment_id}"))
                && message.contains("truncated frame header")
    ));
}

#[test]
fn scan_last_seq_uses_max_sequence_when_frames_are_out_of_order() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join(WAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&wal_path)
        .unwrap();

    let payload = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 42,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let write_frame = |file: &mut std::fs::File, seq: u64, payload: &[u8]| {
        let header = series_def_frame_header(seq, payload.len(), checksum32(payload));
        file.write_all(&header).unwrap();
        file.write_all(payload).unwrap();
    };

    write_frame(&mut file, 2, &payload);
    write_frame(&mut file, 1, &payload);
    file.flush().unwrap();

    assert_eq!(scan_last_seq(&wal_path).unwrap(), 2);
}

#[test]
fn scan_last_seq_stops_at_frame_with_checksum_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join(WAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&wal_path)
        .unwrap();

    let payload = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 42,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let write_frame = |file: &mut std::fs::File, seq: u64, payload: &[u8], crc32: u32| {
        let header = series_def_frame_header(seq, payload.len(), crc32);
        file.write_all(&header).unwrap();
        file.write_all(payload).unwrap();
    };

    write_frame(&mut file, 2, &payload, checksum32(&payload));
    write_frame(
        &mut file,
        10_000,
        &payload,
        checksum32(&payload).wrapping_add(1),
    );
    file.flush().unwrap();

    assert_eq!(scan_last_seq(&wal_path).unwrap(), 2);
}

#[test]
fn failed_append_does_not_advance_next_seq() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join(WAL_FILE_NAME);
    std::fs::File::create(&wal_path).unwrap();

    let writer_file = OpenOptions::new().read(true).open(&wal_path).unwrap();
    let wal = FramedWal {
        dir: temp_dir.path().to_path_buf(),
        path: parking_lot::Mutex::new(PathBuf::from(&wal_path)),
        published_highwater_path: temp_dir.path().join("wal.published"),
        published_highwater_tmp_path: temp_dir.path().join("wal.published.tmp"),
        writer: parking_lot::Mutex::new(std::io::BufWriter::new(writer_file)),
        active_segment: AtomicU64::new(0),
        active_segment_size_bytes: AtomicU64::new(0),
        next_seq: AtomicU64::new(7),
        total_size_bytes: AtomicU64::new(0),
        segment_count: AtomicU64::new(1),
        cached_series_definition_index: parking_lot::Mutex::new(
            CachedSeriesDefinitionIndex::default(),
        ),
        cached_series_definition_index_ready: parking_lot::Condvar::new(),
        last_appended_highwater: parking_lot::Mutex::new(WalHighWatermark {
            segment: 0,
            frame: 6,
        }),
        last_published_highwater: parking_lot::Mutex::new(WalHighWatermark {
            segment: 0,
            frame: 6,
        }),
        last_durable_highwater: parking_lot::Mutex::new(WalHighWatermark {
            segment: 0,
            frame: 6,
        }),
        configured_replay_mode: parking_lot::Mutex::new(WalReplayMode::Strict),
        sync_mode: WalSyncMode::PerAppend,
        last_sync: parking_lot::Mutex::new(Instant::now()),
        segment_max_bytes: DEFAULT_WAL_SEGMENT_MAX_BYTES,
        append_sync_hook: parking_lot::Mutex::new(None),
        cached_series_definition_rebuild_hook: parking_lot::Mutex::new(None),
    };

    let err = wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 11,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    });

    assert!(err.is_err());
    assert_eq!(wal.next_seq.load(Ordering::SeqCst), 7);
}

#[test]
fn sync_failure_after_flush_rolls_back_frame_and_preserves_replay_bookkeeping() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failed_hook = std::sync::Arc::clone(&failed);
    wal.set_append_sync_hook(move || {
        if failed_hook.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        Err(TsinkError::Other("injected WAL sync failure".to_string()))
    });

    let err = wal
        .append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap_err();
    assert!(
        matches!(err, TsinkError::Other(message) if message.contains("injected WAL sync failure"))
    );
    assert!(failed.load(Ordering::SeqCst));
    assert!(wal.replay_frames().unwrap().is_empty());
    assert_eq!(wal.current_highwater(), WalHighWatermark::default());
    assert_eq!(wal.current_durable_highwater(), WalHighWatermark::default());
    assert_eq!(wal.next_seq.load(Ordering::SeqCst), 1);
}

#[test]
fn periodic_append_skips_sync_until_interval_boundary() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(
        temp_dir.path(),
        WalSyncMode::Periodic(std::time::Duration::from_secs(3600)),
    )
    .unwrap();
    wal.set_append_sync_hook(|| {
        Err(TsinkError::Other(
            "periodic append should not sync immediately".to_string(),
        ))
    });

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let replay = wal.replay_frames().unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(
        wal.current_highwater(),
        WalHighWatermark {
            segment: 0,
            frame: 1,
        }
    );
    assert_eq!(wal.current_durable_highwater(), WalHighWatermark::default());
}

#[test]
fn rollback_partial_append_clears_buffered_bytes_and_preserves_next_frame() {
    let temp_dir = TempDir::new().unwrap();
    let wal =
        FramedWal::open_with_buffer_size(temp_dir.path(), WalSyncMode::PerAppend, 4096).unwrap();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    })
    .unwrap();

    let len_before_failure = std::fs::metadata(wal.path()).unwrap().len();

    {
        let mut writer = wal.writer.lock();
        writer.write_all(b"TSFRpartial").unwrap();
        assert!(!writer.buffer().is_empty());
        wal.rollback_partial_append(&mut writer, len_before_failure)
            .unwrap();
        assert!(writer.buffer().is_empty());
    }

    assert_eq!(
        std::fs::metadata(wal.path()).unwrap().len(),
        len_before_failure
    );

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "mem".to_string(),
        labels: vec![Label::new("host", "b")],
    })
    .unwrap();

    let replay = wal.replay_frames().unwrap();
    assert_eq!(replay.len(), 2);

    let first = match &replay[0] {
        ReplayFrame::SeriesDefinition(frame) => frame,
        _ => panic!("expected series definition frame"),
    };
    let second = match &replay[1] {
        ReplayFrame::SeriesDefinition(frame) => frame,
        _ => panic!("expected series definition frame"),
    };

    assert_eq!(first.series_id, 1);
    assert_eq!(second.series_id, 2);
}
