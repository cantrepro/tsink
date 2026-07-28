use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tempfile::TempDir;

use super::{
    checksum32, collect_wal_segment_files, decode_published_highwater_record,
    encode_published_highwater_record, encode_series_definition, replay_from_path,
    replay_from_path_with_mode, scan_last_seq, segment_path, CachedSeriesDefinitionFrame,
    CachedSeriesDefinitionIndex, FramedWal, PublishedHighwaterRecord, ReplayFrame,
    SamplesBatchFrame, SeriesDefinitionFrame, DEFAULT_WAL_SEGMENT_MAX_BYTES, FRAME_HEADER_LEN,
    FRAME_MAGIC, FRAME_TYPE_SERIES_DEF, PUBLISHED_HIGHWATER_MAX_RECORD_LEN,
    PUBLISHED_HIGHWATER_RECORD_LEN, PUBLISHED_HIGHWATER_V2_RECORD_LEN, WAL_FILE_NAME,
    WAL_PUBLISHED_HIGHWATER_FILE_NAME, WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME,
};
use crate::engine::binio::{write_u32_at, write_u64_at};
use crate::engine::chunk::{ChunkPoint, ValueLane};
use crate::engine::segment::WalHighWatermark;
use crate::{
    wal::{WalReplayMode, WalSyncMode},
    DiskCategory, HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, LocalDiskBudget,
    LocalDiskLimits, NativeHistogram, TsinkError, Value,
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

fn wal_directory_image(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            (
                PathBuf::from(entry.file_name()),
                std::fs::read(entry_path).unwrap(),
            )
        })
        .collect()
}

fn rewrite_published_highwater(path: &Path, highwater: WalHighWatermark) {
    let marker = path.join(WAL_PUBLISHED_HIGHWATER_FILE_NAME);
    let mut bytes = fs::read(&marker).unwrap();
    assert_eq!(bytes.len(), PUBLISHED_HIGHWATER_RECORD_LEN);
    write_u64_at(&mut bytes, 4, highwater.segment).unwrap();
    write_u64_at(&mut bytes, 12, highwater.frame).unwrap();
    let checksum = checksum32(&bytes[..20]);
    write_u32_at(&mut bytes, 20, checksum).unwrap();
    fs::write(marker, bytes).unwrap();
}

fn seed_two_published_frames(path: &Path) -> (PathBuf, u64, Vec<u8>) {
    let wal = FramedWal::open(path, WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "first".to_string(),
        labels: vec![],
    })
    .unwrap();
    let segment = wal.path();
    let first_frame_len = fs::metadata(&segment).unwrap().len();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "second".to_string(),
        labels: vec![],
    })
    .unwrap();
    drop(wal);

    let complete_segment = fs::read(&segment).unwrap();
    assert!(complete_segment.len() > first_frame_len as usize);
    (segment, first_frame_len, complete_segment)
}

fn seed_three_segment_published_wal(path: &Path) -> Vec<PathBuf> {
    let definitions = (1..=3)
        .map(|series_id| SeriesDefinitionFrame {
            series_id,
            metric: format!("segment_{series_id}"),
            labels: vec![],
        })
        .collect::<Vec<_>>();
    let segment_max_bytes =
        FramedWal::estimate_series_definition_frame_bytes(&definitions[0]).unwrap();
    let wal =
        FramedWal::open_with_options(path, WalSyncMode::PerAppend, 128, segment_max_bytes).unwrap();
    for definition in &definitions {
        wal.append_series_definition(definition).unwrap();
    }
    drop(wal);

    let segments = collect_wal_segment_files(path)
        .unwrap()
        .into_iter()
        .map(|segment| segment.path)
        .collect::<Vec<_>>();
    assert!(
        segments.len() >= 3,
        "the namespace-integrity tests require at least three published WAL segments",
    );
    segments
}

#[test]
fn published_highwater_codec_preserves_legacy_and_v2_reset_records() {
    let published = WalHighWatermark {
        segment: 7,
        frame: 19,
    };
    let reset_through = WalHighWatermark {
        segment: 6,
        frame: 11,
    };

    let legacy = PublishedHighwaterRecord::commit(published);
    let legacy_bytes = encode_published_highwater_record(legacy);
    assert_eq!(legacy_bytes.len(), PUBLISHED_HIGHWATER_RECORD_LEN);
    assert_eq!(&legacy_bytes[..4], b"TSHW");
    assert_eq!(
        decode_published_highwater_record(&legacy_bytes).unwrap(),
        legacy
    );

    let v2 = PublishedHighwaterRecord::with_reset_floor(published, reset_through).unwrap();
    let v2_bytes = encode_published_highwater_record(v2);
    assert_eq!(v2_bytes.len(), PUBLISHED_HIGHWATER_V2_RECORD_LEN);
    assert_eq!(&v2_bytes[..4], b"TSH2");
    assert_eq!(decode_published_highwater_record(&v2_bytes).unwrap(), v2);

    let mut corrupt = v2_bytes;
    *corrupt.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        decode_published_highwater_record(&corrupt),
        Err(TsinkError::DataCorruption(message)) if message.contains("checksum mismatch")
    ));
    assert!(matches!(
        PublishedHighwaterRecord::with_reset_floor(reset_through, published),
        Err(TsinkError::DataCorruption(message)) if message.contains("reset floor")
    ));
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
fn wal_reopen_discards_persisted_writes_that_never_crossed_publish_boundary() {
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
    assert!(reopened.replay_frames().unwrap().is_empty());
    assert!(reopened.replay_committed_writes().unwrap().is_empty());
    assert!(reopened
        .committed_series_definitions_snapshot()
        .unwrap()
        .is_empty());

    let committed_definition = SeriesDefinitionFrame {
        series_id: 8,
        metric: "committed_after_recovery".to_string(),
        labels: vec![Label::new("host", "b")],
    };
    let committed_batch = SamplesBatchFrame::from_points(
        8,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 20,
            value: Value::F64(2.0),
        }],
    )
    .unwrap();
    reopened
        .append_series_definition(&committed_definition)
        .unwrap();
    reopened.append_samples(&[committed_batch]).unwrap();
    drop(reopened);

    let reopened_again = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    let committed_series_ids = reopened_again
        .replay_committed_writes()
        .unwrap()
        .into_iter()
        .flat_map(|write| write.sample_batches)
        .map(|batch| batch.series_id)
        .collect::<Vec<_>>();
    assert_eq!(committed_series_ids, vec![8]);
}

#[test]
fn strict_wal_open_rejects_corrupt_published_frame_without_mutating_namespace() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "published".to_string(),
        labels: vec![],
    })
    .unwrap();
    let segment_path = wal.path();
    drop(wal);

    let mut bytes = fs::read(&segment_path).unwrap();
    assert!(bytes.len() > FRAME_HEADER_LEN);
    bytes[FRAME_HEADER_LEN] ^= 0x5a;
    fs::write(&segment_path, bytes).unwrap();
    let before = wal_directory_image(temp_dir.path());

    let err = match FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend) {
        Ok(_) => panic!("strict WAL open must reject a corrupt published frame"),
        Err(err) => err,
    };

    assert!(
        matches!(err, TsinkError::DataCorruption(ref message) if message.contains("published WAL frame") && message.contains("checksum mismatch")),
        "{err}"
    );
    assert_eq!(wal_directory_image(temp_dir.path()), before);
}

#[test]
fn wal_open_rejects_empty_boundary_above_replay_floor_without_mutation() {
    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "published".to_string(),
            labels: vec![],
        })
        .unwrap();
        let segment = wal.path();
        drop(wal);
        fs::write(&segment, []).unwrap();
        let before = wal_directory_image(temp_dir.path());

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_floor(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
            WalHighWatermark::default(),
        ) {
            Ok(_) => panic!("{replay_mode:?} open must require the published boundary frame"),
            Err(error) => error,
        };

        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains("WAL publish boundary frame 1 is missing")
            ),
            "{error:?}",
        );
        assert_eq!(
            wal_directory_image(temp_dir.path()),
            before,
            "{replay_mode:?} rejection must not mutate an empty boundary segment",
        );
    }
}

#[test]
fn wal_open_rejects_first_frame_after_boundary_above_replay_floor_without_mutation() {
    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let temp_dir = TempDir::new().unwrap();
        let (segment, first_frame_len, complete_segment) =
            seed_two_published_frames(temp_dir.path());
        rewrite_published_highwater(
            temp_dir.path(),
            WalHighWatermark {
                segment: 0,
                frame: 1,
            },
        );
        fs::write(&segment, &complete_segment[first_frame_len as usize..]).unwrap();
        let before = wal_directory_image(temp_dir.path());

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_floor(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
            WalHighWatermark::default(),
        ) {
            Ok(_) => panic!("{replay_mode:?} open must require the published boundary frame"),
            Err(error) => error,
        };

        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains(
                        "WAL publish boundary frame 1 is missing before frame 2"
                    )
            ),
            "{error:?}",
        );
        assert_eq!(
            wal_directory_image(temp_dir.path()),
            before,
            "{replay_mode:?} rejection must not truncate the higher first frame",
        );
    }
}

#[test]
fn wal_open_rejects_short_published_prefix_above_replay_floor_without_mutation() {
    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let temp_dir = TempDir::new().unwrap();
        let (segment, first_frame_len, complete_segment) =
            seed_two_published_frames(temp_dir.path());
        fs::write(&segment, &complete_segment[..first_frame_len as usize]).unwrap();
        let before = wal_directory_image(temp_dir.path());

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_floor(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
            WalHighWatermark::default(),
        ) {
            Ok(_) => panic!("{replay_mode:?} open must require the published boundary frame"),
            Err(error) => error,
        };

        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains("WAL publish boundary frame 2 is missing")
            ),
            "{error:?}",
        );
        assert_eq!(
            wal_directory_image(temp_dir.path()),
            before,
            "{replay_mode:?} rejection must not mutate the short published prefix",
        );
    }
}

#[test]
fn wal_open_allows_absent_boundary_only_when_checkpoint_floor_covers_marker() {
    for retain_higher_frame in [false, true] {
        let temp_dir = TempDir::new().unwrap();
        let (segment, first_frame_len, complete_segment) =
            seed_two_published_frames(temp_dir.path());
        rewrite_published_highwater(
            temp_dir.path(),
            WalHighWatermark {
                segment: 0,
                frame: 1,
            },
        );
        let boundary_bytes = if retain_higher_frame {
            &complete_segment[first_frame_len as usize..]
        } else {
            &[]
        };
        fs::write(&segment, boundary_bytes).unwrap();
        let marker_before =
            fs::read(temp_dir.path().join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)).unwrap();

        let reopened = FramedWal::open_with_buffer_size_and_disk_budget_and_replay_floor(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            WalReplayMode::Strict,
            WalHighWatermark {
                segment: 0,
                frame: 1,
            },
        )
        .unwrap();

        assert_eq!(
            reopened.current_published_highwater(),
            WalHighWatermark {
                segment: 0,
                frame: 1,
            },
        );
        assert_eq!(
            fs::metadata(&segment).unwrap().len(),
            0,
            "checkpoint-covered empty and higher-first-frame boundaries are both legal",
        );
        assert_eq!(
            fs::read(temp_dir.path().join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)).unwrap(),
            marker_before,
            "checkpoint authorization must not rewrite the publication marker",
        );
    }
}

#[test]
fn strict_wal_open_truncates_corrupt_unpublished_suffix_after_full_preflight() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "published".to_string(),
        labels: vec![],
    })
    .unwrap();
    let segment_path = wal.path();
    let published_len = fs::metadata(&segment_path).unwrap().len();
    drop(wal);

    let mut file = OpenOptions::new().append(true).open(&segment_path).unwrap();
    file.write_all(b"corrupt-unpublished-tail").unwrap();
    file.sync_data().unwrap();
    drop(file);
    assert!(fs::metadata(&segment_path).unwrap().len() > published_len);

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();

    assert_eq!(fs::metadata(&segment_path).unwrap().len(), published_len);
    assert_eq!(
        reopened.current_published_highwater(),
        WalHighWatermark {
            segment: 0,
            frame: 1,
        }
    );
}

#[cfg(unix)]
#[test]
fn wal_truncation_preflight_failure_in_later_segment_leaves_earlier_suffix_unchanged() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "published".to_string(),
        labels: vec![],
    })
    .unwrap();
    let boundary_segment = wal.path();
    drop(wal);

    let mut file = OpenOptions::new()
        .append(true)
        .open(&boundary_segment)
        .unwrap();
    file.write_all(b"unpublished-tail").unwrap();
    file.sync_data().unwrap();
    drop(file);
    let before = fs::read(&boundary_segment).unwrap();

    let inaccessible = segment_path(temp_dir.path(), 1);
    fs::write(&inaccessible, b"later").unwrap();
    fs::set_permissions(&inaccessible, fs::Permissions::from_mode(0o000)).unwrap();
    let result = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend);
    fs::set_permissions(&inaccessible, fs::Permissions::from_mode(0o600)).unwrap();

    if result.is_ok() {
        // Privileged test runners can bypass mode bits; the two-phase behavior is covered by the
        // normal unprivileged CI path.
        return;
    }
    assert_eq!(fs::read(&boundary_segment).unwrap(), before);
}

#[test]
fn wal_reopen_discards_unpublished_frames_from_a_later_segment() {
    let temp_dir = TempDir::new().unwrap();
    let baseline_definition = SeriesDefinitionFrame {
        series_id: 1,
        metric: "baseline".to_string(),
        labels: vec![],
    };
    let baseline_batch = SamplesBatchFrame::from_points(
        1,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 1,
            value: Value::F64(1.0),
        }],
    )
    .unwrap();
    let segment_max_bytes =
        FramedWal::estimate_series_definition_frame_bytes(&baseline_definition).unwrap();
    let wal = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        segment_max_bytes,
    )
    .unwrap();
    wal.append_series_definition(&baseline_definition).unwrap();
    wal.append_samples(&[baseline_batch]).unwrap();
    let published_highwater = wal.current_published_highwater();

    let abandoned_definition = SeriesDefinitionFrame {
        series_id: 2,
        metric: "abandoned".to_string(),
        labels: vec![],
    };
    let abandoned_batch = SamplesBatchFrame::from_points(
        2,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 2,
            value: Value::F64(2.0),
        }],
    )
    .unwrap();
    let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&abandoned_definition)
        .unwrap()
        .saturating_add(
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&abandoned_batch))
                .unwrap(),
        );
    let definition_payload =
        FramedWal::encode_series_definition_frame_payload(&abandoned_definition).unwrap();
    let samples_payload =
        FramedWal::encode_samples_frame_payload(std::slice::from_ref(&abandoned_batch)).unwrap();
    let mut abandoned = wal.begin_logical_write(estimated_bytes).unwrap();
    abandoned
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    abandoned.append_samples_payload(&samples_payload).unwrap();
    let abandoned_highwater = abandoned.persist_pending().unwrap();
    assert!(abandoned_highwater.segment > published_highwater.segment);
    std::mem::forget(abandoned);
    drop(wal);

    let reopened = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        segment_max_bytes,
    )
    .unwrap();
    assert_eq!(reopened.current_published_highwater(), published_highwater);
    let committed_series_ids = reopened
        .replay_committed_writes()
        .unwrap()
        .into_iter()
        .flat_map(|write| write.sample_batches)
        .map(|batch| batch.series_id)
        .collect::<Vec<_>>();
    assert_eq!(committed_series_ids, vec![1]);
    assert_eq!(std::fs::metadata(reopened.path()).unwrap().len(), 0);
}

#[test]
fn wal_open_rejects_a_gap_in_the_published_segment_namespace_without_mutation() {
    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let temp_dir = TempDir::new().unwrap();
        let segments = seed_three_segment_published_wal(temp_dir.path());
        fs::remove_file(&segments[1]).unwrap();
        let before = wal_directory_image(temp_dir.path());

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_floor(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
            WalHighWatermark::default(),
        ) {
            Ok(_) => panic!("{replay_mode:?} open must reject a published WAL segment gap"),
            Err(error) => error,
        };
        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains("non-contiguous WAL segment namespace")
            ),
            "{error:?}",
        );
        assert_eq!(
            wal_directory_image(temp_dir.path()),
            before,
            "{replay_mode:?} rejection must not change WAL files or the publication marker",
        );
    }
}

#[test]
fn wal_open_rejects_duplicate_zero_segment_aliases_without_mutation() {
    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let temp_dir = TempDir::new().unwrap();
        let segments = seed_three_segment_published_wal(temp_dir.path());
        let legacy_alias = temp_dir.path().join(WAL_FILE_NAME);
        fs::copy(&segments[0], &legacy_alias).unwrap();
        let before = wal_directory_image(temp_dir.path());

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
        ) {
            Ok(_) => panic!("{replay_mode:?} open must reject duplicate segment-zero aliases"),
            Err(error) => error,
        };
        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains("duplicate WAL segment id 0")
            ),
            "{error:?}",
        );
        assert_eq!(
            wal_directory_image(temp_dir.path()),
            before,
            "{replay_mode:?} rejection must not change WAL files or the publication marker",
        );
    }
}

#[cfg(unix)]
#[test]
fn wal_open_rejects_a_segment_symlink_without_touching_its_target() {
    use std::os::unix::fs::symlink;

    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let wal_dir = TempDir::new().unwrap();
        let external_dir = TempDir::new().unwrap();
        let external = external_dir.path().join("sentinel");
        fs::write(&external, b"external-sentinel").unwrap();
        let segment = segment_path(wal_dir.path(), 0);
        symlink(&external, &segment).unwrap();

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode(
            wal_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
        ) {
            Ok(_) => panic!("{replay_mode:?} open must reject a recognized segment symlink"),
            Err(error) => error,
        };
        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains("recognized WAL segment must be a regular non-link file")
            ),
            "{error:?}",
        );
        assert_eq!(fs::read(&external).unwrap(), b"external-sentinel");
        assert!(fs::symlink_metadata(&segment)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(
            !wal_dir
                .path()
                .join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)
                .exists(),
            "rejected symlink namespace must not publish a WAL boundary",
        );
        assert_eq!(fs::read_dir(wal_dir.path()).unwrap().count(), 1);
    }
}

#[cfg(unix)]
#[test]
fn wal_open_rejects_a_published_marker_symlink_without_mutating_the_wal_or_target() {
    use std::os::unix::fs::symlink;

    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let root = TempDir::new().unwrap();
        let wal_dir = root.path().join("wal");
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "published".to_string(),
            labels: vec![],
        })
        .unwrap();
        let segment = wal.path();
        let segment_before = fs::read(&segment).unwrap();
        let marker = wal_dir.join(WAL_PUBLISHED_HIGHWATER_FILE_NAME);
        let valid_marker = fs::read(&marker).unwrap();
        drop(wal);

        let external = root.path().join("external-marker");
        fs::write(&external, &valid_marker).unwrap();
        fs::remove_file(&marker).unwrap();
        symlink(&external, &marker).unwrap();

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode(
            &wal_dir,
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
        ) {
            Ok(_) => panic!("{replay_mode:?} open must reject a linked publish marker"),
            Err(error) => error,
        };
        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains(
                        "WAL publish boundary marker must be a regular non-link file"
                    )
            ),
            "{error:?}",
        );
        assert_eq!(fs::read(&segment).unwrap(), segment_before);
        assert_eq!(fs::read(&external).unwrap(), valid_marker);
        assert!(fs::symlink_metadata(&marker)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(
            !wal_dir.join(WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME).exists(),
            "rejected marker reads must not stage a replacement",
        );
    }
}

#[test]
fn wal_open_rejects_an_oversized_published_marker_without_mutating_the_wal() {
    for replay_mode in [WalReplayMode::Strict, WalReplayMode::Salvage] {
        let temp_dir = TempDir::new().unwrap();
        let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "published".to_string(),
            labels: vec![],
        })
        .unwrap();
        let segment = wal.path();
        let segment_before = fs::read(&segment).unwrap();
        drop(wal);

        let marker = temp_dir.path().join(WAL_PUBLISHED_HIGHWATER_FILE_NAME);
        let oversized = vec![0xa5; PUBLISHED_HIGHWATER_MAX_RECORD_LEN + 1];
        fs::write(&marker, &oversized).unwrap();

        let error = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode(
            temp_dir.path(),
            WalSyncMode::PerAppend,
            128,
            None,
            replay_mode,
        ) {
            Ok(_) => panic!("{replay_mode:?} open must reject an oversized publish marker"),
            Err(error) => error,
        };
        assert!(
            matches!(
                &error,
                TsinkError::DataCorruption(message)
                    if message.contains("WAL publish boundary record must be")
            ),
            "{error:?}",
        );
        assert_eq!(fs::read(&segment).unwrap(), segment_before);
        assert_eq!(fs::read(&marker).unwrap(), oversized);
        assert!(
            !temp_dir
                .path()
                .join(WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME)
                .exists(),
            "rejected marker reads must not stage a replacement",
        );
    }
}

#[cfg(unix)]
#[test]
fn wal_publication_unlinks_stale_tmp_links_without_mutating_their_targets() {
    use std::os::unix::fs::symlink;

    for hard_link in [false, true] {
        let root = TempDir::new().unwrap();
        let wal_dir = root.path().join("wal");
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        let external = root.path().join(if hard_link {
            "hard-link-target"
        } else {
            "symlink-target"
        });
        fs::write(&external, b"external-sentinel").unwrap();
        let temporary = wal_dir.join(WAL_PUBLISHED_HIGHWATER_TMP_FILE_NAME);
        if hard_link {
            fs::hard_link(&external, &temporary).unwrap();
        } else {
            symlink(&external, &temporary).unwrap();
        }

        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 1,
            metric: "published".to_string(),
            labels: vec![],
        })
        .unwrap();

        assert_eq!(
            fs::read(&external).unwrap(),
            b"external-sentinel",
            "{} target must not be opened through the owned temporary name",
            if hard_link { "hard-link" } else { "symlink" },
        );
        assert!(
            matches!(
                fs::symlink_metadata(&temporary),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound
            ),
            "successful marker replacement must consume its temporary entry",
        );
        let marker_metadata =
            fs::symlink_metadata(wal_dir.join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)).unwrap();
        assert!(marker_metadata.file_type().is_file());
        assert!(!crate::engine::fs_utils::is_link_or_reparse_point(
            &marker_metadata
        ));
        assert_eq!(wal.replay_frames().unwrap().len(), 1);
    }
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
fn limited_logical_writes_check_quota_under_the_writer_lock() {
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap());
    let payload = FramedWal::encode_series_definition_frame_payload(&SeriesDefinitionFrame {
        series_id: 17,
        metric: "quota_race".to_string(),
        labels: Vec::new(),
    })
    .unwrap();
    let estimated_bytes = FramedWal::frame_size_bytes_for_payload_len(payload.len());
    let limit = wal
        .total_size_bytes()
        .unwrap()
        .saturating_add(estimated_bytes);
    let start = Arc::new(Barrier::new(3));

    let mut handles = Vec::new();
    for _ in 0..2 {
        let wal = Arc::clone(&wal);
        let payload = payload.clone();
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || -> crate::Result<()> {
            start.wait();
            let mut logical = wal.begin_limited_logical_write(estimated_bytes, limit)?;
            logical.append_series_definition_payload(&payload)?;
            logical.persist_pending()?;
            logical.publish_persisted()?;
            Ok(())
        }));
    }

    start.wait();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(TsinkError::WalSizeLimitExceeded { .. })))
            .count(),
        1
    );
    assert_eq!(wal.total_size_bytes().unwrap(), limit);
    assert_runtime_accounting_matches_disk(&wal, temp_dir.path());
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
fn series_definition_cache_memory_model_tracks_pending_commit_and_reset() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_eq!(wal.cached_series_definition_index_memory_usage_bytes(), 0);

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 42,
        metric: "cache_memory_metric".to_string(),
        labels: vec![Label::new("host", "cache-memory-value")],
    })
    .unwrap();
    let pending_bytes = wal.cached_series_definition_index_memory_usage_bytes();
    assert!(pending_bytes > "cache_memory_metric".len());

    wal.append_samples(&[SamplesBatchFrame::from_points(
        42,
        ValueLane::Numeric,
        &[ChunkPoint {
            ts: 1,
            value: Value::F64(1.0),
        }],
    )
    .unwrap()])
        .unwrap();
    let committed_bytes = wal.cached_series_definition_index_memory_usage_bytes();
    assert!(committed_bytes > "cache-memory-value".len());

    wal.reset().unwrap();
    assert_eq!(wal.cached_series_definition_index_memory_usage_bytes(), 0);
}

#[test]
fn conditional_reset_does_not_publish_a_cache_delta_when_newer_wal_data_exists() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 43,
        metric: "cache_reset_skipped".to_string(),
        labels: vec![Label::new("host", "newer")],
    })
    .unwrap();
    let cache_bytes_before = wal.cached_series_definition_index_memory_usage_bytes();
    assert!(cache_bytes_before > 0);

    let callbacks = AtomicU64::new(0);
    assert!(!wal
        .reset_if_current_highwater_at_most(WalHighWatermark::default(), |_| {
            callbacks.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap());
    assert_eq!(callbacks.load(Ordering::Relaxed), 0);
    assert_eq!(
        wal.cached_series_definition_index_memory_usage_bytes(),
        cache_bytes_before
    );
}

#[test]
fn cache_observation_and_reset_delta_are_serialized_by_the_cache_mutex() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap());
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 44,
        metric: "cache_reset_serialized".to_string(),
        labels: vec![Label::new("host", "serialized")],
    })
    .unwrap();
    let observed_before = wal.cached_series_definition_index_memory_usage_bytes();
    assert!(observed_before > 0);

    let accounted = Arc::new(AtomicU64::new(0));
    let (observation_entered_tx, observation_entered_rx) = mpsc::channel();
    let (release_observation_tx, release_observation_rx) = mpsc::channel();
    let observer_wal = Arc::clone(&wal);
    let observer_accounted = Arc::clone(&accounted);
    let observer = thread::spawn(move || {
        observer_wal.with_cached_series_definition_index_memory_usage_bytes(|bytes| {
            observation_entered_tx.send(()).unwrap();
            release_observation_rx.recv().unwrap();
            observer_accounted.store(bytes as u64, Ordering::Release);
        });
    });
    observation_entered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let reset_wal = Arc::clone(&wal);
    let reset_accounted = Arc::clone(&accounted);
    let (reset_started_tx, reset_started_rx) = mpsc::channel();
    let (reset_finished_tx, reset_finished_rx) = mpsc::channel();
    let reset = thread::spawn(move || {
        reset_started_tx.send(()).unwrap();
        let result = reset_wal.reset_if_current_highwater_at_most(
            WalHighWatermark {
                segment: u64::MAX,
                frame: u64::MAX,
            },
            |bytes| reset_accounted.store(bytes as u64, Ordering::Release),
        );
        reset_finished_tx.send(result).unwrap();
    });
    reset_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(
        reset_finished_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "reset must wait until the older cache observation publishes its charge"
    );

    release_observation_tx.send(()).unwrap();
    observer.join().unwrap();
    assert!(reset_finished_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap());
    reset.join().unwrap();

    assert_eq!(
        accounted.load(Ordering::Acquire) as usize,
        wal.cached_series_definition_index_memory_usage_bytes(),
        "the reset delta must win after the serialized older growth observation"
    );
}

#[test]
fn reset_reports_exact_retained_buffer_capacity_during_cache_rebuild() {
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap());
    let rebuild_entered = Arc::new(Barrier::new(2));
    let rebuild_release = Arc::new(Barrier::new(2));
    wal.set_cached_series_definition_rebuild_hook({
        let rebuild_entered = Arc::clone(&rebuild_entered);
        let rebuild_release = Arc::clone(&rebuild_release);
        move || {
            rebuild_entered.wait();
            rebuild_release.wait();
        }
    });

    let rebuild_wal = Arc::clone(&wal);
    let rebuild = thread::spawn(move || rebuild_wal.committed_series_definitions_snapshot());
    rebuild_entered.wait();

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 45,
        metric: "buffered_during_rebuild".to_string(),
        labels: vec![Label::new("host", "buffered")],
    })
    .unwrap();
    assert!(
        wal.cached_series_definition_index_memory_usage_bytes() > 0,
        "the in-flight rebuild should retain the appended frame in its buffer"
    );

    let accounted_after_reset = AtomicU64::new(u64::MAX);
    assert!(wal
        .reset_if_current_highwater_at_most(
            WalHighWatermark {
                segment: u64::MAX,
                frame: u64::MAX,
            },
            |bytes| accounted_after_reset.store(bytes as u64, Ordering::Release),
        )
        .unwrap());
    let exact_after_reset = wal.cached_series_definition_index_memory_usage_bytes();
    assert!(
        exact_after_reset > 0,
        "clear retains the buffered-frame Vec capacity and must keep charging it"
    );
    assert_eq!(
        accounted_after_reset.load(Ordering::Acquire) as usize,
        exact_after_reset
    );

    rebuild_release.wait();
    assert!(rebuild.join().unwrap().unwrap().is_empty());
    wal.clear_cached_series_definition_rebuild_hook();
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
fn wal_reset_reconciles_disk_budget_while_over_logical_quota() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    {
        let wal = FramedWal::open_with_options(
            &wal_dir,
            WalSyncMode::PerAppend,
            128,
            (FRAME_HEADER_LEN as u64) + 40,
        )
        .unwrap();
        for series_id in 0..8 {
            wal.append_series_definition(&SeriesDefinitionFrame {
                series_id,
                metric: format!("reset_budget_{series_id}"),
                labels: vec![Label::new("host", "a")],
            })
            .unwrap();
        }
        assert!(collect_wal_segment_files(&wal_dir).unwrap().len() > 1);
    }

    let budget = LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            max_bytes: Some(1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    assert!(budget.snapshot().over_limit);
    let wal = FramedWal::open_with_buffer_size_and_disk_budget(
        &wal_dir,
        WalSyncMode::PerAppend,
        128,
        Some(Arc::clone(&budget)),
    )
    .unwrap();

    wal.reset().unwrap();

    assert!(wal.replay_frames().unwrap().is_empty());
    assert_runtime_accounting_matches_disk(&wal, &wal_dir);
    assert_eq!(wal.total_size_bytes().unwrap(), 0);
    assert_eq!(wal.segment_count().unwrap(), 1);
    let marker_bytes = fs::metadata(wal_dir.join(WAL_PUBLISHED_HIGHWATER_FILE_NAME))
        .unwrap()
        .len();
    let snapshot = budget.snapshot();
    let accounted_wal_bytes = snapshot
        .categories
        .iter()
        .find(|usage| usage.category == DiskCategory::Wal)
        .map(|usage| usage.bytes)
        .unwrap_or(0);
    assert_eq!(accounted_wal_bytes, marker_bytes);
    assert_eq!(snapshot.accounted_bytes, marker_bytes);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    assert_eq!(snapshot.active_reservations, 0);
    assert_eq!(snapshot.reconciliations_total, 2);
    assert!(snapshot.over_limit);
}

#[test]
fn wal_reset_preserves_frames_when_recovery_headroom_is_unavailable() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    {
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: 7,
            metric: "reset_headroom".to_string(),
            labels: vec![Label::new("host", "a")],
        })
        .unwrap();
    }

    let budget = LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            filesystem_free_headroom_bytes: u64::MAX,
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let wal = FramedWal::open_with_buffer_size_and_disk_budget(
        &wal_dir,
        WalSyncMode::PerAppend,
        128,
        Some(Arc::clone(&budget)),
    )
    .unwrap();
    let frames_before = wal.replay_frames().unwrap();
    let accounted_before = budget.snapshot().accounted_bytes;

    let err = wal.reset().unwrap_err();

    assert!(matches!(
        err,
        TsinkError::InsufficientDiskSpace {
            required,
            available: 0
        } if required == PUBLISHED_HIGHWATER_MAX_RECORD_LEN as u64
    ));
    assert_eq!(wal.replay_frames().unwrap().len(), frames_before.len());
    assert_runtime_accounting_matches_disk(&wal, &wal_dir);
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.accounted_bytes, accounted_before);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    assert_eq!(snapshot.active_reservations, 0);
    assert_eq!(snapshot.rejections_total, 1);
    assert_eq!(snapshot.reconciliations_total, 1);
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
fn reset_marker_sync_failpoint_recovery_finishes_the_authorized_reset() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 7,
        metric: "before_reset".to_string(),
        labels: vec![],
    })
    .unwrap();
    let reset_floor = wal.current_highwater();
    let segment = wal.path();
    let segment_before = fs::read(&segment).unwrap();
    wal.set_durability_failpoint_hook(|point| {
        if point == super::WalDurabilityFailpoint::ResetAfterMarkerSync {
            return Err(TsinkError::Other(
                "injected reset failure after marker sync".to_string(),
            ));
        }
        Ok(())
    });

    let error = wal.reset().unwrap_err();
    assert!(error
        .to_string()
        .contains("injected reset failure after marker sync"));
    assert_eq!(
        fs::read(&segment).unwrap(),
        segment_before,
        "the failpoint fires before any WAL frame is removed",
    );
    wal.clear_durability_failpoint_hook();
    drop(wal);

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_eq!(reopened.current_highwater(), reset_floor);
    assert!(reopened.replay_frames().unwrap().is_empty());
    assert_eq!(fs::metadata(reopened.path()).unwrap().len(), 0);
    reopened
        .append_series_definition(&SeriesDefinitionFrame {
            series_id: 8,
            metric: "after_reset".to_string(),
            labels: vec![],
        })
        .unwrap();
    assert!(reopened.current_published_highwater() > reset_floor);
}

#[test]
fn reset_floor_survives_rotation_reopen_and_a_later_commit() {
    let temp_dir = TempDir::new().unwrap();
    let first = SeriesDefinitionFrame {
        series_id: 7,
        metric: "before_reset".to_string(),
        labels: vec![],
    };
    let segment_max_bytes = FramedWal::estimate_series_definition_frame_bytes(&first).unwrap();
    let wal = FramedWal::open_with_options(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        segment_max_bytes,
    )
    .unwrap();
    wal.append_series_definition(&first).unwrap();
    assert_eq!(
        wal.active_segment(),
        1,
        "the append must leave a newer empty segment"
    );

    wal.reset().unwrap();
    let reset_floor = WalHighWatermark {
        segment: 1,
        frame: 0,
    };
    let marker_path = temp_dir.path().join(WAL_PUBLISHED_HIGHWATER_FILE_NAME);
    let reset_record = decode_published_highwater_record(&fs::read(&marker_path).unwrap()).unwrap();
    assert_eq!(reset_record.highwater, reset_floor);
    assert_eq!(reset_record.reset_through, Some(reset_floor));
    assert_eq!(
        fs::metadata(&marker_path).unwrap().len(),
        PUBLISHED_HIGHWATER_V2_RECORD_LEN as u64,
    );
    drop(wal);

    let reopened = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_eq!(reopened.current_highwater(), reset_floor);
    assert_eq!(reopened.current_durable_highwater(), reset_floor);
    assert!(reopened.replay_frames().unwrap().is_empty());

    reopened
        .append_series_definition(&SeriesDefinitionFrame {
            series_id: 8,
            metric: "after_reset".to_string(),
            labels: vec![],
        })
        .unwrap();
    let committed = reopened.current_published_highwater();
    assert!(committed > reset_floor);
    let committed_record =
        decode_published_highwater_record(&fs::read(&marker_path).unwrap()).unwrap();
    assert_eq!(committed_record.highwater, committed);
    assert_eq!(committed_record.reset_through, Some(reset_floor));
    drop(reopened);

    let reopened_again = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    assert_eq!(reopened_again.current_published_highwater(), committed);
    assert_eq!(reopened_again.replay_frames().unwrap().len(), 1);
}

#[test]
fn post_reset_commit_still_requires_its_exact_published_frame() {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "before_reset".to_string(),
        labels: vec![],
    })
    .unwrap();
    wal.reset().unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 2,
        metric: "after_reset".to_string(),
        labels: vec![],
    })
    .unwrap();
    let segment = wal.path();
    let published = wal.current_published_highwater();
    let marker_before = fs::read(temp_dir.path().join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)).unwrap();
    drop(wal);
    fs::write(&segment, []).unwrap();
    let before = wal_directory_image(temp_dir.path());

    let error = match FramedWal::open(temp_dir.path(), WalSyncMode::PerAppend) {
        Ok(_) => panic!("a post-reset commit must retain an exact published frame"),
        Err(error) => error,
    };
    assert!(
        matches!(
            &error,
            TsinkError::DataCorruption(message)
                if message.contains(&format!(
                    "WAL publish boundary frame {} is missing",
                    published.frame
                ))
        ),
        "{error:?}",
    );
    assert_eq!(wal_directory_image(temp_dir.path()), before);
    assert_eq!(
        fs::read(temp_dir.path().join(WAL_PUBLISHED_HIGHWATER_FILE_NAME)).unwrap(),
        marker_before,
    );
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
    drop(wal);

    let reopened = FramedWal::open_with_buffer_size_and_disk_budget_and_replay_floor(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        128,
        None,
        WalReplayMode::Strict,
        WalHighWatermark {
            segment: 3,
            frame: 8,
        },
    )
    .unwrap();
    assert_eq!(reopened.active_segment(), 3);
    assert_eq!(
        reopened.current_highwater(),
        WalHighWatermark {
            segment: 3,
            frame: 9,
        },
    );
    assert_runtime_accounting_matches_disk(&reopened, temp_dir.path());
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
fn salvage_wal_open_rejects_corrupt_markerless_segment_without_mutating_namespace() {
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
    drop(file);
    let before = fs::read(&wal_path).unwrap();

    let err = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        1,
        None,
        WalReplayMode::Salvage,
    ) {
        Ok(_) => panic!("salvage WAL open must reject a corrupt markerless published prefix"),
        Err(err) => err,
    };

    assert!(
        matches!(
            err,
            TsinkError::DataCorruption(ref message)
                if message.contains("published WAL frame 2")
                    && message.contains("checksum mismatch")
        ),
        "{err}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), before);
    assert_eq!(fs::read_dir(temp_dir.path()).unwrap().count(), 1);
}

#[test]
fn strict_wal_open_rejects_corrupt_active_segment_without_mutating_namespace() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join(WAL_FILE_NAME);
    let payload = encode_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "cpu".to_string(),
        labels: vec![],
    })
    .unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&series_def_frame_header(
        1,
        payload.len(),
        checksum32(&payload),
    ));
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(b"W2FR\x02\x00\x00\x00\x00");
    fs::write(&wal_path, &bytes).unwrap();
    let before = fs::read(&wal_path).unwrap();

    let err = match FramedWal::open_with_buffer_size_and_disk_budget_and_replay_mode(
        temp_dir.path(),
        WalSyncMode::PerAppend,
        1,
        None,
        WalReplayMode::Strict,
    ) {
        Ok(_) => panic!("strict WAL open must reject a corrupt markerless published prefix"),
        Err(err) => err,
    };

    assert!(
        matches!(
            err,
            TsinkError::DataCorruption(ref message)
                if message.contains("published WAL prefix has a truncated frame header")
        ),
        "{err}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), before);
    assert_eq!(fs::read_dir(temp_dir.path()).unwrap().count(), 1);
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
        reset_highwater_floor: parking_lot::Mutex::new(None),
        last_durable_highwater: parking_lot::Mutex::new(WalHighWatermark {
            segment: 0,
            frame: 6,
        }),
        configured_replay_mode: parking_lot::Mutex::new(WalReplayMode::Strict),
        sync_mode: WalSyncMode::PerAppend,
        last_sync: parking_lot::Mutex::new(Instant::now()),
        segment_max_bytes: DEFAULT_WAL_SEGMENT_MAX_BYTES,
        local_disk_budget: None,
        append_sync_hook: parking_lot::Mutex::new(None),
        published_highwater_post_rename_hook: parking_lot::Mutex::new(None),
        cached_series_definition_rebuild_hook: parking_lot::Mutex::new(None),
        durability_failpoint_hook: parking_lot::Mutex::new(None),
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
