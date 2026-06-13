use std::collections::HashMap;
use std::fs;

use tempfile::TempDir;

use super::format::{
    parse_postings_file, parse_series_file, CHUNKS_HEADER_LEN, POSTINGS_HEADER_LEN,
};
use super::{
    chunk_payload_from_record, collect_expired_segment_dirs, load_segment_index_with_series,
    load_segment_indexes_from_dirs_strict_with_series, load_segment_series_metadata, load_segments,
    load_segments_for_level, load_segments_runtime_strict, read_segment_manifest,
    read_segment_manifest_fingerprint, verify_segment_fingerprint, SegmentWriter, WalHighWatermark,
    CHUNK_FLAG_PAYLOAD_ZSTD,
};
use crate::engine::chunk::{
    Chunk, ChunkHeader, ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane,
};
use crate::engine::encoder::Encoder;
use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
use crate::{Label, TsinkError, Value};

#[test]
fn segment_roundtrip_preserves_series_and_chunks() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    let loaded = load_segments(tmp.path()).unwrap();
    assert_eq!(loaded.series.len(), 1);
    assert_eq!(loaded.series[0].metric, "cpu");
    assert_eq!(loaded.next_segment_id, 2);
    assert_eq!(loaded.chunks_by_series.len(), 1);
}

#[test]
fn segment_writer_defers_final_dir_creation_until_publish() {
    let tmp = TempDir::new().unwrap();
    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    assert!(!writer.layout().root.exists());

    let (registry, chunks_by_series) = sample_segment_input();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    assert!(writer.layout().root.exists());
    assert!(writer.layout().manifest_path.exists());
}

#[test]
fn segment_writer_replaces_stale_root_missing_manifest() {
    let tmp = TempDir::new().unwrap();
    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    fs::create_dir_all(&writer.layout().root).unwrap();
    let stale_file = writer.layout().root.join("stale.bin");
    fs::write(&stale_file, b"partial").unwrap();
    assert!(!writer.layout().manifest_path.exists());

    let (registry, chunks_by_series) = sample_segment_input();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    assert!(!stale_file.exists());
    assert!(writer.layout().manifest_path.exists());
    let loaded = load_segments(tmp.path()).unwrap();
    assert_eq!(loaded.next_segment_id, 2);
}

#[test]
fn segment_roundtrip_preserves_wal_highwater() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    let highwater = WalHighWatermark {
        segment: 3,
        frame: 77,
    };
    writer
        .write_segment_with_wal_highwater(&registry, &chunks_by_series, highwater)
        .unwrap();

    let loaded = load_segments_for_level(tmp.path(), 0).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].manifest.wal_highwater, highwater);
}

#[test]
fn segment_publish_returns_error_and_rolls_back_when_parent_sync_fails() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    let level_root = writer.layout().root.parent().unwrap().to_path_buf();
    let _guard = crate::engine::fs_utils::fail_directory_sync_once(
        level_root,
        "injected parent directory sync failure",
    );
    let err = writer
        .write_segment(&registry, &chunks_by_series)
        .expect_err("parent directory sync failure must be surfaced");
    assert!(
        err.to_string()
            .contains("injected parent directory sync failure"),
        "unexpected error: {err:?}"
    );

    assert!(
        !writer.layout().root.exists(),
        "failed publish must not leave a visible segment root behind"
    );
    assert!(
        !writer.staging_layout.root.exists(),
        "failed publish rollback should not restore the staging directory"
    );
}

#[test]
fn runtime_strict_load_rejects_invalid_segment_that_skip_policy_ignores() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    fs::remove_file(writer.layout().postings_path.clone()).unwrap();

    let loaded = load_segments(tmp.path()).unwrap();
    assert!(loaded.series.is_empty());
    assert!(loaded.chunks_by_series.is_empty());
    assert_eq!(loaded.next_segment_id, 1);

    let err = load_segments_runtime_strict(tmp.path()).unwrap_err();
    match err {
        TsinkError::DataCorruption(message) => {
            assert!(message.contains("failed compaction validation"));
            assert!(message.contains("missing postings.bin"));
        }
        other => panic!("expected strict validation error, got {other:?}"),
    }
}

#[test]
fn metadata_helpers_round_trip_manifest_series_and_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    let manifest = writer.write_segment(&registry, &chunks_by_series).unwrap();

    let read_manifest = read_segment_manifest(writer.layout().root.clone()).unwrap();
    assert_eq!(read_manifest, manifest);

    let series = load_segment_series_metadata(writer.layout().root.clone()).unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].metric, "cpu");
    assert_eq!(series[0].labels, vec![Label::new("host", "a")]);

    let fingerprint = read_segment_manifest_fingerprint(writer.layout().root.clone()).unwrap();
    let verified = verify_segment_fingerprint(writer.layout().root.clone()).unwrap();
    assert_eq!(fingerprint, verified);
    assert_eq!(fingerprint.manifest, manifest);
    assert_eq!(fingerprint.files.len(), 4);
    assert_eq!(fingerprint.files[0].kind, 1);
}

#[test]
fn segment_writer_compresses_chunk_payloads_with_zstd() {
    let tmp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series = registry
        .resolve_or_insert("cpu", &[Label::new("host", "zstd")])
        .unwrap();

    let original_payload = vec![7u8; 4096];
    let chunk = Chunk {
        header: ChunkHeader {
            series_id: series.series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: 1,
            min_ts: 10,
            max_ts: 10,
            ts_codec: TimestampCodecId::DeltaVarint,
            value_codec: ValueCodecId::ConstantRle,
        },
        points: Vec::new(),
        encoded_payload: original_payload.clone(),
        wal_highwater: WalHighWatermark::default(),
    };

    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(series.series_id, vec![chunk]);

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    let chunks_bytes = fs::read(writer.layout().chunks_path.clone()).unwrap();
    let series_bytes = fs::read(writer.layout().series_path.clone()).unwrap();
    let chunk_flags_offset = CHUNKS_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1;
    assert_ne!(
        chunks_bytes[chunk_flags_offset] & CHUNK_FLAG_PAYLOAD_ZSTD,
        0,
        "chunk payload should be stored with zstd flag for large compressible payloads"
    );
    let parsed_series = parse_series_file(&series_bytes).unwrap();
    assert_eq!(
        parsed_series.entries[0].value_family,
        Some(SeriesValueFamily::F64)
    );

    let loaded = load_segments(tmp.path()).unwrap();
    let loaded_chunks = loaded.chunks_by_series.get(&series.series_id).unwrap();
    assert_eq!(loaded_chunks.len(), 1);
    assert_eq!(loaded_chunks[0].encoded_payload, original_payload);
    assert_eq!(
        loaded
            .series
            .iter()
            .find(|persisted| persisted.series_id == series.series_id)
            .and_then(|persisted| persisted.value_family),
        Some(SeriesValueFamily::F64)
    );
}

#[test]
fn segment_writer_rewrites_loaded_chunks_without_decoding_payload_family() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();
    let series_id = *chunks_by_series.keys().next().unwrap();

    let initial_writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    initial_writer
        .write_segment(&registry, &chunks_by_series)
        .unwrap();

    let mut loaded = load_segments(tmp.path()).unwrap();
    registry
        .record_series_value_family(
            series_id,
            loaded
                .series
                .iter()
                .find(|persisted| persisted.series_id == series_id)
                .and_then(|persisted| persisted.value_family)
                .unwrap(),
        )
        .unwrap();
    let loaded_chunks = loaded.chunks_by_series.get_mut(&series_id).unwrap();
    loaded_chunks[0].points.clear();
    loaded_chunks[0].encoded_payload = vec![3u8; 4096];

    let rewrite_writer = SegmentWriter::new(tmp.path(), 1, 2).unwrap();
    rewrite_writer
        .write_segment(&registry, &loaded.chunks_by_series)
        .unwrap();

    let rewritten = load_segments_for_level(tmp.path(), 1).unwrap();
    assert_eq!(rewritten.len(), 1);
    let rewritten_chunks = rewritten[0].chunks_by_series.get(&series_id).unwrap();
    assert_eq!(rewritten_chunks[0].encoded_payload, vec![3u8; 4096]);
    assert_eq!(
        rewritten[0]
            .series
            .iter()
            .find(|persisted| persisted.series_id == series_id)
            .and_then(|persisted| persisted.value_family),
        Some(SeriesValueFamily::F64)
    );
}

#[test]
fn segment_writer_compresses_metadata_and_writes_postings_payloads() {
    let tmp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let mut chunks_by_series = HashMap::new();

    for series_idx in 0..512u64 {
        let labels = [
            Label::new("tenant", format!("t{}", series_idx % 32)),
            Label::new("host", format!("h{}", series_idx % 128)),
            Label::new("series", format!("s{series_idx}")),
        ];
        let series_id = registry
            .resolve_or_insert("cpu", &labels)
            .unwrap()
            .series_id;
        chunks_by_series.insert(
            series_id,
            vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
        );
    }

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    let series_bytes = fs::read(writer.layout().series_path.clone()).unwrap();
    let chunk_index_bytes = fs::read(writer.layout().chunk_index_path.clone()).unwrap();
    let postings_bytes = fs::read(writer.layout().postings_path.clone()).unwrap();

    let series_flags = u16::from_le_bytes([series_bytes[6], series_bytes[7]]);
    let chunk_index_flags = u16::from_le_bytes([chunk_index_bytes[6], chunk_index_bytes[7]]);
    assert_ne!(
        series_flags & crate::engine::binio::FILE_FLAG_ZSTD_BODY,
        0,
        "series metadata should be compressed when it materially shrinks"
    );
    assert_ne!(
        chunk_index_flags & crate::engine::binio::FILE_FLAG_ZSTD_BODY,
        0,
        "chunk index should be compressed when it materially shrinks"
    );
    assert!(
        postings_bytes.len() > POSTINGS_HEADER_LEN,
        "persisted segments should carry postings payloads for coarse metadata filtering"
    );

    let parsed_series = parse_series_file(&series_bytes).unwrap();
    let postings = parse_postings_file(&postings_bytes, &parsed_series).unwrap();
    assert_eq!(postings.metric_postings["cpu"].len(), 512);
    assert_eq!(postings.label_name_postings["tenant"].len(), 512);
    assert_eq!(postings.label_name_postings["host"].len(), 512);
    assert_eq!(postings.label_name_postings["series"].len(), 512);
    assert_eq!(
        postings.label_postings[&("tenant".to_string(), "t0".to_string())].len(),
        16
    );

    let loaded = load_segments(tmp.path()).unwrap();
    assert_eq!(loaded.series.len(), chunks_by_series.len());
    assert_eq!(loaded.chunks_by_series.len(), chunks_by_series.len());
}

#[test]
fn collect_expired_segment_dirs_finds_all_levels() {
    let tmp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap();
    let series_id = series.series_id;

    let mut old_chunks = HashMap::new();
    old_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(10, 1.0)])]);

    let mut newer_chunks = HashMap::new();
    newer_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(60, 2.0)])]);

    let l0_writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    l0_writer.write_segment(&registry, &old_chunks).unwrap();

    let l1_writer = SegmentWriter::new(tmp.path(), 1, 2).unwrap();
    l1_writer.write_segment(&registry, &old_chunks).unwrap();

    let l2_writer = SegmentWriter::new(tmp.path(), 2, 3).unwrap();
    l2_writer.write_segment(&registry, &newer_chunks).unwrap();

    let expired = collect_expired_segment_dirs(tmp.path(), 50).unwrap();
    assert_eq!(
        expired,
        vec![
            l0_writer.layout().root.clone(),
            l1_writer.layout().root.clone(),
        ]
    );
}

#[test]
fn strict_index_load_skips_segment_root_that_disappeared_after_inventory() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    let segment_root = writer.layout().root.clone();

    fs::remove_dir_all(&segment_root).unwrap();

    let loaded =
        load_segment_indexes_from_dirs_strict_with_series(vec![segment_root], true).unwrap();
    assert!(loaded.indexed_segments.is_empty());
    assert_eq!(loaded.next_segment_id, 1);
}

#[test]
fn indexed_segment_load_without_series_files_keeps_series_postings_from_chunk_index() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();
    let expected_series_ids = chunks_by_series.keys().copied().collect::<Vec<_>>();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    let indexed = load_segment_index_with_series(writer.layout().root.clone(), false).unwrap();
    let loaded_series_ids = indexed.postings.series_postings.iter().collect::<Vec<_>>();

    assert!(indexed.series.is_empty());
    assert!(indexed.postings.metric_postings.is_empty());
    assert!(indexed.postings.label_name_postings.is_empty());
    assert!(indexed.postings.label_postings.is_empty());
    assert_eq!(loaded_series_ids, expected_series_ids);
}

#[test]
fn chunk_payload_from_record_rejects_out_of_bounds_mapped_record() {
    let tmp = TempDir::new().unwrap();
    let (registry, chunks_by_series) = sample_segment_input();

    let writer = SegmentWriter::new(tmp.path(), 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();

    let indexed = load_segment_index_with_series(writer.layout().root.clone(), false).unwrap();
    let entry = indexed
        .chunk_index
        .entries
        .first()
        .expect("sample segment should contain one chunk");

    let err = chunk_payload_from_record(
        indexed.chunks_mmap.as_slice(),
        entry.chunk_offset,
        entry.chunk_len.saturating_add(1),
    )
    .unwrap_err();

    match err {
        TsinkError::DataCorruption(message) => {
            assert!(
                message.contains("exceeds mapped file size"),
                "expected explicit mapped bounds failure, got: {message}"
            );
        }
        other => panic!("expected mapped chunk bounds error, got {other:?}"),
    }
}

fn sample_segment_input() -> (SeriesRegistry, HashMap<u64, Vec<Chunk>>) {
    let registry = SeriesRegistry::new();
    let series = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap();

    let points = vec![
        ChunkPoint {
            ts: 10,
            value: Value::F64(1.0),
        },
        ChunkPoint {
            ts: 20,
            value: Value::F64(2.0),
        },
    ];
    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
    let chunk = Chunk {
        header: ChunkHeader {
            series_id: series.series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: points.len() as u16,
            min_ts: 10,
            max_ts: 20,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    };

    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(series.series_id, vec![chunk]);
    (registry, chunks_by_series)
}

fn make_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
    let points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();
    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}
