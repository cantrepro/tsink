use std::collections::HashMap;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

use tempfile::TempDir;

use super::{execution, finalize_pending_compaction_replacements, CompactionPassLimits, Compactor};
use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    load_segments, load_segments_for_level, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
use crate::engine::tombstone::{persist_tombstones, TombstoneRange, TOMBSTONES_FILE_NAME};
use crate::{
    HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, LocalDiskLimits,
    NativeHistogram, TsinkError, Value,
};

fn background_recovery_limits(max_items: usize, max_bytes: u64) -> CompactionPassLimits {
    CompactionPassLimits {
        max_directory_entries: max_items,
        max_manifest_inspections: max_items,
        max_source_segments: 8,
        max_source_chunks: usize::MAX,
        max_source_points: usize::MAX,
        max_decoded_bytes: max_bytes,
    }
}

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -1,
            length: 1,
        }],
        negative_deltas: vec![2],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![3, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.25, 0.5],
    }
}

#[test]
fn compacts_overlapping_l0_segments_into_l1() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].header.point_count, 4);
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    assert_eq!(decoded[0].ts, 10);
    assert_eq!(decoded[3].ts, 30);
}

#[test]
fn compaction_preserves_histogram_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("rpc_duration_seconds", &[Label::new("job", "backend")])
        .unwrap()
        .series_id;

    let first_chunk = make_histogram_chunk(series_id, &[10, 20]);
    let second_chunk = make_histogram_chunk(series_id, &[30, 40]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = chunks
        .iter()
        .flat_map(|chunk| execution::decode_chunk_points_for_compaction(chunk).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded.len(), 4);
    for (point, ts) in decoded.iter().zip([10, 20, 30, 40]) {
        assert_eq!(point.ts, ts);
        assert_eq!(point.value, Value::from(sample_histogram()));
    }
}

#[test]
fn compactor_uses_shared_segment_id_allocator() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let next_segment_id = Arc::new(AtomicU64::new(100));
    let compactor =
        Compactor::new_with_segment_id_allocator(temp_dir.path(), 8, Arc::clone(&next_segment_id));
    compactor.compact_once().unwrap();

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.segment_id, 100);
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 101);
}

#[test]
fn compactor_limits_source_window_per_pass() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=6 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert_eq!(l0.len(), 2);
    assert_eq!(l1.len(), 1);
    assert_eq!(
        l0.iter()
            .map(|segment| segment.manifest.segment_id)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
}

#[test]
fn compactor_directory_and_manifest_work_stops_at_the_pass_boundary() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "scan-bound")])
        .unwrap()
        .series_id;
    for segment_id in 1..=3 {
        let ts = segment_id as i64 * 10;
        write_numeric_segment(
            temp.path(),
            &registry,
            series_id,
            0,
            segment_id,
            &[(ts, ts as f64)],
        );
    }
    let limits = CompactionPassLimits {
        max_directory_entries: 2,
        max_manifest_inspections: 2,
        max_source_segments: 8,
        max_source_chunks: usize::MAX,
        max_source_points: usize::MAX,
        max_decoded_bytes: u64::MAX,
    };
    let compactor = Compactor::new(temp.path(), 8).with_compaction_pass_limits(limits);

    let first = compactor.compact_once_with_stats().unwrap();
    assert!(!first.compacted);
    assert!(first.planning_directory_entries_inspected <= 2);
    assert!(first.planning_manifests_inspected <= 2);
    assert!(first.planning_budget_exhausted);
    assert!(first.planning_backlog_observed);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 3);

    let second = compactor.compact_once_with_stats().unwrap();
    assert!(second.planning_directory_entries_inspected <= 2);
    assert!(second.planning_manifests_inspected <= 2);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 3);
}

#[test]
fn shared_maintenance_limits_translate_to_finite_compactor_dimensions() {
    let temp = TempDir::new().unwrap();
    let compactor = Compactor::new(temp.path(), 8).with_maintenance_work_limits(3, 960);

    assert_eq!(compactor.pass_limits.max_directory_entries, 3);
    assert_eq!(compactor.pass_limits.max_manifest_inspections, 3);
    assert_eq!(compactor.pass_limits.max_source_segments, 3);
    assert_eq!(compactor.pass_limits.max_source_chunks, 3);
    assert_eq!(
        compactor.pass_limits.max_source_points,
        960 / std::mem::size_of::<ChunkPoint>().max(1)
    );
    assert_eq!(compactor.pass_limits.max_decoded_bytes, 960);
}

#[test]
fn compactor_shared_cursor_eventually_reaches_later_overlap() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "cursor")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(0, 0.0), (10, 10.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(100, 100.0), (200, 200.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        3,
        &[(150, 150.0), (250, 250.0)],
    );
    let compactor =
        Compactor::new(temp.path(), 8).with_compaction_pass_limits(CompactionPassLimits {
            max_directory_entries: 1,
            max_manifest_inspections: 1,
            max_source_segments: 8,
            max_source_chunks: usize::MAX,
            max_source_points: usize::MAX,
            max_decoded_bytes: u64::MAX,
        });
    let clone = compactor.clone();

    let mut compacted = false;
    for pass in 0..12 {
        let stats = if pass % 2 == 0 {
            compactor.compact_once_with_stats().unwrap()
        } else {
            clone.compact_once_with_stats().unwrap()
        };
        assert!(stats.planning_directory_entries_inspected <= 1);
        assert!(stats.planning_manifests_inspected <= 1);
        if stats.compacted {
            assert_eq!(stats.source_segments, 2);
            compacted = true;
            break;
        }
    }

    assert!(
        compacted,
        "the shared cursor must eventually visit the overlap"
    );
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 1);
    assert_eq!(load_segments_for_level(temp.path(), 1).unwrap().len(), 1);
}

#[test]
fn compactor_huge_backlog_never_loads_more_than_eight_sources() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "backlog")])
        .unwrap()
        .series_id;
    for segment_id in 1..=20 {
        write_numeric_segment(
            temp.path(),
            &registry,
            series_id,
            0,
            segment_id,
            &[(0, segment_id as f64), (10, segment_id as f64)],
        );
    }
    let compactor =
        Compactor::new(temp.path(), 8).with_compaction_pass_limits(CompactionPassLimits {
            max_directory_entries: 20,
            max_manifest_inspections: 20,
            max_source_segments: 8,
            max_source_chunks: 20,
            max_source_points: 100,
            max_decoded_bytes: 16 * 1024 * 1024,
        });

    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(stats.source_segments, 8);
    assert!(stats.source_chunks <= 8);
    assert!(stats.source_points <= 16);
    assert!(stats.planning_directory_entries_inspected <= 20);
    assert!(stats.planning_manifests_inspected <= 20);
    assert!(stats.planning_backlog_observed);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 12);
}

#[test]
fn compactor_byte_limit_is_exact_and_rejection_publishes_nothing() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "byte-bound")])
        .unwrap()
        .series_id;
    let first = write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(0, 0.0), (10, 10.0)],
    );
    let second = write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(5, 5.0), (15, 15.0)],
    );
    let modeled_bytes = modeled_compaction_source_bytes(&[first.clone(), second.clone()]);
    let limits = |max_decoded_bytes| CompactionPassLimits {
        max_directory_entries: 2,
        max_manifest_inspections: 2,
        max_source_segments: 2,
        max_source_chunks: 2,
        max_source_points: 4,
        max_decoded_bytes,
    };

    let rejected = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(limits(modeled_bytes - 1))
        .compact_once_with_stats()
        .unwrap();
    assert!(!rejected.compacted);
    assert!(rejected.planning_budget_exhausted);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 2);
    assert!(load_segments_for_level(temp.path(), 1).unwrap().is_empty());
    assert!(!temp.path().join(super::COMPACTION_REPLACEMENT_DIR).exists());

    let exact = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(limits(modeled_bytes))
        .compact_once_with_stats()
        .unwrap();
    assert!(exact.compacted);
    assert_eq!(exact.source_segments, 2);
    assert_eq!(exact.planning_source_bytes, modeled_bytes);
    assert!(load_segments_for_level(temp.path(), 0).unwrap().is_empty());
    assert_eq!(load_segments_for_level(temp.path(), 1).unwrap().len(), 1);
}

#[test]
fn compactor_source_item_limit_is_exact_at_n_and_n_plus_one() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "item-bound")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(0, 0.0), (10, 10.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(5, 5.0), (15, 15.0)],
    );
    let limits = |max_source_points| CompactionPassLimits {
        max_directory_entries: 2,
        max_manifest_inspections: 2,
        max_source_segments: 2,
        max_source_chunks: 2,
        max_source_points,
        max_decoded_bytes: u64::MAX,
    };

    let n_plus_one = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(limits(3))
        .compact_once_with_stats()
        .unwrap();
    assert!(!n_plus_one.compacted);
    assert!(n_plus_one.planning_budget_exhausted);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 2);
    assert!(load_segments_for_level(temp.path(), 1).unwrap().is_empty());

    let exact_n = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(limits(4))
        .compact_once_with_stats()
        .unwrap();
    assert!(exact_n.compacted);
    assert_eq!(exact_n.source_chunks, 2);
    assert_eq!(exact_n.source_points, 4);
}

#[test]
fn compactor_splits_large_output_into_multiple_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 300;
        let points = (start..start + 300)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();

        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert!(l1.len() >= 2);
    assert!(l1.iter().all(|segment| {
        segment.manifest.point_count <= 2 * super::DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER
    }));

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let total_points = chunks
        .iter()
        .map(|chunk| chunk.header.point_count as usize)
        .sum::<usize>();
    assert_eq!(total_points, 1200);
}

#[test]
fn multi_output_compaction_reserves_complete_peak_before_publishing() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "aggregate-preflight")])
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 300;
        let points = (start..start + 300)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();
        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let initial_budget = crate::LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
        .expect("initial source accounting should succeed");
    let source_bytes = initial_budget.snapshot().accounted_bytes;
    drop(initial_budget);

    let next_segment_id = Arc::new(AtomicU64::new(5));
    let tiny_budget = crate::LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp_dir.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&tiny_budget)),
    );

    let required_peak = match compactor
        .compact_once()
        .expect_err("the aggregate multi-output peak must exceed one byte")
    {
        TsinkError::InsufficientCompactionHeadroom {
            limit,
            used,
            reserved: 0,
            requested,
        } => {
            assert_eq!(limit, source_bytes + 1);
            assert_eq!(used, source_bytes);
            assert!(requested > 1);
            requested
        }
        err => panic!("unexpected aggregate preflight error: {err}"),
    };
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 5);
    assert_compaction_preflight_left_sources_untouched(temp_dir.path());
    assert_compaction_marker_directory_absent(temp_dir.path());
    assert_idle_disk_budget(&tiny_budget);
    drop(compactor);
    drop(tiny_budget);

    // A restart with exactly one byte less than the measured whole-operation peak must still
    // reject before creating the first output. This is the capacity that used to admit an early
    // output and fail only when a later SegmentWriter attempted its own reservation.
    let final_byte_budget = crate::LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak - 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp_dir.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&final_byte_budget)),
    );
    let err = compactor
        .compact_once()
        .expect_err("one byte below the aggregate peak must fail closed");
    assert!(matches!(
        err,
        TsinkError::InsufficientCompactionHeadroom {
            requested,
            reserved: 0,
            ..
        } if requested == required_peak
    ));
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 5);
    assert_compaction_preflight_left_sources_untouched(temp_dir.path());
    assert_compaction_marker_directory_absent(temp_dir.path());
    assert_idle_disk_budget(&final_byte_budget);
    drop(compactor);
    drop(final_byte_budget);

    let rollback_budget = crate::LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    finalize_pending_compaction_replacements_with_disk_budget_for_test(
        temp_dir.path(),
        &rollback_budget,
    );
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp_dir.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&rollback_budget)),
    );
    let second_output_chunks = temp_dir
        .path()
        .join("segments")
        .join("L1")
        .join(".tmp-seg-0000000000000006")
        .join("chunks.bin");
    let write_failure = crate::engine::fs_utils::fail_tmp_write_after_bytes_once(
        second_output_chunks,
        1,
        std::io::ErrorKind::StorageFull,
        "injected second compaction output failure",
    );
    let err = compactor
        .compact_once()
        .expect_err("a failed later output must roll back every earlier output");
    assert!(matches!(
        err,
        TsinkError::Io(ref source) if source.kind() == std::io::ErrorKind::StorageFull
    ));
    drop(write_failure);
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 7);
    assert_compaction_preflight_left_sources_untouched(temp_dir.path());
    assert_compaction_marker_directory_empty_or_absent(temp_dir.path());
    assert_idle_disk_budget(&rollback_budget);
    assert_eq!(rollback_budget.snapshot().accounted_bytes, source_bytes);
    drop(compactor);
    drop(rollback_budget);

    let exact_budget = crate::LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp_dir.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&exact_budget)),
    );
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert!(stats.output_segments >= 2);
    assert_idle_disk_budget(&exact_budget);

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    let output_bytes = l1.iter().try_fold(0u64, |total, segment| {
        total.checked_add(crate::disk_budget::measured_path_bytes(&segment.root).unwrap())
    });
    let output_bytes = output_bytes.expect("output byte sum should not overflow");
    assert!(
        required_peak > output_bytes,
        "operation admission must include the transient replacement marker"
    );
    let before_reconcile = exact_budget.snapshot().accounted_bytes;
    let after_reconcile = exact_budget.reconcile().unwrap().accounted_bytes;
    assert_eq!(before_reconcile, after_reconcile);
    assert_eq!(after_reconcile, output_bytes);
    drop(compactor);
    drop(exact_budget);

    let restarted_budget = crate::LocalDiskBudget::open(
        temp_dir.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    finalize_pending_compaction_replacements_with_disk_budget_for_test(
        temp_dir.path(),
        &restarted_budget,
    );
    assert!(load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .is_empty());
    assert!(load_segments_for_level(temp_dir.path(), 1).unwrap().len() >= 2);
    assert_idle_disk_budget(&restarted_budget);
}

#[test]
fn concurrent_reservation_makes_exact_compaction_peak_fail_at_n_plus_one() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "concurrent-peak")])
        .unwrap()
        .series_id;
    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 300;
        let points = (start..start + 300)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();
        write_numeric_segment(temp.path(), &registry, series_id, 0, segment_id, &points);
    }

    let probe = crate::LocalDiskBudget::open(temp.path(), LocalDiskLimits::default()).unwrap();
    let source_bytes = probe.snapshot().accounted_bytes;
    drop(probe);
    let next_segment_id = Arc::new(AtomicU64::new(5));
    let tiny = crate::LocalDiskBudget::open(
        temp.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&tiny)),
    );
    let required_peak = match compactor.compact_once().unwrap_err() {
        TsinkError::InsufficientCompactionHeadroom { requested, .. } => requested,
        err => panic!("unexpected compaction preflight error: {err}"),
    };
    drop(compactor);
    drop(tiny);

    let exact = crate::LocalDiskBudget::open(
        temp.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let holder_budget = Arc::clone(&exact);
    let holder = thread::spawn(move || {
        let reservation = holder_budget
            .reserve(
                crate::DiskCategory::Segments,
                1,
                crate::DiskReservationKind::Growth,
            )
            .unwrap();
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(reservation);
    });
    held_rx.recv().unwrap();

    let contended = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&exact)),
    );
    let err = contended
        .compact_once()
        .expect_err("one concurrent byte must reject an otherwise exact compaction peak");
    assert!(matches!(
        err,
        TsinkError::InsufficientCompactionHeadroom {
            limit,
            used,
            reserved: 1,
            requested,
        } if limit == source_bytes + required_peak
            && used == source_bytes
            && requested == required_peak
    ));
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 5);
    assert_compaction_preflight_left_sources_untouched(temp.path());
    assert_compaction_marker_directory_absent(temp.path());
    let contended_snapshot = exact.snapshot();
    assert_eq!(contended_snapshot.active_reservations, 1);
    assert_eq!(contended_snapshot.reserved_bytes, 1);
    assert_eq!(contended_snapshot.maintenance_reserved_bytes, 0);

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    assert_idle_disk_budget(&exact);

    let admitted = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&exact)),
    )
    .compact_once_with_stats()
    .unwrap();
    assert!(admitted.compacted);
    assert!(admitted.output_segments >= 2);
    assert_idle_disk_budget(&exact);
}

#[test]
fn compaction_peak_respects_exact_physical_headroom_before_mutation() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "physical-peak")])
        .unwrap()
        .series_id;
    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 300;
        let points = (start..start + 300)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();
        write_numeric_segment(temp.path(), &registry, series_id, 0, segment_id, &points);
    }

    let probe = crate::LocalDiskBudget::open(temp.path(), LocalDiskLimits::default()).unwrap();
    let source_bytes = probe.snapshot().accounted_bytes;
    drop(probe);
    let next_segment_id = Arc::new(AtomicU64::new(5));
    let logical_probe = crate::LocalDiskBudget::open(
        temp.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&logical_probe)),
    );
    let required_peak = match compactor.compact_once().unwrap_err() {
        TsinkError::InsufficientCompactionHeadroom { requested, .. } => requested,
        err => panic!("unexpected compaction preflight error: {err}"),
    };
    drop(compactor);
    drop(logical_probe);

    let headroom = 4096;
    // No logical maximum is configured below. This is the disk-admission behavior retained by
    // ExpertUnlimited: the operation remains one physical free-space/headroom reservation.
    let limits = LocalDiskLimits {
        filesystem_free_headroom_bytes: headroom,
        ..LocalDiskLimits::default()
    };
    let n_minus_one = crate::LocalDiskBudget::open_with_available_space_for_test(
        temp.path(),
        limits,
        required_peak + headroom - 1,
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&n_minus_one)),
    );
    assert!(matches!(
        compactor.compact_once(),
        Err(TsinkError::InsufficientDiskSpace {
            required,
            available,
        }) if required == required_peak && available == required_peak - 1
    ));
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 5);
    assert_compaction_preflight_left_sources_untouched(temp.path());
    assert_compaction_marker_directory_absent(temp.path());
    assert_idle_disk_budget(&n_minus_one);
    drop(compactor);
    drop(n_minus_one);

    let exact = crate::LocalDiskBudget::open_with_available_space_for_test(
        temp.path(),
        limits,
        required_peak + headroom,
    )
    .unwrap();
    let stats = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        next_segment_id,
        Some(Arc::clone(&exact)),
    )
    .compact_once_with_stats()
    .unwrap();
    assert!(stats.compacted);
    assert!(stats.output_segments >= 2);
    assert_idle_disk_budget(&exact);
}

#[test]
fn multi_output_retention_rewrite_reserves_complete_group_before_first_output() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "retention-preflight")])
        .unwrap()
        .series_id;
    let points = (0..1200).map(|ts| (ts, ts as f64)).collect::<Vec<_>>();
    write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &points);
    let source = load_segments_for_level(temp.path(), 0).unwrap().remove(0);
    let initial = crate::LocalDiskBudget::open(temp.path(), LocalDiskLimits::default()).unwrap();
    let source_bytes = initial.snapshot().accounted_bytes;
    drop(initial);
    let next_segment_id = Arc::new(AtomicU64::new(2));
    let tiny = crate::LocalDiskBudget::open(
        temp.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&tiny)),
    )
    .with_output_disk_category(crate::DiskCategory::Temporary);

    let required_peak = match compactor
        .stage_segment_rewrite_with_retention(&source, i64::MIN)
        .expect_err("one byte cannot admit the complete retention output group")
    {
        TsinkError::InsufficientCompactionHeadroom { requested, .. } => requested,
        err => panic!("unexpected retention preflight error: {err}"),
    };
    assert!(required_peak > 1);
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 2);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 1);
    assert_idle_disk_budget(&tiny);
    drop(compactor);
    drop(tiny);

    let final_byte = crate::LocalDiskBudget::open(
        temp.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak - 1),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&final_byte)),
    )
    .with_output_disk_category(crate::DiskCategory::Temporary);
    assert!(matches!(
        compactor.stage_segment_rewrite_with_retention(&source, i64::MIN),
        Err(TsinkError::InsufficientCompactionHeadroom { requested, .. })
            if requested == required_peak
    ));
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 2);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 1);
    drop(compactor);
    drop(final_byte);

    let exact = crate::LocalDiskBudget::open(
        temp.path(),
        LocalDiskLimits {
            max_bytes: Some(source_bytes + required_peak),
            ..LocalDiskLimits::default()
        },
    )
    .unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::clone(&next_segment_id),
        Some(Arc::clone(&exact)),
    )
    .with_output_disk_category(crate::DiskCategory::Temporary);

    let outcome = compactor
        .stage_segment_rewrite_with_retention(&source, i64::MIN)
        .unwrap();

    assert!(outcome.output_roots.len() >= 2);
    assert!(outcome.output_roots.iter().all(|root| root.exists()));
    assert!(source.root.exists());
    assert_idle_disk_budget(&exact);
}

fn finalize_pending_compaction_replacements_with_disk_budget_for_test(
    data_path: &std::path::Path,
    budget: &Arc<crate::LocalDiskBudget>,
) {
    let _ =
        super::finalize_pending_compaction_replacements_with_disk_budget(data_path, Some(budget))
            .unwrap();
}

fn assert_compaction_preflight_left_sources_untouched(data_path: &std::path::Path) {
    assert_eq!(load_segments_for_level(data_path, 0).unwrap().len(), 4);
    assert!(load_segments_for_level(data_path, 1).unwrap().is_empty());
    let l1_root = data_path.join("segments").join("L1");
    if l1_root.exists() {
        assert_eq!(fs::read_dir(l1_root).unwrap().count(), 0);
    }
}

fn assert_compaction_marker_directory_absent(data_path: &std::path::Path) {
    let marker_dir = data_path.join(super::COMPACTION_REPLACEMENT_DIR);
    assert!(
        !marker_dir.exists(),
        "capacity preflight must run before replacement-marker directory creation"
    );
}

fn assert_compaction_marker_directory_empty_or_absent(data_path: &std::path::Path) {
    let marker_dir = data_path.join(super::COMPACTION_REPLACEMENT_DIR);
    if marker_dir.exists() {
        assert_eq!(
            fs::read_dir(marker_dir).unwrap().count(),
            0,
            "failed output rollback must not leave replacement artifacts"
        );
    }
}

fn assert_idle_disk_budget(budget: &Arc<crate::LocalDiskBudget>) {
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.active_reservations, 0);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    assert_eq!(snapshot.reservation_overruns_total, 0);
}

#[test]
fn compactor_purges_tombstoned_points() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 15, end: 21 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    assert!(compactor.compact_once().unwrap());

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    let timestamps = decoded
        .into_iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![10, 30]);
}

#[test]
fn compaction_fails_when_any_source_segment_is_corrupted() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_writer = SegmentWriter::new(temp_dir.path(), 0, 1).unwrap();
    let mut first_chunks = HashMap::new();
    first_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    first_writer
        .write_segment(&registry, &first_chunks)
        .unwrap();

    let second_writer = SegmentWriter::new(temp_dir.path(), 0, 2).unwrap();
    let mut second_chunks = HashMap::new();
    second_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    second_writer
        .write_segment(&registry, &second_chunks)
        .unwrap();

    let corrupt_root = first_writer.layout().root.clone();
    let mut manifest_bytes = fs::read(first_writer.layout().manifest_path.clone()).unwrap();
    manifest_bytes[0] ^= 0xff;
    fs::write(first_writer.layout().manifest_path.clone(), manifest_bytes).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    let err = compactor.compact_once().unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("failed compaction validation"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("manifest crc32 mismatch"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&corrupt_root.display().to_string()),
        "unexpected error: {message}"
    );
    assert!(
        corrupt_root.exists(),
        "corrupt source root should stay in place"
    );
    assert!(
        second_writer.layout().root.exists(),
        "healthy source root should not be compacted away"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 1)
            .unwrap()
            .is_empty(),
        "compaction must not publish replacement output when any source root is corrupt"
    );
}

#[test]
fn finalize_pending_replacements_removes_marked_source_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut l0_chunks = HashMap::new();
    l0_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 1.0)])]);
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &l0_chunks)
        .unwrap();

    let mut l1_chunks = HashMap::new();
    l1_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 2.0)])]);
    SegmentWriter::new(temp_dir.path(), 1, 2)
        .unwrap()
        .write_segment(&registry, &l1_chunks)
        .unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l1.len(), 1);

    let source_root = l0[0].root.clone();
    let output_root = l1[0].root.clone();
    let marker_path = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        std::slice::from_ref(&source_root),
        std::slice::from_ref(&output_root),
    )
    .unwrap();
    assert!(marker_path.exists());

    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();

    assert!(!source_root.exists(), "source segment should be removed");
    assert!(
        output_root.exists(),
        "replacement output should be preserved"
    );
    assert!(
        !marker_path.exists(),
        "replacement marker should be removed after apply"
    );
}

#[test]
fn ready_marker_parent_sync_failure_is_resolved_before_source_retirement() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "ready-sync")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(2, 3.0), (3, 4.0)],
    );

    let marker_dir = temp.path().join(super::COMPACTION_REPLACEMENT_DIR);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_hook = Arc::clone(&calls);
    let _sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |candidate| {
            candidate == marker_dir.as_path()
                && calls_for_hook.fetch_add(1, Ordering::SeqCst) + 1 == 2
        },
        "injected Ready marker parent-sync failure",
    );

    let stats = Compactor::new(temp.path(), 8)
        .compact_once_with_stats()
        .unwrap();

    assert!(stats.compacted);
    assert!(load_segments_for_level(temp.path(), 0).unwrap().is_empty());
    assert_eq!(load_segments_for_level(temp.path(), 1).unwrap().len(), 1);
    assert!(calls.load(Ordering::SeqCst) >= 3);
}

#[test]
fn interrupted_multi_output_compaction_recovers_preparing_and_releases_reservation() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "interrupt")])
        .unwrap()
        .series_id;
    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 300;
        let points = (start..start + 300)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();
        write_numeric_segment(temp.path(), &registry, series_id, 0, segment_id, &points);
    }
    let budget = crate::LocalDiskBudget::open(temp.path(), LocalDiskLimits::default()).unwrap();
    let source_bytes = budget.snapshot().accounted_bytes;
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        2,
        Arc::new(AtomicU64::new(5)),
        Some(Arc::clone(&budget)),
    );
    let interruption = execution::interrupt_after_compaction_output(1);

    let unwind = catch_unwind(AssertUnwindSafe(|| compactor.compact_once()));
    drop(interruption);

    assert!(unwind.is_err());
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 4);
    assert_eq!(load_segments_for_level(temp.path(), 1).unwrap().len(), 1);
    let interrupted_snapshot = budget.snapshot();
    assert_eq!(interrupted_snapshot.active_reservations, 0);
    assert_eq!(interrupted_snapshot.reserved_bytes, 0);
    assert_eq!(interrupted_snapshot.maintenance_reserved_bytes, 0);
    assert!(interrupted_snapshot.accounted_bytes > source_bytes);

    let recovered = super::finalize_pending_compaction_replacements_with_disk_budget(
        temp.path(),
        Some(&budget),
    )
    .unwrap();
    assert!(!recovered.stats.compacted);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 4);
    assert!(load_segments_for_level(temp.path(), 1).unwrap().is_empty());
    assert_eq!(budget.reconcile().unwrap().accounted_bytes, source_bytes);
    assert_idle_disk_budget(&budget);
}

#[test]
fn higher_level_cleanup_removes_publish_survivor_after_writer_rollback_failure() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "rollback")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(2, 3.0), (3, 4.0)],
    );
    let planned_root = temp.path().join("segments/L1/seg-0000000000000003");
    let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
        planned_root.clone(),
        "injected published segment sync failure",
    );
    let _rollback_failure =
        crate::engine::segment::fail_segment_publish_rollback_once(planned_root.clone());
    let compactor =
        Compactor::new_with_segment_id_allocator(temp.path(), 8, Arc::new(AtomicU64::new(3)));

    compactor
        .compact_once()
        .expect_err("the injected publish failure must escape after planned cleanup");

    assert!(!planned_root.exists());
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 2);
    assert_compaction_marker_directory_empty_or_absent(temp.path());
}

#[test]
fn planned_output_collision_preserves_unknown_entry_and_sources() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "collision")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(2, 3.0), (3, 4.0)],
    );
    let collision = temp.path().join("segments/L1/seg-0000000000000003");
    fs::create_dir_all(&collision).unwrap();
    let victim = collision.join("operator-note.txt");
    fs::write(&victim, b"keep me").unwrap();
    let compactor =
        Compactor::new_with_segment_id_allocator(temp.path(), 8, Arc::new(AtomicU64::new(3)));

    let err = compactor.compact_once().unwrap_err();

    assert!(err
        .to_string()
        .contains("planned compaction output already exists"));
    assert_eq!(fs::read(victim).unwrap(), b"keep me");
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 2);
    assert_compaction_marker_directory_absent(temp.path());
}

#[test]
fn fully_tombstoned_compaction_commits_deletion_only_replacement() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "deleted")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(2, 3.0), (3, 4.0)],
    );
    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 0, end: 10 }]);
    persist_tombstones(&temp.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();
    let budget = crate::LocalDiskBudget::open(temp.path(), LocalDiskLimits::default()).unwrap();
    let compactor = Compactor::new_with_segment_id_allocator_and_disk_budget(
        temp.path(),
        8,
        Arc::new(AtomicU64::new(3)),
        Some(Arc::clone(&budget)),
    );

    let stats = compactor.compact_once_with_stats().unwrap();

    assert!(stats.compacted);
    assert_eq!(stats.output_segments, 0);
    assert!(load_segments_for_level(temp.path(), 0).unwrap().is_empty());
    assert!(load_segments_for_level(temp.path(), 1).unwrap().is_empty());
    assert_idle_disk_budget(&budget);
    let before = budget.snapshot().accounted_bytes;
    assert_eq!(budget.reconcile().unwrap().accounted_bytes, before);
}

#[test]
fn recovered_ready_returns_catalog_diff_even_when_final_marker_sync_fails() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "catalog-diff")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();
    let marker_dir = marker.parent().unwrap().to_path_buf();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_hook = Arc::clone(&calls);
    let _sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |candidate| {
            candidate == marker_dir.as_path()
                && calls_for_hook.fetch_add(1, Ordering::SeqCst) + 1 == 4
        },
        "injected final marker unlink sync failure",
    );

    let outcome = Compactor::new(temp.path(), 8)
        .compact_once_with_changes()
        .unwrap();

    assert!(outcome.stats.compacted);
    assert_eq!(outcome.source_roots, vec![source.clone()]);
    assert_eq!(outcome.output_roots, vec![output.clone()]);
    assert!(!source.exists());
    assert!(output.exists());
    assert!(!marker.exists());
}

#[test]
fn ready_recovery_resumes_after_first_source_was_atomically_retired() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "retire-crash")])
        .unwrap()
        .series_id;
    let first = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let second = write_numeric_segment(temp.path(), &registry, series_id, 0, 2, &[(2, 2.0)]);
    let output = write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        1,
        3,
        &[(1, 1.0), (2, 2.0)],
    );
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        &[first.clone(), second.clone()],
        std::slice::from_ref(&output),
    )
    .unwrap();
    let first_retired = retired_source_path(&marker, 0);
    fs::rename(&first, &first_retired).unwrap();

    finalize_pending_compaction_replacements(temp.path()).unwrap();

    assert!(!first.exists());
    assert!(!second.exists());
    assert!(!first_retired.exists());
    assert!(output.exists());
    assert!(!marker.exists());
}

#[test]
fn ready_recovery_finishes_partially_deleted_owned_trash_after_all_sources_retired() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "trash-crash")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();
    let retired = retired_source_path(&marker, 0);
    fs::rename(&source, &retired).unwrap();
    fs::remove_file(retired.join("chunks.bin")).unwrap();

    finalize_pending_compaction_replacements(temp.path()).unwrap();

    assert!(!source.exists());
    assert!(!retired.exists());
    assert!(output.exists());
    assert!(!marker.exists());
}

#[test]
fn cleanup_only_ready_recovery_requires_no_phantom_retirement_headroom() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "zero-headroom")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();
    let retired = retired_source_path(&marker, 0);
    fs::rename(&source, &retired).unwrap();
    fs::remove_file(retired.join("chunks.bin")).unwrap();
    let budget = crate::LocalDiskBudget::open_with_available_space_for_test(
        temp.path(),
        LocalDiskLimits::default(),
        0,
    )
    .unwrap();

    let outcome = super::finalize_pending_compaction_replacements_with_disk_budget(
        temp.path(),
        Some(&budget),
    )
    .unwrap();

    assert!(outcome.stats.compacted);
    assert_eq!(outcome.source_roots, vec![source]);
    assert_eq!(outcome.output_roots, vec![output.clone()]);
    assert!(!retired.exists());
    assert!(output.exists());
    assert!(!marker.exists());
    assert_idle_disk_budget(&budget);
}

#[test]
fn legacy_v1_ready_recovery_accepts_missing_and_partial_canonical_sources() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "legacy")])
        .unwrap()
        .series_id;
    let missing = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let partial = write_numeric_segment(temp.path(), &registry, series_id, 0, 2, &[(2, 2.0)]);
    let output = write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        1,
        3,
        &[(1, 1.0), (2, 2.0)],
    );
    fs::remove_dir_all(&missing).unwrap();
    fs::remove_file(partial.join("chunks.bin")).unwrap();
    let marker = write_raw_replacement_marker(
        temp.path(),
        "replace-0000000000000001-0000000000000001.json",
        serde_json::json!({
            "version": 1,
            "source_segments": [segment_relative(temp.path(), &missing), segment_relative(temp.path(), &partial)],
            "output_segments": [segment_relative(temp.path(), &output)],
        }),
    );

    finalize_pending_compaction_replacements(temp.path()).unwrap();

    assert!(!missing.exists());
    assert!(!partial.exists());
    assert!(output.exists());
    assert!(!marker.exists());
}

#[test]
fn legacy_v1_partial_recovery_preserves_unknown_files_and_subdirectories() {
    for unknown_is_directory in [false, true] {
        let temp = TempDir::new().unwrap();
        let registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("host", "legacy-unknown")])
            .unwrap()
            .series_id;
        let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
        let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
        fs::remove_file(source.join("chunks.bin")).unwrap();
        let unknown = source.join("operator-owned");
        if unknown_is_directory {
            fs::create_dir(&unknown).unwrap();
            fs::write(unknown.join("note"), b"preserve-dir").unwrap();
        } else {
            fs::write(&unknown, b"preserve-file").unwrap();
        }
        write_raw_replacement_marker(
            temp.path(),
            "replace-0000000000000001-0000000000000001.json",
            serde_json::json!({
                "version": 1,
                "source_segments": [segment_relative(temp.path(), &source)],
                "output_segments": [segment_relative(temp.path(), &output)],
            }),
        );

        finalize_pending_compaction_replacements(temp.path()).unwrap_err();

        if unknown_is_directory {
            assert_eq!(fs::read(unknown.join("note")).unwrap(), b"preserve-dir");
        } else {
            assert_eq!(fs::read(unknown).unwrap(), b"preserve-file");
        }
        assert!(source.exists());
        assert!(output.exists());
    }
}

#[cfg(unix)]
#[test]
fn legacy_v1_partial_recovery_preserves_link_entries() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "legacy-link")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let external = temp.path().join("external");
    fs::write(&external, b"preserve").unwrap();
    fs::remove_file(source.join("chunks.bin")).unwrap();
    symlink(&external, source.join("chunks.bin")).unwrap();
    write_raw_replacement_marker(
        temp.path(),
        "replace-0000000000000001-0000000000000001.json",
        serde_json::json!({
            "version": 1,
            "source_segments": [segment_relative(temp.path(), &source)],
            "output_segments": [segment_relative(temp.path(), &output)],
        }),
    );

    finalize_pending_compaction_replacements(temp.path()).unwrap_err();

    assert_eq!(fs::read(external).unwrap(), b"preserve");
    assert!(fs::symlink_metadata(source.join("chunks.bin"))
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(source.exists());
}

#[test]
fn ready_recovery_rejects_incomplete_output_and_source_extra_entries() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "ownership")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = temp.path().join("segments/L1/seg-0000000000000002");
    fs::create_dir_all(&output).unwrap();
    fs::write(output.join("manifest.bin"), b"incomplete").unwrap();
    let marker = write_raw_replacement_marker(
        temp.path(),
        "replace-0000000000000002-0000000000000002.json",
        serde_json::json!({
            "version": 2,
            "phase": "ready",
            "source_segments": [segment_relative(temp.path(), &source)],
            "output_segments": [segment_relative(temp.path(), &output)],
        }),
    );

    finalize_pending_compaction_replacements(temp.path()).unwrap_err();
    assert!(source.exists());
    assert!(output.exists());
    assert!(marker.exists());

    fs::remove_file(&marker).unwrap();
    fs::remove_dir_all(&output).unwrap();
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let extra = source.join("operator-note.txt");
    fs::write(&extra, b"preserve").unwrap();
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();

    finalize_pending_compaction_replacements(temp.path()).unwrap_err();
    assert_eq!(fs::read(extra).unwrap(), b"preserve");
    assert!(source.exists());
    assert!(output.exists());
    assert!(marker.exists());
}

#[test]
fn replacement_marker_rejects_noncanonical_duplicate_and_overlapping_paths() {
    for case in [
        "dot",
        "outside",
        "unsupported",
        "duplicate-source",
        "duplicate-output",
        "overlap",
    ] {
        let temp = TempDir::new().unwrap();
        let registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("cpu", &[Label::new("case", case)])
            .unwrap()
            .series_id;
        let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
        let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
        let source_rel = segment_relative(temp.path(), &source);
        let output_rel = segment_relative(temp.path(), &output);
        let (sources, outputs) = match case {
            "dot" => (vec![".".to_string()], vec![output_rel.clone()]),
            "outside" => (vec!["wal".to_string()], vec![output_rel.clone()]),
            "unsupported" => (
                vec!["segments/L255/seg-0000000000000001".to_string()],
                vec![output_rel.clone()],
            ),
            "duplicate-source" => (
                vec![source_rel.clone(), source_rel.clone()],
                vec![output_rel.clone()],
            ),
            "duplicate-output" => (
                vec![source_rel.clone()],
                vec![output_rel.clone(), output_rel.clone()],
            ),
            "overlap" => (vec![source_rel.clone()], vec![source_rel.clone()]),
            _ => unreachable!(),
        };
        write_raw_replacement_marker(
            temp.path(),
            "replace-0000000000000001-0000000000000001.json",
            serde_json::json!({
                "version": 2,
                "phase": "ready",
                "source_segments": sources,
                "output_segments": outputs,
            }),
        );

        finalize_pending_compaction_replacements(temp.path()).unwrap_err();

        assert!(source.exists(), "source was mutated for case {case}");
        assert!(output.exists(), "output was mutated for case {case}");
    }
}

#[test]
fn ready_recovery_rejects_manifest_identity_mismatch_before_source_deletion() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "identity")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let original_output =
        write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let mismatched_output = original_output.with_file_name("seg-0000000000000003");
    fs::rename(&original_output, &mismatched_output).unwrap();
    execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&mismatched_output),
    )
    .unwrap();

    finalize_pending_compaction_replacements(temp.path()).unwrap_err();

    assert!(source.exists());
    assert!(mismatched_output.exists());
}

#[cfg(unix)]
#[test]
fn ready_recovery_rejects_symlinked_required_file_without_touching_external_target() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "symlink")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let external = temp.path().join("external.bin");
    fs::write(&external, b"external-owned").unwrap();
    fs::remove_file(output.join("chunks.bin")).unwrap();
    symlink(&external, output.join("chunks.bin")).unwrap();
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();

    finalize_pending_compaction_replacements(temp.path()).unwrap_err();

    assert_eq!(fs::read(external).unwrap(), b"external-owned");
    assert!(source.exists());
    assert!(output.exists());
    assert!(marker.exists());
}

#[test]
fn marker_name_collision_selects_unused_path_without_overwriting_stale_marker() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "marker-collision")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let marker_dir = temp.path().join(super::COMPACTION_REPLACEMENT_DIR);
    fs::create_dir_all(&marker_dir).unwrap();
    let stale = marker_dir.join("replace-1111111111111111-2222222222222222.json");
    fs::write(&stale, b"stale-ready-intent").unwrap();
    let _forced = execution::force_next_replacement_marker_candidate(stale.clone());

    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();

    assert_ne!(marker, stale);
    assert_eq!(fs::read(stale).unwrap(), b"stale-ready-intent");
    assert!(marker.exists());
}

#[test]
fn background_recovery_cursor_continues_across_compactor_clones_and_stops_the_wake() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "recovery-cursor")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(0, 1.0), (10, 1.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(5, 2.0), (15, 2.0)],
    );
    let marker_dir = temp.path().join(super::COMPACTION_REPLACEMENT_DIR);
    fs::create_dir_all(&marker_dir).unwrap();
    fs::write(marker_dir.join("operator-note"), b"preserve").unwrap();
    let compactor = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(background_recovery_limits(2, u64::MAX));
    let clone = compactor.clone();

    let first = compactor.compact_background_once_with_changes().unwrap();
    assert!(!first.stats.compacted);
    assert_eq!(first.stats.planning_directory_entries_inspected, 0);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 2);

    let second = clone.compact_background_once_with_changes().unwrap();
    assert!(second.stats.compacted);
    assert!(load_segments_for_level(temp.path(), 0).unwrap().is_empty());
    assert_eq!(load_segments_for_level(temp.path(), 1).unwrap().len(), 1);
    assert_eq!(
        fs::read(marker_dir.join("operator-note")).unwrap(),
        b"preserve"
    );
}

#[test]
fn background_recovery_restart_rescans_without_false_completion() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "recovery-restart")])
        .unwrap()
        .series_id;
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        1,
        &[(0, 1.0), (10, 1.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(5, 2.0), (15, 2.0)],
    );
    let marker_dir = temp.path().join(super::COMPACTION_REPLACEMENT_DIR);
    fs::create_dir_all(&marker_dir).unwrap();
    fs::write(marker_dir.join("operator-note"), b"preserve").unwrap();
    let limits = background_recovery_limits(2, u64::MAX);

    let before_restart = Compactor::new(temp.path(), 8).with_compaction_pass_limits(limits);
    assert!(
        !before_restart
            .compact_background_once_with_changes()
            .unwrap()
            .stats
            .compacted
    );

    let restarted = Compactor::new(temp.path(), 8).with_compaction_pass_limits(limits);
    let rescanned = restarted.compact_background_once_with_changes().unwrap();
    assert!(!rescanned.stats.compacted);
    assert_eq!(rescanned.stats.planning_directory_entries_inspected, 0);
    assert_eq!(load_segments_for_level(temp.path(), 0).unwrap().len(), 2);

    assert!(
        restarted
            .compact_background_once_with_changes()
            .unwrap()
            .stats
            .compacted
    );
}

#[test]
fn background_ready_recovery_accepts_exact_record_limit_and_owns_the_wake() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "recovery-exact")])
        .unwrap()
        .series_id;
    let recovered_source =
        write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(100, 1.0)]);
    let recovered_output =
        write_numeric_segment(temp.path(), &registry, series_id, 1, 10, &[(100, 10.0)]);
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        2,
        &[(0, 2.0), (10, 2.0)],
    );
    write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        0,
        3,
        &[(5, 3.0), (15, 3.0)],
    );
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&recovered_source),
        std::slice::from_ref(&recovered_output),
    )
    .unwrap();
    let compactor = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(background_recovery_limits(2, u64::MAX));

    let recovered = compactor.compact_background_once_with_changes().unwrap();
    assert!(recovered.stats.compacted);
    assert_eq!(recovered.source_roots, vec![recovered_source]);
    assert_eq!(recovered.output_roots, vec![recovered_output]);
    assert!(!marker.exists());
    assert_eq!(
        load_segments_for_level(temp.path(), 0).unwrap().len(),
        2,
        "the exact two-record recovery must not also plan a compaction"
    );

    assert!(
        compactor
            .compact_background_once_with_changes()
            .unwrap()
            .stats
            .compacted,
        "the following wake may plan after the retained scan reaches its end"
    );
}

#[test]
fn background_ready_recovery_rejects_n_plus_one_records_before_mutation() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "recovery-n-plus-one")])
        .unwrap()
        .series_id;
    let first_source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let second_source = write_numeric_segment(temp.path(), &registry, series_id, 0, 2, &[(2, 2.0)]);
    let output = write_numeric_segment(
        temp.path(),
        &registry,
        series_id,
        1,
        3,
        &[(1, 1.0), (2, 2.0)],
    );
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        &[first_source.clone(), second_source.clone()],
        std::slice::from_ref(&output),
    )
    .unwrap();
    let compactor = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(background_recovery_limits(2, u64::MAX));

    let error = compactor
        .compact_background_once_with_changes()
        .expect_err("three records must not fit a two-item recovery pass");
    assert!(matches!(
        error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            item_limit: 2,
            selected_items: 3,
            ..
        }
    ));
    assert!(first_source.exists());
    assert!(second_source.exists());
    assert!(output.exists());
    assert!(marker.exists());
}

#[test]
fn background_recovery_decoded_byte_envelope_is_exact_and_failure_resets_cursor() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "recovery-bytes")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();
    let marker_bytes = fs::symlink_metadata(&marker).unwrap().len();
    let decoded_bytes =
        execution::bounded_compaction_marker_decode_bytes(temp.path(), &marker, marker_bytes);
    assert!(decoded_bytes > marker_bytes);
    let compactor = Compactor::new(temp.path(), 8).with_compaction_pass_limits(
        background_recovery_limits(2, decoded_bytes.saturating_sub(1)),
    );

    let error = compactor
        .compact_background_once_with_changes()
        .expect_err("one byte below the modeled decode heap must reject before decode");
    assert!(matches!(
        error,
        TsinkError::MaintenanceWorkItemTooLarge {
            limit,
            required,
            ..
        } if limit == decoded_bytes - 1 && required == decoded_bytes
    ));
    assert!(source.exists());
    assert!(output.exists());
    assert!(marker.exists());

    let compactor =
        compactor.with_compaction_pass_limits(background_recovery_limits(2, decoded_bytes));
    let recovered = compactor
        .compact_background_once_with_changes()
        .expect("the failed cursor must restart and admit the exact decode envelope");
    assert!(recovered.stats.compacted);
    assert!(!source.exists());
    assert!(output.exists());
    assert!(!marker.exists());
}

#[cfg(unix)]
#[test]
fn background_recovery_rejects_same_size_marker_path_replacement() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "marker-identity")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let marker = execution::write_compaction_replacement_marker(
        temp.path(),
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();
    let marker_bytes = fs::symlink_metadata(&marker).unwrap().len();
    let replacement = temp.path().join("same-size-marker-replacement");
    fs::write(&replacement, vec![b'x'; marker_bytes as usize]).unwrap();
    let _hook = execution::set_compaction_marker_post_open_hook({
        let replacement = replacement.clone();
        move |path| fs::rename(&replacement, path).unwrap()
    });

    let error = Compactor::new(temp.path(), 8)
        .with_compaction_pass_limits(background_recovery_limits(2, u64::MAX))
        .compact_background_once_with_changes()
        .expect_err("a same-size replacement must not be applied through the old open handle");
    assert!(matches!(error, TsinkError::DataCorruption(_)));
    assert!(source.exists());
    assert!(output.exists());
    assert!(marker.exists());
    assert!(!replacement.exists());
}

#[test]
fn compaction_replacement_marker_has_fixed_byte_and_record_decode_caps() {
    let oversized = TempDir::new().unwrap();
    let marker_dir = oversized.path().join(super::COMPACTION_REPLACEMENT_DIR);
    fs::create_dir_all(&marker_dir).unwrap();
    let marker = marker_dir.join("replace-0000000000000001-0000000000000001.json");
    fs::write(
        &marker,
        vec![b'x'; super::MAX_COMPACTION_REPLACEMENT_MARKER_BYTES as usize + 1],
    )
    .unwrap();
    let error = Compactor::new(oversized.path(), 8)
        .with_compaction_pass_limits(background_recovery_limits(usize::MAX, u64::MAX))
        .compact_background_once_with_changes()
        .expect_err("an oversized marker must be rejected before allocation or decode");
    assert!(matches!(error, TsinkError::DataCorruption(_)));
    assert!(marker.exists());

    let too_many = TempDir::new().unwrap();
    let repeated = "segments/L0/seg-0000000000000001";
    let marker = write_raw_replacement_marker(
        too_many.path(),
        "replace-0000000000000001-0000000000000001.json",
        serde_json::json!({
            "version": 2,
            "phase": "preparing",
            "source_segments": vec![repeated; super::MAX_COMPACTION_REPLACEMENT_RECORDS + 1],
            "output_segments": [],
        }),
    );
    Compactor::new(too_many.path(), 8)
        .with_compaction_pass_limits(background_recovery_limits(usize::MAX, u64::MAX))
        .compact_background_once_with_changes()
        .expect_err("a record list beyond the fixed cap must fail during bounded decode");
    assert!(marker.exists());
}

#[test]
fn runtime_finalizer_returns_first_ready_diff_before_later_corrupt_marker() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "ordered-markers")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let first_marker = write_raw_replacement_marker(
        temp.path(),
        "replace-0000000000000001-0000000000000001.json",
        serde_json::json!({
            "version": 2,
            "phase": "ready",
            "source_segments": [segment_relative(temp.path(), &source)],
            "output_segments": [segment_relative(temp.path(), &output)],
        }),
    );
    let corrupt_marker = write_raw_replacement_marker(
        temp.path(),
        "replace-ffffffffffffffff-ffffffffffffffff.json",
        serde_json::json!({
            "version": 2,
            "phase": "ready",
            "source_segments": ["wal"],
            "output_segments": [],
        }),
    );
    let compactor = Compactor::new(temp.path(), 8);

    let recovered = compactor.compact_once_with_changes().unwrap();

    assert_eq!(recovered.source_roots, vec![source]);
    assert_eq!(recovered.output_roots, vec![output]);
    assert!(!first_marker.exists());
    assert!(corrupt_marker.exists());
    compactor
        .compact_once_with_changes()
        .expect_err("the later corrupt marker must fail on the next call");
}

#[test]
fn startup_style_finalizer_drains_multiple_ready_markers() {
    let temp = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "multi-ready")])
        .unwrap()
        .series_id;
    let source_a = write_numeric_segment(temp.path(), &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output_a = write_numeric_segment(temp.path(), &registry, series_id, 1, 2, &[(1, 2.0)]);
    let source_b = write_numeric_segment(temp.path(), &registry, series_id, 0, 3, &[(3, 3.0)]);
    let output_b = write_numeric_segment(temp.path(), &registry, series_id, 2, 4, &[(3, 4.0)]);
    write_raw_replacement_marker(
        temp.path(),
        "replace-0000000000000001-0000000000000001.json",
        serde_json::json!({
            "version": 2,
            "phase": "ready",
            "source_segments": [segment_relative(temp.path(), &source_a)],
            "output_segments": [segment_relative(temp.path(), &output_a)],
        }),
    );
    write_raw_replacement_marker(
        temp.path(),
        "replace-0000000000000002-0000000000000002.json",
        serde_json::json!({
            "version": 2,
            "phase": "ready",
            "source_segments": [segment_relative(temp.path(), &source_b)],
            "output_segments": [segment_relative(temp.path(), &output_b)],
        }),
    );

    finalize_pending_compaction_replacements(temp.path()).unwrap();

    assert!(!source_a.exists());
    assert!(!source_b.exists());
    assert!(output_a.exists());
    assert!(output_b.exists());
}

#[cfg(unix)]
#[test]
fn recovered_diff_preserves_configured_symlink_data_path_prefix() {
    use std::os::unix::fs::symlink;

    let real = TempDir::new().unwrap();
    let configured_parent = TempDir::new().unwrap();
    let configured = configured_parent.path().join("configured-data");
    symlink(real.path(), &configured).unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "lexical")])
        .unwrap()
        .series_id;
    let source = write_numeric_segment(&configured, &registry, series_id, 0, 1, &[(1, 1.0)]);
    let output = write_numeric_segment(&configured, &registry, series_id, 1, 2, &[(1, 2.0)]);
    execution::write_compaction_replacement_marker(
        &configured,
        std::slice::from_ref(&source),
        std::slice::from_ref(&output),
    )
    .unwrap();

    let outcome = Compactor::new(&configured, 8)
        .compact_once_with_changes()
        .unwrap();

    assert_eq!(outcome.source_roots, vec![source]);
    assert_eq!(outcome.output_roots, vec![output]);
    assert!(outcome
        .source_roots
        .iter()
        .all(|root| root.starts_with(&configured)));
}

fn write_numeric_segment(
    data_path: &Path,
    registry: &SeriesRegistry,
    series_id: u64,
    level: u8,
    segment_id: u64,
    points: &[(i64, f64)],
) -> PathBuf {
    let mut chunks = HashMap::new();
    chunks.insert(series_id, vec![make_numeric_chunk(series_id, points)]);
    let writer = SegmentWriter::new(data_path, level, segment_id).unwrap();
    writer.write_segment(registry, &chunks).unwrap();
    writer.layout().root.clone()
}

fn modeled_compaction_source_bytes(roots: &[PathBuf]) -> u64 {
    let mut total = 0u64;
    let mut points = 0usize;
    for root in roots {
        let fingerprint = crate::engine::segment::read_segment_manifest_fingerprint(root).unwrap();
        total = fingerprint
            .files
            .iter()
            .fold(total, |sum, file| sum + file.file_len);
        points += fingerprint.manifest.point_count;
        let chunks = fs::read(root.join("chunks.bin")).unwrap();
        total += crate::engine::segment::decoded_chunks_file_payload_bytes(&chunks).unwrap() as u64;
    }
    total + (points * std::mem::size_of::<ChunkPoint>().max(1)) as u64
}

fn segment_relative(data_path: &Path, segment_root: &Path) -> String {
    segment_root
        .strip_prefix(data_path)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn write_raw_replacement_marker(data_path: &Path, name: &str, value: serde_json::Value) -> PathBuf {
    let marker_dir = data_path.join(super::COMPACTION_REPLACEMENT_DIR);
    fs::create_dir_all(&marker_dir).unwrap();
    let path = marker_dir.join(name);
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    path
}

fn retired_source_path(marker_path: &Path, index: usize) -> PathBuf {
    marker_path.parent().unwrap().join(format!(
        ".retired-{}-{index:016x}",
        marker_path.file_name().unwrap().to_string_lossy()
    ))
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
        wal_lowwater: WalHighWatermark::default(),
        wal_highwater: WalHighWatermark::default(),
    }
}

fn make_histogram_chunk(series_id: u64, timestamps: &[i64]) -> Chunk {
    let histogram = sample_histogram();
    let points = timestamps
        .iter()
        .map(|ts| ChunkPoint {
            ts: *ts,
            value: Value::from(histogram.clone()),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Blob).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Blob,
            value_family: Some(SeriesValueFamily::Histogram),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_lowwater: WalHighWatermark::default(),
        wal_highwater: WalHighWatermark::default(),
    }
}
