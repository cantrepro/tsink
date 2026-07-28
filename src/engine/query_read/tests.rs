use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use super::analysis::{analyze_series_read_sources, SeriesReadAnalysis};
use super::merge::{pop_next_point_from_sources, QueryMergeCursor, QueryMergeSourceRef};
use super::pagination::{RawSeriesPagination, SortedSeriesDedupeMode, SortedSeriesPageCollector};
use super::*;
use crate::engine::chunk::ChunkHeader;
use crate::engine::tombstone::TombstoneMap;
use crate::{
    QueryBudgetError, QueryBudgetLimits, QueryCancellationToken, QueryLimitReason, QueryWorkLimits,
};

fn default_future_skew_window(precision: TimestampPrecision) -> i64 {
    super::super::duration_to_timestamp_units(
        super::super::DEFAULT_FUTURE_SKEW_ALLOWANCE,
        precision,
    )
}

fn test_options(current_time_override: Option<i64>) -> ChunkStorageOptions {
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
        write_timeout: Duration::from_secs(2),
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
        current_time_override,
    }
}

fn live_storage(chunk_point_cap: usize) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        None,
        None,
        1,
        test_options(None),
    )
    .unwrap()
}

fn persistent_numeric_storage(path: &std::path::Path, chunk_point_cap: usize) -> ChunkStorage {
    persistent_numeric_storage_with_query_budget(
        path,
        chunk_point_cap,
        QueryBudgetLimits::default(),
    )
}

fn persistent_numeric_storage_with_query_budget(
    path: &std::path::Path,
    chunk_point_cap: usize,
    limits: QueryBudgetLimits,
) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options_and_disk_budget_and_query_budget(
        chunk_point_cap,
        None,
        Some(path.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        test_options(None),
        None,
        limits,
    )
    .unwrap()
}

fn persistent_blob_storage_with_query_budget(
    path: &std::path::Path,
    chunk_point_cap: usize,
    limits: QueryBudgetLimits,
) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options_and_disk_budget_and_query_budget(
        chunk_point_cap,
        None,
        None,
        Some(path.join(BLOB_LANE_ROOT)),
        1,
        test_options(None),
        None,
        limits,
    )
    .unwrap()
}

fn query_memory_limits(memory_limit: u64) -> QueryBudgetLimits {
    QueryBudgetLimits {
        max_shared_memory_bytes: Some(memory_limit),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(memory_limit),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    }
}

fn unsorted_chunk(series_id: SeriesId, points: &[(i64, f64)]) -> Arc<Chunk> {
    let chunk_points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();
    let min_ts = points.iter().map(|(ts, _)| *ts).min().unwrap_or(i64::MIN);
    let max_ts = points.iter().map(|(ts, _)| *ts).max().unwrap_or(i64::MAX);
    Arc::new(Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: u16::try_from(points.len()).unwrap_or(u16::MAX),
            min_ts,
            max_ts,
            ts_codec: crate::engine::chunk::TimestampCodecId::DeltaOfDeltaBitpack,
            value_codec: crate::engine::chunk::ValueCodecId::GorillaXorF64,
        },
        points: chunk_points,
        encoded_payload: Vec::new(),
        wal_lowwater: WalHighWatermark::default(),
        wal_highwater: WalHighWatermark::default(),
    })
}

struct FakeMergeCursor {
    points: Vec<DataPoint>,
    next_idx: usize,
}

impl FakeMergeCursor {
    fn new(points: Vec<DataPoint>) -> Self {
        Self {
            points,
            next_idx: 0,
        }
    }
}

impl QueryMergeCursor for FakeMergeCursor {
    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.points.get(self.next_idx).map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        let point = self.points.get(self.next_idx).cloned();
        if point.is_some() {
            self.next_idx = self.next_idx.saturating_add(1);
        }
        Ok(point)
    }
}

#[test]
fn analyze_series_read_sources_routes_unsorted_sealed_data_to_append_sort() {
    let analysis = analyze_series_read_sources(
        &[],
        &[unsorted_chunk(7, &[(2, 2.0), (1, 1.0)])],
        &ActiveSeriesSnapshot::default(),
        0,
        10,
    );

    assert_eq!(
        analysis,
        SeriesReadAnalysis {
            estimated_points: 2,
            has_overlap: false,
            requires_output_validation: true,
            requires_timestamp_dedupe: false,
            requires_exact_dedupe: false,
            persisted_source_sorted: true,
            sealed_source_sorted: true,
        }
    );
    assert!(!analysis.can_use_merge_path());
}

#[test]
fn pop_next_point_from_sources_preserves_source_precedence_on_timestamp_ties() {
    let mut persisted =
        FakeMergeCursor::new(vec![DataPoint::new(1, 10.0), DataPoint::new(5, 50.0)]);
    let mut sealed = FakeMergeCursor::new(vec![DataPoint::new(1, 20.0), DataPoint::new(4, 40.0)]);
    let mut active = FakeMergeCursor::new(vec![DataPoint::new(1, 30.0), DataPoint::new(3, 30.5)]);

    let mut sources = [
        QueryMergeSourceRef::new(&mut persisted),
        QueryMergeSourceRef::new(&mut sealed),
        QueryMergeSourceRef::new(&mut active),
    ];
    let mut merged = Vec::new();
    while let Some(point) = pop_next_point_from_sources(&mut sources).unwrap() {
        merged.push(point);
    }

    assert_eq!(
        merged,
        vec![
            DataPoint::new(1, 10.0),
            DataPoint::new(1, 20.0),
            DataPoint::new(1, 30.0),
            DataPoint::new(3, 30.5),
            DataPoint::new(4, 40.0),
            DataPoint::new(5, 50.0),
        ]
    );
}

#[test]
fn sorted_series_page_collector_applies_pagination_after_filters_and_dedupe() {
    let tombstones = [tombstone::TombstoneRange { start: 12, end: 13 }];
    let mut collector = SortedSeriesPageCollector::new(
        Some(11),
        Some(&tombstones),
        SortedSeriesDedupeMode::Timestamp,
        RawSeriesPagination::new(1, Some(1)),
        1,
    );

    assert!(!collector.push(DataPoint::new(10, 10.0)));
    assert!(!collector.push(DataPoint::new(11, 11.0)));
    assert!(!collector.push(DataPoint::new(11, 11.5)));
    assert!(!collector.push(DataPoint::new(12, 12.0)));
    assert!(!collector.push(DataPoint::new(13, 13.0)));
    collector.finish();

    assert_eq!(collector.final_rows_seen(), 2);
    assert_eq!(collector.into_points(), vec![DataPoint::new(13, 13.0)]);
}

#[test]
fn snapshot_in_memory_series_sources_release_shard_locks_without_select_wrapper() {
    let storage = Arc::new(live_storage(512));

    let query_metric = "snapshot_query_metric";
    let query_labels = vec![Label::new("host", "query")];
    let rows = (0..192i64)
        .map(|ts| {
            Row::with_labels(
                query_metric,
                query_labels.clone(),
                DataPoint::new(ts, ts as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let query_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(query_metric, &query_labels)
        .unwrap()
        .series_id;
    let target_shard = ChunkStorage::series_shard_idx(query_series_id);

    let writer_metric = "snapshot_writer_metric";
    let writer_labels = (0..1024usize)
        .find_map(|idx| {
            let labels = vec![Label::new("host", format!("writer-{idx}"))];
            storage
                .insert_rows(&[Row::with_labels(
                    writer_metric,
                    labels.clone(),
                    DataPoint::new(10_000, 10_000.0),
                )])
                .unwrap();
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(writer_metric, &labels)
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id) == target_shard).then_some(labels)
        })
        .expect("expected same-shard writer series");

    let snapshot_storage = Arc::clone(&storage);
    let (snapshot_ready_tx, snapshot_ready_rx) = mpsc::channel();
    let (snapshot_release_tx, snapshot_release_rx) = mpsc::channel();
    let snapshot_thread = thread::spawn(move || {
        let (snapshot, stats) =
            snapshot_storage.snapshot_in_memory_series_sources(query_series_id, 0, 256);
        snapshot_ready_tx
            .send((snapshot.active_point_count(), stats.snapshot_count()))
            .unwrap();
        snapshot_release_rx.recv().unwrap();
    });

    let (point_count, snapshots) = snapshot_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("snapshot did not complete");
    assert_eq!(point_count, 192);
    assert_eq!(snapshots, 1);

    let writer_storage = Arc::clone(&storage);
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_thread = thread::spawn(move || {
        let result = (|| -> Result<()> {
            for offset in 1..=32i64 {
                writer_storage.insert_rows(&[Row::with_labels(
                    writer_metric,
                    writer_labels.clone(),
                    DataPoint::new(10_000 + offset, (10_000 + offset) as f64),
                )])?;
            }
            Ok(())
        })();
        writer_tx.send(result).unwrap();
    });

    let writer_result = writer_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("writer should progress after snapshot creation");
    assert!(writer_result.is_ok());

    snapshot_release_tx.send(()).unwrap();
    writer_thread.join().unwrap();
    snapshot_thread.join().unwrap();
}

#[test]
fn merge_and_append_sort_paths_match_for_persisted_and_active_exact_duplicates() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_storage(temp_dir.path(), 4);
    let labels = vec![Label::new("host", "equivalent")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment_with_outcome().unwrap().persisted);

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;
    storage.visibility.tombstones.write().insert(
        series_id,
        vec![tombstone::TombstoneRange { start: 1, end: 2 }],
    );
    let remote_shard_index = tombstone::ImmutableTombstoneSnapshot::shard_index(series_id);
    let mut remote_shards = (0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT)
        .map(|_| {
            tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(TombstoneMap::new(), 0)
        })
        .collect::<Vec<_>>();
    let mut remote_map = TombstoneMap::new();
    remote_map.insert(
        series_id,
        vec![tombstone::TombstoneRange { start: 2, end: 3 }],
    );
    remote_shards[remote_shard_index] =
        tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(remote_map, 0);
    {
        let _visibility_guard = storage.visibility_write_fence();
        storage
            .tombstone_publication_context()
            .publish_remote_tombstones_locked(
                &storage,
                Arc::new(tombstone::ImmutableTombstoneSnapshot::from_shards(
                    remote_shards,
                )),
            )
            .unwrap();
    }
    let execution = storage
        .begin_query_execution(
            crate::QueryWorkLimits::default(),
            crate::QueryCancellationToken::new(),
        )
        .unwrap()
        .unwrap();
    let plan = TieredQueryPlan::from_cutoffs(0, 10, None, None);

    let (merge_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    assert!(merge_snapshot.analysis.can_use_merge_path());
    let mut merge_points = Vec::new();
    storage
        .execute_series_read_merge_path(
            series_id,
            0,
            10,
            merge_snapshot,
            &mut merge_points,
            Some(&execution),
        )
        .unwrap();

    let (append_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    let mut append_sort_points = Vec::new();
    storage
        .execute_series_read_append_sort_path(
            series_id,
            0,
            10,
            append_snapshot,
            &mut append_sort_points,
            Some(&execution),
        )
        .unwrap();

    assert_eq!(merge_points, append_sort_points);
    assert_eq!(merge_points, vec![DataPoint::new(3, 3.0)]);

    let (merge_page_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    let merge_page = storage
        .collect_raw_series_page_with_merge(
            series_id,
            0,
            10,
            merge_page_snapshot,
            RawSeriesPagination {
                offset: 0,
                limit: Some(10),
            },
            Some(&execution),
        )
        .unwrap();
    let merge_page_reserved = merge_page.reserved_memory_bytes();
    assert_eq!(
        merge_page_reserved,
        modeled_points_retained_bytes(&merge_page.points)
    );
    assert_eq!(
        execution.snapshot().memory_reserved_bytes,
        merge_page_reserved
    );
    let (append_page_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    let append_page = storage
        .collect_raw_series_page_with_append_sort(
            series_id,
            0,
            10,
            append_page_snapshot,
            RawSeriesPagination {
                offset: 0,
                limit: Some(10),
            },
            Some(&execution),
        )
        .unwrap();
    let append_page_reserved = append_page.reserved_memory_bytes();
    assert_eq!(
        append_page_reserved,
        modeled_points_retained_bytes(&append_page.points)
    );
    assert_eq!(
        execution.snapshot().memory_reserved_bytes,
        merge_page_reserved.saturating_add(append_page_reserved)
    );
    assert_eq!(merge_page.points, vec![DataPoint::new(3, 3.0)]);
    assert_eq!(append_page.points, merge_page.points);
    drop(append_page);
    drop(merge_page);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);

    drop(execution);
    storage.visibility.tombstones.write().remove(&series_id);
    {
        let _visibility_guard = storage.visibility_write_fence();
        storage
            .tombstone_publication_context()
            .publish_remote_tombstones_locked(
                &storage,
                tombstone::ImmutableTombstoneSnapshot::empty(),
            )
            .unwrap();
    }
    drop(storage);
}

#[test]
fn raw_page_to_rows_coalescing_peak_is_exact() {
    const POINTS: usize = 32;
    let series = MetricSeries {
        name: "raw_page_row_coalesce".to_string(),
        labels: vec![Label::new("identity", "x".repeat(128))],
    };
    let build_storage = |limits| {
        let storage =
            ChunkStorage::new_with_data_path_and_options_and_disk_budget_and_query_budget(
                512,
                None,
                None,
                None,
                1,
                test_options(None),
                None,
                limits,
            )
            .unwrap();
        let rows = (0..POINTS)
            .map(|index| {
                Row::with_labels(
                    series.name.clone(),
                    series.labels.clone(),
                    DataPoint::new(
                        i64::try_from(index).unwrap(),
                        Value::String(format!("value-{index:04}-{}", "v".repeat(128))),
                    ),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage
    };

    let calibration = build_storage(QueryBudgetLimits::default());
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let result = calibration
        .scan_series_rows_with_execution_result(
            std::slice::from_ref(&series),
            0,
            i64::try_from(POINTS).unwrap(),
            crate::QueryRowsScanOptions::default(),
            &execution,
        )
        .unwrap();
    assert_eq!(result.page.rows.len(), POINTS);
    assert_eq!(
        result.reserved_memory_bytes(),
        modeled_query_rows_retained_bytes(&result.page.rows)
    );
    assert_eq!(
        execution.snapshot().memory_reserved_bytes,
        result.reserved_memory_bytes()
    );
    let exact_peak = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_peak > result.reserved_memory_bytes());
    drop(result);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    drop(execution);

    for (memory_limit, should_succeed) in [(exact_peak, true), (exact_peak - 1, false)] {
        let storage = build_storage(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.scan_series_rows_with_execution_result(
            std::slice::from_ref(&series),
            0,
            i64::try_from(POINTS).unwrap(),
            crate::QueryRowsScanOptions::default(),
            &execution,
        );
        if should_succeed {
            let result = result.expect("the exact raw-page/row coalescing peak must succeed");
            assert_eq!(result.page.rows.len(), POINTS);
            assert_eq!(
                result.reserved_memory_bytes(),
                modeled_query_rows_retained_bytes(&result.page.rows)
            );
            drop(result);
        } else {
            assert!(matches!(
                result.expect_err("one byte below the coalescing peak must fail"),
                TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                    if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
            ));
        }
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn point_retained_memory_model_counts_nested_value_allocations() {
    let text = Value::String("retained-string".repeat(8));
    assert_eq!(
        modeled_value_retained_bytes(&text),
        u64::try_from(value_heap_bytes(&text))
            .unwrap()
            .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
    );

    let histogram = Value::Histogram(Box::new(crate::NativeHistogram {
        count: Some(crate::HistogramCount::Int(7)),
        sum: 11.0,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(crate::HistogramCount::Float(0.5)),
        negative_spans: vec![crate::HistogramBucketSpan {
            offset: -2,
            length: 1,
        }],
        negative_deltas: vec![-1],
        negative_counts: vec![1.0],
        positive_spans: vec![crate::HistogramBucketSpan {
            offset: 1,
            length: 2,
        }],
        positive_deltas: vec![1, 2],
        positive_counts: vec![2.0, 4.0],
        reset_hint: crate::HistogramResetHint::Gauge,
        custom_values: vec![0.25, 0.75],
    }));
    assert_eq!(
        modeled_value_retained_bytes(&histogram),
        u64::try_from(value_heap_bytes(&histogram))
            .unwrap()
            .saturating_add(8 * QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
        "the histogram box and all seven non-empty vectors need separate allowances",
    );
}

#[test]
fn persisted_and_sealed_merge_query_memory_peak_is_exact() {
    const POINTS_PER_SOURCE: usize = 8;
    const TOTAL_POINTS: usize = POINTS_PER_SOURCE * 2;
    const METRIC: &str = "mixed_persisted_sealed_peak";

    let build_storage = |limits| {
        let temp_dir = TempDir::new().unwrap();
        let storage = persistent_numeric_storage_with_query_budget(
            temp_dir.path(),
            POINTS_PER_SOURCE,
            limits,
        );
        storage
            .insert_rows(
                &(0..POINTS_PER_SOURCE)
                    .map(|index| {
                        Row::new(
                            METRIC,
                            DataPoint::new(i64::try_from(index).unwrap(), index as f64),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);

        storage
            .insert_rows(
                &(POINTS_PER_SOURCE..TOTAL_POINTS)
                    .map(|index| {
                        Row::new(
                            METRIC,
                            DataPoint::new(i64::try_from(index).unwrap(), index as f64),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        storage.flush_all_active().unwrap();
        let series_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing(METRIC, &[])
            .unwrap()
            .series_id;
        (temp_dir, storage, series_id)
    };

    let assert_merge_layout = |storage: &ChunkStorage, series_id| {
        let (snapshot, _) = storage
            .snapshot_series_read_sources(
                series_id,
                0,
                i64::try_from(TOTAL_POINTS).unwrap(),
                TieredQueryPlan::from_cutoffs(0, i64::try_from(TOTAL_POINTS).unwrap(), None, None),
            )
            .unwrap();
        assert_eq!(snapshot.persisted.chunks.len(), 1);
        assert_eq!(snapshot.sealed_chunks.len(), 1);
        assert_eq!(snapshot.active_points.point_count(), 0);
        assert!(snapshot.analysis.can_use_merge_path());
    };

    let expected = (0..TOTAL_POINTS)
        .map(|index| DataPoint::new(i64::try_from(index).unwrap(), index as f64))
        .collect::<Vec<_>>();
    let (_calibration_dir, calibration, calibration_series_id) =
        build_storage(QueryBudgetLimits::default());
    assert_merge_layout(&calibration, calibration_series_id);
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let page = calibration
        .collect_raw_series_page_with_plan(
            calibration_series_id,
            0,
            i64::try_from(TOTAL_POINTS).unwrap(),
            TieredQueryPlan::from_cutoffs(0, i64::try_from(TOTAL_POINTS).unwrap(), None, None),
            0,
            None,
            Some(&execution),
            true,
        )
        .unwrap();
    assert_eq!(page.points, expected);
    assert!(page.reached_end);
    assert_eq!(page.stats.hot_persisted_chunks_read, 1);
    let retained_bytes = page.reserved_memory_bytes();
    let exact_peak = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_peak > retained_bytes);
    drop(page);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    drop(execution);
    let calibration_budget = calibration.query_budget_snapshot();
    assert_eq!(calibration_budget.active_queries, 0);
    assert_eq!(calibration_budget.shared_reserved_memory_bytes, 0);
    assert_eq!(calibration_budget.accounting_invariant_violations_total, 0);

    for (memory_limit, should_succeed) in [(exact_peak, true), (exact_peak - 1, false)] {
        let (_temp_dir, storage, series_id) = build_storage(query_memory_limits(memory_limit));
        assert_merge_layout(&storage, series_id);
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.collect_raw_series_page_with_plan(
            series_id,
            0,
            i64::try_from(TOTAL_POINTS).unwrap(),
            TieredQueryPlan::from_cutoffs(0, i64::try_from(TOTAL_POINTS).unwrap(), None, None),
            0,
            None,
            Some(&execution),
            true,
        );
        if should_succeed {
            let page = result.expect("the exact persisted-plus-sealed peak must succeed");
            assert_eq!(page.points, expected);
            assert!(page.reached_end);
            assert_eq!(page.stats.hot_persisted_chunks_read, 1);
            assert_eq!(page.reserved_memory_bytes(), retained_bytes);
            drop(page);
        } else {
            assert!(matches!(
                result.expect_err("one byte below the mixed-source peak must fail"),
                TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                    if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
            ));
        }
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let budget = storage.query_budget_snapshot();
        assert_eq!(budget.active_queries, 0);
        assert_eq!(budget.shared_reserved_memory_bytes, 0);
        assert_eq!(budget.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn zstd_compressed_constant_blob_query_memory_peak_is_exact() {
    const POINTS: usize = 16;
    const METRIC: &str = "compressed_constant_blob_peak";

    let constant_value = Value::String("highly-compressible-query-payload-".repeat(512));
    let build_storage = |limits| {
        let temp_dir = TempDir::new().unwrap();
        let storage = persistent_blob_storage_with_query_budget(temp_dir.path(), POINTS, limits);
        storage
            .insert_rows(
                &(0..POINTS)
                    .map(|index| {
                        Row::new(
                            METRIC,
                            DataPoint::new(i64::try_from(index).unwrap(), constant_value.clone()),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        let series_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing(METRIC, &[])
            .unwrap()
            .series_id;
        (temp_dir, storage, series_id)
    };

    let assert_zstd_constant_chunk = |storage: &ChunkStorage, series_id| {
        let persisted = storage.persisted.persisted_index.read();
        let chunks = persisted.chunk_refs.get(&series_id).unwrap();
        assert_eq!(chunks.len(), 1);
        let chunk = chunks[0];
        assert_eq!(chunk.lane, ValueLane::Blob);
        assert_eq!(chunk.value_codec, chunk::ValueCodecId::ConstantRle);
        let segment = persisted.segment_maps.get(&chunk.segment_slot).unwrap();
        let (decoded_len, compressed) =
            crate::engine::segment::chunk_payload_decoded_len_from_record(
                segment.as_slice(),
                chunk.chunk_offset,
                chunk.chunk_len,
            )
            .unwrap();
        assert!(compressed, "the persisted chunk must use zstd");
        assert!(decoded_len > usize::try_from(chunk.chunk_len).unwrap());
    };

    let expected = (0..POINTS)
        .map(|index| DataPoint::new(i64::try_from(index).unwrap(), constant_value.clone()))
        .collect::<Vec<_>>();
    let (_calibration_dir, calibration, calibration_series_id) =
        build_storage(QueryBudgetLimits::default());
    assert_zstd_constant_chunk(&calibration, calibration_series_id);
    let (snapshot, _) = calibration
        .snapshot_series_read_sources(
            calibration_series_id,
            0,
            i64::try_from(POINTS).unwrap(),
            TieredQueryPlan::from_cutoffs(0, i64::try_from(POINTS).unwrap(), None, None),
        )
        .unwrap();
    assert_eq!(snapshot.persisted.chunks.len(), 1);
    assert!(snapshot.sealed_chunks.is_empty());
    assert_eq!(snapshot.active_points.point_count(), 0);
    assert!(snapshot.analysis.can_use_merge_path());
    drop(snapshot);

    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let page = calibration
        .collect_raw_series_page_with_plan(
            calibration_series_id,
            0,
            i64::try_from(POINTS).unwrap(),
            TieredQueryPlan::from_cutoffs(0, i64::try_from(POINTS).unwrap(), None, None),
            0,
            None,
            Some(&execution),
            true,
        )
        .unwrap();
    assert_eq!(page.points, expected);
    assert!(page.reached_end);
    let retained_bytes = page.reserved_memory_bytes();
    let exact_peak = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_peak > retained_bytes);
    drop(page);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    drop(execution);
    let calibration_budget = calibration.query_budget_snapshot();
    assert_eq!(calibration_budget.active_queries, 0);
    assert_eq!(calibration_budget.shared_reserved_memory_bytes, 0);
    assert_eq!(calibration_budget.accounting_invariant_violations_total, 0);

    for (memory_limit, should_succeed) in [(exact_peak, true), (exact_peak - 1, false)] {
        let (_temp_dir, storage, series_id) = build_storage(query_memory_limits(memory_limit));
        assert_zstd_constant_chunk(&storage, series_id);
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.collect_raw_series_page_with_plan(
            series_id,
            0,
            i64::try_from(POINTS).unwrap(),
            TieredQueryPlan::from_cutoffs(0, i64::try_from(POINTS).unwrap(), None, None),
            0,
            None,
            Some(&execution),
            true,
        );
        if should_succeed {
            let page = result.expect("the exact zstd-decode peak must succeed");
            assert_eq!(page.points, expected);
            assert!(page.reached_end);
            assert_eq!(page.reserved_memory_bytes(), retained_bytes);
            drop(page);
        } else {
            assert!(matches!(
                result.expect_err("one byte below the zstd-decode peak must fail"),
                TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                    if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
            ));
        }
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let budget = storage.query_budget_snapshot();
        assert_eq!(budget.active_queries, 0);
        assert_eq!(budget.shared_reserved_memory_bytes, 0);
        assert_eq!(budget.accounting_invariant_violations_total, 0);
    }
}
