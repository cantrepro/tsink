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
        retention_enforced: false,
        runtime_mode: StorageRuntimeMode::ReadWrite,
        partition_window: i64::MAX,
        max_active_partition_heads_per_series:
            crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
        max_writers: 2,
        write_timeout: Duration::from_secs(2),
        memory_budget_bytes: u64::MAX,
        cardinality_limit: usize::MAX,
        wal_size_limit_bytes: u64::MAX,
        admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
        compaction_interval: DEFAULT_COMPACTION_INTERVAL,
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
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(path.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        test_options(None),
    )
    .unwrap()
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
    let plan = TieredQueryPlan::from_cutoffs(0, 10, None, None);

    let (merge_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    assert!(merge_snapshot.analysis.can_use_merge_path());
    let mut merge_points = Vec::new();
    storage
        .execute_series_read_merge_path(series_id, 0, 10, merge_snapshot, &mut merge_points)
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
        )
        .unwrap();

    assert_eq!(merge_points, append_sort_points);
    assert_eq!(
        merge_points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
        ]
    );

    storage.close().unwrap();
}
