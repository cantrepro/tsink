//! Focused benchmarks for `insert_rows` and `select`.
//!
//! Run all cases:
//!   cargo bench --bench storage_benchmarks -- "^(insert_rows|select|live_head_merge_queries|concurrent_rw|hot_metric_fanout_writes|persist_refresh_long_history|retained_close_long_history|flush_large_sealed_snapshot|metadata_select_time_range|metadata_select_persisted_time_range|metadata_select_persisted_matchers|metadata_select_persisted_segment_scaling|metadata_list_metrics_high_cardinality|startup_persisted_segments|startup_wal_replay)/"
//!
//! Quick check:
//!   cargo bench --bench storage_benchmarks -- "^(insert_rows|select|live_head_merge_queries|concurrent_rw|hot_metric_fanout_writes|persist_refresh_long_history|retained_close_long_history|flush_large_sealed_snapshot|metadata_select_time_range|metadata_select_persisted_time_range|metadata_select_persisted_matchers|metadata_select_persisted_segment_scaling|metadata_list_metrics_high_cardinality|startup_persisted_segments|startup_wal_replay)/" --quick --noplot

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tsink::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use tsink::engine::encoder::Encoder;
use tsink::engine::engine::ChunkStorage;
use tsink::engine::segment::{SegmentWriter, WalHighWatermark};
use tsink::engine::series::{SeriesRegistry, SeriesValueFamily};
use tsink::engine::wal::FramedWal;
use tsink::label::stable_series_identity_hash;
use tsink::wal::WalSyncMode;
use tsink::{
    DataPoint, Label, MetadataShardScope, Row, SeriesMatcher, SeriesSelection, Storage,
    StorageBuilder, TimestampPrecision, Value,
};

const LARGE_SELECT_SIZE: usize = 1_000_000;
const LOAD_BATCH_SIZE: usize = 10_000;
const CONCURRENT_READ_POINTS: usize = 1000;
const CONCURRENT_WRITES_PER_WRITER: usize = 1000;
const CONCURRENT_READS_PER_READER: usize = 1000;
const CONCURRENT_WRITE_TS_OFFSET: i64 = 10_000_000;
const HOT_METRIC_FANOUT_WRITES_PER_WRITER: usize = 1000;
const HOT_METRIC_NAME: &str = "hot_metric_fanout";
const INCREMENTAL_REFRESH_METRIC: &str = "persist_refresh_metric";
const RETAINED_CLOSE_METRIC: &str = "retained_close_metric";
const LARGE_FLUSH_METRIC: &str = "flush_large_snapshot_metric";
const LARGE_FLUSH_CHUNK_POINTS: usize = 2;
const METADATA_SELECT_HISTORY_SERIES: usize = 2048;
const METADATA_SELECT_METRIC: &str = "metadata_select_metric";
const METADATA_SELECT_STEP_NANOS: i64 = 1_000_000;
const METADATA_MATCHER_SHARD_COUNT: u32 = 8;
const PERSISTED_STARTUP_BENCH_METRIC: &str = "startup_persisted_metric";
const WAL_REPLAY_BENCH_METRIC: &str = "wal_replay_metric";
const WAL_REPLAY_CHUNK_POINTS: usize = 256;

struct LiveHeadSelectHarness {
    storage: Arc<dyn Storage>,
    metric: String,
    labels: Vec<Label>,
    start: i64,
    end: i64,
    expected_points: usize,
}

struct LiveHeadSelectAllHarness {
    storage: Arc<dyn Storage>,
    metric: String,
    start: i64,
    end: i64,
    series_count: usize,
    expected_points_per_series: usize,
}

struct IncrementalRefreshHarness {
    _temp_dir: TempDir,
    storage: ChunkStorage,
    labels: Vec<Label>,
    next_ts: i64,
}

struct MetadataTimeRangeHarness {
    _temp_dir: TempDir,
    storage: ChunkStorage,
    selection: SeriesSelection,
}

struct MetadataOverlapTimeRangeHarness {
    _temp_dir: TempDir,
    storage: ChunkStorage,
    selection: SeriesSelection,
}

struct PersistedMetadataTimeRangeHarness {
    _temp_dir: TempDir,
    storage: Arc<dyn Storage>,
    selection: SeriesSelection,
}

struct MetadataMatcherHarness {
    _temp_dir: TempDir,
    storage: Arc<dyn Storage>,
    exact_selection: SeriesSelection,
    broad_exact_selection: SeriesSelection,
    regex_selection: SeriesSelection,
    empty_regex_selection: SeriesSelection,
    negative_selection: SeriesSelection,
    shard_scope: MetadataShardScope,
}

struct PersistedMetadataMatcherHarness {
    _temp_dir: TempDir,
    storage: Arc<dyn Storage>,
    exact_selection: SeriesSelection,
    broad_exact_selection: SeriesSelection,
    regex_selection: SeriesSelection,
    empty_regex_selection: SeriesSelection,
    negative_selection: SeriesSelection,
}

struct WalReplayHarness {
    fixture_dir: TempDir,
    point_count: usize,
}

struct PersistedStartupHarness {
    fixture_dir: TempDir,
    series_count: usize,
    chunk_count: usize,
}

struct PersistedMetadataSegmentScalingHarness {
    _fixture_dir: TempDir,
    storage: Arc<dyn Storage>,
    selection: SeriesSelection,
    series_count: usize,
}

struct FlushHarness {
    _temp_dir: TempDir,
    storage: ChunkStorage,
}

struct RetainedCloseHarness {
    _temp_dir: TempDir,
    storage: Arc<dyn Storage>,
}

fn base_ts_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 3600
}

fn base_ts_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
        - 1_000_000_000
}

fn build_storage() -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        // Keep all generated points in one partition to reduce cross-partition variance.
        .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
        .with_wal_enabled(false)
        .build()
        .unwrap()
}

fn build_live_head_storage(chunk_points: usize) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(chunk_points)
        // Keep all generated points in one partition to reduce cross-partition variance.
        .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
        .with_wal_enabled(false)
        .build()
        .unwrap()
}

fn make_rows(metric: &str, start_ts: i64, count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            let ts = start_ts + i as i64;
            Row::new(metric, DataPoint::new(ts, i as f64))
        })
        .collect()
}

fn make_persisted_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
    let chunk_points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();
    let encoded = Encoder::encode_chunk_points(&chunk_points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: chunk_points.len() as u16,
            min_ts: chunk_points.first().unwrap().ts,
            max_ts: chunk_points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points: chunk_points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}

fn load_series(storage: &Arc<dyn Storage>, metric: &str, points: usize) {
    let base_ts = base_ts_seconds();
    let mut loaded = 0usize;
    while loaded < points {
        let chunk = (points - loaded).min(LOAD_BATCH_SIZE);
        let rows = make_rows(metric, base_ts + loaded as i64, chunk);
        storage.insert_rows(&rows).unwrap();
        loaded += chunk;
    }
}

fn prepare_live_head_select_harness(
    active_points: usize,
    window_points: usize,
) -> LiveHeadSelectHarness {
    let metric = format!("live_head_select_metric_{active_points}_{window_points}");
    let labels = vec![Label::new("host", "bench")];
    let storage = build_live_head_storage(active_points + 1);
    let start_ts = base_ts_seconds();
    let rows = (0..active_points)
        .map(|i| {
            Row::with_labels(
                metric.as_str(),
                labels.clone(),
                DataPoint::new(start_ts + i as i64, i as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let start = start_ts + active_points as i64 - window_points as i64;
    let end = start_ts + active_points as i64;
    let mut warmup = Vec::with_capacity(window_points);
    storage
        .select_into(metric.as_str(), &labels, start, end, &mut warmup)
        .unwrap();
    assert_eq!(warmup.len(), window_points);

    LiveHeadSelectHarness {
        storage,
        metric,
        labels,
        start,
        end,
        expected_points: window_points,
    }
}

fn prepare_live_head_select_all_harness(
    series_count: usize,
    active_points_per_series: usize,
    window_points: usize,
) -> LiveHeadSelectAllHarness {
    let metric = format!(
        "live_head_select_all_metric_{series_count}_{active_points_per_series}_{window_points}"
    );
    let storage = build_live_head_storage(active_points_per_series + 1);
    let start_ts = base_ts_seconds();

    for series_idx in 0..series_count {
        let labels = vec![Label::new("host", format!("series-{series_idx:04}"))];
        let rows = (0..active_points_per_series)
            .map(|point_idx| {
                Row::with_labels(
                    metric.as_str(),
                    labels.clone(),
                    DataPoint::new(
                        start_ts + point_idx as i64,
                        (series_idx * active_points_per_series + point_idx) as f64,
                    ),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let start = start_ts + active_points_per_series as i64 - window_points as i64;
    let end = start_ts + active_points_per_series as i64;
    let warmup = storage.select_all(metric.as_str(), start, end).unwrap();
    assert_eq!(warmup.len(), series_count);
    assert!(warmup
        .iter()
        .all(|(_, points)| points.len() == window_points));

    LiveHeadSelectAllHarness {
        storage,
        metric,
        start,
        end,
        series_count,
        expected_points_per_series: window_points,
    }
}

fn bench_insert_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_rows");
    let base_ts = base_ts_seconds();

    for size in [1usize, 10, 1000] {
        let rows = make_rows("insert_metric", base_ts, size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, {
            let rows = rows.clone();
            move |b, &_size| {
                b.iter_batched(
                    build_storage,
                    |storage| {
                        storage.insert_rows(black_box(rows.as_slice())).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }

    group.finish();
}

fn bench_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("select");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(8));

    for size in [1usize, 10, 1000, LARGE_SELECT_SIZE] {
        let metric = format!("select_metric_{size}");
        let storage = build_storage();
        let start = base_ts_seconds();
        let rows = make_rows(&metric, start, size);
        storage.insert_rows(&rows).unwrap();
        let end = start + size as i64;

        let mut warmup = Vec::with_capacity(size);
        storage
            .select_into(metric.as_str(), &[], start, end, &mut warmup)
            .unwrap();
        assert_eq!(warmup.len(), size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, {
            let metric = metric.clone();
            move |b, &_size| {
                let labels: &[Label] = &[];
                let mut out = Vec::with_capacity(size);
                b.iter(|| {
                    storage
                        .select_into(
                            black_box(metric.as_str()),
                            labels,
                            black_box(start),
                            black_box(end),
                            &mut out,
                        )
                        .unwrap();
                    black_box(out.len());
                });
            }
        });
    }

    group.finish();
}

fn bench_live_head_merge_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("live_head_merge_queries");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    for (active_points, window_points) in [(16_384usize, 128usize), (65_536, 128)] {
        let harness = prepare_live_head_select_harness(active_points, window_points);
        group.throughput(Throughput::Elements(window_points as u64));
        group.bench_function(
            BenchmarkId::new(
                "select_tail_window",
                format!("live{active_points}_window{window_points}"),
            ),
            |b| {
                let labels: &[Label] = &harness.labels;
                let mut out = Vec::with_capacity(harness.expected_points);
                b.iter(|| {
                    harness
                        .storage
                        .select_into(
                            black_box(harness.metric.as_str()),
                            black_box(labels),
                            black_box(harness.start),
                            black_box(harness.end),
                            &mut out,
                        )
                        .unwrap();
                    black_box(out.len());
                });
            },
        );
    }

    for (series_count, active_points_per_series, window_points) in
        [(32usize, 2_048usize, 64usize), (128, 4_096, 64)]
    {
        let harness = prepare_live_head_select_all_harness(
            series_count,
            active_points_per_series,
            window_points,
        );
        group.throughput(Throughput::Elements(
            (harness.series_count * harness.expected_points_per_series) as u64,
        ));
        group.bench_function(
            BenchmarkId::new(
                "select_all_tail_window",
                format!(
                    "series{series_count}_live{active_points_per_series}_window{window_points}"
                ),
            ),
            |b| {
                b.iter(|| {
                    let selected = harness
                        .storage
                        .select_all(
                            black_box(harness.metric.as_str()),
                            black_box(harness.start),
                            black_box(harness.end),
                        )
                        .unwrap();
                    black_box(selected.len());
                    black_box(
                        selected
                            .iter()
                            .map(|(_, points)| points.len())
                            .sum::<usize>(),
                    );
                });
            },
        );
    }

    group.finish();
}

fn prepare_concurrent_storage() -> Arc<dyn Storage> {
    let storage = build_storage();
    load_series(&storage, "concurrent_read_metric", CONCURRENT_READ_POINTS);
    storage
}

fn prepare_hot_metric_fanout_storage(max_writers: usize) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
        .with_wal_enabled(false)
        .with_max_writers(max_writers)
        .build()
        .unwrap()
}

fn prepare_incremental_refresh_storage(history_segments: usize) -> IncrementalRefreshHarness {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let storage = ChunkStorage::try_new_with_data_path(1, None, Some(lane_path), None, 1).unwrap();
    let labels = vec![Label::new("host", "bench")];
    let base_ts = base_ts_nanos();

    for offset in 0..history_segments {
        storage
            .insert_rows(&[Row::with_labels(
                INCREMENTAL_REFRESH_METRIC,
                labels.clone(),
                DataPoint::new(base_ts + offset as i64, offset as f64),
            )])
            .unwrap();
        storage.flush().unwrap();
    }

    IncrementalRefreshHarness {
        _temp_dir: temp_dir,
        storage,
        labels,
        next_ts: base_ts + history_segments as i64,
    }
}

fn build_retained_close_storage(path: &Path) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .with_retention(Duration::from_secs(24 * 3600))
        .with_wal_enabled(false)
        .build()
        .unwrap()
}

fn prepare_retained_close_harness(history_segments: usize) -> RetainedCloseHarness {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let labels = vec![Label::new("host", "bench")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(RETAINED_CLOSE_METRIC, &labels)
        .unwrap()
        .series_id;
    registry
        .record_series_value_family(series_id, SeriesValueFamily::F64)
        .unwrap();
    let base_ts = base_ts_seconds();

    for segment_idx in 0..history_segments {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(base_ts + segment_idx as i64, segment_idx as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_idx as u64 + 1)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = build_retained_close_storage(temp_dir.path());
    storage
        .insert_rows(&[Row::with_labels(
            RETAINED_CLOSE_METRIC,
            labels,
            DataPoint::new(base_ts + history_segments as i64, history_segments as f64),
        )])
        .unwrap();

    RetainedCloseHarness {
        _temp_dir: temp_dir,
        storage,
    }
}

fn prepare_persisted_startup_harness(
    segment_count: usize,
    series_count: usize,
    chunks_per_series_per_segment: usize,
) -> PersistedStartupHarness {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let registry = SeriesRegistry::new();
    let series_ids = (0..series_count)
        .map(|series_idx| {
            let labels = vec![Label::new(
                "host",
                format!("startup-series-{series_idx:04}"),
            )];
            registry
                .resolve_or_insert(PERSISTED_STARTUP_BENCH_METRIC, &labels)
                .unwrap()
                .series_id
        })
        .collect::<Vec<_>>();
    for series_id in &series_ids {
        registry
            .record_series_value_family(*series_id, SeriesValueFamily::F64)
            .unwrap();
    }
    let base_ts = base_ts_seconds().saturating_sub(
        i64::try_from(segment_count.saturating_mul(chunks_per_series_per_segment) * 8).unwrap(),
    );

    for segment_idx in 0..segment_count {
        let mut chunks_by_series = HashMap::new();
        for (series_offset, series_id) in series_ids.iter().copied().enumerate() {
            let mut chunks = Vec::with_capacity(chunks_per_series_per_segment);
            for chunk_idx in 0..chunks_per_series_per_segment {
                let chunk_base = base_ts
                    + (series_offset as i64 * 1_000_000)
                    + (segment_idx as i64 * chunks_per_series_per_segment as i64 * 8)
                    + (chunk_idx as i64 * 8);
                chunks.push(make_persisted_numeric_chunk(
                    series_id,
                    &[(chunk_base, 1.0), (chunk_base + 1, 2.0)],
                ));
            }
            chunks_by_series.insert(series_id, chunks);
        }

        SegmentWriter::new(&lane_path, 0, segment_idx as u64 + 1)
            .unwrap()
            .write_segment(&registry, &chunks_by_series)
            .unwrap();
    }

    let primed = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        primed
            .select_series(&SeriesSelection::new().with_metric(PERSISTED_STARTUP_BENCH_METRIC))
            .unwrap()
            .len(),
        series_count
    );
    primed.close().unwrap();

    PersistedStartupHarness {
        fixture_dir: temp_dir,
        series_count,
        chunk_count: segment_count
            .saturating_mul(series_count)
            .saturating_mul(chunks_per_series_per_segment),
    }
}

fn prepare_persisted_metadata_segment_scaling_harness(
    segment_count: usize,
) -> PersistedMetadataSegmentScalingHarness {
    let startup = prepare_persisted_startup_harness(segment_count, 256, 1);
    let selection = SeriesSelection::new()
        .with_metric(PERSISTED_STARTUP_BENCH_METRIC)
        .with_matcher(SeriesMatcher::regex_match(
            "host",
            "startup-series-(0003|0007|0011)",
        ));
    let storage = StorageBuilder::new()
        .with_data_path(startup.fixture_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let selected = storage.select_series(&selection).unwrap();
    assert_eq!(selected.len(), 3);

    PersistedMetadataSegmentScalingHarness {
        _fixture_dir: startup.fixture_dir,
        storage,
        selection,
        series_count: startup.series_count,
    }
}

fn prepare_flush_harness(series_count: usize, chunks_per_series: usize) -> FlushHarness {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let storage = ChunkStorage::try_new_with_data_path(
        LARGE_FLUSH_CHUNK_POINTS,
        None,
        Some(lane_path),
        None,
        1,
    )
    .unwrap();
    let points_per_series = chunks_per_series * LARGE_FLUSH_CHUNK_POINTS;
    let base_ts = base_ts_nanos();
    let rows = (0..series_count)
        .flat_map(|series_idx| {
            let labels = vec![Label::new("host", format!("flush-series-{series_idx:04}"))];
            (0..points_per_series).map(move |point_idx| {
                Row::with_labels(
                    LARGE_FLUSH_METRIC,
                    labels.clone(),
                    DataPoint::new(base_ts + point_idx as i64, point_idx as f64),
                )
            })
        })
        .collect::<Vec<_>>();

    storage.insert_rows(&rows).unwrap();

    FlushHarness {
        _temp_dir: temp_dir,
        storage,
    }
}

fn prepare_metadata_time_range_harness(history_segments: usize) -> MetadataTimeRangeHarness {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let storage = ChunkStorage::try_new_with_data_path(1, None, Some(lane_path), None, 1).unwrap();
    let series_labels = (0..METADATA_SELECT_HISTORY_SERIES)
        .map(|idx| vec![Label::new("host", format!("series-{idx:04}"))])
        .collect::<Vec<_>>();
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let base_ts =
        now_nanos.saturating_sub((history_segments as i64 + 10_000) * METADATA_SELECT_STEP_NANOS);

    for segment_offset in 0..history_segments {
        let ts = base_ts + segment_offset as i64 * METADATA_SELECT_STEP_NANOS;
        let rows = series_labels
            .iter()
            .map(|labels| {
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    labels.clone(),
                    DataPoint::new(ts, segment_offset as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.flush().unwrap();
    }

    let query_start = base_ts + (history_segments as i64 + 100) * METADATA_SELECT_STEP_NANOS;
    let selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_time_range(query_start, query_start + METADATA_SELECT_STEP_NANOS);
    assert!(storage.select_series(&selection).unwrap().is_empty());

    MetadataTimeRangeHarness {
        _temp_dir: temp_dir,
        storage,
        selection,
    }
}

fn prepare_metadata_overlap_time_range_harness() -> MetadataOverlapTimeRangeHarness {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let storage = ChunkStorage::try_new_with_data_path(8, None, Some(lane_path), None, 1).unwrap();
    let base_ts = base_ts_nanos();
    let rows = (0..METADATA_SELECT_HISTORY_SERIES)
        .flat_map(|idx| {
            let labels = vec![Label::new("host", format!("overlap-series-{idx:04}"))];
            [
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    labels.clone(),
                    DataPoint::new(base_ts, 1.0),
                ),
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    labels.clone(),
                    DataPoint::new(base_ts + 100, 2.0),
                ),
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    labels,
                    DataPoint::new(base_ts + 200, 3.0),
                ),
            ]
        })
        .collect::<Vec<_>>();

    storage.insert_rows(&rows).unwrap();
    storage.flush().unwrap();

    let selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_time_range(base_ts + 50, base_ts + 90);
    assert!(storage.select_series(&selection).unwrap().is_empty());

    MetadataOverlapTimeRangeHarness {
        _temp_dir: temp_dir,
        storage,
        selection,
    }
}

fn build_persisted_metadata_time_range_storage(path: &Path) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
        .with_wal_enabled(false)
        .build()
        .unwrap()
}

fn prepare_persisted_metadata_time_range_harness(
    history_segments: usize,
) -> PersistedMetadataTimeRangeHarness {
    let temp_dir = TempDir::new().unwrap();
    let matching_series = 8usize;
    let query_step = 10i64;
    let base_ts = base_ts_seconds().saturating_sub((history_segments as i64 + 2_000) * query_step);

    {
        let storage = build_persisted_metadata_time_range_storage(temp_dir.path());
        let mixed_rows = (0..METADATA_SELECT_HISTORY_SERIES)
            .map(|idx| {
                let ts = if idx < matching_series {
                    base_ts
                } else {
                    base_ts + 1_000
                };
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    vec![Label::new(
                        "host",
                        format!("persisted-time-series-{idx:04}"),
                    )],
                    DataPoint::new(ts, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&mixed_rows).unwrap();
        storage.close().unwrap();
    }

    for segment_offset in 0..history_segments {
        let storage = build_persisted_metadata_time_range_storage(temp_dir.path());
        let ts = base_ts + 2_000 + (segment_offset as i64 * query_step);
        let rows = (0..METADATA_SELECT_HISTORY_SERIES)
            .map(|idx| {
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    vec![Label::new(
                        "host",
                        format!("persisted-time-series-{idx:04}"),
                    )],
                    DataPoint::new(ts, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = build_persisted_metadata_time_range_storage(temp_dir.path());
    let selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_time_range(base_ts, base_ts + query_step);
    assert_eq!(
        storage.select_series(&selection).unwrap().len(),
        matching_series
    );

    PersistedMetadataTimeRangeHarness {
        _temp_dir: temp_dir,
        storage,
        selection,
    }
}

fn prepare_persisted_metadata_time_range_gap_harness(
    history_segments: usize,
) -> PersistedMetadataTimeRangeHarness {
    let temp_dir = TempDir::new().unwrap();
    let matching_series = 8usize;

    for segment_offset in 0..history_segments {
        let storage = build_persisted_metadata_time_range_storage(temp_dir.path());
        let recent_ts = 1_000 + (segment_offset as i64 * 10);
        let mut rows = Vec::with_capacity(
            METADATA_SELECT_HISTORY_SERIES
                .saturating_mul(2)
                .saturating_add(matching_series),
        );
        for idx in 0..METADATA_SELECT_HISTORY_SERIES {
            let labels = vec![Label::new("host", format!("persisted-gap-series-{idx:04}"))];
            rows.push(Row::with_labels(
                METADATA_SELECT_METRIC,
                labels.clone(),
                DataPoint::new(10, idx as f64),
            ));
            rows.push(Row::with_labels(
                METADATA_SELECT_METRIC,
                labels.clone(),
                DataPoint::new(recent_ts, idx as f64),
            ));
            if idx < matching_series {
                rows.push(Row::with_labels(
                    METADATA_SELECT_METRIC,
                    labels,
                    DataPoint::new(500, idx as f64),
                ));
            }
        }
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = build_persisted_metadata_time_range_storage(temp_dir.path());
    let selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_time_range(500, 520);
    assert_eq!(
        storage.select_series(&selection).unwrap().len(),
        matching_series
    );

    PersistedMetadataTimeRangeHarness {
        _temp_dir: temp_dir,
        storage,
        selection,
    }
}

fn prepare_metadata_matcher_harness() -> MetadataMatcherHarness {
    prepare_metadata_matcher_harness_with_series(METADATA_SELECT_HISTORY_SERIES)
}

fn prepare_metadata_matcher_harness_with_series(series_count: usize) -> MetadataMatcherHarness {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
        .with_wal_enabled(false)
        .with_metadata_shard_count(METADATA_MATCHER_SHARD_COUNT)
        .build()
        .unwrap();

    let base_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut shard_counts = vec![0usize; METADATA_MATCHER_SHARD_COUNT as usize];
    let rows = (0..series_count)
        .map(|idx| {
            let mut labels = vec![
                Label::new("host", format!("matcher-series-{idx:06}")),
                Label::new("tenant", format!("tenant-{:02}", idx % 8)),
            ];
            if idx % 3 != 0 {
                labels.push(Label::new("job", format!("job-{:02}", idx % 32)));
            }
            let shard = (stable_series_identity_hash(METADATA_SELECT_METRIC, &labels)
                % u64::from(METADATA_MATCHER_SHARD_COUNT)) as usize;
            shard_counts[shard] += 1;
            Row::with_labels(
                METADATA_SELECT_METRIC,
                labels,
                DataPoint::new(base_ts + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let target_shard = shard_counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| *count)
        .map(|(idx, _)| idx as u32)
        .unwrap();
    let shard_scope = MetadataShardScope::new(METADATA_MATCHER_SHARD_COUNT, vec![target_shard]);

    let exact_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::equal("job", "job-08"));
    let broad_exact_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::equal("tenant", "tenant-03"));
    let regex_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::regex_match("job", "job-(08|09)"));
    let empty_regex_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::regex_match("job", ".*"));
    let negative_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::regex_no_match("job", "job-(08|09)"));

    assert!(!storage.select_series(&exact_selection).unwrap().is_empty());
    assert!(!storage
        .select_series(&broad_exact_selection)
        .unwrap()
        .is_empty());
    assert!(!storage.select_series(&regex_selection).unwrap().is_empty());
    assert_eq!(
        storage.select_series(&empty_regex_selection).unwrap().len(),
        series_count,
    );
    assert!(!storage
        .select_series(&negative_selection)
        .unwrap()
        .is_empty());
    assert!(!storage
        .select_series_in_shards(&exact_selection, &shard_scope)
        .unwrap()
        .is_empty());

    MetadataMatcherHarness {
        _temp_dir: temp_dir,
        storage,
        exact_selection,
        broad_exact_selection,
        regex_selection,
        empty_regex_selection,
        negative_selection,
        shard_scope,
    }
}

fn prepare_persisted_metadata_matcher_harness() -> PersistedMetadataMatcherHarness {
    prepare_persisted_metadata_matcher_harness_with_series(METADATA_SELECT_HISTORY_SERIES)
}

fn prepare_persisted_metadata_matcher_harness_with_series(
    series_count: usize,
) -> PersistedMetadataMatcherHarness {
    let temp_dir = TempDir::new().unwrap();
    let builder = || {
        StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_secs(10 * 365 * 24 * 60 * 60))
            .with_wal_enabled(false)
            .build()
            .unwrap()
    };

    {
        let storage = builder();
        let base_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let rows = (0..series_count)
            .map(|idx| {
                let mut labels = vec![
                    Label::new("host", format!("persisted-series-{idx:06}")),
                    Label::new("tenant", format!("tenant-{:02}", idx % 8)),
                ];
                if idx % 3 != 0 {
                    labels.push(Label::new("job", format!("job-{:02}", idx % 32)));
                }
                Row::with_labels(
                    METADATA_SELECT_METRIC,
                    labels,
                    DataPoint::new(base_ts + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = builder();
    let live_rows = (0..128usize)
        .map(|idx| {
            let live_idx = idx + series_count;
            let mut labels = vec![
                Label::new("host", format!("live-series-{live_idx:06}")),
                Label::new("tenant", format!("tenant-{:02}", live_idx % 8)),
            ];
            if !live_idx.is_multiple_of(3) {
                labels.push(Label::new("job", format!("job-{:02}", live_idx % 32)));
            }
            Row::with_labels(
                METADATA_SELECT_METRIC,
                labels,
                DataPoint::new(base_ts_seconds() + live_idx as i64, live_idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&live_rows).unwrap();

    let exact_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::equal("job", "job-08"));
    let broad_exact_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::equal("tenant", "tenant-03"));
    let regex_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::regex_match("job", "job-(08|09)"));
    let empty_regex_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::regex_match("job", ".*"));
    let negative_selection = SeriesSelection::new()
        .with_metric(METADATA_SELECT_METRIC)
        .with_matcher(SeriesMatcher::regex_no_match("job", "job-(08|09)"));

    assert!(!storage.select_series(&exact_selection).unwrap().is_empty());
    assert!(!storage
        .select_series(&broad_exact_selection)
        .unwrap()
        .is_empty());
    assert!(!storage.select_series(&regex_selection).unwrap().is_empty());
    assert_eq!(
        storage.select_series(&empty_regex_selection).unwrap().len(),
        series_count + live_rows.len(),
    );
    assert!(!storage
        .select_series(&negative_selection)
        .unwrap()
        .is_empty());

    PersistedMetadataMatcherHarness {
        _temp_dir: temp_dir,
        storage,
        exact_selection,
        broad_exact_selection,
        regex_selection,
        empty_regex_selection,
        negative_selection,
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let file_type = entry.file_type().unwrap();
        let source = entry.path();
        let destination = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&source, &destination);
        } else {
            fs::copy(&source, &destination).unwrap();
        }
    }
}

fn prepare_wal_replay_harness(series_count: usize, writes_per_series: usize) -> WalReplayHarness {
    let temp_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(temp_dir.path().join("wal"), WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::try_new(WAL_REPLAY_CHUNK_POINTS, Some(wal)).unwrap();
    let base_ts = base_ts_nanos();

    for write_idx in 0..writes_per_series {
        let rows = (0..series_count)
            .map(|series_idx| {
                Row::with_labels(
                    WAL_REPLAY_BENCH_METRIC,
                    vec![Label::new("host", format!("replay-{series_idx:04}"))],
                    DataPoint::new(base_ts + write_idx as i64, write_idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    drop(storage);

    WalReplayHarness {
        fixture_dir: temp_dir,
        point_count: series_count.saturating_mul(writes_per_series),
    }
}

fn run_incremental_refresh_once(harness: IncrementalRefreshHarness) {
    harness
        .storage
        .insert_rows(&[Row::with_labels(
            INCREMENTAL_REFRESH_METRIC,
            harness.labels.clone(),
            DataPoint::new(harness.next_ts, harness.next_ts as f64),
        )])
        .unwrap();
    harness.storage.flush().unwrap();
    harness.storage.close().unwrap();
}

fn run_retained_close_once(harness: RetainedCloseHarness) {
    harness.storage.close().unwrap();
}

fn run_flush_once(harness: FlushHarness) {
    harness.storage.flush().unwrap();
    harness.storage.close().unwrap();
}

fn run_concurrent_rw_once(storage: Arc<dyn Storage>, writers: usize, readers: usize) {
    let total_threads = writers + readers;
    let barrier = Arc::new(Barrier::new(total_threads + 1));
    let mut handles = Vec::with_capacity(total_threads);

    for writer_id in 0..writers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let writer_base = base_ts_seconds()
                + CONCURRENT_WRITE_TS_OFFSET
                + (writer_id as i64 * CONCURRENT_WRITES_PER_WRITER as i64);
            let mut row = Row::new(
                "concurrent_write_metric",
                DataPoint::new(writer_base, writer_id as f64),
            );

            barrier.wait();
            for i in 0..CONCURRENT_WRITES_PER_WRITER {
                row.set_data_point(DataPoint::new(writer_base + i as i64, i as f64));
                storage.insert_rows(std::slice::from_ref(&row)).unwrap();
            }
        }));
    }

    for _ in 0..readers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut out = Vec::with_capacity(CONCURRENT_READ_POINTS);
            let read_start = base_ts_seconds();
            let read_end = read_start + CONCURRENT_READ_POINTS as i64;

            barrier.wait();
            for _ in 0..CONCURRENT_READS_PER_READER {
                storage
                    .select_into(
                        "concurrent_read_metric",
                        &[],
                        read_start,
                        read_end,
                        &mut out,
                    )
                    .unwrap();
                black_box(out.len());
            }
        }));
    }

    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }
}

fn run_hot_metric_fanout_writes_once(
    storage: Arc<dyn Storage>,
    writers: usize,
    labelsets_per_writer: usize,
) {
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);

    for writer_id in 0..writers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let writer_base = base_ts_seconds()
                + CONCURRENT_WRITE_TS_OFFSET
                + (writer_id as i64 * HOT_METRIC_FANOUT_WRITES_PER_WRITER as i64);
            let mut rows = (0..labelsets_per_writer)
                .map(|series_idx| {
                    Row::with_labels(
                        HOT_METRIC_NAME,
                        vec![
                            Label::new("writer", format!("w{writer_id}")),
                            Label::new("series", format!("s{series_idx}")),
                        ],
                        DataPoint::new(writer_base, writer_id as f64),
                    )
                })
                .collect::<Vec<_>>();

            barrier.wait();
            for i in 0..HOT_METRIC_FANOUT_WRITES_PER_WRITER {
                let series_idx = i % labelsets_per_writer.max(1);
                rows[series_idx].set_data_point(DataPoint::new(writer_base + i as i64, i as f64));
                storage
                    .insert_rows(std::slice::from_ref(&rows[series_idx]))
                    .unwrap();
            }
        }));
    }

    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench_concurrent_rw(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_rw");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for (writers, readers) in [(1usize, 1usize), (2, 2), (4, 4)] {
        let id = format!("w{writers}_r{readers}");
        let operations =
            (writers * CONCURRENT_WRITES_PER_WRITER + readers * CONCURRENT_READS_PER_READER) as u64;

        group.throughput(Throughput::Elements(operations));
        group.bench_with_input(
            BenchmarkId::new("mix", id),
            &(writers, readers),
            |b, &(w, r)| {
                b.iter_batched(
                    prepare_concurrent_storage,
                    |storage| run_concurrent_rw_once(storage, w, r),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_hot_metric_fanout_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_metric_fanout_writes");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for (writers, labelsets_per_writer) in [(2usize, 64usize), (4, 64), (8, 128)] {
        let id = format!("w{writers}_labelsets{labelsets_per_writer}");
        let operations = (writers * HOT_METRIC_FANOUT_WRITES_PER_WRITER) as u64;

        group.throughput(Throughput::Elements(operations));
        group.bench_with_input(
            BenchmarkId::new("same_metric", id),
            &(writers, labelsets_per_writer),
            |b, &(writers, labelsets_per_writer)| {
                b.iter_batched(
                    || prepare_hot_metric_fanout_storage(writers),
                    |storage| {
                        run_hot_metric_fanout_writes_once(storage, writers, labelsets_per_writer)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_persist_refresh_long_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("persist_refresh_long_history");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for history_segments in [64usize, 256, 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(history_segments),
            &history_segments,
            |b, &history_segments| {
                b.iter_batched(
                    || prepare_incremental_refresh_storage(history_segments),
                    run_incremental_refresh_once,
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_retained_close_long_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("retained_close_long_history");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for history_segments in [64usize, 256] {
        group.throughput(Throughput::Elements(history_segments as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(history_segments),
            &history_segments,
            |b, &history_segments| {
                b.iter_batched(
                    || prepare_retained_close_harness(history_segments),
                    run_retained_close_once,
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_metadata_select_time_range_long_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_time_range_long_history");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for history_segments in [32usize, 128, 512] {
        group.throughput(Throughput::Elements(METADATA_SELECT_HISTORY_SERIES as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(history_segments),
            &history_segments,
            |b, &history_segments| {
                let harness = prepare_metadata_time_range_harness(history_segments);
                b.iter(|| {
                    let selected = harness
                        .storage
                        .select_series(black_box(&harness.selection))
                        .unwrap();
                    black_box(selected.len());
                });
                harness.storage.close().unwrap();
            },
        );
    }

    group.finish();
}

fn bench_flush_large_sealed_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_large_sealed_snapshot");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for (series_count, chunks_per_series) in [(64usize, 32usize), (256, 16)] {
        let total_chunks = series_count * chunks_per_series;
        group.throughput(Throughput::Elements(total_chunks as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "series_chunks",
                format!("series{series_count}_chunks{chunks_per_series}"),
            ),
            &(series_count, chunks_per_series),
            |b, &(series_count, chunks_per_series)| {
                b.iter_batched(
                    || prepare_flush_harness(series_count, chunks_per_series),
                    run_flush_once,
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_metadata_select_time_range_partial_overlap(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_time_range_partial_overlap");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));
    group.throughput(Throughput::Elements(METADATA_SELECT_HISTORY_SERIES as u64));

    group.bench_function("series2048", |b| {
        let harness = prepare_metadata_overlap_time_range_harness();
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.selection))
                .unwrap();
            black_box(selected.len());
        });
        harness.storage.close().unwrap();
    });

    group.finish();
}

fn bench_metadata_select_persisted_time_range_long_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_persisted_time_range_long_history");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for history_segments in [32usize, 128, 512] {
        group.throughput(Throughput::Elements(METADATA_SELECT_HISTORY_SERIES as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(history_segments),
            &history_segments,
            |b, &history_segments| {
                let harness = prepare_persisted_metadata_time_range_harness(history_segments);
                b.iter(|| {
                    let selected = harness
                        .storage
                        .select_series(black_box(&harness.selection))
                        .unwrap();
                    black_box(selected.len());
                });
                harness.storage.close().unwrap();
            },
        );
    }

    group.finish();
}

fn bench_metadata_select_persisted_time_range_gap_long_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_persisted_time_range_gap_long_history");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for history_segments in [32usize, 128, 512] {
        group.throughput(Throughput::Elements(METADATA_SELECT_HISTORY_SERIES as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(history_segments),
            &history_segments,
            |b, &history_segments| {
                let harness = prepare_persisted_metadata_time_range_gap_harness(history_segments);
                b.iter(|| {
                    let selected = harness
                        .storage
                        .select_series(black_box(&harness.selection))
                        .unwrap();
                    black_box(selected.len());
                });
                harness.storage.close().unwrap();
            },
        );
    }

    group.finish();
}

fn bench_metadata_select_matchers(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_matchers");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));
    group.throughput(Throughput::Elements(METADATA_SELECT_HISTORY_SERIES as u64));

    let harness = prepare_metadata_matcher_harness();

    group.bench_function("exact", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.exact_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("broad_exact", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.broad_exact_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("regex", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.regex_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("empty_regex", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.empty_regex_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("negative", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.negative_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("shard_scoped", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series_in_shards(
                    black_box(&harness.exact_selection),
                    black_box(&harness.shard_scope),
                )
                .unwrap();
            black_box(selected.len());
        });
    });

    group.finish();
}

fn bench_metadata_select_persisted_matchers(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_persisted_matchers");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));
    group.throughput(Throughput::Elements(METADATA_SELECT_HISTORY_SERIES as u64));

    let harness = prepare_persisted_metadata_matcher_harness();

    group.bench_function("exact", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.exact_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("broad_exact", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.broad_exact_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("regex", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.regex_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("empty_regex", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.empty_regex_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.bench_function("negative", |b| {
        b.iter(|| {
            let selected = harness
                .storage
                .select_series(black_box(&harness.negative_selection))
                .unwrap();
            black_box(selected.len());
        });
    });

    group.finish();
}

fn bench_metadata_select_broad_matchers_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_broad_matchers_high_cardinality");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for series_count in [8_192usize, 32_768usize] {
        group.throughput(Throughput::Elements(series_count as u64));
        let harness = prepare_metadata_matcher_harness_with_series(series_count);

        group.bench_function(BenchmarkId::new("broad_exact", series_count), |b| {
            b.iter(|| {
                let selected = harness
                    .storage
                    .select_series(black_box(&harness.broad_exact_selection))
                    .unwrap();
                black_box(selected.len());
            });
        });

        group.bench_function(BenchmarkId::new("regex", series_count), |b| {
            b.iter(|| {
                let selected = harness
                    .storage
                    .select_series(black_box(&harness.regex_selection))
                    .unwrap();
                black_box(selected.len());
            });
        });

        group.bench_function(BenchmarkId::new("empty_regex", series_count), |b| {
            b.iter(|| {
                let selected = harness
                    .storage
                    .select_series(black_box(&harness.empty_regex_selection))
                    .unwrap();
                black_box(selected.len());
            });
        });

        group.bench_function(BenchmarkId::new("negative", series_count), |b| {
            b.iter(|| {
                let selected = harness
                    .storage
                    .select_series(black_box(&harness.negative_selection))
                    .unwrap();
                black_box(selected.len());
            });
        });
    }

    group.finish();
}

fn bench_metadata_list_metrics_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_list_metrics_high_cardinality");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for series_count in [8_192usize, 32_768usize] {
        let harness = prepare_metadata_matcher_harness_with_series(series_count);
        assert_eq!(harness.storage.list_metrics().unwrap().len(), series_count);
        group.throughput(Throughput::Elements(series_count as u64));
        group.bench_function(BenchmarkId::new("list_metrics", series_count), |b| {
            b.iter(|| {
                let listed = harness.storage.list_metrics().unwrap();
                black_box(listed.len());
            });
        });
    }

    group.finish();
}

fn bench_startup_persisted_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("startup_persisted_segments");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    for (segment_count, series_count, chunks_per_series_per_segment) in [
        (4usize, 32usize, 32usize),
        (4, 64, 64),
        (32, 512, 1),
        (128, 1024, 1),
    ] {
        let harness = prepare_persisted_startup_harness(
            segment_count,
            series_count,
            chunks_per_series_per_segment,
        );
        group.throughput(Throughput::Elements(harness.chunk_count as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "reopen",
                format!(
                    "segments{segment_count}_series{series_count}_chunks{chunks_per_series_per_segment}"
                ),
            ),
            &(),
            |b, _| {
                b.iter_batched(
                    || {
                        let replay_dir = TempDir::new().unwrap();
                        copy_dir_recursive(harness.fixture_dir.path(), replay_dir.path());
                        replay_dir
                    },
                    |replay_dir| {
                        let storage = StorageBuilder::new()
                            .with_data_path(replay_dir.path())
                            .with_timestamp_precision(TimestampPrecision::Seconds)
                            .with_chunk_points(2)
                            .build()
                            .unwrap();
                        let selected = storage
                            .select_series(
                                &SeriesSelection::new().with_metric(PERSISTED_STARTUP_BENCH_METRIC),
                            )
                            .unwrap();
                        assert_eq!(selected.len(), harness.series_count);
                        black_box(selected.len());
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_metadata_select_persisted_segment_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_select_persisted_segment_scaling");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for segment_count in [1usize, 8, 32, 128] {
        let harness = prepare_persisted_metadata_segment_scaling_harness(segment_count);
        group.throughput(Throughput::Elements(harness.series_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(segment_count),
            &segment_count,
            |b, _| {
                b.iter(|| {
                    let selected = harness
                        .storage
                        .select_series(black_box(&harness.selection))
                        .unwrap();
                    black_box(selected.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_startup_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("startup_wal_replay");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    for (series_count, writes_per_series) in [(256usize, 64usize), (1024, 64)] {
        let harness = prepare_wal_replay_harness(series_count, writes_per_series);
        group.throughput(Throughput::Elements(harness.point_count as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "reopen",
                format!("series{series_count}_writes{writes_per_series}"),
            ),
            &(series_count, writes_per_series),
            |b, _| {
                b.iter_batched(
                    || {
                        let replay_dir = TempDir::new().unwrap();
                        copy_dir_recursive(harness.fixture_dir.path(), replay_dir.path());
                        replay_dir
                    },
                    |replay_dir| {
                        let storage = StorageBuilder::new()
                            .with_data_path(replay_dir.path())
                            .with_timestamp_precision(TimestampPrecision::Seconds)
                            .with_chunk_points(WAL_REPLAY_CHUNK_POINTS)
                            .build()
                            .unwrap();
                        let selected = storage
                            .select_series(
                                &SeriesSelection::new().with_metric(WAL_REPLAY_BENCH_METRIC),
                            )
                            .unwrap();
                        assert_eq!(selected.len(), series_count);
                        black_box(selected.len());
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_rows,
    bench_select,
    bench_live_head_merge_queries,
    bench_concurrent_rw,
    bench_hot_metric_fanout_writes,
    bench_persist_refresh_long_history,
    bench_retained_close_long_history,
    bench_flush_large_sealed_snapshot,
    bench_metadata_select_time_range_long_history,
    bench_metadata_select_time_range_partial_overlap,
    bench_metadata_select_persisted_time_range_long_history,
    bench_metadata_select_persisted_time_range_gap_long_history,
    bench_metadata_select_matchers,
    bench_metadata_select_broad_matchers_high_cardinality,
    bench_metadata_list_metrics_high_cardinality,
    bench_metadata_select_persisted_matchers,
    bench_metadata_select_persisted_segment_scaling,
    bench_startup_persisted_segments,
    bench_startup_wal_replay
);
criterion_main!(benches);
