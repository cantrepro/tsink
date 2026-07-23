//! Mixed-workload effective bytes-per-point measurement harness for storage engine work.
//!
//! One-command baselines:
//! `scripts/measure_bpp.sh`
//! `scripts/measure_bpp.sh mixed-order`

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Barrier, Condvar, Mutex};
use std::thread;
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;
use tsink::engine::chunk::{ChunkPoint, ValueLane};
use tsink::engine::wal::{FramedWal, SamplesBatchFrame, SeriesDefinitionFrame};
use tsink::label::stable_series_identity_hash;
use tsink::{
    DataPoint, Label, MetadataShardScope, MetricSeries, QueryBudgetError, QueryCancellationToken,
    QueryLimitReason, QueryWorkLimits, ResourceProfile, ResourceProfileName, Row, SeriesMatcher,
    SeriesSelection, Storage, StorageBuilder, TimestampPrecision, TsinkError, Value, WalSyncMode,
    DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
};

const SUITE_ACTIVE_SERIES_TARGET: usize = 1_000_000;
const GAUGE_WEIGHT: u16 = 600;
const COUNTER_WEIGHT: u16 = 200;
const SPARSE_WEIGHT: u16 = 100;
const SHORT_LIVED_WEIGHT: u16 = 50;
const WEIGHT_SCALE: u16 = 1000;

#[derive(Debug, Clone)]
struct WorkloadConfig {
    resource_profile: ResourceProfile,
    memory_limit_override_bytes: Option<usize>,
    maintenance_max_bytes_override: Option<u64>,
    runs: usize,
    active_series: usize,
    shared_metric_names: bool,
    new_series_writer_threads: usize,
    new_series_series_per_writer: usize,
    new_series_shared_metric_names: bool,
    new_series_cached_missing_label_names: usize,
    query_saturation_bench: bool,
    query_saturation_workers: usize,
    query_saturation_series: usize,
    query_saturation_points_per_series: usize,
    query_saturation_series_per_query: usize,
    query_saturation_writer_points: usize,
    query_saturation_batch_size: usize,
    prime_all_series: bool,
    warmup_points: usize,
    measured_points: usize,
    batch_size: usize,
    out_of_order_max_seconds: i64,
    out_of_order_permille: u16,
    cross_partition_out_of_order_permille: u16,
    cross_partition_out_of_order_partitions: u16,
    max_active_partition_heads_per_series: usize,
    sparse_emit_permille: u16,
    short_lived_lifetime_steps: u64,
    retention_seconds: u64,
    partition_seconds: u64,
    step_seconds: i64,
    settle_millis: u64,
    seed: u64,
    fail_on_target: bool,
    metadata_selector_bench: bool,
    metadata_selector_series: usize,
    ingest_latency_bench: bool,
    ingest_latency_writer_threads: usize,
    ingest_latency_series_per_writer: usize,
    ingest_latency_duration_secs: u64,
    ingest_latency_memory_limit_bytes: usize,
    ingest_latency_chunk_points: usize,
    ingest_latency_batch_size: usize,
    wal_append_bench: bool,
    wal_append_duration_secs: u64,
    wal_append_batch_points: usize,
}

impl WorkloadConfig {
    fn from_env() -> Self {
        Self {
            resource_profile: parse_resource_profile_env(),
            memory_limit_override_bytes: parse_optional_usize_env("TSINK_MEMORY_LIMIT_BYTES"),
            maintenance_max_bytes_override: parse_optional_u64_env(
                "TSINK_MAINTENANCE_MAX_BYTES_PER_PASS",
            ),
            runs: parse_env("TSINK_BPP_RUNS", 5usize),
            active_series: parse_env("TSINK_ACTIVE_SERIES", SUITE_ACTIVE_SERIES_TARGET),
            shared_metric_names: parse_env_bool("TSINK_SHARED_METRIC_NAMES", false),
            new_series_writer_threads: parse_env("TSINK_NEW_SERIES_WRITERS", 0usize),
            new_series_series_per_writer: parse_env("TSINK_NEW_SERIES_PER_WRITER", 0usize),
            new_series_shared_metric_names: parse_env_bool(
                "TSINK_NEW_SERIES_SHARED_METRIC_NAMES",
                true,
            ),
            new_series_cached_missing_label_names: parse_env(
                "TSINK_NEW_SERIES_CACHED_MISSING_LABEL_NAMES",
                0usize,
            ),
            query_saturation_bench: parse_env_bool("TSINK_QUERY_SATURATION_BENCH", false),
            query_saturation_workers: parse_env("TSINK_QUERY_SATURATION_WORKERS", 0usize),
            query_saturation_series: parse_env("TSINK_QUERY_SATURATION_SERIES", 4_096usize),
            query_saturation_points_per_series: parse_env(
                "TSINK_QUERY_SATURATION_POINTS_PER_SERIES",
                64usize,
            ),
            query_saturation_series_per_query: parse_env(
                "TSINK_QUERY_SATURATION_SERIES_PER_QUERY",
                128usize,
            ),
            query_saturation_writer_points: parse_env(
                "TSINK_QUERY_SATURATION_WRITER_POINTS",
                100_000usize,
            ),
            query_saturation_batch_size: parse_env("TSINK_QUERY_SATURATION_BATCH_SIZE", 4_096usize),
            prime_all_series: parse_env_bool("TSINK_PRIME_ALL_SERIES", true),
            warmup_points: parse_env("TSINK_WARMUP_POINTS", 250_000usize),
            measured_points: parse_env("TSINK_MEASURE_POINTS", 1_000_000usize),
            batch_size: parse_env("TSINK_BATCH_SIZE", 4096usize),
            out_of_order_max_seconds: parse_env("TSINK_OOO_MAX_SECONDS", 300i64),
            out_of_order_permille: parse_env("TSINK_OOO_PERMILLE", 250u16),
            cross_partition_out_of_order_permille: parse_env(
                "TSINK_OOO_CROSS_PARTITION_PERMILLE",
                0u16,
            ),
            cross_partition_out_of_order_partitions: parse_env("TSINK_OOO_CROSS_PARTITIONS", 0u16),
            max_active_partition_heads_per_series: parse_env(
                "TSINK_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES",
                DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            ),
            sparse_emit_permille: parse_env("TSINK_SPARSE_EMIT_PERMILLE", 120u16),
            short_lived_lifetime_steps: parse_env("TSINK_SHORT_LIVED_LIFETIME_STEPS", 64u64),
            retention_seconds: parse_env("TSINK_RETENTION_SECONDS", 7 * 24 * 3600u64),
            partition_seconds: parse_env("TSINK_PARTITION_SECONDS", 3600u64),
            step_seconds: parse_env("TSINK_STEP_SECONDS", 10i64),
            settle_millis: parse_env("TSINK_SETTLE_MILLIS", 1500u64),
            seed: parse_env("TSINK_SEED", 0xC0DEC0DEu64),
            fail_on_target: parse_env_bool("TSINK_FAIL_ON_TARGET", false),
            metadata_selector_bench: parse_env_bool("TSINK_METADATA_SELECTOR_BENCH", false),
            metadata_selector_series: parse_env("TSINK_METADATA_SELECTOR_SERIES", 250_000usize),
            ingest_latency_bench: parse_env_bool("TSINK_INGEST_LATENCY_BENCH", false),
            ingest_latency_writer_threads: parse_env("TSINK_INGEST_LATENCY_WRITERS", 4usize),
            ingest_latency_series_per_writer: parse_env(
                "TSINK_INGEST_LATENCY_SERIES_PER_WRITER",
                64usize,
            ),
            ingest_latency_duration_secs: parse_env("TSINK_INGEST_LATENCY_DURATION_SECS", 5u64),
            ingest_latency_memory_limit_bytes: parse_env(
                "TSINK_INGEST_LATENCY_MEMORY_LIMIT_BYTES",
                256 * 1024usize,
            ),
            ingest_latency_chunk_points: parse_env("TSINK_INGEST_LATENCY_CHUNK_POINTS", 16usize),
            ingest_latency_batch_size: parse_env("TSINK_INGEST_LATENCY_BATCH_SIZE", 32usize),
            wal_append_bench: parse_env_bool("TSINK_WAL_APPEND_BENCH", false),
            wal_append_duration_secs: parse_env("TSINK_WAL_APPEND_DURATION_SECS", 5u64),
            wal_append_batch_points: parse_env("TSINK_WAL_APPEND_BATCH_POINTS", 32usize),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SeriesKind {
    Gauge,
    Counter,
    Sparse,
    ShortLived,
    Blob,
}

#[derive(Debug, Clone)]
struct SeriesSlices {
    gauges: Range<usize>,
    counters: Range<usize>,
    sparse: Range<usize>,
    short_lived: Range<usize>,
    blobs: Range<usize>,
}

impl SeriesSlices {
    fn from_active_series(active_series: usize) -> Self {
        let gauges = active_series * 60 / 100;
        let counters = active_series * 20 / 100;
        let sparse = active_series * 10 / 100;
        let short_lived = active_series * 5 / 100;
        let blobs = active_series.saturating_sub(gauges + counters + sparse + short_lived);

        let gauges_range = 0..gauges;
        let counters_range = gauges_range.end..(gauges_range.end + counters);
        let sparse_range = counters_range.end..(counters_range.end + sparse);
        let short_lived_range = sparse_range.end..(sparse_range.end + short_lived);
        let blobs_range = short_lived_range.end..(short_lived_range.end + blobs);

        Self {
            gauges: gauges_range,
            counters: counters_range,
            sparse: sparse_range,
            short_lived: short_lived_range,
            blobs: blobs_range,
        }
    }

    fn kind_for_series(&self, series_slot: usize) -> SeriesKind {
        if self.gauges.contains(&series_slot) {
            SeriesKind::Gauge
        } else if self.counters.contains(&series_slot) {
            SeriesKind::Counter
        } else if self.sparse.contains(&series_slot) {
            SeriesKind::Sparse
        } else if self.short_lived.contains(&series_slot) {
            SeriesKind::ShortLived
        } else {
            SeriesKind::Blob
        }
    }

    fn range_for_kind(&self, kind: SeriesKind) -> Range<usize> {
        match kind {
            SeriesKind::Gauge => self.gauges.clone(),
            SeriesKind::Counter => self.counters.clone(),
            SeriesKind::Sparse => self.sparse.clone(),
            SeriesKind::ShortLived => self.short_lived.clone(),
            SeriesKind::Blob => self.blobs.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct RunResult {
    selected_profile: ResourceProfileName,
    accounted_memory_limit_bytes: u64,
    local_disk_limit_bytes: u64,
    wal_limit_bytes: u64,
    cardinality_limit: u64,
    max_concurrent_writers: u64,
    max_concurrent_queries: u64,
    maintenance_max_items_per_pass: u64,
    maintenance_max_bytes_per_pass: u64,
    retained_points: u64,
    late_rejected_points: u64,
    max_post_write_accounted_memory_bytes: usize,
    post_settle_accounted_memory_bytes: usize,
    active_and_sealed_bytes: usize,
    registry_bytes: usize,
    metadata_cache_bytes: usize,
    wal_series_definition_cache_bytes: usize,
    write_transient_bytes: usize,
    persisted_index_bytes: usize,
    persisted_mmap_bytes: usize,
    tombstone_bytes: usize,
    local_disk_accounted_bytes_before_close: u64,
    persisted_bytes: u64,
    effective_bpp: f64,
    process_peak_rss_bytes_so_far: Option<u64>,
    data_path: PathBuf,
}

#[derive(Debug, Clone)]
struct NewSeriesRunResult {
    created_series: usize,
    elapsed: Duration,
    series_per_sec: f64,
    writer_p50_ms: f64,
    writer_p95_ms: f64,
    writer_p99_ms: f64,
    writer_max_ms: f64,
    process_peak_rss_bytes_so_far: Option<u64>,
}

#[derive(Debug, Clone)]
struct QuerySaturationRunResult {
    workers: usize,
    seeded_series: usize,
    seeded_points: usize,
    series_per_query: usize,
    queries_succeeded: usize,
    query_points_returned: usize,
    writer_points: usize,
    elapsed: Duration,
    query_p50_ms: f64,
    query_p95_ms: f64,
    query_max_ms: f64,
    writer_ms: f64,
    peak_active_queries: u64,
    peak_shared_reserved_memory_bytes: u64,
    concurrency_rejections: u64,
    active_queries_after: u64,
    shared_reserved_memory_bytes_after: u64,
    process_peak_rss_bytes_so_far: Option<u64>,
}

#[derive(Debug, Clone)]
struct MetadataSelectorRunResult {
    series: usize,
    exact_count: usize,
    exact_elapsed: Duration,
    missing_count: usize,
    missing_elapsed: Duration,
    broad_present_label_count: usize,
    broad_present_label_elapsed: Duration,
    broad_regex_count: usize,
    broad_regex_elapsed: Duration,
    broad_empty_regex_count: usize,
    broad_empty_regex_elapsed: Duration,
    broad_negative_regex_count: usize,
    broad_negative_regex_elapsed: Duration,
    shard_scoped_regex_count: usize,
    shard_scoped_regex_elapsed: Duration,
    shard_scoped_negative_count: usize,
    shard_scoped_negative_elapsed: Duration,
}

#[derive(Debug, Clone)]
struct IngestLatencyRunResult {
    batches: usize,
    points: usize,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    flush_pipeline_runs: u64,
    persist_runs: u64,
    pipeline_timeouts: u64,
    wal_resets: u64,
}

#[derive(Debug, Clone)]
struct WalAppendRunResult {
    frames: u64,
    points: u64,
    size_bytes: u64,
    segment_count: u64,
    elapsed: Duration,
    frames_per_sec: f64,
    points_per_sec: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct BenchInsertOutcome {
    accepted_points: u64,
    late_rejected_points: u64,
}

struct WorkloadGenerator {
    cfg: WorkloadConfig,
    slices: SeriesSlices,
    rng: XorShift64,
    run_id: usize,
    logical_step: u64,
    base_ts: i64,
    series_steps: Vec<u64>,
    counter_values: Vec<u64>,
    delayed_rows: BTreeMap<u64, Vec<Row>>,
}

impl WorkloadGenerator {
    fn new(cfg: WorkloadConfig, run_id: usize, base_ts: i64) -> Self {
        let active_series = cfg.active_series;
        Self {
            slices: SeriesSlices::from_active_series(cfg.active_series),
            rng: XorShift64::new(cfg.seed ^ (run_id as u64).wrapping_mul(0x9E3779B97F4A7C15)),
            cfg,
            run_id,
            logical_step: 0,
            base_ts,
            series_steps: vec![0; active_series],
            counter_values: (0..active_series)
                .map(|slot| (slot % 1024) as u64)
                .collect(),
            delayed_rows: BTreeMap::new(),
        }
    }

    fn prime_all_series<F>(&mut self, mut insert_rows: F) -> Result<u64, String>
    where
        F: FnMut(&[Row]) -> Result<BenchInsertOutcome, String>,
    {
        if !self.cfg.prime_all_series {
            return Ok(0);
        }

        let mut accepted = 0u64;
        let mut batch = Vec::with_capacity(self.cfg.batch_size);

        for series_slot in 0..self.cfg.active_series {
            let kind = self.slices.kind_for_series(series_slot);
            let row = self.build_row(kind, series_slot, false);
            batch.push(row);
            self.logical_step = self.logical_step.saturating_add(1);

            if batch.len() >= self.cfg.batch_size {
                accepted = accepted.saturating_add(insert_rows(&batch)?.accepted_points);
                batch.clear();
            }
        }

        if !batch.is_empty() {
            accepted = accepted.saturating_add(insert_rows(&batch)?.accepted_points);
        }

        Ok(accepted)
    }

    fn ingest_points<F>(
        &mut self,
        target_points: usize,
        measured_phase: bool,
        mut insert_rows: F,
    ) -> Result<BenchInsertOutcome, String>
    where
        F: FnMut(&[Row]) -> Result<BenchInsertOutcome, String>,
    {
        let mut generated = 0u64;
        let mut outcome = BenchInsertOutcome::default();
        let mut batch = Vec::with_capacity(self.cfg.batch_size);

        while generated < target_points as u64 || !self.delayed_rows.is_empty() {
            if generated < target_points as u64 {
                let kind = self.choose_kind();

                if matches!(kind, SeriesKind::Sparse)
                    && self.rng.next_u16_bounded(1000) >= self.cfg.sparse_emit_permille
                {
                    self.logical_step = self.logical_step.saturating_add(1);
                } else {
                    let range = self.slices.range_for_kind(kind);
                    let series_slot = sample_from_range(&mut self.rng, &range);
                    let row = self.build_row(kind, series_slot, measured_phase);
                    generated = generated.saturating_add(1);
                    self.enqueue_row(row, &mut batch);
                    self.logical_step = self.logical_step.saturating_add(1);
                }
            } else {
                self.logical_step = self.logical_step.saturating_add(1);
            }

            self.release_due_delayed_rows(&mut batch);

            if batch.len() >= self.cfg.batch_size {
                let batch_outcome = insert_rows(&batch)?;
                outcome.accepted_points = outcome
                    .accepted_points
                    .saturating_add(batch_outcome.accepted_points);
                outcome.late_rejected_points = outcome
                    .late_rejected_points
                    .saturating_add(batch_outcome.late_rejected_points);
                batch.clear();
            }
        }

        if !batch.is_empty() {
            let batch_outcome = insert_rows(&batch)?;
            outcome.accepted_points = outcome
                .accepted_points
                .saturating_add(batch_outcome.accepted_points);
            outcome.late_rejected_points = outcome
                .late_rejected_points
                .saturating_add(batch_outcome.late_rejected_points);
        }

        outcome.accepted_points = outcome.accepted_points.min(generated);
        Ok(outcome)
    }

    fn choose_kind(&mut self) -> SeriesKind {
        let bucket = self.rng.next_u16_bounded(WEIGHT_SCALE);
        if bucket < GAUGE_WEIGHT {
            SeriesKind::Gauge
        } else if bucket < GAUGE_WEIGHT + COUNTER_WEIGHT {
            SeriesKind::Counter
        } else if bucket < GAUGE_WEIGHT + COUNTER_WEIGHT + SPARSE_WEIGHT {
            SeriesKind::Sparse
        } else if bucket < GAUGE_WEIGHT + COUNTER_WEIGHT + SPARSE_WEIGHT + SHORT_LIVED_WEIGHT {
            SeriesKind::ShortLived
        } else {
            SeriesKind::Blob
        }
    }

    fn build_row(&mut self, kind: SeriesKind, series_slot: usize, measured_phase: bool) -> Row {
        let series_step = self.next_series_step(series_slot);
        let metric = metric_name(kind, self.cfg.shared_metric_names);
        let labels = self.labels_for(kind, series_slot, series_step);
        let timestamp = self.timestamp_for_step(series_step as i64);
        let value = self.value_for(kind, series_slot, series_step, measured_phase);

        Row::with_labels(metric, labels, DataPoint::new(timestamp, value))
    }

    fn next_series_step(&mut self, series_slot: usize) -> u64 {
        let step = self.series_steps[series_slot];
        self.series_steps[series_slot] = step.saturating_add(1);
        step
    }

    fn labels_for(&self, kind: SeriesKind, series_slot: usize, series_step: u64) -> Vec<Label> {
        let mut labels = vec![
            Label::new("tenant", format!("t{}", series_slot % 4096)),
            Label::new("host", format!("h{}", series_slot % 65536)),
            Label::new("series", format!("s{series_slot}")),
            Label::new("run", format!("r{}", self.run_id)),
        ];

        if matches!(kind, SeriesKind::ShortLived) {
            let generation = series_step / self.cfg.short_lived_lifetime_steps.max(1);
            labels.push(Label::new("gen", format!("g{generation}")));
        }

        if matches!(kind, SeriesKind::Blob) {
            labels.push(Label::new("lane", "blob"));
        }

        labels
    }

    fn timestamp_for_step(&mut self, logical_step: i64) -> i64 {
        self.base_ts + logical_step.saturating_mul(self.cfg.step_seconds.max(1))
    }

    fn value_for(
        &mut self,
        kind: SeriesKind,
        series_slot: usize,
        series_step: u64,
        measured_phase: bool,
    ) -> Value {
        match kind {
            SeriesKind::Gauge => {
                let baseline = (series_slot % 10_000) as i64;
                // Slow drift keeps gauge chunks highly compressible.
                let drift = (series_step / 4096) as i64;
                Value::I64(baseline + drift)
            }
            SeriesKind::Counter => {
                // Mostly monotonic with periodic resets.
                let counter = &mut self.counter_values[series_slot];
                if measured_phase && self.rng.next_u16_bounded(10_000) == 0 {
                    *counter = 0;
                } else {
                    *counter = counter.saturating_add(1);
                }
                Value::U64(*counter)
            }
            SeriesKind::Sparse => {
                let amplitude = if measured_phase { 7 } else { 5 };
                let event = self.rng.next_u16_bounded((amplitude + 1) as u16) as i64;
                Value::I64(event)
            }
            SeriesKind::ShortLived => {
                let phase = (series_step % 16) as i64;
                Value::I64(phase + (series_slot % 17) as i64)
            }
            SeriesKind::Blob => Value::String(blob_state(series_slot).to_string()),
        }
    }

    fn enqueue_row(&mut self, row: Row, batch: &mut Vec<Row>) {
        if let Some((min_delay_steps, max_delay_steps)) = self.cross_partition_delay_step_bounds() {
            let should_cross_partition = self.cfg.cross_partition_out_of_order_permille > 0
                && self.rng.next_u16_bounded(1000) < self.cfg.cross_partition_out_of_order_permille;
            if should_cross_partition {
                let delay_steps = min_delay_steps.saturating_add(
                    self.rng.next_u64_bounded(
                        max_delay_steps
                            .saturating_sub(min_delay_steps)
                            .saturating_add(1),
                    ),
                );
                let due_step = self.logical_step.saturating_add(delay_steps);
                self.delayed_rows.entry(due_step).or_default().push(row);
                return;
            }
        }

        let max_delay_steps = self.max_out_of_order_delay_steps();
        let should_delay = max_delay_steps > 0
            && self.cfg.out_of_order_permille > 0
            && self.rng.next_u16_bounded(1000) < self.cfg.out_of_order_permille;

        if should_delay {
            let delay_steps = self.rng.next_u64_bounded(max_delay_steps.saturating_add(1));
            if delay_steps > 0 {
                let due_step = self.logical_step.saturating_add(delay_steps);
                self.delayed_rows.entry(due_step).or_default().push(row);
                return;
            }
        }

        batch.push(row);
    }

    fn release_due_delayed_rows(&mut self, batch: &mut Vec<Row>) {
        while let Some((&due_step, _)) = self.delayed_rows.first_key_value() {
            if due_step > self.logical_step {
                break;
            }

            if let Some(mut due_rows) = self.delayed_rows.remove(&due_step) {
                batch.append(&mut due_rows);
            }
        }
    }

    fn max_out_of_order_delay_steps(&self) -> u64 {
        if self.cfg.out_of_order_max_seconds <= 0 {
            return 0;
        }

        let step_seconds = self.cfg.step_seconds.max(1) as u64;
        (self.cfg.out_of_order_max_seconds as u64) / step_seconds
    }

    fn cross_partition_delay_step_bounds(&self) -> Option<(u64, u64)> {
        if self.cfg.cross_partition_out_of_order_permille == 0
            || self.cfg.cross_partition_out_of_order_partitions == 0
        {
            return None;
        }

        let step_seconds = self.cfg.step_seconds.max(1) as u64;
        let partition_steps = self
            .cfg
            .partition_seconds
            .saturating_add(step_seconds.saturating_sub(1))
            / step_seconds;
        let partition_steps = partition_steps.max(1);
        let max_delay_steps =
            partition_steps.saturating_mul(self.cfg.cross_partition_out_of_order_partitions as u64);
        Some((partition_steps, max_delay_steps.max(partition_steps)))
    }
}

fn blob_state(series_slot: usize) -> &'static str {
    const STATES: [&str; 8] = ["ok", "warn", "error", "idle", "cold", "hot", "up", "down"];
    STATES[series_slot % STATES.len()]
}

fn metric_name(kind: SeriesKind, shared_metric_names: bool) -> &'static str {
    if shared_metric_names {
        return "suite_hot_metric";
    }

    match kind {
        SeriesKind::Gauge => "suite_gauge",
        SeriesKind::Counter => "suite_counter",
        SeriesKind::Sparse => "suite_sparse",
        SeriesKind::ShortLived => "suite_short_lived",
        SeriesKind::Blob => "suite_blob",
    }
}

fn sample_from_range(rng: &mut XorShift64, range: &Range<usize>) -> usize {
    if range.is_empty() {
        return 0;
    }
    range.start + rng.next_usize_bounded(range.end - range.start)
}

fn insert_rows_with_late_write_retry(
    storage: &dyn Storage,
    rows: &[Row],
) -> Result<BenchInsertOutcome, String> {
    match storage.insert_rows(rows) {
        Ok(()) => Ok(BenchInsertOutcome {
            accepted_points: rows.len() as u64,
            late_rejected_points: 0,
        }),
        Err(TsinkError::LateWritePartitionFanoutExceeded { .. }) if rows.len() > 1 => {
            let mid = rows.len() / 2;
            let left = insert_rows_with_late_write_retry(storage, &rows[..mid])?;
            let right = insert_rows_with_late_write_retry(storage, &rows[mid..])?;
            Ok(BenchInsertOutcome {
                accepted_points: left.accepted_points.saturating_add(right.accepted_points),
                late_rejected_points: left
                    .late_rejected_points
                    .saturating_add(right.late_rejected_points),
            })
        }
        Err(TsinkError::LateWritePartitionFanoutExceeded { .. }) => Ok(BenchInsertOutcome {
            accepted_points: 0,
            late_rejected_points: rows.len() as u64,
        }),
        Err(err) => {
            let snapshot = storage.observability_snapshot();
            Err(format!(
                "insert failed: {err}; memory={:?}; flush={:?}; storage_health={:?}",
                snapshot.memory, snapshot.flush, snapshot.health,
            ))
        }
    }
}

fn run_once(cfg: &WorkloadConfig, run_id: usize) -> Result<RunResult, String> {
    let keep_root = env::var("TSINK_BPP_KEEP_DIR").ok();
    let mut temp_dir: Option<TempDir> = None;
    let data_path = if let Some(root) = keep_root {
        PathBuf::from(root).join(format!("run-{run_id:02}"))
    } else {
        let created = TempDir::new().map_err(|e| format!("tempdir create failed: {e}"))?;
        let path = created.path().join(format!("run-{run_id:02}"));
        temp_dir = Some(created);
        path
    };
    fs::create_dir_all(&data_path)
        .map_err(|e| format!("failed to create data path {}: {e}", data_path.display()))?;

    let base_ts = current_unix_seconds().saturating_sub(3600);
    let mut storage_builder = StorageBuilder::new().with_resource_profile(cfg.resource_profile);
    if let Some(memory_limit_bytes) = cfg.memory_limit_override_bytes {
        storage_builder = storage_builder.with_memory_limit(memory_limit_bytes);
    }
    if let Some(maintenance_max_bytes) = cfg.maintenance_max_bytes_override {
        storage_builder =
            storage_builder.with_maintenance_max_bytes_per_pass(maintenance_max_bytes);
    }
    let storage = storage_builder
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(cfg.retention_seconds))
        .with_partition_duration(Duration::from_secs(cfg.partition_seconds))
        .with_max_active_partition_heads_per_series(cfg.max_active_partition_heads_per_series)
        .build()
        .map_err(|e| format!("storage build failed: {e}"))?;
    let resource_configuration = storage.resource_configuration_snapshot();
    if resource_configuration.selected_profile != cfg.resource_profile.name() {
        return Err(format!(
            "requested resource profile {} but storage selected {}",
            resource_profile_label(cfg.resource_profile),
            resource_profile_name_label(resource_configuration.selected_profile),
        ));
    }

    let mut generator = WorkloadGenerator::new(cfg.clone(), run_id, base_ts);

    let mut retained_points = 0u64;
    let mut late_rejected_points = 0u64;
    let mut max_post_write_accounted_memory_bytes =
        storage.observability_snapshot().memory.accounted_bytes;
    retained_points += generator.prime_all_series(|rows| {
        let result = insert_rows_with_late_write_retry(storage.as_ref(), rows);
        max_post_write_accounted_memory_bytes = max_post_write_accounted_memory_bytes
            .max(storage.observability_snapshot().memory.accounted_bytes);
        result.map_err(|e| format!("prime insert failed: {e}"))
    })?;

    let warmup_outcome = generator.ingest_points(cfg.warmup_points, false, |rows| {
        let result = insert_rows_with_late_write_retry(storage.as_ref(), rows);
        max_post_write_accounted_memory_bytes = max_post_write_accounted_memory_bytes
            .max(storage.observability_snapshot().memory.accounted_bytes);
        result.map_err(|e| format!("warmup insert failed: {e}"))
    })?;
    retained_points = retained_points.saturating_add(warmup_outcome.accepted_points);
    late_rejected_points = late_rejected_points.saturating_add(warmup_outcome.late_rejected_points);

    let measured_outcome = generator.ingest_points(cfg.measured_points, true, |rows| {
        let result = insert_rows_with_late_write_retry(storage.as_ref(), rows);
        max_post_write_accounted_memory_bytes = max_post_write_accounted_memory_bytes
            .max(storage.observability_snapshot().memory.accounted_bytes);
        result.map_err(|e| format!("measure insert failed: {e}"))
    })?;
    retained_points = retained_points.saturating_add(measured_outcome.accepted_points);
    late_rejected_points =
        late_rejected_points.saturating_add(measured_outcome.late_rejected_points);

    std::thread::sleep(Duration::from_millis(cfg.settle_millis));
    let observability = storage.observability_snapshot();
    max_post_write_accounted_memory_bytes =
        max_post_write_accounted_memory_bytes.max(observability.memory.accounted_bytes);
    let local_disk_accounted_bytes_before_close = observability
        .local_disk
        .as_ref()
        .map_or(0, |snapshot| snapshot.accounted_bytes);
    storage
        .close()
        .map_err(|e| format!("storage close failed: {e}"))?;

    let persisted_bytes = persisted_bytes(&data_path).map_err(|e| {
        format!(
            "failed to scan persisted bytes {}: {e}",
            data_path.display()
        )
    })?;
    let effective_bpp = if retained_points == 0 {
        0.0
    } else {
        persisted_bytes as f64 / retained_points as f64
    };

    // Keep temp dir alive until after bytes are collected.
    let _temp_guard = temp_dir;

    Ok(RunResult {
        selected_profile: resource_configuration.selected_profile,
        accounted_memory_limit_bytes: resource_configuration
            .resolved_limits
            .storage
            .accounted_memory_bytes
            .ok_or_else(|| "selected profile did not expose a finite memory limit".to_string())?,
        local_disk_limit_bytes: resource_configuration
            .resolved_limits
            .storage
            .local_disk_bytes
            .ok_or_else(|| {
                "selected profile did not expose a finite local-disk limit".to_string()
            })?,
        wal_limit_bytes: resource_configuration
            .resolved_limits
            .storage
            .wal_bytes
            .ok_or_else(|| "selected profile did not expose a finite WAL limit".to_string())?,
        cardinality_limit: resource_configuration
            .resolved_limits
            .storage
            .cardinality
            .ok_or_else(|| {
                "selected profile did not expose a finite cardinality limit".to_string()
            })?,
        max_concurrent_writers: resource_configuration
            .resolved_limits
            .storage
            .max_concurrent_writers
            .ok_or_else(|| "selected profile did not expose a finite writer limit".to_string())?,
        max_concurrent_queries: resource_configuration
            .resolved_limits
            .query
            .max_concurrent_queries
            .ok_or_else(|| {
                "selected profile did not expose a finite query concurrency limit".to_string()
            })?,
        maintenance_max_items_per_pass: resource_configuration
            .resolved_limits
            .maintenance_max_items_per_pass
            .ok_or_else(|| {
                "selected profile did not expose a finite maintenance item limit".to_string()
            })?,
        maintenance_max_bytes_per_pass: resource_configuration
            .resolved_limits
            .maintenance_max_bytes_per_pass
            .ok_or_else(|| {
                "selected profile did not expose a finite maintenance byte limit".to_string()
            })?,
        retained_points,
        late_rejected_points,
        max_post_write_accounted_memory_bytes,
        post_settle_accounted_memory_bytes: observability.memory.accounted_bytes,
        active_and_sealed_bytes: observability.memory.active_and_sealed_bytes,
        registry_bytes: observability.memory.registry_bytes,
        metadata_cache_bytes: observability.memory.metadata_cache_bytes,
        wal_series_definition_cache_bytes: observability.memory.wal_series_definition_cache_bytes,
        write_transient_bytes: observability.memory.write_transient_bytes,
        persisted_index_bytes: observability.memory.persisted_index_bytes,
        persisted_mmap_bytes: observability.memory.persisted_mmap_bytes,
        tombstone_bytes: observability.memory.tombstone_bytes,
        local_disk_accounted_bytes_before_close,
        persisted_bytes,
        effective_bpp,
        process_peak_rss_bytes_so_far: process_peak_rss_bytes_so_far(),
        data_path,
    })
}

fn prime_missing_label_cache(
    storage: &dyn Storage,
    cached_label_names: usize,
) -> Result<(), String> {
    if cached_label_names == 0 {
        return Ok(());
    }

    let metric = "bench_missing_cache_seed_metric";
    let write_ts = current_unix_seconds().saturating_sub(1);
    let rows = (0..cached_label_names)
        .map(|idx| {
            Row::with_labels(
                metric,
                vec![
                    Label::new("seed", format!("s{idx}")),
                    Label::new(format!("cached_label_{idx:04}"), "present"),
                ],
                DataPoint::new(write_ts, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage
        .insert_rows(&rows)
        .map_err(|e| format!("missing-label cache seed insert failed: {e}"))?;

    for idx in 0..cached_label_names {
        let selection = SeriesSelection::new()
            .with_metric(metric)
            .with_matcher(SeriesMatcher::equal(format!("cached_label_{idx:04}"), ""));
        storage
            .select_series(&selection)
            .map_err(|e| format!("missing-label cache seed query failed: {e}"))?;
    }

    Ok(())
}

fn run_parallel_new_series_once(
    cfg: &WorkloadConfig,
    run_id: usize,
) -> Result<NewSeriesRunResult, String> {
    let keep_root = env::var("TSINK_BPP_KEEP_DIR").ok();
    let mut temp_dir: Option<TempDir> = None;
    let data_path = if let Some(root) = keep_root {
        PathBuf::from(root).join(format!("new-series-run-{run_id:02}"))
    } else {
        let created = TempDir::new().map_err(|e| format!("tempdir create failed: {e}"))?;
        let path = created.path().join(format!("new-series-run-{run_id:02}"));
        temp_dir = Some(created);
        path
    };
    fs::create_dir_all(&data_path)
        .map_err(|e| format!("failed to create data path {}: {e}", data_path.display()))?;

    let storage = StorageBuilder::new()
        .with_resource_profile(cfg.resource_profile)
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(cfg.retention_seconds))
        .with_partition_duration(Duration::from_secs(cfg.partition_seconds))
        .with_max_active_partition_heads_per_series(cfg.max_active_partition_heads_per_series)
        .with_max_writers(cfg.new_series_writer_threads.max(1))
        .build()
        .map_err(|e| format!("storage build failed: {e}"))?;

    prime_missing_label_cache(storage.as_ref(), cfg.new_series_cached_missing_label_names)?;

    let writer_count = cfg.new_series_writer_threads.max(1);
    let series_per_writer = cfg.new_series_series_per_writer;
    let write_ts = current_unix_seconds();
    let start_barrier = Arc::new(Barrier::new(writer_count + 1));
    let mut handles = Vec::with_capacity(writer_count);

    for writer_id in 0..writer_count {
        let writer_storage = Arc::clone(&storage);
        let writer_start = Arc::clone(&start_barrier);
        let shared_metric_names = cfg.new_series_shared_metric_names;
        handles.push(thread::spawn(move || -> Result<f64, String> {
            let rows = (0..series_per_writer)
                .map(|series_idx| {
                    let metric = if shared_metric_names {
                        "bench_new_series_hot_metric".to_string()
                    } else {
                        format!("bench_new_series_metric_{writer_id}")
                    };
                    Row::with_labels(
                        metric.as_str(),
                        vec![
                            Label::new("writer", format!("w{writer_id}")),
                            Label::new("series", format!("s{series_idx}")),
                            Label::new("run", format!("r{run_id}")),
                        ],
                        DataPoint::new(write_ts, writer_id as f64),
                    )
                })
                .collect::<Vec<_>>();
            writer_start.wait();
            let started = Instant::now();
            writer_storage
                .insert_rows(&rows)
                .map_err(|e| format!("parallel new-series insert failed: {e}"))?;
            Ok(started.elapsed().as_secs_f64() * 1000.0)
        }));
    }

    start_barrier.wait();
    let started = Instant::now();
    let mut writer_latencies_ms = Vec::with_capacity(writer_count);
    for handle in handles {
        writer_latencies_ms.push(
            handle
                .join()
                .map_err(|_| "parallel new-series worker panicked".to_string())??,
        );
    }
    let elapsed = started.elapsed();

    storage
        .close()
        .map_err(|e| format!("storage close failed: {e}"))?;

    let _temp_guard = temp_dir;
    let created_series = writer_count.saturating_mul(series_per_writer);
    let series_per_sec = if elapsed.is_zero() {
        created_series as f64
    } else {
        created_series as f64 / elapsed.as_secs_f64()
    };

    Ok(NewSeriesRunResult {
        created_series,
        elapsed,
        series_per_sec,
        writer_p50_ms: percentile(writer_latencies_ms.clone(), 0.50),
        writer_p95_ms: percentile(writer_latencies_ms.clone(), 0.95),
        writer_p99_ms: percentile(writer_latencies_ms.clone(), 0.99),
        writer_max_ms: writer_latencies_ms
            .iter()
            .copied()
            .fold(0.0f64, |current, value| current.max(value)),
        process_peak_rss_bytes_so_far: process_peak_rss_bytes_so_far(),
    })
}

fn wait_for_query_saturation_start(gate: &(Mutex<bool>, Condvar)) -> Result<(), String> {
    let (started, wake) = gate;
    let mut started = started
        .lock()
        .map_err(|_| "query saturation start gate was poisoned".to_string())?;
    while !*started {
        started = wake
            .wait(started)
            .map_err(|_| "query saturation start gate was poisoned".to_string())?;
    }
    Ok(())
}

fn release_query_saturation_start(gate: &(Mutex<bool>, Condvar)) -> Result<(), String> {
    let (started, wake) = gate;
    let mut started = started
        .lock()
        .map_err(|_| "query saturation start gate was poisoned".to_string())?;
    *started = true;
    wake.notify_all();
    Ok(())
}

fn run_query_saturation_once(
    cfg: &WorkloadConfig,
    run_id: usize,
) -> Result<QuerySaturationRunResult, String> {
    if cfg.query_saturation_series == 0
        || cfg.query_saturation_points_per_series == 0
        || cfg.query_saturation_series_per_query == 0
        || cfg.query_saturation_writer_points == 0
        || cfg.query_saturation_batch_size == 0
    {
        return Err("query saturation dimensions must all be greater than zero".to_string());
    }
    if cfg.query_saturation_series_per_query > cfg.query_saturation_series {
        return Err(format!(
            "query saturation series_per_query {} exceeds seeded series {}",
            cfg.query_saturation_series_per_query, cfg.query_saturation_series
        ));
    }

    let keep_root = env::var("TSINK_BPP_KEEP_DIR").ok();
    let mut temp_dir: Option<TempDir> = None;
    let data_path = if let Some(root) = keep_root {
        PathBuf::from(root).join(format!("query-saturation-run-{run_id:02}"))
    } else {
        let created = TempDir::new().map_err(|e| format!("tempdir create failed: {e}"))?;
        let path = created
            .path()
            .join(format!("query-saturation-run-{run_id:02}"));
        temp_dir = Some(created);
        path
    };
    fs::create_dir_all(&data_path)
        .map_err(|e| format!("failed to create data path {}: {e}", data_path.display()))?;

    let mut storage_builder = StorageBuilder::new().with_resource_profile(cfg.resource_profile);
    if let Some(memory_limit_bytes) = cfg.memory_limit_override_bytes {
        storage_builder = storage_builder.with_memory_limit(memory_limit_bytes);
    }
    if let Some(maintenance_max_bytes) = cfg.maintenance_max_bytes_override {
        storage_builder =
            storage_builder.with_maintenance_max_bytes_per_pass(maintenance_max_bytes);
    }
    let storage = storage_builder
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(cfg.retention_seconds))
        .with_partition_duration(Duration::from_secs(cfg.partition_seconds))
        .with_max_active_partition_heads_per_series(cfg.max_active_partition_heads_per_series)
        .build()
        .map_err(|e| format!("storage build failed: {e}"))?;
    let resource_configuration = storage.resource_configuration_snapshot();
    let profile_query_workers = resource_configuration
        .resolved_limits
        .query
        .max_concurrent_queries
        .ok_or_else(|| "selected profile has no finite query concurrency limit".to_string())?
        as usize;
    let workers = if cfg.query_saturation_workers == 0 {
        profile_query_workers
    } else {
        cfg.query_saturation_workers
    };
    if workers != profile_query_workers {
        return Err(format!(
            "query saturation requires exactly the profile limit ({profile_query_workers}) workers, got {workers}"
        ));
    }

    let seeded_points = cfg
        .query_saturation_series
        .checked_mul(cfg.query_saturation_points_per_series)
        .ok_or_else(|| "query saturation seed point count overflowed".to_string())?;
    let base_ts = current_unix_seconds()
        .saturating_sub(cfg.query_saturation_points_per_series as i64)
        .saturating_sub(60);
    let query_metric = "bench_query_saturation";
    let mut seed_batch = Vec::with_capacity(cfg.query_saturation_batch_size);
    for series_idx in 0..cfg.query_saturation_series {
        for point_idx in 0..cfg.query_saturation_points_per_series {
            seed_batch.push(Row::with_labels(
                query_metric,
                vec![
                    Label::new("series", format!("s{series_idx}")),
                    Label::new("run", format!("r{run_id}")),
                ],
                DataPoint::new(base_ts.saturating_add(point_idx as i64), point_idx as f64),
            ));
            if seed_batch.len() == cfg.query_saturation_batch_size {
                storage
                    .insert_rows(&seed_batch)
                    .map_err(|e| format!("query saturation seed insert failed: {e}"))?;
                seed_batch.clear();
            }
        }
    }
    if !seed_batch.is_empty() {
        storage
            .insert_rows(&seed_batch)
            .map_err(|e| format!("query saturation seed insert failed: {e}"))?;
    }

    let start_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let mut query_handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let worker_storage = Arc::clone(&storage);
        let worker_gate = Arc::clone(&start_gate);
        let worker_admitted = admitted_tx.clone();
        let series = (0..cfg.query_saturation_series_per_query)
            .map(|offset| {
                let series_idx = (worker_id * cfg.query_saturation_series_per_query + offset)
                    % cfg.query_saturation_series;
                MetricSeries {
                    name: query_metric.to_string(),
                    labels: vec![
                        Label::new("series", format!("s{series_idx}")),
                        Label::new("run", format!("r{run_id}")),
                    ],
                }
            })
            .collect::<Vec<_>>();
        let query_end = base_ts
            .saturating_add(cfg.query_saturation_points_per_series as i64)
            .saturating_add(1);
        query_handles.push(thread::spawn(move || -> Result<(f64, usize), String> {
            let execution = match worker_storage
                .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            {
                Ok(Some(execution)) => {
                    let _ = worker_admitted.send(Ok(()));
                    execution
                }
                Ok(None) => {
                    let message = "built-in storage did not expose query admission".to_string();
                    let _ = worker_admitted.send(Err(message.clone()));
                    return Err(message);
                }
                Err(error) => {
                    let message = format!("query worker admission failed: {error}");
                    let _ = worker_admitted.send(Err(message.clone()));
                    return Err(message);
                }
            };
            wait_for_query_saturation_start(worker_gate.as_ref())?;
            let started = Instant::now();
            let result = worker_storage
                .select_many_with_execution(&series, base_ts, query_end, &execution)
                .map_err(|e| format!("query saturation select failed: {e}"))?;
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            let returned_points = result.iter().map(|item| item.points.len()).sum();
            Ok((elapsed_ms, returned_points))
        }));
    }
    drop(admitted_tx);

    let mut admission_error = None;
    for _ in 0..workers {
        match admitted_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                admission_error = Some(error);
                break;
            }
            Err(error) => {
                admission_error = Some(format!("timed out waiting for query admission: {error}"));
                break;
            }
        }
    }
    if let Some(error) = admission_error {
        let _ = release_query_saturation_start(start_gate.as_ref());
        for handle in query_handles {
            let _ = handle.join();
        }
        let _ = storage.close();
        return Err(error);
    }

    let saturated_snapshot = storage.query_budget_snapshot();
    if saturated_snapshot.active_queries != workers as u64 {
        let _ = release_query_saturation_start(start_gate.as_ref());
        for handle in query_handles {
            let _ = handle.join();
        }
        let _ = storage.close();
        return Err(format!(
            "expected {workers} active queries before release, observed {}",
            saturated_snapshot.active_queries
        ));
    }
    let saturation_rejected = matches!(
        storage.begin_query_execution(
            QueryWorkLimits::default(),
            QueryCancellationToken::new()
        ),
        Err(TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded)))
            if exceeded.reason == QueryLimitReason::ConcurrentQueries
    );
    if !saturation_rejected {
        let _ = release_query_saturation_start(start_gate.as_ref());
        for handle in query_handles {
            let _ = handle.join();
        }
        let _ = storage.close();
        return Err(
            "N+1 query admission did not return the structured concurrency limit".to_string(),
        );
    }

    let writer_storage = Arc::clone(&storage);
    let writer_gate = Arc::clone(&start_gate);
    let writer_points = cfg.query_saturation_writer_points;
    let writer_batch_size = cfg.query_saturation_batch_size;
    let writer_series = cfg.query_saturation_series.clamp(1, 1_024);
    let writer_timestamp_offset = cfg.query_saturation_points_per_series as i64;
    let writer_handle = thread::spawn(move || -> Result<(f64, usize), String> {
        wait_for_query_saturation_start(writer_gate.as_ref())?;
        let started = Instant::now();
        let mut written = 0usize;
        let mut batch = Vec::with_capacity(writer_batch_size);
        while written < writer_points {
            let take = (writer_points - written).min(writer_batch_size);
            batch.clear();
            for offset in 0..take {
                let point_idx = written + offset;
                let series_idx = point_idx % writer_series;
                let series_step = point_idx / writer_series;
                batch.push(Row::with_labels(
                    "bench_query_pressure_writer",
                    vec![
                        Label::new("series", format!("s{series_idx}")),
                        Label::new("run", format!("r{run_id}")),
                    ],
                    DataPoint::new(
                        base_ts
                            .saturating_add(writer_timestamp_offset)
                            .saturating_add(series_step as i64)
                            .saturating_add(1),
                        point_idx as f64,
                    ),
                ));
            }
            writer_storage
                .insert_rows(&batch)
                .map_err(|e| format!("combined query-pressure insert failed: {e}"))?;
            written = written.saturating_add(take);
        }
        Ok((started.elapsed().as_secs_f64() * 1000.0, written))
    });

    let started = Instant::now();
    release_query_saturation_start(start_gate.as_ref())?;
    let mut query_latencies_ms = Vec::with_capacity(workers);
    let mut query_points_returned = 0usize;
    let mut query_error = None;
    for handle in query_handles {
        match handle.join() {
            Ok(Ok((latency_ms, returned_points))) => {
                query_latencies_ms.push(latency_ms);
                query_points_returned = query_points_returned.saturating_add(returned_points);
            }
            Ok(Err(error)) => {
                query_error.get_or_insert(error);
            }
            Err(_) => {
                query_error.get_or_insert_with(|| "query saturation worker panicked".to_string());
            }
        };
    }
    let (writer_ms, written_points) = writer_handle
        .join()
        .map_err(|_| "query-pressure writer panicked".to_string())??;
    let elapsed = started.elapsed();
    if let Some(error) = query_error {
        let _ = storage.close();
        return Err(error);
    }

    let query_snapshot = storage.query_budget_snapshot();
    if query_snapshot.active_queries != 0 || query_snapshot.shared_reserved_memory_bytes != 0 {
        let _ = storage.close();
        return Err(format!(
            "query resources leaked after saturation: active={} reserved_bytes={}",
            query_snapshot.active_queries, query_snapshot.shared_reserved_memory_bytes
        ));
    }
    storage
        .close()
        .map_err(|e| format!("storage close failed: {e}"))?;
    let _temp_guard = temp_dir;

    Ok(QuerySaturationRunResult {
        workers,
        seeded_series: cfg.query_saturation_series,
        seeded_points,
        series_per_query: cfg.query_saturation_series_per_query,
        queries_succeeded: query_latencies_ms.len(),
        query_points_returned,
        writer_points: written_points,
        elapsed,
        query_p50_ms: percentile(query_latencies_ms.clone(), 0.50),
        query_p95_ms: percentile(query_latencies_ms.clone(), 0.95),
        query_max_ms: query_latencies_ms
            .iter()
            .copied()
            .fold(0.0f64, |current, value| current.max(value)),
        writer_ms,
        peak_active_queries: query_snapshot.peak_active_queries,
        peak_shared_reserved_memory_bytes: query_snapshot.peak_shared_reserved_memory_bytes,
        concurrency_rejections: query_snapshot.concurrency_rejections_total,
        active_queries_after: query_snapshot.active_queries,
        shared_reserved_memory_bytes_after: query_snapshot.shared_reserved_memory_bytes,
        process_peak_rss_bytes_so_far: process_peak_rss_bytes_so_far(),
    })
}

fn run_ingest_latency_once(
    cfg: &WorkloadConfig,
    run_id: usize,
) -> Result<IngestLatencyRunResult, String> {
    let keep_root = env::var("TSINK_BPP_KEEP_DIR").ok();
    let mut temp_dir: Option<TempDir> = None;
    let data_path = if let Some(root) = keep_root {
        PathBuf::from(root).join(format!("ingest-latency-run-{run_id:02}"))
    } else {
        let created = TempDir::new().map_err(|e| format!("tempdir create failed: {e}"))?;
        let path = created
            .path()
            .join(format!("ingest-latency-run-{run_id:02}"));
        temp_dir = Some(created);
        path
    };
    fs::create_dir_all(&data_path)
        .map_err(|e| format!("failed to create data path {}: {e}", data_path.display()))?;

    let storage = StorageBuilder::new()
        .with_resource_profile(cfg.resource_profile)
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(cfg.retention_seconds))
        .with_partition_duration(Duration::from_secs(cfg.partition_seconds))
        .with_chunk_points(cfg.ingest_latency_chunk_points)
        .with_max_active_partition_heads_per_series(cfg.max_active_partition_heads_per_series)
        .with_max_writers(cfg.ingest_latency_writer_threads.max(1))
        .with_write_timeout(Duration::from_secs(2))
        .with_memory_limit(cfg.ingest_latency_memory_limit_bytes)
        .build()
        .map_err(|e| format!("storage build failed: {e}"))?;

    let writer_count = cfg.ingest_latency_writer_threads.max(1);
    let batch_size = cfg.ingest_latency_batch_size.max(1);
    let series_per_writer = cfg.ingest_latency_series_per_writer.max(1);
    let duration = Duration::from_secs(cfg.ingest_latency_duration_secs.max(1));
    let start_barrier = Arc::new(Barrier::new(writer_count + 1));
    let mut handles = Vec::with_capacity(writer_count);

    for writer_id in 0..writer_count {
        let writer_storage = Arc::clone(&storage);
        let writer_start = Arc::clone(&start_barrier);
        handles.push(thread::spawn(
            move || -> Result<(Vec<f64>, usize), String> {
                let metric = "bench_ingest_latency_hot_metric";
                let mut latencies_ms = Vec::new();
                let mut total_points = 0usize;
                let mut next_ts = current_unix_seconds();

                writer_start.wait();
                let deadline = Instant::now() + duration;
                while Instant::now() < deadline {
                    let rows = (0..batch_size)
                        .map(|batch_idx| {
                            let series_slot = (total_points + batch_idx) % series_per_writer;
                            Row::with_labels(
                                metric,
                                vec![
                                    Label::new("writer", format!("w{writer_id}")),
                                    Label::new("series", format!("s{series_slot}")),
                                    Label::new("run", format!("r{run_id}")),
                                ],
                                DataPoint::new(next_ts, total_points as f64),
                            )
                        })
                        .collect::<Vec<_>>();
                    let started = Instant::now();
                    writer_storage
                        .insert_rows(&rows)
                        .map_err(|e| format!("ingest latency insert failed: {e}"))?;
                    latencies_ms.push(started.elapsed().as_secs_f64() * 1000.0);
                    total_points = total_points.saturating_add(rows.len());
                    next_ts = next_ts.saturating_add(1);
                }

                Ok((latencies_ms, total_points))
            },
        ));
    }

    start_barrier.wait();

    let mut latencies_ms = Vec::new();
    let mut total_points = 0usize;
    for handle in handles {
        let (writer_latencies, writer_points) = handle
            .join()
            .map_err(|_| "ingest latency worker panicked".to_string())??;
        latencies_ms.extend(writer_latencies);
        total_points = total_points.saturating_add(writer_points);
    }

    if latencies_ms.is_empty() {
        return Err("ingest latency benchmark recorded no batches".to_string());
    }

    let snapshot = storage.observability_snapshot();
    storage
        .close()
        .map_err(|e| format!("storage close failed: {e}"))?;

    let _temp_guard = temp_dir;
    Ok(IngestLatencyRunResult {
        batches: latencies_ms.len(),
        points: total_points,
        p50_ms: percentile(latencies_ms.clone(), 0.50),
        p95_ms: percentile(latencies_ms.clone(), 0.95),
        p99_ms: percentile(latencies_ms.clone(), 0.99),
        max_ms: latencies_ms
            .iter()
            .copied()
            .fold(0.0f64, |current, value| current.max(value)),
        flush_pipeline_runs: snapshot.flush.pipeline_runs_total,
        persist_runs: snapshot.flush.persist_runs_total,
        pipeline_timeouts: snapshot.flush.pipeline_timeout_total,
        wal_resets: snapshot.wal.resets_total,
    })
}

fn run_wal_append_once(cfg: &WorkloadConfig, run_id: usize) -> Result<WalAppendRunResult, String> {
    let keep_root = env::var("TSINK_BPP_KEEP_DIR").ok();
    let mut temp_dir: Option<TempDir> = None;
    let data_path = if let Some(root) = keep_root {
        PathBuf::from(root).join(format!("wal-append-run-{run_id:02}"))
    } else {
        let created = TempDir::new().map_err(|e| format!("tempdir create failed: {e}"))?;
        let path = created.path().join(format!("wal-append-run-{run_id:02}"));
        temp_dir = Some(created);
        path
    };
    let wal_path = data_path.join("wal");
    fs::create_dir_all(&wal_path)
        .map_err(|e| format!("failed to create WAL path {}: {e}", wal_path.display()))?;

    let wal = FramedWal::open(&wal_path, WalSyncMode::Periodic(Duration::from_secs(3600)))
        .map_err(|e| format!("wal open failed: {e}"))?;

    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: 1,
        metric: "bench_wal_append_metric".to_string(),
        labels: vec![Label::new("run", format!("r{run_id}"))],
    })
    .map_err(|e| format!("wal series definition append failed: {e}"))?;

    let batch_points = cfg.wal_append_batch_points.max(1);
    let points = (0..batch_points)
        .map(|idx| ChunkPoint {
            ts: idx as i64,
            value: Value::F64(idx as f64),
        })
        .collect::<Vec<_>>();
    let batch = SamplesBatchFrame::from_points(1, ValueLane::Numeric, &points)
        .map_err(|e| format!("wal batch build failed: {e}"))?;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(cfg.wal_append_duration_secs.max(1));
    let mut frames = 0u64;
    let mut total_points = 0u64;
    while Instant::now() < deadline {
        wal.append_samples(std::slice::from_ref(&batch))
            .map_err(|e| format!("wal samples append failed: {e}"))?;
        frames = frames.saturating_add(1);
        total_points = total_points.saturating_add(batch.point_count as u64);
    }

    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let size_bytes = wal
        .total_size_bytes()
        .map_err(|e| format!("wal size read failed: {e}"))?;
    let segment_count = wal
        .segment_count()
        .map_err(|e| format!("wal segment count read failed: {e}"))?;

    let _temp_guard = temp_dir;
    Ok(WalAppendRunResult {
        frames,
        points: total_points,
        size_bytes,
        segment_count,
        elapsed,
        frames_per_sec: frames as f64 / elapsed_secs,
        points_per_sec: total_points as f64 / elapsed_secs,
    })
}

fn run_metadata_selector_once(
    cfg: &WorkloadConfig,
    run_id: usize,
) -> Result<MetadataSelectorRunResult, String> {
    let keep_root = env::var("TSINK_BPP_KEEP_DIR").ok();
    let mut temp_dir: Option<TempDir> = None;
    let data_path = if let Some(root) = keep_root {
        PathBuf::from(root).join(format!("metadata-selector-run-{run_id:02}"))
    } else {
        let created = TempDir::new().map_err(|e| format!("tempdir create failed: {e}"))?;
        let path = created
            .path()
            .join(format!("metadata-selector-run-{run_id:02}"));
        temp_dir = Some(created);
        path
    };
    fs::create_dir_all(&data_path)
        .map_err(|e| format!("failed to create data path {}: {e}", data_path.display()))?;

    let storage = StorageBuilder::new()
        .with_resource_profile(cfg.resource_profile)
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(cfg.retention_seconds))
        .with_partition_duration(Duration::from_secs(cfg.partition_seconds))
        .with_max_active_partition_heads_per_series(cfg.max_active_partition_heads_per_series)
        .with_metadata_shard_count(8)
        .with_max_writers(2)
        .build()
        .map_err(|e| format!("storage build failed: {e}"))?;

    let write_ts = current_unix_seconds();
    let metadata_shard_count = 8u32;
    let mut shard_counts = vec![0usize; metadata_shard_count as usize];
    let mut rows = Vec::with_capacity(cfg.metadata_selector_series);
    for series_idx in 0..cfg.metadata_selector_series {
        let mut labels = vec![
            Label::new("instance", format!("instance-{series_idx:06}")),
            Label::new("tenant", format!("tenant-{:03}", series_idx % 256)),
            Label::new("rack", format!("rack-{:03}", series_idx % 128)),
        ];
        if series_idx % 3 != 0 {
            labels.push(Label::new("job", format!("job-{:03}", series_idx % 96)));
        }
        let shard = (stable_series_identity_hash("cpu", &labels) % u64::from(metadata_shard_count))
            as usize;
        shard_counts[shard] = shard_counts[shard].saturating_add(1);
        rows.push(Row::with_labels(
            "cpu",
            labels,
            DataPoint::new(write_ts, series_idx as f64),
        ));
    }
    storage
        .insert_rows(&rows)
        .map_err(|e| format!("metadata selector insert failed: {e}"))?;

    let exact = SeriesSelection::new()
        .with_metric("cpu")
        .with_matcher(SeriesMatcher::equal("instance", "instance-000123"));
    let missing = SeriesSelection::new().with_matcher(SeriesMatcher::equal("job", ""));
    let broad_present_label =
        SeriesSelection::new().with_matcher(SeriesMatcher::not_equal("job", ""));
    let broad_regex = SeriesSelection::new()
        .with_matcher(SeriesMatcher::regex_match("job", "job-(008|009|010|011)"));
    let broad_empty_regex =
        SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("job", ".*"));
    let broad_negative_regex =
        SeriesSelection::new().with_matcher(SeriesMatcher::regex_no_match("job", "job-(003|004)"));
    let target_shard = shard_counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| *count)
        .map(|(idx, _)| idx as u32)
        .unwrap_or(0);
    let shard_scope = MetadataShardScope::new(metadata_shard_count, vec![target_shard]);

    storage
        .select_series(&broad_regex)
        .map_err(|e| format!("metadata selector broad regex warmup failed: {e}"))?;
    storage
        .select_series(&exact)
        .map_err(|e| format!("metadata selector exact warmup failed: {e}"))?;
    storage
        .select_series(&missing)
        .map_err(|e| format!("metadata selector missing warmup failed: {e}"))?;
    storage
        .select_series(&broad_present_label)
        .map_err(|e| format!("metadata selector broad present-label warmup failed: {e}"))?;
    storage
        .select_series(&broad_empty_regex)
        .map_err(|e| format!("metadata selector empty regex warmup failed: {e}"))?;
    storage
        .select_series(&broad_negative_regex)
        .map_err(|e| format!("metadata selector negative regex warmup failed: {e}"))?;
    storage
        .select_series_in_shards(&broad_regex, &shard_scope)
        .map_err(|e| format!("metadata selector shard regex warmup failed: {e}"))?;
    storage
        .select_series_in_shards(&broad_negative_regex, &shard_scope)
        .map_err(|e| format!("metadata selector shard negative warmup failed: {e}"))?;

    let started = Instant::now();
    let exact_count = storage
        .select_series(&exact)
        .map_err(|e| format!("metadata selector exact failed: {e}"))?
        .len();
    let exact_elapsed = started.elapsed();

    let started = Instant::now();
    let missing_count = storage
        .select_series(&missing)
        .map_err(|e| format!("metadata selector missing failed: {e}"))?
        .len();
    let missing_elapsed = started.elapsed();

    let started = Instant::now();
    let broad_present_label_count = storage
        .select_series(&broad_present_label)
        .map_err(|e| format!("metadata selector broad present-label failed: {e}"))?
        .len();
    let broad_present_label_elapsed = started.elapsed();

    let started = Instant::now();
    let broad_regex_count = storage
        .select_series(&broad_regex)
        .map_err(|e| format!("metadata selector broad regex failed: {e}"))?
        .len();
    let broad_regex_elapsed = started.elapsed();

    let started = Instant::now();
    let broad_empty_regex_count = storage
        .select_series(&broad_empty_regex)
        .map_err(|e| format!("metadata selector empty regex failed: {e}"))?
        .len();
    let broad_empty_regex_elapsed = started.elapsed();

    let started = Instant::now();
    let broad_negative_regex_count = storage
        .select_series(&broad_negative_regex)
        .map_err(|e| format!("metadata selector negative regex failed: {e}"))?
        .len();
    let broad_negative_regex_elapsed = started.elapsed();

    let started = Instant::now();
    let shard_scoped_regex_count = storage
        .select_series_in_shards(&broad_regex, &shard_scope)
        .map_err(|e| format!("metadata selector shard regex failed: {e}"))?
        .len();
    let shard_scoped_regex_elapsed = started.elapsed();

    let started = Instant::now();
    let shard_scoped_negative_count = storage
        .select_series_in_shards(&broad_negative_regex, &shard_scope)
        .map_err(|e| format!("metadata selector shard negative failed: {e}"))?
        .len();
    let shard_scoped_negative_elapsed = started.elapsed();

    storage
        .close()
        .map_err(|e| format!("storage close failed: {e}"))?;

    let _temp_guard = temp_dir;
    Ok(MetadataSelectorRunResult {
        series: cfg.metadata_selector_series,
        exact_count,
        exact_elapsed,
        missing_count,
        missing_elapsed,
        broad_present_label_count,
        broad_present_label_elapsed,
        broad_regex_count,
        broad_regex_elapsed,
        broad_empty_regex_count,
        broad_empty_regex_elapsed,
        broad_negative_regex_count,
        broad_negative_regex_elapsed,
        shard_scoped_regex_count,
        shard_scoped_regex_elapsed,
        shard_scoped_negative_count,
        shard_scoped_negative_elapsed,
    })
}

fn persisted_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let entry_path = entry.path();

        if metadata.is_dir() {
            total = total.saturating_add(persisted_bytes(&entry_path)?);
            continue;
        }

        if entry_path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"))
        {
            continue;
        }

        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

#[cfg(unix)]
fn process_peak_rss_bytes_so_far() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` points to writable storage for one `rusage`, and `RUSAGE_SELF` asks the
    // kernel only for this benchmark process. A successful call initializes the complete value.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: `getrusage` returned success and therefore initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    let raw = u64::try_from(usage.ru_maxrss).ok()?;
    if cfg!(target_vendor = "apple") {
        // Darwin reports `ru_maxrss` in bytes.
        Some(raw)
    } else {
        // Linux and the other supported Unix targets report `ru_maxrss` in KiB.
        Some(raw.saturating_mul(1_024))
    }
}

#[cfg(not(unix))]
fn process_peak_rss_bytes_so_far() -> Option<u64> {
    None
}

fn current_unix_seconds() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

fn optional_u64_text(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".to_string(), |value| value.to_string())
}

fn percentile(mut values: Vec<f64>, p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    values.sort_by(|a, b| a.total_cmp(b));
    let rank = ((values.len() - 1) as f64 * p).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn percentile_u64(mut values: Vec<u64>, p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }

    values.sort_unstable();
    let rank = ((values.len() - 1) as f64 * p).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn parse_resource_profile_env() -> ResourceProfile {
    let raw = env::var("TSINK_RESOURCE_PROFILE").unwrap_or_else(|_| "embedded".to_string());
    match raw.trim().to_ascii_lowercase().as_str() {
        "test" => ResourceProfile::Test,
        "embedded" => ResourceProfile::Embedded,
        "edge" => ResourceProfile::Edge,
        "server" => ResourceProfile::Server,
        _ => panic!("invalid TSINK_RESOURCE_PROFILE={raw:?}; expected test|embedded|edge|server"),
    }
}

fn parse_optional_usize_env(name: &str) -> Option<usize> {
    env::var(name).ok().map(|raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("invalid {name}={raw:?}; expected a non-negative integer"))
    })
}

fn parse_optional_u64_env(name: &str) -> Option<u64> {
    env::var(name).ok().map(|raw| {
        raw.parse::<u64>()
            .unwrap_or_else(|_| panic!("invalid {name}={raw:?}; expected a non-negative integer"))
    })
}

fn resource_profile_label(profile: ResourceProfile) -> &'static str {
    match profile {
        ResourceProfile::Test => "test",
        ResourceProfile::Embedded => "embedded",
        ResourceProfile::Edge => "edge",
        ResourceProfile::Server => "server",
        ResourceProfile::Custom(_) => "custom",
        ResourceProfile::ExpertUnlimited => "expert_unlimited",
    }
}

fn resource_profile_name_label(profile: ResourceProfileName) -> &'static str {
    match profile {
        ResourceProfileName::Unreported => "unreported",
        ResourceProfileName::Test => "test",
        ResourceProfileName::Embedded => "embedded",
        ResourceProfileName::Edge => "edge",
        ResourceProfileName::Server => "server",
        ResourceProfileName::Custom => "custom",
        ResourceProfileName::ExpertUnlimited => "expert_unlimited",
    }
}

fn parse_env<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<T>().ok())
        .unwrap_or(default)
}

fn parse_env_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" => true,
            "0" | "false" | "no" | "n" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 { 0x9E3779B97F4A7C15 } else { seed };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize_bounded(&mut self, upper: usize) -> usize {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() as usize) % upper
    }

    fn next_u64_bounded(&mut self, upper: u64) -> u64 {
        if upper == 0 {
            return 0;
        }
        self.next_u64() % upper
    }

    fn next_u16_bounded(&mut self, upper: u16) -> u16 {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() % upper as u64) as u16
    }
}

fn main() {
    let cfg = WorkloadConfig::from_env();
    println!(
        "WORKLOAD_CONFIGURATION resource_profile={} memory_limit_override_bytes={} maintenance_max_bytes_override={} seed={}",
        resource_profile_label(cfg.resource_profile),
        cfg.memory_limit_override_bytes
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
        cfg.maintenance_max_bytes_override
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
        cfg.seed,
    );

    if cfg.ingest_latency_bench {
        println!("workload: starting ingest latency benchmark");
        println!(
            "workload: runs={} writers={} series_per_writer={} batch_size={} duration_secs={} memory_limit_bytes={} chunk_points={}",
            cfg.runs,
            cfg.ingest_latency_writer_threads,
            cfg.ingest_latency_series_per_writer,
            cfg.ingest_latency_batch_size,
            cfg.ingest_latency_duration_secs,
            cfg.ingest_latency_memory_limit_bytes,
            cfg.ingest_latency_chunk_points
        );

        let mut p95s = Vec::with_capacity(cfg.runs);
        let mut failures = 0usize;
        for run_id in 0..cfg.runs {
            match run_ingest_latency_once(&cfg, run_id) {
                Ok(result) => {
                    println!(
                        "INGEST_LATENCY_RESULT run={} batches={} points={} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3} flush_pipeline_runs={} persist_runs={} pipeline_timeouts={} wal_resets={}",
                        run_id + 1,
                        result.batches,
                        result.points,
                        result.p50_ms,
                        result.p95_ms,
                        result.p99_ms,
                        result.max_ms,
                        result.flush_pipeline_runs,
                        result.persist_runs,
                        result.pipeline_timeouts,
                        result.wal_resets,
                    );
                    p95s.push(result.p95_ms);
                }
                Err(err) => {
                    failures += 1;
                    eprintln!("INGEST_LATENCY_RESULT run={} ERROR {}", run_id + 1, err);
                }
            }
        }

        if p95s.is_empty() {
            eprintln!("workload: all ingest latency runs failed");
            std::process::exit(1);
        }

        println!(
            "INGEST_LATENCY_SUITE_RESULT resource_profile={} runs={} failures={} p50_p95_ms={:.3} p95_p95_ms={:.3}",
            resource_profile_label(cfg.resource_profile),
            p95s.len(),
            failures,
            percentile(p95s.clone(), 0.50),
            percentile(p95s, 0.95),
        );
        return;
    }

    if cfg.wal_append_bench {
        println!("workload: starting WAL append benchmark");
        println!(
            "workload: runs={} duration_secs={} batch_points={} sync_mode=periodic",
            cfg.runs, cfg.wal_append_duration_secs, cfg.wal_append_batch_points
        );

        let mut frames_per_sec = Vec::with_capacity(cfg.runs);
        let mut failures = 0usize;
        for run_id in 0..cfg.runs {
            match run_wal_append_once(&cfg, run_id) {
                Ok(result) => {
                    println!(
                        "WAL_APPEND_RESULT run={} frames={} points={} elapsed_ms={} frames_per_sec={:.1} points_per_sec={:.1} size_bytes={} segments={}",
                        run_id + 1,
                        result.frames,
                        result.points,
                        result.elapsed.as_millis(),
                        result.frames_per_sec,
                        result.points_per_sec,
                        result.size_bytes,
                        result.segment_count,
                    );
                    frames_per_sec.push(result.frames_per_sec);
                }
                Err(err) => {
                    failures += 1;
                    eprintln!("WAL_APPEND_RESULT run={} ERROR {}", run_id + 1, err);
                }
            }
        }

        if frames_per_sec.is_empty() {
            eprintln!("workload: all WAL append runs failed");
            std::process::exit(1);
        }

        println!(
            "WAL_APPEND_SUITE_RESULT runs={} failures={} p50_frames_per_sec={:.1} p95_frames_per_sec={:.1}",
            frames_per_sec.len(),
            failures,
            percentile(frames_per_sec.clone(), 0.50),
            percentile(frames_per_sec, 0.95),
        );
        return;
    }

    if cfg.metadata_selector_bench {
        println!("workload: starting metadata selector benchmark");
        println!(
            "workload: runs={} metadata_selector_series={}",
            cfg.runs, cfg.metadata_selector_series
        );

        for run_id in 0..cfg.runs {
            match run_metadata_selector_once(&cfg, run_id) {
                Ok(result) => {
                    println!(
                        "METADATA_SELECTOR_RESULT run={} series={} exact_count={} exact_ms={} missing_count={} missing_ms={} broad_present_label_count={} broad_present_label_ms={} broad_regex_count={} broad_regex_ms={} broad_empty_regex_count={} broad_empty_regex_ms={} broad_negative_regex_count={} broad_negative_regex_ms={} shard_scoped_regex_count={} shard_scoped_regex_ms={} shard_scoped_negative_count={} shard_scoped_negative_ms={}",
                        run_id + 1,
                        result.series,
                        result.exact_count,
                        result.exact_elapsed.as_millis(),
                        result.missing_count,
                        result.missing_elapsed.as_millis(),
                        result.broad_present_label_count,
                        result.broad_present_label_elapsed.as_millis(),
                        result.broad_regex_count,
                        result.broad_regex_elapsed.as_millis(),
                        result.broad_empty_regex_count,
                        result.broad_empty_regex_elapsed.as_millis(),
                        result.broad_negative_regex_count,
                        result.broad_negative_regex_elapsed.as_millis(),
                        result.shard_scoped_regex_count,
                        result.shard_scoped_regex_elapsed.as_millis(),
                        result.shard_scoped_negative_count,
                        result.shard_scoped_negative_elapsed.as_millis(),
                    );
                }
                Err(err) => {
                    eprintln!("METADATA_SELECTOR_RESULT run={} ERROR {}", run_id + 1, err);
                    std::process::exit(1);
                }
            }
        }
        return;
    }

    if cfg.query_saturation_bench {
        println!("workload: starting query saturation benchmark");
        println!(
            "workload: runs={} configured_workers={} seeded_series={} points_per_series={} series_per_query={} writer_points={} batch_size={}",
            cfg.runs,
            cfg.query_saturation_workers,
            cfg.query_saturation_series,
            cfg.query_saturation_points_per_series,
            cfg.query_saturation_series_per_query,
            cfg.query_saturation_writer_points,
            cfg.query_saturation_batch_size,
        );

        let mut query_p95_ms = Vec::with_capacity(cfg.runs);
        let mut writer_ms = Vec::with_capacity(cfg.runs);
        let mut peak_query_memory_bytes = Vec::with_capacity(cfg.runs);
        let mut process_peak_rss_bytes = Vec::with_capacity(cfg.runs);
        let mut failures = 0usize;
        for run_id in 0..cfg.runs {
            match run_query_saturation_once(&cfg, run_id) {
                Ok(result) => {
                    println!(
                        "QUERY_SATURATION_RESULT run={} workers={} seeded_series={} seeded_points={} series_per_query={} queries_succeeded={} query_points_returned={} writer_points={} elapsed_ms={} query_p50_ms={:.3} query_p95_ms={:.3} query_max_ms={:.3} writer_ms={:.3} peak_active_queries={} peak_shared_reserved_memory_bytes={} concurrency_rejections={} active_queries_after={} shared_reserved_memory_bytes_after={} process_peak_rss_bytes_so_far={}",
                        run_id + 1,
                        result.workers,
                        result.seeded_series,
                        result.seeded_points,
                        result.series_per_query,
                        result.queries_succeeded,
                        result.query_points_returned,
                        result.writer_points,
                        result.elapsed.as_millis(),
                        result.query_p50_ms,
                        result.query_p95_ms,
                        result.query_max_ms,
                        result.writer_ms,
                        result.peak_active_queries,
                        result.peak_shared_reserved_memory_bytes,
                        result.concurrency_rejections,
                        result.active_queries_after,
                        result.shared_reserved_memory_bytes_after,
                        optional_u64_text(result.process_peak_rss_bytes_so_far),
                    );
                    query_p95_ms.push(result.query_p95_ms);
                    writer_ms.push(result.writer_ms);
                    peak_query_memory_bytes.push(result.peak_shared_reserved_memory_bytes);
                    if let Some(bytes) = result.process_peak_rss_bytes_so_far {
                        process_peak_rss_bytes.push(bytes);
                    }
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    eprintln!("QUERY_SATURATION_RESULT run={} ERROR {}", run_id + 1, error);
                }
            }
        }
        if query_p95_ms.is_empty() {
            eprintln!("workload: all query saturation runs failed");
            std::process::exit(1);
        }
        println!(
            "QUERY_SATURATION_SUITE_RESULT resource_profile={} runs={} failures={} p50_query_p95_ms={:.3} p95_query_p95_ms={:.3} p50_writer_ms={:.3} p95_writer_ms={:.3} p50_peak_shared_reserved_memory_bytes={} p95_peak_shared_reserved_memory_bytes={} max_process_peak_rss_bytes_so_far={}",
            resource_profile_label(cfg.resource_profile),
            query_p95_ms.len(),
            failures,
            percentile(query_p95_ms.clone(), 0.50),
            percentile(query_p95_ms, 0.95),
            percentile(writer_ms.clone(), 0.50),
            percentile(writer_ms, 0.95),
            percentile_u64(peak_query_memory_bytes.clone(), 0.50),
            percentile_u64(peak_query_memory_bytes, 0.95),
            optional_u64_text(process_peak_rss_bytes.iter().copied().max()),
        );
        return;
    }

    if cfg.new_series_writer_threads > 0 && cfg.new_series_series_per_writer > 0 {
        println!("workload: starting parallel new-series benchmark");
        println!(
            "workload: runs={} new_series_writers={} new_series_per_writer={} shared_metric_names={} cached_missing_label_names={}",
            cfg.runs,
            cfg.new_series_writer_threads,
            cfg.new_series_series_per_writer,
            cfg.new_series_shared_metric_names,
            cfg.new_series_cached_missing_label_names
        );

        let mut series_per_sec = Vec::with_capacity(cfg.runs);
        let mut writer_p95_ms = Vec::with_capacity(cfg.runs);
        let mut process_peak_rss_bytes = Vec::with_capacity(cfg.runs);
        let mut failures = 0usize;
        for run_id in 0..cfg.runs {
            match run_parallel_new_series_once(&cfg, run_id) {
                Ok(result) => {
                    println!(
                        "NEW_SERIES_RESULT run={} created_series={} elapsed_ms={} series_per_sec={:.3} writer_p50_ms={:.3} writer_p95_ms={:.3} writer_p99_ms={:.3} writer_max_ms={:.3} process_peak_rss_bytes_so_far={}",
                        run_id + 1,
                        result.created_series,
                        result.elapsed.as_millis(),
                        result.series_per_sec,
                        result.writer_p50_ms,
                        result.writer_p95_ms,
                        result.writer_p99_ms,
                        result.writer_max_ms,
                        optional_u64_text(result.process_peak_rss_bytes_so_far),
                    );
                    series_per_sec.push(result.series_per_sec);
                    writer_p95_ms.push(result.writer_p95_ms);
                    if let Some(bytes) = result.process_peak_rss_bytes_so_far {
                        process_peak_rss_bytes.push(bytes);
                    }
                }
                Err(err) => {
                    failures += 1;
                    eprintln!("NEW_SERIES_RESULT run={} ERROR {}", run_id + 1, err);
                }
            }
        }

        if series_per_sec.is_empty() {
            eprintln!("workload: all parallel new-series runs failed");
            std::process::exit(1);
        }

        println!(
            "NEW_SERIES_SUITE_RESULT resource_profile={} runs={} failures={} p50_series_per_sec={:.3} p95_series_per_sec={:.3} p50_writer_p95_ms={:.3} p95_writer_p95_ms={:.3} max_process_peak_rss_bytes_so_far={}",
            resource_profile_label(cfg.resource_profile),
            series_per_sec.len(),
            failures,
            percentile(series_per_sec.clone(), 0.50),
            percentile(series_per_sec, 0.95),
            percentile(writer_p95_ms.clone(), 0.50),
            percentile(writer_p95_ms, 0.95),
            optional_u64_text(process_peak_rss_bytes.iter().copied().max()),
        );
        return;
    }

    println!("workload: starting mixed workload suite");
    println!(
        "workload: runs={} active_series={} warmup_points={} measured_points={} batch_size={}",
        cfg.runs, cfg.active_series, cfg.warmup_points, cfg.measured_points, cfg.batch_size
    );
    println!(
        "workload: retention={}s partition={}s max_active_partition_heads_per_series={} ooo_max={}s ooo_permille={} ooo_cross_partition_permille={} ooo_cross_partitions={} sparse_emit_permille={} prime_all_series={} shared_metric_names={}",
        cfg.retention_seconds,
        cfg.partition_seconds,
        cfg.max_active_partition_heads_per_series,
        cfg.out_of_order_max_seconds,
        cfg.out_of_order_permille,
        cfg.cross_partition_out_of_order_permille,
        cfg.cross_partition_out_of_order_partitions,
        cfg.sparse_emit_permille,
        cfg.prime_all_series,
        cfg.shared_metric_names
    );

    if cfg.active_series < SUITE_ACTIVE_SERIES_TARGET {
        println!(
            "workload: WARNING active_series={} is below suite target {}",
            cfg.active_series, SUITE_ACTIVE_SERIES_TARGET
        );
    }

    let mut run_bpps = Vec::with_capacity(cfg.runs);
    let mut run_max_post_write_accounted_memory_bytes = Vec::with_capacity(cfg.runs);
    let mut run_post_settle_accounted_memory_bytes = Vec::with_capacity(cfg.runs);
    let mut run_local_disk_bytes = Vec::with_capacity(cfg.runs);
    let mut run_persisted_bytes = Vec::with_capacity(cfg.runs);
    let mut run_process_peak_rss_bytes = Vec::with_capacity(cfg.runs);
    let mut failures = 0usize;

    for run_id in 0..cfg.runs {
        match run_once(&cfg, run_id) {
            Ok(result) => {
                println!(
                    "RUN_RESULT run={} resource_profile={} accounted_memory_limit_bytes={} local_disk_limit_bytes={} wal_limit_bytes={} cardinality_limit={} max_concurrent_writers={} max_concurrent_queries={} maintenance_max_items_per_pass={} maintenance_max_bytes_per_pass={} retained_points={} late_rejected_points={} max_post_write_accounted_memory_bytes={} post_settle_accounted_memory_bytes={} active_and_sealed_bytes={} registry_bytes={} metadata_cache_bytes={} wal_series_definition_cache_bytes={} write_transient_bytes={} persisted_index_bytes={} persisted_mmap_bytes={} tombstone_bytes={} local_disk_accounted_bytes_before_close={} persisted_bytes={} effective_bpp={:.6} process_peak_rss_bytes_so_far={} path={}",
                    run_id + 1,
                    resource_profile_name_label(result.selected_profile),
                    result.accounted_memory_limit_bytes,
                    result.local_disk_limit_bytes,
                    result.wal_limit_bytes,
                    result.cardinality_limit,
                    result.max_concurrent_writers,
                    result.max_concurrent_queries,
                    result.maintenance_max_items_per_pass,
                    result.maintenance_max_bytes_per_pass,
                    result.retained_points,
                    result.late_rejected_points,
                    result.max_post_write_accounted_memory_bytes,
                    result.post_settle_accounted_memory_bytes,
                    result.active_and_sealed_bytes,
                    result.registry_bytes,
                    result.metadata_cache_bytes,
                    result.wal_series_definition_cache_bytes,
                    result.write_transient_bytes,
                    result.persisted_index_bytes,
                    result.persisted_mmap_bytes,
                    result.tombstone_bytes,
                    result.local_disk_accounted_bytes_before_close,
                    result.persisted_bytes,
                    result.effective_bpp,
                    optional_u64_text(result.process_peak_rss_bytes_so_far),
                    result.data_path.display()
                );
                run_bpps.push(result.effective_bpp);
                run_max_post_write_accounted_memory_bytes
                    .push(result.max_post_write_accounted_memory_bytes as u64);
                run_post_settle_accounted_memory_bytes
                    .push(result.post_settle_accounted_memory_bytes as u64);
                run_local_disk_bytes.push(result.local_disk_accounted_bytes_before_close);
                run_persisted_bytes.push(result.persisted_bytes);
                if let Some(bytes) = result.process_peak_rss_bytes_so_far {
                    run_process_peak_rss_bytes.push(bytes);
                }
            }
            Err(err) => {
                failures += 1;
                eprintln!("RUN_RESULT run={} ERROR {}", run_id + 1, err);
            }
        }
    }

    if run_bpps.is_empty() {
        eprintln!("workload: all runs failed");
        std::process::exit(1);
    }

    let p50 = percentile(run_bpps.clone(), 0.50);
    let p95 = percentile(run_bpps.clone(), 0.95);
    let p50_max_post_write_accounted_memory_bytes =
        percentile_u64(run_max_post_write_accounted_memory_bytes.clone(), 0.50);
    let p95_max_post_write_accounted_memory_bytes =
        percentile_u64(run_max_post_write_accounted_memory_bytes.clone(), 0.95);
    let max_post_write_accounted_memory_bytes = run_max_post_write_accounted_memory_bytes
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    let p50_post_settle_accounted_memory_bytes =
        percentile_u64(run_post_settle_accounted_memory_bytes.clone(), 0.50);
    let p95_post_settle_accounted_memory_bytes =
        percentile_u64(run_post_settle_accounted_memory_bytes.clone(), 0.95);
    let max_post_settle_accounted_memory_bytes = run_post_settle_accounted_memory_bytes
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    let p50_local_disk_bytes = percentile_u64(run_local_disk_bytes.clone(), 0.50);
    let p95_local_disk_bytes = percentile_u64(run_local_disk_bytes.clone(), 0.95);
    let max_local_disk_bytes = run_local_disk_bytes.iter().copied().max().unwrap_or(0);
    let p50_persisted_bytes = percentile_u64(run_persisted_bytes.clone(), 0.50);
    let p95_persisted_bytes = percentile_u64(run_persisted_bytes.clone(), 0.95);
    let max_persisted_bytes = run_persisted_bytes.iter().copied().max().unwrap_or(0);

    println!(
        "SUITE_RESULT resource_profile={} runs={} failures={} p50_effective_bpp={:.6} p95_effective_bpp={:.6} p50_max_post_write_accounted_memory_bytes={} p95_max_post_write_accounted_memory_bytes={} max_post_write_accounted_memory_bytes={} p50_post_settle_accounted_memory_bytes={} p95_post_settle_accounted_memory_bytes={} max_post_settle_accounted_memory_bytes={} p50_local_disk_accounted_bytes_before_close={} p95_local_disk_accounted_bytes_before_close={} max_local_disk_accounted_bytes_before_close={} p50_persisted_bytes={} p95_persisted_bytes={} max_persisted_bytes={} max_process_peak_rss_bytes_so_far={}",
        resource_profile_label(cfg.resource_profile),
        run_bpps.len(),
        failures,
        p50,
        p95,
        p50_max_post_write_accounted_memory_bytes,
        p95_max_post_write_accounted_memory_bytes,
        max_post_write_accounted_memory_bytes,
        p50_post_settle_accounted_memory_bytes,
        p95_post_settle_accounted_memory_bytes,
        max_post_settle_accounted_memory_bytes,
        p50_local_disk_bytes,
        p95_local_disk_bytes,
        max_local_disk_bytes,
        p50_persisted_bytes,
        p95_persisted_bytes,
        max_persisted_bytes,
        optional_u64_text(run_process_peak_rss_bytes.iter().copied().max()),
    );

    let target_p50_ok = p50 <= 0.75;
    let target_p95_ok = p95 <= 1.0;
    let all_runs_ok = failures == 0;
    println!(
        "TARGET_CHECK all_runs_succeeded={} p50<=0.75={} p95<=1.0={} pass={}",
        all_runs_ok,
        target_p50_ok,
        target_p95_ok,
        all_runs_ok && target_p50_ok && target_p95_ok
    );

    if cfg.fail_on_target && !(all_runs_ok && target_p50_ok && target_p95_ok) {
        std::process::exit(2);
    }
}
