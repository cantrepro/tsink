//! Mixed-workload effective bytes-per-point measurement harness for storage engine work.
//!
//! One-command baseline:
//! `scripts/measure_bpp.sh`

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision, Value};

const SUITE_ACTIVE_SERIES_TARGET: usize = 1_000_000;
const GAUGE_WEIGHT: u16 = 600;
const COUNTER_WEIGHT: u16 = 200;
const SPARSE_WEIGHT: u16 = 100;
const SHORT_LIVED_WEIGHT: u16 = 50;
const WEIGHT_SCALE: u16 = 1000;

#[derive(Debug, Clone)]
struct WorkloadConfig {
    runs: usize,
    active_series: usize,
    prime_all_series: bool,
    warmup_points: usize,
    measured_points: usize,
    batch_size: usize,
    out_of_order_max_seconds: i64,
    out_of_order_permille: u16,
    sparse_emit_permille: u16,
    short_lived_lifetime_steps: u64,
    retention_seconds: u64,
    partition_seconds: u64,
    step_seconds: i64,
    settle_millis: u64,
    seed: u64,
    fail_on_target: bool,
}

impl WorkloadConfig {
    fn from_env() -> Self {
        Self {
            runs: parse_env("TSINK_BPP_RUNS", 5usize),
            active_series: parse_env("TSINK_ACTIVE_SERIES", SUITE_ACTIVE_SERIES_TARGET),
            prime_all_series: parse_env_bool("TSINK_PRIME_ALL_SERIES", true),
            warmup_points: parse_env("TSINK_WARMUP_POINTS", 250_000usize),
            measured_points: parse_env("TSINK_MEASURE_POINTS", 1_000_000usize),
            batch_size: parse_env("TSINK_BATCH_SIZE", 4096usize),
            out_of_order_max_seconds: parse_env("TSINK_OOO_MAX_SECONDS", 300i64),
            out_of_order_permille: parse_env("TSINK_OOO_PERMILLE", 250u16),
            sparse_emit_permille: parse_env("TSINK_SPARSE_EMIT_PERMILLE", 120u16),
            short_lived_lifetime_steps: parse_env("TSINK_SHORT_LIVED_LIFETIME_STEPS", 64u64),
            retention_seconds: parse_env("TSINK_RETENTION_SECONDS", 7 * 24 * 3600u64),
            partition_seconds: parse_env("TSINK_PARTITION_SECONDS", 3600u64),
            step_seconds: parse_env("TSINK_STEP_SECONDS", 10i64),
            settle_millis: parse_env("TSINK_SETTLE_MILLIS", 1500u64),
            seed: parse_env("TSINK_SEED", 0xC0DEC0DEu64),
            fail_on_target: parse_env_bool("TSINK_FAIL_ON_TARGET", false),
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
    retained_points: u64,
    persisted_bytes: u64,
    effective_bpp: f64,
    data_path: PathBuf,
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
        F: FnMut(&[Row]) -> Result<(), String>,
    {
        if !self.cfg.prime_all_series {
            return Ok(0);
        }

        let mut inserted = 0u64;
        let mut batch = Vec::with_capacity(self.cfg.batch_size);

        for series_slot in 0..self.cfg.active_series {
            let kind = self.slices.kind_for_series(series_slot);
            let row = self.build_row(kind, series_slot, false);
            batch.push(row);
            inserted += 1;
            self.logical_step = self.logical_step.saturating_add(1);

            if batch.len() >= self.cfg.batch_size {
                insert_rows(&batch)?;
                batch.clear();
            }
        }

        if !batch.is_empty() {
            insert_rows(&batch)?;
        }

        Ok(inserted)
    }

    fn ingest_points<F>(
        &mut self,
        target_points: usize,
        measured_phase: bool,
        mut insert_rows: F,
    ) -> Result<u64, String>
    where
        F: FnMut(&[Row]) -> Result<(), String>,
    {
        let mut generated = 0u64;
        let mut inserted = 0u64;
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
                    self.enqueue_row(row, &mut batch, &mut inserted);
                    self.logical_step = self.logical_step.saturating_add(1);
                }
            } else {
                self.logical_step = self.logical_step.saturating_add(1);
            }

            self.release_due_delayed_rows(&mut batch, &mut inserted);

            if batch.len() >= self.cfg.batch_size {
                insert_rows(&batch)?;
                batch.clear();
            }
        }

        if !batch.is_empty() {
            insert_rows(&batch)?;
        }

        Ok(inserted.min(generated))
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
        let metric = metric_name(kind);
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

    fn enqueue_row(&mut self, row: Row, batch: &mut Vec<Row>, inserted: &mut u64) {
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
        *inserted = inserted.saturating_add(1);
    }

    fn release_due_delayed_rows(&mut self, batch: &mut Vec<Row>, inserted: &mut u64) {
        loop {
            let Some((&due_step, _)) = self.delayed_rows.first_key_value() else {
                break;
            };
            if due_step > self.logical_step {
                break;
            }

            if let Some(mut due_rows) = self.delayed_rows.remove(&due_step) {
                *inserted = inserted.saturating_add(due_rows.len() as u64);
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
}

fn blob_state(series_slot: usize) -> &'static str {
    const STATES: [&str; 8] = ["ok", "warn", "error", "idle", "cold", "hot", "up", "down"];
    STATES[series_slot % STATES.len()]
}

fn metric_name(kind: SeriesKind) -> &'static str {
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
    let storage = StorageBuilder::new()
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(cfg.retention_seconds))
        .with_partition_duration(Duration::from_secs(cfg.partition_seconds))
        .build()
        .map_err(|e| format!("storage build failed: {e}"))?;

    let mut generator = WorkloadGenerator::new(cfg.clone(), run_id, base_ts);

    let mut retained_points = 0u64;
    retained_points += generator.prime_all_series(|rows| {
        storage
            .insert_rows(rows)
            .map_err(|e| format!("prime insert failed: {e}"))
    })?;

    retained_points += generator.ingest_points(cfg.warmup_points, false, |rows| {
        storage
            .insert_rows(rows)
            .map_err(|e| format!("warmup insert failed: {e}"))
    })?;

    retained_points += generator.ingest_points(cfg.measured_points, true, |rows| {
        storage
            .insert_rows(rows)
            .map_err(|e| format!("measure insert failed: {e}"))
    })?;

    std::thread::sleep(Duration::from_millis(cfg.settle_millis));
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
        retained_points,
        persisted_bytes,
        effective_bpp,
        data_path,
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

fn current_unix_seconds() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

fn percentile(mut values: Vec<f64>, p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    values.sort_by(|a, b| a.total_cmp(b));
    let rank = ((values.len() - 1) as f64 * p).round() as usize;
    values[rank.min(values.len() - 1)]
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

    println!("workload: starting mixed workload suite");
    println!(
        "workload: runs={} active_series={} warmup_points={} measured_points={} batch_size={}",
        cfg.runs, cfg.active_series, cfg.warmup_points, cfg.measured_points, cfg.batch_size
    );
    println!(
        "workload: retention={}s partition={}s ooo_max={}s ooo_permille={} sparse_emit_permille={} prime_all_series={}",
        cfg.retention_seconds,
        cfg.partition_seconds,
        cfg.out_of_order_max_seconds,
        cfg.out_of_order_permille,
        cfg.sparse_emit_permille,
        cfg.prime_all_series
    );

    if cfg.active_series < SUITE_ACTIVE_SERIES_TARGET {
        println!(
            "workload: WARNING active_series={} is below suite target {}",
            cfg.active_series, SUITE_ACTIVE_SERIES_TARGET
        );
    }

    let mut run_bpps = Vec::with_capacity(cfg.runs);
    let mut failures = 0usize;

    for run_id in 0..cfg.runs {
        match run_once(&cfg, run_id) {
            Ok(result) => {
                println!(
                    "RUN_RESULT run={} retained_points={} persisted_bytes={} effective_bpp={:.6} path={}",
                    run_id + 1,
                    result.retained_points,
                    result.persisted_bytes,
                    result.effective_bpp,
                    result.data_path.display()
                );
                run_bpps.push(result.effective_bpp);
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

    println!(
        "SUITE_RESULT runs={} failures={} p50_effective_bpp={:.6} p95_effective_bpp={:.6}",
        run_bpps.len(),
        failures,
        p50,
        p95
    );

    let target_p50_ok = p50 <= 0.75;
    let target_p95_ok = p95 <= 1.0;
    println!(
        "TARGET_CHECK p50<=0.75={} p95<=1.0={} pass={}",
        target_p50_ok,
        target_p95_ok,
        target_p50_ok && target_p95_ok
    );

    if cfg.fail_on_target && !(target_p50_ok && target_p95_ok) {
        std::process::exit(2);
    }
}
