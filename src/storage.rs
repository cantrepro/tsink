//! Main storage API and query helpers for tsink.

use crate::value::{i64_to_f64_exact, u64_to_f64_exact};
use crate::wal::WalSyncMode;
use crate::{
    Aggregator as TypedAggregator, BytesAggregation, Codec, CodecAggregator, DataPoint, Label,
    Result, Row, TsinkError, Value,
};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Timestamp precision for data points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

/// A unique metric series identified by name and label set.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricSeries {
    pub name: String,
    pub labels: Vec<Label>,
}

/// Label matcher operator for series selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SeriesMatcherOp {
    Equal,
    NotEqual,
    RegexMatch,
    RegexNoMatch,
}

/// Label matcher for series selection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesMatcher {
    pub name: String,
    pub op: SeriesMatcherOp,
    pub value: String,
}

impl SeriesMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: SeriesMatcherOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }

    #[must_use]
    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::Equal, value)
    }

    #[must_use]
    pub fn not_equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::NotEqual, value)
    }

    #[must_use]
    pub fn regex_match(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::RegexMatch, value)
    }

    #[must_use]
    pub fn regex_no_match(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::RegexNoMatch, value)
    }
}

/// Selection request for matching metric series.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesSelection {
    pub metric: Option<String>,
    pub matchers: Vec<SeriesMatcher>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

impl SeriesSelection {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_metric(mut self, metric: impl Into<String>) -> Self {
        self.metric = Some(metric.into());
        self
    }

    #[must_use]
    pub fn with_matcher(mut self, matcher: SeriesMatcher) -> Self {
        self.matchers.push(matcher);
        self
    }

    #[must_use]
    pub fn with_matchers(mut self, matchers: Vec<SeriesMatcher>) -> Self {
        self.matchers = matchers;
        self
    }

    #[must_use]
    pub fn with_time_range(mut self, start: i64, end: i64) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self
    }

    pub(crate) fn normalized_time_range(&self) -> Result<Option<(i64, i64)>> {
        match (self.start, self.end) {
            (None, None) => Ok(None),
            (Some(start), Some(end)) => {
                if start >= end {
                    return Err(TsinkError::InvalidTimeRange { start, end });
                }
                Ok(Some((start, end)))
            }
            _ => Err(TsinkError::InvalidConfiguration(
                "series selection requires both start and end when time range filtering is enabled"
                    .to_string(),
            )),
        }
    }
}

/// Runtime observability snapshot for the storage engine internals.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StorageObservabilitySnapshot {
    pub wal: WalObservabilitySnapshot,
    pub flush: FlushObservabilitySnapshot,
    pub compaction: CompactionObservabilitySnapshot,
    pub query: QueryObservabilitySnapshot,
}

/// WAL internals snapshot.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WalObservabilitySnapshot {
    pub enabled: bool,
    pub size_bytes: u64,
    pub segment_count: u64,
    pub active_segment: u64,
    pub highwater_segment: u64,
    pub highwater_frame: u64,
    pub replay_runs_total: u64,
    pub replay_frames_total: u64,
    pub replay_series_definitions_total: u64,
    pub replay_sample_batches_total: u64,
    pub replay_points_total: u64,
    pub replay_errors_total: u64,
    pub replay_duration_nanos_total: u64,
    pub append_series_definitions_total: u64,
    pub append_sample_batches_total: u64,
    pub append_points_total: u64,
    pub append_bytes_total: u64,
    pub append_errors_total: u64,
    pub resets_total: u64,
    pub reset_errors_total: u64,
}

/// Flush/persist internals snapshot.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FlushObservabilitySnapshot {
    pub pipeline_runs_total: u64,
    pub pipeline_success_total: u64,
    pub pipeline_timeout_total: u64,
    pub pipeline_errors_total: u64,
    pub pipeline_duration_nanos_total: u64,
    pub active_flush_runs_total: u64,
    pub active_flush_errors_total: u64,
    pub active_flushed_series_total: u64,
    pub active_flushed_chunks_total: u64,
    pub active_flushed_points_total: u64,
    pub persist_runs_total: u64,
    pub persist_success_total: u64,
    pub persist_noop_total: u64,
    pub persist_errors_total: u64,
    pub persisted_series_total: u64,
    pub persisted_chunks_total: u64,
    pub persisted_points_total: u64,
    pub persisted_segments_total: u64,
    pub persist_duration_nanos_total: u64,
    pub evicted_sealed_chunks_total: u64,
}

/// Compaction internals snapshot.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompactionObservabilitySnapshot {
    pub runs_total: u64,
    pub success_total: u64,
    pub noop_total: u64,
    pub errors_total: u64,
    pub source_segments_total: u64,
    pub output_segments_total: u64,
    pub source_chunks_total: u64,
    pub output_chunks_total: u64,
    pub source_points_total: u64,
    pub output_points_total: u64,
    pub duration_nanos_total: u64,
}

/// Query internals snapshot.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryObservabilitySnapshot {
    pub select_calls_total: u64,
    pub select_errors_total: u64,
    pub select_duration_nanos_total: u64,
    pub select_points_returned_total: u64,
    pub select_with_options_calls_total: u64,
    pub select_with_options_errors_total: u64,
    pub select_with_options_duration_nanos_total: u64,
    pub select_with_options_points_returned_total: u64,
    pub select_all_calls_total: u64,
    pub select_all_errors_total: u64,
    pub select_all_duration_nanos_total: u64,
    pub select_all_series_returned_total: u64,
    pub select_all_points_returned_total: u64,
    pub select_series_calls_total: u64,
    pub select_series_errors_total: u64,
    pub select_series_duration_nanos_total: u64,
    pub select_series_returned_total: u64,
    pub merge_path_queries_total: u64,
    pub append_sort_path_queries_total: u64,
}

/// Storage provides thread-safe capabilities for insertion and retrieval from time-series storage.
pub trait Storage: Send + Sync {
    /// Inserts rows into the storage.
    fn insert_rows(&self, rows: &[Row]) -> Result<()>;

    /// Selects data points for a metric within the given time range.
    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>>;

    /// Selects data points into a caller-provided buffer, allowing allocation reuse.
    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        *out = self.select(metric, labels, start, end)?;
        Ok(())
    }

    /// Selects with additional options like downsampling, aggregation, and pagination.
    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>>;

    /// Selects data points for a metric regardless of labels.
    /// Returns a map of label sets to their corresponding data points.
    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>>;

    /// Lists all known metric series currently present in storage.
    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        Err(TsinkError::Other(
            "list_metrics is not implemented for this storage backend".to_string(),
        ))
    }

    /// Lists known metric series and may include additional WAL-scanned series.
    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        self.list_metrics()
    }

    /// Selects metric series by structured label matchers (non-PromQL API).
    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        if let Some(metric) = &selection.metric {
            if metric.is_empty() {
                return Err(TsinkError::MetricRequired);
            }
            if metric.len() > u16::MAX as usize {
                return Err(TsinkError::InvalidMetricName(format!(
                    "metric name too long: {} bytes (max {})",
                    metric.len(),
                    u16::MAX as usize
                )));
            }
        }

        let time_range = selection.normalized_time_range()?;
        let compiled_matchers = compile_series_matchers(&selection.matchers)?;

        let mut series = self.list_metrics()?;
        series.retain(|entry| {
            if selection
                .metric
                .as_ref()
                .is_some_and(|metric| entry.name != *metric)
            {
                return false;
            }

            compiled_matchers
                .iter()
                .all(|matcher| matcher.matches(&entry.name, &entry.labels))
        });

        if let Some((start, end)) = time_range {
            let mut filtered = Vec::with_capacity(series.len());
            for entry in series {
                let points = self.select(&entry.name, &entry.labels, start, end)?;
                if !points.is_empty() {
                    filtered.push(entry);
                }
            }
            series = filtered;
        }

        series.sort();
        Ok(series)
    }

    /// Returns currently estimated in-memory bytes owned by the storage engine.
    fn memory_used(&self) -> usize {
        0
    }

    /// Returns configured in-memory byte budget for the storage engine.
    ///
    /// `usize::MAX` means "no explicit budget configured".
    fn memory_budget(&self) -> usize {
        usize::MAX
    }

    /// Returns low-level runtime observability counters/gauges.
    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        StorageObservabilitySnapshot::default()
    }

    /// Writes an atomic on-disk snapshot to `destination`.
    ///
    /// Snapshot support is backend-specific and may not be available for all storage
    /// implementations.
    fn snapshot(&self, _destination: &Path) -> Result<()> {
        Err(TsinkError::InvalidConfiguration(
            "snapshot is not implemented for this storage backend".to_string(),
        ))
    }

    /// Closes the storage gracefully.
    fn close(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledSeriesMatcher {
    pub name: String,
    pub op: SeriesMatcherOp,
    pub value: String,
    pub regex: Option<Regex>,
}

impl CompiledSeriesMatcher {
    pub(crate) fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        let actual = if self.name == "__name__" {
            Some(metric)
        } else {
            labels
                .iter()
                .find(|label| label.name == self.name)
                .map(|label| label.value.as_str())
        };

        match self.op {
            SeriesMatcherOp::Equal => actual.is_some_and(|value| value == self.value),
            SeriesMatcherOp::NotEqual => actual.is_none_or(|value| value != self.value),
            SeriesMatcherOp::RegexMatch => actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
            SeriesMatcherOp::RegexNoMatch => !actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
        }
    }
}

pub(crate) fn compile_series_matchers(
    matchers: &[SeriesMatcher],
) -> Result<Vec<CompiledSeriesMatcher>> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        if matcher.name.is_empty() {
            return Err(TsinkError::InvalidLabel(
                "series matcher label name cannot be empty".to_string(),
            ));
        }

        let regex = match matcher.op {
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                let anchored = format!("^(?:{})$", matcher.value);
                Some(Regex::new(&anchored).map_err(|err| {
                    TsinkError::InvalidConfiguration(format!(
                        "invalid matcher regex '{}': {err}",
                        matcher.value
                    ))
                })?)
            }
            SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => None,
        };

        compiled.push(CompiledSeriesMatcher {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        });
    }
    Ok(compiled)
}

/// Builder for creating a Storage instance.
pub struct StorageBuilder {
    data_path: Option<PathBuf>,
    retention: Duration,
    retention_enforced: bool,
    timestamp_precision: TimestampPrecision,
    chunk_points: usize,
    max_writers: usize,
    write_timeout: Duration,
    partition_duration: Duration,
    memory_limit_bytes: usize,
    cardinality_limit: usize,
    wal_enabled: bool,
    wal_size_limit_bytes: usize,
    wal_buffer_size: usize,
    wal_sync_mode: WalSyncMode,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self {
            data_path: None,
            retention: Duration::from_secs(14 * 24 * 3600),
            retention_enforced: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            chunk_points: crate::engine::DEFAULT_CHUNK_POINTS,
            max_writers: crate::cgroup::default_workers_limit(),
            write_timeout: Duration::from_secs(30),
            partition_duration: Duration::from_secs(3600),
            memory_limit_bytes: usize::MAX,
            cardinality_limit: usize::MAX,
            wal_enabled: true,
            wal_size_limit_bytes: usize::MAX,
            wal_buffer_size: 4096,
            wal_sync_mode: WalSyncMode::default(),
        }
    }
}

impl StorageBuilder {
    /// Creates a new StorageBuilder with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the data path for persistent storage.
    #[must_use]
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.data_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the retention period.
    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// Enables or disables retention enforcement.
    ///
    /// When disabled, points are never rejected or filtered due to retention.
    #[must_use]
    pub fn with_retention_enforced(mut self, enforced: bool) -> Self {
        self.retention_enforced = enforced;
        self
    }

    /// Sets the timestamp precision.
    #[must_use]
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    /// Sets the target points-per-chunk for the storage engine.
    #[must_use]
    pub fn with_chunk_points(mut self, points: usize) -> Self {
        self.chunk_points = points.clamp(1, u16::MAX as usize);
        self
    }

    /// Sets the maximum number of concurrent writers.
    #[must_use]
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.max_writers = if max_writers == 0 {
            crate::cgroup::default_workers_limit().max(1)
        } else {
            max_writers
        };
        self
    }

    /// Sets the write timeout.
    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Sets the partition duration.
    #[must_use]
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.partition_duration = duration;
        self
    }

    /// Sets a global in-memory byte budget for active + sealed chunks.
    ///
    /// When exceeded, the engine applies backpressure by persisting sealed chunks to L0
    /// and evicting the oldest sealed chunks from RAM before admitting new writes.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = bytes;
        self
    }

    /// Sets a hard upper bound for total series cardinality.
    ///
    /// New metric+label combinations are rejected once the limit is reached.
    #[must_use]
    pub fn with_cardinality_limit(mut self, series: usize) -> Self {
        self.cardinality_limit = series;
        self
    }

    /// Enables or disables WAL.
    #[must_use]
    pub fn with_wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Sets a hard upper bound for on-disk WAL bytes across all WAL segments.
    ///
    /// `usize::MAX` disables the limit.
    #[must_use]
    pub fn with_wal_size_limit(mut self, bytes: usize) -> Self {
        self.wal_size_limit_bytes = bytes;
        self
    }

    /// Sets the WAL buffer size.
    #[must_use]
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.wal_buffer_size = size;
        self
    }

    /// Sets WAL fsync policy.
    #[must_use]
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.wal_sync_mode = mode;
        self
    }

    /// Builds the Storage instance.
    pub fn build(self) -> Result<Arc<dyn Storage>> {
        crate::engine::build_storage(self)
    }

    /// Atomically restores `data_path` from a previously created snapshot directory.
    pub fn restore_from_snapshot(
        snapshot_path: impl AsRef<Path>,
        data_path: impl AsRef<Path>,
    ) -> Result<()> {
        crate::engine::restore_storage_from_snapshot(snapshot_path.as_ref(), data_path.as_ref())
    }

    pub(crate) fn chunk_points(&self) -> usize {
        self.chunk_points
    }

    pub(crate) fn data_path(&self) -> Option<&Path> {
        self.data_path.as_deref()
    }

    pub(crate) fn retention(&self) -> Duration {
        self.retention
    }

    pub(crate) fn retention_enforced(&self) -> bool {
        self.retention_enforced
    }

    pub(crate) fn timestamp_precision(&self) -> TimestampPrecision {
        self.timestamp_precision
    }

    pub(crate) fn max_writers(&self) -> usize {
        self.max_writers
    }

    pub(crate) fn write_timeout(&self) -> Duration {
        self.write_timeout
    }

    pub(crate) fn partition_duration(&self) -> Duration {
        self.partition_duration
    }

    pub(crate) fn memory_limit_bytes(&self) -> usize {
        self.memory_limit_bytes
    }

    pub(crate) fn cardinality_limit(&self) -> usize {
        self.cardinality_limit
    }

    pub(crate) fn wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    pub(crate) fn wal_size_limit_bytes(&self) -> usize {
        self.wal_size_limit_bytes
    }

    pub(crate) fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    pub(crate) fn wal_sync_mode(&self) -> WalSyncMode {
        self.wal_sync_mode
    }
}

/// Aggregation applied to query results or buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    None,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
    Count,
    Median,
    Range,
    Variance,
    StdDev,
}

/// Downsampling configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownsampleOptions {
    pub interval: i64,
}

/// Options to customize queries (aggregation, downsampling, pagination).
#[derive(Clone)]
pub struct QueryOptions {
    pub labels: Vec<Label>,
    pub start: i64,
    pub end: i64,
    pub aggregation: Aggregation,
    pub downsample: Option<DownsampleOptions>,
    pub custom_aggregation: Option<Arc<dyn BytesAggregation>>,
    pub limit: Option<usize>,
    pub offset: usize,
}

impl QueryOptions {
    /// Create query options for a time range.
    #[must_use]
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            labels: Vec::new(),
            start,
            end,
            aggregation: Aggregation::None,
            downsample: None,
            custom_aggregation: None,
            limit: None,
            offset: 0,
        }
    }

    /// Attach labels to filter the series.
    #[must_use]
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }

    /// Apply pagination.
    #[must_use]
    pub fn with_pagination(mut self, offset: usize, limit: Option<usize>) -> Self {
        self.offset = offset;
        self.limit = limit;
        self
    }

    /// Apply downsampling using the given interval and aggregation.
    #[must_use]
    pub fn with_downsample(mut self, interval: i64, aggregation: Aggregation) -> Self {
        self.downsample = Some(DownsampleOptions { interval });
        self.aggregation = aggregation;
        self
    }

    /// Apply aggregation without downsampling (reduces the whole series to one point).
    #[must_use]
    pub fn with_aggregation(mut self, aggregation: Aggregation) -> Self {
        self.aggregation = aggregation;
        self
    }

    /// Apply a custom bytes aggregation by providing a codec and typed aggregator.
    #[must_use]
    pub fn with_custom_bytes_aggregation<C, A>(mut self, codec: C, aggregator: A) -> Self
    where
        C: Codec + 'static,
        A: TypedAggregator<C::Item> + 'static,
    {
        self.custom_aggregation = Some(Arc::new(CodecAggregator::new(codec, aggregator)));
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericDomain {
    F64,
    I64,
    U64,
    Mixed,
}

fn aggregation_name(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::None => "none",
        Aggregation::Sum => "sum",
        Aggregation::Min => "min",
        Aggregation::Max => "max",
        Aggregation::Avg => "avg",
        Aggregation::First => "first",
        Aggregation::Last => "last",
        Aggregation::Count => "count",
        Aggregation::Median => "median",
        Aggregation::Range => "range",
        Aggregation::Variance => "variance",
        Aggregation::StdDev => "stddev",
    }
}

fn is_nan_f64(value: &Value) -> bool {
    matches!(value, Value::F64(v) if v.is_nan())
}

fn numeric_domain(points: &[DataPoint], aggregation: Aggregation) -> Result<Option<NumericDomain>> {
    let mut has_f64 = false;
    let mut has_i64 = false;
    let mut has_u64 = false;

    for point in points {
        match &point.value {
            Value::F64(v) => {
                if !v.is_nan() {
                    has_f64 = true;
                }
            }
            Value::I64(_) => has_i64 = true,
            Value::U64(_) => has_u64 = true,
            _ => {
                return Err(TsinkError::UnsupportedAggregation {
                    aggregation: aggregation_name(aggregation).to_string(),
                    value_type: point.value.kind().to_string(),
                });
            }
        }
    }

    let count = has_f64 as u8 + has_i64 as u8 + has_u64 as u8;
    if count == 0 {
        Ok(None)
    } else if count > 1 {
        Ok(Some(NumericDomain::Mixed))
    } else if has_f64 {
        Ok(Some(NumericDomain::F64))
    } else if has_i64 {
        Ok(Some(NumericDomain::I64))
    } else {
        Ok(Some(NumericDomain::U64))
    }
}

fn numeric_values_f64(points: &[DataPoint], aggregation: Aggregation) -> Result<Vec<f64>> {
    let mut values = Vec::with_capacity(points.len());
    for point in points {
        match &point.value {
            Value::F64(v) if v.is_nan() => {}
            Value::F64(v) => values.push(*v),
            Value::I64(v) => {
                let Some(value) = i64_to_f64_exact(*v) else {
                    return Err(TsinkError::UnsupportedAggregation {
                        aggregation: aggregation_name(aggregation).to_string(),
                        value_type: format!(
                            "{} (cannot be represented exactly as f64)",
                            point.value.kind()
                        ),
                    });
                };
                values.push(value);
            }
            Value::U64(v) => {
                let Some(value) = u64_to_f64_exact(*v) else {
                    return Err(TsinkError::UnsupportedAggregation {
                        aggregation: aggregation_name(aggregation).to_string(),
                        value_type: format!(
                            "{} (cannot be represented exactly as f64)",
                            point.value.kind()
                        ),
                    });
                };
                values.push(value);
            }
            _ => {
                return Err(TsinkError::UnsupportedAggregation {
                    aggregation: aggregation_name(aggregation).to_string(),
                    value_type: point.value.kind().to_string(),
                });
            }
        }
    }
    Ok(values)
}

fn integral_f64_to_i128(value: f64) -> Option<i128> {
    if !value.is_finite() {
        return None;
    }

    let bits = value.to_bits();
    let sign = (bits >> 63) != 0;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);

    let magnitude = if exponent_bits == 0 {
        if fraction == 0 {
            0u128
        } else {
            return None;
        }
    } else {
        let exponent = exponent_bits - 1023;
        let significand = ((1u64 << 52) | fraction) as u128;

        if exponent < 0 {
            return None;
        }

        if exponent >= 52 {
            let shift = (exponent - 52) as u32;
            significand.checked_shl(shift)?
        } else {
            let shift = (52 - exponent) as u32;
            let mask = (1u128 << shift) - 1;
            if (significand & mask) != 0 {
                return None;
            }
            significand >> shift
        }
    };

    if sign {
        let min_abs = (i128::MAX as u128) + 1;
        if magnitude == min_abs {
            Some(i128::MIN)
        } else if magnitude <= i128::MAX as u128 {
            Some(-(magnitude as i128))
        } else {
            None
        }
    } else if magnitude <= i128::MAX as u128 {
        Some(magnitude as i128)
    } else {
        None
    }
}

fn cmp_f64_with_i128(value: f64, integer: i128) -> std::cmp::Ordering {
    if value.is_nan() {
        return value.total_cmp(&(integer as f64));
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }

    let rounded = integer as f64;
    let ord = value.total_cmp(&rounded);
    if !ord.is_eq() {
        return ord;
    }

    integral_f64_to_i128(value)
        .map(|parsed| parsed.cmp(&integer))
        .unwrap_or(std::cmp::Ordering::Equal)
}

fn value_cmp(lhs: &Value, rhs: &Value) -> Result<std::cmp::Ordering> {
    match (lhs, rhs) {
        (Value::F64(a), Value::F64(b)) => Ok(a.total_cmp(b)),
        (Value::F64(a), Value::I64(b)) => Ok(cmp_f64_with_i128(*a, *b as i128)),
        (Value::F64(a), Value::U64(b)) => Ok(cmp_f64_with_i128(*a, *b as i128)),
        (Value::I64(a), Value::F64(b)) => Ok(cmp_f64_with_i128(*b, *a as i128).reverse()),
        (Value::I64(a), Value::I64(b)) => Ok(a.cmp(b)),
        (Value::I64(a), Value::U64(b)) => Ok((*a as i128).cmp(&(*b as i128))),
        (Value::U64(a), Value::F64(b)) => Ok(cmp_f64_with_i128(*b, *a as i128).reverse()),
        (Value::U64(a), Value::I64(b)) => Ok((*a as i128).cmp(&(*b as i128))),
        (Value::U64(a), Value::U64(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Ok(a.cmp(b)),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        _ => Err(TsinkError::ValueTypeMismatch {
            expected: lhs.kind().to_string(),
            actual: rhs.kind().to_string(),
        }),
    }
}

fn min_point(points: &[DataPoint]) -> Result<Option<DataPoint>> {
    let mut best: Option<DataPoint> = None;
    for point in points {
        if is_nan_f64(&point.value) {
            continue;
        }

        match &best {
            Some(current) if value_cmp(&point.value, &current.value)?.is_lt() => {
                best = Some(point.clone())
            }
            None => best = Some(point.clone()),
            _ => {}
        }
    }
    Ok(best)
}

fn max_point(points: &[DataPoint]) -> Result<Option<DataPoint>> {
    let mut best: Option<DataPoint> = None;
    for point in points {
        if is_nan_f64(&point.value) {
            continue;
        }

        match &best {
            Some(current) if value_cmp(&point.value, &current.value)?.is_gt() => {
                best = Some(point.clone())
            }
            None => best = Some(point.clone()),
            _ => {}
        }
    }
    Ok(best)
}

fn sum_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let Some(domain) = numeric_domain(points, Aggregation::Sum)? else {
        return Ok(None);
    };

    let value = match domain {
        NumericDomain::F64 => {
            let sum: f64 = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::F64(v) if !v.is_nan() => Some(*v),
                    _ => None,
                })
                .sum();
            Value::F64(sum)
        }
        NumericDomain::I64 => {
            let sum = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::I64(v) => Some(*v as i128),
                    _ => None,
                })
                .sum::<i128>()
                .clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            Value::I64(sum)
        }
        NumericDomain::U64 => {
            let sum = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::U64(v) => Some(*v as u128),
                    _ => None,
                })
                .sum::<u128>()
                .min(u64::MAX as u128) as u64;
            Value::U64(sum)
        }
        NumericDomain::Mixed => {
            let sum: f64 = numeric_values_f64(points, Aggregation::Sum)?
                .into_iter()
                .sum();
            Value::F64(sum)
        }
    };

    Ok(Some(value))
}

fn average_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let values = numeric_values_f64(points, Aggregation::Avg)?;
    if values.is_empty() {
        return Ok(None);
    }
    let sum: f64 = values.iter().sum();
    Ok(Some(Value::F64(sum / values.len() as f64)))
}

fn median_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let mut values = numeric_values_f64(points, Aggregation::Median)?;
    if values.is_empty() {
        return Ok(None);
    }

    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    let median = if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    };

    Ok(Some(Value::F64(median)))
}

fn range_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let Some(domain) = numeric_domain(points, Aggregation::Range)? else {
        return Ok(None);
    };

    let value = match domain {
        NumericDomain::F64 => {
            let values: Vec<f64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::F64(v) if !v.is_nan() => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap();
            Value::F64(max - min)
        }
        NumericDomain::I64 => {
            let values: Vec<i64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::I64(v) => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values.iter().min().copied().unwrap() as i128;
            let max = values.iter().max().copied().unwrap() as i128;
            Value::I64((max - min).clamp(i64::MIN as i128, i64::MAX as i128) as i64)
        }
        NumericDomain::U64 => {
            let values: Vec<u64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::U64(v) => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values.iter().min().copied().unwrap();
            let max = values.iter().max().copied().unwrap();
            Value::U64(max.saturating_sub(min))
        }
        NumericDomain::Mixed => {
            // Preserve integer precision when the mixed set is i64/u64 only.
            let mut int_min: Option<i128> = None;
            let mut int_max: Option<i128> = None;
            let mut has_f64 = false;

            for point in points {
                match &point.value {
                    Value::I64(v) => {
                        let value = *v as i128;
                        int_min = Some(int_min.map_or(value, |min| min.min(value)));
                        int_max = Some(int_max.map_or(value, |max| max.max(value)));
                    }
                    Value::U64(v) => {
                        let value = *v as i128;
                        int_min = Some(int_min.map_or(value, |min| min.min(value)));
                        int_max = Some(int_max.map_or(value, |max| max.max(value)));
                    }
                    Value::F64(v) if v.is_nan() => {}
                    Value::F64(_) => {
                        has_f64 = true;
                        break;
                    }
                    _ => {
                        return Err(TsinkError::UnsupportedAggregation {
                            aggregation: aggregation_name(Aggregation::Range).to_string(),
                            value_type: point.value.kind().to_string(),
                        });
                    }
                }
            }

            if !has_f64 {
                let (Some(min), Some(max)) = (int_min, int_max) else {
                    return Ok(None);
                };
                return Ok(Some(Value::F64((max - min) as f64)));
            }

            let values = numeric_values_f64(points, Aggregation::Range)?;
            if values.is_empty() {
                return Ok(None);
            }
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap();
            Value::F64(max - min)
        }
    };

    Ok(Some(value))
}

fn population_variance_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let values = numeric_values_f64(points, Aggregation::Variance)?;
    if values.is_empty() {
        return Ok(None);
    }

    let mut count = 0.0f64;
    let mut mean = 0.0f64;
    let mut m2 = 0.0f64;

    for value in values {
        count += 1.0;
        let delta = value - mean;
        mean += delta / count;
        let delta2 = value - mean;
        m2 += delta * delta2;
    }

    Ok(Some(Value::F64(m2 / count)))
}

pub(crate) fn aggregate_series(
    points: &[DataPoint],
    aggregation: Aggregation,
) -> Result<Option<DataPoint>> {
    let Some(last) = points.last() else {
        return Ok(None);
    };

    let aggregated = match aggregation {
        Aggregation::None => None,
        Aggregation::First => return Ok(points.first().cloned()),
        Aggregation::Last => return Ok(points.last().cloned()),
        Aggregation::Count => Some(DataPoint::new(
            last.timestamp,
            Value::U64(
                points
                    .iter()
                    .filter(|point| !is_nan_f64(&point.value))
                    .count() as u64,
            ),
        )),
        Aggregation::Sum => sum_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value)),
        Aggregation::Avg => {
            average_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::Min => min_point(points)?,
        Aggregation::Max => max_point(points)?,
        Aggregation::Median => {
            median_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::Range => {
            range_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::Variance => {
            population_variance_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::StdDev => population_variance_numeric(points)?
            .and_then(|value| match value {
                Value::F64(v) => Some(Value::F64(v.sqrt())),
                _ => None,
            })
            .map(|value| DataPoint::new(last.timestamp, value)),
    };

    Ok(aggregated)
}

fn aggregate_bucket(
    points: &[DataPoint],
    aggregation: Aggregation,
    bucket_start: i64,
) -> Result<Option<DataPoint>> {
    if points.is_empty() {
        return Ok(None);
    }

    let aggregated = match aggregation {
        Aggregation::None => None,
        Aggregation::First => points
            .first()
            .map(|point| DataPoint::new(bucket_start, point.value.clone())),
        Aggregation::Last => points
            .last()
            .map(|point| DataPoint::new(bucket_start, point.value.clone())),
        Aggregation::Count => Some(DataPoint::new(
            bucket_start,
            Value::U64(
                points
                    .iter()
                    .filter(|point| !is_nan_f64(&point.value))
                    .count() as u64,
            ),
        )),
        Aggregation::Sum => sum_numeric(points)?.map(|value| DataPoint::new(bucket_start, value)),
        Aggregation::Avg => {
            average_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::Min => {
            min_point(points)?.map(|point| DataPoint::new(bucket_start, point.value))
        }
        Aggregation::Max => {
            max_point(points)?.map(|point| DataPoint::new(bucket_start, point.value))
        }
        Aggregation::Median => {
            median_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::Range => {
            range_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::Variance => {
            population_variance_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::StdDev => population_variance_numeric(points)?
            .and_then(|value| match value {
                Value::F64(v) => Some(Value::F64(v.sqrt())),
                _ => None,
            })
            .map(|value| DataPoint::new(bucket_start, value)),
    };

    Ok(aggregated)
}

fn bucket_start_for(ts: i64, start: i64, interval: i64) -> i64 {
    let rel = ts as i128 - start as i128;
    let bucket = start as i128 + rel.div_euclid(interval as i128) * interval as i128;
    bucket.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

pub(crate) fn downsample_points(
    points: &[DataPoint],
    interval: i64,
    aggregation: Aggregation,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    if points.is_empty() || interval <= 0 || start >= end {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut idx = 0;

    while idx < points.len() && points[idx].timestamp < start {
        idx += 1;
    }

    while idx < points.len() {
        if points[idx].timestamp >= end {
            break;
        }

        let bucket_start = bucket_start_for(points[idx].timestamp, start, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        let bucket_begin = idx;

        while idx < points.len() {
            let ts = points[idx].timestamp;
            if ts >= end || ts >= bucket_end {
                break;
            }
            idx += 1;
        }

        if let Some(dp) = aggregate_bucket(&points[bucket_begin..idx], aggregation, bucket_start)? {
            result.push(dp);
        }
    }

    Ok(result)
}

pub(crate) fn downsample_points_with_custom(
    points: &[DataPoint],
    interval: i64,
    aggregation: &dyn BytesAggregation,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    if points.is_empty() || interval <= 0 || start >= end {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut idx = 0;

    while idx < points.len() && points[idx].timestamp < start {
        idx += 1;
    }

    while idx < points.len() {
        if points[idx].timestamp >= end {
            break;
        }

        let bucket_start = bucket_start_for(points[idx].timestamp, start, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        let bucket_begin = idx;

        while idx < points.len() {
            let ts = points[idx].timestamp;
            if ts >= end || ts >= bucket_end {
                break;
            }
            idx += 1;
        }

        if let Some(dp) = aggregation.aggregate_bucket(&points[bucket_begin..idx], bucket_start)? {
            result.push(dp);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{aggregate_bucket, aggregate_series};
    use crate::{Aggregation, DataPoint, TsinkError, Value};

    #[test]
    fn mixed_i64_u64_min_max_use_integer_order_for_large_values() {
        let larger = i64::MAX;
        let smaller = (i64::MAX as u64).saturating_sub(1);

        let min_points = vec![
            DataPoint::new(1, Value::I64(larger)),
            DataPoint::new(2, Value::U64(smaller)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::U64(smaller));

        let max_points = vec![
            DataPoint::new(1, Value::U64(smaller)),
            DataPoint::new(2, Value::I64(larger)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::I64(larger));
    }

    #[test]
    fn mixed_i64_u64_range_preserves_unit_difference_at_high_values() {
        let points = vec![
            DataPoint::new(1, Value::I64(i64::MAX - 1)),
            DataPoint::new(2, Value::U64(i64::MAX as u64)),
        ];

        let range = aggregate_series(&points, Aggregation::Range)
            .unwrap()
            .unwrap();
        assert_eq!(range.value, Value::F64(1.0));
    }

    #[test]
    fn mixed_i64_f64_min_max_preserve_order_for_large_values() {
        let larger_f64 = i64::MAX as f64;
        let smaller_i64 = i64::MAX - 1;

        let min_points = vec![
            DataPoint::new(1, Value::F64(larger_f64)),
            DataPoint::new(2, Value::I64(smaller_i64)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::I64(smaller_i64));

        let max_points = vec![
            DataPoint::new(1, Value::I64(smaller_i64)),
            DataPoint::new(2, Value::F64(larger_f64)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::F64(larger_f64));
    }

    #[test]
    fn mixed_u64_f64_min_max_preserve_order_for_large_values() {
        let larger_f64 = u64::MAX as f64;
        let smaller_u64 = u64::MAX - 1;

        let min_points = vec![
            DataPoint::new(1, Value::F64(larger_f64)),
            DataPoint::new(2, Value::U64(smaller_u64)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::U64(smaller_u64));

        let max_points = vec![
            DataPoint::new(1, Value::U64(smaller_u64)),
            DataPoint::new(2, Value::F64(larger_f64)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::F64(larger_f64));
    }

    #[test]
    fn variance_rejects_non_representable_large_integers() {
        let points = vec![
            DataPoint::new(1, Value::I64(i64::MAX - 1)),
            DataPoint::new(2, Value::I64(i64::MAX)),
        ];

        let err = aggregate_series(&points, Aggregation::Variance).unwrap_err();
        assert!(matches!(err, TsinkError::UnsupportedAggregation { .. }));
        assert!(err
            .to_string()
            .contains("cannot be represented exactly as f64"));
    }

    #[test]
    fn all_nan_numeric_aggregations_return_none_for_series() {
        let points = vec![
            DataPoint::new(1, Value::F64(f64::NAN)),
            DataPoint::new(2, Value::F64(f64::NAN)),
        ];

        for aggregation in [
            Aggregation::Sum,
            Aggregation::Avg,
            Aggregation::Min,
            Aggregation::Max,
            Aggregation::Median,
            Aggregation::Range,
            Aggregation::Variance,
            Aggregation::StdDev,
        ] {
            assert!(aggregate_series(&points, aggregation).unwrap().is_none());
        }
    }

    #[test]
    fn all_nan_numeric_aggregations_return_none_for_bucket() {
        let points = vec![
            DataPoint::new(1, Value::F64(f64::NAN)),
            DataPoint::new(2, Value::F64(f64::NAN)),
        ];

        for aggregation in [
            Aggregation::Sum,
            Aggregation::Avg,
            Aggregation::Min,
            Aggregation::Max,
            Aggregation::Median,
            Aggregation::Range,
            Aggregation::Variance,
            Aggregation::StdDev,
        ] {
            assert!(aggregate_bucket(&points, aggregation, 1_000)
                .unwrap()
                .is_none());
        }
    }
}
