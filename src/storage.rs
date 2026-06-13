//! Main storage API and query helpers for tsink.

use crate::wal::WalSyncMode;
use crate::{
    Aggregator as TypedAggregator, BytesAggregation, Codec, CodecAggregator, DataPoint, Label,
    Result, Row, TsinkError,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const DEFAULT_CHUNK_POINTS: usize = 2048;

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
        crate::query_selection::select_series_by_scan(self, selection)
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
            chunk_points: DEFAULT_CHUNK_POINTS,
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
