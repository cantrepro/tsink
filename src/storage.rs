//! Main storage API and query helpers for tsink.

use crate::validation::validate_metric;
use crate::wal::{WalReplayMode, WalSyncMode};
use crate::{
    Aggregator as TypedAggregator, BytesAggregation, Codec, CodecAggregator, DataPoint, Label,
    Result, Row, TsinkError,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const DEFAULT_CHUNK_POINTS: usize = 2048;
pub(crate) const DEFAULT_REMOTE_SEGMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageRuntimeMode {
    #[default]
    ReadWrite,
    ComputeOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSegmentCachePolicy {
    #[default]
    MetadataOnly,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricSeries {
    pub name: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesPoints {
    pub series: MetricSeries,
    pub points: Vec<DataPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SeriesMatcherOp {
    Equal,
    NotEqual,
    RegexMatch,
    RegexNoMatch,
}

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataShardScope {
    pub shard_count: u32,
    pub shards: Vec<u32>,
}

impl MetadataShardScope {
    #[must_use]
    pub fn new(shard_count: u32, shards: Vec<u32>) -> Self {
        Self {
            shard_count,
            shards,
        }
    }

    pub fn normalized(&self) -> Result<Self> {
        if self.shard_count == 0 {
            return Err(TsinkError::InvalidConfiguration(
                "metadata shard scope requires shard_count > 0".to_string(),
            ));
        }

        let mut shards = self.shards.clone();
        shards.sort_unstable();
        shards.dedup();
        if let Some(shard) = shards
            .iter()
            .copied()
            .find(|shard| *shard >= self.shard_count)
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "metadata shard scope shard {shard} is out of range for shard_count {}",
                self.shard_count
            )));
        }

        Ok(Self {
            shard_count: self.shard_count,
            shards,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardWindowDigest {
    pub shard: u32,
    pub shard_count: u32,
    pub window_start: i64,
    pub window_end: i64,
    pub series_count: u64,
    pub point_count: u64,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardWindowScanOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_series: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ShardWindowRowsPage {
    pub shard: u32,
    pub shard_count: u32,
    pub window_start: i64,
    pub window_end: i64,
    pub series_scanned: u64,
    pub rows_scanned: u64,
    pub truncated: bool,
    pub next_row_offset: Option<u64>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRowsScanOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct QueryRowsPage {
    pub rows_scanned: u64,
    pub truncated: bool,
    pub next_row_offset: Option<u64>,
    pub rows: Vec<Row>,
}

/// Durability guarantee established for a successful write when the call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAcknowledgement {
    /// The write is visible in memory but was not protected by a crash-recovery log.
    ///
    /// This is the conservative default for backends that do not expose stronger durability
    /// metadata at the API boundary, and for storage configured without a WAL.
    Volatile,
    /// The write was appended to the WAL but may still be lost in a crash window.
    ///
    /// This is typical for [`WalSyncMode::Periodic`] before a later fsync or persistence step
    /// makes the write durable.
    Appended,
    /// The write is known crash-safe when the call returns.
    Durable,
}

impl WriteAcknowledgement {
    #[must_use]
    pub const fn is_durable(self) -> bool {
        matches!(self, Self::Durable)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Volatile => "volatile",
            Self::Appended => "appended",
            Self::Durable => "durable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteResult {
    pub acknowledgement: WriteAcknowledgement,
}

impl WriteResult {
    #[must_use]
    pub const fn new(acknowledgement: WriteAcknowledgement) -> Self {
        Self { acknowledgement }
    }

    #[must_use]
    pub const fn volatile() -> Self {
        Self::new(WriteAcknowledgement::Volatile)
    }

    #[must_use]
    pub const fn appended() -> Self {
        Self::new(WriteAcknowledgement::Appended)
    }

    #[must_use]
    pub const fn durable() -> Self {
        Self::new(WriteAcknowledgement::Durable)
    }

    #[must_use]
    pub const fn is_durable(self) -> bool {
        self.acknowledgement.is_durable()
    }
}

/// Outcome metadata for a delete-series operation.
/// `matched_series` counts selector matches and `tombstones_applied` counts
/// only the matched series whose tombstone state changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteSeriesResult {
    pub matched_series: u64,
    pub tombstones_applied: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StorageObservabilitySnapshot {
    pub memory: MemoryObservabilitySnapshot,
    pub wal: WalObservabilitySnapshot,
    pub retention: RetentionObservabilitySnapshot,
    pub flush: FlushObservabilitySnapshot,
    pub compaction: CompactionObservabilitySnapshot,
    pub query: QueryObservabilitySnapshot,
    pub rollups: RollupObservabilitySnapshot,
    pub remote: RemoteStorageObservabilitySnapshot,
    pub health: StorageHealthSnapshot,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MemoryObservabilitySnapshot {
    pub budgeted_bytes: usize,
    pub excluded_bytes: usize,
    pub active_and_sealed_bytes: usize,
    pub registry_bytes: usize,
    pub metadata_cache_bytes: usize,
    pub persisted_index_bytes: usize,
    #[serde(default)]
    pub persisted_mmap_bytes: usize,
    pub tombstone_bytes: usize,
    pub excluded_persisted_mmap_bytes: usize,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WalObservabilitySnapshot {
    pub enabled: bool,
    /// Configured WAL sync mode (`per-append`, `periodic`, or `disabled`).
    pub sync_mode: String,
    /// Whether a successful write acknowledgement implies crash-safe durability immediately.
    pub acknowledged_writes_durable: bool,
    pub size_bytes: u64,
    pub segment_count: u64,
    pub active_segment: u64,
    /// Highest committed write appended to the WAL.
    pub highwater_segment: u64,
    pub highwater_frame: u64,
    /// Highest committed write known durable via WAL fsync or later segment persistence.
    pub durable_highwater_segment: u64,
    pub durable_highwater_frame: u64,
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

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RetentionObservabilitySnapshot {
    pub max_observed_timestamp: Option<i64>,
    pub recency_reference_timestamp: Option<i64>,
    pub future_skew_window: i64,
    pub future_skew_points_total: u64,
    pub future_skew_max_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FlushObservabilitySnapshot {
    pub pipeline_runs_total: u64,
    pub pipeline_success_total: u64,
    pub pipeline_timeout_total: u64,
    pub pipeline_errors_total: u64,
    pub pipeline_duration_nanos_total: u64,
    #[serde(default)]
    pub admission_backpressure_delays_total: u64,
    #[serde(default)]
    pub admission_backpressure_delay_nanos_total: u64,
    #[serde(default)]
    pub admission_pressure_relief_requests_total: u64,
    #[serde(default)]
    pub admission_pressure_relief_observed_total: u64,
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
    pub tier_moves_total: u64,
    pub tier_move_errors_total: u64,
    pub expired_segments_total: u64,
    pub hot_segments_visible: u64,
    pub warm_segments_visible: u64,
    pub cold_segments_visible: u64,
}

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
    pub merge_path_shard_snapshots_total: u64,
    pub merge_path_shard_snapshot_wait_nanos_total: u64,
    pub merge_path_shard_snapshot_hold_nanos_total: u64,
    pub append_sort_path_queries_total: u64,
    pub hot_only_query_plans_total: u64,
    pub warm_tier_query_plans_total: u64,
    pub cold_tier_query_plans_total: u64,
    pub hot_tier_persisted_chunks_read_total: u64,
    pub warm_tier_persisted_chunks_read_total: u64,
    pub cold_tier_persisted_chunks_read_total: u64,
    pub warm_tier_fetch_duration_nanos_total: u64,
    pub cold_tier_fetch_duration_nanos_total: u64,
    pub rollup_query_plans_total: u64,
    pub partial_rollup_query_plans_total: u64,
    pub rollup_points_read_total: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RemoteStorageObservabilitySnapshot {
    pub enabled: bool,
    pub runtime_mode: StorageRuntimeMode,
    pub cache_policy: RemoteSegmentCachePolicy,
    pub metadata_refresh_interval_ms: u64,
    pub mirror_hot_segments: bool,
    pub catalog_refreshes_total: u64,
    pub catalog_refresh_errors_total: u64,
    pub accessible: bool,
    pub last_refresh_attempt_unix_ms: Option<u64>,
    pub last_successful_refresh_unix_ms: Option<u64>,
    pub consecutive_refresh_failures: u64,
    pub next_refresh_retry_unix_ms: Option<u64>,
    pub backoff_active: bool,
    pub last_refresh_error: Option<String>,
}

impl Default for RemoteStorageObservabilitySnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            metadata_refresh_interval_ms: 0,
            mirror_hot_segments: false,
            catalog_refreshes_total: 0,
            catalog_refresh_errors_total: 0,
            accessible: true,
            last_refresh_attempt_unix_ms: None,
            last_successful_refresh_unix_ms: None,
            consecutive_refresh_failures: 0,
            next_refresh_retry_unix_ms: None,
            backoff_active: false,
            last_refresh_error: None,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StorageHealthSnapshot {
    pub background_errors_total: u64,
    pub maintenance_errors_total: u64,
    pub degraded: bool,
    pub fail_fast_enabled: bool,
    pub fail_fast_triggered: bool,
    pub last_background_error: Option<String>,
    pub last_maintenance_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupPolicy {
    pub id: String,
    pub metric: String,
    #[serde(default)]
    pub match_labels: Vec<Label>,
    pub interval: i64,
    pub aggregation: Aggregation,
    #[serde(default)]
    pub bucket_origin: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupPolicyStatus {
    pub policy: RollupPolicy,
    pub matched_series: u64,
    pub materialized_series: u64,
    pub materialized_through: Option<i64>,
    pub lag: Option<i64>,
    pub last_run_started_at_ms: Option<u64>,
    pub last_run_completed_at_ms: Option<u64>,
    pub last_run_duration_nanos: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupObservabilitySnapshot {
    pub worker_runs_total: u64,
    pub worker_success_total: u64,
    pub worker_errors_total: u64,
    pub policy_runs_total: u64,
    pub buckets_materialized_total: u64,
    pub points_materialized_total: u64,
    pub last_run_duration_nanos: u64,
    #[serde(default)]
    pub policies: Vec<RollupPolicyStatus>,
}

pub trait Storage: Send + Sync {
    /// Inserts rows into the storage.
    ///
    /// This compatibility method only reports success or failure. Use
    /// [`Storage::insert_rows_with_result`] when the caller needs to distinguish between
    /// volatile, append-complete, and durable-complete acknowledgements.
    fn insert_rows(&self, rows: &[Row]) -> Result<()>;

    /// Inserts rows and returns the durability guarantee established when the call succeeds.
    ///
    /// Backends that do not override this method conservatively report
    /// [`WriteAcknowledgement::Volatile`] for non-empty writes because they do not expose a
    /// stronger crash-safety contract at the API boundary.
    fn insert_rows_with_result(&self, rows: &[Row]) -> Result<WriteResult> {
        self.insert_rows(rows)?;
        Ok(if rows.is_empty() {
            WriteResult::durable()
        } else {
            WriteResult::volatile()
        })
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>>;

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

    fn select_many(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
    ) -> Result<Vec<SeriesPoints>> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let mut out = Vec::with_capacity(series.len());
        for item in series {
            let points = match self.select(&item.name, &item.labels, start, end) {
                Ok(points) => points,
                Err(TsinkError::NoDataPoints { .. }) => Vec::new(),
                Err(err) => return Err(err),
            };
            out.push(SeriesPoints {
                series: item.clone(),
                points,
            });
        }
        Ok(out)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>>;

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>>;

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        Err(TsinkError::Other(
            "list_metrics is not implemented for this storage backend".to_string(),
        ))
    }

    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        self.list_metrics()
    }

    /// Lists known metric series within a shard scope.
    ///
    /// Backends must override this to provide a bounded shard-scoped implementation.
    fn list_metrics_in_shards(&self, scope: &MetadataShardScope) -> Result<Vec<MetricSeries>> {
        let scope = scope.normalized()?;
        if scope.shards.is_empty() {
            return Ok(Vec::new());
        }
        Err(TsinkError::UnsupportedOperation {
            operation: "list_metrics_in_shards",
            reason: "bounded shard-scoped metadata is not implemented by this storage backend"
                .to_string(),
        })
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        crate::query_selection::select_series_by_scan(self, selection)
    }

    /// Selects metric series by structured label matchers within a shard scope.
    ///
    /// Backends must override this to provide a bounded shard-scoped implementation.
    fn select_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        let _ = crate::query_selection::prepare_series_selection(selection)?;
        let scope = scope.normalized()?;
        if scope.shards.is_empty() {
            return Ok(Vec::new());
        }
        Err(TsinkError::UnsupportedOperation {
            operation: "select_series_in_shards",
            reason: "bounded shard-scoped metadata is not implemented by this storage backend"
                .to_string(),
        })
    }

    fn compute_shard_window_digest(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
    ) -> Result<ShardWindowDigest> {
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;

        let scope = MetadataShardScope::new(shard_count, vec![shard]).normalized()?;
        let mut series = self.list_metrics_in_shards(&scope)?;
        series.sort_by_cached_key(|entry| {
            shard_window_series_identity_key(entry.name.as_str(), &entry.labels)
        });

        let mut points = Vec::new();
        let mut point_hashes = Vec::new();
        let mut fingerprint = SHARD_WINDOW_FNV_OFFSET_BASIS;
        let mut series_count = 0u64;
        let mut point_count = 0u64;
        for metric_series in series {
            self.select_into(
                metric_series.name.as_str(),
                &metric_series.labels,
                window_start,
                window_end,
                &mut points,
            )?;
            if points.is_empty() {
                continue;
            }

            let identity_key = shard_window_series_identity_key(
                metric_series.name.as_str(),
                &metric_series.labels,
            );
            point_hashes.clear();
            point_hashes.extend(points.iter().map(shard_window_hash_data_point));
            point_hashes.sort_unstable();

            shard_window_fnv1a_update(&mut fingerprint, identity_key.as_bytes());
            shard_window_fnv1a_update(
                &mut fingerprint,
                &u64::try_from(point_hashes.len())
                    .unwrap_or(u64::MAX)
                    .to_le_bytes(),
            );
            for point_hash in &point_hashes {
                shard_window_fnv1a_update(&mut fingerprint, &point_hash.to_le_bytes());
            }

            series_count = series_count.saturating_add(1);
            point_count =
                point_count.saturating_add(u64::try_from(point_hashes.len()).unwrap_or(u64::MAX));
        }

        Ok(ShardWindowDigest {
            shard,
            shard_count,
            window_start,
            window_end,
            series_count,
            point_count,
            fingerprint,
        })
    }

    fn scan_shard_window_rows(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: ShardWindowScanOptions,
    ) -> Result<ShardWindowRowsPage> {
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;
        validate_shard_window_scan_options(options)?;

        let scope = MetadataShardScope::new(shard_count, vec![shard]).normalized()?;
        let mut series = self.list_metrics_in_shards(&scope)?;
        series.sort_by_cached_key(|entry| {
            shard_window_series_identity_key(entry.name.as_str(), &entry.labels)
        });

        let max_series =
            u64::try_from(options.max_series.unwrap_or(usize::MAX)).unwrap_or(u64::MAX);
        let max_rows = options.max_rows.unwrap_or(usize::MAX);
        let row_offset = options.row_offset.unwrap_or(0);

        let mut response = ShardWindowRowsPage {
            shard,
            shard_count,
            window_start,
            window_end,
            series_scanned: 0,
            rows_scanned: 0,
            truncated: false,
            next_row_offset: None,
            rows: Vec::new(),
        };

        let mut points = Vec::new();
        let mut stream_row_offset = 0u64;
        let mut remaining_series_budget = max_series;
        'series_scan: for metric_series in series {
            self.select_into(
                metric_series.name.as_str(),
                &metric_series.labels,
                window_start,
                window_end,
                &mut points,
            )?;
            if points.is_empty() {
                continue;
            }

            sort_data_points_for_shard_window(&mut points);

            let mut counted_series_for_budget = false;
            for point in points.iter() {
                if stream_row_offset < row_offset {
                    stream_row_offset = stream_row_offset.saturating_add(1);
                    continue;
                }

                if !counted_series_for_budget {
                    if remaining_series_budget == 0 {
                        response.truncated = true;
                        response.next_row_offset = Some(stream_row_offset);
                        break 'series_scan;
                    }
                    remaining_series_budget = remaining_series_budget.saturating_sub(1);
                    response.series_scanned = response.series_scanned.saturating_add(1);
                    counted_series_for_budget = true;
                }

                if response.rows.len() >= max_rows {
                    response.truncated = true;
                    response.next_row_offset = Some(stream_row_offset);
                    break 'series_scan;
                }

                response.rows_scanned = response.rows_scanned.saturating_add(1);
                response.rows.push(Row::with_labels(
                    metric_series.name.clone(),
                    metric_series.labels.clone(),
                    point.clone(),
                ));
                stream_row_offset = stream_row_offset.saturating_add(1);
            }
        }

        Ok(response)
    }

    fn scan_series_rows(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        validate_query_rows_scan_options(options)?;

        let max_rows = options.max_rows.unwrap_or(usize::MAX);
        let row_offset = options.row_offset.unwrap_or(0);

        let mut response = QueryRowsPage {
            rows_scanned: 0,
            truncated: false,
            next_row_offset: None,
            rows: Vec::new(),
        };

        let mut points = Vec::new();
        let mut stream_row_offset = 0u64;
        'series_scan: for metric_series in series {
            self.select_into(
                metric_series.name.as_str(),
                &metric_series.labels,
                start,
                end,
                &mut points,
            )?;
            if points.is_empty() {
                continue;
            }

            for point in points.iter() {
                if stream_row_offset < row_offset {
                    stream_row_offset = stream_row_offset.saturating_add(1);
                    continue;
                }

                if response.rows.len() >= max_rows {
                    response.truncated = true;
                    response.next_row_offset = Some(stream_row_offset);
                    break 'series_scan;
                }

                response.rows_scanned = response.rows_scanned.saturating_add(1);
                response.rows.push(Row::with_labels(
                    metric_series.name.clone(),
                    metric_series.labels.clone(),
                    point.clone(),
                ));
                stream_row_offset = stream_row_offset.saturating_add(1);
            }
        }

        Ok(response)
    }

    fn scan_metric_rows(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        validate_metric(metric)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        validate_query_rows_scan_options(options)?;

        let series = self
            .list_metrics()?
            .into_iter()
            .filter(|entry| entry.name == metric)
            .collect::<Vec<_>>();
        self.scan_series_rows(&series, start, end, options)
    }

    /// Adds deletion tombstones for series selected by matchers and optional time range.
    ///
    /// Implementations that cannot durably persist tombstones, such as compute-only
    /// query nodes, must reject the request instead of reporting an ephemeral success.
    fn delete_series(&self, _selection: &SeriesSelection) -> Result<DeleteSeriesResult> {
        Err(TsinkError::InvalidConfiguration(
            "delete_series is not implemented for this storage backend".to_string(),
        ))
    }

    fn memory_used(&self) -> usize {
        0
    }

    /// Returns configured in-memory byte budget for the storage engine.
    ///
    /// `usize::MAX` means "no explicit budget configured".
    fn memory_budget(&self) -> usize {
        usize::MAX
    }

    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        StorageObservabilitySnapshot::default()
    }

    fn apply_rollup_policies(
        &self,
        _policies: Vec<RollupPolicy>,
    ) -> Result<RollupObservabilitySnapshot> {
        Err(TsinkError::InvalidConfiguration(
            "rollup policies are not implemented for this storage backend".to_string(),
        ))
    }

    fn trigger_rollup_run(&self) -> Result<RollupObservabilitySnapshot> {
        Err(TsinkError::InvalidConfiguration(
            "rollup runtime is not implemented for this storage backend".to_string(),
        ))
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

    fn close(&self) -> Result<()>;
}

pub struct StorageBuilder {
    data_path: Option<PathBuf>,
    object_store_path: Option<PathBuf>,
    retention: Duration,
    retention_enforced: bool,
    hot_tier_retention: Option<Duration>,
    warm_tier_retention: Option<Duration>,
    runtime_mode: StorageRuntimeMode,
    remote_segment_cache_policy: RemoteSegmentCachePolicy,
    remote_segment_refresh_interval: Duration,
    mirror_hot_segments_to_object_store: bool,
    timestamp_precision: TimestampPrecision,
    chunk_points: usize,
    max_writers: usize,
    write_timeout: Duration,
    partition_duration: Duration,
    max_active_partition_heads_per_series: usize,
    memory_limit_bytes: usize,
    cardinality_limit: usize,
    wal_enabled: bool,
    wal_size_limit_bytes: usize,
    wal_buffer_size: usize,
    wal_sync_mode: WalSyncMode,
    wal_replay_mode: WalReplayMode,
    background_fail_fast: bool,
    metadata_shard_count: Option<u32>,
    #[cfg(test)]
    current_time_override: Option<i64>,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self {
            data_path: None,
            object_store_path: None,
            retention: Duration::from_secs(14 * 24 * 3600),
            retention_enforced: false,
            hot_tier_retention: None,
            warm_tier_retention: None,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: DEFAULT_REMOTE_SEGMENT_REFRESH_INTERVAL,
            mirror_hot_segments_to_object_store: false,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            chunk_points: DEFAULT_CHUNK_POINTS,
            max_writers: crate::cgroup::default_workers_limit(),
            write_timeout: Duration::from_secs(30),
            partition_duration: Duration::from_secs(3600),
            max_active_partition_heads_per_series: DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            memory_limit_bytes: usize::MAX,
            cardinality_limit: usize::MAX,
            wal_enabled: true,
            wal_size_limit_bytes: usize::MAX,
            wal_buffer_size: 4096,
            wal_sync_mode: WalSyncMode::default(),
            wal_replay_mode: WalReplayMode::Strict,
            background_fail_fast: true,
            metadata_shard_count: None,
            #[cfg(test)]
            current_time_override: None,
        }
    }
}

impl StorageBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.data_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the root path for object-storage-backed warm/cold segment tiers.
    ///
    /// The path should typically reference storage outside the local `data_path`
    /// volume, such as an object-store mount or remote-backed filesystem.
    #[must_use]
    pub fn with_object_store_path(mut self, path: impl AsRef<Path>) -> Self {
        self.object_store_path = Some(path.as_ref().to_path_buf());
        self
    }

    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self.retention_enforced = true;
        self
    }

    /// Enables or disables retention enforcement.
    ///
    /// Builders default to allowing historical/backfill timestamps. When enforcement is
    /// disabled, points are never rejected or filtered due to retention.
    #[must_use]
    pub fn with_retention_enforced(mut self, enforced: bool) -> Self {
        self.retention_enforced = enforced;
        self
    }

    /// Configures hot, warm, and cold tier cutoffs within the global retention window.
    ///
    /// Calling this also enables retention enforcement.
    ///
    /// Data newer than `hot_retention` stays on local storage. Data older than
    /// `hot_retention` moves to the warm object-store tier. Data older than
    /// `warm_retention` moves to the cold object-store tier until global retention
    /// expires it entirely.
    #[must_use]
    pub fn with_tiered_retention_policy(
        mut self,
        hot_retention: Duration,
        warm_retention: Duration,
    ) -> Self {
        self.retention_enforced = true;
        self.hot_tier_retention = Some(hot_retention);
        self.warm_tier_retention = Some(warm_retention);
        self
    }

    #[must_use]
    pub fn with_runtime_mode(mut self, mode: StorageRuntimeMode) -> Self {
        self.runtime_mode = mode;
        self
    }

    #[must_use]
    pub fn with_remote_segment_cache_policy(mut self, policy: RemoteSegmentCachePolicy) -> Self {
        self.remote_segment_cache_policy = policy;
        self
    }

    #[must_use]
    pub fn with_remote_segment_refresh_interval(mut self, interval: Duration) -> Self {
        self.remote_segment_refresh_interval = interval.max(Duration::from_millis(1));
        self
    }

    #[must_use]
    pub fn with_mirror_hot_segments_to_object_store(mut self, enabled: bool) -> Self {
        self.mirror_hot_segments_to_object_store = enabled;
        self
    }

    #[must_use]
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    #[must_use]
    pub fn with_chunk_points(mut self, points: usize) -> Self {
        self.chunk_points = points.clamp(1, u16::MAX as usize);
        self
    }

    #[must_use]
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.max_writers = if max_writers == 0 {
            crate::cgroup::default_workers_limit().max(1)
        } else {
            max_writers
        };
        self
    }

    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.partition_duration = duration;
        self
    }

    /// Sets the maximum number of simultaneously open partition heads per series.
    ///
    /// The engine keeps a bounded set of open partition heads per series. When a write
    /// advances into a newer partition and the bound is full, the oldest open partition
    /// head is sealed to make room. When a write would open a new older partition after
    /// the bound is already full, the write is rejected instead of force-sealing another
    /// active head.
    #[must_use]
    pub fn with_max_active_partition_heads_per_series(mut self, max_heads: usize) -> Self {
        self.max_active_partition_heads_per_series = max_heads.max(1);
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

    #[must_use]
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.wal_buffer_size = size;
        self
    }

    #[must_use]
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.wal_sync_mode = mode;
        self
    }

    /// Sets WAL replay policy when corruption is encountered mid-log.
    ///
    /// Builders default to [`WalReplayMode::Strict`] so durable startup never silently drops
    /// corrupted WAL history unless salvage is opted into explicitly.
    #[must_use]
    pub fn with_wal_replay_mode(mut self, mode: WalReplayMode) -> Self {
        self.wal_replay_mode = mode;
        self
    }

    /// Controls whether background durability worker failures fence service.
    ///
    /// Builders default to `true` so flush, compaction, and persisted-refresh failures stop
    /// admitting new work unless callers opt out explicitly.
    #[must_use]
    pub fn with_background_fail_fast(mut self, enabled: bool) -> Self {
        self.background_fail_fast = enabled;
        self
    }

    #[must_use]
    pub fn with_metadata_shard_count(mut self, shard_count: u32) -> Self {
        self.metadata_shard_count = Some(shard_count);
        self
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_current_time_override_for_tests(mut self, timestamp: i64) -> Self {
        self.current_time_override = Some(timestamp);
        self
    }

    pub fn build(self) -> Result<Arc<dyn Storage>> {
        crate::engine::build_storage(self)
    }

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

    pub(crate) fn object_store_path(&self) -> Option<&Path> {
        self.object_store_path.as_deref()
    }

    pub(crate) fn hot_tier_retention(&self) -> Option<Duration> {
        self.hot_tier_retention
    }

    pub(crate) fn warm_tier_retention(&self) -> Option<Duration> {
        self.warm_tier_retention
    }

    pub(crate) fn timestamp_precision(&self) -> TimestampPrecision {
        self.timestamp_precision
    }

    pub(crate) fn runtime_mode(&self) -> StorageRuntimeMode {
        self.runtime_mode
    }

    pub(crate) fn remote_segment_cache_policy(&self) -> RemoteSegmentCachePolicy {
        self.remote_segment_cache_policy
    }

    pub(crate) fn remote_segment_refresh_interval(&self) -> Duration {
        self.remote_segment_refresh_interval
    }

    pub(crate) fn mirror_hot_segments_to_object_store(&self) -> bool {
        self.mirror_hot_segments_to_object_store
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

    pub(crate) fn max_active_partition_heads_per_series(&self) -> usize {
        self.max_active_partition_heads_per_series.max(1)
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

    pub(crate) fn wal_replay_mode(&self) -> WalReplayMode {
        self.wal_replay_mode
    }

    pub(crate) fn background_fail_fast(&self) -> bool {
        self.background_fail_fast
    }

    pub(crate) fn metadata_shard_count(&self) -> Option<u32> {
        self.metadata_shard_count
    }

    #[cfg(test)]
    pub(crate) fn current_time_override_for_tests(&self) -> Option<i64> {
        self.current_time_override
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn metric_series_matches_shard_scope(
    series: &MetricSeries,
    scope: &MetadataShardScope,
) -> bool {
    if scope.shard_count == 0 {
        return false;
    }

    let shard = (crate::label::stable_series_identity_hash(series.name.as_str(), &series.labels)
        % u64::from(scope.shard_count)) as u32;
    scope.shards.contains(&shard)
}

pub(crate) fn validate_shard_window_request(
    shard: u32,
    shard_count: u32,
    window_start: i64,
    window_end: i64,
) -> Result<()> {
    if shard_count == 0 {
        return Err(TsinkError::InvalidConfiguration(
            "shard_count must be greater than zero".to_string(),
        ));
    }
    if shard >= shard_count {
        return Err(TsinkError::InvalidConfiguration(format!(
            "shard {shard} is out of range for shard_count {shard_count}"
        )));
    }
    if window_start >= window_end {
        return Err(TsinkError::InvalidTimeRange {
            start: window_start,
            end: window_end,
        });
    }
    Ok(())
}

pub(crate) fn validate_shard_window_scan_options(options: ShardWindowScanOptions) -> Result<()> {
    if options.max_series.is_some_and(|value| value == 0) {
        return Err(TsinkError::InvalidConfiguration(
            "max_series must be greater than zero when set".to_string(),
        ));
    }
    if options.max_rows.is_some_and(|value| value == 0) {
        return Err(TsinkError::InvalidConfiguration(
            "max_rows must be greater than zero when set".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_query_rows_scan_options(options: QueryRowsScanOptions) -> Result<()> {
    if options.max_rows.is_some_and(|value| value == 0) {
        return Err(TsinkError::InvalidConfiguration(
            "max_rows must be greater than zero when set".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn shard_window_series_identity_key(metric: &str, labels: &[Label]) -> String {
    crate::label::canonical_series_identity_key(metric, labels)
}

pub(crate) const SHARD_WINDOW_FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const SHARD_WINDOW_FNV_PRIME: u64 = 0x100000001b3;

pub(crate) fn shard_window_hash_data_point(point: &DataPoint) -> u64 {
    let mut hash = SHARD_WINDOW_FNV_OFFSET_BASIS;
    shard_window_fnv1a_update(&mut hash, &point.timestamp.to_le_bytes());
    match serde_json::to_vec(&point.value) {
        Ok(encoded) => shard_window_fnv1a_update(&mut hash, &encoded),
        Err(_) => shard_window_fnv1a_update(&mut hash, format!("{:?}", point.value).as_bytes()),
    }
    hash
}

pub(crate) fn shard_window_fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(SHARD_WINDOW_FNV_PRIME);
    }
}

pub(crate) fn sort_data_points_for_shard_window(points: &mut [DataPoint]) {
    points.sort_by_cached_key(|point| {
        (
            point.timestamp,
            serde_json::to_vec(&point.value).unwrap_or_default(),
        )
    });
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Aggregation {
    #[default]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownsampleOptions {
    pub interval: i64,
}

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

    #[must_use]
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }

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
