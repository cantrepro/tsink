use crate::cluster::config::{ClusterReadConsistency, ClusterReadPartialResponsePolicy};
use crate::cluster::membership::MembershipView;
use crate::cluster::planner::{
    ReadExecutionPlan, ReadPlanOwnerMode, ReadPlanTarget, ReadPlannerError, ShardAwareQueryPlanner,
};
use crate::cluster::query_merge::{
    MergeLimitError, ReadMergeLimits, SeriesIdentity, SeriesMetadataMerger, SeriesPointsMerger,
};
use crate::cluster::replication::stable_series_identity_hash;
use crate::cluster::ring::ShardRing;
use crate::cluster::rpc::{
    InternalListMetricsRequest, InternalListMetricsResponse, InternalSelectRequest,
    InternalSelectSeriesRequest, RpcClient, RpcError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tsink::{
    DataPoint, Label, MetadataShardScope, MetricSeries, NativeHistogram, QueryBudgetError,
    QueryExecution, QueryExecutionAccounting, QueryExecutionSnapshot, QueryLimitReason,
    QueryMemoryReservation, QueryWorkLimits, SeriesSelection, SeriesSelectionPreparationError,
    Storage, TsinkError, Value,
};

pub use tsink::SeriesPoints;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPolicy {
    mode: ClusterReadConsistency,
}

impl ReadPolicy {
    pub fn new(mode: ClusterReadConsistency) -> Self {
        Self { mode }
    }

    pub fn requires_full_consistency(self) -> bool {
        matches!(self.mode, ClusterReadConsistency::Strict)
    }

    pub fn mode(self) -> ClusterReadConsistency {
        self.mode
    }

    pub fn required_acks(self, replica_count: usize) -> usize {
        let replicas = replica_count.max(1);
        match self.mode {
            ClusterReadConsistency::Eventual => 1,
            ClusterReadConsistency::Quorum => (replicas / 2) + 1,
            ClusterReadConsistency::Strict => replicas,
        }
    }

    fn metadata_owner_mode(self) -> ReadPlanOwnerMode {
        match self.mode {
            ClusterReadConsistency::Eventual => ReadPlanOwnerMode::PrimaryOnly,
            ClusterReadConsistency::Quorum | ClusterReadConsistency::Strict => {
                ReadPlanOwnerMode::AllReplicas
            }
        }
    }

    fn points_owner_mode(self) -> ReadPlanOwnerMode {
        self.metadata_owner_mode()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFanoutResponseMetadata {
    pub consistency: ClusterReadConsistency,
    pub partial_response_policy: ClusterReadPartialResponsePolicy,
    pub partial_response: bool,
    pub warnings: Vec<String>,
}

impl ReadFanoutResponseMetadata {
    fn success(
        consistency: ClusterReadConsistency,
        partial_response_policy: ClusterReadPartialResponsePolicy,
    ) -> Self {
        Self {
            consistency,
            partial_response_policy,
            partial_response: false,
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadFanoutResponse<T> {
    pub value: T,
    pub metadata: ReadFanoutResponseMetadata,
    pub(crate) _reservation: Option<Arc<QueryMemoryReservation>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadFanoutMetricsSnapshot {
    pub requests_total: u64,
    pub failures_total: u64,
    pub duration_nanos_total: u64,
    pub remote_requests_total: u64,
    pub remote_failures_total: u64,
    pub resource_rejections_total: u64,
    pub resource_acquire_wait_nanos_total: u64,
    pub resource_active_queries: u64,
    pub resource_active_merged_points: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFanoutOperationMetricsSnapshot {
    pub operation: String,
    pub requests_total: u64,
    pub failures_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFanoutPeerMetricsSnapshot {
    pub node_id: String,
    pub operation: String,
    pub remote_requests_total: u64,
    pub remote_failures_total: u64,
    pub remote_request_duration_nanos_total: u64,
    pub remote_request_duration_count: u64,
    pub remote_request_duration_buckets: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFanoutLabeledMetricsSnapshot {
    pub operations: Vec<ReadFanoutOperationMetricsSnapshot>,
    pub peers: Vec<ReadFanoutPeerMetricsSnapshot>,
}

const FANOUT_OPERATION_SELECT_SERIES: &str = "select_series";
const FANOUT_OPERATION_LIST_METRICS: &str = "list_metrics";
const FANOUT_OPERATION_SELECT_POINTS: &str = "select_points";
const FANOUT_REMOTE_OPERATION_SELECT_BATCH: &str = "select_batch";
const FANOUT_REMOTE_OPERATION_SELECT_LEGACY: &str = "select_legacy";
const REMOTE_SELECT_BATCH_SIZE: usize = 128;

const FANOUT_REMOTE_REQUEST_LATENCY_BUCKETS_NANOS: [u64; 8] = [
    1_000_000,     // 1ms
    5_000_000,     // 5ms
    10_000_000,    // 10ms
    25_000_000,    // 25ms
    50_000_000,    // 50ms
    100_000_000,   // 100ms
    250_000_000,   // 250ms
    1_000_000_000, // 1s
];

pub const FANOUT_REMOTE_REQUEST_LATENCY_BUCKETS_SECONDS: [&str; 8] = [
    "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "1",
];

pub const DEFAULT_READ_MAX_INFLIGHT_QUERIES: usize = 64;
pub const DEFAULT_READ_MAX_INFLIGHT_MERGED_POINTS: usize = 20_000_000;
pub const DEFAULT_READ_RESOURCE_ACQUIRE_TIMEOUT_MS: u64 = 25;

const READ_RESOURCE_GLOBAL_QUERY_SLOTS: &str = "global_inflight_queries";
const READ_RESOURCE_GLOBAL_MERGED_POINTS: &str = "global_inflight_merged_points";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadResourceGuardrails {
    pub max_inflight_queries: usize,
    pub max_inflight_merged_points: usize,
    pub acquire_timeout: Duration,
}

impl ReadResourceGuardrails {
    pub fn validate(self) -> Result<(), String> {
        if self.max_inflight_queries == 0 {
            return Err("read max in-flight queries must be greater than zero".to_string());
        }
        if self.max_inflight_merged_points == 0 {
            return Err("read max in-flight merged points must be greater than zero".to_string());
        }
        if self.max_inflight_merged_points > u32::MAX as usize {
            return Err(format!(
                "read max in-flight merged points must be <= {}, got {}",
                u32::MAX,
                self.max_inflight_merged_points
            ));
        }
        Ok(())
    }
}

impl Default for ReadResourceGuardrails {
    fn default() -> Self {
        Self {
            max_inflight_queries: DEFAULT_READ_MAX_INFLIGHT_QUERIES,
            max_inflight_merged_points: DEFAULT_READ_MAX_INFLIGHT_MERGED_POINTS,
            acquire_timeout: Duration::from_millis(DEFAULT_READ_RESOURCE_ACQUIRE_TIMEOUT_MS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadFanoutExecutor {
    local_node_id: String,
    ring: ShardRing,
    planner: ShardAwareQueryPlanner,
    fanout_concurrency: usize,
    policy: ReadPolicy,
    partial_response_policy: ClusterReadPartialResponsePolicy,
    merge_limits: ReadMergeLimits,
    resources: Arc<ReadResourceGuard>,
}

#[derive(Debug, Clone)]
pub enum ReadFanoutError {
    InvalidRequest {
        message: String,
    },
    QueryBudget {
        error: QueryBudgetError,
    },
    MissingShardOwners {
        shard: u32,
    },
    MissingOwnerEndpoint {
        node_id: String,
    },
    LocalSelectSeries {
        message: String,
    },
    LocalListMetrics {
        message: String,
    },
    LocalSelectBatch {
        series_count: usize,
        message: String,
    },
    RemoteSelectSeries {
        node_id: String,
        endpoint: String,
        source: Box<RpcError>,
    },
    RemoteListMetrics {
        node_id: String,
        endpoint: String,
        source: Box<RpcError>,
    },
    RemoteSelectBatch {
        node_id: String,
        endpoint: String,
        series_count: usize,
        source: Box<RpcError>,
    },
    MergeLimitExceeded {
        message: String,
    },
    ResourceLimitExceeded {
        resource: &'static str,
        requested: usize,
        limit: usize,
        retryable: bool,
    },
    ConsistencyUnmet {
        operation: String,
        mode: ClusterReadConsistency,
        target: String,
        required_acks: usize,
        acknowledged_acks: usize,
        total_replicas: usize,
        _reservation: Option<Arc<QueryMemoryReservation>>,
    },
    TaskJoin {
        message: String,
    },
}

impl ReadFanoutError {
    fn with_consistency_reservation(mut self, reservation: Option<QueryMemoryReservation>) -> Self {
        if let Self::ConsistencyUnmet {
            _reservation: slot, ..
        } = &mut self
        {
            *slot = reservation.map(Arc::new);
        }
        self
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::InvalidRequest { .. } => false,
            Self::QueryBudget { .. } => false,
            Self::MissingShardOwners { .. } | Self::MissingOwnerEndpoint { .. } => false,
            Self::MergeLimitExceeded { .. } => false,
            Self::ResourceLimitExceeded { retryable, .. } => *retryable,
            Self::TaskJoin { .. } => true,
            Self::ConsistencyUnmet { .. } => true,
            Self::LocalSelectSeries { .. }
            | Self::LocalListMetrics { .. }
            | Self::LocalSelectBatch { .. } => true,
            Self::RemoteSelectSeries { source, .. }
            | Self::RemoteListMetrics { source, .. }
            | Self::RemoteSelectBatch { source, .. } => source.retryable(),
        }
    }
}

impl fmt::Display for ReadFanoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message } => {
                write!(f, "{message}")
            }
            Self::QueryBudget { error } => {
                write!(f, "{error}")
            }
            Self::MissingShardOwners { shard } => {
                write!(f, "read fanout failed: shard {shard} has no owner")
            }
            Self::MissingOwnerEndpoint { node_id } => {
                write!(
                    f,
                    "read fanout failed: owner node '{node_id}' has no known endpoint"
                )
            }
            Self::LocalSelectSeries { message } => {
                write!(f, "local select_series failed: {message}")
            }
            Self::LocalListMetrics { message } => {
                write!(f, "local list_metrics failed: {message}")
            }
            Self::LocalSelectBatch {
                series_count,
                message,
            } => {
                write!(
                    f,
                    "local select_batch failed for {series_count} series: {message}"
                )
            }
            Self::RemoteSelectSeries {
                node_id,
                endpoint,
                source,
            } => {
                write!(
                    f,
                    "remote select_series failed for node '{node_id}' ({endpoint}): {source}"
                )
            }
            Self::RemoteListMetrics {
                node_id,
                endpoint,
                source,
            } => {
                write!(
                    f,
                    "remote list_metrics failed for node '{node_id}' ({endpoint}): {source}"
                )
            }
            Self::RemoteSelectBatch {
                node_id,
                endpoint,
                series_count,
                source,
            } => {
                write!(
                    f,
                    "remote select_batch failed for {series_count} series on node '{node_id}' ({endpoint}): {source}"
                )
            }
            Self::MergeLimitExceeded { message } => {
                write!(f, "{message}")
            }
            Self::ResourceLimitExceeded {
                resource,
                requested,
                limit,
                retryable,
            } => {
                if *retryable {
                    write!(
                        f,
                        "read fanout saturated: {resource} limit {limit} reached (requested {requested}), retry later"
                    )
                } else {
                    write!(
                        f,
                        "read fanout request exceeds configured {resource} limit: requested {requested}, max {limit}"
                    )
                }
            }
            Self::ConsistencyUnmet {
                operation,
                mode,
                target,
                required_acks,
                acknowledged_acks,
                total_replicas,
                ..
            } => {
                write!(
                    f,
                    "read consistency unmet for {operation} on {target}: mode={mode}, required_acks={required_acks}, acknowledged_acks={acknowledged_acks}, total_replicas={total_replicas}"
                )
            }
            Self::TaskJoin { message } => {
                write!(f, "read fanout task failed: {message}")
            }
        }
    }
}

impl std::error::Error for ReadFanoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QueryBudget { error } => Some(error),
            Self::RemoteSelectSeries { source, .. }
            | Self::RemoteListMetrics { source, .. }
            | Self::RemoteSelectBatch { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct FanoutQueryMemory {
    reservation: Option<QueryMemoryReservation>,
}

impl FanoutQueryMemory {
    fn reserve_additional(
        &mut self,
        execution: Option<&QueryExecution>,
        additional_bytes: u64,
    ) -> Result<(), ReadFanoutError> {
        let Some(execution) = execution else {
            return Ok(());
        };
        if additional_bytes == 0 {
            return execution
                .checkpoint()
                .map_err(query_budget_error_to_fanout_error);
        }

        if let Some(reservation) = &mut self.reservation {
            let next = reservation.bytes().saturating_add(additional_bytes);
            reservation
                .resize(next)
                .map_err(query_budget_error_to_fanout_error)
        } else {
            self.reservation = Some(
                execution
                    .reserve_memory(additional_bytes)
                    .map_err(query_budget_error_to_fanout_error)?,
            );
            Ok(())
        }
    }
}

const FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
// A consistency error reaches the public adapter while its source reservation remains live.
// Retain explicit room for the simultaneous text-response body copy and small header collection.
const FANOUT_CONSISTENCY_ERROR_RESPONSE_FIXED_ALLOCATION_BYTES: u64 = 2 * 1024;

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    saturating_u64_from_usize(capacity)
        .saturating_mul(saturating_u64_from_usize(std::mem::size_of::<T>()))
        .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_string_retained_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        saturating_u64_from_usize(value.capacity())
            .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_owned_str_retained_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        saturating_u64_from_usize(value.len())
            .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_btree_entry_bytes<K, V>() -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<(K, V)>())
        .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_histogram_retained_bytes(histogram: &NativeHistogram) -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<NativeHistogram>())
        .saturating_add(modeled_vec_capacity_bytes::<tsink::HistogramBucketSpan>(
            histogram.negative_spans.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<i64>(
            histogram.negative_deltas.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.negative_counts.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<tsink::HistogramBucketSpan>(
            histogram.positive_spans.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<i64>(
            histogram.positive_deltas.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.positive_counts.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.custom_values.capacity(),
        ))
}

fn modeled_value_heap_retained_bytes(value: &Value) -> u64 {
    match value {
        Value::Bytes(bytes) => modeled_vec_capacity_bytes::<u8>(bytes.capacity()),
        Value::String(text) => modeled_string_retained_bytes(text),
        Value::Histogram(histogram) => modeled_histogram_retained_bytes(histogram),
        Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => 0,
    }
}

fn modeled_value_payload_returned_bytes(value: &Value) -> u64 {
    tsink::value::modeled_query_value_payload_bytes(value)
}

fn modeled_point_identity_heap_retained_bytes(value: &Value) -> u64 {
    match value {
        Value::Bytes(bytes) => modeled_vec_capacity_bytes::<u8>(bytes.capacity()),
        Value::String(text) => modeled_string_retained_bytes(text),
        Value::Histogram(histogram) => {
            // `SeriesPointsMerger` canonicalizes histogram identities through JSON. Bound the
            // serialized length using the maximum textual width of each numeric field plus fixed
            // field-name/object punctuation overhead. `serde_json::to_vec` may retain roughly
            // twice its final length after geometric growth, so double the length bound before
            // adding the allocation allowance.
            512u64
                .saturating_add(
                    saturating_u64_from_usize(
                        histogram
                            .negative_spans
                            .len()
                            .saturating_add(histogram.positive_spans.len()),
                    )
                    .saturating_mul(64),
                )
                .saturating_add(
                    saturating_u64_from_usize(
                        histogram
                            .negative_deltas
                            .len()
                            .saturating_add(histogram.positive_deltas.len()),
                    )
                    .saturating_mul(24),
                )
                .saturating_add(
                    saturating_u64_from_usize(
                        histogram
                            .negative_counts
                            .len()
                            .saturating_add(histogram.positive_counts.len())
                            .saturating_add(histogram.custom_values.len()),
                    )
                    .saturating_mul(32),
                )
                .saturating_mul(2)
                .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
        }
        Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => 0,
    }
}

fn modeled_label_heap_retained_bytes(label: &Label) -> u64 {
    modeled_string_retained_bytes(&label.name)
        .saturating_add(modeled_string_retained_bytes(&label.value))
}

fn modeled_metric_series_heap_retained_bytes(series: &MetricSeries) -> u64 {
    modeled_string_retained_bytes(&series.name)
        .saturating_add(modeled_vec_capacity_bytes::<Label>(
            series.labels.capacity(),
        ))
        .saturating_add(series.labels.iter().fold(0u64, |bytes, label| {
            bytes.saturating_add(modeled_label_heap_retained_bytes(label))
        }))
}

pub(crate) fn modeled_metric_series_vec_retained_bytes(series: &Vec<MetricSeries>) -> u64 {
    modeled_vec_capacity_bytes::<MetricSeries>(series.capacity()).saturating_add(
        series.iter().fold(0u64, |bytes, item| {
            bytes.saturating_add(modeled_metric_series_heap_retained_bytes(item))
        }),
    )
}

pub(super) fn modeled_points_vec_retained_bytes(points: &Vec<DataPoint>) -> u64 {
    modeled_vec_capacity_bytes::<DataPoint>(points.capacity()).saturating_add(
        points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(modeled_value_heap_retained_bytes(&point.value))
        }),
    )
}

pub(crate) fn modeled_series_points_vec_retained_bytes(series: &Vec<SeriesPoints>) -> u64 {
    modeled_vec_capacity_bytes::<SeriesPoints>(series.capacity()).saturating_add(
        series.iter().fold(0u64, |bytes, item| {
            bytes
                .saturating_add(modeled_metric_series_heap_retained_bytes(&item.series))
                .saturating_add(modeled_points_vec_retained_bytes(&item.points))
        }),
    )
}

pub(crate) fn modeled_matched_selectors_vec_retained_bytes(matched: &Vec<bool>) -> u64 {
    modeled_vec_capacity_bytes::<bool>(matched.capacity())
}

fn modeled_labels_returned_bytes(labels: &[Label]) -> u64 {
    labels.iter().fold(0u64, |bytes, label| {
        bytes
            .saturating_add(saturating_u64_from_usize(std::mem::size_of::<Label>()))
            .saturating_add(saturating_u64_from_usize(label.name.len()))
            .saturating_add(saturating_u64_from_usize(label.value.len()))
    })
}

fn modeled_metric_series_returned_bytes(series: &MetricSeries) -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<MetricSeries>())
        .saturating_add(saturating_u64_from_usize(series.name.len()))
        .saturating_add(modeled_labels_returned_bytes(&series.labels))
}

pub(crate) fn modeled_metric_series_slice_returned_bytes(series: &[MetricSeries]) -> u64 {
    series.iter().fold(0u64, |bytes, item| {
        bytes.saturating_add(modeled_metric_series_returned_bytes(item))
    })
}

fn modeled_series_points_identity_returned_bytes(series: &MetricSeries) -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<SeriesPoints>())
        .saturating_add(saturating_u64_from_usize(series.name.len()))
        .saturating_add(modeled_labels_returned_bytes(&series.labels))
}

fn modeled_point_returned_bytes(point: &DataPoint) -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<DataPoint>())
        .saturating_add(modeled_value_payload_returned_bytes(&point.value))
}

pub(crate) fn modeled_series_points_returned_bytes(series: &[SeriesPoints]) -> u64 {
    series.iter().fold(0u64, |bytes, item| {
        bytes
            .saturating_add(modeled_series_points_identity_returned_bytes(&item.series))
            .saturating_add(item.points.iter().fold(0u64, |point_bytes, point| {
                point_bytes.saturating_add(modeled_point_returned_bytes(point))
            }))
    })
}

fn modeled_metadata_merge_bytes(series: &[MetricSeries]) -> u64 {
    series.iter().fold(0u64, |bytes, item| {
        bytes
            .saturating_add(saturating_u64_from_usize(std::mem::size_of::<(
                SeriesIdentity,
                MetricSeries,
            )>()))
            .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
            // `SeriesMetadataMerger` retains both the owned identity and the owned value.
            .saturating_add(modeled_metric_series_heap_retained_bytes(item).saturating_mul(2))
    })
}

fn modeled_series_identity_map_entry_bytes<T>(series: &MetricSeries) -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<(SeriesIdentity, T)>())
        .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(modeled_metric_series_heap_retained_bytes(series))
}

fn modeled_points_merge_bytes(series: &[SeriesPoints]) -> u64 {
    series.iter().fold(0u64, |bytes, item| {
        let series_bytes = bytes
            .saturating_add(saturating_u64_from_usize(std::mem::size_of::<(
                SeriesIdentity,
                BTreeMap<(i64, Value), DataPoint>,
            )>()))
            .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
            // The merger retains an owned `SeriesIdentity` and an owned `MetricSeries`.
            .saturating_add(
                modeled_metric_series_heap_retained_bytes(&item.series).saturating_mul(2),
            );
        item.points.iter().fold(series_bytes, |point_bytes, point| {
            point_bytes
                .saturating_add(saturating_u64_from_usize(std::mem::size_of::<(
                    (i64, Value),
                    DataPoint,
                )>()))
                .saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
                .saturating_add(modeled_point_identity_heap_retained_bytes(&point.value))
                // The map value retains the original point alongside its owned identity.
                .saturating_add(modeled_value_heap_retained_bytes(&point.value))
        })
    })
}

fn fanout_collection_growth_capacity_upper(len: usize) -> usize {
    if len == 0 {
        0
    } else if len <= 4 {
        4
    } else {
        len.checked_next_power_of_two().unwrap_or(usize::MAX)
    }
}

fn modeled_diagnostic_string_upper_bytes(len: usize) -> u64 {
    if len == 0 {
        return 0;
    }
    // `String` grows geometrically. Immediately before its final growth the old capacity is
    // smaller than the required length, so twice the required length conservatively covers the
    // retained capacity; the common allocation allowance covers allocator size-class rounding.
    let capacity = len.max(8).saturating_mul(2);
    saturating_u64_from_usize(capacity).saturating_add(FANOUT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_series_identity_display_len(series: &MetricSeries) -> usize {
    if series.labels.is_empty() {
        return series.name.len();
    }
    series.labels.iter().fold(
        series
            .name
            .len()
            .saturating_add(2)
            .saturating_add(series.labels.len().saturating_sub(1)),
        |len, label| {
            len.saturating_add(label.name.len())
                .saturating_add(1)
                .saturating_add(label.value.len())
        },
    )
}

fn modeled_series_target_construction_transient_bytes(series: &MetricSeries) -> u64 {
    if series.labels.is_empty() {
        return modeled_diagnostic_string_upper_bytes(series.name.len());
    }
    let label_pieces = modeled_vec_capacity_bytes::<String>(
        fanout_collection_growth_capacity_upper(series.labels.len()),
    )
    .saturating_add(series.labels.iter().fold(0u64, |bytes, label| {
        let piece_len = label
            .name
            .len()
            .saturating_add(1)
            .saturating_add(label.value.len());
        bytes.saturating_add(modeled_diagnostic_string_upper_bytes(piece_len))
    }));
    let joined_len =
        series
            .labels
            .iter()
            .fold(series.labels.len().saturating_sub(1), |len, label| {
                len.saturating_add(label.name.len())
                    .saturating_add(1)
                    .saturating_add(label.value.len())
            });
    let display_len = modeled_series_identity_display_len(series);
    label_pieces
        .saturating_add(modeled_diagnostic_string_upper_bytes(joined_len))
        .saturating_add(modeled_diagnostic_string_upper_bytes(display_len))
}

#[derive(Debug, Default)]
struct ConsistencyDiagnosticUpper {
    gap_count: usize,
    gap_string_bytes: u64,
    warning_string_bytes: u64,
    max_target_len: usize,
    max_target_construction_transient_bytes: u64,
}

impl ConsistencyDiagnosticUpper {
    fn push_target(
        &mut self,
        operation: &str,
        target_len: usize,
        target_construction_transient_bytes: u64,
    ) {
        self.gap_count = self.gap_count.saturating_add(1);
        self.gap_string_bytes = self
            .gap_string_bytes
            .saturating_add(modeled_diagnostic_string_upper_bytes(operation.len()))
            .saturating_add(modeled_diagnostic_string_upper_bytes(target_len));
        let warning_len = 256usize
            .saturating_add(operation.len())
            .saturating_add(target_len);
        self.warning_string_bytes = self
            .warning_string_bytes
            .saturating_add(modeled_diagnostic_string_upper_bytes(warning_len));
        self.max_target_len = self.max_target_len.max(target_len);
        self.max_target_construction_transient_bytes = self
            .max_target_construction_transient_bytes
            .max(target_construction_transient_bytes);
    }

    fn finish(self, operation: &str, consistency_error: bool) -> u64 {
        if self.gap_count == 0 {
            return 0;
        }
        let gap_bytes = modeled_vec_capacity_bytes::<ReadConsistencyGap>(
            fanout_collection_growth_capacity_upper(self.gap_count),
        )
        .saturating_add(self.gap_string_bytes)
        .saturating_add(self.max_target_construction_transient_bytes);
        if consistency_error {
            let error_display_len = 256usize
                .saturating_add(operation.len())
                .saturating_add(self.max_target_len);
            gap_bytes
                .saturating_add(modeled_diagnostic_string_upper_bytes(operation.len()))
                .saturating_add(modeled_diagnostic_string_upper_bytes(self.max_target_len))
                .saturating_add(modeled_diagnostic_string_upper_bytes(error_display_len))
                .saturating_add(modeled_diagnostic_string_upper_bytes(error_display_len))
                .saturating_add(FANOUT_CONSISTENCY_ERROR_RESPONSE_FIXED_ALLOCATION_BYTES)
        } else {
            gap_bytes
                .saturating_add(modeled_vec_capacity_bytes::<String>(
                    fanout_collection_growth_capacity_upper(self.gap_count),
                ))
                .saturating_add(self.warning_string_bytes)
        }
    }
}

fn query_budget_error_to_fanout_error(error: QueryBudgetError) -> ReadFanoutError {
    ReadFanoutError::QueryBudget { error }
}

fn checkpoint_execution(execution: Option<&QueryExecution>) -> Result<(), ReadFanoutError> {
    execution.map_or(Ok(()), |execution| {
        execution
            .checkpoint()
            .map_err(query_budget_error_to_fanout_error)
    })
}

fn remaining_remote_select_limit(
    limit: Option<u64>,
    current: u64,
    reason: QueryLimitReason,
) -> Result<Option<u64>, ReadFanoutError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let remaining = limit.saturating_sub(current);
    if remaining == 0 {
        return Err(query_budget_error_to_fanout_error(
            tsink::QueryLimitExceeded::new(reason, limit, current, 1).into(),
        ));
    }
    Ok(Some(remaining))
}

fn remaining_remote_select_limits(
    execution: &QueryExecution,
) -> Result<QueryWorkLimits, ReadFanoutError> {
    execution
        .checkpoint()
        .map_err(query_budget_error_to_fanout_error)?;
    let snapshot = execution.snapshot();
    let mut limits = execution.limits();
    // Scans, evaluator steps, and pattern expansion are cumulative work even when replicas return
    // duplicate logical output. Result limits remain at their original finite values on each
    // sequential peer call; the coordinator charges the deduplicated output as it merges.
    limits.max_samples_scanned = remaining_remote_select_limit(
        limits.max_samples_scanned,
        snapshot.samples_scanned,
        QueryLimitReason::SamplesScanned,
    )?;
    limits.max_pattern_expansion = remaining_remote_select_limit(
        limits.max_pattern_expansion,
        snapshot.pattern_expansion,
        QueryLimitReason::PatternExpansion,
    )?;
    limits.max_steps =
        remaining_remote_select_limit(limits.max_steps, snapshot.steps, QueryLimitReason::Steps)?;
    if let Some(deadline) = execution.cancellation_token().deadline() {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(query_budget_error_to_fanout_error(
                QueryBudgetError::DeadlineExceeded,
            ));
        };
        if remaining.is_zero() {
            return Err(query_budget_error_to_fanout_error(
                QueryBudgetError::DeadlineExceeded,
            ));
        }
        limits.max_wall_time = Some(remaining);
    }
    Ok(limits)
}

fn validate_remote_select_batch_accounting(
    selectors: &[MetricSeries],
    series: &[SeriesPoints],
    matched: &[bool],
    snapshot: QueryExecutionSnapshot,
) -> Result<(), String> {
    if series.len() != selectors.len() {
        return Err(format!(
            "response series count {} does not match selector count {}",
            series.len(),
            selectors.len()
        ));
    }
    if matched.len() != selectors.len() {
        return Err(format!(
            "matched-selector count {} does not match selector count {}",
            matched.len(),
            selectors.len()
        ));
    }
    for ((selector, item), matched) in selectors.iter().zip(series).zip(matched) {
        if &item.series != selector {
            return Err(
                "response series identity or ordering does not match the request".to_string(),
            );
        }
        if !matched && !item.points.is_empty() {
            return Err("an unmatched selector returned points".to_string());
        }
    }

    let matched_count =
        saturating_u64_from_usize(matched.iter().filter(|matched| **matched).count());
    if snapshot.series_matched != matched_count {
        return Err(format!(
            "reported matched-series count {} does not equal matched-selector count {matched_count}",
            snapshot.series_matched
        ));
    }
    let returned_samples = series.iter().fold(0u64, |count, item| {
        count.saturating_add(saturating_u64_from_usize(item.points.len()))
    });
    if snapshot.samples_returned < returned_samples {
        return Err(format!(
            "reported returned-sample count {} is smaller than response count {returned_samples}",
            snapshot.samples_returned
        ));
    }
    if snapshot.samples_scanned < snapshot.samples_returned {
        return Err("reported scanned-sample count is smaller than returned samples".to_string());
    }
    let returned_bytes = modeled_series_points_returned_bytes(series);
    if snapshot.returned_bytes < returned_bytes {
        return Err(format!(
            "reported returned bytes {} are smaller than modeled response bytes {returned_bytes}",
            snapshot.returned_bytes
        ));
    }
    let minimum_vector_size = series
        .iter()
        .fold(saturating_u64_from_usize(series.len()), |size, item| {
            size.max(saturating_u64_from_usize(item.points.len()))
        });
    if snapshot.intermediate_vector_size < minimum_vector_size {
        return Err(format!(
            "reported intermediate-vector high-water {} is smaller than response high-water {minimum_vector_size}",
            snapshot.intermediate_vector_size
        ));
    }
    Ok(())
}

fn charge_remote_select_work(
    execution: &QueryExecution,
    snapshot: QueryExecutionSnapshot,
) -> Result<(), ReadFanoutError> {
    execution
        .checkpoint()
        .map_err(query_budget_error_to_fanout_error)?;
    execution
        .charge_samples_scanned(snapshot.samples_scanned)
        .map_err(query_budget_error_to_fanout_error)?;
    execution
        .charge_pattern_expansion(snapshot.pattern_expansion)
        .map_err(query_budget_error_to_fanout_error)?;
    execution
        .charge_steps(snapshot.steps)
        .map_err(query_budget_error_to_fanout_error)?;
    execution
        .observe_intermediate_vector_size(snapshot.intermediate_vector_size)
        .map_err(query_budget_error_to_fanout_error)
}

fn is_remote_select_query_control_error(error: &ReadFanoutError) -> bool {
    let ReadFanoutError::RemoteSelectBatch { source, .. } = error else {
        return matches!(error, ReadFanoutError::QueryBudget { .. });
    };
    match source.as_ref() {
        RpcError::HttpStatus {
            error_code: Some(code),
            ..
        } => {
            code == "invalid_query_limits"
                || code == "query_accounting_unavailable"
                || code == "query_accounting_invalid"
                || code == "query_cancelled"
                || code == "query_deadline_exceeded"
                || code.starts_with("query_limit_")
        }
        RpcError::Deserialize { .. }
        | RpcError::ResponseTooLarge { .. }
        | RpcError::ProtocolVersionMismatch { .. }
        | RpcError::CompatibilityRejected { .. }
        | RpcError::Serialize { .. }
        | RpcError::QueryBudget { .. } => true,
        RpcError::Timeout { .. } | RpcError::Transport { .. } | RpcError::HttpStatus { .. } => {
            false
        }
    }
}

fn observe_metric_series_vector(
    execution: &QueryExecution,
    series: &[MetricSeries],
) -> Result<(), ReadFanoutError> {
    execution
        .observe_intermediate_vector_size(saturating_u64_from_usize(series.len()))
        .map_err(query_budget_error_to_fanout_error)
}

fn observe_series_points_vectors(
    execution: &QueryExecution,
    series: &[SeriesPoints],
) -> Result<(), ReadFanoutError> {
    let high_water = series
        .iter()
        .fold(series.len(), |largest, item| largest.max(item.points.len()));
    execution
        .observe_intermediate_vector_size(saturating_u64_from_usize(high_water))
        .map_err(query_budget_error_to_fanout_error)
}

fn charge_local_metadata_result(
    execution: &QueryExecution,
    accounting: QueryExecutionAccounting,
    series: &[MetricSeries],
) -> Result<(), ReadFanoutError> {
    let series_count = saturating_u64_from_usize(series.len());
    if accounting == QueryExecutionAccounting::Unaccounted {
        execution
            .charge_series_matched(series_count)
            .map_err(query_budget_error_to_fanout_error)?;
        execution
            .charge_returned_bytes(modeled_metric_series_slice_returned_bytes(series))
            .map_err(query_budget_error_to_fanout_error)?;
    }
    observe_metric_series_vector(execution, series)
}

fn charge_local_points_result(
    execution: &QueryExecution,
    accounting: QueryExecutionAccounting,
    series: &[SeriesPoints],
) -> Result<(), ReadFanoutError> {
    if accounting == QueryExecutionAccounting::Unaccounted {
        let series_count = saturating_u64_from_usize(series.len());
        let samples = series.iter().fold(0u64, |count, item| {
            count.saturating_add(saturating_u64_from_usize(item.points.len()))
        });
        execution
            .charge_series_matched(series_count)
            .map_err(query_budget_error_to_fanout_error)?;
        execution
            .charge_samples_scanned(samples)
            .map_err(query_budget_error_to_fanout_error)?;
        execution
            .charge_samples_returned(samples)
            .map_err(query_budget_error_to_fanout_error)?;
        execution
            .charge_returned_bytes(modeled_series_points_returned_bytes(series))
            .map_err(query_budget_error_to_fanout_error)?;
    }
    observe_series_points_vectors(execution, series)
}

static CLUSTER_FANOUT_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_DURATION_NANOS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_REMOTE_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_REMOTE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_RESOURCE_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_RESOURCE_ACQUIRE_WAIT_NANOS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_RESOURCE_ACTIVE_QUERIES: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_RESOURCE_ACTIVE_MERGED_POINTS: AtomicU64 = AtomicU64::new(0);
static CLUSTER_FANOUT_LABELED_METRICS: OnceLock<Mutex<ReadFanoutLabeledMetrics>> = OnceLock::new();

#[derive(Debug)]
struct ReadResourceGuard {
    query_slots: Arc<Semaphore>,
    merged_points_budget: Arc<Semaphore>,
    acquire_timeout: Duration,
    max_inflight_queries: usize,
    max_inflight_merged_points: usize,
}

impl ReadResourceGuard {
    fn new(guardrails: ReadResourceGuardrails) -> Result<Self, String> {
        guardrails.validate()?;
        Ok(Self {
            query_slots: Arc::new(Semaphore::new(guardrails.max_inflight_queries)),
            merged_points_budget: Arc::new(Semaphore::new(guardrails.max_inflight_merged_points)),
            acquire_timeout: guardrails.acquire_timeout,
            max_inflight_queries: guardrails.max_inflight_queries,
            max_inflight_merged_points: guardrails.max_inflight_merged_points,
        })
    }
}

#[derive(Debug)]
struct ReadResourceLease {
    _query_slot: OwnedSemaphorePermit,
    _merged_points: OwnedSemaphorePermit,
    reserved_merged_points: u64,
}

impl Drop for ReadResourceLease {
    fn drop(&mut self) {
        CLUSTER_FANOUT_RESOURCE_ACTIVE_QUERIES.fetch_sub(1, Ordering::Relaxed);
        CLUSTER_FANOUT_RESOURCE_ACTIVE_MERGED_POINTS
            .fetch_sub(self.reserved_merged_points, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Default)]
struct LatencyHistogram {
    bucket_counts: [u64; FANOUT_REMOTE_REQUEST_LATENCY_BUCKETS_NANOS.len()],
    count: u64,
    sum_nanos: u64,
}

impl LatencyHistogram {
    fn record(&mut self, duration_nanos: u64) {
        self.count = self.count.saturating_add(1);
        self.sum_nanos = self.sum_nanos.saturating_add(duration_nanos);
        for (idx, upper_bound) in FANOUT_REMOTE_REQUEST_LATENCY_BUCKETS_NANOS
            .iter()
            .enumerate()
        {
            if duration_nanos <= *upper_bound {
                self.bucket_counts[idx] = self.bucket_counts[idx].saturating_add(1);
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct PerPeerFanoutMetrics {
    remote_requests_total: u64,
    remote_failures_total: u64,
    remote_request_latency: LatencyHistogram,
}

#[derive(Debug, Clone, Default)]
struct ReadFanoutLabeledMetrics {
    operation_requests_total: BTreeMap<String, u64>,
    operation_failures_total: BTreeMap<String, u64>,
    peer_metrics: BTreeMap<(String, String), PerPeerFanoutMetrics>,
}

pub fn read_fanout_metrics_snapshot() -> ReadFanoutMetricsSnapshot {
    ReadFanoutMetricsSnapshot {
        requests_total: CLUSTER_FANOUT_REQUESTS_TOTAL.load(Ordering::Relaxed),
        failures_total: CLUSTER_FANOUT_FAILURES_TOTAL.load(Ordering::Relaxed),
        duration_nanos_total: CLUSTER_FANOUT_DURATION_NANOS_TOTAL.load(Ordering::Relaxed),
        remote_requests_total: CLUSTER_FANOUT_REMOTE_REQUESTS_TOTAL.load(Ordering::Relaxed),
        remote_failures_total: CLUSTER_FANOUT_REMOTE_FAILURES_TOTAL.load(Ordering::Relaxed),
        resource_rejections_total: CLUSTER_FANOUT_RESOURCE_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        resource_acquire_wait_nanos_total: CLUSTER_FANOUT_RESOURCE_ACQUIRE_WAIT_NANOS_TOTAL
            .load(Ordering::Relaxed),
        resource_active_queries: CLUSTER_FANOUT_RESOURCE_ACTIVE_QUERIES.load(Ordering::Relaxed),
        resource_active_merged_points: CLUSTER_FANOUT_RESOURCE_ACTIVE_MERGED_POINTS
            .load(Ordering::Relaxed),
    }
}

pub fn read_fanout_labeled_metrics_snapshot() -> ReadFanoutLabeledMetricsSnapshot {
    with_fanout_labeled_metrics(|metrics| {
        let mut operations = metrics
            .operation_requests_total
            .iter()
            .map(
                |(operation, requests_total)| ReadFanoutOperationMetricsSnapshot {
                    operation: operation.clone(),
                    requests_total: *requests_total,
                    failures_total: metrics
                        .operation_failures_total
                        .get(operation)
                        .copied()
                        .unwrap_or(0),
                },
            )
            .collect::<Vec<_>>();
        operations.sort_by(|left, right| left.operation.cmp(&right.operation));

        let peers = metrics
            .peer_metrics
            .iter()
            .map(
                |((node_id, operation), peer)| ReadFanoutPeerMetricsSnapshot {
                    node_id: node_id.clone(),
                    operation: operation.clone(),
                    remote_requests_total: peer.remote_requests_total,
                    remote_failures_total: peer.remote_failures_total,
                    remote_request_duration_nanos_total: peer.remote_request_latency.sum_nanos,
                    remote_request_duration_count: peer.remote_request_latency.count,
                    remote_request_duration_buckets: peer
                        .remote_request_latency
                        .bucket_counts
                        .to_vec(),
                },
            )
            .collect::<Vec<_>>();

        ReadFanoutLabeledMetricsSnapshot { operations, peers }
    })
}

fn with_fanout_labeled_metrics<T>(mut f: impl FnMut(&mut ReadFanoutLabeledMetrics) -> T) -> T {
    let lock = CLUSTER_FANOUT_LABELED_METRICS
        .get_or_init(|| Mutex::new(ReadFanoutLabeledMetrics::default()));
    let mut guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut guard)
}

fn track_fanout_operation_start(operation: &str) {
    CLUSTER_FANOUT_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    with_fanout_labeled_metrics(|metrics| {
        let entry = metrics
            .operation_requests_total
            .entry(operation.to_string())
            .or_insert(0);
        *entry = entry.saturating_add(1);
    });
}

fn track_fanout_operation_complete(operation: &str, duration_nanos: u64, failed: bool) {
    CLUSTER_FANOUT_DURATION_NANOS_TOTAL.fetch_add(duration_nanos, Ordering::Relaxed);
    if failed {
        CLUSTER_FANOUT_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        with_fanout_labeled_metrics(|metrics| {
            let entry = metrics
                .operation_failures_total
                .entry(operation.to_string())
                .or_insert(0);
            *entry = entry.saturating_add(1);
        });
    }
}

fn track_remote_request(node_id: &str, operation: &str, duration_nanos: u64, failed: bool) {
    CLUSTER_FANOUT_REMOTE_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    if failed {
        CLUSTER_FANOUT_REMOTE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    with_fanout_labeled_metrics(|metrics| {
        let entry = metrics
            .peer_metrics
            .entry((node_id.to_string(), operation.to_string()))
            .or_insert_with(PerPeerFanoutMetrics::default);
        entry.remote_requests_total = entry.remote_requests_total.saturating_add(1);
        if failed {
            entry.remote_failures_total = entry.remote_failures_total.saturating_add(1);
        }
        entry.remote_request_latency.record(duration_nanos);
    });
}

fn track_resource_rejection() {
    CLUSTER_FANOUT_RESOURCE_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn track_resource_acquire_wait(duration_nanos: u64) {
    CLUSTER_FANOUT_RESOURCE_ACQUIRE_WAIT_NANOS_TOTAL.fetch_add(duration_nanos, Ordering::Relaxed);
}

fn track_resource_acquired(reserved_merged_points: u64) {
    CLUSTER_FANOUT_RESOURCE_ACTIVE_QUERIES.fetch_add(1, Ordering::Relaxed);
    CLUSTER_FANOUT_RESOURCE_ACTIVE_MERGED_POINTS
        .fetch_add(reserved_merged_points, Ordering::Relaxed);
}

fn saturating_elapsed_nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[allow(clippy::result_large_err)]
impl ReadFanoutExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: String,
        ring: ShardRing,
        membership: &MembershipView,
        fanout_concurrency: usize,
        read_consistency: ClusterReadConsistency,
        partial_response_policy: ClusterReadPartialResponsePolicy,
        merge_limits: ReadMergeLimits,
        resource_guardrails: ReadResourceGuardrails,
    ) -> Result<Self, String> {
        if fanout_concurrency == 0 {
            return Err("fanout concurrency must be greater than zero".to_string());
        }
        merge_limits.validate()?;
        resource_guardrails.validate()?;
        let planner = ShardAwareQueryPlanner::new(local_node_id.clone(), ring.clone(), membership)?;
        let resources = Arc::new(ReadResourceGuard::new(resource_guardrails)?);

        Ok(Self {
            local_node_id,
            ring,
            planner,
            fanout_concurrency,
            policy: ReadPolicy::new(read_consistency),
            partial_response_policy,
            merge_limits,
            resources,
        })
    }

    #[allow(dead_code)]
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn read_consistency_mode(&self) -> ClusterReadConsistency {
        self.policy.mode()
    }

    pub fn read_partial_response_policy(&self) -> ClusterReadPartialResponsePolicy {
        self.partial_response_policy
    }

    pub fn resource_guardrails(&self) -> ReadResourceGuardrails {
        ReadResourceGuardrails {
            max_inflight_queries: self.resources.max_inflight_queries,
            max_inflight_merged_points: self.resources.max_inflight_merged_points,
            acquire_timeout: self.resources.acquire_timeout,
        }
    }

    pub fn reconfigured_for_topology(
        &self,
        ring: ShardRing,
        membership: &MembershipView,
    ) -> Result<Self, String> {
        let planner = self
            .planner
            .reconfigured_for_topology(ring.clone(), membership)?;
        Ok(Self {
            local_node_id: self.local_node_id.clone(),
            ring,
            planner,
            fanout_concurrency: self.fanout_concurrency,
            policy: self.policy,
            partial_response_policy: self.partial_response_policy,
            merge_limits: self.merge_limits,
            resources: Arc::clone(&self.resources),
        })
    }

    pub fn with_partial_response_policy(
        &self,
        partial_response_policy: ClusterReadPartialResponsePolicy,
    ) -> Self {
        let mut cloned = self.clone();
        cloned.partial_response_policy = partial_response_policy;
        cloned
    }

    pub fn with_read_consistency(&self, read_consistency: ClusterReadConsistency) -> Self {
        let mut cloned = self.clone();
        cloned.policy = ReadPolicy::new(read_consistency);
        cloned
    }

    fn metadata_budget_estimate(&self) -> usize {
        self.merge_limits.max_series.max(1)
    }

    fn points_budget_estimate(&self, series_count: usize) -> usize {
        let estimated_points = series_count.saturating_mul(self.merge_limits.max_points_per_series);
        estimated_points
            .min(self.merge_limits.max_total_points)
            .max(1)
    }

    async fn acquire_read_resources(
        &self,
        merged_points_budget: usize,
    ) -> Result<ReadResourceLease, ReadFanoutError> {
        let requested = merged_points_budget.max(1);
        if requested > self.resources.max_inflight_merged_points {
            track_resource_rejection();
            return Err(ReadFanoutError::ResourceLimitExceeded {
                resource: READ_RESOURCE_GLOBAL_MERGED_POINTS,
                requested,
                limit: self.resources.max_inflight_merged_points,
                retryable: false,
            });
        }

        let acquire_started = Instant::now();
        let query_slot = self.acquire_query_slot().await.map_err(|retryable| {
            ReadFanoutError::ResourceLimitExceeded {
                resource: READ_RESOURCE_GLOBAL_QUERY_SLOTS,
                requested: 1,
                limit: self.resources.max_inflight_queries,
                retryable,
            }
        })?;
        let merged_points = match self.acquire_merged_points(requested).await {
            Ok(permit) => permit,
            Err(retryable) => {
                drop(query_slot);
                return Err(ReadFanoutError::ResourceLimitExceeded {
                    resource: READ_RESOURCE_GLOBAL_MERGED_POINTS,
                    requested,
                    limit: self.resources.max_inflight_merged_points,
                    retryable,
                });
            }
        };

        let wait_nanos = saturating_elapsed_nanos(acquire_started.elapsed());
        track_resource_acquire_wait(wait_nanos);
        let reserved_merged_points = u64::try_from(requested).unwrap_or(u64::MAX);
        track_resource_acquired(reserved_merged_points);
        Ok(ReadResourceLease {
            _query_slot: query_slot,
            _merged_points: merged_points,
            reserved_merged_points,
        })
    }

    async fn acquire_query_slot(&self) -> Result<OwnedSemaphorePermit, bool> {
        if let Ok(permit) = Arc::clone(&self.resources.query_slots).try_acquire_owned() {
            return Ok(permit);
        }
        let acquire = Arc::clone(&self.resources.query_slots).acquire_owned();
        match tokio::time::timeout(self.resources.acquire_timeout, acquire).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(false),
            Err(_) => {
                track_resource_rejection();
                Err(true)
            }
        }
    }

    async fn acquire_merged_points(&self, requested: usize) -> Result<OwnedSemaphorePermit, bool> {
        let permits = u32::try_from(requested).expect("requested permits validated to u32 range");
        if let Ok(permit) =
            Arc::clone(&self.resources.merged_points_budget).try_acquire_many_owned(permits)
        {
            return Ok(permit);
        }
        let acquire = Arc::clone(&self.resources.merged_points_budget).acquire_many_owned(permits);
        match tokio::time::timeout(self.resources.acquire_timeout, acquire).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(false),
            Err(_) => {
                track_resource_rejection();
                Err(true)
            }
        }
    }

    pub async fn select_series_with_ring_version_detailed(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        selection: &SeriesSelection,
        ring_version: u64,
    ) -> Result<ReadFanoutResponse<Vec<MetricSeries>>, ReadFanoutError> {
        self.select_series_with_ring_version_detailed_impl(
            storage,
            rpc_client,
            selection,
            ring_version,
            None,
            FANOUT_OPERATION_SELECT_SERIES,
        )
        .await
        .map(|response| ReadFanoutResponse {
            value: response.value.series,
            metadata: response.metadata,
            _reservation: response._reservation,
        })
    }

    pub(crate) async fn select_series_with_ring_version_detailed_accounted_with_execution(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        selection: &SeriesSelection,
        ring_version: u64,
        execution: &QueryExecution,
    ) -> Result<ReadFanoutResponse<AccountedMetricSeries>, ReadFanoutError> {
        self.select_series_with_ring_version_detailed_impl(
            storage,
            rpc_client,
            selection,
            ring_version,
            Some(execution),
            FANOUT_OPERATION_SELECT_SERIES,
        )
        .await
    }

    async fn select_series_with_ring_version_detailed_impl(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        selection: &SeriesSelection,
        ring_version: u64,
        execution: Option<&QueryExecution>,
        operation: &'static str,
    ) -> Result<ReadFanoutResponse<AccountedMetricSeries>, ReadFanoutError> {
        let ring_version = ring_version.max(1);
        let started = Instant::now();
        track_fanout_operation_start(operation);
        let result = async {
            checkpoint_execution(execution)?;
            let _selection_preparation = match execution {
                Some(execution) => Some(selection.prepare_with_execution(execution).map_err(
                    |error| match error {
                        SeriesSelectionPreparationError::Validation(error) => {
                            ReadFanoutError::InvalidRequest {
                                message: error.to_string(),
                            }
                        }
                        SeriesSelectionPreparationError::Query(error) => {
                            query_budget_error_to_fanout_error(error)
                        }
                        other => ReadFanoutError::InvalidRequest {
                            message: other.to_string(),
                        },
                    },
                )?),
                None => {
                    selection
                        .validate()
                        .map_err(|error| ReadFanoutError::InvalidRequest {
                            message: error.to_string(),
                        })?;
                    None
                }
            };
            let mut query_memory = FanoutQueryMemory::default();
            if let Some(execution) = execution {
                let planning_upper_bound =
                    self.modeled_select_series_planning_upper_bound(selection)?;
                query_memory.reserve_additional(Some(execution), planning_upper_bound)?;
            }
            let _resource_lease = self
                .acquire_read_resources(self.metadata_budget_estimate())
                .await?;
            let plan = self.plan_select_series(selection, ring_version)?;
            let requirements =
                self.shard_requirements(&plan.candidate_shards, self.policy.metadata_owner_mode())?;
            let mut acknowledged_acks = plan
                .candidate_shards
                .iter()
                .map(|shard| (*shard, 0usize))
                .collect::<BTreeMap<_, _>>();
            let mut merged = SeriesMetadataMerger::new(self.merge_limits);
            if !plan.local_shards.is_empty() {
                let local_accounting = storage.select_series_in_shards_execution_accounting();
                match self
                    .local_select_series(
                        storage,
                        selection,
                        &plan.local_shards,
                        execution,
                        local_accounting,
                    )
                    .await
                {
                    Ok(local_series) => {
                        if let Some(execution) = execution {
                            charge_local_metadata_result(
                                execution,
                                local_accounting,
                                &local_series.series,
                            )?;
                        }
                        if execution.is_some() {
                            query_memory.reserve_additional(
                                execution,
                                modeled_metric_series_vec_retained_bytes(&local_series.series)
                                    .saturating_add(modeled_metadata_merge_bytes(
                                        &local_series.series,
                                    )),
                            )?;
                        }
                        for shard in &plan.local_shards {
                            let entry = acknowledged_acks.entry(*shard).or_insert(0);
                            *entry = entry.saturating_add(1);
                        }
                        merged
                            .extend(local_series.series)
                            .map_err(read_merge_error_to_fanout_error)?;
                    }
                    Err(error @ ReadFanoutError::QueryBudget { .. }) => return Err(error),
                    Err(error @ ReadFanoutError::InvalidRequest { .. }) => return Err(error),
                    Err(_) => {}
                }
            }

            if let Some(execution) = execution {
                self.remote_select_series_bounded(
                    rpc_client,
                    selection,
                    ring_version,
                    &plan.remote_targets,
                    execution,
                    &mut query_memory,
                    &mut acknowledged_acks,
                    &mut merged,
                    operation,
                )
                .await?;
            } else {
                let remote_series = self
                    .remote_select_series_collect(
                        rpc_client,
                        selection,
                        ring_version,
                        &plan.remote_targets,
                    )
                    .await?;
                for target in remote_series {
                    if let Ok(node_series) = target.series {
                        for shard in &target.shards {
                            let entry = acknowledged_acks.entry(*shard).or_insert(0);
                            *entry = entry.saturating_add(1);
                        }
                        for series in node_series {
                            merged
                                .insert(series)
                                .map_err(read_merge_error_to_fanout_error)?;
                        }
                    }
                }
            }

            let diagnostic_upper = self.modeled_shard_consistency_diagnostics_upper_bytes(
                operation,
                &requirements,
                &acknowledged_acks,
            );
            query_memory.reserve_additional(execution, diagnostic_upper)?;
            checkpoint_execution(execution)?;
            let consistency_gaps =
                self.shard_consistency_gaps(operation, &requirements, &acknowledged_acks);
            let metadata = match self.evaluate_consistency_gaps(operation, consistency_gaps) {
                Ok(metadata) => metadata,
                Err(error) => {
                    return Err(error.with_consistency_reservation(query_memory.reservation.take()))
                }
            };
            let value = merged.into_series();
            if let Some(execution) = execution {
                observe_metric_series_vector(execution, &value)?;
                query_memory.reserve_additional(
                    Some(execution),
                    modeled_vec_capacity_bytes::<MetricSeries>(value.capacity()),
                )?;
            }
            checkpoint_execution(execution)?;
            let reservation = match execution {
                Some(execution) => Some(match query_memory.reservation.take() {
                    Some(reservation) => reservation,
                    None => execution
                        .reserve_memory(0)
                        .map_err(query_budget_error_to_fanout_error)?,
                }),
                None => None,
            };
            Ok::<_, ReadFanoutError>(ReadFanoutResponse {
                value: AccountedMetricSeries {
                    series: value,
                    _reservation: reservation,
                },
                metadata,
                _reservation: None,
            })
        }
        .await;
        track_fanout_operation_complete(
            operation,
            saturating_elapsed_nanos(started.elapsed()),
            result.is_err(),
        );
        result
    }

    #[allow(dead_code)]
    pub async fn list_metrics_with_ring_version(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        ring_version: u64,
    ) -> Result<Vec<MetricSeries>, ReadFanoutError> {
        self.list_metrics_with_ring_version_detailed(storage, rpc_client, ring_version)
            .await
            .map(|response| response.value)
    }

    pub async fn list_metrics_with_ring_version_detailed(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        ring_version: u64,
    ) -> Result<ReadFanoutResponse<Vec<MetricSeries>>, ReadFanoutError> {
        let ring_version = ring_version.max(1);
        let started = Instant::now();
        track_fanout_operation_start(FANOUT_OPERATION_LIST_METRICS);
        let result = async {
            let _resource_lease = self
                .acquire_read_resources(self.metadata_budget_estimate())
                .await?;
            let plan = self.plan_list_metrics(ring_version)?;
            let requirements =
                self.shard_requirements(&plan.candidate_shards, self.policy.metadata_owner_mode())?;
            let mut acknowledged_acks = plan
                .candidate_shards
                .iter()
                .map(|shard| (*shard, 0usize))
                .collect::<BTreeMap<_, _>>();
            let remote_metrics = self
                .remote_list_metrics_collect(rpc_client, ring_version, &plan.remote_targets)
                .await?;

            let mut merged = SeriesMetadataMerger::new(self.merge_limits);
            if !plan.local_shards.is_empty() {
                if let Ok(local_metrics) =
                    self.local_list_metrics(storage, &plan.local_shards).await
                {
                    for shard in &plan.local_shards {
                        let entry = acknowledged_acks.entry(*shard).or_insert(0);
                        *entry = entry.saturating_add(1);
                    }
                    merged
                        .extend(local_metrics)
                        .map_err(read_merge_error_to_fanout_error)?;
                }
            }

            for target in remote_metrics {
                if let Ok(node_metrics) = target.series {
                    for shard in &target.shards {
                        let entry = acknowledged_acks.entry(*shard).or_insert(0);
                        *entry = entry.saturating_add(1);
                    }
                    merged
                        .extend(node_metrics)
                        .map_err(read_merge_error_to_fanout_error)?;
                }
            }

            let consistency_gaps = self.shard_consistency_gaps(
                FANOUT_OPERATION_LIST_METRICS,
                &requirements,
                &acknowledged_acks,
            );
            let metadata =
                self.evaluate_consistency_gaps(FANOUT_OPERATION_LIST_METRICS, consistency_gaps)?;
            Ok::<_, ReadFanoutError>(ReadFanoutResponse {
                value: merged.into_series(),
                metadata,
                _reservation: None,
            })
        }
        .await;
        track_fanout_operation_complete(
            FANOUT_OPERATION_LIST_METRICS,
            saturating_elapsed_nanos(started.elapsed()),
            result.is_err(),
        );
        result
    }

    pub(crate) async fn list_metrics_with_ring_version_detailed_accounted_with_execution(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        ring_version: u64,
        execution: &QueryExecution,
    ) -> Result<ReadFanoutResponse<AccountedMetricSeries>, ReadFanoutError> {
        let selection = SeriesSelection::new();
        self.select_series_with_ring_version_detailed_impl(
            storage,
            rpc_client,
            &selection,
            ring_version,
            Some(execution),
            FANOUT_OPERATION_LIST_METRICS,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn select_points_for_series_with_ring_version(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
    ) -> Result<Vec<SeriesPoints>, ReadFanoutError> {
        self.select_points_for_series_with_ring_version_detailed(
            storage,
            rpc_client,
            series,
            start,
            end,
            ring_version,
        )
        .await
        .map(|response| response.value)
    }

    pub async fn select_points_for_series_with_ring_version_detailed(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
    ) -> Result<ReadFanoutResponse<Vec<SeriesPoints>>, ReadFanoutError> {
        self.select_points_for_series_with_ring_version_detailed_impl(
            storage,
            rpc_client,
            series,
            start,
            end,
            ring_version,
            None,
        )
        .await
        .map(|response| ReadFanoutResponse {
            value: response.value.series,
            metadata: response.metadata,
            _reservation: response._reservation,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn select_points_for_series_with_ring_version_detailed_with_execution(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
        execution: &QueryExecution,
    ) -> Result<ReadFanoutResponse<Vec<SeriesPoints>>, ReadFanoutError> {
        self.select_points_for_series_with_ring_version_detailed_accounted_with_execution(
            storage,
            rpc_client,
            series,
            start,
            end,
            ring_version,
            execution,
        )
        .await
        .map(|response| {
            let mut value = response.value;
            let reservation = value.take_reservation().map(Arc::new);
            ReadFanoutResponse {
                value: value.series,
                metadata: response.metadata,
                _reservation: reservation,
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn select_points_for_series_with_ring_version_detailed_accounted_with_execution(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
        execution: &QueryExecution,
    ) -> Result<ReadFanoutResponse<AccountedSeriesPoints>, ReadFanoutError> {
        self.select_points_for_series_with_ring_version_detailed_impl(
            storage,
            rpc_client,
            series,
            start,
            end,
            ring_version,
            Some(execution),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_points_for_series_with_ring_version_detailed_impl(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
        execution: Option<&QueryExecution>,
    ) -> Result<ReadFanoutResponse<AccountedSeriesPoints>, ReadFanoutError> {
        let ring_version = ring_version.max(1);
        let started = Instant::now();
        track_fanout_operation_start(FANOUT_OPERATION_SELECT_POINTS);
        let result = async {
            checkpoint_execution(execution)?;
            if let Some(execution) = execution {
                observe_metric_series_vector(execution, series)?;
            }
            let mut query_memory = FanoutQueryMemory::default();
            if execution.is_some() {
                let planning_upper_bound =
                    self.modeled_select_points_planning_upper_bound(series)?;
                query_memory.reserve_additional(execution, planning_upper_bound)?;
            }
            let _resource_lease = self
                .acquire_read_resources(self.points_budget_estimate(series.len()))
                .await?;
            let plan = self.plan_select_points(series, start, end, ring_version)?;
            let mut unique_series = BTreeMap::<SeriesIdentity, MetricSeries>::new();
            for item in series {
                let current_len = unique_series.len();
                match unique_series.entry(SeriesIdentity::from_series(item)) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        if current_len >= self.merge_limits.max_series {
                            return Err(read_merge_error_to_fanout_error(
                                MergeLimitError::Series {
                                    limit: self.merge_limits.max_series,
                                    attempted: current_len.saturating_add(1),
                                },
                            ));
                        }
                        entry.insert(item.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
            }

            let mut by_owner: BTreeMap<String, Vec<MetricSeries>> = BTreeMap::new();
            let mut requirements = BTreeMap::<SeriesIdentity, ReadRequirement>::new();
            for (identity, item) in &unique_series {
                let owners = self.owners_for_series_borrowed(
                    &item.name,
                    &item.labels,
                    self.policy.points_owner_mode(),
                )?;
                let required_acks = self.policy.required_acks(owners.len());
                requirements.insert(
                    identity.clone(),
                    ReadRequirement {
                        required_acks,
                        total_replicas: owners.len(),
                    },
                );
                for owner in owners {
                    by_owner
                        .entry(owner.clone())
                        .or_default()
                        .push(item.clone());
                }
            }

            let mut acknowledged_acks = requirements
                .keys()
                .map(|identity| (identity.clone(), 0usize))
                .collect::<BTreeMap<_, _>>();
            let mut accounted_series = BTreeSet::<SeriesIdentity>::new();
            let mut matched_series = BTreeSet::<SeriesIdentity>::new();
            let mut collected_points = SeriesPointsMerger::new(self.merge_limits);

            if let Some(local_series) = by_owner.remove(&self.local_node_id) {
                let local_accounting = storage.select_many_execution_accounting();
                match self
                    .local_select_points(
                        storage,
                        local_series,
                        start,
                        end,
                        execution,
                        local_accounting,
                    )
                    .await
                {
                    Ok(local_result) => {
                        if let Some(execution) = execution {
                            charge_local_points_result(
                                execution,
                                local_accounting,
                                &local_result.series,
                            )?;
                        }
                        if execution.is_some() {
                            query_memory.reserve_additional(
                                execution,
                                modeled_series_points_vec_retained_bytes(&local_result.series)
                                    .saturating_add(modeled_points_merge_bytes(
                                        &local_result.series,
                                    )),
                            )?;
                        }
                        for (item, matched) in
                            local_result.series.into_iter().zip(local_result.matched)
                        {
                            let identity = SeriesIdentity::from_series(&item.series);
                            if execution.is_some() {
                                accounted_series.insert(identity.clone());
                                if matched {
                                    matched_series.insert(identity.clone());
                                }
                            }
                            let entry = acknowledged_acks.entry(identity.clone()).or_insert(0);
                            *entry = entry.saturating_add(1);
                            collected_points
                                .merge_series_points(&item.series, item.points)
                                .map_err(read_merge_error_to_fanout_error)?;
                        }
                    }
                    Err(error @ ReadFanoutError::QueryBudget { .. }) => return Err(error),
                    Err(error @ ReadFanoutError::InvalidRequest { .. }) => return Err(error),
                    Err(_) => {}
                }
            }

            if let Some(execution) = execution {
                self.remote_select_points_bounded(
                    rpc_client,
                    by_owner,
                    &plan.remote_targets,
                    start,
                    end,
                    ring_version,
                    execution,
                    &mut query_memory,
                    &mut acknowledged_acks,
                    &mut accounted_series,
                    &mut matched_series,
                    &mut collected_points,
                )
                .await?;
            } else {
                let remote_points = self
                    .remote_select_points_collect(
                        rpc_client,
                        by_owner,
                        &plan.remote_targets,
                        start,
                        end,
                        ring_version,
                    )
                    .await?;
                for node in remote_points {
                    if let Ok(items) = node.points {
                        for item in items {
                            let identity = SeriesIdentity::from_series(&item.series);
                            let entry = acknowledged_acks.entry(identity.clone()).or_insert(0);
                            *entry = entry.saturating_add(1);
                            collected_points
                                .merge_series_points(&item.series, item.points)
                                .map_err(read_merge_error_to_fanout_error)?;
                        }
                    }
                }
            }
            checkpoint_execution(execution)?;

            let diagnostic_upper = self.modeled_series_consistency_diagnostics_upper_bytes(
                &requirements,
                &acknowledged_acks,
                &unique_series,
            )?;
            query_memory.reserve_additional(execution, diagnostic_upper)?;
            checkpoint_execution(execution)?;
            let consistency_gaps = self.series_consistency_gaps(&requirements, &acknowledged_acks);
            let metadata = match self
                .evaluate_consistency_gaps(FANOUT_OPERATION_SELECT_POINTS, consistency_gaps)
            {
                Ok(metadata) => metadata,
                Err(error) => {
                    return Err(error.with_consistency_reservation(query_memory.reservation.take()))
                }
            };

            if let Some(execution) = execution {
                for (identity, series) in &unique_series {
                    if accounted_series.contains(identity) {
                        continue;
                    }
                    execution
                        .charge_returned_bytes(modeled_series_points_identity_returned_bytes(
                            series,
                        ))
                        .map_err(query_budget_error_to_fanout_error)?;
                }
            }

            let mut merged_points = collected_points.into_points();
            let mut merged = Vec::with_capacity(unique_series.len());
            let mut matched = Vec::with_capacity(unique_series.len());
            for (identity, series) in unique_series {
                matched.push(execution.is_none() || matched_series.contains(&identity));
                let points = merged_points.remove(&identity).unwrap_or_default();
                merged.push(SeriesPoints { series, points });
            }
            if let Some(execution) = execution {
                observe_series_points_vectors(execution, &merged)?;
                query_memory.reserve_additional(
                    Some(execution),
                    modeled_series_points_vec_retained_bytes(&merged),
                )?;
            }
            checkpoint_execution(execution)?;
            let reservation = match execution {
                Some(execution) => Some(match query_memory.reservation.take() {
                    Some(reservation) => reservation,
                    None => execution
                        .reserve_memory(0)
                        .map_err(query_budget_error_to_fanout_error)?,
                }),
                None => None,
            };
            Ok::<_, ReadFanoutError>(ReadFanoutResponse {
                value: AccountedSeriesPoints {
                    series: merged,
                    matched,
                    _reservation: reservation,
                },
                metadata,
                _reservation: None,
            })
        }
        .await;
        track_fanout_operation_complete(
            FANOUT_OPERATION_SELECT_POINTS,
            saturating_elapsed_nanos(started.elapsed()),
            result.is_err(),
        );
        result
    }

    #[cfg(test)]
    fn owners_for_series(
        &self,
        metric: &str,
        labels: &[Label],
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<Vec<String>, ReadFanoutError> {
        Ok(self
            .owners_for_series_borrowed(metric, labels, owner_mode)?
            .to_vec())
    }

    fn owners_for_series_borrowed<'a>(
        &'a self,
        metric: &str,
        labels: &[Label],
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<&'a [String], ReadFanoutError> {
        let series_hash = stable_series_identity_hash(metric, labels);
        let shard = self.ring.shard_for_series_id(series_hash);
        let Some(owners) = self.ring.owners_for_shard(shard) else {
            return Err(ReadFanoutError::MissingShardOwners { shard });
        };
        if owners.is_empty() {
            return Err(ReadFanoutError::MissingShardOwners { shard });
        }
        Ok(match owner_mode {
            ReadPlanOwnerMode::AllReplicas => owners,
            ReadPlanOwnerMode::PrimaryOnly => &owners[..1],
        })
    }

    fn modeled_select_points_planning_upper_bound(
        &self,
        series: &[MetricSeries],
    ) -> Result<u64, ReadFanoutError> {
        let candidate_count = series
            .len()
            .min(usize::try_from(self.ring.shard_count()).unwrap_or(usize::MAX));
        let candidate_tree_bytes = saturating_u64_from_usize(candidate_count)
            .saturating_mul(modeled_btree_entry_bytes::<u32, ()>());
        let mut bytes = candidate_tree_bytes
            .saturating_add(modeled_vec_capacity_bytes::<u32>(candidate_count))
            // Conservatively retain enough space for a wholly local plan as well as the
            // remote-target representation below.
            .saturating_add(modeled_vec_capacity_bytes::<u32>(candidate_count))
            // Unique-series values and identities.
            .saturating_add(modeled_metadata_merge_bytes(series))
            // Complete local and remote batch accounting retains one existence bit per selector.
            .saturating_add(modeled_vec_capacity_bytes::<bool>(series.len()));

        let mut owner_assignments = 0usize;
        for item in series {
            bytes = bytes
                .saturating_add(modeled_series_identity_map_entry_bytes::<ReadRequirement>(
                    item,
                ))
                .saturating_add(modeled_series_identity_map_entry_bytes::<usize>(item))
                .saturating_add(
                    modeled_series_identity_map_entry_bytes::<()>(item).saturating_mul(2),
                );

            let owners = self.owners_for_series_borrowed(
                &item.name,
                &item.labels,
                self.policy.points_owner_mode(),
            )?;
            for owner in owners {
                let Some(endpoint) = self.planner.endpoint_for_node(owner) else {
                    return Err(ReadFanoutError::MissingOwnerEndpoint {
                        node_id: owner.clone(),
                    });
                };
                owner_assignments = owner_assignments.saturating_add(1);

                bytes = bytes
                    // `ShardAwareQueryPlanner::plan_with_candidates` temporarily groups shard
                    // IDs by owned node ID in a B-tree of B-trees.
                    .saturating_add(modeled_btree_entry_bytes::<String, BTreeSet<u32>>())
                    .saturating_add(modeled_owned_str_retained_bytes(owner))
                    .saturating_add(modeled_btree_entry_bytes::<u32, ()>())
                    // The completed plan owns a target ID, endpoint, and shard vector. Counting
                    // one target per assignment is an upper bound when assignments coalesce.
                    .saturating_add(modeled_owned_str_retained_bytes(owner))
                    .saturating_add(modeled_owned_str_retained_bytes(endpoint))
                    .saturating_add(modeled_vec_capacity_bytes::<u32>(1))
                    // The fanout owner map owns a node ID plus a clone of this series.
                    .saturating_add(modeled_btree_entry_bytes::<String, Vec<MetricSeries>>())
                    .saturating_add(modeled_owned_str_retained_bytes(owner))
                    .saturating_add(modeled_vec_capacity_bytes::<MetricSeries>(1))
                    .saturating_add(modeled_metric_series_heap_retained_bytes(item))
                    // A bounded remote call owns a batch clone and the RPC request owns a second
                    // selector clone while the owner map remains live.
                    .saturating_add(modeled_vec_capacity_bytes::<MetricSeries>(1).saturating_mul(2))
                    .saturating_add(
                        modeled_metric_series_heap_retained_bytes(item).saturating_mul(2),
                    );
            }
        }

        Ok(
            bytes.saturating_add(modeled_vec_capacity_bytes::<ReadPlanTarget>(
                owner_assignments,
            )),
        )
    }

    fn modeled_select_series_planning_upper_bound(
        &self,
        selection: &SeriesSelection,
    ) -> Result<u64, ReadFanoutError> {
        let candidate_count = usize::try_from(self.ring.shard_count()).unwrap_or(usize::MAX);
        let mut bytes = saturating_u64_from_usize(candidate_count)
            .saturating_mul(modeled_btree_entry_bytes::<u32, ()>())
            .saturating_add(modeled_vec_capacity_bytes::<u32>(candidate_count).saturating_mul(2))
            .saturating_add(
                saturating_u64_from_usize(candidate_count)
                    .saturating_mul(modeled_btree_entry_bytes::<u32, ReadRequirement>()),
            )
            .saturating_add(
                saturating_u64_from_usize(candidate_count)
                    .saturating_mul(modeled_btree_entry_bytes::<u32, usize>()),
            );
        let selection_bytes = selection
            .metric
            .as_deref()
            .map_or(0, modeled_owned_str_retained_bytes)
            .saturating_add(modeled_vec_capacity_bytes::<tsink::SeriesMatcher>(
                selection.matchers.len(),
            ))
            .saturating_add(selection.matchers.iter().fold(0u64, |total, matcher| {
                total
                    .saturating_add(modeled_owned_str_retained_bytes(&matcher.name))
                    .saturating_add(modeled_owned_str_retained_bytes(&matcher.value))
            }));

        let mut owner_assignments = 0usize;
        for shard in 0..self.ring.shard_count() {
            let Some(owners) = self.ring.owners_for_shard(shard) else {
                return Err(ReadFanoutError::MissingShardOwners { shard });
            };
            if owners.is_empty() {
                return Err(ReadFanoutError::MissingShardOwners { shard });
            }
            let owners = match self.policy.metadata_owner_mode() {
                ReadPlanOwnerMode::AllReplicas => owners,
                ReadPlanOwnerMode::PrimaryOnly => &owners[..1],
            };
            for owner in owners {
                let Some(endpoint) = self.planner.endpoint_for_node(owner) else {
                    return Err(ReadFanoutError::MissingOwnerEndpoint {
                        node_id: owner.clone(),
                    });
                };
                owner_assignments = owner_assignments.saturating_add(1);
                bytes = bytes
                    .saturating_add(modeled_btree_entry_bytes::<String, BTreeSet<u32>>())
                    .saturating_add(modeled_owned_str_retained_bytes(owner))
                    .saturating_add(modeled_btree_entry_bytes::<u32, ()>())
                    .saturating_add(modeled_owned_str_retained_bytes(owner))
                    .saturating_add(modeled_owned_str_retained_bytes(endpoint))
                    .saturating_add(modeled_vec_capacity_bytes::<u32>(1))
                    // One bounded request retains its shard scope and selection clone.
                    .saturating_add(modeled_vec_capacity_bytes::<u32>(1))
                    .saturating_add(selection_bytes);
            }
        }
        Ok(
            bytes.saturating_add(modeled_vec_capacity_bytes::<ReadPlanTarget>(
                owner_assignments,
            )),
        )
    }

    fn plan_select_series(
        &self,
        selection: &SeriesSelection,
        ring_version: u64,
    ) -> Result<ReadExecutionPlan, ReadFanoutError> {
        self.planner
            .plan_select_series_with_owner_mode(
                selection,
                ring_version,
                self.policy.metadata_owner_mode(),
            )
            .map_err(read_planner_error_to_fanout_error)
    }

    fn plan_list_metrics(&self, ring_version: u64) -> Result<ReadExecutionPlan, ReadFanoutError> {
        self.planner
            .plan_list_metrics_with_owner_mode(ring_version, self.policy.metadata_owner_mode())
            .map_err(read_planner_error_to_fanout_error)
    }

    fn plan_select_points(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
    ) -> Result<ReadExecutionPlan, ReadFanoutError> {
        self.planner
            .plan_select_points_with_owner_mode(
                series,
                start,
                end,
                ring_version,
                self.policy.points_owner_mode(),
            )
            .map_err(read_planner_error_to_fanout_error)
    }

    fn shard_requirements(
        &self,
        shards: &[u32],
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<BTreeMap<u32, ReadRequirement>, ReadFanoutError> {
        let mut requirements = BTreeMap::new();
        for shard in shards {
            let Some(owners) = self.ring.owners_for_shard(*shard) else {
                return Err(ReadFanoutError::MissingShardOwners { shard: *shard });
            };
            if owners.is_empty() {
                return Err(ReadFanoutError::MissingShardOwners { shard: *shard });
            }
            let total_replicas = match owner_mode {
                ReadPlanOwnerMode::AllReplicas => owners.len(),
                ReadPlanOwnerMode::PrimaryOnly => 1,
            };
            requirements.insert(
                *shard,
                ReadRequirement {
                    required_acks: self.policy.required_acks(total_replicas),
                    total_replicas,
                },
            );
        }
        Ok(requirements)
    }

    fn shard_consistency_gaps(
        &self,
        operation: &str,
        requirements: &BTreeMap<u32, ReadRequirement>,
        acknowledged_acks: &BTreeMap<u32, usize>,
    ) -> Vec<ReadConsistencyGap> {
        let mut gaps = Vec::new();
        for (shard, requirement) in requirements {
            let acknowledged = acknowledged_acks.get(shard).copied().unwrap_or(0);
            if acknowledged < requirement.required_acks {
                gaps.push(ReadConsistencyGap {
                    operation: operation.to_string(),
                    target: format!("shard {shard}"),
                    required_acks: requirement.required_acks,
                    acknowledged_acks: acknowledged,
                    total_replicas: requirement.total_replicas,
                });
            }
        }
        gaps
    }

    fn modeled_shard_consistency_diagnostics_upper_bytes(
        &self,
        operation: &str,
        requirements: &BTreeMap<u32, ReadRequirement>,
        acknowledged_acks: &BTreeMap<u32, usize>,
    ) -> u64 {
        let mut upper = ConsistencyDiagnosticUpper::default();
        for (shard, requirement) in requirements {
            let acknowledged = acknowledged_acks.get(shard).copied().unwrap_or(0);
            if acknowledged < requirement.required_acks {
                // `u32` shard IDs require at most ten decimal digits.
                upper.push_target(operation, "shard ".len().saturating_add(10), 0);
            }
        }
        upper.finish(
            operation,
            self.policy.requires_full_consistency()
                || matches!(
                    self.partial_response_policy,
                    ClusterReadPartialResponsePolicy::Deny
                ),
        )
    }

    fn series_consistency_gaps(
        &self,
        requirements: &BTreeMap<SeriesIdentity, ReadRequirement>,
        acknowledged_acks: &BTreeMap<SeriesIdentity, usize>,
    ) -> Vec<ReadConsistencyGap> {
        let mut gaps = Vec::new();
        for (identity, requirement) in requirements {
            let acknowledged = acknowledged_acks.get(identity).copied().unwrap_or(0);
            if acknowledged < requirement.required_acks {
                gaps.push(ReadConsistencyGap {
                    operation: FANOUT_OPERATION_SELECT_POINTS.to_string(),
                    target: format!("series {}", identity.display()),
                    required_acks: requirement.required_acks,
                    acknowledged_acks: acknowledged,
                    total_replicas: requirement.total_replicas,
                });
            }
        }
        gaps
    }

    fn modeled_series_consistency_diagnostics_upper_bytes(
        &self,
        requirements: &BTreeMap<SeriesIdentity, ReadRequirement>,
        acknowledged_acks: &BTreeMap<SeriesIdentity, usize>,
        unique_series: &BTreeMap<SeriesIdentity, MetricSeries>,
    ) -> Result<u64, ReadFanoutError> {
        let mut upper = ConsistencyDiagnosticUpper::default();
        for (identity, requirement) in requirements {
            let acknowledged = acknowledged_acks.get(identity).copied().unwrap_or(0);
            if acknowledged >= requirement.required_acks {
                continue;
            }
            let series =
                unique_series
                    .get(identity)
                    .ok_or_else(|| ReadFanoutError::InvalidRequest {
                        message: "series consistency requirements omitted their source identity"
                            .to_string(),
                    })?;
            let target_len = "series "
                .len()
                .saturating_add(modeled_series_identity_display_len(series));
            upper.push_target(
                FANOUT_OPERATION_SELECT_POINTS,
                target_len,
                modeled_series_target_construction_transient_bytes(series),
            );
        }
        Ok(upper.finish(
            FANOUT_OPERATION_SELECT_POINTS,
            self.policy.requires_full_consistency()
                || matches!(
                    self.partial_response_policy,
                    ClusterReadPartialResponsePolicy::Deny
                ),
        ))
    }

    fn evaluate_consistency_gaps(
        &self,
        operation: &str,
        mut consistency_gaps: Vec<ReadConsistencyGap>,
    ) -> Result<ReadFanoutResponseMetadata, ReadFanoutError> {
        let mut metadata =
            ReadFanoutResponseMetadata::success(self.policy.mode(), self.partial_response_policy);
        if consistency_gaps.is_empty() {
            return Ok(metadata);
        }
        // Gap targets are unique, so unstable sorting preserves deterministic output ordering
        // without allocating the scratch buffer used by stable slice sorting.
        consistency_gaps.sort_unstable_by(|left, right| left.target.cmp(&right.target));

        if self.policy.requires_full_consistency()
            || matches!(
                self.partial_response_policy,
                ClusterReadPartialResponsePolicy::Deny
            )
        {
            let first = &consistency_gaps[0];
            return Err(ReadFanoutError::ConsistencyUnmet {
                operation: operation.to_string(),
                mode: self.policy.mode(),
                target: first.target.clone(),
                required_acks: first.required_acks,
                acknowledged_acks: first.acknowledged_acks,
                total_replicas: first.total_replicas,
                _reservation: None,
            });
        }

        metadata.partial_response = true;
        metadata.warnings = consistency_gaps
            .into_iter()
            .map(|gap| {
                format!(
                    "partial read for {} on {}: mode={}, required_acks={}, acknowledged_acks={}, total_replicas={}",
                    gap.operation,
                    gap.target,
                    self.policy.mode(),
                    gap.required_acks,
                    gap.acknowledged_acks,
                    gap.total_replicas
                )
            })
            .collect();
        Ok(metadata)
    }

    async fn local_select_series(
        &self,
        storage: &Arc<dyn Storage>,
        selection: &SeriesSelection,
        shards: &[u32],
        execution: Option<&QueryExecution>,
        accounting: QueryExecutionAccounting,
    ) -> Result<AccountedMetricSeries, ReadFanoutError> {
        if execution.is_some() && accounting != QueryExecutionAccounting::Complete {
            return Err(ReadFanoutError::InvalidRequest {
                message: "bounded local select_series requires complete result accounting"
                    .to_string(),
            });
        }
        let storage = Arc::clone(storage);
        let selection = selection.clone();
        let shard_scope = MetadataShardScope::new(self.ring.shard_count(), shards.to_vec());
        let bounded = execution.is_some();
        let execution = execution.cloned();
        let result = tokio::task::spawn_blocking(move || match execution {
            Some(execution) => storage.select_series_in_shards_with_execution_result(
                &selection,
                &shard_scope,
                &execution,
            ),
            None => storage
                .select_series_in_shards(&selection, &shard_scope)
                .map(tsink::SelectSeriesExecutionResult::unaccounted),
        })
        .await;
        match result {
            Ok(Ok(mut series)) if bounded => {
                let reservation = series.take_memory_reservation().ok_or_else(|| {
                    ReadFanoutError::InvalidRequest {
                        message: "complete select_series accounting omitted its result reservation"
                            .to_string(),
                    }
                })?;
                Ok(AccountedMetricSeries {
                    series: std::mem::take(&mut series.series),
                    _reservation: Some(reservation),
                })
            }
            Ok(Ok(mut series)) => Ok(AccountedMetricSeries {
                series: std::mem::take(&mut series.series),
                _reservation: None,
            }),
            Ok(Err(TsinkError::QueryBudget(error))) => {
                Err(query_budget_error_to_fanout_error(error))
            }
            Ok(Err(err)) => Err(ReadFanoutError::LocalSelectSeries {
                message: err.to_string(),
            }),
            Err(err) => Err(ReadFanoutError::TaskJoin {
                message: err.to_string(),
            }),
        }
    }

    async fn local_list_metrics(
        &self,
        storage: &Arc<dyn Storage>,
        shards: &[u32],
    ) -> Result<Vec<MetricSeries>, ReadFanoutError> {
        let storage = Arc::clone(storage);
        let shard_scope = MetadataShardScope::new(self.ring.shard_count(), shards.to_vec());
        let result =
            tokio::task::spawn_blocking(move || storage.list_metrics_in_shards(&shard_scope)).await;
        match result {
            Ok(Ok(series)) => Ok(series),
            Ok(Err(err)) => Err(ReadFanoutError::LocalListMetrics {
                message: err.to_string(),
            }),
            Err(err) => Err(ReadFanoutError::TaskJoin {
                message: err.to_string(),
            }),
        }
    }

    async fn local_select_points(
        &self,
        storage: &Arc<dyn Storage>,
        series: Vec<MetricSeries>,
        start: i64,
        end: i64,
        execution: Option<&QueryExecution>,
        accounting: QueryExecutionAccounting,
    ) -> Result<AccountedSeriesPoints, ReadFanoutError> {
        if execution.is_some() && accounting != QueryExecutionAccounting::Complete {
            return Err(ReadFanoutError::InvalidRequest {
                message: "bounded local select_batch requires complete result accounting"
                    .to_string(),
            });
        }
        let storage = Arc::clone(storage);
        let series_count = series.len();
        let bounded = execution.is_some();
        let execution = execution.cloned();
        let result = tokio::task::spawn_blocking(move || {
            let detailed = match execution {
                Some(execution) => {
                    storage.select_many_with_execution_result(&series, start, end, &execution)?
                }
                None => tsink::SelectManyExecutionResult::unaccounted(
                    storage.select_many(&series, start, end)?,
                ),
            };
            Ok::<_, TsinkError>((series, detailed))
        })
        .await;
        match result {
            Ok(Ok((selectors, mut detailed))) => {
                if detailed.series.len() != selectors.len()
                    || detailed
                        .series
                        .iter()
                        .zip(&selectors)
                        .any(|(item, selector)| item.series != *selector)
                {
                    return Err(ReadFanoutError::InvalidRequest {
                        message:
                            "select_batch result identities or ordering did not match the request"
                                .to_string(),
                    });
                }
                if bounded {
                    let matched = detailed.matched_selectors.take().ok_or_else(|| {
                        ReadFanoutError::InvalidRequest {
                            message:
                                "complete select_batch accounting omitted selector-existence bits"
                                    .to_string(),
                        }
                    })?;
                    if matched.len() != selectors.len() {
                        return Err(ReadFanoutError::InvalidRequest {
                            message: format!(
                                "select_batch returned {} existence bits for {} selectors",
                                matched.len(),
                                selectors.len()
                            ),
                        });
                    }
                    let reservation = detailed.take_memory_reservation().ok_or_else(|| {
                        ReadFanoutError::InvalidRequest {
                            message:
                                "complete select_batch accounting omitted its result reservation"
                                    .to_string(),
                        }
                    })?;
                    Ok(AccountedSeriesPoints {
                        series: std::mem::take(&mut detailed.series),
                        matched,
                        _reservation: Some(reservation),
                    })
                } else {
                    let matched = vec![true; detailed.series.len()];
                    Ok(AccountedSeriesPoints {
                        series: std::mem::take(&mut detailed.series),
                        matched,
                        _reservation: None,
                    })
                }
            }
            Ok(Err(TsinkError::QueryBudget(error))) => {
                Err(query_budget_error_to_fanout_error(error))
            }
            Ok(Err(err)) => Err(ReadFanoutError::LocalSelectBatch {
                series_count,
                message: err.to_string(),
            }),
            Err(err) => Err(ReadFanoutError::TaskJoin {
                message: err.to_string(),
            }),
        }
    }

    async fn remote_select_series_collect(
        &self,
        rpc_client: &RpcClient,
        selection: &SeriesSelection,
        ring_version: u64,
        targets: &[ReadPlanTarget],
    ) -> Result<Vec<RemoteSelectSeriesResult>, ReadFanoutError> {
        let semaphore = Arc::new(Semaphore::new(self.fanout_concurrency));
        let mut tasks = tokio::task::JoinSet::new();
        for target in targets {
            let permit = semaphore.clone().acquire_owned().await.map_err(|err| {
                ReadFanoutError::TaskJoin {
                    message: err.to_string(),
                }
            })?;
            let rpc_client = rpc_client.clone();
            let node_id = target.node_id.clone();
            let endpoint = target.endpoint.clone();
            let shards = target.shards.clone();
            let request = InternalSelectSeriesRequest {
                ring_version,
                shard_scope: Some(MetadataShardScope::new(
                    self.ring.shard_count(),
                    shards.clone(),
                )),
                selection: selection.clone(),
                query_limits: None,
            };
            tasks.spawn(async move {
                let _permit = permit;
                let request_started = Instant::now();
                let response = rpc_client.select_series(&endpoint, &request).await;
                let request_duration_nanos = saturating_elapsed_nanos(request_started.elapsed());
                track_remote_request(
                    &node_id,
                    FANOUT_OPERATION_SELECT_SERIES,
                    request_duration_nanos,
                    response.is_err(),
                );
                RemoteSelectSeriesResult {
                    node_id: node_id.clone(),
                    shards,
                    series: response.map(|response| response.series).map_err(|source| {
                        ReadFanoutError::RemoteSelectSeries {
                            node_id,
                            endpoint,
                            source: Box::new(source),
                        }
                    }),
                }
            });
        }

        let mut out = Vec::new();
        while let Some(join_result) = tasks.join_next().await {
            out.push(join_result.map_err(|err| ReadFanoutError::TaskJoin {
                message: err.to_string(),
            })?);
        }
        out.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn remote_select_series_bounded(
        &self,
        rpc_client: &RpcClient,
        selection: &SeriesSelection,
        ring_version: u64,
        targets: &[ReadPlanTarget],
        execution: &QueryExecution,
        query_memory: &mut FanoutQueryMemory,
        acknowledged_acks: &mut BTreeMap<u32, usize>,
        merged: &mut SeriesMetadataMerger,
        operation: &'static str,
    ) -> Result<(), ReadFanoutError> {
        for target in targets {
            execution
                .checkpoint()
                .map_err(query_budget_error_to_fanout_error)?;
            let query_limits = remaining_remote_select_limits(execution)?;
            let request = InternalSelectSeriesRequest {
                ring_version,
                shard_scope: Some(MetadataShardScope::new(
                    self.ring.shard_count(),
                    target.shards.clone(),
                )),
                selection: selection.clone(),
                query_limits: Some(query_limits),
            };
            let request_started = Instant::now();
            let response = rpc_client
                .select_series_accounted(&target.endpoint, &request, execution)
                .await;
            let request_duration_nanos = saturating_elapsed_nanos(request_started.elapsed());
            track_remote_request(
                &target.node_id,
                operation,
                request_duration_nanos,
                response.is_err(),
            );
            let mut response = match response {
                Ok(response) => response,
                Err(RpcError::QueryBudget { error }) => {
                    return Err(query_budget_error_to_fanout_error(error));
                }
                Err(source) if source.retryable() => {
                    continue;
                }
                Err(source) => {
                    return Err(ReadFanoutError::RemoteSelectSeries {
                        node_id: target.node_id.clone(),
                        endpoint: target.endpoint.clone(),
                        source: Box::new(source),
                    });
                }
            };
            let accounting = response.response.accounting.take().ok_or_else(|| {
                ReadFanoutError::RemoteSelectSeries {
                    node_id: target.node_id.clone(),
                    endpoint: target.endpoint.clone(),
                    source: Box::new(RpcError::Deserialize {
                        message: "bounded select_series response omitted execution accounting"
                            .to_string(),
                    }),
                }
            })?;
            let snapshot = accounting.execution;
            let series_count = saturating_u64_from_usize(response.response.series.len());
            if snapshot.series_matched < series_count {
                return Err(ReadFanoutError::RemoteSelectSeries {
                    node_id: target.node_id.clone(),
                    endpoint: target.endpoint.clone(),
                    source: Box::new(RpcError::Deserialize {
                        message: format!(
                            "bounded select_series reported {} matches for {series_count} returned series",
                            snapshot.series_matched
                        ),
                    }),
                });
            }
            let returned_bytes =
                modeled_metric_series_slice_returned_bytes(&response.response.series);
            if snapshot.returned_bytes < returned_bytes {
                return Err(ReadFanoutError::RemoteSelectSeries {
                    node_id: target.node_id.clone(),
                    endpoint: target.endpoint.clone(),
                    source: Box::new(RpcError::Deserialize {
                        message: format!(
                            "bounded select_series reported {} returned bytes below modeled response {returned_bytes}",
                            snapshot.returned_bytes
                        ),
                    }),
                });
            }
            charge_remote_select_work(execution, snapshot)?;
            observe_metric_series_vector(execution, &response.response.series)?;
            query_memory.reserve_additional(
                Some(execution),
                modeled_metric_series_vec_retained_bytes(&response.response.series)
                    .saturating_add(modeled_metadata_merge_bytes(&response.response.series)),
            )?;

            for shard in &target.shards {
                let entry = acknowledged_acks.entry(*shard).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            for series in response.response.series {
                let returned_bytes = modeled_metric_series_returned_bytes(&series);
                let inserted = merged
                    .insert_counted(series)
                    .map_err(read_merge_error_to_fanout_error)?;
                if inserted {
                    execution
                        .charge_series_matched(1)
                        .map_err(query_budget_error_to_fanout_error)?;
                    execution
                        .charge_returned_bytes(returned_bytes)
                        .map_err(query_budget_error_to_fanout_error)?;
                }
            }
            drop(response.reservation);
        }
        Ok(())
    }

    async fn remote_list_metrics_collect(
        &self,
        rpc_client: &RpcClient,
        ring_version: u64,
        targets: &[ReadPlanTarget],
    ) -> Result<Vec<RemoteListMetricsResult>, ReadFanoutError> {
        let semaphore = Arc::new(Semaphore::new(self.fanout_concurrency));
        let mut tasks = tokio::task::JoinSet::new();
        for target in targets {
            let permit = semaphore.clone().acquire_owned().await.map_err(|err| {
                ReadFanoutError::TaskJoin {
                    message: err.to_string(),
                }
            })?;
            let rpc_client = rpc_client.clone();
            let node_id = target.node_id.clone();
            let endpoint = target.endpoint.clone();
            let shards = target.shards.clone();
            let request = InternalListMetricsRequest {
                ring_version,
                shard_scope: Some(MetadataShardScope::new(
                    self.ring.shard_count(),
                    shards.clone(),
                )),
                query_limits: None,
            };
            tasks.spawn(async move {
                let _permit = permit;
                let request_started = Instant::now();
                let response = rpc_client
                    .list_metrics_with_request(&endpoint, &request)
                    .await;
                let request_duration_nanos = saturating_elapsed_nanos(request_started.elapsed());
                track_remote_request(
                    &node_id,
                    FANOUT_OPERATION_LIST_METRICS,
                    request_duration_nanos,
                    response.is_err(),
                );
                RemoteListMetricsResult {
                    node_id: node_id.clone(),
                    shards,
                    series: response
                        .map(|InternalListMetricsResponse { series, .. }| series)
                        .map_err(|source| ReadFanoutError::RemoteListMetrics {
                            node_id,
                            endpoint,
                            source: Box::new(source),
                        }),
                }
            });
        }

        let mut out = Vec::new();
        while let Some(join_result) = tasks.join_next().await {
            out.push(join_result.map_err(|err| ReadFanoutError::TaskJoin {
                message: err.to_string(),
            })?);
        }
        out.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn remote_select_points_bounded(
        &self,
        rpc_client: &RpcClient,
        mut by_owner: BTreeMap<String, Vec<MetricSeries>>,
        targets: &[ReadPlanTarget],
        start: i64,
        end: i64,
        ring_version: u64,
        execution: &QueryExecution,
        query_memory: &mut FanoutQueryMemory,
        acknowledged_acks: &mut BTreeMap<SeriesIdentity, usize>,
        accounted_series: &mut BTreeSet<SeriesIdentity>,
        matched_series: &mut BTreeSet<SeriesIdentity>,
        collected_points: &mut SeriesPointsMerger,
    ) -> Result<(), ReadFanoutError> {
        // Execute bounded peer batches in deterministic order. This lets each request receive the
        // remaining cumulative scan/step budget and prevents several peers from independently
        // consuming the full query allowance before the coordinator can observe their work.
        for target in targets {
            let Some(series) = by_owner.remove(&target.node_id) else {
                continue;
            };
            for batch in series.chunks(REMOTE_SELECT_BATCH_SIZE.max(1)) {
                execution
                    .checkpoint()
                    .map_err(query_budget_error_to_fanout_error)?;
                let selectors = batch.to_vec();
                let query_limits = remaining_remote_select_limits(execution)?;
                let response = match remote_select_points_batch_with_legacy_fallback(
                    rpc_client,
                    &target.node_id,
                    &target.endpoint,
                    selectors.clone(),
                    start,
                    end,
                    ring_version,
                    Some(query_limits),
                    Some(execution),
                )
                .await
                {
                    Ok(response) => response,
                    Err(error)
                        if error.retryable() && !is_remote_select_query_control_error(&error) =>
                    {
                        // Preserve the configured partial-response semantics. No acknowledgement
                        // is recorded for this batch, so strict/deny policies still fail and
                        // allow-mode responses carry an explicit consistency warning.
                        break;
                    }
                    Err(error) => return Err(error),
                };
                let Some(snapshot) = response.execution else {
                    return Err(ReadFanoutError::RemoteSelectBatch {
                        node_id: target.node_id.clone(),
                        endpoint: target.endpoint.clone(),
                        series_count: selectors.len(),
                        source: Box::new(RpcError::Deserialize {
                            message: "bounded select_batch response omitted execution accounting"
                                .to_string(),
                        }),
                    });
                };
                let Some(matched) = response.matched else {
                    return Err(ReadFanoutError::RemoteSelectBatch {
                        node_id: target.node_id.clone(),
                        endpoint: target.endpoint.clone(),
                        series_count: selectors.len(),
                        source: Box::new(RpcError::Deserialize {
                            message:
                                "bounded select_batch response omitted matched-selector accounting"
                                    .to_string(),
                        }),
                    });
                };
                validate_remote_select_batch_accounting(
                    &selectors,
                    &response.series,
                    &matched,
                    snapshot,
                )
                .map_err(|message| ReadFanoutError::RemoteSelectBatch {
                    node_id: target.node_id.clone(),
                    endpoint: target.endpoint.clone(),
                    series_count: selectors.len(),
                    source: Box::new(RpcError::Deserialize { message }),
                })?;
                charge_remote_select_work(execution, snapshot)?;
                observe_series_points_vectors(execution, &response.series)?;
                query_memory.reserve_additional(
                    Some(execution),
                    modeled_series_points_vec_retained_bytes(&response.series)
                        .saturating_add(modeled_points_merge_bytes(&response.series)),
                )?;
                drop(response._reservation);

                for (item, matched) in response.series.into_iter().zip(matched) {
                    let identity = SeriesIdentity::from_series(&item.series);
                    let entry = acknowledged_acks.entry(identity.clone()).or_insert(0);
                    *entry = entry.saturating_add(1);
                    let identity_bytes =
                        modeled_series_points_identity_returned_bytes(&item.series);
                    let outcome = collected_points
                        .merge_series_points_counted(&item.series, item.points)
                        .map_err(read_merge_error_to_fanout_error)?;
                    accounted_series.insert(identity.clone());
                    if matched && matched_series.insert(identity) {
                        execution
                            .charge_series_matched(1)
                            .map_err(query_budget_error_to_fanout_error)?;
                    }
                    execution
                        .charge_samples_returned(saturating_u64_from_usize(outcome.inserted_points))
                        .map_err(query_budget_error_to_fanout_error)?;
                    execution
                        .charge_returned_bytes(outcome.inserted_point_bytes.saturating_add(
                            if outcome.inserted_series {
                                identity_bytes
                            } else {
                                0
                            },
                        ))
                        .map_err(query_budget_error_to_fanout_error)?;
                }
            }
        }
        if let Some(node_id) = by_owner.keys().next().cloned() {
            return Err(ReadFanoutError::MissingOwnerEndpoint { node_id });
        }
        Ok(())
    }

    async fn remote_select_points_collect(
        &self,
        rpc_client: &RpcClient,
        mut by_owner: BTreeMap<String, Vec<MetricSeries>>,
        targets: &[ReadPlanTarget],
        start: i64,
        end: i64,
        ring_version: u64,
    ) -> Result<Vec<RemoteSelectPointsResult>, ReadFanoutError> {
        let semaphore = Arc::new(Semaphore::new(self.fanout_concurrency));
        let mut tasks = tokio::task::JoinSet::new();

        for target in targets {
            let Some(series) = by_owner.remove(&target.node_id) else {
                continue;
            };
            let permit = semaphore.clone().acquire_owned().await.map_err(|err| {
                ReadFanoutError::TaskJoin {
                    message: err.to_string(),
                }
            })?;
            let rpc_client = rpc_client.clone();
            let node_id = target.node_id.clone();
            let endpoint = target.endpoint.clone();
            tasks.spawn(async move {
                let _permit = permit;
                let mut out = Vec::with_capacity(series.len());
                for batch in series.chunks(REMOTE_SELECT_BATCH_SIZE.max(1)) {
                    let batch = batch.to_vec();
                    match remote_select_points_batch_with_legacy_fallback(
                        &rpc_client,
                        &node_id,
                        &endpoint,
                        batch,
                        start,
                        end,
                        ring_version,
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(mut batch_points) => out.append(&mut batch_points.series),
                        Err(err) => {
                            return RemoteSelectPointsResult {
                                node_id: node_id.clone(),
                                points: Err(err),
                            };
                        }
                    }
                }
                RemoteSelectPointsResult {
                    node_id,
                    points: Ok(out),
                }
            });
        }
        if let Some(node_id) = by_owner.keys().next().cloned() {
            return Err(ReadFanoutError::MissingOwnerEndpoint { node_id });
        }

        let mut out = Vec::new();
        while let Some(join_result) = tasks.join_next().await {
            out.push(join_result.map_err(|err| ReadFanoutError::TaskJoin {
                message: err.to_string(),
            })?);
        }

        out.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(out)
    }
}

#[allow(clippy::too_many_arguments)]
async fn remote_select_points_batch_with_legacy_fallback(
    rpc_client: &RpcClient,
    node_id: &str,
    endpoint: &str,
    series: Vec<MetricSeries>,
    start: i64,
    end: i64,
    ring_version: u64,
    query_limits: Option<QueryWorkLimits>,
    execution: Option<&QueryExecution>,
) -> Result<RemoteSelectBatchOutcome, ReadFanoutError> {
    let request_started = Instant::now();
    let request = crate::cluster::rpc::InternalSelectBatchRequest {
        ring_version,
        selectors: series.clone(),
        start,
        end,
        query_limits,
    };
    let response = match execution {
        Some(execution) => rpc_client
            .select_batch_accounted(endpoint, &request, execution)
            .await
            .map(|accounted| (accounted.response, Some(accounted.reservation))),
        None => rpc_client
            .select_batch(endpoint, &request)
            .await
            .map(|response| (response, None)),
    };
    let request_duration_nanos = saturating_elapsed_nanos(request_started.elapsed());
    track_remote_request(
        node_id,
        FANOUT_REMOTE_OPERATION_SELECT_BATCH,
        request_duration_nanos,
        response.is_err(),
    );

    match response {
        Ok((response, reservation)) => {
            let (execution, matched) = response.accounting.map_or((None, None), |accounting| {
                (Some(accounting.execution), accounting.matched_selectors)
            });
            return Ok(RemoteSelectBatchOutcome {
                series: response.series,
                matched,
                execution,
                _reservation: reservation,
            });
        }
        Err(RpcError::QueryBudget { error }) => {
            return Err(query_budget_error_to_fanout_error(error));
        }
        Err(source @ RpcError::HttpStatus { status: 404, .. }) if query_limits.is_some() => {
            return Err(ReadFanoutError::RemoteSelectBatch {
                node_id: node_id.to_string(),
                endpoint: endpoint.to_string(),
                series_count: series.len(),
                source: Box::new(source),
            });
        }
        Err(RpcError::HttpStatus { status: 404, .. }) => {}
        Err(source) => {
            return Err(ReadFanoutError::RemoteSelectBatch {
                node_id: node_id.to_string(),
                endpoint: endpoint.to_string(),
                series_count: series.len(),
                source: Box::new(source),
            });
        }
    }

    let mut out = Vec::with_capacity(series.len());
    for item in series {
        let request = InternalSelectRequest {
            ring_version,
            metric: item.name.clone(),
            labels: item.labels.clone(),
            start,
            end,
        };
        let request_started = Instant::now();
        let response = rpc_client.select(endpoint, &request).await;
        let request_duration_nanos = saturating_elapsed_nanos(request_started.elapsed());
        track_remote_request(
            node_id,
            FANOUT_REMOTE_OPERATION_SELECT_LEGACY,
            request_duration_nanos,
            response.is_err(),
        );
        match response {
            Ok(response) => out.push(SeriesPoints {
                series: item,
                points: response.points,
            }),
            Err(source) => {
                return Err(ReadFanoutError::RemoteSelectBatch {
                    node_id: node_id.to_string(),
                    endpoint: endpoint.to_string(),
                    series_count: 1,
                    source: Box::new(source),
                });
            }
        }
    }

    Ok(RemoteSelectBatchOutcome {
        series: out,
        matched: None,
        execution: None,
        _reservation: None,
    })
}

#[derive(Debug, Clone)]
struct ReadConsistencyGap {
    operation: String,
    target: String,
    required_acks: usize,
    acknowledged_acks: usize,
    total_replicas: usize,
}

#[derive(Debug, Clone, Copy)]
struct ReadRequirement {
    required_acks: usize,
    total_replicas: usize,
}

#[derive(Debug, Clone)]
struct RemoteSelectSeriesResult {
    node_id: String,
    shards: Vec<u32>,
    series: Result<Vec<MetricSeries>, ReadFanoutError>,
}

#[derive(Debug, Clone)]
struct RemoteListMetricsResult {
    node_id: String,
    shards: Vec<u32>,
    series: Result<Vec<MetricSeries>, ReadFanoutError>,
}

#[derive(Debug, Clone)]
struct RemoteSelectPointsResult {
    node_id: String,
    points: Result<Vec<SeriesPoints>, ReadFanoutError>,
}

#[derive(Debug)]
pub(crate) struct AccountedSeriesPoints {
    pub(crate) series: Vec<SeriesPoints>,
    pub(crate) matched: Vec<bool>,
    _reservation: Option<QueryMemoryReservation>,
}

#[derive(Debug)]
pub(crate) struct AccountedMetricSeries {
    pub(crate) series: Vec<MetricSeries>,
    _reservation: Option<QueryMemoryReservation>,
}

impl AccountedMetricSeries {
    pub(crate) fn take_reservation(&mut self) -> Option<QueryMemoryReservation> {
        self._reservation.take()
    }
}

impl AccountedSeriesPoints {
    pub(crate) fn take_reservation(&mut self) -> Option<QueryMemoryReservation> {
        self._reservation.take()
    }
}

#[derive(Debug)]
struct RemoteSelectBatchOutcome {
    series: Vec<SeriesPoints>,
    matched: Option<Vec<bool>>,
    execution: Option<QueryExecutionSnapshot>,
    _reservation: Option<QueryMemoryReservation>,
}

fn read_planner_error_to_fanout_error(err: ReadPlannerError) -> ReadFanoutError {
    match err {
        ReadPlannerError::MissingShardOwners { shard } => {
            ReadFanoutError::MissingShardOwners { shard }
        }
        ReadPlannerError::MissingOwnerEndpoint { node_id } => {
            ReadFanoutError::MissingOwnerEndpoint { node_id }
        }
    }
}

fn read_merge_error_to_fanout_error(err: MergeLimitError) -> ReadFanoutError {
    ReadFanoutError::MergeLimitExceeded {
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::{ClusterConfig, ClusterReadPartialResponsePolicy};
    use crate::cluster::membership::{ClusterNode, MembershipView};
    use crate::cluster::rpc::{
        InternalSelectBatchAccounting, InternalSelectBatchRequest, InternalSelectBatchResponse,
        InternalSelectRequest, InternalSelectResponse, InternalSelectSeriesAccounting,
        InternalSelectSeriesResponse, RpcClientConfig,
    };
    use crate::http::{read_http_request, write_http_response, HttpResponse};
    use serde_json::json;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tsink::{
        DataPoint, QueryBudget, QueryBudgetLimits, QueryCancellationToken, QueryLimitReason,
        QueryWorkLimits, Row, StorageBuilder, TimestampPrecision,
    };

    #[test]
    fn strict_mode_requires_full_consistency() {
        assert!(ReadPolicy::new(ClusterReadConsistency::Strict).requires_full_consistency());
        assert!(!ReadPolicy::new(ClusterReadConsistency::Eventual).requires_full_consistency());
    }

    #[test]
    fn remaining_remote_select_limits_rejects_exhausted_scan_budget() {
        let budget = QueryBudget::new(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_samples_scanned: Some(3),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .expect("query budget should build");
        let execution = budget.begin_query().expect("bounded query should admit");
        execution
            .charge_samples_scanned(3)
            .expect("the exact scan-work limit should pass");

        match remaining_remote_select_limits(&execution)
            .expect_err("an exhausted scan budget must reject another peer request")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::SamplesScanned),
            other => panic!("unexpected error: {other}"),
        }

        drop(execution);
        assert_eq!(budget.snapshot().active_queries, 0);
    }

    #[test]
    fn series_points_returned_bytes_depend_on_content_not_capacity() {
        let histogram = NativeHistogram {
            count: Some(tsink::HistogramCount::Int(3)),
            sum: 6.0,
            schema: 1,
            zero_threshold: 0.0,
            zero_count: Some(tsink::HistogramCount::Int(0)),
            negative_spans: Vec::new(),
            negative_deltas: Vec::new(),
            negative_counts: Vec::new(),
            positive_spans: vec![tsink::HistogramBucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![1, 2],
            positive_counts: vec![1.0, 2.0],
            reset_hint: tsink::HistogramResetHint::No,
            custom_values: vec![0.5],
        };
        let compact = vec![SeriesPoints {
            series: MetricSeries {
                name: "payloads".to_string(),
                labels: vec![Label::new("host", "a")],
            },
            points: vec![
                DataPoint::new(10, Value::Bytes(vec![1, 2, 3])),
                DataPoint::new(11, Value::String("abc".to_string())),
                DataPoint::new(12, Value::from(histogram)),
            ],
        }];
        let mut roomy = compact.clone();
        roomy.reserve(32);
        roomy[0].points.reserve(32);
        if let Value::Bytes(bytes) = &mut roomy[0].points[0].value {
            bytes.reserve(128);
        }
        if let Value::String(text) = &mut roomy[0].points[1].value {
            text.reserve(128);
        }
        if let Value::Histogram(histogram) = &mut roomy[0].points[2].value {
            histogram.positive_spans.reserve(32);
            histogram.positive_deltas.reserve(32);
            histogram.positive_counts.reserve(32);
            histogram.custom_values.reserve(32);
        }
        assert_eq!(compact, roomy);

        assert_eq!(
            modeled_series_points_returned_bytes(&compact),
            modeled_series_points_returned_bytes(&roomy)
        );
        assert_ne!(
            modeled_series_points_vec_retained_bytes(&compact),
            modeled_series_points_vec_retained_bytes(&roomy)
        );
    }

    #[test]
    fn series_points_intermediate_limit_tracks_vector_high_water_not_total_samples() {
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("query budget should build");
        let vectors = vec![
            SeriesPoints {
                series: MetricSeries {
                    name: "high_water".to_string(),
                    labels: vec![Label::new("host", "a")],
                },
                points: vec![
                    DataPoint::new(1_700_000_000_000, 1.0),
                    DataPoint::new(1_700_000_001_000, 2.0),
                ],
            },
            SeriesPoints {
                series: MetricSeries {
                    name: "high_water".to_string(),
                    labels: vec![Label::new("host", "b")],
                },
                points: vec![
                    DataPoint::new(1_700_000_000_000, 3.0),
                    DataPoint::new(1_700_000_001_000, 4.0),
                ],
            },
        ];

        let exact = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_intermediate_vector_size: Some(2),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should admit");
        observe_series_points_vectors(&exact, &vectors)
            .expect("two outer entries and two points per entry should fit a limit of two");
        assert_eq!(exact.snapshot().intermediate_vector_size, 2);
        drop(exact);

        let one_over = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_intermediate_vector_size: Some(1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-over query should admit");
        match observe_series_points_vectors(&one_over, &vectors)
            .expect_err("a vector of length two must exceed a limit of one")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::IntermediateVectorSize),
            other => panic!("unexpected error: {other}"),
        }
        drop(one_over);

        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn execution_aware_points_preflight_planning_memory_exactly() {
        let executor = build_executor(ClusterReadConsistency::Eventual, ReadMergeLimits::default());
        let storage = build_test_storage();
        let series = vec![
            MetricSeries {
                name: "planning_memory".to_string(),
                labels: vec![Label::new("host", "a")],
            },
            MetricSeries {
                name: "planning_memory".to_string(),
                labels: vec![Label::new("host", "b")],
            },
        ];
        storage
            .insert_rows(
                &series
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        Row::with_labels(
                            item.name.clone(),
                            item.labels.clone(),
                            DataPoint::new(1_700_000_000_010, (index + 1) as f64),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .expect("planning-memory rows should insert");
        let rpc = rpc_client();

        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("calibration budget should build");
        let calibration = calibration_budget
            .begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("calibration query should admit");
        let response = executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                &series,
                1_700_000_000_000,
                1_700_000_000_100,
                1,
                &calibration,
            )
            .await
            .expect("calibration query should succeed");
        assert_eq!(response.value.len(), 2);
        drop(response);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibrated_peak = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(calibrated_peak > 0);

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact budget should build");
        let exact = exact_budget
            .begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("exact query should admit");
        executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                &series,
                1_700_000_000_000,
                1_700_000_000_100,
                1,
                &exact,
            )
            .await
            .expect("the calibrated exact planning-memory limit should pass");
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak - 1),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under budget should build");
        let one_under = one_under_budget
            .begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("one-under query should admit");
        match executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                &series,
                1_700_000_000_000,
                1_700_000_000_100,
                1,
                &one_under,
            )
            .await
            .expect_err("one byte below the calibrated planning-memory peak must fail")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = one_under_budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn execution_aware_metadata_preflight_planning_memory_exactly() {
        let executor = build_executor(ClusterReadConsistency::Eventual, ReadMergeLimits::default());
        let storage = build_test_storage();
        let rpc = rpc_client();
        let selection = SeriesSelection::new().with_metric("missing_planning_memory");

        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("calibration budget should build");
        let calibration = calibration_budget
            .begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("calibration query should admit");
        let response = executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                &selection,
                1,
                &calibration,
            )
            .await
            .expect("calibration query should succeed");
        assert!(response.value.series.is_empty());
        drop(response);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibrated_peak = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(calibrated_peak > 0);

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact budget should build");
        let exact = exact_budget
            .begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("exact query should admit");
        let response = executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage, &rpc, &selection, 1, &exact,
            )
            .await
            .expect("the calibrated exact planning-memory limit should pass");
        assert!(response.value.series.is_empty());
        drop(response);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak - 1),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under budget should build");
        let one_under = one_under_budget
            .begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("one-under query should admit");
        match executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage, &rpc, &selection, 1, &one_under,
            )
            .await
            .expect_err("one byte below the calibrated planning-memory peak must fail")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = one_under_budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn execution_aware_consistency_warnings_are_guarded_across_all_fanout_paths() {
        let executor = build_executor_with_topology_and_partial_policy(
            ClusterReadConsistency::Eventual,
            ClusterReadPartialResponsePolicy::Allow,
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        );
        let storage = build_test_storage();
        let rpc = rpc_client();
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(128 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(128 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        })
        .expect("consistency-warning budget should build");

        let series_execution = budget
            .begin_query()
            .expect("series consistency query should admit");
        let series_response = executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                &SeriesSelection::new().with_metric("missing_consistency_series"),
                1,
                &series_execution,
            )
            .await
            .expect("eventual series fanout should return partial metadata");
        assert!(series_response.metadata.partial_response);
        assert!(series_response
            .metadata
            .warnings
            .iter()
            .any(|warning| warning.contains("select_series")));
        assert!(series_execution.snapshot().memory_reserved_bytes > 0);
        drop(series_response);
        assert_eq!(series_execution.snapshot().memory_reserved_bytes, 0);
        drop(series_execution);

        let list_execution = budget
            .begin_query()
            .expect("list consistency query should admit");
        let list_response = executor
            .list_metrics_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                1,
                &list_execution,
            )
            .await
            .expect("eventual list fanout should return partial metadata");
        assert!(list_response.metadata.partial_response);
        assert!(list_response
            .metadata
            .warnings
            .iter()
            .any(|warning| warning.contains("list_metrics")));
        assert!(list_execution.snapshot().memory_reserved_bytes > 0);
        drop(list_response);
        assert_eq!(list_execution.snapshot().memory_reserved_bytes, 0);
        drop(list_execution);

        let points_series = find_series_with_primary_owner(&executor, "node-c");
        let points_execution = budget
            .begin_query()
            .expect("points consistency query should admit");
        let points_response = executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&points_series),
                1_700_000_000_000,
                1_700_000_000_100,
                1,
                &points_execution,
            )
            .await
            .expect("eventual points fanout should return partial metadata");
        assert!(points_response.metadata.partial_response);
        assert!(points_response
            .metadata
            .warnings
            .iter()
            .any(|warning| warning.contains("select_points")));
        assert!(points_execution.snapshot().memory_reserved_bytes > 0);
        drop(points_response);
        assert_eq!(points_execution.snapshot().memory_reserved_bytes, 0);
        drop(points_execution);

        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.queries_started_total, 3);
        assert_eq!(released.queries_completed_total, 3);
        assert_eq!(released.peak_active_queries, 1);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn partial_warning_memory_limit_is_exact_and_one_under_releases() {
        let executor = build_executor_with_topology_and_partial_policy(
            ClusterReadConsistency::Eventual,
            ClusterReadPartialResponsePolicy::Allow,
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        );
        let storage = build_test_storage();
        let rpc = rpc_client();
        let series = find_series_with_primary_owner(&executor, "node-c");

        let calibration_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(128 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(128 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        })
        .expect("partial-warning calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("partial-warning calibration should admit");
        let response = executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                0,
                1,
                1,
                &calibration,
            )
            .await
            .expect("partial-warning calibration should succeed");
        assert!(response.metadata.partial_response);
        assert!(calibration.snapshot().memory_reserved_bytes > 0);
        drop(response);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibrated_peak = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(calibrated_peak > 1);

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact partial-warning budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact partial-warning query should admit");
        let response = executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                0,
                1,
                1,
                &exact,
            )
            .await
            .expect("the exact partial-warning memory limit should pass");
        assert!(response.metadata.partial_response);
        drop(response);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak - 1),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under partial-warning budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under partial-warning query should admit");
        match executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                0,
                1,
                1,
                &one_under,
            )
            .await
            .expect_err("one byte below the partial-warning peak must fail")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = one_under_budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn consistency_error_response_memory_limit_is_exact_and_one_under_releases() {
        let executor = build_executor_with_topology_and_partial_policy(
            ClusterReadConsistency::Strict,
            ClusterReadPartialResponsePolicy::Deny,
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        );
        let storage = build_test_storage();
        let rpc = rpc_client();
        let series = find_series_with_primary_owner(&executor, "node-c");

        let calibration_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(128 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(128 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        })
        .expect("consistency-error calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("consistency-error calibration should admit");
        let error = executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                0,
                1,
                1,
                &calibration,
            )
            .await
            .expect_err("strict consistency should fail with unavailable replicas");
        assert!(matches!(error, ReadFanoutError::ConsistencyUnmet { .. }));
        assert!(calibration.snapshot().memory_reserved_bytes > 0);
        let response = crate::handlers::fanout_error_response(error);
        assert_eq!(response.status, 409);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibrated_peak = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(calibrated_peak > 1);

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact consistency-error budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact consistency-error query should admit");
        let error = executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                0,
                1,
                1,
                &exact,
            )
            .await
            .expect_err("strict consistency should still return its bounded error");
        assert!(matches!(error, ReadFanoutError::ConsistencyUnmet { .. }));
        let response = crate::handlers::fanout_error_response(error);
        assert_eq!(response.status, 409);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(calibrated_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(calibrated_peak - 1),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under consistency-error budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under consistency-error query should admit");
        match executor
            .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                0,
                1,
                1,
                &one_under,
            )
            .await
            .expect_err("one byte below the consistency-error peak must fail")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = one_under_budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn consistency_diagnostic_preflight_honors_cancellation_and_deadline() {
        let executor = build_executor_with_topology_and_partial_policy(
            ClusterReadConsistency::Eventual,
            ClusterReadPartialResponsePolicy::Allow,
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        );
        let storage = build_test_storage();
        let rpc = rpc_client();
        let selection = SeriesSelection::new().with_metric("cancelled_consistency_query");
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("control budget should build");

        let cancellation = QueryCancellationToken::new();
        let cancelled = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("cancelled query should admit before cancellation");
        cancellation.cancel();
        match executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage, &rpc, &selection, 1, &cancelled,
            )
            .await
            .expect_err("cancelled fanout should stop at its checkpoint")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::Cancelled,
            } => {}
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(cancelled.snapshot().memory_reserved_bytes, 0);
        drop(cancelled);

        let deadline = QueryCancellationToken::new().with_timeout(Duration::from_millis(1));
        let expired = budget
            .begin_query_with(QueryWorkLimits::default(), deadline)
            .expect("deadline query should admit before expiry");
        tokio::time::sleep(Duration::from_millis(2)).await;
        match executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage, &rpc, &selection, 1, &expired,
            )
            .await
            .expect_err("expired fanout should stop at its checkpoint")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::DeadlineExceeded,
            } => {}
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(expired.snapshot().memory_reserved_bytes, 0);
        drop(expired);

        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
        assert_eq!(released.cancellations_total, 1);
        assert_eq!(released.deadline_exceeded_total, 1);
    }

    #[tokio::test]
    async fn bounded_remote_metadata_forwards_residual_limits_and_dedupes_accounting() {
        let remote_series = MetricSeries {
            name: "remote_metadata_budget".to_string(),
            labels: vec![Label::new("host", "remote")],
        };
        let (node_b_endpoint, node_b_limits, node_b_server) =
            spawn_select_series_server_with_accounting(vec![remote_series.clone()], 3).await;
        let (node_c_endpoint, node_c_limits, node_c_server) =
            spawn_select_series_server_with_accounting(vec![remote_series.clone()], 4).await;
        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            reserve_unused_endpoint(),
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(32 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(32 * 1024 * 1024),
                max_series_matched: Some(1),
                max_samples_scanned: Some(7),
                max_returned_bytes: Some(1024 * 1024),
                max_pattern_expansion: Some(10_000),
                max_intermediate_vector_size: Some(100),
                ..QueryWorkLimits::default()
            },
        })
        .expect("query budget should build");
        let execution = budget
            .begin_query()
            .expect("bounded metadata query should admit");
        let response = executor
            .select_series_with_ring_version_detailed_accounted_with_execution(
                &storage,
                &rpc_client(),
                &SeriesSelection::new().with_metric("remote_metadata_budget"),
                1,
                &execution,
            )
            .await
            .expect("bounded remote metadata should succeed");
        assert_eq!(response.value.series, vec![remote_series]);
        assert_eq!(execution.snapshot().series_matched, 1);
        assert_eq!(execution.snapshot().samples_scanned, 7);
        assert!(execution.snapshot().memory_reserved_bytes > 0);

        node_b_server.await.expect("node-b server should join");
        node_c_server.await.expect("node-c server should join");
        let node_b_limits = node_b_limits
            .lock()
            .expect("node-b limits lock")
            .expect("node-b should receive bounded limits");
        let node_c_limits = node_c_limits
            .lock()
            .expect("node-c limits lock")
            .expect("node-c should receive bounded limits");
        assert_eq!(node_b_limits.max_samples_scanned, Some(7));
        assert_eq!(node_c_limits.max_samples_scanned, Some(4));

        drop(response);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    fn build_executor(
        read_consistency: ClusterReadConsistency,
        merge_limits: ReadMergeLimits,
    ) -> ReadFanoutExecutor {
        build_executor_with_guardrails(
            read_consistency,
            merge_limits,
            ReadResourceGuardrails::default(),
        )
    }

    fn build_executor_with_guardrails(
        read_consistency: ClusterReadConsistency,
        merge_limits: ReadMergeLimits,
        resource_guardrails: ReadResourceGuardrails,
    ) -> ReadFanoutExecutor {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            shards: 64,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring = ShardRing::build(cfg.shards, cfg.replication_factor, &membership)
            .expect("ring should build");
        ReadFanoutExecutor::new(
            "node-a".to_string(),
            ring,
            &membership,
            2,
            read_consistency,
            ClusterReadPartialResponsePolicy::Allow,
            merge_limits,
            resource_guardrails,
        )
        .expect("fanout executor should build")
    }

    fn build_executor_with_topology(
        read_consistency: ClusterReadConsistency,
        local_endpoint: String,
        node_b_endpoint: String,
        node_c_endpoint: String,
        merge_limits: ReadMergeLimits,
    ) -> ReadFanoutExecutor {
        build_executor_with_topology_and_guardrails(
            read_consistency,
            ClusterReadPartialResponsePolicy::Allow,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            merge_limits,
            ReadResourceGuardrails::default(),
        )
    }

    fn build_executor_with_topology_and_guardrails(
        read_consistency: ClusterReadConsistency,
        partial_response_policy: ClusterReadPartialResponsePolicy,
        local_endpoint: String,
        node_b_endpoint: String,
        node_c_endpoint: String,
        merge_limits: ReadMergeLimits,
        resource_guardrails: ReadResourceGuardrails,
    ) -> ReadFanoutExecutor {
        build_executor_with_topology_and_partial_policy(
            read_consistency,
            partial_response_policy,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            merge_limits,
            resource_guardrails,
        )
    }

    fn build_executor_with_topology_and_partial_policy(
        read_consistency: ClusterReadConsistency,
        partial_response_policy: ClusterReadPartialResponsePolicy,
        local_endpoint: String,
        node_b_endpoint: String,
        node_c_endpoint: String,
        merge_limits: ReadMergeLimits,
        resource_guardrails: ReadResourceGuardrails,
    ) -> ReadFanoutExecutor {
        let mut nodes = vec![
            ClusterNode {
                id: "node-a".to_string(),
                endpoint: local_endpoint,
            },
            ClusterNode {
                id: "node-b".to_string(),
                endpoint: node_b_endpoint,
            },
            ClusterNode {
                id: "node-c".to_string(),
                endpoint: node_c_endpoint,
            },
        ];
        nodes.sort();
        let membership = MembershipView {
            local_node_id: "node-a".to_string(),
            nodes,
        };
        let ring = ShardRing::build(64, 3, &membership).expect("ring should build");
        ReadFanoutExecutor::new(
            "node-a".to_string(),
            ring,
            &membership,
            4,
            read_consistency,
            partial_response_policy,
            merge_limits,
            resource_guardrails,
        )
        .expect("fanout executor should build")
    }

    fn build_test_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(64)
            .build()
            .expect("storage should build")
    }

    fn reserve_unused_endpoint() -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("endpoint should bind");
        let endpoint = listener
            .local_addr()
            .expect("endpoint should resolve")
            .to_string();
        drop(listener);
        endpoint
    }

    fn find_series_with_primary_owner(executor: &ReadFanoutExecutor, owner: &str) -> MetricSeries {
        for idx in 0..200_000u32 {
            let series = MetricSeries {
                name: "consistency_metric".to_string(),
                labels: vec![Label::new("candidate", idx.to_string())],
            };
            let owners = executor
                .owners_for_series(&series.name, &series.labels, ReadPlanOwnerMode::AllReplicas)
                .expect("owners should resolve");
            if owners.first().is_some_and(|primary| primary == owner) {
                return series;
            }
        }
        panic!("failed to find series with primary owner '{owner}'");
    }

    fn find_two_series_with_primary_owner(
        executor: &ReadFanoutExecutor,
        owner: &str,
    ) -> [MetricSeries; 2] {
        let first = find_series_with_primary_owner(executor, owner);
        for idx in 200_000u32..400_000u32 {
            let series = MetricSeries {
                name: "consistency_metric".to_string(),
                labels: vec![Label::new("candidate", idx.to_string())],
            };
            if series == first {
                continue;
            }
            let owners = executor
                .owners_for_series(&series.name, &series.labels, ReadPlanOwnerMode::AllReplicas)
                .expect("owners should resolve");
            if owners.first().is_some_and(|primary| primary == owner) {
                return [first, series];
            }
        }
        panic!("failed to find two series with primary owner '{owner}'");
    }

    fn rpc_client() -> RpcClient {
        RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(150),
            max_retries: 0,
            protocol_version: crate::cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: crate::cluster::rpc::CompatibilityProfile::default(),
            internal_mtls: None,
        })
    }

    async fn spawn_select_server(
        points: Vec<DataPoint>,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        spawn_select_server_with_accounting(points, None, None).await
    }

    async fn spawn_select_server_with_accounting(
        points: Vec<DataPoint>,
        scanned_samples: Option<u64>,
        matched_selectors: Option<Vec<bool>>,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener
            .local_addr()
            .expect("listener should resolve")
            .to_string();
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_task = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept()).await
            {
                let mut read_buffer = Vec::new();
                let request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("request should decode");
                requests_task.fetch_add(1, Ordering::Relaxed);
                let response = match request.path_without_query() {
                    "/internal/v1/select" => {
                        let _: InternalSelectRequest =
                            serde_json::from_slice(&request.body).expect("payload should decode");
                        HttpResponse::new(
                            200,
                            serde_json::to_vec(&InternalSelectResponse { points })
                                .expect("response should encode"),
                        )
                    }
                    "/internal/v1/select_batch" => {
                        let payload: InternalSelectBatchRequest =
                            serde_json::from_slice(&request.body).expect("payload should decode");
                        let bounded = payload.query_limits.is_some();
                        let series = payload
                            .selectors
                            .into_iter()
                            .map(|series| SeriesPoints {
                                series,
                                points: points.clone(),
                            })
                            .collect::<Vec<_>>();
                        let matched_selectors = matched_selectors
                            .clone()
                            .unwrap_or_else(|| vec![true; series.len()]);
                        assert_eq!(
                            matched_selectors.len(),
                            series.len(),
                            "test accounting flags must align with selectors"
                        );
                        let accounting = bounded.then(|| InternalSelectBatchAccounting {
                            execution: QueryExecutionSnapshot {
                                memory_reserved_bytes: 0,
                                series_matched: saturating_u64_from_usize(
                                    matched_selectors.iter().filter(|matched| **matched).count(),
                                ),
                                samples_scanned: scanned_samples.unwrap_or_else(|| {
                                    series.iter().fold(0u64, |count, item| {
                                        count.saturating_add(saturating_u64_from_usize(
                                            item.points.len(),
                                        ))
                                    })
                                }),
                                samples_returned: series.iter().fold(0u64, |count, item| {
                                    count.saturating_add(saturating_u64_from_usize(
                                        item.points.len(),
                                    ))
                                }),
                                returned_bytes: modeled_series_points_returned_bytes(&series),
                                pattern_expansion: 0,
                                steps: 0,
                                intermediate_vector_size: series.iter().fold(
                                    saturating_u64_from_usize(series.len()),
                                    |size, item| {
                                        size.max(saturating_u64_from_usize(item.points.len()))
                                    },
                                ),
                            },
                            matched_selectors: Some(matched_selectors),
                        });
                        HttpResponse::new(
                            200,
                            serde_json::to_vec(&InternalSelectBatchResponse { series, accounting })
                                .expect("response should encode"),
                        )
                    }
                    other => panic!("unexpected path: {other}"),
                }
                .with_header("Content-Type", "application/json");
                write_http_response(&mut stream, &response)
                    .await
                    .expect("response should write");
            }
        });
        (endpoint, requests, server)
    }

    async fn spawn_select_series_server_with_accounting(
        series: Vec<MetricSeries>,
        scanned_samples: u64,
    ) -> (
        String,
        Arc<Mutex<Option<QueryWorkLimits>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener
            .local_addr()
            .expect("listener should resolve")
            .to_string();
        let observed_limits = Arc::new(Mutex::new(None));
        let observed_limits_task = Arc::clone(&observed_limits);
        let server = tokio::spawn(async move {
            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept()).await
            {
                let mut read_buffer = Vec::new();
                let request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("request should decode");
                assert_eq!(request.path_without_query(), "/internal/v1/select_series");
                let payload: InternalSelectSeriesRequest =
                    serde_json::from_slice(&request.body).expect("payload should decode");
                *observed_limits_task.lock().expect("limits lock") = payload.query_limits;
                let accounting = InternalSelectSeriesAccounting {
                    execution: QueryExecutionSnapshot {
                        memory_reserved_bytes: 0,
                        series_matched: saturating_u64_from_usize(series.len()),
                        samples_scanned: scanned_samples,
                        samples_returned: 0,
                        returned_bytes: modeled_metric_series_slice_returned_bytes(&series),
                        pattern_expansion: 0,
                        steps: 0,
                        intermediate_vector_size: saturating_u64_from_usize(series.len()),
                    },
                };
                let response = HttpResponse::new(
                    200,
                    serde_json::to_vec(&InternalSelectSeriesResponse {
                        series,
                        accounting: Some(accounting),
                    })
                    .expect("response should encode"),
                )
                .with_header("Content-Type", "application/json");
                write_http_response(&mut stream, &response)
                    .await
                    .expect("response should write");
            }
        });
        (endpoint, observed_limits, server)
    }

    async fn spawn_legacy_select_only_server(
        points: Vec<DataPoint>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener
            .local_addr()
            .expect("listener should resolve")
            .to_string();
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_task = Arc::clone(&requests);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let paths_task = Arc::clone(&paths);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let Ok(Ok((mut stream, _))) =
                    tokio::time::timeout(Duration::from_secs(2), listener.accept()).await
                else {
                    break;
                };
                let mut read_buffer = Vec::new();
                let request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("request should decode");
                requests_task.fetch_add(1, Ordering::Relaxed);
                paths_task
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(request.path_without_query().to_string());

                let response = match request.path_without_query() {
                    "/internal/v1/select_batch" => HttpResponse::new(
                        404,
                        serde_json::to_vec(&json!({
                            "code": "not_found",
                            "error": "not found",
                            "retryable": false
                        }))
                        .expect("response should encode"),
                    ),
                    "/internal/v1/select" => {
                        let _: InternalSelectRequest =
                            serde_json::from_slice(&request.body).expect("payload should decode");
                        HttpResponse::new(
                            200,
                            serde_json::to_vec(&InternalSelectResponse {
                                points: points.clone(),
                            })
                            .expect("response should encode"),
                        )
                    }
                    other => panic!("unexpected path: {other}"),
                }
                .with_header("Content-Type", "application/json");
                write_http_response(&mut stream, &response)
                    .await
                    .expect("response should write");
            }
        });
        (endpoint, requests, paths, server)
    }

    #[tokio::test]
    async fn list_metrics_returns_local_series_in_single_node_mode() {
        let executor = build_executor(ClusterReadConsistency::Eventual, ReadMergeLimits::default());
        let storage = build_test_storage();
        storage
            .insert_rows(&[tsink::Row::with_labels(
                "local_metric",
                vec![Label::new("node", "a")],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("local insert should succeed");

        let rpc = RpcClient::new(crate::cluster::rpc::RpcClientConfig {
            internal_auth_token: "cluster-token".to_string(),
            ..crate::cluster::rpc::RpcClientConfig::default()
        });
        let metrics = executor
            .list_metrics_with_ring_version(&storage, &rpc, 1)
            .await
            .expect("fanout list should succeed");
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "local_metric");
    }

    #[test]
    fn rejects_zero_fanout_concurrency() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            shards: 64,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring = ShardRing::build(cfg.shards, cfg.replication_factor, &membership)
            .expect("ring should build");

        let err = ReadFanoutExecutor::new(
            "node-a".to_string(),
            ring,
            &membership,
            0,
            ClusterReadConsistency::Eventual,
            ClusterReadPartialResponsePolicy::Allow,
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        )
        .expect_err("zero concurrency should fail");
        assert!(err.contains("fanout concurrency"));
    }

    #[test]
    fn rejects_invalid_resource_guardrails() {
        let err = ReadResourceGuardrails {
            max_inflight_queries: 0,
            max_inflight_merged_points: 1,
            acquire_timeout: Duration::from_millis(1),
        }
        .validate()
        .expect_err("zero in-flight query limit should fail");
        assert!(err.contains("max in-flight queries"));

        let err = ReadResourceGuardrails {
            max_inflight_queries: 1,
            max_inflight_merged_points: 0,
            acquire_timeout: Duration::from_millis(1),
        }
        .validate()
        .expect_err("zero in-flight merged points should fail");
        assert!(err.contains("max in-flight merged points"));
    }

    #[tokio::test]
    async fn select_points_fails_when_global_merged_points_limit_is_exceeded() {
        let executor = build_executor_with_guardrails(
            ClusterReadConsistency::Eventual,
            ReadMergeLimits {
                max_series: 8,
                max_points_per_series: 100,
                max_total_points: 100,
            },
            ReadResourceGuardrails {
                max_inflight_queries: 4,
                max_inflight_merged_points: 32,
                acquire_timeout: Duration::from_millis(25),
            },
        );
        let storage = build_test_storage();
        let series = MetricSeries {
            name: "global_points_guardrail_metric".to_string(),
            labels: vec![Label::new("instance", "a")],
        };
        let rpc = rpc_client();

        let err = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_100,
                1,
            )
            .await
            .expect_err("global merged points limit should fail request");

        match err {
            ReadFanoutError::ResourceLimitExceeded {
                resource,
                requested,
                limit,
                retryable,
            } => {
                assert_eq!(resource, READ_RESOURCE_GLOBAL_MERGED_POINTS);
                assert_eq!(requested, 100);
                assert_eq!(limit, 32);
                assert!(!retryable);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn global_query_slot_timeout_returns_retryable_resource_limit_error() {
        let guardrails = ReadResourceGuardrails {
            max_inflight_queries: 1,
            max_inflight_merged_points: 1_000_000,
            acquire_timeout: Duration::from_millis(20),
        };
        let executor = build_executor_with_guardrails(
            ClusterReadConsistency::Eventual,
            ReadMergeLimits::default(),
            guardrails,
        );
        let first_lease = executor
            .acquire_read_resources(1)
            .await
            .expect("first lease should acquire the only query slot");
        let err = executor
            .acquire_read_resources(1)
            .await
            .expect_err("second lease should fail while query slot is held");

        match err {
            ReadFanoutError::ResourceLimitExceeded {
                resource,
                requested,
                limit,
                retryable,
            } => {
                assert_eq!(resource, READ_RESOURCE_GLOBAL_QUERY_SLOTS);
                assert_eq!(requested, 1);
                assert_eq!(limit, 1);
                assert!(retryable);
            }
            other => panic!("unexpected error: {other}"),
        }
        drop(first_lease);
    }

    #[tokio::test]
    async fn eventual_mode_prefers_primary_replica() {
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_111, 7.0)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-b");
        let rpc = rpc_client();

        let response = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_200,
                1,
            )
            .await
            .expect("eventual read should succeed");

        assert_eq!(response.len(), 1);
        assert_eq!(response[0].series, series);
        assert_eq!(response[0].points.len(), 1);
        assert_eq!(response[0].points[0].timestamp, 1_700_000_000_111);

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn execution_aware_remote_points_enforce_exact_and_one_over_budgets_and_release() {
        let remote_points = vec![
            DataPoint::new(1_700_000_000_111, 7.0),
            DataPoint::new(1_700_000_000_112, 8.0),
        ];
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(remote_points.clone()).await;
        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            reserve_unused_endpoint(),
            node_b_endpoint,
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-b");
        let rpc = rpc_client();
        let returned_bytes = modeled_series_points_identity_returned_bytes(&series).saturating_add(
            remote_points.iter().fold(0u64, |bytes, point| {
                bytes.saturating_add(modeled_point_returned_bytes(point))
            }),
        );
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(32 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(32 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        })
        .expect("query budget should build");
        let exact = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_series_matched: Some(1),
                    max_samples_scanned: Some(2),
                    max_samples_returned: Some(2),
                    max_returned_bytes: Some(returned_bytes),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should admit");

        let response = executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_200,
                1,
                &exact,
            )
            .await
            .expect("exact remote limits should succeed");
        assert_eq!(response.value.len(), 1);
        assert_eq!(response.value[0].points, remote_points);
        let exact_snapshot = exact.snapshot();
        assert_eq!(exact_snapshot.series_matched, 1);
        assert_eq!(exact_snapshot.samples_scanned, 2);
        assert_eq!(exact_snapshot.samples_returned, 2);
        assert_eq!(exact_snapshot.returned_bytes, returned_bytes);
        drop(response);
        let exact_snapshot = exact.snapshot();
        assert_eq!(exact_snapshot.memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        drop(exact);
        assert_eq!(budget.snapshot().active_queries, 0);

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);

        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(remote_points).await;
        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            reserve_unused_endpoint(),
            node_b_endpoint,
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
        );
        let series = find_series_with_primary_owner(&executor, "node-b");
        let one_over = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_samples_returned: Some(1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-over query should admit");
        let error = executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_200,
                1,
                &one_over,
            )
            .await
            .expect_err("one-over remote sample limit should fail");
        match error {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::SamplesReturned),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_over.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        drop(one_over);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn remote_points_charge_reported_scans_not_just_returned_samples() {
        let remote_points = vec![
            DataPoint::new(1_700_000_000_111, 7.0),
            DataPoint::new(1_700_000_000_112, 8.0),
        ];
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server_with_accounting(remote_points.clone(), Some(5), None).await;
        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            reserve_unused_endpoint(),
            node_b_endpoint,
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-b");
        let rpc = rpc_client();
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("query budget should build");
        let exact = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_samples_scanned: Some(5),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should admit");
        executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_200,
                1,
                &exact,
            )
            .await
            .expect("the exact reported scan count should fit");
        assert_eq!(exact.snapshot().samples_scanned, 5);
        assert_eq!(exact.snapshot().samples_returned, 2);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);

        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server_with_accounting(remote_points, Some(5), None).await;
        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            reserve_unused_endpoint(),
            node_b_endpoint,
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
        );
        let series = find_series_with_primary_owner(&executor, "node-b");
        let one_under = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_samples_scanned: Some(4),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-under query should admit");
        match executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_200,
                1,
                &one_under,
            )
            .await
            .expect_err("one below the remote reported scan count must fail")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::SamplesScanned),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn remote_points_use_existence_flags_for_empty_and_missing_series() {
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server_with_accounting(Vec::new(), Some(0), Some(vec![true, false])).await;
        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            reserve_unused_endpoint(),
            node_b_endpoint,
            reserve_unused_endpoint(),
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_two_series_with_primary_owner(&executor, "node-b");
        let rpc = rpc_client();
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("query budget should build");
        let execution = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_series_matched: Some(1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("query should admit");
        let response = executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                &series,
                1_700_000_000_000,
                1_700_000_000_200,
                1,
                &execution,
            )
            .await
            .expect("one empty-existing and one missing selector should fit a series limit of one");
        assert_eq!(response.value.len(), 2);
        assert!(response.value.iter().all(|item| item.points.is_empty()));
        assert_eq!(execution.snapshot().series_matched, 1);
        assert_eq!(execution.snapshot().samples_scanned, 0);
        assert_eq!(execution.snapshot().samples_returned, 0);
        drop(response);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn eventual_mode_batches_multiple_series_for_same_owner() {
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_222, 8.0)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let [first, second] = find_two_series_with_primary_owner(&executor, "node-b");
        let rpc = rpc_client();

        let response = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                &[first.clone(), second.clone()],
                1_700_000_000_000,
                1_700_000_000_300,
                1,
            )
            .await
            .expect("batched read should succeed");

        assert_eq!(response.len(), 2);
        let returned = response
            .into_iter()
            .map(|item| item.series)
            .collect::<Vec<_>>();
        assert!(returned.contains(&first));
        assert!(returned.contains(&second));

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn eventual_mode_falls_back_to_legacy_select_when_peer_lacks_batch_endpoint() {
        let (node_b_endpoint, node_b_requests, node_b_paths, node_b_server) =
            spawn_legacy_select_only_server(vec![DataPoint::new(1_700_000_000_223, 8.5)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Eventual,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-b");
        let rpc = rpc_client();

        let response = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_300,
                1,
            )
            .await
            .expect("legacy fallback read should succeed");

        assert_eq!(response.len(), 1);
        assert_eq!(response[0].series, series);
        assert_eq!(
            response[0].points,
            vec![DataPoint::new(1_700_000_000_223, 8.5)]
        );

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 2);
        assert_eq!(
            node_b_paths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            vec![
                "/internal/v1/select_batch".to_string(),
                "/internal/v1/select".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn quorum_mode_succeeds_when_primary_is_unavailable() {
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_333, 9.0)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Quorum,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-c");
        storage
            .insert_rows(&[Row::with_labels(
                series.name.clone(),
                series.labels.clone(),
                DataPoint::new(1_700_000_000_333, 9.0),
            )])
            .expect("local insert should succeed");
        let rpc = rpc_client();

        let response = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_400,
                1,
            )
            .await
            .expect("quorum read should succeed with one replica down");

        assert_eq!(response.len(), 1);
        assert!(response[0]
            .points
            .iter()
            .any(|point| point.timestamp == 1_700_000_000_333));

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn strict_mode_fails_when_any_replica_is_unavailable() {
        let (node_b_endpoint, _node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_555, 5.0)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Strict,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-c");
        storage
            .insert_rows(&[Row::with_labels(
                series.name.clone(),
                series.labels.clone(),
                DataPoint::new(1_700_000_000_555, 5.0),
            )])
            .expect("local insert should succeed");
        let rpc = rpc_client();

        let err = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_600,
                1,
            )
            .await
            .expect_err("strict mode should fail when one replica is down");

        match err {
            ReadFanoutError::ConsistencyUnmet {
                mode,
                required_acks,
                acknowledged_acks,
                total_replicas,
                ..
            } => {
                assert_eq!(mode, ClusterReadConsistency::Strict);
                assert_eq!(required_acks, 3);
                assert_eq!(acknowledged_acks, 2);
                assert_eq!(total_replicas, 3);
            }
            other => panic!("unexpected error: {other}"),
        }

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
    }

    #[tokio::test]
    async fn eventual_mode_returns_partial_metadata_when_primary_is_unavailable() {
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_600, 6.0)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology_and_partial_policy(
            ClusterReadConsistency::Eventual,
            ClusterReadPartialResponsePolicy::Allow,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-c");
        let rpc = rpc_client();
        let identity_bytes = modeled_series_points_identity_returned_bytes(&series);
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("query budget should build");
        let exact = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_series_matched: Some(1),
                    max_returned_bytes: Some(identity_bytes),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact partial query should admit");

        let response = executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_800,
                1,
                &exact,
            )
            .await
            .expect("eventual mode should allow partial result");

        assert_eq!(response.value.len(), 1);
        assert_eq!(response.value[0].series, series);
        assert!(response.value[0].points.is_empty());
        assert!(response.metadata.partial_response);
        assert_eq!(
            response.metadata.partial_response_policy,
            ClusterReadPartialResponsePolicy::Allow
        );
        assert!(response
            .metadata
            .warnings
            .iter()
            .any(|warning| warning.contains("mode=eventual")));
        assert_eq!(exact.snapshot().series_matched, 0);
        assert_eq!(exact.snapshot().returned_bytes, identity_bytes);
        drop(response);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);

        let one_under = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_returned_bytes: Some(identity_bytes - 1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-under partial query should admit");
        match executor
            .select_points_for_series_with_ring_version_detailed_with_execution(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_800,
                1,
                &one_under,
            )
            .await
            .expect_err("one-under synthetic identity budget must fail")
        {
            ReadFanoutError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => assert_eq!(exceeded.reason, QueryLimitReason::ReturnedBytes),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn eventual_mode_fails_when_partial_policy_is_deny() {
        let (node_b_endpoint, node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_600, 6.0)]).await;
        let node_c_endpoint = reserve_unused_endpoint();
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology_and_partial_policy(
            ClusterReadConsistency::Eventual,
            ClusterReadPartialResponsePolicy::Deny,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
            ReadResourceGuardrails::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-c");
        let rpc = rpc_client();

        let err = executor
            .select_points_for_series_with_ring_version_detailed(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_800,
                1,
            )
            .await
            .expect_err("deny policy should reject partial response");

        match err {
            ReadFanoutError::ConsistencyUnmet {
                mode,
                required_acks,
                acknowledged_acks,
                total_replicas,
                ..
            } => {
                assert_eq!(mode, ClusterReadConsistency::Eventual);
                assert_eq!(required_acks, 1);
                assert_eq!(acknowledged_acks, 0);
                assert_eq!(total_replicas, 1);
            }
            other => panic!("unexpected error: {other}"),
        }

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        assert_eq!(node_b_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn quorum_mode_reconciles_lagging_replica_points() {
        let (node_b_endpoint, _node_b_requests, node_b_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_700, 11.0)]).await;
        let (node_c_endpoint, _node_c_requests, node_c_server) =
            spawn_select_server(vec![DataPoint::new(1_700_000_000_600, 10.0)]).await;
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Quorum,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-b");
        storage
            .insert_rows(&[Row::with_labels(
                series.name.clone(),
                series.labels.clone(),
                DataPoint::new(1_700_000_000_700, 11.0),
            )])
            .expect("local insert should succeed");
        let rpc = rpc_client();

        let response = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_500,
                1_700_000_000_800,
                1,
            )
            .await
            .expect("quorum read should succeed");

        assert_eq!(response.len(), 1);
        let timestamps = response[0]
            .points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1_700_000_000_600, 1_700_000_000_700]);

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        tokio::time::timeout(Duration::from_secs(3), node_c_server)
            .await
            .expect("node-c server should complete")
            .expect("node-c server should not panic");
    }

    #[tokio::test]
    async fn quorum_mode_dedupes_duplicate_replica_points_deterministically() {
        let (node_b_endpoint, _node_b_requests, node_b_server) = spawn_select_server(vec![
            DataPoint::new(1_700_000_000_700, 11.0),
            DataPoint::new(1_700_000_000_700, 11.0),
            DataPoint::new(1_700_000_000_701, 12.0),
        ])
        .await;
        let (node_c_endpoint, _node_c_requests, node_c_server) = spawn_select_server(vec![
            DataPoint::new(1_700_000_000_700, 11.0),
            DataPoint::new(1_700_000_000_702, 13.0),
        ])
        .await;
        let local_endpoint = reserve_unused_endpoint();

        let executor = build_executor_with_topology(
            ClusterReadConsistency::Quorum,
            local_endpoint,
            node_b_endpoint,
            node_c_endpoint,
            ReadMergeLimits::default(),
        );
        let storage = build_test_storage();
        let series = find_series_with_primary_owner(&executor, "node-b");
        storage
            .insert_rows(&[Row::with_labels(
                series.name.clone(),
                series.labels.clone(),
                DataPoint::new(1_700_000_000_700, 11.0),
            )])
            .expect("local insert should succeed");
        let rpc = rpc_client();

        let response = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_600,
                1_700_000_000_800,
                1,
            )
            .await
            .expect("quorum read should succeed");

        assert_eq!(response.len(), 1);
        let timestamps = response[0]
            .points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(
            timestamps,
            vec![1_700_000_000_700, 1_700_000_000_701, 1_700_000_000_702]
        );

        tokio::time::timeout(Duration::from_secs(3), node_b_server)
            .await
            .expect("node-b server should complete")
            .expect("node-b server should not panic");
        tokio::time::timeout(Duration::from_secs(3), node_c_server)
            .await
            .expect("node-c server should complete")
            .expect("node-c server should not panic");
    }

    #[tokio::test]
    async fn list_metrics_fails_when_merge_series_limit_is_exceeded() {
        let executor = build_executor(
            ClusterReadConsistency::Eventual,
            ReadMergeLimits {
                max_series: 1,
                max_points_per_series: 100,
                max_total_points: 100,
            },
        );
        let storage = build_test_storage();
        storage
            .insert_rows(&[
                Row::new("metric_a", DataPoint::new(1_700_000_000_000, 1.0)),
                Row::new("metric_b", DataPoint::new(1_700_000_000_001, 2.0)),
            ])
            .expect("insert should succeed");
        let rpc = rpc_client();

        let err = executor
            .list_metrics_with_ring_version(&storage, &rpc, 1)
            .await
            .expect_err("series limit should be enforced");
        match err {
            ReadFanoutError::MergeLimitExceeded { message } => {
                assert!(message.contains("series limit exceeded"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn select_points_fails_when_merge_point_limit_is_exceeded() {
        let executor = build_executor(
            ClusterReadConsistency::Eventual,
            ReadMergeLimits {
                max_series: 4,
                max_points_per_series: 2,
                max_total_points: 100,
            },
        );
        let storage = build_test_storage();
        let series = MetricSeries {
            name: "limited_metric".to_string(),
            labels: vec![Label::new("instance", "a")],
        };
        storage
            .insert_rows(&[
                Row::with_labels(
                    series.name.clone(),
                    series.labels.clone(),
                    DataPoint::new(1_700_000_000_000, 1.0),
                ),
                Row::with_labels(
                    series.name.clone(),
                    series.labels.clone(),
                    DataPoint::new(1_700_000_000_001, 2.0),
                ),
                Row::with_labels(
                    series.name.clone(),
                    series.labels.clone(),
                    DataPoint::new(1_700_000_000_002, 3.0),
                ),
            ])
            .expect("insert should succeed");
        let rpc = rpc_client();

        let err = executor
            .select_points_for_series_with_ring_version(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_003,
                1,
            )
            .await
            .expect_err("point limit should be enforced");
        match err {
            ReadFanoutError::MergeLimitExceeded { message } => {
                assert!(message.contains("point limit exceeded"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
