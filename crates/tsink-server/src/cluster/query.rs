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
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tsink::{Label, MetadataShardScope, MetricSeries, SeriesSelection, Storage};

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
        source: RpcError,
    },
    RemoteListMetrics {
        node_id: String,
        endpoint: String,
        source: RpcError,
    },
    RemoteSelectBatch {
        node_id: String,
        endpoint: String,
        series_count: usize,
        source: RpcError,
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
    },
    TaskJoin {
        message: String,
    },
}

impl ReadFanoutError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::InvalidRequest { .. } => false,
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

impl std::error::Error for ReadFanoutError {}

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
        let ring_version = ring_version.max(1);
        let started = Instant::now();
        track_fanout_operation_start(FANOUT_OPERATION_SELECT_SERIES);
        let result = async {
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

            let remote_series = self
                .remote_select_series_collect(
                    rpc_client,
                    selection,
                    ring_version,
                    &plan.remote_targets,
                )
                .await?;

            let mut merged = SeriesMetadataMerger::new(self.merge_limits);
            if !plan.local_shards.is_empty() {
                if let Ok(local_series) = self
                    .local_select_series(storage, selection, &plan.local_shards)
                    .await
                {
                    for shard in &plan.local_shards {
                        let entry = acknowledged_acks.entry(*shard).or_insert(0);
                        *entry = entry.saturating_add(1);
                    }
                    merged
                        .extend(local_series)
                        .map_err(read_merge_error_to_fanout_error)?;
                }
            }

            for target in remote_series {
                if let Ok(node_series) = target.series {
                    for shard in &target.shards {
                        let entry = acknowledged_acks.entry(*shard).or_insert(0);
                        *entry = entry.saturating_add(1);
                    }
                    merged
                        .extend(node_series)
                        .map_err(read_merge_error_to_fanout_error)?;
                }
            }

            let consistency_gaps = self.shard_consistency_gaps(
                FANOUT_OPERATION_SELECT_SERIES,
                &requirements,
                &acknowledged_acks,
            );
            let metadata =
                self.evaluate_consistency_gaps(FANOUT_OPERATION_SELECT_SERIES, consistency_gaps)?;
            Ok::<_, ReadFanoutError>(ReadFanoutResponse {
                value: merged.into_series(),
                metadata,
            })
        }
        .await;
        track_fanout_operation_complete(
            FANOUT_OPERATION_SELECT_SERIES,
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
        let ring_version = ring_version.max(1);
        let started = Instant::now();
        track_fanout_operation_start(FANOUT_OPERATION_SELECT_POINTS);
        let result = async {
            let _resource_lease = self
                .acquire_read_resources(self.points_budget_estimate(series.len()))
                .await?;
            let plan = self.plan_select_points(series, start, end, ring_version)?;
            let mut unique_series = BTreeMap::<SeriesIdentity, MetricSeries>::new();
            for item in series {
                unique_series
                    .entry(SeriesIdentity::from_series(item))
                    .or_insert_with(|| item.clone());
            }
            if unique_series.len() > self.merge_limits.max_series {
                return Err(read_merge_error_to_fanout_error(MergeLimitError::Series {
                    limit: self.merge_limits.max_series,
                    attempted: unique_series.len(),
                }));
            }

            let mut by_owner: BTreeMap<String, Vec<MetricSeries>> = BTreeMap::new();
            let mut requirements = BTreeMap::<SeriesIdentity, ReadRequirement>::new();
            for (identity, item) in &unique_series {
                let owners = self.owners_for_series(
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
                    by_owner.entry(owner).or_default().push(item.clone());
                }
            }

            let mut acknowledged_acks = requirements
                .keys()
                .map(|identity| (identity.clone(), 0usize))
                .collect::<BTreeMap<_, _>>();
            let mut collected_points = SeriesPointsMerger::new(self.merge_limits);

            if let Some(local_series) = by_owner.remove(&self.local_node_id) {
                if let Ok(local_points) = self
                    .local_select_points(storage, local_series, start, end)
                    .await
                {
                    for item in local_points {
                        let identity = SeriesIdentity::from_series(&item.series);
                        let entry = acknowledged_acks.entry(identity.clone()).or_insert(0);
                        *entry = entry.saturating_add(1);
                        collected_points
                            .merge_series_points(&item.series, item.points)
                            .map_err(read_merge_error_to_fanout_error)?;
                    }
                }
            }

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

            let consistency_gaps = self.series_consistency_gaps(&requirements, &acknowledged_acks);
            let metadata =
                self.evaluate_consistency_gaps(FANOUT_OPERATION_SELECT_POINTS, consistency_gaps)?;

            let mut merged_points = collected_points.into_points();
            let mut merged = Vec::with_capacity(unique_series.len());
            for (identity, series) in unique_series {
                let points = merged_points.remove(&identity).unwrap_or_default();
                merged.push(SeriesPoints { series, points });
            }
            Ok::<_, ReadFanoutError>(ReadFanoutResponse {
                value: merged,
                metadata,
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

    fn owners_for_series(
        &self,
        metric: &str,
        labels: &[Label],
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<Vec<String>, ReadFanoutError> {
        let series_hash = stable_series_identity_hash(metric, labels);
        let shard = self.ring.shard_for_series_id(series_hash);
        let Some(owners) = self.ring.owners_for_shard(shard) else {
            return Err(ReadFanoutError::MissingShardOwners { shard });
        };
        if owners.is_empty() {
            return Err(ReadFanoutError::MissingShardOwners { shard });
        }
        Ok(match owner_mode {
            ReadPlanOwnerMode::AllReplicas => owners.to_vec(),
            ReadPlanOwnerMode::PrimaryOnly => vec![owners[0].clone()],
        })
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
        consistency_gaps.sort_by(|left, right| left.target.cmp(&right.target));

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
    ) -> Result<Vec<MetricSeries>, ReadFanoutError> {
        let storage = Arc::clone(storage);
        let selection = selection.clone();
        let shard_scope = MetadataShardScope::new(self.ring.shard_count(), shards.to_vec());
        let result = tokio::task::spawn_blocking(move || {
            storage.select_series_in_shards(&selection, &shard_scope)
        })
        .await;
        match result {
            Ok(Ok(series)) => Ok(series),
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
    ) -> Result<Vec<SeriesPoints>, ReadFanoutError> {
        let storage = Arc::clone(storage);
        let series_count = series.len();
        let result =
            tokio::task::spawn_blocking(move || storage.select_many(&series, start, end)).await;
        match result {
            Ok(Ok(points)) => Ok(points),
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
                            source,
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
                        .map(|InternalListMetricsResponse { series }| series)
                        .map_err(|source| ReadFanoutError::RemoteListMetrics {
                            node_id,
                            endpoint,
                            source,
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
                    )
                    .await
                    {
                        Ok(mut batch_points) => out.append(&mut batch_points),
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

async fn remote_select_points_batch_with_legacy_fallback(
    rpc_client: &RpcClient,
    node_id: &str,
    endpoint: &str,
    series: Vec<MetricSeries>,
    start: i64,
    end: i64,
    ring_version: u64,
) -> Result<Vec<SeriesPoints>, ReadFanoutError> {
    let request_started = Instant::now();
    let response = rpc_client
        .select_batch(
            endpoint,
            &crate::cluster::rpc::InternalSelectBatchRequest {
                ring_version,
                selectors: series.clone(),
                start,
                end,
            },
        )
        .await;
    let request_duration_nanos = saturating_elapsed_nanos(request_started.elapsed());
    track_remote_request(
        node_id,
        FANOUT_REMOTE_OPERATION_SELECT_BATCH,
        request_duration_nanos,
        response.is_err(),
    );

    match response {
        Ok(response) => return Ok(response.series),
        Err(RpcError::HttpStatus { status: 404, .. }) => {}
        Err(source) => {
            return Err(ReadFanoutError::RemoteSelectBatch {
                node_id: node_id.to_string(),
                endpoint: endpoint.to_string(),
                series_count: series.len(),
                source,
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
                    source,
                });
            }
        }
    }

    Ok(out)
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
        InternalSelectBatchRequest, InternalSelectBatchResponse, InternalSelectRequest,
        InternalSelectResponse, RpcClientConfig,
    };
    use crate::http::{read_http_request, write_http_response, HttpResponse};
    use serde_json::json;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tsink::{DataPoint, Row, StorageBuilder, TimestampPrecision};

    #[test]
    fn strict_mode_requires_full_consistency() {
        assert!(ReadPolicy::new(ClusterReadConsistency::Strict).requires_full_consistency());
        assert!(!ReadPolicy::new(ClusterReadConsistency::Eventual).requires_full_consistency());
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
                        let series = payload
                            .selectors
                            .into_iter()
                            .map(|series| SeriesPoints {
                                series,
                                points: points.clone(),
                            })
                            .collect::<Vec<_>>();
                        HttpResponse::new(
                            200,
                            serde_json::to_vec(&InternalSelectBatchResponse { series })
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

        let response = executor
            .select_points_for_series_with_ring_version_detailed(
                &storage,
                &rpc,
                std::slice::from_ref(&series),
                1_700_000_000_000,
                1_700_000_000_800,
                1,
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
