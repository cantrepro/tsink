use crate::cluster::config::ClusterWriteConsistency;
use crate::cluster::membership::MembershipView;
use crate::cluster::outbox::{HintedHandoffOutbox, OutboxEnqueueError};
use crate::cluster::ring::ShardRing;
use crate::cluster::rpc::{
    required_capabilities_for_rows, InternalIngestRowsRequest, InternalRow, RpcClient, RpcError,
    DEFAULT_INTERNAL_RING_VERSION,
};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::task::JoinSet;
use tsink::{Label, Row, Storage, Value};

pub const WRITE_CONSISTENCY_OVERRIDE_HEADER: &str = "x-tsink-write-consistency";
pub const CLUSTER_WRITE_MAX_BATCH_ROWS_ENV: &str = "TSINK_CLUSTER_WRITE_MAX_BATCH_ROWS";
pub const CLUSTER_WRITE_MAX_INFLIGHT_BATCHES_ENV: &str = "TSINK_CLUSTER_WRITE_MAX_INFLIGHT_BATCHES";
const DEFAULT_WRITE_MAX_BATCH_ROWS: usize = 1_024;
const DEFAULT_WRITE_MAX_INFLIGHT_BATCHES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteAckPolicy {
    mode: ClusterWriteConsistency,
}

impl WriteAckPolicy {
    pub fn new(mode: ClusterWriteConsistency) -> Self {
        Self { mode }
    }

    pub fn required_acks(self, replication_factor: u16) -> u16 {
        self.mode.required_acks(replication_factor)
    }

    pub fn mode(self) -> ClusterWriteConsistency {
        self.mode
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteConsistencyOutcome {
    pub mode: ClusterWriteConsistency,
    pub required_acks: u16,
    pub acknowledged_replicas_min: u16,
}

#[derive(Debug, Clone)]
pub struct RoutedWriteBatch {
    pub owner_node_id: String,
    pub endpoint: String,
    pub idempotency_key: String,
    pub rows: Vec<Row>,
    pub shard_row_counts: BTreeMap<u32, u64>,
}

#[derive(Debug, Clone, Default)]
pub struct WritePlan {
    pub local_rows: Vec<Row>,
    pub local_shard_row_counts: BTreeMap<u32, u64>,
    pub remote_batches: Vec<RoutedWriteBatch>,
    pub shard_row_counts: BTreeMap<u32, u64>,
    pub shard_replica_owners: BTreeMap<u32, Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutedWriteStats {
    pub inserted_rows: usize,
    pub local_rows: usize,
    pub remote_rows: usize,
    pub remote_batches: usize,
    pub consistency: Option<WriteConsistencyOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRoutingMetricsSnapshot {
    pub requests_total: u64,
    pub local_rows_total: u64,
    pub routed_rows_total: u64,
    pub routed_batches_total: u64,
    pub failures_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRoutingShardMetricsSnapshot {
    pub shard: u32,
    pub rows_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRoutingPeerMetricsSnapshot {
    pub node_id: String,
    pub routed_rows_total: u64,
    pub routed_batches_total: u64,
    pub remote_requests_total: u64,
    pub remote_failures_total: u64,
    pub remote_request_duration_nanos_total: u64,
    pub remote_request_duration_count: u64,
    pub remote_request_duration_buckets: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRoutingLabeledMetricsSnapshot {
    pub shards: Vec<WriteRoutingShardMetricsSnapshot>,
    pub peers: Vec<WriteRoutingPeerMetricsSnapshot>,
}

const WRITE_REMOTE_REQUEST_LATENCY_BUCKETS_NANOS: [u64; 8] = [
    1_000_000,     // 1ms
    5_000_000,     // 5ms
    10_000_000,    // 10ms
    25_000_000,    // 25ms
    50_000_000,    // 50ms
    100_000_000,   // 100ms
    250_000_000,   // 250ms
    1_000_000_000, // 1s
];

pub const WRITE_REMOTE_REQUEST_LATENCY_BUCKETS_SECONDS: [&str; 8] = [
    "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "1",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRouterTuning {
    pub max_remote_batch_rows: usize,
    pub max_inflight_remote_batches: usize,
}

impl Default for WriteRouterTuning {
    fn default() -> Self {
        Self {
            max_remote_batch_rows: DEFAULT_WRITE_MAX_BATCH_ROWS,
            max_inflight_remote_batches: DEFAULT_WRITE_MAX_INFLIGHT_BATCHES,
        }
    }
}

impl WriteRouterTuning {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            max_remote_batch_rows: parse_env_usize(
                CLUSTER_WRITE_MAX_BATCH_ROWS_ENV,
                defaults.max_remote_batch_rows,
                true,
            )?,
            max_inflight_remote_batches: parse_env_usize(
                CLUSTER_WRITE_MAX_INFLIGHT_BATCHES_ENV,
                defaults.max_inflight_remote_batches,
                true,
            )?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WriteRouter {
    local_node_id: String,
    ring: ShardRing,
    endpoints_by_node: BTreeMap<String, String>,
    default_write_consistency: ClusterWriteConsistency,
    tuning: WriteRouterTuning,
    outbox: Option<Arc<HintedHandoffOutbox>>,
}

#[derive(Debug, Clone)]
pub enum WriteRoutingError {
    InvalidConsistencyOverride {
        requested: ClusterWriteConsistency,
        configured: ClusterWriteConsistency,
    },
    MissingShardOwners {
        shard: u32,
    },
    MissingOwnerEndpoint {
        node_id: String,
    },
    RemoteIngest {
        node_id: String,
        endpoint: String,
        source: RpcError,
    },
    RemoteInsertedRowsMismatch {
        node_id: String,
        endpoint: String,
        expected: usize,
        actual: usize,
    },
    OutboxEnqueue {
        node_id: String,
        source: OutboxEnqueueError,
    },
    InsufficientReplicas {
        shard: u32,
        mode: ClusterWriteConsistency,
        required_acks: u16,
        acknowledged_acks: u16,
        max_possible_acks: u16,
    },
    ConsistencyTimeout {
        shard: u32,
        mode: ClusterWriteConsistency,
        required_acks: u16,
        acknowledged_acks: u16,
        max_possible_acks: u16,
    },
    TaskJoin {
        message: String,
    },
}

impl WriteRoutingError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::InvalidConsistencyOverride { .. }
            | Self::MissingShardOwners { .. }
            | Self::MissingOwnerEndpoint { .. }
            | Self::RemoteInsertedRowsMismatch { .. }
            | Self::InsufficientReplicas { .. } => false,
            Self::ConsistencyTimeout { .. } | Self::TaskJoin { .. } => true,
            Self::OutboxEnqueue { source, .. } => source.retryable(),
            Self::RemoteIngest { source, .. } => source.retryable(),
        }
    }
}

impl fmt::Display for WriteRoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConsistencyOverride {
                requested,
                configured,
            } => {
                write!(
                    f,
                    "write consistency override '{requested}' is stronger than configured cluster mode '{configured}'"
                )
            }
            Self::MissingShardOwners { shard } => {
                write!(f, "write routing failed: shard {shard} has no owner")
            }
            Self::MissingOwnerEndpoint { node_id } => {
                write!(
                    f,
                    "write routing failed: owner node '{node_id}' has no known endpoint"
                )
            }
            Self::RemoteIngest {
                node_id,
                endpoint,
                source,
            } => {
                write!(
                    f,
                    "remote write to node '{node_id}' ({endpoint}) failed: {source}"
                )
            }
            Self::RemoteInsertedRowsMismatch {
                node_id,
                endpoint,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "remote write to node '{node_id}' ({endpoint}) inserted {actual} rows but expected {expected}"
                )
            }
            Self::OutboxEnqueue { node_id, source } => {
                write!(
                    f,
                    "failed to enqueue hinted handoff for node '{node_id}': {source}"
                )
            }
            Self::InsufficientReplicas {
                shard,
                mode,
                required_acks,
                acknowledged_acks,
                max_possible_acks,
            } => {
                write!(
                    f,
                    "insufficient replicas for shard {shard} in {mode} mode: required {required_acks} acks, got {acknowledged_acks}, max possible {max_possible_acks}"
                )
            }
            Self::ConsistencyTimeout {
                shard,
                mode,
                required_acks,
                acknowledged_acks,
                max_possible_acks,
            } => {
                write!(
                    f,
                    "write consistency timeout for shard {shard} in {mode} mode: required {required_acks} acks, got {acknowledged_acks}, max possible {max_possible_acks}"
                )
            }
            Self::TaskJoin { message } => {
                write!(f, "write coordinator task join failed: {message}")
            }
        }
    }
}

impl std::error::Error for WriteRoutingError {}

static CLUSTER_WRITE_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_WRITE_LOCAL_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_WRITE_ROUTED_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_WRITE_ROUTED_BATCHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_WRITE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_WRITE_LABELED_METRICS: OnceLock<Mutex<WriteRoutingLabeledMetrics>> = OnceLock::new();

#[derive(Debug, Clone, Default)]
struct LatencyHistogram {
    bucket_counts: [u64; WRITE_REMOTE_REQUEST_LATENCY_BUCKETS_NANOS.len()],
    count: u64,
    sum_nanos: u64,
}

impl LatencyHistogram {
    fn record(&mut self, duration_nanos: u64) {
        self.count = self.count.saturating_add(1);
        self.sum_nanos = self.sum_nanos.saturating_add(duration_nanos);
        for (idx, upper_bound) in WRITE_REMOTE_REQUEST_LATENCY_BUCKETS_NANOS
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
struct PerPeerRoutingMetrics {
    routed_rows_total: u64,
    routed_batches_total: u64,
    remote_requests_total: u64,
    remote_failures_total: u64,
    remote_request_latency: LatencyHistogram,
}

#[derive(Debug, Clone, Default)]
struct WriteRoutingLabeledMetrics {
    shard_rows_total: BTreeMap<u32, u64>,
    peers: BTreeMap<String, PerPeerRoutingMetrics>,
}

#[derive(Debug, Clone, Default)]
struct PendingRemoteBatch {
    rows: Vec<Row>,
    shard_row_counts: BTreeMap<u32, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CoordinatorShardState {
    required_acks: u16,
    acknowledged_acks: u16,
    pending_replicas: u16,
    timeout_failures: u16,
}

impl CoordinatorShardState {
    fn from_replica_count(replica_count: u16, required_acks: u16) -> Self {
        Self {
            required_acks,
            acknowledged_acks: 0,
            pending_replicas: replica_count,
            timeout_failures: 0,
        }
    }

    fn record_success(&mut self) {
        self.pending_replicas = self.pending_replicas.saturating_sub(1);
        self.acknowledged_acks = self.acknowledged_acks.saturating_add(1);
    }

    fn record_failure(&mut self, timeout: bool) {
        self.pending_replicas = self.pending_replicas.saturating_sub(1);
        if timeout {
            self.timeout_failures = self.timeout_failures.saturating_add(1);
        }
    }

    fn is_satisfied(&self) -> bool {
        self.acknowledged_acks >= self.required_acks
    }

    fn max_possible_acks(&self) -> u16 {
        self.acknowledged_acks.saturating_add(self.pending_replicas)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsistencyFailureKind {
    InsufficientReplicas,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsistencyFailure {
    shard: u32,
    required_acks: u16,
    acknowledged_acks: u16,
    max_possible_acks: u16,
    kind: ConsistencyFailureKind,
}

impl ConsistencyFailure {
    fn into_error(self, mode: ClusterWriteConsistency) -> WriteRoutingError {
        match self.kind {
            ConsistencyFailureKind::Timeout => WriteRoutingError::ConsistencyTimeout {
                shard: self.shard,
                mode,
                required_acks: self.required_acks,
                acknowledged_acks: self.acknowledged_acks,
                max_possible_acks: self.max_possible_acks,
            },
            ConsistencyFailureKind::InsufficientReplicas => {
                WriteRoutingError::InsufficientReplicas {
                    shard: self.shard,
                    mode,
                    required_acks: self.required_acks,
                    acknowledged_acks: self.acknowledged_acks,
                    max_possible_acks: self.max_possible_acks,
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct WriteCoordinatorState {
    mode: ClusterWriteConsistency,
    shards: BTreeMap<u32, CoordinatorShardState>,
}

impl WriteCoordinatorState {
    fn new(
        mode: ClusterWriteConsistency,
        policy: WriteAckPolicy,
        shard_replica_owners: &BTreeMap<u32, Vec<String>>,
    ) -> Self {
        let mut shards = BTreeMap::new();
        for (shard, owners) in shard_replica_owners {
            let replica_count = u16::try_from(owners.len()).unwrap_or(u16::MAX);
            let required_acks = policy.required_acks(replica_count);
            shards.insert(
                *shard,
                CoordinatorShardState::from_replica_count(replica_count, required_acks),
            );
        }
        Self { mode, shards }
    }

    fn record_success_for_shards<'a>(&mut self, shards: impl Iterator<Item = &'a u32>) {
        for shard in shards {
            if let Some(state) = self.shards.get_mut(shard) {
                state.record_success();
            }
        }
    }

    fn record_failure_for_shards<'a>(
        &mut self,
        shards: impl Iterator<Item = &'a u32>,
        timeout: bool,
    ) {
        for shard in shards {
            if let Some(state) = self.shards.get_mut(shard) {
                state.record_failure(timeout);
            }
        }
    }

    fn first_impossible_failure(&self) -> Option<ConsistencyFailure> {
        for (shard, state) in &self.shards {
            if state.is_satisfied() {
                continue;
            }
            if state.max_possible_acks() < state.required_acks {
                let kind = if state.timeout_failures > 0 {
                    ConsistencyFailureKind::Timeout
                } else {
                    ConsistencyFailureKind::InsufficientReplicas
                };
                return Some(ConsistencyFailure {
                    shard: *shard,
                    required_acks: state.required_acks,
                    acknowledged_acks: state.acknowledged_acks,
                    max_possible_acks: state.max_possible_acks(),
                    kind,
                });
            }
        }
        None
    }

    fn first_unsatisfied_failure(&self) -> Option<ConsistencyFailure> {
        for (shard, state) in &self.shards {
            if state.is_satisfied() {
                continue;
            }
            let kind = if state.timeout_failures > 0 {
                ConsistencyFailureKind::Timeout
            } else {
                ConsistencyFailureKind::InsufficientReplicas
            };
            return Some(ConsistencyFailure {
                shard: *shard,
                required_acks: state.required_acks,
                acknowledged_acks: state.acknowledged_acks,
                max_possible_acks: state.max_possible_acks(),
                kind,
            });
        }
        None
    }

    fn consistency_outcome(&self) -> Option<WriteConsistencyOutcome> {
        if self.shards.is_empty() {
            return None;
        }
        let required_acks = self
            .shards
            .values()
            .map(|state| state.required_acks)
            .max()
            .unwrap_or(0);
        let acknowledged_replicas_min = self
            .shards
            .values()
            .map(|state| state.acknowledged_acks)
            .min()
            .unwrap_or(0);
        Some(WriteConsistencyOutcome {
            mode: self.mode,
            required_acks,
            acknowledged_replicas_min,
        })
    }
}

#[derive(Debug, Clone)]
struct RemoteBatchResult {
    batch: RoutedWriteBatch,
    result: Result<(), WriteRoutingError>,
}

async fn execute_remote_batch_with_ring_version(
    rpc_client: RpcClient,
    batch: RoutedWriteBatch,
    ring_version: u64,
) -> RemoteBatchResult {
    let expected_rows = batch.rows.len();
    let request = InternalIngestRowsRequest {
        ring_version: ring_version.max(1),
        idempotency_key: Some(batch.idempotency_key.clone()),
        required_capabilities: required_capabilities_for_rows(&batch.rows),
        rows: batch.rows.iter().map(InternalRow::from).collect(),
    };
    let request_start = Instant::now();
    let response = rpc_client.ingest_rows(&batch.endpoint, &request).await;
    let request_duration_nanos = saturating_elapsed_nanos(request_start.elapsed());

    let result = match response {
        Ok(response) => {
            record_write_remote_request_metrics(
                &batch.owner_node_id,
                request_duration_nanos,
                false,
            );
            if response.inserted_rows != expected_rows {
                record_write_remote_failure(&batch.owner_node_id);
                Err(WriteRoutingError::RemoteInsertedRowsMismatch {
                    node_id: batch.owner_node_id.clone(),
                    endpoint: batch.endpoint.clone(),
                    expected: expected_rows,
                    actual: response.inserted_rows,
                })
            } else {
                Ok(())
            }
        }
        Err(err) => {
            record_write_remote_request_metrics(&batch.owner_node_id, request_duration_nanos, true);
            Err(WriteRoutingError::RemoteIngest {
                node_id: batch.owner_node_id.clone(),
                endpoint: batch.endpoint.clone(),
                source: err,
            })
        }
    };

    RemoteBatchResult { batch, result }
}

#[allow(clippy::result_large_err)]
impl WriteRouter {
    pub fn new(
        local_node_id: String,
        ring: ShardRing,
        membership: &MembershipView,
        default_write_consistency: ClusterWriteConsistency,
    ) -> Result<Self, String> {
        let endpoints_by_node = membership
            .nodes
            .iter()
            .map(|node| (node.id.clone(), node.endpoint.clone()))
            .collect::<BTreeMap<_, _>>();
        if !endpoints_by_node.contains_key(&local_node_id) {
            return Err(format!(
                "local node '{local_node_id}' is missing from cluster membership view"
            ));
        }

        Ok(Self {
            local_node_id,
            ring,
            endpoints_by_node,
            default_write_consistency,
            tuning: WriteRouterTuning::default(),
            outbox: None,
        })
    }

    pub fn with_tuning(mut self, tuning: WriteRouterTuning) -> Self {
        self.tuning = tuning;
        self
    }

    pub fn with_outbox(mut self, outbox: Arc<HintedHandoffOutbox>) -> Self {
        self.outbox = Some(outbox);
        self
    }

    #[allow(dead_code)]
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn reconfigured_for_topology(
        &self,
        ring: ShardRing,
        membership: &MembershipView,
    ) -> Result<Self, String> {
        let mut router = Self::new(
            self.local_node_id.clone(),
            ring,
            membership,
            self.default_write_consistency,
        )?;
        router.tuning = self.tuning;
        router.outbox = self.outbox.clone();
        Ok(router)
    }

    pub fn parse_write_consistency_override(
        raw_value: &str,
    ) -> Result<ClusterWriteConsistency, String> {
        ClusterWriteConsistency::from_str(raw_value)
    }

    pub fn with_default_write_consistency(
        mut self,
        default_write_consistency: ClusterWriteConsistency,
    ) -> Self {
        self.default_write_consistency = default_write_consistency;
        self
    }

    pub fn resolve_write_policy(
        &self,
        requested_override: Option<ClusterWriteConsistency>,
    ) -> Result<WriteAckPolicy, WriteRoutingError> {
        let requested = requested_override.unwrap_or(self.default_write_consistency);
        if write_consistency_strength(requested)
            > write_consistency_strength(self.default_write_consistency)
        {
            return Err(WriteRoutingError::InvalidConsistencyOverride {
                requested,
                configured: self.default_write_consistency,
            });
        }
        Ok(WriteAckPolicy::new(requested))
    }

    pub fn plan_rows(&self, rows: Vec<Row>) -> Result<WritePlan, WriteRoutingError> {
        let mut plan = WritePlan::default();
        if rows.is_empty() {
            return Ok(plan);
        }

        let mut remote_rows_by_owner: BTreeMap<String, PendingRemoteBatch> = BTreeMap::new();
        for row in rows {
            let series_hash = stable_series_identity_hash(row.metric(), row.labels());
            let shard = self.ring.shard_for_series_id(series_hash);
            *plan.shard_row_counts.entry(shard).or_insert(0) += 1;
            let owners = self.replica_owners_for_shard(shard)?.to_vec();
            plan.shard_replica_owners
                .entry(shard)
                .or_insert_with(|| owners.clone());

            for owner in owners {
                if owner == self.local_node_id {
                    plan.local_rows.push(row.clone());
                    *plan.local_shard_row_counts.entry(shard).or_insert(0) += 1;
                } else {
                    let entry = remote_rows_by_owner.entry(owner).or_default();
                    entry.rows.push(row.clone());
                    *entry.shard_row_counts.entry(shard).or_insert(0) += 1;
                }
            }
        }

        for (owner_node_id, pending_batch) in remote_rows_by_owner {
            let Some(endpoint) = self.endpoints_by_node.get(&owner_node_id) else {
                return Err(WriteRoutingError::MissingOwnerEndpoint {
                    node_id: owner_node_id,
                });
            };
            let PendingRemoteBatch {
                rows: owner_rows,
                shard_row_counts,
            } = pending_batch;
            if owner_rows.len() <= self.tuning.max_remote_batch_rows {
                let idempotency_key = build_forwarded_batch_idempotency_key(
                    &self.local_node_id,
                    &owner_node_id,
                    &owner_rows,
                );
                plan.remote_batches.push(RoutedWriteBatch {
                    owner_node_id,
                    endpoint: endpoint.clone(),
                    idempotency_key,
                    rows: owner_rows,
                    shard_row_counts,
                });
                continue;
            }

            for chunk in owner_rows.chunks(self.tuning.max_remote_batch_rows) {
                let chunk_rows = chunk.to_vec();
                let idempotency_key = build_forwarded_batch_idempotency_key(
                    &self.local_node_id,
                    &owner_node_id,
                    &chunk_rows,
                );
                let chunk_shard_row_counts = self.shard_row_counts_for_rows(&chunk_rows);
                plan.remote_batches.push(RoutedWriteBatch {
                    owner_node_id: owner_node_id.clone(),
                    endpoint: endpoint.clone(),
                    idempotency_key,
                    rows: chunk_rows,
                    shard_row_counts: chunk_shard_row_counts,
                });
            }
        }

        Ok(plan)
    }

    #[allow(dead_code)]
    pub async fn route_and_write(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        rows: Vec<Row>,
    ) -> Result<RoutedWriteStats, WriteRoutingError> {
        self.route_and_write_with_consistency_and_ring_version(
            storage,
            rpc_client,
            rows,
            None,
            DEFAULT_INTERNAL_RING_VERSION,
        )
        .await
    }

    pub async fn route_and_write_with_consistency_and_ring_version(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        rows: Vec<Row>,
        requested_consistency: Option<ClusterWriteConsistency>,
        ring_version: u64,
    ) -> Result<RoutedWriteStats, WriteRoutingError> {
        let policy = self.resolve_write_policy(requested_consistency)?;
        self.route_and_write_with_policy(storage, rpc_client, rows, policy, ring_version)
            .await
    }

    async fn route_and_write_with_policy(
        &self,
        storage: &Arc<dyn Storage>,
        rpc_client: &RpcClient,
        rows: Vec<Row>,
        policy: WriteAckPolicy,
        ring_version: u64,
    ) -> Result<RoutedWriteStats, WriteRoutingError> {
        CLUSTER_WRITE_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);

        let plan = self.plan_rows(rows)?;
        if plan.shard_replica_owners.is_empty() {
            return Ok(RoutedWriteStats::default());
        }
        record_write_plan_metrics(&plan);

        let WritePlan {
            local_rows,
            local_shard_row_counts,
            remote_batches,
            shard_replica_owners,
            ..
        } = plan;

        let mut coordinator =
            WriteCoordinatorState::new(policy.mode(), policy, &shard_replica_owners);
        let mut remote_batches_iter = remote_batches.into_iter();
        let mut remote_tasks = JoinSet::new();
        for _ in 0..self.tuning.max_inflight_remote_batches {
            let Some(batch) = remote_batches_iter.next() else {
                break;
            };
            let rpc_client = rpc_client.clone();
            remote_tasks.spawn(execute_remote_batch_with_ring_version(
                rpc_client,
                batch,
                ring_version,
            ));
        }

        let local_rows_count = local_rows.len();
        let mut local_rows_written = 0usize;
        let mut remote_rows_count = 0usize;
        let mut remote_batches_count = 0usize;

        if local_rows_count > 0 {
            let storage = Arc::clone(storage);
            let result =
                tokio::task::spawn_blocking(move || storage.insert_rows(&local_rows)).await;
            match result {
                Ok(Ok(())) => {
                    local_rows_written = local_rows_count;
                    coordinator.record_success_for_shards(local_shard_row_counts.keys());
                }
                Ok(Err(_)) | Err(_) => {
                    coordinator.record_failure_for_shards(local_shard_row_counts.keys(), false);
                }
            }
        }

        if let Some(failure) = coordinator.first_impossible_failure() {
            CLUSTER_WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(failure.into_error(policy.mode()));
        }

        while let Some(joined) = remote_tasks.join_next().await {
            let batch = match joined {
                Ok(batch) => batch,
                Err(err) => {
                    CLUSTER_WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    return Err(WriteRoutingError::TaskJoin {
                        message: err.to_string(),
                    });
                }
            };

            match batch.result {
                Ok(()) => {
                    remote_rows_count = remote_rows_count.saturating_add(batch.batch.rows.len());
                    remote_batches_count = remote_batches_count.saturating_add(1);
                    coordinator.record_success_for_shards(batch.batch.shard_row_counts.keys());
                }
                Err(err) => {
                    let timed_out = matches!(
                        &err,
                        WriteRoutingError::RemoteIngest {
                            source: RpcError::Timeout { .. }
                                | RpcError::HttpStatus { status: 504, .. },
                            ..
                        }
                    );
                    if err.retryable() {
                        if let Err(outbox_err) =
                            self.enqueue_failed_batch(&batch.batch, ring_version)
                        {
                            CLUSTER_WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                            return Err(outbox_err);
                        }
                    }
                    coordinator
                        .record_failure_for_shards(batch.batch.shard_row_counts.keys(), timed_out);
                }
            }

            if let Some(next_batch) = remote_batches_iter.next() {
                let rpc_client = rpc_client.clone();
                remote_tasks.spawn(execute_remote_batch_with_ring_version(
                    rpc_client,
                    next_batch,
                    ring_version,
                ));
            }
        }

        if let Some(failure) = coordinator.first_unsatisfied_failure() {
            CLUSTER_WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(failure.into_error(policy.mode()));
        }

        CLUSTER_WRITE_LOCAL_ROWS_TOTAL.fetch_add(local_rows_written as u64, Ordering::Relaxed);
        CLUSTER_WRITE_ROUTED_ROWS_TOTAL.fetch_add(remote_rows_count as u64, Ordering::Relaxed);
        CLUSTER_WRITE_ROUTED_BATCHES_TOTAL
            .fetch_add(remote_batches_count as u64, Ordering::Relaxed);

        Ok(RoutedWriteStats {
            inserted_rows: local_rows_written + remote_rows_count,
            local_rows: local_rows_written,
            remote_rows: remote_rows_count,
            remote_batches: remote_batches_count,
            consistency: coordinator.consistency_outcome(),
        })
    }

    fn enqueue_failed_batch(
        &self,
        batch: &RoutedWriteBatch,
        ring_version: u64,
    ) -> Result<(), WriteRoutingError> {
        let Some(outbox) = self.outbox.as_ref() else {
            return Ok(());
        };

        outbox
            .enqueue_replica_write_with_capabilities(
                &batch.owner_node_id,
                &batch.endpoint,
                &batch.idempotency_key,
                ring_version,
                &required_capabilities_for_rows(&batch.rows),
                &batch.rows,
            )
            .map_err(|source| {
                eprintln!(
                    "cluster hinted handoff enqueue failed for node '{}': {source}",
                    batch.owner_node_id
                );
                WriteRoutingError::OutboxEnqueue {
                    node_id: batch.owner_node_id.clone(),
                    source,
                }
            })
    }

    #[allow(dead_code)]
    pub fn owner_for_series<'a>(
        &'a self,
        metric: &str,
        labels: &[Label],
    ) -> Result<&'a str, WriteRoutingError> {
        let series_hash = stable_series_identity_hash(metric, labels);
        let shard = self.ring.shard_for_series_id(series_hash);
        self.owner_for_shard(shard)
    }

    #[allow(dead_code)]
    fn owner_for_shard(&self, shard: u32) -> Result<&str, WriteRoutingError> {
        let owners = self.replica_owners_for_shard(shard)?;
        let Some(owner) = owners.first() else {
            return Err(WriteRoutingError::MissingShardOwners { shard });
        };
        Ok(owner)
    }

    fn replica_owners_for_shard(&self, shard: u32) -> Result<&[String], WriteRoutingError> {
        let Some(owners) = self.ring.owners_for_shard(shard) else {
            return Err(WriteRoutingError::MissingShardOwners { shard });
        };
        if owners.is_empty() {
            return Err(WriteRoutingError::MissingShardOwners { shard });
        }
        Ok(owners)
    }

    fn shard_row_counts_for_rows(&self, rows: &[Row]) -> BTreeMap<u32, u64> {
        let mut counts = BTreeMap::new();
        for row in rows {
            let series_hash = stable_series_identity_hash(row.metric(), row.labels());
            let shard = self.ring.shard_for_series_id(series_hash);
            *counts.entry(shard).or_insert(0) += 1;
        }
        counts
    }
}

pub fn write_routing_metrics_snapshot() -> WriteRoutingMetricsSnapshot {
    WriteRoutingMetricsSnapshot {
        requests_total: CLUSTER_WRITE_REQUESTS_TOTAL.load(Ordering::Relaxed),
        local_rows_total: CLUSTER_WRITE_LOCAL_ROWS_TOTAL.load(Ordering::Relaxed),
        routed_rows_total: CLUSTER_WRITE_ROUTED_ROWS_TOTAL.load(Ordering::Relaxed),
        routed_batches_total: CLUSTER_WRITE_ROUTED_BATCHES_TOTAL.load(Ordering::Relaxed),
        failures_total: CLUSTER_WRITE_FAILURES_TOTAL.load(Ordering::Relaxed),
    }
}

pub fn write_routing_labeled_metrics_snapshot() -> WriteRoutingLabeledMetricsSnapshot {
    with_write_labeled_metrics(|metrics| {
        let shards = metrics
            .shard_rows_total
            .iter()
            .map(|(shard, rows_total)| WriteRoutingShardMetricsSnapshot {
                shard: *shard,
                rows_total: *rows_total,
            })
            .collect::<Vec<_>>();
        let peers = metrics
            .peers
            .iter()
            .map(|(node_id, peer)| WriteRoutingPeerMetricsSnapshot {
                node_id: node_id.clone(),
                routed_rows_total: peer.routed_rows_total,
                routed_batches_total: peer.routed_batches_total,
                remote_requests_total: peer.remote_requests_total,
                remote_failures_total: peer.remote_failures_total,
                remote_request_duration_nanos_total: peer.remote_request_latency.sum_nanos,
                remote_request_duration_count: peer.remote_request_latency.count,
                remote_request_duration_buckets: peer.remote_request_latency.bucket_counts.to_vec(),
            })
            .collect::<Vec<_>>();
        WriteRoutingLabeledMetricsSnapshot { shards, peers }
    })
}

fn with_write_labeled_metrics<T>(mut f: impl FnMut(&mut WriteRoutingLabeledMetrics) -> T) -> T {
    let lock = CLUSTER_WRITE_LABELED_METRICS
        .get_or_init(|| Mutex::new(WriteRoutingLabeledMetrics::default()));
    let mut guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut guard)
}

fn record_write_plan_metrics(plan: &WritePlan) {
    with_write_labeled_metrics(|metrics| {
        for (shard, rows_total) in &plan.shard_row_counts {
            let entry = metrics.shard_rows_total.entry(*shard).or_insert(0);
            *entry = entry.saturating_add(*rows_total);
        }
        for batch in &plan.remote_batches {
            let entry = metrics
                .peers
                .entry(batch.owner_node_id.clone())
                .or_insert_with(PerPeerRoutingMetrics::default);
            entry.routed_batches_total = entry.routed_batches_total.saturating_add(1);
            entry.routed_rows_total = entry
                .routed_rows_total
                .saturating_add(batch.rows.len() as u64);
        }
    });
}

fn record_write_remote_request_metrics(node_id: &str, duration_nanos: u64, failed: bool) {
    with_write_labeled_metrics(|metrics| {
        let entry = metrics
            .peers
            .entry(node_id.to_string())
            .or_insert_with(PerPeerRoutingMetrics::default);
        entry.remote_requests_total = entry.remote_requests_total.saturating_add(1);
        if failed {
            entry.remote_failures_total = entry.remote_failures_total.saturating_add(1);
        }
        entry.remote_request_latency.record(duration_nanos);
    });
}

fn record_write_remote_failure(node_id: &str) {
    with_write_labeled_metrics(|metrics| {
        let entry = metrics
            .peers
            .entry(node_id.to_string())
            .or_insert_with(PerPeerRoutingMetrics::default);
        entry.remote_failures_total = entry.remote_failures_total.saturating_add(1);
    });
}

fn saturating_elapsed_nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub fn build_forwarded_batch_idempotency_key(
    coordinator_node_id: &str,
    owner_node_id: &str,
    rows: &[Row],
) -> String {
    let mut row_fingerprints = rows.iter().map(stable_row_fingerprint).collect::<Vec<_>>();
    row_fingerprints.sort_unstable();

    let mut hash = 0xcbf29ce484222325u64;
    hash = stable_fnv1a64_update(hash, b"tsink-idem-v1");
    hash = stable_fnv1a64_update(hash, coordinator_node_id.as_bytes());
    hash = stable_fnv1a64_update(hash, b"->");
    hash = stable_fnv1a64_update(hash, owner_node_id.as_bytes());
    hash = stable_fnv1a64_update(hash, &(row_fingerprints.len() as u64).to_le_bytes());
    for fingerprint in row_fingerprints {
        hash = stable_fnv1a64_update(hash, &fingerprint.to_le_bytes());
    }

    let routing_hash = stable_fnv1a64(
        format!(
            "{}->{}",
            coordinator_node_id.to_ascii_lowercase(),
            owner_node_id.to_ascii_lowercase()
        )
        .as_bytes(),
    );

    format!("tsink:v1:{routing_hash:016x}:{hash:016x}")
}

pub fn stable_series_identity_hash(metric: &str, labels: &[Label]) -> u64 {
    tsink::label::stable_series_identity_hash(metric, labels)
}

fn stable_fnv1a64(bytes: &[u8]) -> u64 {
    stable_fnv1a64_update(0xcbf29ce484222325u64, bytes)
}

fn stable_fnv1a64_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn stable_row_fingerprint(row: &Row) -> u64 {
    let mut hash = stable_series_identity_hash(row.metric(), row.labels());
    hash = stable_fnv1a64_update(hash, &row.data_point().timestamp.to_le_bytes());
    hash = stable_hash_value(hash, &row.data_point().value);
    hash
}

fn stable_hash_value(hash: u64, value: &Value) -> u64 {
    match value {
        Value::F64(inner) => {
            let normalized = if inner.is_nan() {
                f64::NAN.to_bits()
            } else {
                inner.to_bits()
            };
            stable_fnv1a64_update(
                stable_fnv1a64_update(hash, b"f64"),
                &normalized.to_le_bytes(),
            )
        }
        Value::I64(inner) => {
            stable_fnv1a64_update(stable_fnv1a64_update(hash, b"i64"), &inner.to_le_bytes())
        }
        Value::U64(inner) => {
            stable_fnv1a64_update(stable_fnv1a64_update(hash, b"u64"), &inner.to_le_bytes())
        }
        Value::Bool(inner) => {
            stable_fnv1a64_update(stable_fnv1a64_update(hash, b"bool"), &[*inner as u8])
        }
        Value::Bytes(inner) => stable_fnv1a64_update(stable_fnv1a64_update(hash, b"bytes"), inner),
        Value::String(inner) => {
            stable_fnv1a64_update(stable_fnv1a64_update(hash, b"string"), inner.as_bytes())
        }
        Value::Histogram(inner) => match serde_json::to_vec(inner) {
            Ok(bytes) => stable_fnv1a64_update(stable_fnv1a64_update(hash, b"histogram"), &bytes),
            Err(_) => stable_fnv1a64_update(stable_fnv1a64_update(hash, b"histogram"), b""),
        },
    }
}

fn parse_env_usize(var: &str, default: usize, enforce_positive: bool) -> Result<usize, String> {
    let raw = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };

    let parsed = raw
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("{var} must be an integer, got '{raw}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

fn write_consistency_strength(mode: ClusterWriteConsistency) -> u8 {
    match mode {
        ClusterWriteConsistency::One => 0,
        ClusterWriteConsistency::Quorum => 1,
        ClusterWriteConsistency::All => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::dedupe::validate_idempotency_key;
    use crate::cluster::membership::MembershipView;
    use crate::cluster::outbox::{HintedHandoffOutbox, OutboxConfig, OutboxEnqueueError};
    use crate::cluster::rpc::{
        InternalIngestRowsRequest, InternalIngestRowsResponse, RpcClientConfig,
        DEFAULT_INTERNAL_RING_VERSION,
    };
    use crate::http::{read_http_request, write_http_response, HttpResponse};
    use std::collections::HashSet;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tsink::{DataPoint, StorageBuilder, TimestampPrecision};

    #[test]
    fn quorum_policy_requires_majority() {
        let policy = WriteAckPolicy::new(ClusterWriteConsistency::Quorum);
        assert_eq!(policy.required_acks(1), 1);
        assert_eq!(policy.required_acks(2), 2);
        assert_eq!(policy.required_acks(3), 2);
    }

    fn build_router(default_write_consistency: ClusterWriteConsistency) -> WriteRouter {
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
        WriteRouter::new(
            "node-a".to_string(),
            ring,
            &membership,
            default_write_consistency,
        )
        .expect("router should build")
    }

    fn build_local_only_router(default_write_consistency: ClusterWriteConsistency) -> WriteRouter {
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
        WriteRouter::new(
            "node-a".to_string(),
            ring,
            &membership,
            default_write_consistency,
        )
        .expect("router should build")
    }

    fn build_router_with_endpoints(
        local_endpoint: String,
        seed_nodes: Vec<(&str, String)>,
        replication_factor: u16,
        default_write_consistency: ClusterWriteConsistency,
    ) -> WriteRouter {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some(local_endpoint),
            seeds: seed_nodes
                .iter()
                .map(|(node_id, endpoint)| format!("{node_id}@{endpoint}"))
                .collect(),
            shards: 64,
            replication_factor,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring = ShardRing::build(cfg.shards, cfg.replication_factor, &membership)
            .expect("ring should build");
        WriteRouter::new(
            "node-a".to_string(),
            ring,
            &membership,
            default_write_consistency,
        )
        .expect("router should build")
    }

    fn build_rows_for_owner(router: &WriteRouter, owner: &str, count: usize) -> Vec<Row> {
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        let mut idx = 0usize;
        while rows.len() < count {
            let labels = vec![Label::new("instance", format!("i-{idx}"))];
            let metric = format!("cpu_usage_{idx}");
            if seen.insert((metric.clone(), labels[0].value.clone()))
                && router
                    .owner_for_series(&metric, &labels)
                    .expect("owner resolution should succeed")
                    == owner
            {
                rows.push(Row::with_labels(
                    metric,
                    labels,
                    DataPoint::new(1_700_000_000_000 + idx as i64, idx as f64),
                ));
            }
            idx += 1;
        }
        rows
    }

    fn build_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|idx| {
                Row::with_labels(
                    format!("cpu_usage_{idx}"),
                    vec![Label::new("instance", format!("i-{idx}"))],
                    DataPoint::new(1_700_000_000_000 + idx as i64, idx as f64),
                )
            })
            .collect()
    }

    fn build_test_outbox(temp_dir: &TempDir, max_record_bytes: u64) -> Arc<HintedHandoffOutbox> {
        Arc::new(
            HintedHandoffOutbox::open(
                temp_dir.path().join("outbox.log"),
                OutboxConfig {
                    max_entries: 32,
                    max_bytes: 8 * 1024 * 1024,
                    max_peer_bytes: 8 * 1024 * 1024,
                    max_log_bytes: 8 * 1024 * 1024,
                    replay_interval_secs: 1,
                    replay_batch_size: 16,
                    max_backoff_secs: 2,
                    max_record_bytes,
                    cleanup_interval_secs: 1,
                    cleanup_min_stale_records: 1,
                    stalled_peer_age_secs: 1,
                    stalled_peer_min_entries: 1,
                    stalled_peer_min_bytes: 1,
                },
            )
            .expect("test outbox should open"),
        )
    }

    fn reserve_unused_endpoint() -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("temporary endpoint should bind");
        let endpoint = listener
            .local_addr()
            .expect("temporary endpoint should expose local address")
            .to_string();
        drop(listener);
        endpoint
    }

    async fn spawn_success_ingest_server() -> (String, JoinHandle<()>) {
        spawn_success_ingest_server_with_expected_ring_version(DEFAULT_INTERNAL_RING_VERSION).await
    }

    async fn spawn_success_ingest_server_with_expected_ring_version(
        expected_ring_version: u64,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address")
            .to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should decode");
            assert_eq!(request.path_without_query(), "/internal/v1/ingest_rows");

            let parsed: InternalIngestRowsRequest =
                serde_json::from_slice(&request.body).expect("ingest payload should decode");
            let idempotency_key = parsed
                .idempotency_key
                .as_deref()
                .expect("idempotency key should be present on forwarded writes");
            validate_idempotency_key(idempotency_key).expect("idempotency key should be valid");
            assert_eq!(parsed.ring_version, expected_ring_version);
            let response = HttpResponse::new(
                200,
                serde_json::to_vec(&InternalIngestRowsResponse {
                    inserted_rows: parsed.rows.len(),
                })
                .expect("response should encode"),
            )
            .with_header("Content-Type", "application/json");
            tolerate_test_client_disconnect(write_http_response(&mut stream, &response).await)
                .expect("response should write");
        });
        (addr, server)
    }

    async fn spawn_success_ingest_server_on_endpoint(
        endpoint: &str,
        expected_rows: usize,
    ) -> JoinHandle<()> {
        let listener = TcpListener::bind(endpoint)
            .await
            .expect("listener should bind");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should decode");
            assert_eq!(request.path_without_query(), "/internal/v1/ingest_rows");

            let parsed: InternalIngestRowsRequest =
                serde_json::from_slice(&request.body).expect("ingest payload should decode");
            let idempotency_key = parsed
                .idempotency_key
                .as_deref()
                .expect("idempotency key should be present on forwarded writes");
            validate_idempotency_key(idempotency_key).expect("idempotency key should be valid");
            assert_eq!(parsed.ring_version, DEFAULT_INTERNAL_RING_VERSION);
            assert_eq!(parsed.rows.len(), expected_rows);

            let response = HttpResponse::new(
                200,
                serde_json::to_vec(&InternalIngestRowsResponse {
                    inserted_rows: parsed.rows.len(),
                })
                .expect("response should encode"),
            )
            .with_header("Content-Type", "application/json");
            tolerate_test_client_disconnect(write_http_response(&mut stream, &response).await)
                .expect("response should write");
        })
    }

    fn tolerate_test_client_disconnect(result: Result<(), String>) -> Result<(), String> {
        match result {
            Ok(()) => Ok(()),
            Err(err) if err.contains("Broken pipe") || err.contains("Connection reset by peer") => {
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn spawn_timeout_ingest_server(delay: Duration) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address")
            .to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let _ = read_http_request(&mut stream, &mut read_buffer).await;
            tokio::time::sleep(delay).await;
        });
        (addr, server)
    }

    #[test]
    fn plan_rows_splits_local_and_remote_owners() {
        let router = build_router(ClusterWriteConsistency::Quorum);
        let mut rows = build_rows_for_owner(&router, "node-a", 2);
        rows.extend(build_rows_for_owner(&router, "node-b", 3));

        let plan = router.plan_rows(rows).expect("plan should build");
        assert_eq!(plan.local_rows.len(), 2);
        assert_eq!(plan.remote_batches.len(), 1);
        assert_eq!(plan.remote_batches[0].owner_node_id, "node-b");
        assert_eq!(plan.remote_batches[0].rows.len(), 3);
        assert!(!plan.shard_replica_owners.is_empty());
        for owners in plan.shard_replica_owners.values() {
            assert_eq!(owners.len(), 1);
        }
    }

    #[test]
    fn plan_rows_replicates_to_all_owners_when_rf_two() {
        let router = build_router_with_endpoints(
            "127.0.0.1:9301".to_string(),
            vec![("node-b", "127.0.0.1:9302".to_string())],
            2,
            ClusterWriteConsistency::Quorum,
        );
        let rows = build_rows(4);

        let plan = router.plan_rows(rows).expect("plan should build");
        assert_eq!(plan.local_rows.len(), 4);
        assert_eq!(plan.remote_batches.len(), 1);
        assert_eq!(plan.remote_batches[0].rows.len(), 4);
        for owners in plan.shard_replica_owners.values() {
            assert_eq!(owners.len(), 2);
        }
    }

    #[test]
    fn plan_rows_splits_remote_batches_by_tuning_limit() {
        let router = build_router_with_endpoints(
            "127.0.0.1:9311".to_string(),
            vec![("node-b", "127.0.0.1:9312".to_string())],
            2,
            ClusterWriteConsistency::Quorum,
        )
        .with_tuning(WriteRouterTuning {
            max_remote_batch_rows: 2,
            max_inflight_remote_batches: 8,
        });
        let rows = build_rows(5);

        let plan = router.plan_rows(rows).expect("plan should build");
        assert_eq!(plan.local_rows.len(), 5);
        assert_eq!(plan.remote_batches.len(), 3);
        let batch_sizes = plan
            .remote_batches
            .iter()
            .map(|batch| batch.rows.len())
            .collect::<Vec<_>>();
        assert_eq!(batch_sizes, vec![2, 2, 1]);
        for batch in &plan.remote_batches {
            assert_eq!(batch.owner_node_id, "node-b");
            assert!(!batch.idempotency_key.is_empty());
            let shard_count_total = batch.shard_row_counts.values().copied().sum::<u64>() as usize;
            assert_eq!(shard_count_total, batch.rows.len());
        }
    }

    #[tokio::test]
    async fn route_and_write_inserts_rows_locally_when_single_node() {
        let router = build_local_only_router(ClusterWriteConsistency::Quorum);
        let rows = build_rows_for_owner(&router, "node-a", 3);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let stats = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect("routing should succeed");
        assert_eq!(stats.local_rows, 3);
        assert_eq!(stats.remote_rows, 0);
        assert_eq!(stats.inserted_rows, 3);
        let consistency = stats
            .consistency
            .expect("consistency metadata should be set");
        assert_eq!(consistency.mode, ClusterWriteConsistency::Quorum);
        assert_eq!(consistency.required_acks, 1);
        assert_eq!(consistency.acknowledged_replicas_min, 1);

        let local_rows = build_rows_for_owner(&router, "node-a", 3);
        for row in local_rows {
            let points = storage
                .select(
                    row.metric(),
                    row.labels(),
                    row.data_point().timestamp,
                    row.data_point().timestamp + 1,
                )
                .expect("local point should exist");
            assert_eq!(points.len(), 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_propagates_custom_ring_version() {
        let expected_ring_version = 42;
        let (endpoint, server) =
            spawn_success_ingest_server_with_expected_ring_version(expected_ring_version).await;
        let router = build_router_with_endpoints(
            "127.0.0.1:9400".to_string(),
            vec![("node-b", endpoint)],
            2,
            ClusterWriteConsistency::Quorum,
        );
        let rows = build_rows(3);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let stats = router
            .route_and_write_with_consistency_and_ring_version(
                &storage,
                &rpc_client,
                rows,
                None,
                expected_ring_version,
            )
            .await
            .expect("routing should succeed");
        assert_eq!(stats.local_rows, 3);
        assert_eq!(stats.remote_rows, 3);
        assert_eq!(stats.inserted_rows, 6);

        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_quorum_succeeds_with_one_replica_down() {
        let (healthy_endpoint, healthy_server) = spawn_success_ingest_server().await;
        let router = build_router_with_endpoints(
            "127.0.0.1:9401".to_string(),
            vec![
                ("node-b", healthy_endpoint),
                ("node-c", "127.0.0.1:0".to_string()),
            ],
            3,
            ClusterWriteConsistency::Quorum,
        );
        let rows = build_rows(5);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let stats = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect("quorum write should succeed");
        assert_eq!(stats.local_rows, 5);
        assert_eq!(stats.remote_rows, 5);
        assert_eq!(stats.inserted_rows, 10);
        let consistency = stats
            .consistency
            .expect("consistency metadata should be present");
        assert_eq!(consistency.mode, ClusterWriteConsistency::Quorum);
        assert_eq!(consistency.required_acks, 2);
        assert_eq!(consistency.acknowledged_replicas_min, 2);

        healthy_server
            .await
            .expect("healthy remote server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_quorum_enqueues_failed_replica_in_outbox() {
        let (healthy_endpoint, healthy_server) = spawn_success_ingest_server().await;
        let temp_dir = TempDir::new().expect("temp dir should create");
        let outbox = build_test_outbox(&temp_dir, 1024 * 1024);
        let router = build_router_with_endpoints(
            "127.0.0.1:9421".to_string(),
            vec![
                ("node-b", healthy_endpoint),
                ("node-c", "127.0.0.1:0".to_string()),
            ],
            3,
            ClusterWriteConsistency::Quorum,
        )
        .with_outbox(Arc::clone(&outbox));
        let rows = build_rows(4);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let stats = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect("quorum write should succeed");
        assert_eq!(stats.local_rows, 4);
        assert_eq!(stats.remote_rows, 4);
        assert_eq!(stats.inserted_rows, 8);

        let backlog = outbox.backlog_snapshot();
        assert_eq!(backlog.queued_entries, 1);
        assert!(backlog.queued_bytes > 0);
        let peers = outbox.peer_backlog_snapshot();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "node-c");
        assert_eq!(peers[0].queued_entries, 1);

        healthy_server
            .await
            .expect("healthy remote server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_one_succeeds_when_all_remote_replicas_are_unavailable() {
        let router = build_router_with_endpoints(
            "127.0.0.1:9441".to_string(),
            vec![
                ("node-b", "127.0.0.1:0".to_string()),
                ("node-c", "127.0.0.2:0".to_string()),
            ],
            3,
            ClusterWriteConsistency::One,
        );
        let rows = build_rows(3);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let stats = router
            .route_and_write(&storage, &rpc_client, rows.clone())
            .await
            .expect("one consistency should tolerate unavailable replicas");
        assert_eq!(stats.local_rows, rows.len());
        assert_eq!(stats.remote_rows, 0);
        assert_eq!(stats.inserted_rows, rows.len());
        let consistency = stats
            .consistency
            .expect("consistency metadata should be present");
        assert_eq!(consistency.mode, ClusterWriteConsistency::One);
        assert_eq!(consistency.required_acks, 1);
        assert_eq!(consistency.acknowledged_replicas_min, 1);

        for row in rows {
            let points = storage
                .select(
                    row.metric(),
                    row.labels(),
                    row.data_point().timestamp,
                    row.data_point().timestamp + 1,
                )
                .expect("local point should exist");
            assert_eq!(points.len(), 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_quorum_fails_when_majority_is_unavailable() {
        let router = build_router_with_endpoints(
            "127.0.0.1:9451".to_string(),
            vec![
                ("node-b", "127.0.0.1:0".to_string()),
                ("node-c", "127.0.0.2:0".to_string()),
            ],
            3,
            ClusterWriteConsistency::Quorum,
        );
        let rows = build_rows(3);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let err = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect_err("quorum consistency should fail without majority");
        assert!(matches!(
            err,
            WriteRoutingError::InsufficientReplicas {
                mode: ClusterWriteConsistency::Quorum,
                required_acks: 2,
                acknowledged_acks: 1,
                max_possible_acks: 1,
                ..
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_quorum_replays_outbox_after_restart_without_data_loss() {
        let (healthy_endpoint, healthy_server) = spawn_success_ingest_server().await;
        let recoverable_endpoint = reserve_unused_endpoint();
        let temp_dir = TempDir::new().expect("temp dir should create");
        let rows = build_rows(3);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        {
            let outbox = build_test_outbox(&temp_dir, 1024 * 1024);
            let router = build_router_with_endpoints(
                "127.0.0.1:9461".to_string(),
                vec![
                    ("node-b", healthy_endpoint.clone()),
                    ("node-c", recoverable_endpoint.clone()),
                ],
                3,
                ClusterWriteConsistency::Quorum,
            )
            .with_outbox(Arc::clone(&outbox));
            let rpc_client = RpcClient::new(RpcClientConfig {
                timeout: Duration::from_millis(100),
                max_retries: 0,
                internal_auth_token: "cluster-test-token".to_string(),
                internal_auth_runtime: None,
                ..RpcClientConfig::default()
            });

            let stats = router
                .route_and_write(&storage, &rpc_client, rows.clone())
                .await
                .expect("quorum write should succeed");
            assert_eq!(stats.local_rows, rows.len());
            assert_eq!(stats.remote_rows, rows.len());
            assert_eq!(stats.inserted_rows, rows.len() * 2);
            let consistency = stats
                .consistency
                .expect("consistency metadata should be present");
            assert_eq!(consistency.mode, ClusterWriteConsistency::Quorum);
            assert_eq!(consistency.required_acks, 2);
            assert_eq!(consistency.acknowledged_replicas_min, 2);
            assert_eq!(outbox.backlog_snapshot().queued_entries, 1);

            for row in &rows {
                let points = storage
                    .select(
                        row.metric(),
                        row.labels(),
                        row.data_point().timestamp,
                        row.data_point().timestamp + 1,
                    )
                    .expect("local point should exist");
                assert_eq!(points.len(), 1);
            }
        }

        let reopened_outbox = build_test_outbox(&temp_dir, 1024 * 1024);
        assert_eq!(reopened_outbox.backlog_snapshot().queued_entries, 1);
        let recovering_server =
            spawn_success_ingest_server_on_endpoint(&recoverable_endpoint, rows.len()).await;
        let replay_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(250),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });
        reopened_outbox
            .replay_due_once(&replay_client)
            .await
            .expect("outbox replay should succeed");
        assert_eq!(reopened_outbox.backlog_snapshot().queued_entries, 0);

        recovering_server
            .await
            .expect("recovering server task should complete");
        healthy_server
            .await
            .expect("healthy server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_reports_error_when_outbox_rejects_enqueue() {
        let (healthy_endpoint, healthy_server) = spawn_success_ingest_server().await;
        let temp_dir = TempDir::new().expect("temp dir should create");
        let outbox = build_test_outbox(&temp_dir, 64);
        let router = build_router_with_endpoints(
            "127.0.0.1:9431".to_string(),
            vec![
                ("node-b", healthy_endpoint),
                ("node-c", "127.0.0.1:0".to_string()),
            ],
            3,
            ClusterWriteConsistency::Quorum,
        )
        .with_outbox(Arc::clone(&outbox));
        let rows = build_rows(2);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let err = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect_err("write should fail when outbox cannot enqueue");
        assert!(matches!(
            err,
            WriteRoutingError::OutboxEnqueue {
                source: OutboxEnqueueError::RecordTooLarge { .. },
                ..
            }
        ));
        assert_eq!(outbox.backlog_snapshot().queued_entries, 0);

        healthy_server
            .await
            .expect("healthy remote server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_all_fails_when_replica_is_unavailable() {
        let (healthy_endpoint, healthy_server) = spawn_success_ingest_server().await;
        let router = build_router_with_endpoints(
            "127.0.0.1:9501".to_string(),
            vec![
                ("node-b", healthy_endpoint),
                ("node-c", "127.0.0.1:0".to_string()),
            ],
            3,
            ClusterWriteConsistency::All,
        );
        let rows = build_rows(3);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(100),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let err = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect_err("all consistency should fail");
        match err {
            WriteRoutingError::InsufficientReplicas {
                mode: ClusterWriteConsistency::All,
                required_acks: 3,
                acknowledged_acks,
                max_possible_acks: 2,
                ..
            } => assert!(acknowledged_acks <= 2),
            other => panic!("unexpected write error: {other:?}"),
        }

        healthy_server
            .await
            .expect("healthy remote server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_and_write_reports_timeout_when_quorum_unmet() {
        let (slow_endpoint, slow_server) =
            spawn_timeout_ingest_server(Duration::from_millis(200)).await;
        let router = build_router_with_endpoints(
            "127.0.0.1:9601".to_string(),
            vec![("node-b", slow_endpoint)],
            2,
            ClusterWriteConsistency::Quorum,
        );
        let rows = build_rows(2);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(25),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            internal_auth_runtime: None,
            ..RpcClientConfig::default()
        });

        let err = router
            .route_and_write(&storage, &rpc_client, rows)
            .await
            .expect_err("quorum write should time out");
        assert!(matches!(
            err,
            WriteRoutingError::ConsistencyTimeout {
                mode: ClusterWriteConsistency::Quorum,
                required_acks: 2,
                acknowledged_acks: 1,
                ..
            }
        ));

        tokio::time::timeout(Duration::from_secs(1), slow_server)
            .await
            .expect("slow server task should finish")
            .expect("slow server task should not panic");
    }

    #[test]
    fn write_consistency_override_rejects_stronger_than_configured_mode() {
        let router = build_router(ClusterWriteConsistency::Quorum);
        let err = router
            .resolve_write_policy(Some(ClusterWriteConsistency::All))
            .expect_err("stronger overrides should be rejected");
        assert!(matches!(
            err,
            WriteRoutingError::InvalidConsistencyOverride {
                requested: ClusterWriteConsistency::All,
                configured: ClusterWriteConsistency::Quorum,
            }
        ));
    }

    #[test]
    fn stable_series_hash_is_label_order_invariant() {
        let hash_a =
            stable_series_identity_hash("metric", &[Label::new("b", "2"), Label::new("a", "1")]);
        let hash_b =
            stable_series_identity_hash("metric", &[Label::new("a", "1"), Label::new("b", "2")]);
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn forwarded_batch_idempotency_key_is_stable_for_same_logical_batch() {
        let rows_a = vec![
            Row::with_labels(
                "cpu_usage",
                vec![Label::new("instance", "a"), Label::new("job", "api")],
                DataPoint::new(1_700_000_000_000, 1.0),
            ),
            Row::with_labels(
                "cpu_usage",
                vec![Label::new("instance", "b"), Label::new("job", "api")],
                DataPoint::new(1_700_000_000_100, 2.0),
            ),
        ];
        let rows_b = vec![rows_a[1].clone(), rows_a[0].clone()];

        let key_a = build_forwarded_batch_idempotency_key("node-a", "node-b", &rows_a);
        let key_b = build_forwarded_batch_idempotency_key("node-a", "node-b", &rows_b);

        assert_eq!(key_a, key_b);
        validate_idempotency_key(&key_a).expect("generated key should be valid");
    }
}
