use crate::admission;
use crate::cluster::consensus::{ControlConsensusRuntime, ProposeOutcome};
use crate::cluster::control::{ControlNodeStatus, ControlState, ShardHandoffPhase};
use crate::cluster::hotspot;
use crate::cluster::membership::{ClusterNode, MembershipView};
use crate::cluster::replication::stable_series_identity_hash;
use crate::cluster::ring::ShardRing;
use crate::cluster::rpc::{
    InternalControlCommand, InternalDigestWindowRequest, InternalDigestWindowResponse,
    InternalRepairBackfillRequest, InternalRepairBackfillResponse, RpcClient,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tsink::{DataPoint, Label, Storage, TsinkError};

const DIGEST_INTERVAL_SECS_ENV: &str = "TSINK_CLUSTER_DIGEST_INTERVAL_SECS";
const DIGEST_WINDOW_SECS_ENV: &str = "TSINK_CLUSTER_DIGEST_WINDOW_SECS";
const DIGEST_MAX_SHARDS_PER_TICK_ENV: &str = "TSINK_CLUSTER_DIGEST_MAX_SHARDS_PER_TICK";
const DIGEST_MAX_MISMATCH_REPORTS_ENV: &str = "TSINK_CLUSTER_DIGEST_MAX_MISMATCH_REPORTS";
const DIGEST_MAX_BYTES_PER_TICK_ENV: &str = "TSINK_CLUSTER_DIGEST_MAX_BYTES_PER_TICK";
const REPAIR_MAX_MISMATCHES_PER_TICK_ENV: &str = "TSINK_CLUSTER_REPAIR_MAX_MISMATCHES_PER_TICK";
const REPAIR_MAX_SERIES_PER_TICK_ENV: &str = "TSINK_CLUSTER_REPAIR_MAX_SERIES_PER_TICK";
const REPAIR_MAX_ROWS_PER_TICK_ENV: &str = "TSINK_CLUSTER_REPAIR_MAX_ROWS_PER_TICK";
const REPAIR_MAX_RUNTIME_MS_PER_TICK_ENV: &str = "TSINK_CLUSTER_REPAIR_MAX_RUNTIME_MS_PER_TICK";
const REPAIR_FAILURE_BACKOFF_SECS_ENV: &str = "TSINK_CLUSTER_REPAIR_FAILURE_BACKOFF_SECS";
const REBALANCE_INTERVAL_SECS_ENV: &str = "TSINK_CLUSTER_REBALANCE_INTERVAL_SECS";
const REBALANCE_MAX_ROWS_PER_TICK_ENV: &str = "TSINK_CLUSTER_REBALANCE_MAX_ROWS_PER_TICK";
const REBALANCE_MAX_SHARDS_PER_TICK_ENV: &str = "TSINK_CLUSTER_REBALANCE_MAX_SHARDS_PER_TICK";

pub const DEFAULT_DIGEST_INTERVAL_SECS: u64 = 30;
pub const DEFAULT_DIGEST_WINDOW_SECS: u64 = 300;
pub const DEFAULT_DIGEST_MAX_SHARDS_PER_TICK: usize = 64;
pub const DEFAULT_DIGEST_MAX_MISMATCH_REPORTS: usize = 128;
pub const DEFAULT_DIGEST_MAX_BYTES_PER_TICK: usize = 262_144;
pub const DEFAULT_REPAIR_MAX_MISMATCHES_PER_TICK: usize = 2;
pub const DEFAULT_REPAIR_MAX_SERIES_PER_TICK: usize = 256;
pub const DEFAULT_REPAIR_MAX_ROWS_PER_TICK: usize = 16_384;
pub const DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK: u64 = 100;
pub const DEFAULT_REPAIR_FAILURE_BACKOFF_SECS: u64 = 30;
pub const DEFAULT_REBALANCE_INTERVAL_SECS: u64 = 5;
pub const DEFAULT_REBALANCE_MAX_ROWS_PER_TICK: usize = 10_000;
pub const DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DigestExchangeConfig {
    pub interval: Duration,
    pub window: Duration,
    pub max_shards_per_tick: usize,
    pub max_mismatch_reports: usize,
    pub max_bytes_per_tick: usize,
    pub max_repair_mismatches_per_tick: usize,
    pub max_repair_series_per_tick: usize,
    pub max_repair_rows_per_tick: usize,
    pub max_repair_runtime_per_tick: Duration,
    pub repair_failure_backoff: Duration,
    pub rebalance_interval: Duration,
    pub rebalance_max_rows_per_tick: usize,
    pub rebalance_max_shards_per_tick: usize,
}

impl Default for DigestExchangeConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_DIGEST_INTERVAL_SECS),
            window: Duration::from_secs(DEFAULT_DIGEST_WINDOW_SECS),
            max_shards_per_tick: DEFAULT_DIGEST_MAX_SHARDS_PER_TICK,
            max_mismatch_reports: DEFAULT_DIGEST_MAX_MISMATCH_REPORTS,
            max_bytes_per_tick: DEFAULT_DIGEST_MAX_BYTES_PER_TICK,
            max_repair_mismatches_per_tick: DEFAULT_REPAIR_MAX_MISMATCHES_PER_TICK,
            max_repair_series_per_tick: DEFAULT_REPAIR_MAX_SERIES_PER_TICK,
            max_repair_rows_per_tick: DEFAULT_REPAIR_MAX_ROWS_PER_TICK,
            max_repair_runtime_per_tick: Duration::from_millis(
                DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
            ),
            repair_failure_backoff: Duration::from_secs(DEFAULT_REPAIR_FAILURE_BACKOFF_SECS),
            rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
            rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
            rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
        }
    }
}

impl DigestExchangeConfig {
    pub fn from_env() -> Result<Self, String> {
        let interval_secs =
            parse_env_u64(DIGEST_INTERVAL_SECS_ENV, DEFAULT_DIGEST_INTERVAL_SECS, true)?;
        let window_secs = parse_env_u64(DIGEST_WINDOW_SECS_ENV, DEFAULT_DIGEST_WINDOW_SECS, true)?;
        let max_shards_per_tick = parse_env_usize(
            DIGEST_MAX_SHARDS_PER_TICK_ENV,
            DEFAULT_DIGEST_MAX_SHARDS_PER_TICK,
            true,
        )?;
        let max_mismatch_reports = parse_env_usize(
            DIGEST_MAX_MISMATCH_REPORTS_ENV,
            DEFAULT_DIGEST_MAX_MISMATCH_REPORTS,
            true,
        )?;
        let max_bytes_per_tick = parse_env_usize(
            DIGEST_MAX_BYTES_PER_TICK_ENV,
            DEFAULT_DIGEST_MAX_BYTES_PER_TICK,
            true,
        )?;
        let max_repair_mismatches_per_tick = parse_env_usize(
            REPAIR_MAX_MISMATCHES_PER_TICK_ENV,
            DEFAULT_REPAIR_MAX_MISMATCHES_PER_TICK,
            true,
        )?;
        let max_repair_series_per_tick = parse_env_usize(
            REPAIR_MAX_SERIES_PER_TICK_ENV,
            DEFAULT_REPAIR_MAX_SERIES_PER_TICK,
            true,
        )?;
        let max_repair_rows_per_tick = parse_env_usize(
            REPAIR_MAX_ROWS_PER_TICK_ENV,
            DEFAULT_REPAIR_MAX_ROWS_PER_TICK,
            true,
        )?;
        let max_repair_runtime_ms_per_tick = parse_env_u64(
            REPAIR_MAX_RUNTIME_MS_PER_TICK_ENV,
            DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
            true,
        )?;
        let repair_failure_backoff_secs = parse_env_u64(
            REPAIR_FAILURE_BACKOFF_SECS_ENV,
            DEFAULT_REPAIR_FAILURE_BACKOFF_SECS,
            false,
        )?;
        let rebalance_interval_secs = parse_env_u64(
            REBALANCE_INTERVAL_SECS_ENV,
            DEFAULT_REBALANCE_INTERVAL_SECS,
            true,
        )?;
        let rebalance_max_rows_per_tick = parse_env_usize(
            REBALANCE_MAX_ROWS_PER_TICK_ENV,
            DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
            true,
        )?;
        let rebalance_max_shards_per_tick = parse_env_usize(
            REBALANCE_MAX_SHARDS_PER_TICK_ENV,
            DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            true,
        )?;
        Ok(Self {
            interval: Duration::from_secs(interval_secs),
            window: Duration::from_secs(window_secs),
            max_shards_per_tick,
            max_mismatch_reports,
            max_bytes_per_tick,
            max_repair_mismatches_per_tick,
            max_repair_series_per_tick,
            max_repair_rows_per_tick,
            max_repair_runtime_per_tick: Duration::from_millis(max_repair_runtime_ms_per_tick),
            repair_failure_backoff: Duration::from_secs(repair_failure_backoff_secs),
            rebalance_interval: Duration::from_secs(rebalance_interval_secs),
            rebalance_max_rows_per_tick,
            rebalance_max_shards_per_tick,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DigestMismatchReport {
    pub shard: u32,
    pub peer_node_id: String,
    pub peer_endpoint: String,
    pub ring_version: u64,
    pub window_start: i64,
    pub window_end: i64,
    pub local_series_count: u64,
    pub local_point_count: u64,
    pub local_fingerprint: u64,
    pub remote_series_count: u64,
    pub remote_point_count: u64,
    pub remote_fingerprint: u64,
    pub detected_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestExchangeSnapshot {
    pub interval_secs: u64,
    pub window_secs: u64,
    pub max_shards_per_tick: usize,
    pub max_mismatch_reports: usize,
    pub max_bytes_per_tick: usize,
    pub max_repair_mismatches_per_tick: usize,
    pub max_repair_series_per_tick: usize,
    pub max_repair_rows_per_tick: usize,
    pub max_repair_runtime_ms_per_tick: u64,
    pub repair_failure_backoff_secs: u64,
    pub repair_paused: bool,
    pub repair_cancel_generation: u64,
    pub repair_cancellations_total: u64,
    pub runs_total: u64,
    pub windows_compared_total: u64,
    pub windows_success_total: u64,
    pub windows_failed_total: u64,
    pub local_compute_failures_total: u64,
    pub mismatches_total: u64,
    pub bytes_exchanged_total: u64,
    pub bytes_exchanged_last_run: u64,
    pub budget_exhaustions_total: u64,
    pub windows_skipped_budget_total: u64,
    pub budget_exhausted_last_run: bool,
    pub last_run_unix_ms: u64,
    pub last_success_unix_ms: u64,
    pub last_ring_version: u64,
    pub compared_shards_last_run: u64,
    pub prioritized_shards_last_run: u64,
    pub compared_peers_last_run: u64,
    pub repairs_attempted_total: u64,
    pub repairs_succeeded_total: u64,
    pub repairs_failed_total: u64,
    pub repairs_skipped_budget_total: u64,
    pub repairs_skipped_non_additive_total: u64,
    pub repairs_skipped_backoff_total: u64,
    pub repairs_skipped_paused_total: u64,
    pub repairs_skipped_time_budget_total: u64,
    pub repairs_cancelled_total: u64,
    pub repair_time_budget_exhaustions_total: u64,
    pub repair_time_budget_exhausted_last_run: bool,
    pub repair_series_scanned_total: u64,
    pub repair_rows_scanned_total: u64,
    pub repair_rows_inserted_total: u64,
    pub repairs_attempted_last_run: u64,
    pub repairs_succeeded_last_run: u64,
    pub repairs_failed_last_run: u64,
    pub repairs_cancelled_last_run: u64,
    pub repairs_skipped_backoff_last_run: u64,
    pub repair_rows_inserted_last_run: u64,
    pub last_error: Option<String>,
    pub mismatches: Vec<DigestMismatchReport>,
}

impl DigestExchangeSnapshot {
    pub fn empty() -> Self {
        let config = DigestExchangeConfig::default();
        Self::from_state(
            config,
            &DigestExchangeMetrics::default(),
            RepairControlSnapshot::default(),
        )
    }

    fn from_state(
        config: DigestExchangeConfig,
        metrics: &DigestExchangeMetrics,
        control: RepairControlSnapshot,
    ) -> Self {
        Self {
            interval_secs: config.interval.as_secs(),
            window_secs: config.window.as_secs(),
            max_shards_per_tick: config.max_shards_per_tick,
            max_mismatch_reports: config.max_mismatch_reports,
            max_bytes_per_tick: config.max_bytes_per_tick,
            max_repair_mismatches_per_tick: config.max_repair_mismatches_per_tick,
            max_repair_series_per_tick: config.max_repair_series_per_tick,
            max_repair_rows_per_tick: config.max_repair_rows_per_tick,
            max_repair_runtime_ms_per_tick: u64::try_from(
                config.max_repair_runtime_per_tick.as_millis(),
            )
            .unwrap_or(u64::MAX),
            repair_failure_backoff_secs: config.repair_failure_backoff.as_secs(),
            repair_paused: control.paused,
            repair_cancel_generation: control.cancel_generation,
            repair_cancellations_total: control.cancellations_total,
            runs_total: metrics.runs_total,
            windows_compared_total: metrics.windows_compared_total,
            windows_success_total: metrics.windows_success_total,
            windows_failed_total: metrics.windows_failed_total,
            local_compute_failures_total: metrics.local_compute_failures_total,
            mismatches_total: metrics.mismatches_total,
            bytes_exchanged_total: metrics.bytes_exchanged_total,
            bytes_exchanged_last_run: metrics.bytes_exchanged_last_run,
            budget_exhaustions_total: metrics.budget_exhaustions_total,
            windows_skipped_budget_total: metrics.windows_skipped_budget_total,
            budget_exhausted_last_run: metrics.budget_exhausted_last_run,
            last_run_unix_ms: metrics.last_run_unix_ms,
            last_success_unix_ms: metrics.last_success_unix_ms,
            last_ring_version: metrics.last_ring_version,
            compared_shards_last_run: metrics.compared_shards_last_run,
            prioritized_shards_last_run: metrics.prioritized_shards_last_run,
            compared_peers_last_run: metrics.compared_peers_last_run,
            repairs_attempted_total: metrics.repairs_attempted_total,
            repairs_succeeded_total: metrics.repairs_succeeded_total,
            repairs_failed_total: metrics.repairs_failed_total,
            repairs_skipped_budget_total: metrics.repairs_skipped_budget_total,
            repairs_skipped_non_additive_total: metrics.repairs_skipped_non_additive_total,
            repairs_skipped_backoff_total: metrics.repairs_skipped_backoff_total,
            repairs_skipped_paused_total: metrics.repairs_skipped_paused_total,
            repairs_skipped_time_budget_total: metrics.repairs_skipped_time_budget_total,
            repairs_cancelled_total: metrics.repairs_cancelled_total,
            repair_time_budget_exhaustions_total: metrics.repair_time_budget_exhaustions_total,
            repair_time_budget_exhausted_last_run: metrics.repair_time_budget_exhausted_last_run,
            repair_series_scanned_total: metrics.repair_series_scanned_total,
            repair_rows_scanned_total: metrics.repair_rows_scanned_total,
            repair_rows_inserted_total: metrics.repair_rows_inserted_total,
            repairs_attempted_last_run: metrics.repairs_attempted_last_run,
            repairs_succeeded_last_run: metrics.repairs_succeeded_last_run,
            repairs_failed_last_run: metrics.repairs_failed_last_run,
            repairs_cancelled_last_run: metrics.repairs_cancelled_last_run,
            repairs_skipped_backoff_last_run: metrics.repairs_skipped_backoff_last_run,
            repair_rows_inserted_last_run: metrics.repair_rows_inserted_last_run,
            last_error: metrics.last_error.clone(),
            mismatches: metrics.mismatches.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct DigestExchangeRunOutcome {
    compared_shards: u64,
    prioritized_shards: u64,
    compared_peers: u64,
    repairs_attempted: u64,
    repairs_succeeded: u64,
    repairs_failed: u64,
    repairs_skipped_budget: u64,
    repairs_skipped_non_additive: u64,
    repairs_skipped_backoff: u64,
    repairs_skipped_paused: u64,
    repairs_skipped_time_budget: u64,
    repairs_cancelled: u64,
    repair_time_budget_exhausted: bool,
    repair_series_scanned: u64,
    repair_rows_scanned: u64,
    repair_rows_inserted: u64,
    windows_compared: u64,
    windows_success: u64,
    windows_failed: u64,
    local_compute_failures: u64,
    bytes_exchanged: u64,
    windows_skipped_budget: u64,
    budget_exhausted: bool,
    ring_version: u64,
    next_shard_cursor: u32,
    mismatches: Vec<DigestMismatchReport>,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct DigestExchangeMetrics {
    runs_total: u64,
    windows_compared_total: u64,
    windows_success_total: u64,
    windows_failed_total: u64,
    local_compute_failures_total: u64,
    mismatches_total: u64,
    bytes_exchanged_total: u64,
    bytes_exchanged_last_run: u64,
    budget_exhaustions_total: u64,
    windows_skipped_budget_total: u64,
    budget_exhausted_last_run: bool,
    last_run_unix_ms: u64,
    last_success_unix_ms: u64,
    last_ring_version: u64,
    compared_shards_last_run: u64,
    prioritized_shards_last_run: u64,
    compared_peers_last_run: u64,
    repairs_attempted_total: u64,
    repairs_succeeded_total: u64,
    repairs_failed_total: u64,
    repairs_skipped_budget_total: u64,
    repairs_skipped_non_additive_total: u64,
    repairs_skipped_backoff_total: u64,
    repairs_skipped_paused_total: u64,
    repairs_skipped_time_budget_total: u64,
    repairs_cancelled_total: u64,
    repair_time_budget_exhaustions_total: u64,
    repair_time_budget_exhausted_last_run: bool,
    repair_series_scanned_total: u64,
    repair_rows_scanned_total: u64,
    repair_rows_inserted_total: u64,
    repairs_attempted_last_run: u64,
    repairs_succeeded_last_run: u64,
    repairs_failed_last_run: u64,
    repairs_cancelled_last_run: u64,
    repairs_skipped_backoff_last_run: u64,
    repair_rows_inserted_last_run: u64,
    next_shard_cursor: u32,
    last_error: Option<String>,
    mismatches: VecDeque<DigestMismatchReport>,
}

#[derive(Debug, Clone, Copy)]
struct RepairBudget {
    remaining_mismatches: usize,
    remaining_series: usize,
    remaining_rows: usize,
}

impl RepairBudget {
    fn from_config(config: DigestExchangeConfig) -> Self {
        Self {
            remaining_mismatches: config.max_repair_mismatches_per_tick,
            remaining_series: config.max_repair_series_per_tick,
            remaining_rows: config.max_repair_rows_per_tick,
        }
    }

    fn exhausted(self) -> bool {
        self.remaining_mismatches == 0 || self.remaining_series == 0 || self.remaining_rows == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RepairControlSnapshot {
    pub paused: bool,
    pub cancel_generation: u64,
    pub cancellations_total: u64,
}

#[derive(Debug, Default)]
struct RepairControlState {
    paused: bool,
    cancel_generation: u64,
    cancellations_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RebalanceSchedulerControlSnapshot {
    pub paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceJobSnapshot {
    pub shard: u32,
    pub from_node_id: String,
    pub to_node_id: String,
    pub activation_ring_version: u64,
    pub phase: ShardHandoffPhase,
    pub copied_rows: u64,
    pub pending_rows: u64,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceMoveCandidateSnapshot {
    pub shard: u32,
    pub from_node_id: String,
    pub to_node_id: String,
    pub pressure_score: f64,
    pub movement_cost_score: f64,
    pub imbalance_improvement_score: f64,
    pub decision_score: f64,
    pub source_node_pressure: f64,
    pub target_node_pressure: f64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceSloGuardSnapshot {
    pub write_pressure_ratio: f64,
    pub query_pressure_ratio: f64,
    pub cluster_query_pressure_ratio: f64,
    pub effective_max_rows_per_tick: usize,
    pub block_new_handoffs: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceSchedulerSnapshot {
    pub interval_secs: u64,
    pub max_rows_per_tick: usize,
    pub max_shards_per_tick: usize,
    pub paused: bool,
    pub is_local_control_leader: bool,
    pub active_jobs: usize,
    pub runs_total: u64,
    pub jobs_considered_last_run: u64,
    pub jobs_advanced_total: u64,
    pub jobs_completed_total: u64,
    pub rows_scheduled_total: u64,
    pub rows_scheduled_last_run: u64,
    pub proposals_committed_total: u64,
    pub proposals_pending_total: u64,
    pub proposal_failures_total: u64,
    pub moves_blocked_by_slo_total: u64,
    pub effective_max_rows_per_tick_last_run: usize,
    pub last_run_unix_ms: u64,
    pub last_success_unix_ms: u64,
    pub last_error: Option<String>,
    pub slo_guard: RebalanceSloGuardSnapshot,
    pub candidate_moves: Vec<RebalanceMoveCandidateSnapshot>,
    pub jobs: Vec<RebalanceJobSnapshot>,
}

impl RebalanceSchedulerSnapshot {
    pub fn empty() -> Self {
        Self {
            interval_secs: DEFAULT_REBALANCE_INTERVAL_SECS,
            max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
            max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            paused: false,
            is_local_control_leader: false,
            active_jobs: 0,
            runs_total: 0,
            jobs_considered_last_run: 0,
            jobs_advanced_total: 0,
            jobs_completed_total: 0,
            rows_scheduled_total: 0,
            rows_scheduled_last_run: 0,
            proposals_committed_total: 0,
            proposals_pending_total: 0,
            proposal_failures_total: 0,
            moves_blocked_by_slo_total: 0,
            effective_max_rows_per_tick_last_run: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
            last_run_unix_ms: 0,
            last_success_unix_ms: 0,
            last_error: None,
            slo_guard: RebalanceSloGuardSnapshot {
                write_pressure_ratio: 0.0,
                query_pressure_ratio: 0.0,
                cluster_query_pressure_ratio: 0.0,
                effective_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                block_new_handoffs: false,
                reason: None,
            },
            candidate_moves: Vec::new(),
            jobs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RebalanceSchedulerRunOutcome {
    is_local_control_leader: bool,
    active_jobs: usize,
    jobs_considered: u64,
    jobs_advanced: u64,
    jobs_completed: u64,
    rows_scheduled: u64,
    proposals_committed: u64,
    proposals_pending: u64,
    proposal_failures: u64,
    moves_blocked_by_slo: u64,
    effective_max_rows_per_tick: usize,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct RebalanceSchedulerMetrics {
    is_local_control_leader: bool,
    active_jobs: usize,
    runs_total: u64,
    jobs_considered_last_run: u64,
    jobs_advanced_total: u64,
    jobs_completed_total: u64,
    rows_scheduled_total: u64,
    rows_scheduled_last_run: u64,
    proposals_committed_total: u64,
    proposals_pending_total: u64,
    proposal_failures_total: u64,
    moves_blocked_by_slo_total: u64,
    effective_max_rows_per_tick_last_run: usize,
    last_run_unix_ms: u64,
    last_success_unix_ms: u64,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct RebalanceSchedulerControlState {
    paused: bool,
}

#[derive(Debug, Clone)]
struct DigestPeerTarget {
    node_id: String,
    endpoint: String,
    request_ring_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RepairResumeCursorKey {
    peer_endpoint: String,
    ring_version: u64,
    shard: u32,
}

#[derive(Debug, Clone)]
struct RepairCandidate {
    peer_node_id: String,
    peer_endpoint: String,
    ring_version: u64,
    point_gap: u64,
    series_gap: u64,
    has_resume_cursor: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairAttemptStatus {
    Succeeded,
    Failed,
    Cancelled,
    SkippedPaused,
    SkippedBudget,
    SkippedBackoff,
    SkippedTimeBudget,
}

#[derive(Clone)]
pub struct DigestExchangeRuntime {
    local_node_id: String,
    rpc_client: RpcClient,
    control_consensus: Arc<ControlConsensusRuntime>,
    config: DigestExchangeConfig,
    metrics: Arc<Mutex<DigestExchangeMetrics>>,
    repair_control: Arc<Mutex<RepairControlState>>,
    repair_run_inflight: Arc<Mutex<bool>>,
    rebalance_control: Arc<Mutex<RebalanceSchedulerControlState>>,
    rebalance_run_inflight: Arc<Mutex<bool>>,
    rebalance_metrics: Arc<Mutex<RebalanceSchedulerMetrics>>,
    repair_resume_cursors: Arc<Mutex<BTreeMap<RepairResumeCursorKey, u64>>>,
    repair_failure_backoff_until_ms: Arc<Mutex<BTreeMap<RepairResumeCursorKey, u64>>>,
    digest_probe: Option<Arc<DigestProbeFn>>,
    repair_probe: Option<Arc<RepairProbeFn>>,
}

type DigestProbeFn = dyn Fn(&str, &InternalDigestWindowRequest) -> Result<InternalDigestWindowResponse, String>
    + Send
    + Sync;

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum RepairProbeRequest {
    Backfill(InternalRepairBackfillRequest),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum RepairProbeResponse {
    Backfill(InternalRepairBackfillResponse),
}

type RepairProbeFn =
    dyn Fn(&str, RepairProbeRequest) -> Result<RepairProbeResponse, String> + Send + Sync;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairRunTriggerError {
    AlreadyRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceRunTriggerError {
    AlreadyRunning,
}

#[derive(Debug)]
struct RunInflightGuard {
    slot: Arc<Mutex<bool>>,
}

impl RunInflightGuard {
    fn try_acquire(slot: &Arc<Mutex<bool>>) -> Option<Self> {
        let mut inflight = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *inflight {
            return None;
        }
        *inflight = true;
        Some(Self {
            slot: Arc::clone(slot),
        })
    }
}

impl Drop for RunInflightGuard {
    fn drop(&mut self) {
        let mut inflight = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *inflight = false;
    }
}

impl std::fmt::Debug for DigestExchangeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DigestExchangeRuntime")
            .field("local_node_id", &self.local_node_id)
            .field("rpc_client", &self.rpc_client)
            .field("control_consensus", &self.control_consensus)
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .field("repair_control", &self.repair_control)
            .field("repair_run_inflight", &self.repair_run_inflight)
            .field("rebalance_control", &self.rebalance_control)
            .field("rebalance_run_inflight", &self.rebalance_run_inflight)
            .field("rebalance_metrics", &self.rebalance_metrics)
            .field("repair_resume_cursors", &self.repair_resume_cursors)
            .field(
                "repair_failure_backoff_until_ms",
                &self.repair_failure_backoff_until_ms,
            )
            .field(
                "digest_probe",
                &self.digest_probe.as_ref().map(|_| "<installed>"),
            )
            .field(
                "repair_probe",
                &self.repair_probe.as_ref().map(|_| "<installed>"),
            )
            .finish()
    }
}

impl DigestExchangeRuntime {
    pub fn new(
        local_node_id: String,
        rpc_client: RpcClient,
        control_consensus: Arc<ControlConsensusRuntime>,
        config: DigestExchangeConfig,
    ) -> Self {
        Self {
            local_node_id,
            rpc_client,
            control_consensus,
            config,
            metrics: Arc::new(Mutex::new(DigestExchangeMetrics::default())),
            repair_control: Arc::new(Mutex::new(RepairControlState::default())),
            repair_run_inflight: Arc::new(Mutex::new(false)),
            rebalance_control: Arc::new(Mutex::new(RebalanceSchedulerControlState::default())),
            rebalance_run_inflight: Arc::new(Mutex::new(false)),
            rebalance_metrics: Arc::new(Mutex::new(RebalanceSchedulerMetrics::default())),
            repair_resume_cursors: Arc::new(Mutex::new(BTreeMap::new())),
            repair_failure_backoff_until_ms: Arc::new(Mutex::new(BTreeMap::new())),
            digest_probe: None,
            repair_probe: None,
        }
    }

    #[cfg(test)]
    fn with_digest_probe(mut self, digest_probe: Arc<DigestProbeFn>) -> Self {
        self.digest_probe = Some(digest_probe);
        self
    }

    #[cfg(test)]
    fn with_repair_probe(mut self, repair_probe: Arc<RepairProbeFn>) -> Self {
        self.repair_probe = Some(repair_probe);
        self
    }

    pub fn snapshot(&self) -> DigestExchangeSnapshot {
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let control = self.repair_control_snapshot();
        DigestExchangeSnapshot::from_state(self.config, &metrics, control)
    }

    pub fn repair_control_snapshot(&self) -> RepairControlSnapshot {
        let control = self
            .repair_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        RepairControlSnapshot {
            paused: control.paused,
            cancel_generation: control.cancel_generation,
            cancellations_total: control.cancellations_total,
        }
    }

    pub fn is_repair_run_inflight(&self) -> bool {
        *self
            .repair_run_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    pub fn set_repair_run_inflight_for_test(&self, value: bool) {
        let mut inflight = self
            .repair_run_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *inflight = value;
    }

    pub fn pause_repairs(&self) -> RepairControlSnapshot {
        let mut control = self
            .repair_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        control.paused = true;
        RepairControlSnapshot {
            paused: control.paused,
            cancel_generation: control.cancel_generation,
            cancellations_total: control.cancellations_total,
        }
    }

    pub fn resume_repairs(&self) -> RepairControlSnapshot {
        let mut control = self
            .repair_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        control.paused = false;
        RepairControlSnapshot {
            paused: control.paused,
            cancel_generation: control.cancel_generation,
            cancellations_total: control.cancellations_total,
        }
    }

    pub fn cancel_repairs(&self) -> RepairControlSnapshot {
        let mut control = self
            .repair_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        control.cancel_generation = control.cancel_generation.saturating_add(1);
        control.cancellations_total = control.cancellations_total.saturating_add(1);
        RepairControlSnapshot {
            paused: control.paused,
            cancel_generation: control.cancel_generation,
            cancellations_total: control.cancellations_total,
        }
    }

    pub fn rebalance_control_snapshot(&self) -> RebalanceSchedulerControlSnapshot {
        let control = self
            .rebalance_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        RebalanceSchedulerControlSnapshot {
            paused: control.paused,
        }
    }

    pub fn is_rebalance_run_inflight(&self) -> bool {
        *self
            .rebalance_run_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    pub fn set_rebalance_run_inflight_for_test(&self, value: bool) {
        let mut inflight = self
            .rebalance_run_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *inflight = value;
    }

    pub fn pause_rebalance(&self) -> RebalanceSchedulerControlSnapshot {
        let mut control = self
            .rebalance_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        control.paused = true;
        RebalanceSchedulerControlSnapshot {
            paused: control.paused,
        }
    }

    pub fn resume_rebalance(&self) -> RebalanceSchedulerControlSnapshot {
        let mut control = self
            .rebalance_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        control.paused = false;
        RebalanceSchedulerControlSnapshot {
            paused: control.paused,
        }
    }

    pub fn rebalance_snapshot(&self) -> RebalanceSchedulerSnapshot {
        let control = self.rebalance_control_snapshot();
        let state = self.control_consensus.current_state();
        let slo_guard = compute_rebalance_slo_guard(self.config.rebalance_max_rows_per_tick);
        let candidate_moves = collect_rebalance_move_candidates(&self.local_node_id, &state);
        let metrics = self
            .rebalance_metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut jobs = state
            .transitions
            .iter()
            .filter(|transition| transition.handoff.phase.is_active())
            .map(|transition| RebalanceJobSnapshot {
                shard: transition.shard,
                from_node_id: transition.from_node_id.clone(),
                to_node_id: transition.to_node_id.clone(),
                activation_ring_version: transition.activation_ring_version,
                phase: transition.handoff.phase,
                copied_rows: transition.handoff.copied_rows,
                pending_rows: transition.handoff.pending_rows,
                updated_unix_ms: transition.handoff.updated_unix_ms,
            })
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| {
            left.shard
                .cmp(&right.shard)
                .then_with(|| left.from_node_id.cmp(&right.from_node_id))
                .then_with(|| left.to_node_id.cmp(&right.to_node_id))
        });

        RebalanceSchedulerSnapshot {
            interval_secs: self.config.rebalance_interval.as_secs(),
            max_rows_per_tick: self.config.rebalance_max_rows_per_tick,
            max_shards_per_tick: self.config.rebalance_max_shards_per_tick,
            paused: control.paused,
            is_local_control_leader: metrics.is_local_control_leader,
            active_jobs: jobs.len(),
            runs_total: metrics.runs_total,
            jobs_considered_last_run: metrics.jobs_considered_last_run,
            jobs_advanced_total: metrics.jobs_advanced_total,
            jobs_completed_total: metrics.jobs_completed_total,
            rows_scheduled_total: metrics.rows_scheduled_total,
            rows_scheduled_last_run: metrics.rows_scheduled_last_run,
            proposals_committed_total: metrics.proposals_committed_total,
            proposals_pending_total: metrics.proposals_pending_total,
            proposal_failures_total: metrics.proposal_failures_total,
            moves_blocked_by_slo_total: metrics.moves_blocked_by_slo_total,
            effective_max_rows_per_tick_last_run: metrics.effective_max_rows_per_tick_last_run,
            last_run_unix_ms: metrics.last_run_unix_ms,
            last_success_unix_ms: metrics.last_success_unix_ms,
            last_error: metrics.last_error.clone(),
            slo_guard,
            candidate_moves,
            jobs,
        }
    }

    pub fn start_rebalance_worker(&self) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                runtime.run_rebalance_once().await;
                tokio::time::sleep(runtime.config.rebalance_interval).await;
            }
        })
    }

    pub fn start_worker(&self, storage: Arc<dyn Storage>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                runtime.run_once(Arc::clone(&storage)).await;
                tokio::time::sleep(runtime.config.interval).await;
            }
        })
    }

    pub async fn trigger_repair_run(
        &self,
        storage: Arc<dyn Storage>,
    ) -> Result<DigestExchangeSnapshot, RepairRunTriggerError> {
        let Some(_guard) = RunInflightGuard::try_acquire(&self.repair_run_inflight) else {
            return Err(RepairRunTriggerError::AlreadyRunning);
        };
        self.run_once_with_guard(storage).await;
        Ok(self.snapshot())
    }

    pub async fn trigger_rebalance_run(
        &self,
    ) -> Result<RebalanceSchedulerSnapshot, RebalanceRunTriggerError> {
        let Some(_guard) = RunInflightGuard::try_acquire(&self.rebalance_run_inflight) else {
            return Err(RebalanceRunTriggerError::AlreadyRunning);
        };
        self.run_rebalance_once_with_guard().await;
        Ok(self.rebalance_snapshot())
    }

    pub async fn run_rebalance_once(&self) {
        let Some(_guard) = RunInflightGuard::try_acquire(&self.rebalance_run_inflight) else {
            return;
        };
        self.run_rebalance_once_with_guard().await;
    }

    async fn run_rebalance_once_with_guard(&self) {
        let outcome = self.collect_rebalance_once().await;
        let now_ms = unix_timestamp_millis();
        let mut metrics = self
            .rebalance_metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        metrics.is_local_control_leader = outcome.is_local_control_leader;
        metrics.active_jobs = outcome.active_jobs;
        metrics.runs_total = metrics.runs_total.saturating_add(1);
        metrics.jobs_considered_last_run = outcome.jobs_considered;
        metrics.jobs_advanced_total = metrics
            .jobs_advanced_total
            .saturating_add(outcome.jobs_advanced);
        metrics.jobs_completed_total = metrics
            .jobs_completed_total
            .saturating_add(outcome.jobs_completed);
        metrics.rows_scheduled_total = metrics
            .rows_scheduled_total
            .saturating_add(outcome.rows_scheduled);
        metrics.rows_scheduled_last_run = outcome.rows_scheduled;
        metrics.proposals_committed_total = metrics
            .proposals_committed_total
            .saturating_add(outcome.proposals_committed);
        metrics.proposals_pending_total = metrics
            .proposals_pending_total
            .saturating_add(outcome.proposals_pending);
        metrics.proposal_failures_total = metrics
            .proposal_failures_total
            .saturating_add(outcome.proposal_failures);
        metrics.moves_blocked_by_slo_total = metrics
            .moves_blocked_by_slo_total
            .saturating_add(outcome.moves_blocked_by_slo);
        metrics.effective_max_rows_per_tick_last_run = outcome.effective_max_rows_per_tick;
        metrics.last_run_unix_ms = now_ms;
        if outcome.proposal_failures == 0 {
            metrics.last_success_unix_ms = now_ms;
        }
        metrics.last_error = outcome.last_error;
    }

    pub async fn run_once(&self, storage: Arc<dyn Storage>) {
        let Some(_guard) = RunInflightGuard::try_acquire(&self.repair_run_inflight) else {
            return;
        };
        self.run_once_with_guard(storage).await;
    }

    async fn run_once_with_guard(&self, storage: Arc<dyn Storage>) {
        let start_shard_cursor = {
            let metrics = self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            metrics.next_shard_cursor
        };
        let outcome = self.collect_once(storage, start_shard_cursor).await;
        let now_ms = unix_timestamp_millis();
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        metrics.runs_total = metrics.runs_total.saturating_add(1);
        metrics.windows_compared_total = metrics
            .windows_compared_total
            .saturating_add(outcome.windows_compared);
        metrics.windows_success_total = metrics
            .windows_success_total
            .saturating_add(outcome.windows_success);
        metrics.windows_failed_total = metrics
            .windows_failed_total
            .saturating_add(outcome.windows_failed);
        metrics.local_compute_failures_total = metrics
            .local_compute_failures_total
            .saturating_add(outcome.local_compute_failures);
        metrics.mismatches_total = metrics
            .mismatches_total
            .saturating_add(u64::try_from(outcome.mismatches.len()).unwrap_or(u64::MAX));
        metrics.bytes_exchanged_total = metrics
            .bytes_exchanged_total
            .saturating_add(outcome.bytes_exchanged);
        metrics.bytes_exchanged_last_run = outcome.bytes_exchanged;
        metrics.windows_skipped_budget_total = metrics
            .windows_skipped_budget_total
            .saturating_add(outcome.windows_skipped_budget);
        metrics.budget_exhaustions_total = metrics
            .budget_exhaustions_total
            .saturating_add(u64::from(outcome.budget_exhausted));
        metrics.budget_exhausted_last_run = outcome.budget_exhausted;
        metrics.last_run_unix_ms = now_ms;
        metrics.last_ring_version = outcome.ring_version.max(1);
        metrics.compared_shards_last_run = outcome.compared_shards;
        metrics.prioritized_shards_last_run = outcome.prioritized_shards;
        metrics.compared_peers_last_run = outcome.compared_peers;
        metrics.repairs_attempted_total = metrics
            .repairs_attempted_total
            .saturating_add(outcome.repairs_attempted);
        metrics.repairs_succeeded_total = metrics
            .repairs_succeeded_total
            .saturating_add(outcome.repairs_succeeded);
        metrics.repairs_failed_total = metrics
            .repairs_failed_total
            .saturating_add(outcome.repairs_failed);
        metrics.repairs_skipped_budget_total = metrics
            .repairs_skipped_budget_total
            .saturating_add(outcome.repairs_skipped_budget);
        metrics.repairs_skipped_non_additive_total = metrics
            .repairs_skipped_non_additive_total
            .saturating_add(outcome.repairs_skipped_non_additive);
        metrics.repairs_skipped_backoff_total = metrics
            .repairs_skipped_backoff_total
            .saturating_add(outcome.repairs_skipped_backoff);
        metrics.repairs_skipped_paused_total = metrics
            .repairs_skipped_paused_total
            .saturating_add(outcome.repairs_skipped_paused);
        metrics.repairs_skipped_time_budget_total = metrics
            .repairs_skipped_time_budget_total
            .saturating_add(outcome.repairs_skipped_time_budget);
        metrics.repairs_cancelled_total = metrics
            .repairs_cancelled_total
            .saturating_add(outcome.repairs_cancelled);
        metrics.repair_time_budget_exhaustions_total = metrics
            .repair_time_budget_exhaustions_total
            .saturating_add(u64::from(outcome.repair_time_budget_exhausted));
        metrics.repair_time_budget_exhausted_last_run = outcome.repair_time_budget_exhausted;
        metrics.repair_series_scanned_total = metrics
            .repair_series_scanned_total
            .saturating_add(outcome.repair_series_scanned);
        metrics.repair_rows_scanned_total = metrics
            .repair_rows_scanned_total
            .saturating_add(outcome.repair_rows_scanned);
        metrics.repair_rows_inserted_total = metrics
            .repair_rows_inserted_total
            .saturating_add(outcome.repair_rows_inserted);
        metrics.repairs_attempted_last_run = outcome.repairs_attempted;
        metrics.repairs_succeeded_last_run = outcome.repairs_succeeded;
        metrics.repairs_failed_last_run = outcome.repairs_failed;
        metrics.repairs_cancelled_last_run = outcome.repairs_cancelled;
        metrics.repairs_skipped_backoff_last_run = outcome.repairs_skipped_backoff;
        metrics.repair_rows_inserted_last_run = outcome.repair_rows_inserted;
        metrics.next_shard_cursor = outcome.next_shard_cursor;
        metrics.last_error = outcome.last_error;
        if outcome.windows_success > 0 {
            metrics.last_success_unix_ms = now_ms;
        }
        for mismatch in outcome.mismatches {
            metrics.mismatches.push_back(mismatch);
        }
        while metrics.mismatches.len() > self.config.max_mismatch_reports {
            metrics.mismatches.pop_front();
        }
    }

    async fn collect_rebalance_once(&self) -> RebalanceSchedulerRunOutcome {
        let mut outcome = RebalanceSchedulerRunOutcome::default();
        let state = self.control_consensus.current_state();
        let slo_guard = compute_rebalance_slo_guard(self.config.rebalance_max_rows_per_tick);
        let candidate_moves = collect_rebalance_move_candidates(&self.local_node_id, &state);
        outcome.effective_max_rows_per_tick = slo_guard.effective_max_rows_per_tick;
        let mut transitions = state
            .transitions
            .iter()
            .filter(|transition| transition.handoff.phase.is_active())
            .cloned()
            .collect::<Vec<_>>();
        transitions.sort_by(|left, right| {
            handoff_phase_priority(left.handoff.phase)
                .cmp(&handoff_phase_priority(right.handoff.phase))
                .then_with(|| left.shard.cmp(&right.shard))
                .then_with(|| left.from_node_id.cmp(&right.from_node_id))
                .then_with(|| left.to_node_id.cmp(&right.to_node_id))
        });

        outcome.active_jobs = transitions.len();
        outcome.is_local_control_leader = self.control_consensus.is_local_control_leader();
        if self.rebalance_control_snapshot().paused || !outcome.is_local_control_leader {
            return outcome;
        }

        if transitions.is_empty() {
            match plan_elastic_membership_command(&self.local_node_id, &state, &candidate_moves) {
                Ok(Some(command)) => {
                    let description = elastic_rebalance_command_label(&command);
                    if matches!(command, InternalControlCommand::BeginShardHandoff { .. }) {
                        outcome.jobs_considered = 1;
                        if slo_guard.block_new_handoffs {
                            outcome.moves_blocked_by_slo =
                                outcome.moves_blocked_by_slo.saturating_add(1);
                            outcome.last_error =
                                Some(slo_guard.reason.clone().unwrap_or_else(|| {
                                    "rebalance SLO guard blocked a new shard handoff".to_string()
                                }));
                            return outcome;
                        }
                    }
                    match self
                        .control_consensus
                        .propose_command(&self.rpc_client, command)
                        .await
                    {
                        Ok(ProposeOutcome::Committed { .. }) => {
                            outcome.proposals_committed =
                                outcome.proposals_committed.saturating_add(1);
                            if description == "begin_shard_handoff" {
                                outcome.jobs_advanced = outcome.jobs_advanced.saturating_add(1);
                                outcome.active_jobs = 1;
                            }
                        }
                        Ok(ProposeOutcome::Pending {
                            required,
                            acknowledged,
                        }) => {
                            outcome.proposals_pending = outcome.proposals_pending.saturating_add(1);
                            outcome.last_error = Some(format!(
                                "elastic rebalance proposal pending quorum for {description} ({acknowledged}/{required} acknowledgements)"
                            ));
                        }
                        Err(err) => {
                            outcome.proposal_failures = outcome.proposal_failures.saturating_add(1);
                            outcome.last_error = Some(format!(
                                "elastic rebalance proposal failed for {description}: {err}"
                            ));
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    outcome.proposal_failures = outcome.proposal_failures.saturating_add(1);
                    outcome.last_error = Some(format!("elastic rebalance planning failed: {err}"));
                }
            }
            return outcome;
        }

        let mut remaining_rows =
            u64::try_from(slo_guard.effective_max_rows_per_tick).unwrap_or(u64::MAX);
        let mut remaining_jobs = self.config.rebalance_max_shards_per_tick;

        for transition in transitions {
            if remaining_jobs == 0 {
                break;
            }

            let mut rows_scheduled_for_job = 0u64;
            let command = match transition.handoff.phase {
                ShardHandoffPhase::Warmup => {
                    if transition.handoff.pending_rows == 0 {
                        Some(InternalControlCommand::UpdateShardHandoff {
                            shard: transition.shard,
                            phase: ShardHandoffPhase::Cutover,
                            copied_rows: Some(transition.handoff.copied_rows),
                            pending_rows: Some(0),
                            last_error: None,
                        })
                    } else if remaining_rows == 0 {
                        None
                    } else {
                        rows_scheduled_for_job =
                            transition.handoff.pending_rows.min(remaining_rows);
                        let new_pending = transition
                            .handoff
                            .pending_rows
                            .saturating_sub(rows_scheduled_for_job);
                        let new_copied = transition
                            .handoff
                            .copied_rows
                            .saturating_add(rows_scheduled_for_job);
                        let next_phase = if new_pending == 0 {
                            ShardHandoffPhase::Cutover
                        } else {
                            ShardHandoffPhase::Warmup
                        };
                        Some(InternalControlCommand::UpdateShardHandoff {
                            shard: transition.shard,
                            phase: next_phase,
                            copied_rows: Some(new_copied),
                            pending_rows: Some(new_pending),
                            last_error: None,
                        })
                    }
                }
                ShardHandoffPhase::Cutover => Some(InternalControlCommand::UpdateShardHandoff {
                    shard: transition.shard,
                    phase: ShardHandoffPhase::FinalSync,
                    copied_rows: Some(transition.handoff.copied_rows),
                    pending_rows: Some(transition.handoff.pending_rows),
                    last_error: None,
                }),
                ShardHandoffPhase::FinalSync => {
                    if transition.handoff.pending_rows == 0 {
                        Some(InternalControlCommand::UpdateShardHandoff {
                            shard: transition.shard,
                            phase: ShardHandoffPhase::Completed,
                            copied_rows: Some(transition.handoff.copied_rows),
                            pending_rows: Some(0),
                            last_error: None,
                        })
                    } else if remaining_rows == 0 {
                        None
                    } else {
                        rows_scheduled_for_job =
                            transition.handoff.pending_rows.min(remaining_rows);
                        let new_pending = transition
                            .handoff
                            .pending_rows
                            .saturating_sub(rows_scheduled_for_job);
                        let new_copied = transition
                            .handoff
                            .copied_rows
                            .saturating_add(rows_scheduled_for_job);
                        let next_phase = if new_pending == 0 {
                            ShardHandoffPhase::Completed
                        } else {
                            ShardHandoffPhase::FinalSync
                        };
                        Some(InternalControlCommand::UpdateShardHandoff {
                            shard: transition.shard,
                            phase: next_phase,
                            copied_rows: Some(new_copied),
                            pending_rows: Some(new_pending),
                            last_error: None,
                        })
                    }
                }
                ShardHandoffPhase::Completed | ShardHandoffPhase::Failed => None,
            };

            let Some(command) = command else {
                continue;
            };

            let completes_job = matches!(
                command,
                InternalControlCommand::UpdateShardHandoff {
                    phase: ShardHandoffPhase::Completed,
                    ..
                } | InternalControlCommand::CompleteShardHandoff { .. }
            );
            remaining_rows = remaining_rows.saturating_sub(rows_scheduled_for_job);
            remaining_jobs = remaining_jobs.saturating_sub(1);
            outcome.jobs_considered = outcome.jobs_considered.saturating_add(1);
            outcome.rows_scheduled = outcome
                .rows_scheduled
                .saturating_add(rows_scheduled_for_job);

            match self
                .control_consensus
                .propose_command(&self.rpc_client, command)
                .await
            {
                Ok(ProposeOutcome::Committed { .. }) => {
                    outcome.proposals_committed = outcome.proposals_committed.saturating_add(1);
                    outcome.jobs_advanced = outcome.jobs_advanced.saturating_add(1);
                    if completes_job {
                        outcome.jobs_completed = outcome.jobs_completed.saturating_add(1);
                    }
                }
                Ok(ProposeOutcome::Pending {
                    required,
                    acknowledged,
                }) => {
                    outcome.proposals_pending = outcome.proposals_pending.saturating_add(1);
                    outcome.last_error = Some(format!(
                        "rebalance proposal pending quorum for shard {} ({acknowledged}/{required} acknowledgements)",
                        transition.shard
                    ));
                }
                Err(err) => {
                    outcome.proposal_failures = outcome.proposal_failures.saturating_add(1);
                    outcome.last_error = Some(format!(
                        "rebalance proposal failed for shard {}: {err}",
                        transition.shard
                    ));
                }
            }
        }

        outcome
    }

    async fn collect_once(
        &self,
        storage: Arc<dyn Storage>,
        start_shard_cursor: u32,
    ) -> DigestExchangeRunOutcome {
        let mut outcome = DigestExchangeRunOutcome::default();
        let mut repair_budget = RepairBudget::from_config(self.config);
        let mut repair_deadline = None;
        let state = self.control_consensus.current_state();
        let ring_version = state.ring_version.max(1);
        outcome.ring_version = ring_version;
        self.prune_repair_state_for_ring(ring_version);

        let shard_count = state.ring.shard_count.max(1);
        let mut next_shard_cursor = start_shard_cursor % shard_count;
        let window_end = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let window_ms = i64::try_from(self.config.window.as_millis()).unwrap_or(i64::MAX);
        let window_start = window_end.saturating_sub(window_ms);

        let prioritized_shards = prioritized_handoff_shards(&state);
        let prioritized_shard_set = prioritized_shards.iter().copied().collect::<BTreeSet<_>>();

        let mut sampled_shards = 0usize;
        for shard in prioritized_shards {
            if sampled_shards >= self.config.max_shards_per_tick {
                break;
            }
            if self
                .compare_shard_with_peers(
                    Arc::clone(&storage),
                    &state,
                    ring_version,
                    shard,
                    window_start,
                    window_end,
                    &mut repair_deadline,
                    &mut repair_budget,
                    &mut outcome,
                )
                .await
            {
                sampled_shards = sampled_shards.saturating_add(1);
                outcome.prioritized_shards = outcome.prioritized_shards.saturating_add(1);
            }
            if outcome.budget_exhausted {
                break;
            }
        }

        let mut scanned_rotating_shards = false;
        for offset in 0..shard_count {
            if sampled_shards >= self.config.max_shards_per_tick {
                break;
            }
            let shard = (start_shard_cursor + offset) % shard_count;
            next_shard_cursor = shard.saturating_add(1) % shard_count;
            scanned_rotating_shards = true;
            if prioritized_shard_set.contains(&shard) {
                continue;
            }

            if self
                .compare_shard_with_peers(
                    Arc::clone(&storage),
                    &state,
                    ring_version,
                    shard,
                    window_start,
                    window_end,
                    &mut repair_deadline,
                    &mut repair_budget,
                    &mut outcome,
                )
                .await
            {
                sampled_shards = sampled_shards.saturating_add(1);
            }
            if outcome.budget_exhausted {
                break;
            }
        }

        if !scanned_rotating_shards && sampled_shards >= self.config.max_shards_per_tick {
            next_shard_cursor = (start_shard_cursor.saturating_add(1)) % shard_count;
        }
        if outcome.budget_exhausted && !scanned_rotating_shards {
            next_shard_cursor = (start_shard_cursor.saturating_add(1)) % shard_count;
        }
        outcome.next_shard_cursor = next_shard_cursor;
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn compare_shard_with_peers(
        &self,
        storage: Arc<dyn Storage>,
        state: &ControlState,
        ring_version: u64,
        shard: u32,
        window_start: i64,
        window_end: i64,
        repair_deadline: &mut Option<Instant>,
        repair_budget: &mut RepairBudget,
        outcome: &mut DigestExchangeRunOutcome,
    ) -> bool {
        let owners = state.owners_for_shard_at_ring_version(shard, ring_version);
        let local_is_owner = owners.iter().any(|owner| owner == &self.local_node_id);
        let local_handoff_target = state.transitions.iter().find(|transition| {
            transition.shard == shard
                && transition.to_node_id == self.local_node_id
                && transition.handoff.phase.is_active()
        });
        if !local_is_owner && local_handoff_target.is_none() {
            return false;
        }

        let mut peer_targets = owners
            .into_iter()
            .filter(|owner| owner != &self.local_node_id)
            .filter_map(|peer_node_id| {
                let peer_record = match state.node_record(&peer_node_id) {
                    Some(record) => record,
                    None => {
                        outcome.windows_failed = outcome.windows_failed.saturating_add(1);
                        outcome.last_error = Some(format!(
                            "digest exchange peer '{peer_node_id}' missing from control-state membership"
                        ));
                        return None;
                    }
                };
                Some(DigestPeerTarget {
                    node_id: peer_record.id.clone(),
                    endpoint: peer_record.endpoint.clone(),
                    request_ring_version: ring_version,
                })
            })
            .collect::<Vec<_>>();

        if let Some(transition) = local_handoff_target {
            let Some(source_node) = state.node_record(&transition.from_node_id) else {
                outcome.windows_failed = outcome.windows_failed.saturating_add(1);
                outcome.last_error = Some(format!(
                    "digest exchange handoff source '{}' missing from control-state membership",
                    transition.from_node_id
                ));
                return false;
            };
            let source_ring_version = if ring_version >= transition.activation_ring_version {
                transition_stale_bridge_ring_version(transition.activation_ring_version)
            } else {
                ring_version
            };
            if !peer_targets.iter().any(|target| {
                target.node_id == source_node.id
                    && target.request_ring_version == source_ring_version
            }) {
                peer_targets.push(DigestPeerTarget {
                    node_id: source_node.id.clone(),
                    endpoint: source_node.endpoint.clone(),
                    request_ring_version: source_ring_version,
                });
            }
        }

        if peer_targets.is_empty() {
            return false;
        }

        outcome.compared_shards = outcome.compared_shards.saturating_add(1);
        let local_digest = match compute_shard_window_digest_async(
            Arc::clone(&storage),
            shard,
            state.ring.shard_count.max(1),
            ring_version,
            window_start,
            window_end,
        )
        .await
        {
            Ok(digest) => digest,
            Err(err) => {
                outcome.local_compute_failures = outcome.local_compute_failures.saturating_add(1);
                outcome.windows_failed = outcome
                    .windows_failed
                    .saturating_add(u64::try_from(peer_targets.len()).unwrap_or(u64::MAX));
                outcome.last_error = Some(format!(
                    "local digest compute failed for shard {shard}: {err}"
                ));
                return true;
            }
        };

        let max_bytes_per_tick = u64::try_from(self.config.max_bytes_per_tick).unwrap_or(u64::MAX);
        let mut repair_candidates = Vec::new();
        for (peer_index, peer_target) in peer_targets.iter().enumerate() {
            let request = InternalDigestWindowRequest {
                ring_version: peer_target.request_ring_version,
                shard,
                window_start,
                window_end,
            };
            let estimated_request_bytes = estimated_json_bytes(&request);
            let estimated_response_bytes = estimated_digest_response_upper_bound(&request);
            let estimated_exchange_bytes =
                estimated_request_bytes.saturating_add(estimated_response_bytes);
            if digest_budget_would_be_exceeded(
                outcome.bytes_exchanged,
                estimated_exchange_bytes,
                max_bytes_per_tick,
            ) {
                let remaining_windows =
                    u64::try_from(peer_targets.len().saturating_sub(peer_index))
                        .unwrap_or(u64::MAX);
                outcome.windows_skipped_budget = outcome
                    .windows_skipped_budget
                    .saturating_add(remaining_windows);
                outcome.budget_exhausted = true;
                outcome.last_error = Some(format!(
                    "digest byte budget exhausted at {}/{} bytes while comparing shard {}",
                    outcome.bytes_exchanged, max_bytes_per_tick, shard
                ));
                break;
            }
            outcome.bytes_exchanged = outcome
                .bytes_exchanged
                .saturating_add(u64::try_from(estimated_request_bytes).unwrap_or(u64::MAX));
            outcome.windows_compared = outcome.windows_compared.saturating_add(1);
            outcome.compared_peers = outcome.compared_peers.saturating_add(1);

            let remote_digest_result = if let Some(digest_probe) = self.digest_probe.as_ref() {
                digest_probe(&peer_target.endpoint, &request)
            } else {
                self.rpc_client
                    .digest_window(&peer_target.endpoint, &request)
                    .await
                    .map_err(|err| err.to_string())
            };

            match remote_digest_result {
                Ok(remote_digest) => {
                    outcome.bytes_exchanged = outcome.bytes_exchanged.saturating_add(
                        u64::try_from(estimated_json_bytes(&remote_digest)).unwrap_or(u64::MAX),
                    );
                    if outcome.bytes_exchanged > max_bytes_per_tick {
                        outcome.budget_exhausted = true;
                        outcome.last_error = Some(format!(
                            "digest byte budget exceeded at {}/{} bytes while comparing shard {}",
                            outcome.bytes_exchanged, max_bytes_per_tick, shard
                        ));
                    }
                    if remote_digest.ring_version != peer_target.request_ring_version
                        || remote_digest.shard != shard
                        || remote_digest.window_start != window_start
                        || remote_digest.window_end != window_end
                    {
                        outcome.windows_failed = outcome.windows_failed.saturating_add(1);
                        outcome.last_error = Some(format!(
                            "digest exchange response mismatch from peer '{}' for shard {}",
                            peer_target.node_id, shard
                        ));
                        continue;
                    }

                    outcome.windows_success = outcome.windows_success.saturating_add(1);
                    if local_digest.series_count != remote_digest.series_count
                        || local_digest.point_count != remote_digest.point_count
                        || local_digest.fingerprint != remote_digest.fingerprint
                    {
                        let mismatch = DigestMismatchReport {
                            shard,
                            peer_node_id: peer_target.node_id.clone(),
                            peer_endpoint: peer_target.endpoint.clone(),
                            ring_version: peer_target.request_ring_version,
                            window_start,
                            window_end,
                            local_series_count: local_digest.series_count,
                            local_point_count: local_digest.point_count,
                            local_fingerprint: local_digest.fingerprint,
                            remote_series_count: remote_digest.series_count,
                            remote_point_count: remote_digest.point_count,
                            remote_fingerprint: remote_digest.fingerprint,
                            detected_unix_ms: unix_timestamp_millis(),
                        };
                        outcome.mismatches.push(mismatch);
                        if remote_digest.point_count > local_digest.point_count
                            || remote_digest.series_count > local_digest.series_count
                        {
                            hotspot::record_repair_mismatch(
                                shard,
                                remote_digest
                                    .series_count
                                    .saturating_sub(local_digest.series_count),
                                remote_digest
                                    .point_count
                                    .saturating_sub(local_digest.point_count),
                            );
                            let cursor_key = RepairResumeCursorKey {
                                peer_endpoint: peer_target.endpoint.clone(),
                                ring_version: peer_target.request_ring_version,
                                shard,
                            };
                            repair_candidates.push(RepairCandidate {
                                peer_node_id: peer_target.node_id.clone(),
                                peer_endpoint: peer_target.endpoint.clone(),
                                ring_version: peer_target.request_ring_version,
                                point_gap: remote_digest
                                    .point_count
                                    .saturating_sub(local_digest.point_count),
                                series_gap: remote_digest
                                    .series_count
                                    .saturating_sub(local_digest.series_count),
                                has_resume_cursor: self.repair_resume_cursor(&cursor_key).is_some(),
                            });
                        } else {
                            outcome.repairs_skipped_non_additive =
                                outcome.repairs_skipped_non_additive.saturating_add(1);
                        }
                    }
                }
                Err(err) => {
                    outcome.windows_failed = outcome.windows_failed.saturating_add(1);
                    outcome.last_error = Some(format!(
                        "digest exchange RPC to peer '{}' failed: {err}",
                        peer_target.node_id
                    ));
                }
            }
            if outcome.budget_exhausted {
                break;
            }
        }

        if !repair_candidates.is_empty() {
            repair_candidates.sort_by(|left, right| {
                right
                    .has_resume_cursor
                    .cmp(&left.has_resume_cursor)
                    .then_with(|| right.point_gap.cmp(&left.point_gap))
                    .then_with(|| right.series_gap.cmp(&left.series_gap))
                    .then_with(|| left.peer_node_id.cmp(&right.peer_node_id))
                    .then_with(|| left.peer_endpoint.cmp(&right.peer_endpoint))
                    .then_with(|| left.ring_version.cmp(&right.ring_version))
            });

            for candidate in repair_candidates {
                let status = self
                    .try_backfill_from_peer(
                        storage.as_ref(),
                        candidate.peer_endpoint.as_str(),
                        candidate.ring_version,
                        shard,
                        state.ring.shard_count.max(1),
                        window_start,
                        window_end,
                        repair_deadline,
                        repair_budget,
                        outcome,
                    )
                    .await;
                if status == RepairAttemptStatus::SkippedBackoff {
                    continue;
                }
                break;
            }
        }

        true
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_backfill_from_peer(
        &self,
        storage: &dyn Storage,
        peer_endpoint: &str,
        ring_version: u64,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        repair_deadline: &mut Option<Instant>,
        repair_budget: &mut RepairBudget,
        outcome: &mut DigestExchangeRunOutcome,
    ) -> RepairAttemptStatus {
        let cursor_key = RepairResumeCursorKey {
            peer_endpoint: peer_endpoint.to_string(),
            ring_version,
            shard,
        };
        let control = self.repair_control_snapshot();
        if control.paused {
            outcome.repairs_skipped_paused = outcome.repairs_skipped_paused.saturating_add(1);
            return RepairAttemptStatus::SkippedPaused;
        }
        let now_ms = unix_timestamp_millis();
        if let Some(backoff_until_ms) = self.repair_failure_backoff_until_ms(&cursor_key) {
            if now_ms < backoff_until_ms {
                outcome.repairs_skipped_backoff = outcome.repairs_skipped_backoff.saturating_add(1);
                return RepairAttemptStatus::SkippedBackoff;
            }
            self.clear_repair_failure_backoff(&cursor_key);
        }
        if repair_budget.exhausted() {
            outcome.repairs_skipped_budget = outcome.repairs_skipped_budget.saturating_add(1);
            return RepairAttemptStatus::SkippedBudget;
        }
        let repair_deadline = *repair_deadline.get_or_insert_with(|| {
            Instant::now()
                .checked_add(self.config.max_repair_runtime_per_tick)
                .unwrap_or_else(Instant::now)
        });
        if Instant::now() >= repair_deadline {
            outcome.repairs_skipped_time_budget =
                outcome.repairs_skipped_time_budget.saturating_add(1);
            outcome.repair_time_budget_exhausted = true;
            return RepairAttemptStatus::SkippedTimeBudget;
        }
        let cancel_generation = control.cancel_generation;

        repair_budget.remaining_mismatches = repair_budget.remaining_mismatches.saturating_sub(1);
        outcome.repairs_attempted = outcome.repairs_attempted.saturating_add(1);
        let mut next_row_offset = self.repair_resume_cursor(&cursor_key);
        if self.is_repair_cancelled(cancel_generation) {
            outcome.repairs_cancelled = outcome.repairs_cancelled.saturating_add(1);
            self.set_repair_resume_cursor(&cursor_key, next_row_offset);
            return RepairAttemptStatus::Cancelled;
        }

        loop {
            if self.is_repair_cancelled(cancel_generation) {
                outcome.repairs_cancelled = outcome.repairs_cancelled.saturating_add(1);
                self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                return RepairAttemptStatus::Cancelled;
            }
            if repair_budget.remaining_series == 0 || repair_budget.remaining_rows == 0 {
                outcome.repairs_skipped_budget = outcome.repairs_skipped_budget.saturating_add(1);
                self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                return RepairAttemptStatus::SkippedBudget;
            }
            if Instant::now() >= repair_deadline {
                outcome.repairs_skipped_time_budget =
                    outcome.repairs_skipped_time_budget.saturating_add(1);
                outcome.repair_time_budget_exhausted = true;
                self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                return RepairAttemptStatus::SkippedTimeBudget;
            }

            let backfill_request = InternalRepairBackfillRequest {
                ring_version,
                shard,
                window_start,
                window_end,
                max_series: Some(repair_budget.remaining_series),
                max_rows: Some(repair_budget.remaining_rows),
                row_offset: next_row_offset,
            };
            let backfill_response = match self
                .fetch_peer_repair_backfill(peer_endpoint, &backfill_request)
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                    outcome.last_error = Some(format!(
                        "repair backfill fetch failed from peer '{peer_endpoint}' for shard {shard}: {err}"
                    ));
                    self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                    self.arm_repair_failure_backoff(&cursor_key);
                    return RepairAttemptStatus::Failed;
                }
            };
            if backfill_response.ring_version != ring_version
                || backfill_response.shard != shard
                || backfill_response.window_start != window_start
                || backfill_response.window_end != window_end
            {
                outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                outcome.last_error = Some(format!(
                    "repair backfill response mismatch from peer '{peer_endpoint}' for shard {shard}"
                ));
                self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                self.arm_repair_failure_backoff(&cursor_key);
                return RepairAttemptStatus::Failed;
            }

            let scanned_series =
                usize::try_from(backfill_response.series_scanned).unwrap_or(usize::MAX);
            repair_budget.remaining_series = repair_budget
                .remaining_series
                .saturating_sub(scanned_series);
            outcome.repair_series_scanned = outcome
                .repair_series_scanned
                .saturating_add(backfill_response.series_scanned);
            outcome.repair_rows_scanned = outcome
                .repair_rows_scanned
                .saturating_add(backfill_response.rows_scanned);

            let mut inserted_rows_this_attempt = 0u64;
            let mut local_point_hashes_by_series: BTreeMap<(String, Vec<Label>), HashSet<u64>> =
                BTreeMap::new();
            let mut rows_to_insert = Vec::new();
            for row in backfill_response.rows {
                if self.is_repair_cancelled(cancel_generation) {
                    outcome.repairs_cancelled = outcome.repairs_cancelled.saturating_add(1);
                    self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                    return RepairAttemptStatus::Cancelled;
                }
                if repair_budget.remaining_rows == 0 {
                    break;
                }
                let row_shard = (stable_series_identity_hash(row.metric.as_str(), &row.labels)
                    % u64::from(shard_count.max(1))) as u32;
                if row_shard != shard {
                    outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                    outcome.last_error = Some(format!(
                        "repair backfill response from peer '{peer_endpoint}' contained row for shard {row_shard}, expected shard {shard}"
                    ));
                    self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                    self.arm_repair_failure_backoff(&cursor_key);
                    return RepairAttemptStatus::Failed;
                }

                let key = (row.metric.clone(), row.labels.clone());
                let local_point_hashes =
                    if let Some(existing) = local_point_hashes_by_series.get_mut(&key) {
                        existing
                    } else {
                        let local_points = match storage.select(
                            row.metric.as_str(),
                            &row.labels,
                            window_start,
                            window_end,
                        ) {
                            Ok(points) => points,
                            Err(TsinkError::NoDataPoints { .. }) => Vec::new(),
                            Err(err) => {
                                outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                                outcome.last_error = Some(format!(
                                    "repair local-select failed for shard {} metric '{}': {err}",
                                    shard, row.metric
                                ));
                                self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                                self.arm_repair_failure_backoff(&cursor_key);
                                return RepairAttemptStatus::Failed;
                            }
                        };
                        local_point_hashes_by_series
                            .entry(key)
                            .or_insert_with(|| local_points.iter().map(hash_data_point).collect())
                    };

                if !local_point_hashes.insert(hash_data_point(&row.data_point)) {
                    continue;
                }

                rows_to_insert.push(row.into_row());
                repair_budget.remaining_rows = repair_budget.remaining_rows.saturating_sub(1);
            }

            if !rows_to_insert.is_empty() {
                if let Err(err) = storage.insert_rows(&rows_to_insert) {
                    outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                    outcome.last_error = Some(format!(
                        "repair local-insert failed for shard {}: {err}",
                        shard
                    ));
                    self.set_repair_resume_cursor(&cursor_key, next_row_offset);
                    self.arm_repair_failure_backoff(&cursor_key);
                    return RepairAttemptStatus::Failed;
                }
                hotspot::record_repair_rows_inserted(shard, &rows_to_insert);
                inserted_rows_this_attempt = inserted_rows_this_attempt
                    .saturating_add(u64::try_from(rows_to_insert.len()).unwrap_or(u64::MAX));
            }

            outcome.repair_rows_inserted = outcome
                .repair_rows_inserted
                .saturating_add(inserted_rows_this_attempt);

            if !backfill_response.truncated {
                outcome.repairs_succeeded = outcome.repairs_succeeded.saturating_add(1);
                self.clear_repair_resume_cursor(&cursor_key);
                self.clear_repair_failure_backoff(&cursor_key);
                return RepairAttemptStatus::Succeeded;
            }

            let Some(response_next_row_offset) = backfill_response.next_row_offset else {
                outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                outcome.last_error = Some(format!(
                    "repair backfill response from peer '{peer_endpoint}' for shard {shard} was truncated without next_row_offset"
                ));
                self.clear_repair_resume_cursor(&cursor_key);
                self.arm_repair_failure_backoff(&cursor_key);
                return RepairAttemptStatus::Failed;
            };
            let current_row_offset = next_row_offset.unwrap_or(0);
            if response_next_row_offset <= current_row_offset {
                outcome.repairs_failed = outcome.repairs_failed.saturating_add(1);
                outcome.last_error = Some(format!(
                    "repair backfill response from peer '{peer_endpoint}' for shard {shard} returned non-advancing next_row_offset ({response_next_row_offset} <= {current_row_offset})"
                ));
                self.clear_repair_resume_cursor(&cursor_key);
                self.arm_repair_failure_backoff(&cursor_key);
                return RepairAttemptStatus::Failed;
            }
            next_row_offset = Some(response_next_row_offset);
            self.set_repair_resume_cursor(&cursor_key, next_row_offset);
        }
    }

    async fn fetch_peer_repair_backfill(
        &self,
        endpoint: &str,
        request: &InternalRepairBackfillRequest,
    ) -> Result<InternalRepairBackfillResponse, String> {
        if let Some(repair_probe) = self.repair_probe.as_ref() {
            return match repair_probe(endpoint, RepairProbeRequest::Backfill(request.clone())) {
                Ok(RepairProbeResponse::Backfill(response)) => Ok(response),
                Err(err) => Err(err),
            };
        }
        self.rpc_client
            .repair_backfill(endpoint, request)
            .await
            .map_err(|err| err.to_string())
    }

    fn is_repair_cancelled(&self, expected_cancel_generation: u64) -> bool {
        self.repair_control_snapshot().cancel_generation != expected_cancel_generation
    }

    fn repair_resume_cursor(&self, key: &RepairResumeCursorKey) -> Option<u64> {
        let cursors = self
            .repair_resume_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cursors.get(key).copied()
    }

    fn set_repair_resume_cursor(&self, key: &RepairResumeCursorKey, row_offset: Option<u64>) {
        let mut cursors = self
            .repair_resume_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(row_offset) = row_offset {
            cursors.insert(key.clone(), row_offset);
        } else {
            cursors.remove(key);
        }
    }

    fn clear_repair_resume_cursor(&self, key: &RepairResumeCursorKey) {
        self.set_repair_resume_cursor(key, None);
    }

    fn repair_failure_backoff_until_ms(&self, key: &RepairResumeCursorKey) -> Option<u64> {
        let backoffs = self
            .repair_failure_backoff_until_ms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        backoffs.get(key).copied()
    }

    fn set_repair_failure_backoff_until_ms(
        &self,
        key: &RepairResumeCursorKey,
        backoff_until_ms: Option<u64>,
    ) {
        let mut backoffs = self
            .repair_failure_backoff_until_ms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(backoff_until_ms) = backoff_until_ms {
            backoffs.insert(key.clone(), backoff_until_ms);
        } else {
            backoffs.remove(key);
        }
    }

    fn arm_repair_failure_backoff(&self, key: &RepairResumeCursorKey) {
        let backoff_ms =
            u64::try_from(self.config.repair_failure_backoff.as_millis()).unwrap_or(u64::MAX);
        if backoff_ms == 0 {
            self.clear_repair_failure_backoff(key);
            return;
        }
        let backoff_until_ms = unix_timestamp_millis().saturating_add(backoff_ms);
        self.set_repair_failure_backoff_until_ms(key, Some(backoff_until_ms));
    }

    fn clear_repair_failure_backoff(&self, key: &RepairResumeCursorKey) {
        self.set_repair_failure_backoff_until_ms(key, None);
    }

    fn prune_repair_state_for_ring(&self, ring_version: u64) {
        let mut cursors = self
            .repair_resume_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cursors.retain(|key, _| key.ring_version == ring_version);
        let mut backoffs = self
            .repair_failure_backoff_until_ms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        backoffs.retain(|key, _| key.ring_version == ring_version);
    }
}

fn estimated_json_bytes<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_vec(value)
        .map(|body| body.len())
        .unwrap_or(0)
}

fn estimated_digest_response_upper_bound(request: &InternalDigestWindowRequest) -> usize {
    estimated_json_bytes(&InternalDigestWindowResponse {
        shard: request.shard,
        ring_version: request.ring_version,
        window_start: request.window_start,
        window_end: request.window_end,
        series_count: u64::MAX,
        point_count: u64::MAX,
        fingerprint: u64::MAX,
    })
}

fn digest_budget_would_be_exceeded(
    current_bytes: u64,
    additional_bytes: usize,
    budget: u64,
) -> bool {
    current_bytes.saturating_add(u64::try_from(additional_bytes).unwrap_or(u64::MAX)) > budget
}

fn elastic_rebalance_command_label(command: &InternalControlCommand) -> &'static str {
    match command {
        InternalControlCommand::BeginShardHandoff { .. } => "begin_shard_handoff",
        InternalControlCommand::ActivateNode { .. } => "activate_node",
        InternalControlCommand::RemoveNode { .. } => "remove_node",
        InternalControlCommand::SetLeader { .. }
        | InternalControlCommand::JoinNode { .. }
        | InternalControlCommand::LeaveNode { .. }
        | InternalControlCommand::RecommissionNode { .. }
        | InternalControlCommand::UpdateShardHandoff { .. }
        | InternalControlCommand::CompleteShardHandoff { .. } => "membership_command",
    }
}

fn compute_rebalance_slo_guard(max_rows_per_tick: usize) -> RebalanceSloGuardSnapshot {
    let write_metrics = admission::write_admission_metrics_snapshot();
    let write_guardrails = admission::global_public_write_admission()
        .ok()
        .map(|controller| controller.guardrails());
    let read_metrics = admission::read_admission_metrics_snapshot();
    let read_guardrails = admission::global_public_read_admission()
        .ok()
        .map(|controller| controller.guardrails());
    let fanout_metrics = crate::cluster::query::read_fanout_metrics_snapshot();

    let write_pressure_ratio = write_guardrails
        .map(|guardrails| {
            ratio_u64(
                write_metrics.active_requests,
                guardrails.max_inflight_requests,
            )
            .max(ratio_u64(
                write_metrics.active_rows,
                guardrails.max_inflight_rows,
            ))
        })
        .unwrap_or(0.0);
    let query_pressure_ratio = read_guardrails
        .map(|guardrails| {
            ratio_u64(
                read_metrics.active_requests,
                guardrails.max_inflight_requests,
            )
            .max(ratio_u64(
                read_metrics.active_queries,
                guardrails.max_inflight_queries,
            ))
        })
        .unwrap_or(0.0);
    let cluster_query_pressure_ratio = ratio_u64(
        fanout_metrics.resource_active_queries,
        crate::cluster::query::DEFAULT_READ_MAX_INFLIGHT_QUERIES,
    )
    .max(ratio_u64(
        fanout_metrics.resource_active_merged_points,
        crate::cluster::query::DEFAULT_READ_MAX_INFLIGHT_MERGED_POINTS,
    ));

    let dominant_pressure = write_pressure_ratio
        .max(query_pressure_ratio)
        .max(cluster_query_pressure_ratio);
    let effective_max_rows_per_tick = if dominant_pressure >= 0.90 {
        0
    } else if dominant_pressure >= 0.75 {
        (max_rows_per_tick / 4).max(1)
    } else if dominant_pressure >= 0.50 {
        (max_rows_per_tick / 2).max(1)
    } else {
        max_rows_per_tick
    };
    let block_new_handoffs = dominant_pressure >= 0.85;
    let reason = if !block_new_handoffs {
        None
    } else if write_pressure_ratio >= query_pressure_ratio
        && write_pressure_ratio >= cluster_query_pressure_ratio
    {
        Some(format!(
            "rebalance SLO guard blocked a new handoff because public write pressure is {:.2}x capacity",
            write_pressure_ratio
        ))
    } else if query_pressure_ratio >= cluster_query_pressure_ratio {
        Some(format!(
            "rebalance SLO guard blocked a new handoff because public read pressure is {:.2}x capacity",
            query_pressure_ratio
        ))
    } else {
        Some(format!(
            "rebalance SLO guard blocked a new handoff because cluster query fanout pressure is {:.2}x capacity",
            cluster_query_pressure_ratio
        ))
    };

    RebalanceSloGuardSnapshot {
        write_pressure_ratio,
        query_pressure_ratio,
        cluster_query_pressure_ratio,
        effective_max_rows_per_tick,
        block_new_handoffs,
        reason,
    }
}

fn collect_rebalance_move_candidates(
    local_node_id: &str,
    state: &ControlState,
) -> Vec<RebalanceMoveCandidateSnapshot> {
    let tracker = hotspot::hotspot_tracker_snapshot();
    collect_rebalance_move_candidates_with_tracker(local_node_id, state, &tracker)
}

fn collect_rebalance_move_candidates_with_tracker(
    local_node_id: &str,
    state: &ControlState,
    tracker: &hotspot::HotspotTrackerSnapshot,
) -> Vec<RebalanceMoveCandidateSnapshot> {
    let current_ring = match ShardRing::from_snapshot(
        state.effective_ring_snapshot_at_ring_version(state.ring_version.max(1)),
    ) {
        Ok(ring) => ring,
        Err(_) => return Vec::new(),
    };
    let Some(desired_membership) = desired_membership_from_control_state(local_node_id, state)
    else {
        return Vec::new();
    };
    let desired_ring = match ShardRing::build(
        state.ring.shard_count,
        state.ring.replication_factor,
        &desired_membership,
    ) {
        Ok(ring) => ring,
        Err(_) => return Vec::new(),
    };

    let leaving_nodes = state
        .nodes
        .iter()
        .filter(|node| node.status == ControlNodeStatus::Leaving)
        .map(|node| node.id.as_str())
        .collect::<HashSet<_>>();

    let shard_pressure = tracker
        .shards
        .iter()
        .map(|shard| {
            (
                shard.shard,
                (
                    tracked_shard_pressure_score(shard, tracker),
                    tracked_shard_movement_cost(shard, tracker),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut node_pressure = BTreeMap::<String, f64>::new();
    for shard in 0..current_ring.shard_count() {
        let pressure = shard_pressure
            .get(&shard)
            .map(|(score, _)| *score)
            .unwrap_or(0.0);
        if pressure == 0.0 {
            continue;
        }
        if let Some(owners) = current_ring.owners_for_shard(shard) {
            for owner in owners {
                let entry = node_pressure.entry(owner.clone()).or_insert(0.0);
                *entry += pressure;
            }
        }
    }
    for node in &state.nodes {
        node_pressure.entry(node.id.clone()).or_insert(0.0);
    }
    let average_node_pressure = if node_pressure.is_empty() {
        0.0
    } else {
        node_pressure.values().sum::<f64>() / node_pressure.len() as f64
    };

    let mut candidates = Vec::new();
    for shard in 0..state.ring.shard_count {
        let current_owners = current_ring.owners_for_shard(shard).unwrap_or(&[]);
        let desired_owners = desired_ring.owners_for_shard(shard).unwrap_or(&[]);
        if ownership_sets_match(current_owners, desired_owners) {
            continue;
        }

        let from_owner = current_owners
            .iter()
            .find(|owner| !desired_owners.contains(owner) && leaving_nodes.contains(owner.as_str()))
            .or_else(|| {
                current_owners
                    .iter()
                    .find(|owner| !desired_owners.contains(owner))
            })
            .cloned();
        let to_owner = desired_owners
            .iter()
            .find(|owner| !current_owners.contains(owner))
            .cloned();
        let (Some(from_node_id), Some(to_node_id)) = (from_owner, to_owner) else {
            continue;
        };

        let (pressure_score, movement_cost_score) =
            shard_pressure.get(&shard).copied().unwrap_or((0.0, 0.0));
        let source_node_pressure = node_pressure.get(&from_node_id).copied().unwrap_or(0.0);
        let target_node_pressure = node_pressure.get(&to_node_id).copied().unwrap_or(0.0);
        let before = deviation_from_average(source_node_pressure, average_node_pressure)
            + deviation_from_average(target_node_pressure, average_node_pressure);
        let after = deviation_from_average(
            (source_node_pressure - pressure_score).max(0.0),
            average_node_pressure,
        ) + deviation_from_average(
            target_node_pressure + pressure_score,
            average_node_pressure,
        );
        let imbalance_improvement_score = (before - after).max(0.0);
        let leaving_bonus = if leaving_nodes.contains(from_node_id.as_str()) {
            1_000.0
        } else {
            0.0
        };
        let underloaded_bonus = if target_node_pressure < average_node_pressure {
            5.0
        } else {
            0.0
        };
        let decision_score = leaving_bonus + imbalance_improvement_score * 10.0 + underloaded_bonus
            - movement_cost_score;
        let reason = if leaving_nodes.contains(from_node_id.as_str()) {
            "drain_leaving_node".to_string()
        } else if imbalance_improvement_score > movement_cost_score {
            "reduce_hot_source_pressure".to_string()
        } else {
            "lowest_cost_membership_move".to_string()
        };

        candidates.push(RebalanceMoveCandidateSnapshot {
            shard,
            from_node_id,
            to_node_id,
            pressure_score,
            movement_cost_score,
            imbalance_improvement_score,
            decision_score,
            source_node_pressure,
            target_node_pressure,
            reason,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .decision_score
            .total_cmp(&left.decision_score)
            .then_with(|| right.pressure_score.total_cmp(&left.pressure_score))
            .then_with(|| left.shard.cmp(&right.shard))
    });
    candidates.truncate(8);
    candidates
}

fn plan_elastic_membership_command(
    local_node_id: &str,
    state: &ControlState,
    candidate_moves: &[RebalanceMoveCandidateSnapshot],
) -> Result<Option<InternalControlCommand>, String> {
    let current_ring = ShardRing::from_snapshot(
        state.effective_ring_snapshot_at_ring_version(state.ring_version.max(1)),
    )
    .map_err(|err| format!("failed to restore current ring for elastic rebalance: {err}"))?;

    for node in &state.nodes {
        if node.status == ControlNodeStatus::Leaving
            && !node_has_current_ring_ownership(&current_ring, node.id.as_str())
            && !node_has_active_handoff(state, node.id.as_str())
        {
            return Ok(Some(InternalControlCommand::RemoveNode {
                node_id: node.id.clone(),
            }));
        }
    }

    let Some(desired_membership) = desired_membership_from_control_state(local_node_id, state)
    else {
        return Ok(None);
    };
    let desired_ring = ShardRing::build(
        state.ring.shard_count,
        state.ring.replication_factor,
        &desired_membership,
    )
    .map_err(|err| format!("failed to build desired ring for elastic rebalance: {err}"))?;

    if let Some(candidate) = candidate_moves.first() {
        return Ok(Some(InternalControlCommand::BeginShardHandoff {
            shard: candidate.shard,
            from_node_id: candidate.from_node_id.clone(),
            to_node_id: candidate.to_node_id.clone(),
            activation_ring_version: state.ring_version.saturating_add(1),
        }));
    }

    for shard in 0..state.ring.shard_count {
        let current_owners = current_ring.owners_for_shard(shard).unwrap_or(&[]);
        let desired_owners = desired_ring.owners_for_shard(shard).unwrap_or(&[]);
        if !ownership_sets_match(current_owners, desired_owners) {
            let from_node_id = current_owners.first().cloned().unwrap_or_default();
            let to_node_id = desired_owners.first().cloned().unwrap_or_default();
            if !from_node_id.is_empty() && !to_node_id.is_empty() && from_node_id != to_node_id {
                return Ok(Some(InternalControlCommand::BeginShardHandoff {
                    shard,
                    from_node_id,
                    to_node_id,
                    activation_ring_version: state.ring_version.saturating_add(1),
                }));
            }
        }
    }

    for node in &state.nodes {
        if node.status == ControlNodeStatus::Joining
            && node_membership_balanced(node.id.as_str(), &current_ring, &desired_ring)
        {
            return Ok(Some(InternalControlCommand::ActivateNode {
                node_id: node.id.clone(),
            }));
        }
    }

    Ok(None)
}

fn tracked_shard_pressure_score(
    shard: &hotspot::HotspotShardCountersSnapshot,
    snapshot: &hotspot::HotspotTrackerSnapshot,
) -> f64 {
    let total_ingest = snapshot
        .shards
        .iter()
        .map(|item| item.ingest_rows_total)
        .sum::<u64>();
    let total_query = snapshot
        .shards
        .iter()
        .map(|item| item.query_shard_hits_total)
        .sum::<u64>();
    let total_repair = snapshot
        .shards
        .iter()
        .map(|item| {
            item.repair_point_gap_total
                .saturating_add(item.repair_rows_inserted_total)
        })
        .sum::<u64>();
    let shard_slots = snapshot.shards.len().max(1);
    let ingest_ratio = ratio_counter(shard.ingest_rows_total, total_ingest, shard_slots);
    let query_ratio = ratio_counter(shard.query_shard_hits_total, total_query, shard_slots);
    let repair_ratio = ratio_counter(
        shard
            .repair_point_gap_total
            .saturating_add(shard.repair_rows_inserted_total),
        total_repair,
        shard_slots,
    );
    ingest_ratio * 3.0 + query_ratio * 2.5 + repair_ratio * 2.5
}

fn tracked_shard_movement_cost(
    shard: &hotspot::HotspotShardCountersSnapshot,
    snapshot: &hotspot::HotspotTrackerSnapshot,
) -> f64 {
    let total_ingest = snapshot
        .shards
        .iter()
        .map(|item| item.ingest_rows_total)
        .sum::<u64>();
    let total_query = snapshot
        .shards
        .iter()
        .map(|item| item.query_shard_hits_total)
        .sum::<u64>();
    let total_repair = snapshot
        .shards
        .iter()
        .map(|item| {
            item.repair_point_gap_total
                .saturating_add(item.repair_rows_inserted_total)
        })
        .sum::<u64>();
    let shard_slots = snapshot.shards.len().max(1);
    let ingest_ratio = ratio_counter(shard.ingest_rows_total, total_ingest, shard_slots);
    let query_ratio = ratio_counter(shard.query_shard_hits_total, total_query, shard_slots);
    let repair_ratio = ratio_counter(
        shard
            .repair_point_gap_total
            .saturating_add(shard.repair_rows_inserted_total),
        total_repair,
        shard_slots,
    );
    ingest_ratio * 2.5 + query_ratio * 2.0 + repair_ratio * 1.5
}

fn ratio_counter(value: u64, total: u64, slots: usize) -> f64 {
    if value == 0 || total == 0 || slots == 0 {
        return 0.0;
    }
    let average = total as f64 / slots as f64;
    if average <= 0.0 {
        0.0
    } else {
        value as f64 / average
    }
}

fn ratio_u64(value: u64, limit: usize) -> f64 {
    if value == 0 || limit == 0 {
        return 0.0;
    }
    value as f64 / limit as f64
}

fn deviation_from_average(value: f64, average: f64) -> f64 {
    (value - average).abs()
}

fn desired_membership_from_control_state(
    local_node_id: &str,
    state: &ControlState,
) -> Option<MembershipView> {
    let mut nodes = state
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                node.status,
                ControlNodeStatus::Active | ControlNodeStatus::Joining
            )
        })
        .map(|node| ClusterNode {
            id: node.id.clone(),
            endpoint: node.endpoint.clone(),
        })
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        return None;
    }
    nodes.sort();
    Some(MembershipView {
        local_node_id: local_node_id.to_string(),
        nodes,
    })
}

fn ownership_sets_match(current_owners: &[String], desired_owners: &[String]) -> bool {
    let current = current_owners
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let desired = desired_owners
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    current == desired
}

fn node_has_current_ring_ownership(ring: &ShardRing, node_id: &str) -> bool {
    (0..ring.shard_count()).any(|shard| {
        ring.owners_for_shard(shard)
            .is_some_and(|owners| owners.iter().any(|owner| owner == node_id))
    })
}

fn node_has_active_handoff(state: &ControlState, node_id: &str) -> bool {
    state.transitions.iter().any(|transition| {
        transition.handoff.phase.is_active()
            && (transition.from_node_id == node_id || transition.to_node_id == node_id)
    })
}

fn node_membership_balanced(
    node_id: &str,
    current_ring: &ShardRing,
    desired_ring: &ShardRing,
) -> bool {
    (0..current_ring.shard_count()).all(|shard| {
        let current_has = current_ring
            .owners_for_shard(shard)
            .is_some_and(|owners| owners.iter().any(|owner| owner == node_id));
        let desired_has = desired_ring
            .owners_for_shard(shard)
            .is_some_and(|owners| owners.iter().any(|owner| owner == node_id));
        current_has == desired_has
    })
}

fn prioritized_handoff_shards(state: &ControlState) -> Vec<u32> {
    let mut transitions = state
        .transitions
        .iter()
        .filter(|transition| transition.handoff.phase.is_active())
        .map(|transition| {
            (
                handoff_phase_priority(transition.handoff.phase),
                transition.shard,
            )
        })
        .collect::<Vec<_>>();
    transitions.sort_unstable();
    transitions.into_iter().map(|(_, shard)| shard).collect()
}

fn handoff_phase_priority(phase: ShardHandoffPhase) -> u8 {
    match phase {
        ShardHandoffPhase::FinalSync => 0,
        ShardHandoffPhase::Cutover => 1,
        ShardHandoffPhase::Warmup => 2,
        ShardHandoffPhase::Completed => 3,
        ShardHandoffPhase::Failed => 4,
    }
}

fn transition_stale_bridge_ring_version(activation_ring_version: u64) -> u64 {
    activation_ring_version.saturating_sub(1).max(1)
}

async fn compute_shard_window_digest_async(
    storage: Arc<dyn Storage>,
    shard: u32,
    shard_count: u32,
    ring_version: u64,
    window_start: i64,
    window_end: i64,
) -> Result<InternalDigestWindowResponse, String> {
    let task = tokio::task::spawn_blocking(move || {
        compute_shard_window_digest(
            storage.as_ref(),
            shard,
            shard_count,
            ring_version,
            window_start,
            window_end,
        )
    });
    task.await
        .map_err(|err| format!("digest compute task failed: {err}"))?
}

pub fn compute_shard_window_digest(
    storage: &dyn Storage,
    shard: u32,
    shard_count: u32,
    ring_version: u64,
    window_start: i64,
    window_end: i64,
) -> Result<InternalDigestWindowResponse, String> {
    let digest = storage
        .compute_shard_window_digest(shard, shard_count, window_start, window_end)
        .map_err(|err| format!("storage shard-window digest failed: {err}"))?;

    Ok(InternalDigestWindowResponse {
        shard: digest.shard,
        ring_version: ring_version.max(1),
        window_start: digest.window_start,
        window_end: digest.window_end,
        series_count: digest.series_count,
        point_count: digest.point_count,
        fingerprint: digest.fingerprint,
    })
}

fn hash_data_point(point: &DataPoint) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    fnv1a_update(&mut hash, &point.timestamp.to_le_bytes());
    match serde_json::to_vec(&point.value) {
        Ok(encoded) => fnv1a_update(&mut hash, &encoded),
        Err(_) => fnv1a_update(&mut hash, format!("{:?}", point.value).as_bytes()),
    }
    hash
}

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn parse_env_u64(var: &str, default: u64, enforce_positive: bool) -> Result<u64, String> {
    let value = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{var} must be an integer, got '{value}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_env_usize(var: &str, default: usize, enforce_positive: bool) -> Result<usize, String> {
    let value = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("{var} must be an integer, got '{value}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

fn unix_timestamp_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::consensus::ControlConsensusConfig;
    use crate::cluster::control::{ControlState, ControlStateStore};
    use crate::cluster::membership::MembershipView;
    use crate::cluster::ring::ShardRing;
    use crate::cluster::rpc::{RpcClientConfig, INTERNAL_RPC_PROTOCOL_VERSION};
    use std::collections::BTreeSet;
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;
    use tsink::{Row, StorageBuilder, TimestampPrecision};

    const TEST_DIGEST_SHARD_COUNT: u32 = 4;

    fn build_test_storage(shard_count: u32) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(shard_count)
            .build()
            .expect("storage should build")
    }

    #[test]
    fn compute_shard_window_digest_is_deterministic() {
        let shard_count = 16;
        let storage = build_test_storage(shard_count);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "digest_metric",
                    vec![Label::new("job", "a")],
                    DataPoint::new(1_700_000_000_001, 1.0),
                ),
                Row::with_labels(
                    "digest_metric",
                    vec![Label::new("job", "a")],
                    DataPoint::new(1_700_000_000_002, 2.0),
                ),
                Row::with_labels(
                    "digest_metric",
                    vec![Label::new("job", "b")],
                    DataPoint::new(1_700_000_000_003, 3.0),
                ),
            ])
            .expect("seed rows should insert");

        let shard = (stable_series_identity_hash("digest_metric", &[Label::new("job", "a")])
            % u64::from(shard_count)) as u32;

        let digest_a = compute_shard_window_digest(
            storage.as_ref(),
            shard,
            shard_count,
            1,
            1_700_000_000_000,
            1_700_000_001_000,
        )
        .expect("digest should compute");
        let digest_b = compute_shard_window_digest(
            storage.as_ref(),
            shard,
            shard_count,
            1,
            1_700_000_000_000,
            1_700_000_001_000,
        )
        .expect("digest should compute");
        assert_eq!(digest_a, digest_b);
    }

    #[test]
    fn digest_config_from_env_rejects_invalid_values() {
        let interval_key = DIGEST_INTERVAL_SECS_ENV;
        let interval_previous = std::env::var(interval_key).ok();
        std::env::set_var(interval_key, "0");
        let err = DigestExchangeConfig::from_env().expect_err("zero interval should fail");
        assert!(err.contains("must be greater than zero"));
        match interval_previous {
            Some(value) => std::env::set_var(interval_key, value),
            None => std::env::remove_var(interval_key),
        }

        let bytes_key = DIGEST_MAX_BYTES_PER_TICK_ENV;
        let bytes_previous = std::env::var(bytes_key).ok();
        std::env::set_var(bytes_key, "0");
        let err = DigestExchangeConfig::from_env().expect_err("zero byte budget should fail");
        assert!(err.contains("must be greater than zero"));
        match bytes_previous {
            Some(value) => std::env::set_var(bytes_key, value),
            None => std::env::remove_var(bytes_key),
        }

        let runtime_key = REPAIR_MAX_RUNTIME_MS_PER_TICK_ENV;
        let runtime_previous = std::env::var(runtime_key).ok();
        std::env::set_var(runtime_key, "0");
        let err = DigestExchangeConfig::from_env().expect_err("zero repair runtime should fail");
        assert!(err.contains("must be greater than zero"));
        match runtime_previous {
            Some(value) => std::env::set_var(runtime_key, value),
            None => std::env::remove_var(runtime_key),
        }

        let rebalance_rows_key = REBALANCE_MAX_ROWS_PER_TICK_ENV;
        let rebalance_rows_previous = std::env::var(rebalance_rows_key).ok();
        std::env::set_var(rebalance_rows_key, "0");
        let err =
            DigestExchangeConfig::from_env().expect_err("zero rebalance rows per tick should fail");
        assert!(err.contains("must be greater than zero"));
        match rebalance_rows_previous {
            Some(value) => std::env::set_var(rebalance_rows_key, value),
            None => std::env::remove_var(rebalance_rows_key),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_records_digest_mismatch_from_peer_response() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let observed_shards = Arc::new(StdMutex::new(Vec::new()));
        let observed_shards_probe = Arc::clone(&observed_shards);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            observed_shards_probe
                .lock()
                .expect("lock")
                .push(request.shard);
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 1,
                point_count: 99,
                fingerprint: 42,
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, request| match request {
            RepairProbeRequest::Backfill(request) => Ok(RepairProbeResponse::Backfill(
                InternalRepairBackfillResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_scanned: 0,
                    rows_scanned: 0,
                    truncated: false,
                    next_row_offset: None,
                    rows: Vec::new(),
                },
            )),
        }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        storage
            .insert_rows(&[Row::with_labels(
                "digest_metric",
                vec![Label::new("job", "local")],
                DataPoint::new(1_700_000_000_100, 1.0),
            )])
            .expect("seed row should insert");

        runtime.run_once(storage.clone()).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.runs_total, 1);
        assert_eq!(snapshot.windows_compared_total, 1);
        assert_eq!(snapshot.windows_success_total, 1);
        assert_eq!(snapshot.windows_failed_total, 0);
        assert_eq!(snapshot.mismatches_total, 1);
        assert_eq!(snapshot.mismatches.len(), 1);
        assert_eq!(snapshot.compared_shards_last_run, 1);
        assert_eq!(snapshot.prioritized_shards_last_run, 0);
        assert_eq!(snapshot.compared_peers_last_run, 1);
        assert!(snapshot.last_error.is_none());
        assert_eq!(observed_shards.lock().expect("lock").len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_rotates_sampled_shards_and_caps_retained_mismatch_reports() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let observed_shards = Arc::new(StdMutex::new(Vec::new()));
        let observed_shards_probe = Arc::clone(&observed_shards);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 2,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            observed_shards_probe
                .lock()
                .expect("lock")
                .push(request.shard);
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 0,
                point_count: 0,
                fingerprint: 1,
            })
        }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage.clone()).await;
        runtime.run_once(storage.clone()).await;
        runtime.run_once(storage.clone()).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.runs_total, 3);
        assert_eq!(snapshot.windows_compared_total, 3);
        assert_eq!(snapshot.windows_success_total, 3);
        assert_eq!(snapshot.mismatches_total, 3);
        assert_eq!(snapshot.mismatches.len(), 2);

        let observed_shards = observed_shards.lock().expect("lock").clone();
        assert_eq!(observed_shards.len(), 3);
        assert_ne!(observed_shards[0], observed_shards[1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_bootstraps_handoff_target_shard_during_warmup() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut transition_context = None::<(u32, String, u64)>;
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 4,
                max_repair_series_per_tick: 16,
                max_repair_rows_per_tick: 16,
                ..DigestExchangeConfig::default()
            },
            |state| {
                let current_ring_version = state.ring_version.max(1);
                let (transition_shard, source_node_id) = (0..state.ring.shard_count.max(1))
                    .find_map(|shard| {
                        let owners =
                            state.owners_for_shard_at_ring_version(shard, current_ring_version);
                        if owners.iter().any(|owner| owner == "node-a") {
                            return None;
                        }
                        owners
                            .iter()
                            .find(|owner| owner.as_str() != "node-a")
                            .cloned()
                            .map(|owner| (shard, owner))
                    })
                    .expect("expected shard owned by remote source node");
                state
                    .apply_begin_shard_handoff(
                        transition_shard,
                        source_node_id.as_str(),
                        "node-a",
                        current_ring_version.saturating_add(1),
                    )
                    .expect("handoff begin should apply");
                transition_context = Some((transition_shard, source_node_id, current_ring_version));
            },
        );
        let (transition_shard, source_node_id, source_ring_version) =
            transition_context.expect("transition context should be captured");
        let state = runtime.control_consensus.current_state();
        let source_endpoint = state
            .node_record(&source_node_id)
            .map(|node| node.endpoint.clone())
            .expect("source endpoint should exist");
        let shard_count = state.ring.shard_count.max(1);
        let metric = "handoff_bootstrap_warmup_metric";
        let labels = vec![
            find_label_for_shard(metric, "job", transition_shard, shard_count)
                .expect("label should map to transition shard"),
        ];

        let observed_digest = Arc::new(StdMutex::new(Vec::<(String, u64, u32)>::new()));
        let observed_digest_probe = Arc::clone(&observed_digest);
        let observed_repair = Arc::new(StdMutex::new(Vec::<(String, u64, u32)>::new()));
        let observed_repair_probe = Arc::clone(&observed_repair);
        let source_endpoint_for_digest = source_endpoint.clone();
        let source_endpoint_for_repair = source_endpoint.clone();
        let labels_for_repair = labels.clone();
        let metric_for_repair = metric.to_string();
        let runtime = runtime
            .with_digest_probe(Arc::new(move |endpoint, request| {
                observed_digest_probe.lock().expect("lock").push((
                    endpoint.to_string(),
                    request.ring_version,
                    request.shard,
                ));
                let (series_count, point_count, fingerprint) =
                    if endpoint == source_endpoint_for_digest {
                        (1, 1, FNV_OFFSET_BASIS.wrapping_add(1))
                    } else {
                        (0, 0, FNV_OFFSET_BASIS)
                    };
                Ok(InternalDigestWindowResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_count,
                    point_count,
                    fingerprint,
                })
            }))
            .with_repair_probe(Arc::new(move |endpoint, request| match request {
                RepairProbeRequest::Backfill(request) => {
                    observed_repair_probe.lock().expect("lock").push((
                        endpoint.to_string(),
                        request.ring_version,
                        request.shard,
                    ));
                    let rows = if endpoint == source_endpoint_for_repair {
                        vec![crate::cluster::rpc::InternalRow {
                            metric: metric_for_repair.clone(),
                            labels: labels_for_repair.clone(),
                            data_point: DataPoint::new(1_700_000_000_175, 17.5),
                        }]
                    } else {
                        Vec::new()
                    };
                    Ok(RepairProbeResponse::Backfill(
                        InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: if rows.is_empty() { 0 } else { 1 },
                            rows_scanned: u64::try_from(rows.len()).unwrap_or(u64::MAX),
                            truncated: false,
                            next_row_offset: None,
                            rows,
                        },
                    ))
                }
            }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage.clone()).await;

        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.repairs_attempted_total, 1);
        assert_eq!(snapshot.repairs_succeeded_total, 1);
        assert_eq!(snapshot.repairs_failed_total, 0);
        assert_eq!(snapshot.repair_rows_inserted_total, 1);

        let digest_calls = observed_digest.lock().expect("lock").clone();
        assert!(digest_calls.iter().any(|(endpoint, ring, shard)| {
            endpoint == &source_endpoint
                && *ring == source_ring_version
                && *shard == transition_shard
        }));
        let repair_calls = observed_repair.lock().expect("lock").clone();
        assert!(repair_calls.iter().any(|(endpoint, ring, shard)| {
            endpoint == &source_endpoint
                && *ring == source_ring_version
                && *shard == transition_shard
        }));

        let points = storage
            .select(metric, &labels, 1_700_000_000_000, 1_700_000_001_000)
            .expect("select should succeed");
        assert_eq!(points.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_queries_transition_source_at_stale_ring_after_cutover() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut transition_context = None::<(u32, String, u64)>;
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 8,
                max_repair_series_per_tick: 16,
                max_repair_rows_per_tick: 16,
                ..DigestExchangeConfig::default()
            },
            |state| {
                let current_ring_version = state.ring_version.max(1);
                let (transition_shard, source_node_id) = (0..state.ring.shard_count.max(1))
                    .find_map(|shard| {
                        let owners =
                            state.owners_for_shard_at_ring_version(shard, current_ring_version);
                        if owners.iter().any(|owner| owner == "node-a") {
                            return None;
                        }
                        owners
                            .iter()
                            .find(|owner| owner.as_str() != "node-a")
                            .cloned()
                            .map(|owner| (shard, owner))
                    })
                    .expect("expected shard owned by remote source node");
                let activation_ring_version = current_ring_version.saturating_add(1);
                state
                    .apply_begin_shard_handoff(
                        transition_shard,
                        source_node_id.as_str(),
                        "node-a",
                        activation_ring_version,
                    )
                    .expect("handoff begin should apply");
                state
                    .apply_shard_handoff_progress(
                        transition_shard,
                        ShardHandoffPhase::Cutover,
                        None,
                        None,
                        None,
                    )
                    .expect("cutover should apply");
                transition_context = Some((
                    transition_shard,
                    source_node_id,
                    transition_stale_bridge_ring_version(activation_ring_version),
                ));
            },
        );
        let (transition_shard, source_node_id, stale_source_ring_version) =
            transition_context.expect("transition context should be captured");
        let state = runtime.control_consensus.current_state();
        let source_endpoint = state
            .node_record(&source_node_id)
            .map(|node| node.endpoint.clone())
            .expect("source endpoint should exist");
        let shard_count = state.ring.shard_count.max(1);
        let metric = "handoff_bootstrap_cutover_metric";
        let labels = vec![
            find_label_for_shard(metric, "job", transition_shard, shard_count)
                .expect("label should map to transition shard"),
        ];

        let observed_digest = Arc::new(StdMutex::new(Vec::<(String, u64, u32)>::new()));
        let observed_digest_probe = Arc::clone(&observed_digest);
        let observed_repair = Arc::new(StdMutex::new(Vec::<(String, u64, u32)>::new()));
        let observed_repair_probe = Arc::clone(&observed_repair);
        let source_endpoint_for_digest = source_endpoint.clone();
        let source_endpoint_for_repair = source_endpoint.clone();
        let labels_for_repair = labels.clone();
        let metric_for_repair = metric.to_string();
        let runtime = runtime
            .with_digest_probe(Arc::new(move |endpoint, request| {
                observed_digest_probe.lock().expect("lock").push((
                    endpoint.to_string(),
                    request.ring_version,
                    request.shard,
                ));
                let (series_count, point_count, fingerprint) =
                    if endpoint == source_endpoint_for_digest {
                        (1, 1, FNV_OFFSET_BASIS.wrapping_add(2))
                    } else {
                        (0, 0, FNV_OFFSET_BASIS)
                    };
                Ok(InternalDigestWindowResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_count,
                    point_count,
                    fingerprint,
                })
            }))
            .with_repair_probe(Arc::new(move |endpoint, request| match request {
                RepairProbeRequest::Backfill(request) => {
                    observed_repair_probe.lock().expect("lock").push((
                        endpoint.to_string(),
                        request.ring_version,
                        request.shard,
                    ));
                    let rows = if endpoint == source_endpoint_for_repair {
                        vec![crate::cluster::rpc::InternalRow {
                            metric: metric_for_repair.clone(),
                            labels: labels_for_repair.clone(),
                            data_point: DataPoint::new(1_700_000_000_225, 22.5),
                        }]
                    } else {
                        Vec::new()
                    };
                    Ok(RepairProbeResponse::Backfill(
                        InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: if rows.is_empty() { 0 } else { 1 },
                            rows_scanned: u64::try_from(rows.len()).unwrap_or(u64::MAX),
                            truncated: false,
                            next_row_offset: None,
                            rows,
                        },
                    ))
                }
            }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage.clone()).await;

        let snapshot = runtime.snapshot();
        assert!(snapshot.repairs_attempted_total >= 1);
        assert!(snapshot.repairs_succeeded_total >= 1);
        assert_eq!(snapshot.repairs_failed_total, 0);
        assert!(snapshot.repair_rows_inserted_total >= 1);

        let digest_calls = observed_digest.lock().expect("lock").clone();
        assert!(digest_calls.iter().any(|(endpoint, ring, shard)| {
            endpoint == &source_endpoint
                && *ring == stale_source_ring_version
                && *shard == transition_shard
        }));
        let repair_calls = observed_repair.lock().expect("lock").clone();
        assert!(repair_calls.iter().any(|(endpoint, ring, shard)| {
            endpoint == &source_endpoint
                && *ring == stale_source_ring_version
                && *shard == transition_shard
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_backfills_missing_points_when_remote_digest_is_ahead() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let shard_count = 4u32;
        let metric = "repair_metric";
        let shard_labels = (0..shard_count)
            .map(|shard| {
                let label = find_label_for_shard(metric, "job", shard, shard_count)
                    .expect("should find label for shard");
                (shard, vec![label])
            })
            .collect::<Vec<_>>();
        let now_ts = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let existing_ts = now_ts.saturating_sub(1_000);
        let backfill_ts = now_ts.saturating_sub(500);
        let observed_shard = Arc::new(StdMutex::new(None::<u32>));
        let observed_shard_digest = Arc::clone(&observed_shard);
        let observed_shard_repair = Arc::clone(&observed_shard);
        let shard_labels_for_probe = shard_labels.clone();

        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 1,
                max_repair_series_per_tick: 8,
                max_repair_rows_per_tick: 8,
                max_repair_runtime_per_tick: Duration::from_millis(
                    DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
                ),
                repair_failure_backoff: Duration::from_secs(DEFAULT_REPAIR_FAILURE_BACKOFF_SECS),
                rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
                rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            *observed_shard_digest.lock().expect("lock") = Some(request.shard);
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 1,
                point_count: 2,
                fingerprint: 99,
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, request| match request {
            RepairProbeRequest::Backfill(request) => {
                let shard = observed_shard_repair
                    .lock()
                    .expect("lock")
                    .expect("digest probe should set compared shard");
                let labels = shard_labels_for_probe
                    .iter()
                    .find_map(|(series_shard, labels)| {
                        if *series_shard == shard {
                            Some(labels.clone())
                        } else {
                            None
                        }
                    })
                    .expect("labels for shard should exist");
                Ok(RepairProbeResponse::Backfill(
                    InternalRepairBackfillResponse {
                        shard: request.shard,
                        ring_version: request.ring_version,
                        window_start: request.window_start,
                        window_end: request.window_end,
                        series_scanned: 1,
                        rows_scanned: 2,
                        truncated: false,
                        next_row_offset: None,
                        rows: vec![
                            crate::cluster::rpc::InternalRow {
                                metric: metric.to_string(),
                                labels: labels.clone(),
                                data_point: DataPoint::new(existing_ts, 1.0),
                            },
                            crate::cluster::rpc::InternalRow {
                                metric: metric.to_string(),
                                labels,
                                data_point: DataPoint::new(backfill_ts, 2.0),
                            },
                        ],
                    },
                ))
            }
        }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        for (_, labels) in &shard_labels {
            storage
                .insert_rows(&[Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(existing_ts, 1.0),
                )])
                .expect("seed row should insert");
        }

        runtime.run_once(storage.clone()).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.repairs_attempted_total, 1);
        assert_eq!(snapshot.repairs_succeeded_total, 1);
        assert_eq!(snapshot.repairs_failed_total, 0);
        assert_eq!(snapshot.repair_rows_inserted_total, 1);

        let repaired_shard = observed_shard
            .lock()
            .expect("lock")
            .expect("run should compare one shard");
        let repaired_labels = shard_labels
            .iter()
            .find_map(|(series_shard, labels)| {
                if *series_shard == repaired_shard {
                    Some(labels.clone())
                } else {
                    None
                }
            })
            .expect("labels for repaired shard should exist");
        let repaired_points = storage
            .select(
                metric,
                &repaired_labels,
                now_ts.saturating_sub(10_000),
                now_ts.saturating_add(1),
            )
            .expect("select should succeed");
        assert_eq!(repaired_points.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_backfill_continues_truncated_pages_with_row_offset_cursor() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let shard_count = 4u32;
        let metric = "repair_metric_paged";
        let shard_labels = (0..shard_count)
            .map(|shard| {
                let label = find_label_for_shard(metric, "job", shard, shard_count)
                    .expect("should find label for shard");
                (shard, vec![label])
            })
            .collect::<Vec<_>>();
        let now_ts = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let existing_ts = now_ts.saturating_sub(1_000);
        let backfill_ts = now_ts.saturating_sub(500);
        let observed_shard = Arc::new(StdMutex::new(None::<u32>));
        let observed_shard_digest = Arc::clone(&observed_shard);
        let observed_offsets = Arc::new(StdMutex::new(Vec::<Option<u64>>::new()));
        let observed_offsets_probe = Arc::clone(&observed_offsets);
        let shard_labels_for_probe = shard_labels.clone();
        let metric_for_probe = metric.to_string();

        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 1,
                max_repair_series_per_tick: 8,
                max_repair_rows_per_tick: 8,
                max_repair_runtime_per_tick: Duration::from_millis(
                    DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
                ),
                repair_failure_backoff: Duration::from_secs(DEFAULT_REPAIR_FAILURE_BACKOFF_SECS),
                rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
                rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            *observed_shard_digest.lock().expect("lock") = Some(request.shard);
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 1,
                point_count: 2,
                fingerprint: 1234,
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, request| match request {
            RepairProbeRequest::Backfill(request) => {
                observed_offsets_probe
                    .lock()
                    .expect("lock")
                    .push(request.row_offset);
                let labels = shard_labels_for_probe
                    .iter()
                    .find_map(|(series_shard, labels)| {
                        if *series_shard == request.shard {
                            Some(labels.clone())
                        } else {
                            None
                        }
                    })
                    .expect("labels for shard should exist");
                let response = match request.row_offset.unwrap_or(0) {
                    0 => InternalRepairBackfillResponse {
                        shard: request.shard,
                        ring_version: request.ring_version,
                        window_start: request.window_start,
                        window_end: request.window_end,
                        series_scanned: 1,
                        rows_scanned: 1,
                        truncated: true,
                        next_row_offset: Some(1),
                        rows: vec![crate::cluster::rpc::InternalRow {
                            metric: metric_for_probe.clone(),
                            labels: labels.clone(),
                            data_point: DataPoint::new(existing_ts, 1.0),
                        }],
                    },
                    1 => InternalRepairBackfillResponse {
                        shard: request.shard,
                        ring_version: request.ring_version,
                        window_start: request.window_start,
                        window_end: request.window_end,
                        series_scanned: 1,
                        rows_scanned: 1,
                        truncated: false,
                        next_row_offset: None,
                        rows: vec![crate::cluster::rpc::InternalRow {
                            metric: metric_for_probe.clone(),
                            labels,
                            data_point: DataPoint::new(backfill_ts, 2.0),
                        }],
                    },
                    _ => InternalRepairBackfillResponse {
                        shard: request.shard,
                        ring_version: request.ring_version,
                        window_start: request.window_start,
                        window_end: request.window_end,
                        series_scanned: 0,
                        rows_scanned: 0,
                        truncated: false,
                        next_row_offset: None,
                        rows: Vec::new(),
                    },
                };
                Ok(RepairProbeResponse::Backfill(response))
            }
        }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        for (_, labels) in &shard_labels {
            storage
                .insert_rows(&[Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(existing_ts, 1.0),
                )])
                .expect("seed row should insert");
        }

        runtime.run_once(storage.clone()).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.repairs_attempted_total, 1);
        assert_eq!(snapshot.repairs_succeeded_total, 1);
        assert_eq!(snapshot.repairs_failed_total, 0);
        assert_eq!(snapshot.repair_rows_inserted_total, 1);

        let observed_offsets = observed_offsets.lock().expect("lock").clone();
        assert_eq!(observed_offsets, vec![None, Some(1)]);

        let repaired_shard = observed_shard
            .lock()
            .expect("lock")
            .expect("run should compare one shard");
        let repaired_labels = shard_labels
            .iter()
            .find_map(|(series_shard, labels)| {
                if *series_shard == repaired_shard {
                    Some(labels.clone())
                } else {
                    None
                }
            })
            .expect("labels for repaired shard should exist");
        let repaired_points = storage
            .select(
                metric,
                &repaired_labels,
                now_ts.saturating_sub(10_000),
                now_ts.saturating_add(1),
            )
            .expect("select should succeed");
        assert_eq!(repaired_points.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_backfill_resumes_from_saved_cursor_after_series_budget_exhaustion() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut prioritized_shard = None::<u32>;
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 1,
                max_repair_series_per_tick: 1,
                max_repair_rows_per_tick: 8,
                max_repair_runtime_per_tick: Duration::from_millis(
                    DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
                ),
                repair_failure_backoff: Duration::from_secs(DEFAULT_REPAIR_FAILURE_BACKOFF_SECS),
                rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
                rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            },
            |state| {
                let activation_ring_version = state.ring_version.saturating_add(1);
                let (transition_shard, to_node_id) = (0..state.ring.shard_count)
                    .find_map(|shard| {
                        let owners =
                            state.owners_for_shard_at_ring_version(shard, state.ring_version);
                        if !owners.iter().any(|owner| owner == "node-a") {
                            return None;
                        }
                        if !owners.iter().any(|owner| owner == "node-b") {
                            return Some((shard, "node-b"));
                        }
                        if !owners.iter().any(|owner| owner == "node-c") {
                            return Some((shard, "node-c"));
                        }
                        None
                    })
                    .expect("should find local shard with alternate transition target");
                state
                    .apply_begin_shard_handoff(
                        transition_shard,
                        "node-a",
                        to_node_id,
                        activation_ring_version,
                    )
                    .expect("handoff begin should apply");
                prioritized_shard = Some(transition_shard);
            },
        );
        let prioritized_shard = prioritized_shard.expect("prioritized shard should exist");
        let shard_count = runtime
            .control_consensus
            .current_state()
            .ring
            .shard_count
            .max(1);
        let metric = "repair_metric_resume_cursor";
        let labels = vec![
            find_label_for_shard(metric, "job", prioritized_shard, shard_count)
                .expect("label should map to prioritized shard"),
        ];
        let now_ts = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let existing_ts = now_ts.saturating_sub(2_000);
        let missing_ts = now_ts.saturating_sub(1_000);

        let observed_offsets = Arc::new(StdMutex::new(Vec::<Option<u64>>::new()));
        let observed_offsets_probe = Arc::clone(&observed_offsets);
        let metric_for_probe = metric.to_string();
        let labels_for_probe = labels.clone();
        let runtime = runtime
            .with_digest_probe(Arc::new(move |_endpoint, request| {
                Ok(InternalDigestWindowResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_count: 1,
                    point_count: 2,
                    fingerprint: FNV_OFFSET_BASIS.wrapping_add(1),
                })
            }))
            .with_repair_probe(Arc::new(move |_endpoint, request| match request {
                RepairProbeRequest::Backfill(request) => {
                    observed_offsets_probe
                        .lock()
                        .expect("lock")
                        .push(request.row_offset);
                    let response = match request.row_offset.unwrap_or(0) {
                        0 => InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: 1,
                            rows_scanned: 1,
                            truncated: true,
                            next_row_offset: Some(1),
                            rows: vec![crate::cluster::rpc::InternalRow {
                                metric: metric_for_probe.clone(),
                                labels: labels_for_probe.clone(),
                                data_point: DataPoint::new(existing_ts, 1.0),
                            }],
                        },
                        1 => InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: 1,
                            rows_scanned: 1,
                            truncated: false,
                            next_row_offset: None,
                            rows: vec![crate::cluster::rpc::InternalRow {
                                metric: metric_for_probe.clone(),
                                labels: labels_for_probe.clone(),
                                data_point: DataPoint::new(missing_ts, 2.0),
                            }],
                        },
                        _ => InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: 0,
                            rows_scanned: 0,
                            truncated: false,
                            next_row_offset: None,
                            rows: Vec::new(),
                        },
                    };
                    Ok(RepairProbeResponse::Backfill(response))
                }
            }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels.clone(),
                DataPoint::new(existing_ts, 1.0),
            )])
            .expect("seed row should insert");

        runtime.run_once(storage.clone()).await;
        let first_snapshot = runtime.snapshot();
        assert_eq!(first_snapshot.repairs_attempted_total, 1);
        assert_eq!(first_snapshot.repairs_succeeded_total, 0);
        assert_eq!(first_snapshot.repairs_failed_total, 0);
        assert_eq!(first_snapshot.repairs_skipped_budget_total, 1);
        assert_eq!(first_snapshot.repair_rows_inserted_total, 0);

        runtime.run_once(storage.clone()).await;
        let second_snapshot = runtime.snapshot();
        assert_eq!(second_snapshot.repairs_attempted_total, 2);
        assert_eq!(second_snapshot.repairs_succeeded_total, 1);
        assert_eq!(second_snapshot.repairs_failed_total, 0);
        assert_eq!(second_snapshot.repair_rows_inserted_total, 1);

        let observed_offsets = observed_offsets.lock().expect("lock").clone();
        assert_eq!(observed_offsets, vec![None, Some(1)]);

        let repaired_points = storage
            .select(
                metric,
                &labels,
                now_ts.saturating_sub(10_000),
                now_ts.saturating_add(1),
            )
            .expect("select should succeed");
        assert_eq!(repaired_points.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_skips_non_additive_mismatch_repair() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let repair_probe_calls = Arc::new(StdMutex::new(0u64));
        let repair_probe_calls_ref = Arc::clone(&repair_probe_calls);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 0,
                point_count: 0,
                fingerprint: 42,
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, _request| {
            *repair_probe_calls_ref.lock().expect("lock") += 1;
            Ok(RepairProbeResponse::Backfill(
                InternalRepairBackfillResponse {
                    shard: 0,
                    ring_version: 1,
                    window_start: 0,
                    window_end: 0,
                    series_scanned: 0,
                    rows_scanned: 0,
                    truncated: false,
                    next_row_offset: None,
                    rows: Vec::new(),
                },
            ))
        }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        storage
            .insert_rows(&[Row::with_labels(
                "digest_metric",
                vec![Label::new("job", "local")],
                DataPoint::new(1_700_000_000_100, 1.0),
            )])
            .expect("seed row should insert");

        runtime.run_once(storage).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.repairs_attempted_total, 0);
        assert_eq!(snapshot.repairs_skipped_non_additive_total, 1);
        assert_eq!(*repair_probe_calls.lock().expect("lock"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_skips_repair_when_paused() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let repair_probe_calls = Arc::new(StdMutex::new(0u64));
        let repair_probe_calls_ref = Arc::clone(&repair_probe_calls);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 1,
                point_count: 2,
                fingerprint: 77,
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, _request| {
            *repair_probe_calls_ref.lock().expect("lock") += 1;
            Ok(RepairProbeResponse::Backfill(
                InternalRepairBackfillResponse {
                    shard: 0,
                    ring_version: 1,
                    window_start: 0,
                    window_end: 0,
                    series_scanned: 0,
                    rows_scanned: 0,
                    truncated: false,
                    next_row_offset: None,
                    rows: Vec::new(),
                },
            ))
        }));
        runtime.pause_repairs();

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        storage
            .insert_rows(&[Row::with_labels(
                "digest_metric",
                vec![Label::new("job", "local")],
                DataPoint::new(1_700_000_000_100, 1.0),
            )])
            .expect("seed row should insert");

        runtime.run_once(storage).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.repairs_attempted_total, 0);
        assert_eq!(snapshot.repairs_skipped_paused_total, 1);
        assert_eq!(*repair_probe_calls.lock().expect("lock"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_skips_repair_while_failure_backoff_is_active() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let repair_probe_calls = Arc::new(StdMutex::new(0u64));
        let repair_probe_calls_ref = Arc::clone(&repair_probe_calls);
        let base_runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 4,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 8,
                max_repair_series_per_tick: 32,
                max_repair_rows_per_tick: 32,
                max_repair_runtime_per_tick: Duration::from_millis(
                    DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
                ),
                repair_failure_backoff: Duration::from_secs(300),
                rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
                rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            },
            |_| {},
        );
        let state = base_runtime.control_consensus.current_state();
        let local_node_id = base_runtime.local_node_id.clone();
        let target_shard = (0..state.ring.shard_count.max(1))
            .find(|shard| {
                let owners =
                    state.owners_for_shard_at_ring_version(*shard, state.ring_version.max(1));
                owners.iter().any(|owner| owner == &local_node_id)
                    && owners.iter().any(|owner| owner != &local_node_id)
            })
            .expect("expected at least one comparable shard");
        let runtime = base_runtime
            .with_digest_probe(Arc::new(move |_endpoint, request| {
                let (series_count, point_count, fingerprint) = if request.shard == target_shard {
                    (1, 1, FNV_OFFSET_BASIS.wrapping_add(1))
                } else {
                    (0, 0, FNV_OFFSET_BASIS)
                };
                Ok(InternalDigestWindowResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_count,
                    point_count,
                    fingerprint,
                })
            }))
            .with_repair_probe(Arc::new(move |_endpoint, request| {
                *repair_probe_calls_ref.lock().expect("lock") += 1;
                match request {
                    RepairProbeRequest::Backfill(_) => {
                        Err("injected repair backfill failure".to_string())
                    }
                }
            }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage.clone()).await;
        runtime.run_once(storage).await;

        let snapshot = runtime.snapshot();
        assert_eq!(*repair_probe_calls.lock().expect("lock"), 1);
        assert_eq!(snapshot.repairs_attempted_total, 1);
        assert_eq!(snapshot.repairs_failed_total, 1);
        assert_eq!(snapshot.repairs_skipped_backoff_total, 1);
        assert_eq!(snapshot.repairs_skipped_backoff_last_run, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_falls_back_to_next_peer_when_primary_candidate_is_in_backoff() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut transition_context = None::<(u32, String, String)>;
        let base_runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 4,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 8,
                max_repair_series_per_tick: 32,
                max_repair_rows_per_tick: 32,
                max_repair_runtime_per_tick: Duration::from_millis(
                    DEFAULT_REPAIR_MAX_RUNTIME_MS_PER_TICK,
                ),
                repair_failure_backoff: Duration::from_secs(300),
                rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
                rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            },
            |state| {
                let current_ring_version = state.ring_version.max(1);
                let (transition_shard, owners) = (0..state.ring.shard_count.max(1))
                    .find_map(|shard| {
                        let owners =
                            state.owners_for_shard_at_ring_version(shard, current_ring_version);
                        if owners.iter().any(|owner| owner == "node-a") || owners.len() < 2 {
                            return None;
                        }
                        Some((shard, owners))
                    })
                    .expect("expected handoff shard with two remote owners");
                let primary_source_node = owners
                    .first()
                    .cloned()
                    .expect("handoff source owner should exist");
                let fallback_node = owners
                    .iter()
                    .find(|owner| owner.as_str() != primary_source_node.as_str())
                    .cloned()
                    .expect("fallback owner should exist");
                state
                    .apply_begin_shard_handoff(
                        transition_shard,
                        primary_source_node.as_str(),
                        "node-a",
                        current_ring_version.saturating_add(1),
                    )
                    .expect("handoff begin should apply");
                let primary_endpoint = state
                    .node_record(&primary_source_node)
                    .map(|node| node.endpoint.clone())
                    .expect("primary source endpoint should exist");
                let fallback_endpoint = state
                    .node_record(&fallback_node)
                    .map(|node| node.endpoint.clone())
                    .expect("fallback endpoint should exist");
                transition_context = Some((transition_shard, primary_endpoint, fallback_endpoint));
            },
        );
        let (target_shard, primary_endpoint, fallback_endpoint) =
            transition_context.expect("transition context should exist");
        let primary_endpoint_for_digest = primary_endpoint.clone();
        let fallback_endpoint_for_digest = fallback_endpoint.clone();
        let primary_endpoint_for_repair = primary_endpoint.clone();
        let fallback_endpoint_for_repair = fallback_endpoint.clone();
        let repair_probe_calls = Arc::new(StdMutex::new(Vec::<String>::new()));
        let repair_probe_calls_ref = Arc::clone(&repair_probe_calls);
        let runtime = base_runtime
            .with_digest_probe(Arc::new(move |endpoint, request| {
                let (series_count, point_count, fingerprint) = if request.shard != target_shard {
                    (0, 0, FNV_OFFSET_BASIS)
                } else if endpoint == primary_endpoint_for_digest {
                    (2, 20, FNV_OFFSET_BASIS.wrapping_add(20))
                } else if endpoint == fallback_endpoint_for_digest {
                    (1, 10, FNV_OFFSET_BASIS.wrapping_add(10))
                } else {
                    (0, 0, FNV_OFFSET_BASIS)
                };
                Ok(InternalDigestWindowResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_count,
                    point_count,
                    fingerprint,
                })
            }))
            .with_repair_probe(Arc::new(move |endpoint, request| {
                repair_probe_calls_ref
                    .lock()
                    .expect("lock")
                    .push(endpoint.to_string());
                if endpoint == primary_endpoint_for_repair {
                    return Err("injected primary peer repair failure".to_string());
                }
                assert_eq!(endpoint, fallback_endpoint_for_repair);
                match request {
                    RepairProbeRequest::Backfill(request) => Ok(RepairProbeResponse::Backfill(
                        InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: 0,
                            rows_scanned: 0,
                            truncated: false,
                            next_row_offset: None,
                            rows: Vec::new(),
                        },
                    )),
                }
            }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage.clone()).await;
        runtime.run_once(storage).await;

        let snapshot = runtime.snapshot();
        let repair_calls = repair_probe_calls.lock().expect("lock").clone();
        assert_eq!(repair_calls.len(), 2);
        assert_eq!(repair_calls[0], primary_endpoint);
        assert_eq!(repair_calls[1], fallback_endpoint);
        assert_eq!(snapshot.repairs_attempted_total, 2);
        assert_eq!(snapshot.repairs_failed_total, 1);
        assert_eq!(snapshot.repairs_succeeded_total, 1);
        assert_eq!(snapshot.repairs_skipped_backoff_total, 1);
        assert_eq!(snapshot.repairs_skipped_backoff_last_run, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_honors_repair_time_budget_and_skips_backfill() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let repair_probe_calls = Arc::new(StdMutex::new(0u64));
        let repair_probe_calls_ref = Arc::clone(&repair_probe_calls);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 4,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 8,
                max_repair_series_per_tick: 32,
                max_repair_rows_per_tick: 32,
                max_repair_runtime_per_tick: Duration::from_millis(1),
                repair_failure_backoff: Duration::from_secs(DEFAULT_REPAIR_FAILURE_BACKOFF_SECS),
                rebalance_interval: Duration::from_secs(DEFAULT_REBALANCE_INTERVAL_SECS),
                rebalance_max_rows_per_tick: DEFAULT_REBALANCE_MAX_ROWS_PER_TICK,
                rebalance_max_shards_per_tick: DEFAULT_REBALANCE_MAX_SHARDS_PER_TICK,
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 1,
                point_count: 1,
                fingerprint: u64::from(request.shard).saturating_add(1),
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, request| match request {
            RepairProbeRequest::Backfill(request) => {
                let mut calls = repair_probe_calls_ref.lock().expect("lock");
                *calls = calls.saturating_add(1);
                if *calls == 1 {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(RepairProbeResponse::Backfill(
                    InternalRepairBackfillResponse {
                        shard: request.shard,
                        ring_version: request.ring_version,
                        window_start: request.window_start,
                        window_end: request.window_end,
                        series_scanned: 0,
                        rows_scanned: 0,
                        truncated: false,
                        next_row_offset: None,
                        rows: Vec::new(),
                    },
                ))
            }
        }));

        let state = runtime.control_consensus.current_state();
        let local_node_id = runtime.local_node_id.clone();
        let comparable_shards = (0..state.ring.shard_count.max(1))
            .filter(|shard| {
                let owners =
                    state.owners_for_shard_at_ring_version(*shard, state.ring_version.max(1));
                owners.iter().any(|owner| owner == &local_node_id)
                    && owners.iter().any(|owner| owner != &local_node_id)
            })
            .count();
        assert!(
            comparable_shards >= 2,
            "expected at least two comparable shards to validate time-budget skipping"
        );

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage).await;
        let snapshot = runtime.snapshot();
        assert_eq!(*repair_probe_calls.lock().expect("lock"), 1);
        assert_eq!(snapshot.repairs_attempted_total, 1);
        assert!(snapshot.repairs_skipped_time_budget_total >= 1);
        assert_eq!(snapshot.repair_time_budget_exhaustions_total, 1);
        assert!(snapshot.repair_time_budget_exhausted_last_run);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn run_once_backfill_progresses_under_concurrent_ingest_workload() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let metric = "repair_concurrency_metric";
        let shard_count = 4u32;
        let shard_labels = (0..shard_count)
            .map(|shard| {
                let label = find_label_for_shard(metric, "job", shard, shard_count)
                    .expect("should find label for shard");
                (shard, vec![label])
            })
            .collect::<Vec<_>>();
        let now_ts = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let seed_ts = now_ts.saturating_sub(2_000);
        let backfill_ts = now_ts.saturating_sub(1_000);
        let digest_calls = Arc::new(StdMutex::new(0u64));
        let digest_calls_probe = Arc::clone(&digest_calls);
        let repair_calls = Arc::new(StdMutex::new(0u64));
        let repair_calls_probe = Arc::clone(&repair_calls);
        let shard_labels_for_probe = shard_labels.clone();
        let metric_for_probe = metric.to_string();

        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 2,
                max_mismatch_reports: 16,
                max_bytes_per_tick: 64 * 1024,
                max_repair_mismatches_per_tick: 8,
                max_repair_series_per_tick: 64,
                max_repair_rows_per_tick: 64,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            *digest_calls_probe.lock().expect("lock") += 1;
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 10_000,
                point_count: 1_000_000,
                fingerprint: u64::from(request.shard).saturating_add(100),
            })
        }))
        .with_repair_probe(Arc::new(move |_endpoint, request| match request {
            RepairProbeRequest::Backfill(request) => {
                *repair_calls_probe.lock().expect("lock") += 1;
                let labels = shard_labels_for_probe
                    .iter()
                    .find_map(|(series_shard, labels)| {
                        if *series_shard == request.shard {
                            Some(labels.clone())
                        } else {
                            None
                        }
                    })
                    .expect("labels for request shard should exist");
                Ok(RepairProbeResponse::Backfill(
                    InternalRepairBackfillResponse {
                        shard: request.shard,
                        ring_version: request.ring_version,
                        window_start: request.window_start,
                        window_end: request.window_end,
                        series_scanned: 1,
                        rows_scanned: 1,
                        truncated: false,
                        next_row_offset: None,
                        rows: vec![crate::cluster::rpc::InternalRow {
                            metric: metric_for_probe.clone(),
                            labels,
                            data_point: DataPoint::new(backfill_ts, 42.0),
                        }],
                    },
                ))
            }
        }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        for (_, labels) in &shard_labels {
            storage
                .insert_rows(&[Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(seed_ts, 1.0),
                )])
                .expect("seed row should insert");
        }

        let ingest_storage = storage.clone();
        let ingest_labels = shard_labels.clone();
        let metric_for_ingest = metric.to_string();
        let ingest_task = tokio::spawn(async move {
            for idx in 0..200u64 {
                let labels = ingest_labels[(idx as usize) % ingest_labels.len()]
                    .1
                    .clone();
                let ts = now_ts.saturating_add(i64::try_from(idx).unwrap_or(i64::MAX));
                ingest_storage
                    .insert_rows(&[Row::with_labels(
                        metric_for_ingest.as_str(),
                        labels,
                        DataPoint::new(ts, idx as f64),
                    )])
                    .expect("ingest row should insert");
                tokio::task::yield_now().await;
            }
        });

        for _ in 0..32 {
            runtime.run_once(storage.clone()).await;
            tokio::task::yield_now().await;
        }
        ingest_task
            .await
            .expect("concurrent ingest task should finish");

        let snapshot = runtime.snapshot();
        assert!(snapshot.repairs_attempted_total > 0);
        assert_eq!(snapshot.repairs_failed_total, 0);
        assert_eq!(snapshot.repairs_cancelled_total, 0);
        assert!(snapshot.repair_rows_inserted_total >= 1);
        assert!(*digest_calls.lock().expect("lock") > 0);
        assert!(*repair_calls.lock().expect("lock") > 0);

        let sample_labels = shard_labels
            .first()
            .expect("sample labels should exist")
            .1
            .clone();
        let sample_points = storage
            .select(
                metric,
                &sample_labels,
                now_ts.saturating_sub(10_000),
                now_ts.saturating_add(10_000),
            )
            .expect("select should succeed");
        assert!(sample_points.len() >= 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_records_cancelled_repair_attempt() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let metric = "digest_metric";
        let shard_count = 4u32;
        let shard_labels = (0..shard_count)
            .map(|shard| {
                let label = find_label_for_shard(metric, "job", shard, shard_count)
                    .expect("should find label for shard");
                vec![label]
            })
            .collect::<Vec<_>>();
        let shard_labels_for_probe = shard_labels.clone();
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        );
        let cancel_runtime = runtime.clone();
        let runtime = runtime
            .with_digest_probe(Arc::new(move |_endpoint, request| {
                Ok(InternalDigestWindowResponse {
                    shard: request.shard,
                    ring_version: request.ring_version,
                    window_start: request.window_start,
                    window_end: request.window_end,
                    series_count: 1,
                    point_count: 2,
                    fingerprint: 77,
                })
            }))
            .with_repair_probe(Arc::new(move |_endpoint, request| match request {
                RepairProbeRequest::Backfill(request) => {
                    let labels = shard_labels_for_probe
                        .first()
                        .cloned()
                        .expect("test labels should exist");
                    cancel_runtime.cancel_repairs();
                    Ok(RepairProbeResponse::Backfill(
                        InternalRepairBackfillResponse {
                            shard: request.shard,
                            ring_version: request.ring_version,
                            window_start: request.window_start,
                            window_end: request.window_end,
                            series_scanned: 1,
                            rows_scanned: 2,
                            truncated: false,
                            next_row_offset: None,
                            rows: vec![
                                crate::cluster::rpc::InternalRow {
                                    metric: metric.to_string(),
                                    labels: labels.clone(),
                                    data_point: DataPoint::new(1_700_000_000_100, 1.0),
                                },
                                crate::cluster::rpc::InternalRow {
                                    metric: metric.to_string(),
                                    labels,
                                    data_point: DataPoint::new(1_700_000_000_200, 2.0),
                                },
                            ],
                        },
                    ))
                }
            }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        for labels in &shard_labels {
            storage
                .insert_rows(&[Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(1_700_000_000_100, 1.0),
                )])
                .expect("seed row should insert");
        }

        runtime.run_once(storage).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.repairs_attempted_total, 1);
        assert_eq!(snapshot.repairs_cancelled_total, 1);
        assert_eq!(snapshot.repairs_succeeded_total, 0);
        assert_eq!(snapshot.repair_rows_inserted_total, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_honors_digest_byte_budget_and_skips_windows() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let digest_calls = Arc::new(StdMutex::new(0usize));
        let digest_calls_probe = Arc::clone(&digest_calls);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 4,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 1,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            *digest_calls_probe.lock().expect("lock") += 1;
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 0,
                point_count: 0,
                fingerprint: 0,
            })
        }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);
        storage
            .insert_rows(&[Row::with_labels(
                "budget_metric",
                vec![Label::new("job", "local")],
                DataPoint::new(1_700_000_000_001, 1.0),
            )])
            .expect("seed row should insert");

        runtime.run_once(storage).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.runs_total, 1);
        assert_eq!(snapshot.windows_compared_total, 0);
        assert_eq!(snapshot.bytes_exchanged_last_run, 0);
        assert_eq!(snapshot.budget_exhaustions_total, 1);
        assert!(snapshot.budget_exhausted_last_run);
        assert!(snapshot.windows_skipped_budget_total >= 1);
        assert_eq!(*digest_calls.lock().expect("lock"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_prioritizes_active_handoff_shards_before_rotating_shards() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let observed_shards = Arc::new(StdMutex::new(Vec::new()));
        let observed_shards_probe = Arc::clone(&observed_shards);
        let expected_priority_shard = Arc::new(StdMutex::new(None));
        let expected_priority_shard_mut = Arc::clone(&expected_priority_shard);
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 8,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |state| {
                let activation_ring_version = state.ring_version.saturating_add(1);
                let (transition_shard, to_node_id) = (0..state.ring.shard_count)
                    .find_map(|shard| {
                        let owners =
                            state.owners_for_shard_at_ring_version(shard, state.ring_version);
                        if !owners.iter().any(|owner| owner == "node-a") {
                            return None;
                        }
                        if !owners.iter().any(|owner| owner == "node-b") {
                            return Some((shard, "node-b"));
                        }
                        if !owners.iter().any(|owner| owner == "node-c") {
                            return Some((shard, "node-c"));
                        }
                        None
                    })
                    .expect("should find local shard with alternate transition target");
                *expected_priority_shard_mut.lock().expect("lock") = Some(transition_shard);
                state
                    .apply_begin_shard_handoff(
                        transition_shard,
                        "node-a",
                        to_node_id,
                        activation_ring_version,
                    )
                    .expect("handoff begin should apply");
            },
        )
        .with_digest_probe(Arc::new(move |_endpoint, request| {
            observed_shards_probe
                .lock()
                .expect("lock")
                .push(request.shard);
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 0,
                point_count: 0,
                fingerprint: 1,
            })
        }));
        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        runtime.run_once(storage).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.compared_shards_last_run, 1);
        assert_eq!(snapshot.prioritized_shards_last_run, 1);
        assert_eq!(snapshot.compared_peers_last_run, 1);

        let expected_priority_shard = expected_priority_shard
            .lock()
            .expect("lock")
            .expect("expected shard should be set");
        let observed_shards = observed_shards.lock().expect("lock").clone();
        assert_eq!(observed_shards, vec![expected_priority_shard]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_once_detects_controlled_drift_across_rotation_window() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let runtime = build_digest_runtime_for_test(
            temp_dir.path(),
            "127.0.0.1:9302",
            DigestExchangeConfig {
                interval: Duration::from_secs(1),
                window: Duration::from_secs(60),
                max_shards_per_tick: 1,
                max_mismatch_reports: 64,
                max_bytes_per_tick: 64 * 1024,
                ..DigestExchangeConfig::default()
            },
            |_| {},
        );

        let state = runtime.control_consensus.current_state();
        let ring_version = state.ring_version.max(1);
        let comparable_shards = (0..state.ring.shard_count)
            .filter(|shard| {
                let owners = state.owners_for_shard_at_ring_version(*shard, ring_version);
                owners.iter().any(|owner| owner == "node-a") && owners.len() > 1
            })
            .collect::<Vec<_>>();
        assert!(
            comparable_shards.len() >= 2,
            "expected at least two comparable shards for rotation test"
        );
        let drift_shards = comparable_shards
            .iter()
            .take(2)
            .copied()
            .collect::<BTreeSet<_>>();

        let observed_shards = Arc::new(StdMutex::new(Vec::new()));
        let observed_shards_probe = Arc::clone(&observed_shards);
        let drift_shards_probe = drift_shards.clone();
        let runtime = runtime.with_digest_probe(Arc::new(move |_endpoint, request| {
            observed_shards_probe
                .lock()
                .expect("lock")
                .push(request.shard);
            let mismatched = drift_shards_probe.contains(&request.shard);
            Ok(InternalDigestWindowResponse {
                shard: request.shard,
                ring_version: request.ring_version,
                window_start: request.window_start,
                window_end: request.window_end,
                series_count: 0,
                point_count: 0,
                fingerprint: if mismatched {
                    FNV_OFFSET_BASIS.wrapping_add(1)
                } else {
                    FNV_OFFSET_BASIS
                },
            })
        }));

        let storage = build_test_storage(TEST_DIGEST_SHARD_COUNT);

        for _ in 0..comparable_shards.len() {
            runtime.run_once(storage.clone()).await;
        }

        let snapshot = runtime.snapshot();
        assert_eq!(
            snapshot.windows_compared_total,
            u64::try_from(comparable_shards.len()).unwrap_or(u64::MAX)
        );
        assert_eq!(
            snapshot.mismatches_total,
            u64::try_from(drift_shards.len()).unwrap_or(u64::MAX)
        );

        let observed_shards = observed_shards.lock().expect("lock").clone();
        let observed_unique = observed_shards.into_iter().collect::<BTreeSet<_>>();
        let comparable_unique = comparable_shards.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(observed_unique, comparable_unique);

        let mismatch_shards = snapshot
            .mismatches
            .iter()
            .map(|mismatch| mismatch.shard)
            .collect::<BTreeSet<_>>();
        assert_eq!(mismatch_shards, drift_shards);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rebalance_scheduler_respects_rows_per_tick_budget() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let runtime = build_single_node_rebalance_runtime_for_test(
            temp_dir.path(),
            DigestExchangeConfig {
                rebalance_max_rows_per_tick: 10,
                rebalance_max_shards_per_tick: 1,
                ..DigestExchangeConfig::default()
            },
            |state| {
                state
                    .apply_join_node("node-b", "127.0.0.1:9302")
                    .expect("join should apply");
                let activation_ring_version = state.ring_version.saturating_add(1);
                state
                    .apply_begin_shard_handoff(0, "node-a", "node-b", activation_ring_version)
                    .expect("handoff begin should apply");
                state
                    .apply_shard_handoff_progress(
                        0,
                        ShardHandoffPhase::Warmup,
                        Some(0),
                        Some(25),
                        None,
                    )
                    .expect("handoff warmup progress should apply");
                state.leader_node_id = Some("node-a".to_string());
            },
        );

        runtime.run_rebalance_once().await;
        let first = runtime.control_consensus.current_state();
        let transition = first
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("handoff transition should exist");
        assert_eq!(transition.handoff.phase, ShardHandoffPhase::Warmup);
        assert_eq!(transition.handoff.copied_rows, 10);
        assert_eq!(transition.handoff.pending_rows, 15);
        assert_eq!(runtime.rebalance_snapshot().rows_scheduled_last_run, 10);

        runtime.run_rebalance_once().await;
        let second = runtime.control_consensus.current_state();
        let transition = second
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("handoff transition should exist");
        assert_eq!(transition.handoff.phase, ShardHandoffPhase::Warmup);
        assert_eq!(transition.handoff.copied_rows, 20);
        assert_eq!(transition.handoff.pending_rows, 5);
        assert_eq!(runtime.rebalance_snapshot().rows_scheduled_last_run, 10);

        runtime.run_rebalance_once().await;
        let third = runtime.control_consensus.current_state();
        let transition = third
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("handoff transition should exist");
        assert_eq!(transition.handoff.phase, ShardHandoffPhase::Cutover);
        assert_eq!(transition.handoff.copied_rows, 25);
        assert_eq!(transition.handoff.pending_rows, 0);
        assert_eq!(runtime.rebalance_snapshot().rows_scheduled_last_run, 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rebalance_scheduler_pause_resume_controls_execution() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let runtime = build_single_node_rebalance_runtime_for_test(
            temp_dir.path(),
            DigestExchangeConfig {
                rebalance_max_rows_per_tick: 4,
                rebalance_max_shards_per_tick: 1,
                ..DigestExchangeConfig::default()
            },
            |state| {
                state
                    .apply_join_node("node-b", "127.0.0.1:9302")
                    .expect("join should apply");
                let activation_ring_version = state.ring_version.saturating_add(1);
                state
                    .apply_begin_shard_handoff(0, "node-a", "node-b", activation_ring_version)
                    .expect("handoff begin should apply");
                state
                    .apply_shard_handoff_progress(
                        0,
                        ShardHandoffPhase::Warmup,
                        Some(0),
                        Some(9),
                        None,
                    )
                    .expect("handoff warmup progress should apply");
                state.leader_node_id = Some("node-a".to_string());
            },
        );

        runtime.pause_rebalance();
        runtime.run_rebalance_once().await;
        let paused_state = runtime.control_consensus.current_state();
        let paused_transition = paused_state
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("handoff transition should exist");
        assert_eq!(paused_transition.handoff.copied_rows, 0);
        assert_eq!(paused_transition.handoff.pending_rows, 9);

        runtime.resume_rebalance();
        runtime.run_rebalance_once().await;
        let resumed_state = runtime.control_consensus.current_state();
        let resumed_transition = resumed_state
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("handoff transition should exist");
        assert_eq!(resumed_transition.handoff.copied_rows, 4);
        assert_eq!(resumed_transition.handoff.pending_rows, 5);

        let snapshot = runtime.rebalance_snapshot();
        assert!(!snapshot.paused);
        assert!(snapshot.runs_total >= 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rebalance_snapshot_includes_active_handoff_jobs() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let runtime = build_single_node_rebalance_runtime_for_test(
            temp_dir.path(),
            DigestExchangeConfig::default(),
            |state| {
                state
                    .apply_join_node("node-b", "127.0.0.1:9302")
                    .expect("join should apply");
                let activation_ring_version = state.ring_version.saturating_add(1);
                state
                    .apply_begin_shard_handoff(0, "node-a", "node-b", activation_ring_version)
                    .expect("handoff begin should apply");
                state
                    .apply_shard_handoff_progress(
                        0,
                        ShardHandoffPhase::Warmup,
                        Some(3),
                        Some(7),
                        None,
                    )
                    .expect("handoff warmup progress should apply");
                state.leader_node_id = Some("node-a".to_string());
            },
        );

        let snapshot = runtime.rebalance_snapshot();
        assert_eq!(snapshot.active_jobs, 1);
        assert_eq!(snapshot.jobs.len(), 1);
        assert_eq!(snapshot.jobs[0].shard, 0);
        assert_eq!(snapshot.jobs[0].phase, ShardHandoffPhase::Warmup);
        assert_eq!(snapshot.jobs[0].pending_rows, 7);
    }

    #[test]
    fn rebalance_candidate_ranking_prefers_hotter_mismatch_shard() {
        let cluster_config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            shards: 16,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership =
            MembershipView::from_config(&cluster_config).expect("membership should build");
        let ring = ShardRing::build(
            cluster_config.shards,
            cluster_config.replication_factor,
            &membership,
        )
        .expect("ring should build");
        let mut state = ControlState::from_runtime(&membership, &ring);
        state
            .apply_join_node("node-d", "127.0.0.1:9304")
            .expect("join should apply");
        state.leader_node_id = Some("node-a".to_string());

        let baseline = collect_rebalance_move_candidates_with_tracker(
            "node-a",
            &state,
            &hotspot::HotspotTrackerSnapshot {
                generated_unix_ms: 0,
                shards: Vec::new(),
                tenants: Vec::new(),
            },
        );
        assert!(baseline.len() >= 2);
        let hot_shard = baseline
            .last()
            .map(|candidate| candidate.shard)
            .expect("expected at least one candidate");
        let baseline_index = baseline
            .iter()
            .position(|candidate| candidate.shard == hot_shard)
            .expect("baseline candidate should exist");
        assert!(baseline_index > 0);

        let hot_label =
            find_label_for_shard("hot_metric", "instance", hot_shard, ring.shard_count())
                .expect("hot shard label should be found");
        let mut hot_rows = Vec::new();
        for idx in 0..10_000 {
            hot_rows.push(Row::with_labels(
                "hot_metric",
                vec![hot_label.clone()],
                DataPoint::new(idx, idx as f64),
            ));
        }
        let tracker = hotspot::hotspot_tracker_snapshot_for_rows(Some(&ring), &hot_rows);
        let ranked = collect_rebalance_move_candidates_with_tracker("node-a", &state, &tracker);
        let ranked_index = ranked
            .iter()
            .position(|candidate| candidate.shard == hot_shard)
            .expect("ranked candidate should exist");
        assert!(
            ranked[ranked_index].decision_score > baseline[baseline_index].decision_score,
            "expected hotspot pressure to lift the candidate's decision score"
        );
        assert!(
            ranked[ranked_index].pressure_score > baseline[baseline_index].pressure_score,
            "expected hotspot pressure to increase the shard pressure score"
        );
    }

    fn find_label_for_shard(
        metric: &str,
        label_name: &str,
        target_shard: u32,
        shard_count: u32,
    ) -> Option<Label> {
        (0..10_000).find_map(|idx| {
            let label = Label::new(label_name, format!("v-{target_shard}-{idx}"));
            let shard = (stable_series_identity_hash(metric, std::slice::from_ref(&label))
                % u64::from(shard_count.max(1))) as u32;
            if shard == target_shard {
                Some(label)
            } else {
                None
            }
        })
    }

    fn build_digest_runtime_for_test<F>(
        temp_dir: &std::path::Path,
        peer_endpoint: &str,
        config: DigestExchangeConfig,
        mutate_control_state: F,
    ) -> DigestExchangeRuntime
    where
        F: FnOnce(&mut ControlState),
    {
        let cluster_config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                format!("node-b@{peer_endpoint}"),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            shards: 4,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let membership =
            MembershipView::from_config(&cluster_config).expect("membership should build");
        let ring = ShardRing::build(
            cluster_config.shards,
            cluster_config.replication_factor,
            &membership,
        )
        .expect("ring should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.join("control/state.json"))
                .expect("state store should open"),
        );
        let mut bootstrap = ControlState::from_runtime(&membership, &ring);
        mutate_control_state(&mut bootstrap);
        let recovered = state_store
            .load_or_bootstrap(bootstrap)
            .expect("control state should bootstrap");
        let control_consensus = Arc::new(
            ControlConsensusRuntime::open(
                membership.clone(),
                state_store,
                recovered,
                temp_dir.join("control/log.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: crate::cluster::rpc::CompatibilityProfile::default(),
            internal_mtls: None,
        });
        DigestExchangeRuntime::new(
            membership.local_node_id.clone(),
            rpc_client,
            control_consensus,
            config,
        )
    }

    fn build_single_node_rebalance_runtime_for_test<F>(
        temp_dir: &std::path::Path,
        config: DigestExchangeConfig,
        mutate_control_state: F,
    ) -> DigestExchangeRuntime
    where
        F: FnOnce(&mut ControlState),
    {
        let cluster_config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            shards: 1,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership =
            MembershipView::from_config(&cluster_config).expect("membership should build");
        let ring = ShardRing::build(
            cluster_config.shards,
            cluster_config.replication_factor,
            &membership,
        )
        .expect("ring should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.join("control/single-state.json"))
                .expect("state store should open"),
        );
        let mut bootstrap = ControlState::from_runtime(&membership, &ring);
        mutate_control_state(&mut bootstrap);
        let recovered = state_store
            .load_or_bootstrap(bootstrap)
            .expect("control state should bootstrap");
        let control_consensus = Arc::new(
            ControlConsensusRuntime::open(
                membership.clone(),
                state_store,
                recovered,
                temp_dir.join("control/single-log.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: crate::cluster::rpc::CompatibilityProfile::default(),
            internal_mtls: None,
        });
        DigestExchangeRuntime::new(
            membership.local_node_id.clone(),
            rpc_client,
            control_consensus,
            config,
        )
    }
}
