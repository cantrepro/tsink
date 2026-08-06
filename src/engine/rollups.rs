use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::metrics::{QueryObservabilityCounters, RollupObservabilityCounters};
use super::query_exec::modeled_points_retained_bytes;
use super::*;
use crate::engine::fs_utils::write_file_atomically_and_sync_parent_budgeted;
use crate::engine::query::TieredQueryPlan;
use crate::engine::tombstone::{TombstoneMap, TombstoneRange};
use crate::query_aggregation::{bucket_start_for_origin, downsample_points_with_origin};
use crate::storage::{
    RollupMetricsObservabilitySnapshot, RollupMetricsPolicyStatus, RollupObservabilitySnapshot,
    RollupPolicy, RollupPolicyStatus,
};
use crate::validation::{validate_labels, validate_metric};
use crate::Aggregation;

#[path = "rollups/json_decode.rs"]
mod json_decode;
#[path = "rollups/materialization.rs"]
mod materialization;
#[path = "rollups/policy.rs"]
mod policy;
#[path = "rollups/runtime.rs"]
mod runtime;
#[path = "rollups/state_journal.rs"]
mod state_journal;

#[cfg(test)]
pub(super) use self::runtime::load_rollup_state_json;
#[cfg(test)]
use self::runtime::{load_rollup_state, persist_rollup_state};

pub(super) const ROLLUP_DIR_NAME: &str = ".rollups";
const ROLLUP_POLICIES_FILE_NAME: &str = "policies.json";
const ROLLUP_STATE_FILE_NAME: &str = "state.json";
const ROLLUP_POLICIES_MAGIC: &str = "tsink-rollup-policies";
const ROLLUP_STATE_MAGIC: &str = "tsink-rollup-state";
const ROLLUP_SCHEMA_VERSION: u16 = 1;
const ROLLUP_STATE_SNAPSHOT_MAX_ITEMS: usize = 65_536;
const ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES: usize = 64 * 1024 * 1024;
pub(super) const INTERNAL_ROLLUP_METRIC_PREFIX: &str = "__tsink_rollup__:";

#[cfg(test)]
type RollupPolicyStartHook = dyn Fn(&RollupPolicy) + Send + Sync + 'static;

#[cfg(test)]
type RollupSourceReadHook = dyn Fn(&RollupPolicy, SeriesId) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
type RollupStatePersistHook = dyn Fn() -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollupPolicyPersistHookPoint {
    CandidatePublished,
}

#[cfg(test)]
type RollupPolicyPersistHook =
    dyn Fn(RollupPolicyPersistHookPoint) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
#[derive(Default)]
struct RollupTestHooks {
    policy_start_hook: RwLock<Option<Arc<RollupPolicyStartHook>>>,
    source_read_hook: RwLock<Option<Arc<RollupSourceReadHook>>>,
    state_persist_hook: RwLock<Option<Arc<RollupStatePersistHook>>>,
    policy_persist_hook: RwLock<Option<Arc<RollupPolicyPersistHook>>>,
    metrics_snapshot_policy_copies: AtomicU64,
    status_snapshot_policy_copies: AtomicU64,
}

#[cfg(test)]
impl std::fmt::Debug for RollupTestHooks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RollupTestHooks").finish()
    }
}

#[derive(Debug)]
pub(super) struct RollupRuntimeState {
    dir_path: Option<PathBuf>,
    policies_path: Option<PathBuf>,
    state_path: Option<PathBuf>,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    journal_memory_limit_bytes: usize,
    snapshot_publication_fenced: AtomicBool,
    snapshot_visibility: RwLock<()>,
    policies: RwLock<Vec<RollupPolicy>>,
    checkpoints: RwLock<HashMap<String, BTreeMap<String, i64>>>,
    pending_materializations:
        RwLock<HashMap<String, BTreeMap<String, PendingRollupMaterialization>>>,
    pending_delete_invalidations: RwLock<Vec<PendingRollupDeleteInvalidation>>,
    generations: RwLock<HashMap<String, u64>>,
    state_envelope_usage: Mutex<RollupStateEnvelopeUsage>,
    journal_epoch: AtomicU64,
    policy_stats: RwLock<BTreeMap<String, PolicyRunState>>,
    #[cfg(test)]
    test_hooks: RollupTestHooks,
}

#[derive(Debug, Clone, Default)]
struct PolicyRunState {
    matched_series: u64,
    materialized_series: u64,
    materialized_through: Option<i64>,
    source_traversal_complete: bool,
    last_run_started_at_ms: Option<u64>,
    last_run_completed_at_ms: Option<u64>,
    last_run_duration_nanos: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRollupPoliciesFile {
    magic: String,
    version: u16,
    policies: Vec<RollupPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRollupStateFile {
    magic: String,
    version: u16,
    #[serde(default)]
    journal_epoch: u64,
    checkpoints: Vec<PersistedRollupCheckpoint>,
    #[serde(default)]
    pending_materializations: Vec<PersistedPendingRollupMaterialization>,
    #[serde(default)]
    pending_delete_invalidations: Vec<PersistedPendingRollupDeleteInvalidation>,
    #[serde(default)]
    generations: Vec<PersistedRollupGeneration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRollupCheckpoint {
    policy_id: String,
    source_key: String,
    materialized_through: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRollupGeneration {
    policy_id: String,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRollupMaterialization {
    checkpoint: i64,
    materialized_through: i64,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRollupDeleteInvalidation {
    tombstone: TombstoneRange,
    series_ids: Vec<SeriesId>,
    affected_policy_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPendingRollupMaterialization {
    policy_id: String,
    source_key: String,
    checkpoint: i64,
    materialized_through: i64,
    generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPendingRollupDeleteInvalidation {
    tombstone: TombstoneRange,
    series_ids: Vec<SeriesId>,
    affected_policy_ids: Vec<String>,
}

#[derive(Debug, Default)]
struct LoadedRollupState {
    journal_epoch: u64,
    checkpoints: HashMap<String, BTreeMap<String, i64>>,
    pending_materializations: HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    pending_delete_invalidations: Vec<PendingRollupDeleteInvalidation>,
    generations: HashMap<String, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RollupStateEnvelopeUsage {
    items: usize,
    modeled_bytes: usize,
}

#[derive(Debug, Clone)]
struct RollupRuntimeSnapshot {
    policies: Vec<RollupPolicy>,
    checkpoints: HashMap<String, BTreeMap<String, i64>>,
    pending_materializations: HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    pending_delete_invalidations: Vec<PendingRollupDeleteInvalidation>,
    generations: HashMap<String, u64>,
    policy_stats: BTreeMap<String, PolicyRunState>,
}

#[derive(Debug)]
enum RollupSnapshotPersistenceOutcome {
    Clean,
    CommittedWithCleanupDebt(TsinkError),
}

impl RollupSnapshotPersistenceOutcome {
    fn into_cleanup_debt(self) -> Option<TsinkError> {
        match self {
            Self::Clean => None,
            Self::CommittedWithCleanupDebt(error) => Some(error),
        }
    }

    fn report_cleanup_debt(self) {
        if let Some(error) = self.into_cleanup_debt() {
            tracing::warn!(
                error = %error,
                "committed rollup snapshot left conservatively-accounted postcommit cleanup debt"
            );
        }
    }
}

#[derive(Debug, Clone)]
struct RollupSourceSeries {
    series_id: SeriesId,
    labels: Vec<Label>,
    source_key: String,
}

#[derive(Debug, Clone)]
pub(in crate::engine) struct RollupQueryCandidate {
    pub(in crate::engine) policy: RollupPolicy,
    pub(in crate::engine) metric: String,
    pub(in crate::engine) materialized_through: i64,
}

trait RollupSourceReadOps {
    fn live_series_ids(
        &self,
        candidate_series_ids: Vec<SeriesId>,
        prune_dead: bool,
    ) -> Result<Vec<SeriesId>>;

    fn query_tier_plan(&self, start: i64, end: i64) -> TieredQueryPlan;

    fn begin_rollup_source_execution(&self) -> Result<QueryExecution>;

    fn collect_points_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)>;

    fn bounded_recency_reference_timestamp(&self) -> Option<i64>;
}

#[derive(Clone, Copy)]
struct RollupStateStoreContext<'a> {
    state: &'a RollupRuntimeState,
}

#[derive(Clone, Copy)]
struct RollupRegistryReadContext<'a> {
    registry: &'a RwLock<SeriesRegistry>,
}

#[derive(Clone, Copy)]
struct RollupDeleteRepairContext<'a> {
    store: RollupStateStoreContext<'a>,
    tombstones: &'a RwLock<TombstoneMap>,
}

#[derive(Clone, Copy)]
struct RollupInvalidationContext<'a> {
    store: RollupStateStoreContext<'a>,
    registry: RollupRegistryReadContext<'a>,
}

#[derive(Clone, Copy)]
struct RollupSourceReadContext<'a> {
    registry: RollupRegistryReadContext<'a>,
    ops: &'a dyn RollupSourceReadOps,
}

trait RollupMaterializedWriteOps {
    fn insert_rows_with_held_permit(
        &self,
        rows: &[Row],
        write_permit: &crate::concurrency::SemaphoreGuard<'_>,
    ) -> Result<WriteResult>;
}

#[derive(Clone, Copy)]
struct RollupMaterializedWriteContext<'storage, 'permit, 'semaphore> {
    ops: &'storage dyn RollupMaterializedWriteOps,
    write_permit: &'permit crate::concurrency::SemaphoreGuard<'semaphore>,
    write_batch_limits: crate::WriteBatchLimits,
}

#[derive(Clone, Copy)]
struct RollupQuerySelectionContext<'a> {
    store: RollupStateStoreContext<'a>,
    registry: RollupRegistryReadContext<'a>,
    source_reads: RollupSourceReadContext<'a>,
    rollup_observability: &'a RollupObservabilityCounters,
    query_observability: &'a QueryObservabilityCounters,
}

#[derive(Clone, Copy)]
struct RollupRunCoordinationContext<'a> {
    run_lock: &'a Mutex<()>,
    traversal_cursor: &'a Mutex<BackgroundRollupCursor>,
}

#[derive(Debug, Default)]
struct PolicyRunReport {
    matched_series: u64,
    materialized_series: u64,
    materialized_through: Option<i64>,
    buckets_materialized: u64,
    points_materialized: u64,
    checkpoint_changed: bool,
}

/// In-memory cursor shared by bounded background and explicit rollup passes.
///
/// Policy mutations share the rollup run lock and carry a generation. A changed or removed policy
/// therefore invalidates this cursor without persisting maintenance-only state.
#[derive(Debug)]
pub(in crate::engine) struct BackgroundRollupCursor {
    policy_id: Option<String>,
    policy_generation: u64,
    after_series_id: Option<SeriesId>,
    checkpoint_persistence_pending: bool,
    cycle_complete: bool,
}

impl Default for BackgroundRollupCursor {
    fn default() -> Self {
        Self {
            policy_id: None,
            policy_generation: 0,
            after_series_id: None,
            checkpoint_persistence_pending: false,
            cycle_complete: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::engine) struct RollupTraversalProgress {
    complete: bool,
    continuation_policy_id: Option<String>,
    continuation_after_series_id: Option<SeriesId>,
}

#[derive(Debug)]
struct BoundedRollupSourceBatch {
    sources: Vec<RollupSourceSeries>,
    last_visited_series_id: Option<SeriesId>,
    has_more: bool,
}

#[derive(Debug, Clone, Copy)]
struct BackgroundRollupPassLimits {
    source_limit: usize,
    item_limit: usize,
    byte_limit: u64,
    modeled_source_bytes: u64,
}

pub(super) fn is_internal_rollup_metric(metric: &str) -> bool {
    metric.starts_with(INTERNAL_ROLLUP_METRIC_PREFIX)
}

pub(super) fn rollup_metric_name(policy: &RollupPolicy, generation: u64) -> String {
    if generation == 0 {
        return format!(
            "{INTERNAL_ROLLUP_METRIC_PREFIX}{}:{}",
            policy.id, policy.metric
        );
    }

    format!(
        "{INTERNAL_ROLLUP_METRIC_PREFIX}{}:g{}:{}",
        policy.id, generation, policy.metric
    )
}

fn policy_matches_source(policy: &RollupPolicy, metric: &str, labels: &[Label]) -> bool {
    policy.metric == metric
        && policy
            .match_labels
            .iter()
            .all(|required| labels.iter().any(|candidate| candidate == required))
}

fn source_series_key(metric: &str, labels: &[Label]) -> String {
    crate::storage::shard_window_series_identity_key(metric, labels)
}

fn aligned_materialized_end(policy: &RollupPolicy, max_observed: i64) -> Option<i64> {
    (max_observed != i64::MIN)
        .then(|| bucket_start_for_origin(max_observed, policy.bucket_origin, policy.interval))
}

fn is_bucket_aligned(timestamp: i64, policy: &RollupPolicy) -> bool {
    bucket_start_for_origin(timestamp, policy.bucket_origin, policy.interval) == timestamp
}

impl RollupSourceReadOps for ChunkStorage {
    fn live_series_ids(
        &self,
        candidate_series_ids: Vec<SeriesId>,
        prune_dead: bool,
    ) -> Result<Vec<SeriesId>> {
        ChunkStorage::live_series_ids(self, candidate_series_ids, prune_dead)
    }

    fn query_tier_plan(&self, start: i64, end: i64) -> TieredQueryPlan {
        ChunkStorage::query_tier_plan(self, start, end)
    }

    fn begin_rollup_source_execution(&self) -> Result<QueryExecution> {
        // A rollup source is still an engine query even though it is not entered through the
        // public read API. Carry the instance's complete query envelope (including its scan and
        // deadline limits) and tighten unlimited/looser configurations to the finite maintenance
        // byte ceiling. One execution now spans both reads and every downstream transformation,
        // so all simultaneously retained source/output memory is charged together.
        let maintenance_bytes = self.runtime.maintenance_max_bytes_per_pass;
        let maintenance_limits = if maintenance_bytes == u64::MAX {
            QueryWorkLimits::default()
        } else {
            let modeled_point_bytes =
                u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX);
            let max_points = maintenance_bytes
                .checked_div(modeled_point_bytes.max(1))
                .unwrap_or(0)
                .max(1);
            QueryWorkLimits {
                max_series_matched: Some(1),
                max_samples_scanned: Some(max_points),
                max_samples_returned: Some(max_points),
                max_returned_bytes: Some(maintenance_bytes),
                max_intermediate_vector_size: Some(max_points),
                max_memory_bytes: Some(maintenance_bytes),
                ..QueryWorkLimits::default()
            }
        };
        let execution = self
            .query_budget
            .begin_query_with(maintenance_limits, QueryCancellationToken::new())?;
        execution.charge_series_matched(1)?;
        Ok(execution)
    }

    fn collect_points_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        execution: &QueryExecution,
    ) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
        let points = ChunkStorage::collect_points_for_series_with_plan(
            self,
            series_id,
            start,
            end,
            plan,
            Some(execution),
            true,
        )
        .map(|(points, _)| points)?;
        // Query-read allocation is preflighted internally. Retain the resulting vector on the
        // same execution before handing it to rollup transformation so later buffers cannot
        // collectively exceed either the per-query or shared query-memory limit.
        let reservation = execution.reserve_memory(modeled_points_retained_bytes(&points))?;
        Ok((points, reservation))
    }

    fn bounded_recency_reference_timestamp(&self) -> Option<i64> {
        ChunkStorage::bounded_recency_reference_timestamp(self)
    }
}

impl RollupMaterializedWriteOps for ChunkStorage {
    fn insert_rows_with_held_permit(
        &self,
        rows: &[Row],
        write_permit: &crate::concurrency::SemaphoreGuard<'_>,
    ) -> Result<WriteResult> {
        ChunkStorage::insert_rollup_rows_with_held_permit(self, rows, write_permit)
    }
}

impl ChunkStorage {
    fn rollup_state_store_context(&self) -> RollupStateStoreContext<'_> {
        RollupStateStoreContext {
            state: &self.rollups.runtime,
        }
    }

    fn rollup_registry_read_context(&self) -> RollupRegistryReadContext<'_> {
        RollupRegistryReadContext {
            registry: &self.catalog.registry,
        }
    }

    fn rollup_delete_repair_context(&self) -> RollupDeleteRepairContext<'_> {
        RollupDeleteRepairContext {
            store: self.rollup_state_store_context(),
            tombstones: &self.visibility.tombstones,
        }
    }

    fn rollup_invalidation_context(&self) -> RollupInvalidationContext<'_> {
        RollupInvalidationContext {
            store: self.rollup_state_store_context(),
            registry: self.rollup_registry_read_context(),
        }
    }

    fn rollup_source_read_context(&self) -> RollupSourceReadContext<'_> {
        RollupSourceReadContext {
            registry: self.rollup_registry_read_context(),
            ops: self,
        }
    }

    fn rollup_materialized_write_context<'permit, 'semaphore>(
        &self,
        write_permit: &'permit crate::concurrency::SemaphoreGuard<'semaphore>,
    ) -> RollupMaterializedWriteContext<'_, 'permit, 'semaphore> {
        RollupMaterializedWriteContext {
            ops: self,
            write_permit,
            write_batch_limits: self.runtime.write_batch_limits,
        }
    }

    fn rollup_query_selection_context(&self) -> RollupQuerySelectionContext<'_> {
        RollupQuerySelectionContext {
            store: self.rollup_state_store_context(),
            registry: self.rollup_registry_read_context(),
            source_reads: self.rollup_source_read_context(),
            rollup_observability: &self.observability.rollup,
            query_observability: &self.observability.query,
        }
    }

    fn rollup_run_coordination_context(&self) -> RollupRunCoordinationContext<'_> {
        RollupRunCoordinationContext {
            run_lock: &self.rollups.run_lock,
            traversal_cursor: &self.rollups.traversal_cursor,
        }
    }
}

#[cfg(test)]
#[path = "rollups/tests.rs"]
mod tests;
