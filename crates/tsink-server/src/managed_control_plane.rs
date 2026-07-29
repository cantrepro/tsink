use crate::tenant::{TenantAdmissionSurface, TenantRequestError};
use crate::usage::UsageAccounting;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tsink::{QueryExecution, QueryMemoryReservation};

const MANAGED_CONTROL_PLANE_DIR: &str = "managed-control-plane";
const MANAGED_CONTROL_PLANE_STATE_FILE: &str = "state.json";
const MANAGED_CONTROL_PLANE_MAGIC: &str = "tsink-managed-control-plane";
const MANAGED_CONTROL_PLANE_SCHEMA_VERSION: u16 = 1;
const DEFAULT_BACKUP_RETENTION_COPIES: u32 = 7;
const DEFAULT_AUDIT_QUERY_LIMIT: usize = 100;
const MAX_RESOURCE_ID_LEN: usize = 128;
const MANAGED_TENANT_INGEST_WINDOW_MS: u64 = 1_000;
const MANAGED_STATUS_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
// A one-entry BTreeMap can retain a whole fixed-capacity root node. Charge each logical entry
// enough for that worst case rather than assuming the allocator packs labels densely.
const MANAGED_STATUS_BTREE_ENTRY_ALLOWANCE_BYTES: u64 = 1_024;
const MANAGED_STATUS_CHECKPOINT_INTERVAL: usize = 32;

#[derive(Debug)]
pub struct ManagedControlPlane {
    state_path: Option<PathBuf>,
    local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    state: Mutex<ManagedControlPlaneStateFile>,
    request_runtimes: Mutex<BTreeMap<String, Arc<ManagedTenantRequestRuntime>>>,
    #[cfg(test)]
    status_projection_output_string_materializations: AtomicU64,
}

#[derive(Debug)]
pub enum ManagedControlPlaneMutationError {
    Rejected(String),
    Persistence(tsink::TsinkError),
}

impl std::fmt::Display for ManagedControlPlaneMutationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(detail) => formatter.write_str(detail),
            Self::Persistence(source) => {
                write!(
                    formatter,
                    "managed control-plane state persistence failed: {source}"
                )
            }
        }
    }
}

impl std::error::Error for ManagedControlPlaneMutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(_) => None,
            Self::Persistence(source) => Some(source),
        }
    }
}

impl From<String> for ManagedControlPlaneMutationError {
    fn from(detail: String) -> Self {
        Self::Rejected(detail)
    }
}

impl From<tsink::TsinkError> for ManagedControlPlaneMutationError {
    fn from(source: tsink::TsinkError) -> Self {
        Self::Persistence(source)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ManagedControlPlaneStateFile {
    magic: String,
    schema_version: u16,
    updated_unix_ms: u64,
    last_audit_seq: u64,
    #[serde(default)]
    deployments: BTreeMap<String, ManagedDeployment>,
    #[serde(default)]
    tenants: BTreeMap<String, ManagedTenant>,
    #[serde(default)]
    audit_entries: Vec<ManagedControlPlaneAuditEntry>,
}

impl Default for ManagedControlPlaneStateFile {
    fn default() -> Self {
        Self {
            magic: MANAGED_CONTROL_PLANE_MAGIC.to_string(),
            schema_version: MANAGED_CONTROL_PLANE_SCHEMA_VERSION,
            updated_unix_ms: unix_timestamp_millis(),
            last_audit_seq: 0,
            deployments: BTreeMap::new(),
            tenants: BTreeMap::new(),
            audit_entries: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct ManagedTenantRequestRuntime {
    active_query_units: Mutex<u64>,
    recent_ingest_units: Mutex<VecDeque<ManagedTenantIngestWindowEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManagedTenantIngestWindowEntry {
    unix_ms: u64,
    units: u64,
}

#[derive(Debug, Clone)]
pub struct ManagedTenantRequestPolicy {
    tenant: ManagedTenant,
    runtime: Arc<ManagedTenantRequestRuntime>,
}

#[derive(Debug, Default)]
pub struct ManagedTenantRequestGuard {
    _permits: Vec<ManagedTenantAdmissionPermit>,
}

#[derive(Debug)]
enum ManagedTenantAdmissionPermit {
    QueryConcurrency {
        runtime: Arc<ManagedTenantRequestRuntime>,
        units: u64,
    },
}

impl Drop for ManagedTenantAdmissionPermit {
    fn drop(&mut self) {
        match self {
            Self::QueryConcurrency { runtime, units } => {
                let mut active_units = runtime
                    .active_query_units
                    .lock()
                    .expect("managed tenant query admission mutex should not be poisoned");
                *active_units = active_units.saturating_sub(*units);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedControlPlaneActor {
    pub id: String,
    pub scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedControlPlaneStatusSnapshot {
    pub durable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_path: Option<String>,
    pub deployments_total: u64,
    pub tenants_total: u64,
    pub active_tenants_total: u64,
    pub maintenance_active_total: u64,
    pub upgrade_rollouts_in_progress_total: u64,
    pub backup_policies_total: u64,
    pub audit_records_total: u64,
    pub updated_unix_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ManagedControlPlaneStatusProjection {
    pub(crate) status: ManagedControlPlaneStatusSnapshot,
    pub(crate) deployments: Vec<ManagedDeploymentSummary>,
    pub(crate) current_tenant: Option<ManagedTenant>,
}

#[derive(Debug)]
#[must_use = "dropping the projection releases its query-memory reservation"]
pub(crate) struct AccountedManagedControlPlaneStatusProjection {
    projection: ManagedControlPlaneStatusProjection,
    _reservation: QueryMemoryReservation,
}

impl AccountedManagedControlPlaneStatusProjection {
    #[cfg(test)]
    fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

impl std::ops::Deref for AccountedManagedControlPlaneStatusProjection {
    type Target = ManagedControlPlaneStatusProjection;

    fn deref(&self) -> &Self::Target {
        &self.projection
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedControlPlaneStateSnapshot {
    pub status: ManagedControlPlaneStatusSnapshot,
    #[serde(default)]
    pub deployments: Vec<ManagedDeployment>,
    #[serde(default)]
    pub tenants: Vec<ManagedTenant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedDeploymentSummary {
    pub id: String,
    pub region: String,
    pub plan: String,
    pub lifecycle: DeploymentLifecycleState,
    pub tenant_count: u64,
    pub active_tenant_count: u64,
    pub backup_enabled: bool,
    pub maintenance_active: bool,
    pub upgrade_state: UpgradeRolloutState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_version: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum DeploymentLifecycleState {
    #[default]
    Requested,
    Provisioning,
    Ready,
    Decommissioning,
    Decommissioned,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum UpgradeRolloutState {
    #[default]
    Idle,
    Pending,
    InProgress,
    Complete,
    Paused,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum TenantLifecycleState {
    #[default]
    Provisioning,
    Active,
    Suspended,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BackupRunOutcome {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedBackupPolicy {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    pub retention_copies: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_snapshot_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub runs_total: u64,
    pub failures_total: u64,
}

impl Default for ManagedBackupPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule: None,
            retention_copies: DEFAULT_BACKUP_RETENTION_COPIES,
            target: None,
            last_attempt_unix_ms: None,
            last_success_unix_ms: None,
            last_snapshot_path: None,
            last_error: None,
            runs_total: 0,
            failures_total: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ManagedMaintenancePolicy {
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_start_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_end_unix_ms: Option<u64>,
    #[serde(default)]
    pub allowed_mutations: Vec<String>,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedUpgradePlan {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_version: Option<String>,
    pub channel: String,
    pub strategy: String,
    pub state: UpgradeRolloutState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

impl Default for ManagedUpgradePlan {
    fn default() -> Self {
        Self {
            desired_version: None,
            observed_version: None,
            channel: "stable".to_string(),
            strategy: "rolling".to_string(),
            state: UpgradeRolloutState::Idle,
            requested_unix_ms: None,
            started_unix_ms: None,
            completed_unix_ms: None,
            notes: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedDeployment {
    pub id: String,
    pub display_name: String,
    pub region: String,
    pub plan: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_plane_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_plane_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_store_path: Option<String>,
    pub lifecycle: DeploymentLifecycleState,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub backup_policy: ManagedBackupPolicy,
    pub maintenance: ManagedMaintenancePolicy,
    pub upgrade: ManagedUpgradePlan,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedTenant {
    pub id: String,
    pub deployment_id: String,
    pub display_name: String,
    pub lifecycle: TenantLifecycleState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_limit_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingest_rate_limit_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_concurrency_limit: Option<u32>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedControlPlaneAuditEntry {
    pub seq: u64,
    pub unix_ms: u64,
    pub actor_id: String,
    pub actor_scope: String,
    pub operation: String,
    pub target_kind: String,
    pub target_id: String,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedDeploymentProvisionRequest {
    pub deployment_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub plan: Option<String>,
    #[serde(default)]
    pub control_plane_endpoint: Option<String>,
    #[serde(default)]
    pub data_plane_endpoint: Option<String>,
    #[serde(default)]
    pub object_store_path: Option<String>,
    #[serde(default)]
    pub lifecycle: Option<DeploymentLifecycleState>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedBackupPolicyApplyRequest {
    pub deployment_id: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub retention_copies: Option<u32>,
    #[serde(default)]
    pub target: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedBackupRunRecordRequest {
    pub deployment_id: String,
    pub outcome: BackupRunOutcome,
    #[serde(default)]
    pub completed_unix_ms: Option<u64>,
    #[serde(default)]
    pub snapshot_path: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedMaintenanceApplyRequest {
    pub deployment_id: String,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub window_start_unix_ms: Option<u64>,
    #[serde(default)]
    pub window_end_unix_ms: Option<u64>,
    #[serde(default)]
    pub allowed_mutations: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedUpgradeApplyRequest {
    pub deployment_id: String,
    #[serde(default)]
    pub desired_version: Option<String>,
    #[serde(default)]
    pub observed_version: Option<String>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub state: Option<UpgradeRolloutState>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedTenantApplyRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub deployment_id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub lifecycle: Option<TenantLifecycleState>,
    #[serde(default)]
    pub retention_days: Option<u32>,
    #[serde(default)]
    pub storage_limit_bytes: Option<u64>,
    #[serde(default)]
    pub ingest_rate_limit_per_sec: Option<u64>,
    #[serde(default)]
    pub query_concurrency_limit: Option<u32>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedTenantLifecycleRequest {
    pub tenant_id: String,
    pub lifecycle: TenantLifecycleState,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedControlPlaneAuditFilter {
    pub limit: usize,
    pub target_kind: Option<String>,
    pub target_id: Option<String>,
    pub operation: Option<String>,
}

impl ManagedControlPlaneAuditFilter {
    pub fn normalize(mut self) -> Self {
        if self.limit == 0 {
            self.limit = DEFAULT_AUDIT_QUERY_LIMIT;
        }
        self.target_kind = normalize_optional_field(self.target_kind);
        self.target_id = normalize_optional_field(self.target_id);
        self.operation = normalize_optional_field(self.operation);
        self
    }
}

impl ManagedControlPlane {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(data_path: Option<&Path>) -> Result<Self, String> {
        Self::open_with_disk_budget(data_path, None)
    }

    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, String> {
        let state_path = match data_path {
            Some(data_path) => {
                let directory = data_path.join(MANAGED_CONTROL_PLANE_DIR);
                if let Some(budget) = local_disk_budget.as_ref() {
                    budget
                        .create_dir_all_and_sync_parents(&directory)
                        .map_err(|err| {
                            format!(
                                "failed to durably create managed control-plane directory {}: {err}",
                                directory.display()
                            )
                        })?;
                } else {
                    fs::create_dir_all(&directory).map_err(|err| {
                        format!(
                            "failed to create managed control-plane directory {}: {err}",
                            directory.display()
                        )
                    })?;
                }
                let path = directory.join(MANAGED_CONTROL_PLANE_STATE_FILE);
                if let Some(budget) = local_disk_budget.as_ref() {
                    budget.cleanup_atomic_write_temps(&path).map_err(|err| {
                        format!(
                            "failed to clean managed control-plane temporary files for {}: {err}",
                            path.display()
                        )
                    })?;
                    budget.validate_managed_file_path(&path).map_err(|err| {
                        format!(
                            "failed to validate managed control-plane state {}: {err}",
                            path.display()
                        )
                    })?;
                }
                Some(path)
            }
            None if local_disk_budget.is_some() => {
                return Err(
                    "managed control plane cannot use a local disk budget without a data path"
                        .to_string(),
                )
            }
            None => None,
        };

        let state = if let Some(path) = state_path.as_deref() {
            if path.exists() {
                load_state_file(path)?
            } else {
                let state = ManagedControlPlaneStateFile::default();
                persist_state_file(path, &state, local_disk_budget.as_ref()).map_err(|err| {
                    format!(
                        "failed to initialize managed control-plane state {}: {err}",
                        path.display()
                    )
                })?;
                state
            }
        } else {
            ManagedControlPlaneStateFile::default()
        };

        Ok(Self {
            state_path,
            local_disk_budget,
            state: Mutex::new(state),
            request_runtimes: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            status_projection_output_string_materializations: AtomicU64::new(0),
        })
    }

    pub fn tenant_request_policy(&self, tenant_id: &str) -> Option<ManagedTenantRequestPolicy> {
        let tenant = self.tenant_snapshot(tenant_id)?;
        Some(ManagedTenantRequestPolicy {
            tenant,
            runtime: self.request_runtime_for(tenant_id),
        })
    }

    fn request_runtime_for(&self, tenant_id: &str) -> Arc<ManagedTenantRequestRuntime> {
        let mut runtimes = self
            .request_runtimes
            .lock()
            .expect("managed tenant runtime cache mutex should not be poisoned");
        Arc::clone(
            runtimes
                .entry(tenant_id.to_string())
                .or_insert_with(|| Arc::new(ManagedTenantRequestRuntime::default())),
        )
    }

    pub fn state_snapshot(&self) -> ManagedControlPlaneStateSnapshot {
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        ManagedControlPlaneStateSnapshot {
            status: status_snapshot_for_state(&state, self.state_path.as_deref()),
            deployments: state.deployments.values().cloned().collect(),
            tenants: state.tenants.values().cloned().collect(),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn status_snapshot(&self) -> ManagedControlPlaneStatusSnapshot {
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        status_snapshot_for_state(&state, self.state_path.as_deref())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn deployment_summaries(&self) -> Vec<ManagedDeploymentSummary> {
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        deployment_summaries_for_state(&state)
    }

    pub fn tenant_snapshot(&self, tenant_id: &str) -> Option<ManagedTenant> {
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        state.tenants.get(tenant_id).cloned()
    }

    /// Captures the exact managed-control-plane inputs used by direct TSDB status.
    ///
    /// The authoritative state is locked once so status counts, deployment summaries, and the
    /// optional current tenant all describe one generation. Every retained allocation is measured
    /// and reserved before any output String, Vec, or BTreeMap is materialized.
    pub(crate) fn status_projection_for_with_execution(
        &self,
        tenant_id: &str,
        execution: &QueryExecution,
    ) -> Result<AccountedManagedControlPlaneStatusProjection, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        execution.checkpoint()?;

        let state_path_len = self
            .state_path
            .as_deref()
            .map(displayed_managed_status_path_len);
        let mut peak_bytes =
            modeled_managed_status_vec_bytes::<ManagedDeploymentSummary>(state.deployments.len())
                .saturating_add(
                    state_path_len
                        .map(modeled_managed_status_len_bytes)
                        .unwrap_or(0),
                );

        let mut status_counts = ManagedStatusDerivedCounts::default();
        for (index, deployment) in state.deployments.values().enumerate() {
            checkpoint_managed_status(execution, index)?;
            status_counts.maintenance_active = status_counts
                .maintenance_active
                .saturating_add(usize::from(deployment.maintenance.active));
            status_counts.upgrade_rollouts_in_progress = status_counts
                .upgrade_rollouts_in_progress
                .saturating_add(usize::from(matches!(
                    deployment.upgrade.state,
                    UpgradeRolloutState::Pending
                        | UpgradeRolloutState::InProgress
                        | UpgradeRolloutState::Paused
                )));
            status_counts.backup_policies = status_counts
                .backup_policies
                .saturating_add(usize::from(deployment.backup_policy.enabled));
            peak_bytes = peak_bytes
                .saturating_add(modeled_managed_status_str_bytes(&deployment.id))
                .saturating_add(modeled_managed_status_str_bytes(&deployment.region))
                .saturating_add(modeled_managed_status_str_bytes(&deployment.plan))
                .saturating_add(
                    deployment
                        .upgrade
                        .desired_version
                        .as_deref()
                        .map(modeled_managed_status_str_bytes)
                        .unwrap_or(0),
                )
                .saturating_add(
                    deployment
                        .upgrade
                        .observed_version
                        .as_deref()
                        .map(modeled_managed_status_str_bytes)
                        .unwrap_or(0),
                );
        }

        for (index, tenant) in state.tenants.values().enumerate() {
            checkpoint_managed_status(execution, index)?;
            status_counts.active_tenants = status_counts.active_tenants.saturating_add(
                usize::from(tenant.lifecycle == TenantLifecycleState::Active),
            );
        }

        let source_tenant = state.tenants.get(tenant_id);
        if let Some(tenant) = source_tenant {
            execution.checkpoint()?;
            peak_bytes = peak_bytes
                .saturating_add(modeled_managed_status_str_bytes(&tenant.id))
                .saturating_add(modeled_managed_status_str_bytes(&tenant.deployment_id))
                .saturating_add(modeled_managed_status_str_bytes(&tenant.display_name))
                .saturating_add(
                    modeled_managed_status_btree_entries_bytes::<String, String>(
                        tenant.labels.len(),
                    ),
                )
                .saturating_add(
                    tenant
                        .lifecycle_note
                        .as_deref()
                        .map(modeled_managed_status_str_bytes)
                        .unwrap_or(0),
                );
            for (key, value) in &tenant.labels {
                execution.checkpoint()?;
                peak_bytes = peak_bytes
                    .saturating_add(modeled_managed_status_str_bytes(key))
                    .saturating_add(modeled_managed_status_str_bytes(value));
            }
        }

        let mut reservation = execution.reserve_memory(peak_bytes)?;
        execution.checkpoint()?;

        let state_path =
            self.state_path
                .as_deref()
                .zip(state_path_len)
                .map(|(path, displayed_len)| {
                    self.materialize_status_projection_path(path, displayed_len)
                });
        let status =
            status_snapshot_for_state_with_path_and_counts(&state, state_path, status_counts);

        let mut deployments = Vec::with_capacity(state.deployments.len());
        for (index, deployment) in state.deployments.values().enumerate() {
            checkpoint_managed_status(execution, index)?;
            let tenant_counts =
                deployment_tenant_counts_for_state_with_execution(&state, deployment, execution)?;
            deployments.push(deployment_summary_for_state_with_dynamic(
                deployment,
                tenant_counts,
                self.clone_status_projection_string(&deployment.id),
                self.clone_status_projection_string(&deployment.region),
                self.clone_status_projection_string(&deployment.plan),
                deployment
                    .upgrade
                    .desired_version
                    .as_deref()
                    .map(|value| self.clone_status_projection_string(value)),
                deployment
                    .upgrade
                    .observed_version
                    .as_deref()
                    .map(|value| self.clone_status_projection_string(value)),
            ));
        }

        let current_tenant = source_tenant
            .map(|tenant| self.clone_status_projection_tenant(tenant, execution))
            .transpose()?;
        let projection = ManagedControlPlaneStatusProjection {
            status,
            deployments,
            current_tenant,
        };
        reservation.resize(modeled_managed_status_projection_retained_bytes(
            &projection,
        ))?;
        drop(state);
        execution.checkpoint()?;

        Ok(AccountedManagedControlPlaneStatusProjection {
            projection,
            _reservation: reservation,
        })
    }

    fn clone_status_projection_tenant(
        &self,
        tenant: &ManagedTenant,
        execution: &QueryExecution,
    ) -> Result<ManagedTenant, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let mut labels = BTreeMap::new();
        for (key, value) in &tenant.labels {
            execution.checkpoint()?;
            labels.insert(
                self.clone_status_projection_string(key),
                self.clone_status_projection_string(value),
            );
        }
        Ok(ManagedTenant {
            id: self.clone_status_projection_string(&tenant.id),
            deployment_id: self.clone_status_projection_string(&tenant.deployment_id),
            display_name: self.clone_status_projection_string(&tenant.display_name),
            lifecycle: tenant.lifecycle,
            retention_days: tenant.retention_days,
            storage_limit_bytes: tenant.storage_limit_bytes,
            ingest_rate_limit_per_sec: tenant.ingest_rate_limit_per_sec,
            query_concurrency_limit: tenant.query_concurrency_limit,
            labels,
            created_unix_ms: tenant.created_unix_ms,
            updated_unix_ms: tenant.updated_unix_ms,
            lifecycle_note: tenant
                .lifecycle_note
                .as_deref()
                .map(|value| self.clone_status_projection_string(value)),
        })
    }

    fn clone_status_projection_string(&self, value: &str) -> String {
        #[cfg(test)]
        self.status_projection_output_string_materializations
            .fetch_add(1, AtomicOrdering::Relaxed);
        clone_managed_status_string(value)
    }

    fn materialize_status_projection_path(&self, path: &Path, displayed_len: usize) -> String {
        #[cfg(test)]
        self.status_projection_output_string_materializations
            .fetch_add(1, AtomicOrdering::Relaxed);
        let mut output = String::with_capacity(displayed_len);
        write!(&mut output, "{}", path.display())
            .expect("formatting a managed control-plane state path into a String should succeed");
        output
    }

    #[cfg(test)]
    fn reset_status_projection_output_string_materializations(&self) {
        self.status_projection_output_string_materializations
            .store(0, AtomicOrdering::Relaxed);
    }

    #[cfg(test)]
    fn status_projection_output_string_materializations(&self) -> u64 {
        self.status_projection_output_string_materializations
            .load(AtomicOrdering::Relaxed)
    }

    pub fn query_audit(
        &self,
        filter: ManagedControlPlaneAuditFilter,
    ) -> Vec<ManagedControlPlaneAuditEntry> {
        let filter = filter.normalize();
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        state
            .audit_entries
            .iter()
            .rev()
            .filter(|entry| {
                filter
                    .target_kind
                    .as_ref()
                    .map(|value| entry.target_kind == *value)
                    .unwrap_or(true)
                    && filter
                        .target_id
                        .as_ref()
                        .map(|value| entry.target_id == *value)
                        .unwrap_or(true)
                    && filter
                        .operation
                        .as_ref()
                        .map(|value| entry.operation == *value)
                        .unwrap_or(true)
            })
            .take(filter.limit)
            .cloned()
            .collect()
    }

    pub fn provision_deployment(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedDeploymentProvisionRequest,
    ) -> Result<ManagedDeployment, ManagedControlPlaneMutationError> {
        let deployment_id = validate_resource_id("deploymentId", &request.deployment_id)?;
        self.mutate(
            actor,
            "provision_deployment",
            "deployment",
            &deployment_id,
            |state, now| {
                let existing = state.deployments.get(&deployment_id).cloned();
                let mut deployment = existing.clone().unwrap_or_else(|| ManagedDeployment {
                    id: deployment_id.clone(),
                    display_name: deployment_id.clone(),
                    region: String::new(),
                    plan: String::new(),
                    control_plane_endpoint: None,
                    data_plane_endpoint: None,
                    object_store_path: None,
                    lifecycle: DeploymentLifecycleState::Requested,
                    labels: BTreeMap::new(),
                    created_unix_ms: now,
                    updated_unix_ms: now,
                    backup_policy: ManagedBackupPolicy::default(),
                    maintenance: ManagedMaintenancePolicy::default(),
                    upgrade: ManagedUpgradePlan::default(),
                });

                deployment.display_name = match request.display_name.clone() {
                    Some(value) => validate_non_empty_field("displayName", value)?,
                    None if existing.is_none() => deployment_id.clone(),
                    None => deployment.display_name,
                };
                deployment.region = match request.region.clone() {
                    Some(value) => validate_non_empty_field("region", value)?,
                    None if existing.is_none() => {
                        return Err(
                            "region is required when provisioning a new deployment".to_string()
                        )
                    }
                    None => deployment.region,
                };
                deployment.plan = match request.plan.clone() {
                    Some(value) => validate_non_empty_field("plan", value)?,
                    None if existing.is_none() => {
                        return Err(
                            "plan is required when provisioning a new deployment".to_string()
                        )
                    }
                    None => deployment.plan,
                };
                if let Some(labels) = request.labels.clone() {
                    deployment.labels = normalize_labels(labels, "labels")?;
                }
                if let Some(lifecycle) = request.lifecycle {
                    if matches!(lifecycle, DeploymentLifecycleState::Decommissioned)
                        && state.tenants.values().any(|tenant| {
                            tenant.deployment_id == deployment_id
                                && tenant.lifecycle != TenantLifecycleState::Deleted
                        })
                    {
                        return Err(format!(
                            "deployment '{deployment_id}' still has tenants that are not deleted"
                        ));
                    }
                    deployment.lifecycle = lifecycle;
                }
                if request.control_plane_endpoint.is_some() {
                    deployment.control_plane_endpoint =
                        normalize_optional_field(request.control_plane_endpoint.clone());
                }
                if request.data_plane_endpoint.is_some() {
                    deployment.data_plane_endpoint =
                        normalize_optional_field(request.data_plane_endpoint.clone());
                }
                if request.object_store_path.is_some() {
                    deployment.object_store_path =
                        normalize_optional_field(request.object_store_path.clone());
                }
                deployment.updated_unix_ms = now;

                state
                    .deployments
                    .insert(deployment_id.clone(), deployment.clone());
                Ok(deployment)
            },
        )
    }

    pub fn apply_backup_policy(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedBackupPolicyApplyRequest,
    ) -> Result<ManagedDeployment, ManagedControlPlaneMutationError> {
        let deployment_id = validate_resource_id("deploymentId", &request.deployment_id)?;
        self.mutate(actor, "apply_backup_policy", "deployment", &deployment_id, |state, now| {
            let deployment = state
                .deployments
                .get_mut(&deployment_id)
                .ok_or_else(|| format!("unknown deployment '{deployment_id}'"))?;
            let enabled = request.enabled.unwrap_or(deployment.backup_policy.enabled);
            let schedule = if request.schedule.is_some() {
                normalize_optional_field(request.schedule.clone())
            } else {
                deployment.backup_policy.schedule.clone()
            };
            let target = if request.target.is_some() {
                normalize_optional_field(request.target.clone())
            } else {
                deployment.backup_policy.target.clone()
            };
            let retention_copies =
                request
                    .retention_copies
                    .unwrap_or(deployment.backup_policy.retention_copies);
            if retention_copies == 0 {
                return Err("retentionCopies must be greater than zero".to_string());
            }
            if enabled {
                if schedule.is_none() {
                    return Err(format!(
                        "deployment '{deployment_id}' backup policy requires a schedule when enabled"
                    ));
                }
                if target.is_none() {
                    return Err(format!(
                        "deployment '{deployment_id}' backup policy requires a target when enabled"
                    ));
                }
            }
            deployment.backup_policy.enabled = enabled;
            deployment.backup_policy.schedule = schedule;
            deployment.backup_policy.target = target;
            deployment.backup_policy.retention_copies = retention_copies;
            deployment.updated_unix_ms = now;
            Ok(deployment.clone())
        })
    }

    pub fn record_backup_run(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedBackupRunRecordRequest,
    ) -> Result<ManagedDeployment, ManagedControlPlaneMutationError> {
        let deployment_id = validate_resource_id("deploymentId", &request.deployment_id)?;
        self.mutate(
            actor,
            "record_backup_run",
            "deployment",
            &deployment_id,
            |state, now| {
                let deployment = state
                    .deployments
                    .get_mut(&deployment_id)
                    .ok_or_else(|| format!("unknown deployment '{deployment_id}'"))?;
                if !deployment.backup_policy.enabled {
                    return Err(format!(
                        "deployment '{deployment_id}' does not have an enabled backup policy"
                    ));
                }
                let completed_unix_ms = request.completed_unix_ms.unwrap_or(now);
                deployment.backup_policy.last_attempt_unix_ms = Some(completed_unix_ms);
                deployment.backup_policy.runs_total =
                    deployment.backup_policy.runs_total.saturating_add(1);
                match request.outcome {
                    BackupRunOutcome::Success => {
                        let snapshot_path = normalize_optional_field(request.snapshot_path.clone())
                            .ok_or_else(|| {
                                "snapshotPath is required when recording a successful backup run"
                                    .to_string()
                            })?;
                        deployment.backup_policy.last_success_unix_ms = Some(completed_unix_ms);
                        deployment.backup_policy.last_snapshot_path = Some(snapshot_path);
                        deployment.backup_policy.last_error = None;
                    }
                    BackupRunOutcome::Failed => {
                        deployment.backup_policy.failures_total =
                            deployment.backup_policy.failures_total.saturating_add(1);
                        deployment.backup_policy.last_error = Some(
                            normalize_optional_field(request.error.clone()).unwrap_or_else(|| {
                                "backup run failed without a recorded error".to_string()
                            }),
                        );
                    }
                }
                deployment.updated_unix_ms = now;
                Ok(deployment.clone())
            },
        )
    }

    pub fn apply_maintenance(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedMaintenanceApplyRequest,
    ) -> Result<ManagedDeployment, ManagedControlPlaneMutationError> {
        let deployment_id = validate_resource_id("deploymentId", &request.deployment_id)?;
        self.mutate(
            actor,
            "apply_maintenance",
            "deployment",
            &deployment_id,
            |state, now| {
                let deployment = state
                    .deployments
                    .get_mut(&deployment_id)
                    .ok_or_else(|| format!("unknown deployment '{deployment_id}'"))?;
                if let Some(active) = request.active {
                    deployment.maintenance.active = active;
                }
                if request.reason.is_some() {
                    deployment.maintenance.reason =
                        normalize_optional_field(request.reason.clone());
                }
                if request.window_start_unix_ms.is_some() {
                    deployment.maintenance.window_start_unix_ms = request.window_start_unix_ms;
                }
                if request.window_end_unix_ms.is_some() {
                    deployment.maintenance.window_end_unix_ms = request.window_end_unix_ms;
                }
                if let Some(allowed_mutations) = request.allowed_mutations.clone() {
                    deployment.maintenance.allowed_mutations =
                        normalize_string_list(allowed_mutations, "allowedMutations")?;
                }
                deployment.maintenance.updated_unix_ms = now;
                deployment.updated_unix_ms = now;
                Ok(deployment.clone())
            },
        )
    }

    pub fn apply_upgrade(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedUpgradeApplyRequest,
    ) -> Result<ManagedDeployment, ManagedControlPlaneMutationError> {
        let deployment_id = validate_resource_id("deploymentId", &request.deployment_id)?;
        self.mutate(
            actor,
            "apply_upgrade",
            "deployment",
            &deployment_id,
            |state, now| {
                let deployment = state
                    .deployments
                    .get_mut(&deployment_id)
                    .ok_or_else(|| format!("unknown deployment '{deployment_id}'"))?;
                let desired_version = if request.desired_version.is_some() {
                    normalize_optional_field(request.desired_version.clone())
                } else {
                    deployment.upgrade.desired_version.clone()
                };
                if desired_version.is_none() {
                    return Err(format!(
                        "deployment '{deployment_id}' upgrade state requires desiredVersion"
                    ));
                }
                let desired_version_changed = desired_version != deployment.upgrade.desired_version;
                deployment.upgrade.desired_version = desired_version;
                if request.observed_version.is_some() {
                    deployment.upgrade.observed_version =
                        normalize_optional_field(request.observed_version.clone());
                }
                if let Some(channel) = request.channel.clone() {
                    deployment.upgrade.channel = validate_non_empty_field("channel", channel)?;
                }
                if let Some(strategy) = request.strategy.clone() {
                    deployment.upgrade.strategy = validate_non_empty_field("strategy", strategy)?;
                }
                if request.notes.is_some() {
                    deployment.upgrade.notes = normalize_optional_field(request.notes.clone());
                }

                let next_state = request.state.unwrap_or({
                    if desired_version_changed {
                        UpgradeRolloutState::Pending
                    } else {
                        deployment.upgrade.state
                    }
                });
                if desired_version_changed || matches!(next_state, UpgradeRolloutState::Pending) {
                    deployment.upgrade.requested_unix_ms = Some(now);
                }
                match next_state {
                    UpgradeRolloutState::Idle => {
                        deployment.upgrade.started_unix_ms = None;
                        deployment.upgrade.completed_unix_ms = None;
                    }
                    UpgradeRolloutState::Pending => {
                        deployment.upgrade.completed_unix_ms = None;
                    }
                    UpgradeRolloutState::InProgress => {
                        deployment.upgrade.started_unix_ms.get_or_insert(now);
                        deployment.upgrade.completed_unix_ms = None;
                    }
                    UpgradeRolloutState::Complete => {
                        deployment.upgrade.started_unix_ms.get_or_insert(now);
                        deployment.upgrade.completed_unix_ms = Some(now);
                        if deployment.upgrade.observed_version.is_none() {
                            deployment.upgrade.observed_version =
                                deployment.upgrade.desired_version.clone();
                        }
                    }
                    UpgradeRolloutState::Paused => {
                        deployment.upgrade.started_unix_ms.get_or_insert(now);
                        deployment.upgrade.completed_unix_ms = None;
                    }
                    UpgradeRolloutState::Cancelled => {
                        deployment.upgrade.completed_unix_ms = Some(now);
                    }
                }
                deployment.upgrade.state = next_state;
                deployment.updated_unix_ms = now;
                Ok(deployment.clone())
            },
        )
    }

    pub fn apply_tenant(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedTenantApplyRequest,
    ) -> Result<ManagedTenant, ManagedControlPlaneMutationError> {
        let tenant_id = validate_resource_id("tenantId", &request.tenant_id)?;
        self.mutate(actor, "apply_tenant", "tenant", &tenant_id, |state, now| {
            let existing = state.tenants.get(&tenant_id).cloned();
            let mut tenant = existing.clone().unwrap_or_else(|| ManagedTenant {
                id: tenant_id.clone(),
                deployment_id: String::new(),
                display_name: tenant_id.clone(),
                lifecycle: TenantLifecycleState::Provisioning,
                retention_days: None,
                storage_limit_bytes: None,
                ingest_rate_limit_per_sec: None,
                query_concurrency_limit: None,
                labels: BTreeMap::new(),
                created_unix_ms: now,
                updated_unix_ms: now,
                lifecycle_note: None,
            });

            let deployment_id = if let Some(deployment_id) = request.deployment_id.clone() {
                validate_resource_id("deploymentId", &deployment_id)?
            } else if existing.is_none() {
                return Err("deploymentId is required when creating a managed tenant".to_string());
            } else {
                tenant.deployment_id.clone()
            };
            let deployment = state
                .deployments
                .get(&deployment_id)
                .ok_or_else(|| format!("unknown deployment '{deployment_id}'"))?;
            let lifecycle = request.lifecycle.unwrap_or(tenant.lifecycle);
            if matches!(
                deployment.lifecycle,
                DeploymentLifecycleState::Decommissioning | DeploymentLifecycleState::Decommissioned
            ) && !matches!(
                lifecycle,
                TenantLifecycleState::Deleting | TenantLifecycleState::Deleted
            ) {
                return Err(format!(
                    "deployment '{deployment_id}' is not accepting active tenants"
                ));
            }
            if matches!(lifecycle, TenantLifecycleState::Active)
                && deployment.lifecycle != DeploymentLifecycleState::Ready
            {
                return Err(format!(
                    "tenant '{tenant_id}' cannot become active until deployment '{deployment_id}' is ready"
                ));
            }

            tenant.deployment_id = deployment_id;
            tenant.display_name = match request.display_name.clone() {
                Some(value) => validate_non_empty_field("displayName", value)?,
                None if existing.is_none() => tenant_id.clone(),
                None => tenant.display_name,
            };
            tenant.lifecycle = lifecycle;
            tenant.retention_days = request.retention_days.or(tenant.retention_days);
            tenant.storage_limit_bytes =
                request.storage_limit_bytes.or(tenant.storage_limit_bytes);
            tenant.ingest_rate_limit_per_sec = request
                .ingest_rate_limit_per_sec
                .or(tenant.ingest_rate_limit_per_sec);
            tenant.query_concurrency_limit = request
                .query_concurrency_limit
                .or(tenant.query_concurrency_limit);
            if let Some(labels) = request.labels.clone() {
                tenant.labels = normalize_labels(labels, "labels")?;
            }
            tenant.updated_unix_ms = now;

            state.tenants.insert(tenant_id.clone(), tenant.clone());
            Ok(tenant)
        })
    }

    pub fn apply_tenant_lifecycle(
        &self,
        actor: ManagedControlPlaneActor,
        request: ManagedTenantLifecycleRequest,
    ) -> Result<ManagedTenant, ManagedControlPlaneMutationError> {
        let tenant_id = validate_resource_id("tenantId", &request.tenant_id)?;
        self.mutate(actor, "apply_tenant_lifecycle", "tenant", &tenant_id, |state, now| {
            let deployment_id = state
                .tenants
                .get(&tenant_id)
                .map(|tenant| tenant.deployment_id.clone())
                .ok_or_else(|| format!("unknown tenant '{tenant_id}'"))?;
            let tenant = state
                .tenants
                .get_mut(&tenant_id)
                .ok_or_else(|| format!("unknown tenant '{tenant_id}'"))?;
            if matches!(request.lifecycle, TenantLifecycleState::Active) {
                let deployment = state
                    .deployments
                    .get(&deployment_id)
                    .ok_or_else(|| format!("unknown deployment '{deployment_id}'"))?;
                if deployment.lifecycle != DeploymentLifecycleState::Ready {
                    return Err(format!(
                        "tenant '{tenant_id}' cannot become active until deployment '{deployment_id}' is ready"
                    ));
                }
            }
            tenant.lifecycle = request.lifecycle;
            if request.note.is_some() {
                tenant.lifecycle_note = normalize_optional_field(request.note.clone());
            }
            tenant.updated_unix_ms = now;
            Ok(tenant.clone())
        })
    }

    fn mutate<T, F>(
        &self,
        actor: ManagedControlPlaneActor,
        operation: &str,
        target_kind: &str,
        target_id: &str,
        f: F,
    ) -> Result<T, ManagedControlPlaneMutationError>
    where
        F: FnOnce(&mut ManagedControlPlaneStateFile, u64) -> Result<T, String>,
    {
        let mut state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        let now = unix_timestamp_millis();
        let current = state.clone();
        let mut candidate = current.clone();
        match f(&mut candidate, now) {
            Ok(value) => {
                candidate.updated_unix_ms = now;
                append_audit_entry(
                    &mut candidate,
                    now,
                    ManagedControlPlaneAuditEntryInput {
                        actor,
                        operation: operation.to_string(),
                        target_kind: target_kind.to_string(),
                        target_id: target_id.to_string(),
                        outcome: "success".to_string(),
                        detail: None,
                    },
                );
                if let Some(path) = self.state_path.as_deref() {
                    persist_state_file(path, &candidate, self.local_disk_budget.as_ref())?;
                }
                *state = candidate;
                Ok(value)
            }
            Err(err) => {
                let mut audited = current;
                audited.updated_unix_ms = now;
                append_audit_entry(
                    &mut audited,
                    now,
                    ManagedControlPlaneAuditEntryInput {
                        actor,
                        operation: operation.to_string(),
                        target_kind: target_kind.to_string(),
                        target_id: target_id.to_string(),
                        outcome: "error".to_string(),
                        detail: Some(err.clone()),
                    },
                );
                if let Some(path) = self.state_path.as_deref() {
                    persist_state_file(path, &audited, self.local_disk_budget.as_ref())?;
                }
                *state = audited;
                Err(ManagedControlPlaneMutationError::Rejected(err))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ManagedStatusDerivedCounts {
    active_tenants: usize,
    maintenance_active: usize,
    upgrade_rollouts_in_progress: usize,
    backup_policies: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct ManagedDeploymentTenantCounts {
    total: usize,
    active: usize,
}

fn checkpoint_managed_status(
    execution: &QueryExecution,
    index: usize,
) -> Result<(), tsink::QueryBudgetError> {
    if index.is_multiple_of(MANAGED_STATUS_CHECKPOINT_INTERVAL) {
        execution.checkpoint()?;
    }
    Ok(())
}

#[derive(Default)]
struct ManagedStatusPathLen {
    bytes: usize,
}

impl std::fmt::Write for ManagedStatusPathLen {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.bytes = self.bytes.saturating_add(value.len());
        Ok(())
    }
}

fn displayed_managed_status_path_len(path: &Path) -> usize {
    let mut counter = ManagedStatusPathLen::default();
    write!(&mut counter, "{}", path.display())
        .expect("counting formatted managed control-plane path bytes should succeed");
    counter.bytes
}

fn modeled_managed_status_len_bytes(len: usize) -> u64 {
    if len == 0 {
        return 0;
    }
    u64::try_from(len)
        .unwrap_or(u64::MAX)
        .saturating_add(MANAGED_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_managed_status_str_bytes(value: &str) -> u64 {
    modeled_managed_status_len_bytes(value.len())
}

fn modeled_managed_status_string_bytes(value: &String) -> u64 {
    modeled_managed_status_len_bytes(value.capacity())
}

fn modeled_managed_status_vec_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
        .saturating_add(MANAGED_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_managed_status_btree_entries_bytes<K, V>(entries: usize) -> u64 {
    u64::try_from(entries).unwrap_or(u64::MAX).saturating_mul(
        u64::try_from(std::mem::size_of::<K>())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(std::mem::size_of::<V>()).unwrap_or(u64::MAX))
            .saturating_add(MANAGED_STATUS_BTREE_ENTRY_ALLOWANCE_BYTES),
    )
}

fn clone_managed_status_string(value: &str) -> String {
    let mut cloned = String::with_capacity(value.len());
    cloned.push_str(value);
    cloned
}

fn modeled_managed_tenant_retained_bytes(tenant: &ManagedTenant) -> u64 {
    modeled_managed_status_string_bytes(&tenant.id)
        .saturating_add(modeled_managed_status_string_bytes(&tenant.deployment_id))
        .saturating_add(modeled_managed_status_string_bytes(&tenant.display_name))
        .saturating_add(
            modeled_managed_status_btree_entries_bytes::<String, String>(tenant.labels.len()),
        )
        .saturating_add(tenant.labels.iter().fold(0u64, |bytes, (key, value)| {
            bytes
                .saturating_add(modeled_managed_status_string_bytes(key))
                .saturating_add(modeled_managed_status_string_bytes(value))
        }))
        .saturating_add(
            tenant
                .lifecycle_note
                .as_ref()
                .map(modeled_managed_status_string_bytes)
                .unwrap_or(0),
        )
}

fn modeled_managed_status_projection_retained_bytes(
    projection: &ManagedControlPlaneStatusProjection,
) -> u64 {
    projection
        .status
        .state_path
        .as_ref()
        .map(modeled_managed_status_string_bytes)
        .unwrap_or(0)
        .saturating_add(
            modeled_managed_status_vec_bytes::<ManagedDeploymentSummary>(
                projection.deployments.capacity(),
            ),
        )
        .saturating_add(
            projection
                .deployments
                .iter()
                .fold(0u64, |bytes, deployment| {
                    bytes
                        .saturating_add(modeled_managed_status_string_bytes(&deployment.id))
                        .saturating_add(modeled_managed_status_string_bytes(&deployment.region))
                        .saturating_add(modeled_managed_status_string_bytes(&deployment.plan))
                        .saturating_add(
                            deployment
                                .desired_version
                                .as_ref()
                                .map(modeled_managed_status_string_bytes)
                                .unwrap_or(0),
                        )
                        .saturating_add(
                            deployment
                                .observed_version
                                .as_ref()
                                .map(modeled_managed_status_string_bytes)
                                .unwrap_or(0),
                        )
                }),
        )
        .saturating_add(
            projection
                .current_tenant
                .as_ref()
                .map(modeled_managed_tenant_retained_bytes)
                .unwrap_or(0),
        )
}

fn status_snapshot_for_state(
    state: &ManagedControlPlaneStateFile,
    state_path: Option<&Path>,
) -> ManagedControlPlaneStatusSnapshot {
    status_snapshot_for_state_with_path(state, state_path.map(|path| path.display().to_string()))
}

fn status_snapshot_for_state_with_path(
    state: &ManagedControlPlaneStateFile,
    state_path: Option<String>,
) -> ManagedControlPlaneStatusSnapshot {
    status_snapshot_for_state_with_path_and_counts(
        state,
        state_path,
        managed_status_derived_counts_for_state(state),
    )
}

fn managed_status_derived_counts_for_state(
    state: &ManagedControlPlaneStateFile,
) -> ManagedStatusDerivedCounts {
    ManagedStatusDerivedCounts {
        active_tenants: state
            .tenants
            .values()
            .filter(|tenant| tenant.lifecycle == TenantLifecycleState::Active)
            .count(),
        maintenance_active: state
            .deployments
            .values()
            .filter(|deployment| deployment.maintenance.active)
            .count(),
        upgrade_rollouts_in_progress: state
            .deployments
            .values()
            .filter(|deployment| {
                matches!(
                    deployment.upgrade.state,
                    UpgradeRolloutState::Pending
                        | UpgradeRolloutState::InProgress
                        | UpgradeRolloutState::Paused
                )
            })
            .count(),
        backup_policies: state
            .deployments
            .values()
            .filter(|deployment| deployment.backup_policy.enabled)
            .count(),
    }
}

fn status_snapshot_for_state_with_path_and_counts(
    state: &ManagedControlPlaneStateFile,
    state_path: Option<String>,
    counts: ManagedStatusDerivedCounts,
) -> ManagedControlPlaneStatusSnapshot {
    ManagedControlPlaneStatusSnapshot {
        durable: state_path.is_some(),
        state_path,
        deployments_total: u64::try_from(state.deployments.len()).unwrap_or(u64::MAX),
        tenants_total: u64::try_from(state.tenants.len()).unwrap_or(u64::MAX),
        active_tenants_total: u64::try_from(counts.active_tenants).unwrap_or(u64::MAX),
        maintenance_active_total: u64::try_from(counts.maintenance_active).unwrap_or(u64::MAX),
        upgrade_rollouts_in_progress_total: u64::try_from(counts.upgrade_rollouts_in_progress)
            .unwrap_or(u64::MAX),
        backup_policies_total: u64::try_from(counts.backup_policies).unwrap_or(u64::MAX),
        audit_records_total: u64::try_from(state.audit_entries.len()).unwrap_or(u64::MAX),
        updated_unix_ms: state.updated_unix_ms,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn deployment_summaries_for_state(
    state: &ManagedControlPlaneStateFile,
) -> Vec<ManagedDeploymentSummary> {
    state
        .deployments
        .values()
        .map(|deployment| {
            deployment_summary_for_state_with_dynamic(
                deployment,
                deployment_tenant_counts_for_state(state, deployment),
                deployment.id.clone(),
                deployment.region.clone(),
                deployment.plan.clone(),
                deployment.upgrade.desired_version.clone(),
                deployment.upgrade.observed_version.clone(),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn deployment_summary_for_state_with_dynamic(
    deployment: &ManagedDeployment,
    tenant_counts: ManagedDeploymentTenantCounts,
    id: String,
    region: String,
    plan: String,
    desired_version: Option<String>,
    observed_version: Option<String>,
) -> ManagedDeploymentSummary {
    ManagedDeploymentSummary {
        id,
        region,
        plan,
        lifecycle: deployment.lifecycle,
        tenant_count: u64::try_from(tenant_counts.total).unwrap_or(u64::MAX),
        active_tenant_count: u64::try_from(tenant_counts.active).unwrap_or(u64::MAX),
        backup_enabled: deployment.backup_policy.enabled,
        maintenance_active: deployment.maintenance.active,
        upgrade_state: deployment.upgrade.state,
        desired_version,
        observed_version,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn deployment_tenant_counts_for_state(
    state: &ManagedControlPlaneStateFile,
    deployment: &ManagedDeployment,
) -> ManagedDeploymentTenantCounts {
    state.tenants.values().fold(
        ManagedDeploymentTenantCounts::default(),
        |mut counts, tenant| {
            if tenant.deployment_id == deployment.id {
                counts.total = counts.total.saturating_add(1);
                if tenant.lifecycle == TenantLifecycleState::Active {
                    counts.active = counts.active.saturating_add(1);
                }
            }
            counts
        },
    )
}

fn deployment_tenant_counts_for_state_with_execution(
    state: &ManagedControlPlaneStateFile,
    deployment: &ManagedDeployment,
    execution: &QueryExecution,
) -> Result<ManagedDeploymentTenantCounts, tsink::QueryBudgetError> {
    let mut counts = ManagedDeploymentTenantCounts::default();
    for (index, tenant) in state.tenants.values().enumerate() {
        checkpoint_managed_status(execution, index)?;
        if tenant.deployment_id == deployment.id {
            counts.total = counts.total.saturating_add(1);
            if tenant.lifecycle == TenantLifecycleState::Active {
                counts.active = counts.active.saturating_add(1);
            }
        }
    }
    Ok(counts)
}

impl ManagedTenantRequestPolicy {
    pub fn authorize(&self) -> Result<(), TenantRequestError> {
        match self.tenant.lifecycle {
            TenantLifecycleState::Active => Ok(()),
            TenantLifecycleState::Provisioning => Err(managed_tenant_rejected(
                403,
                "tenant_managed_lifecycle_blocked",
                format!(
                    "tenant '{}' is provisioning and not accepting data-plane traffic",
                    self.tenant.id
                ),
            )),
            TenantLifecycleState::Suspended => Err(managed_tenant_rejected(
                403,
                "tenant_managed_lifecycle_blocked",
                format!(
                    "tenant '{}' is suspended and not accepting data-plane traffic",
                    self.tenant.id
                ),
            )),
            TenantLifecycleState::Deleting => Err(managed_tenant_rejected(
                403,
                "tenant_managed_lifecycle_blocked",
                format!(
                    "tenant '{}' is deleting and not accepting data-plane traffic",
                    self.tenant.id
                ),
            )),
            TenantLifecycleState::Deleted => Err(managed_tenant_rejected(
                403,
                "tenant_managed_lifecycle_blocked",
                format!(
                    "tenant '{}' is deleted and not accepting data-plane traffic",
                    self.tenant.id
                ),
            )),
        }
    }

    pub fn admit(
        &self,
        surface: TenantAdmissionSurface,
        requested_units: usize,
        usage_accounting: Option<&UsageAccounting>,
    ) -> Result<ManagedTenantRequestGuard, TenantRequestError> {
        self.authorize()?;
        let mut permits = Vec::new();
        match surface {
            TenantAdmissionSurface::Ingest => {
                if let Some(limit) = self.tenant.storage_limit_bytes {
                    let current_bytes = usage_accounting
                        .and_then(|accounting| {
                            accounting.latest_storage_snapshot_for(&self.tenant.id)
                        })
                        .map(|snapshot| snapshot.logical_storage_bytes)
                        .unwrap_or(0);
                    if current_bytes >= limit {
                        return Err(managed_tenant_rejected(
                            413,
                            "tenant_managed_storage_limit_exceeded",
                            format!(
                                "tenant '{}' exceeded managed storage limit: {current_bytes} >= {limit}",
                                self.tenant.id
                            ),
                        ));
                    }
                }

                if let Some(limit) = self.tenant.ingest_rate_limit_per_sec {
                    let now = unix_timestamp_millis();
                    let mut recent = self
                        .runtime
                        .recent_ingest_units
                        .lock()
                        .expect("managed tenant ingest admission mutex should not be poisoned");
                    trim_managed_ingest_window(&mut recent, now);
                    let used_units = recent.iter().map(|entry| entry.units).sum::<u64>();
                    let requested_units_u64 = u64::try_from(requested_units).unwrap_or(u64::MAX);
                    if used_units.saturating_add(requested_units_u64) > limit {
                        return Err(managed_tenant_rejected(
                            429,
                            "tenant_managed_ingest_rate_limit_exceeded",
                            format!(
                                "tenant '{}' exceeded managed ingest rate limit: {} > {} rows/sec",
                                self.tenant.id,
                                used_units.saturating_add(requested_units_u64),
                                limit
                            ),
                        ));
                    }
                    if requested_units_u64 > 0 {
                        recent.push_back(ManagedTenantIngestWindowEntry {
                            unix_ms: now,
                            units: requested_units_u64,
                        });
                    }
                }
            }
            TenantAdmissionSurface::Query => {
                if let Some(limit) = self.tenant.query_concurrency_limit {
                    let limit = u64::from(limit);
                    let requested_units_u64 =
                        u64::try_from(requested_units.max(1)).unwrap_or(u64::MAX);
                    let mut active_units = self
                        .runtime
                        .active_query_units
                        .lock()
                        .expect("managed tenant query admission mutex should not be poisoned");
                    if active_units.saturating_add(requested_units_u64) > limit {
                        return Err(managed_tenant_rejected(
                            429,
                            "tenant_managed_query_concurrency_limit_exceeded",
                            format!(
                                "tenant '{}' exceeded managed query concurrency limit: {} > {}",
                                self.tenant.id,
                                active_units.saturating_add(requested_units_u64),
                                limit
                            ),
                        ));
                    }
                    *active_units = active_units.saturating_add(requested_units_u64);
                    permits.push(ManagedTenantAdmissionPermit::QueryConcurrency {
                        runtime: Arc::clone(&self.runtime),
                        units: requested_units_u64,
                    });
                }
            }
            TenantAdmissionSurface::Metadata | TenantAdmissionSurface::Retention => {}
        }

        Ok(ManagedTenantRequestGuard { _permits: permits })
    }
}

fn managed_tenant_rejected(status: u16, code: &'static str, message: String) -> TenantRequestError {
    TenantRequestError::Rejected {
        status,
        code,
        message,
    }
}

fn trim_managed_ingest_window(
    recent: &mut VecDeque<ManagedTenantIngestWindowEntry>,
    now_unix_ms: u64,
) {
    while recent.front().is_some_and(|entry| {
        entry
            .unix_ms
            .saturating_add(MANAGED_TENANT_INGEST_WINDOW_MS)
            <= now_unix_ms
    }) {
        recent.pop_front();
    }
}

struct ManagedControlPlaneAuditEntryInput {
    actor: ManagedControlPlaneActor,
    operation: String,
    target_kind: String,
    target_id: String,
    outcome: String,
    detail: Option<String>,
}

fn append_audit_entry(
    state: &mut ManagedControlPlaneStateFile,
    now: u64,
    input: ManagedControlPlaneAuditEntryInput,
) {
    state.last_audit_seq = state.last_audit_seq.saturating_add(1);
    state.audit_entries.push(ManagedControlPlaneAuditEntry {
        seq: state.last_audit_seq,
        unix_ms: now,
        actor_id: input.actor.id,
        actor_scope: input.actor.scope,
        operation: input.operation,
        target_kind: input.target_kind,
        target_id: input.target_id,
        outcome: input.outcome,
        detail: input.detail,
    });
}

fn load_state_file(path: &Path) -> Result<ManagedControlPlaneStateFile, String> {
    let raw = fs::read_to_string(path).map_err(|err| {
        format!(
            "failed to read managed control-plane state {}: {err}",
            path.display()
        )
    })?;
    let state: ManagedControlPlaneStateFile = serde_json::from_str(&raw).map_err(|err| {
        format!(
            "failed to parse managed control-plane state {}: {err}",
            path.display()
        )
    })?;
    if state.magic != MANAGED_CONTROL_PLANE_MAGIC {
        return Err(format!(
            "managed control-plane state {} has unsupported magic '{}'",
            path.display(),
            state.magic
        ));
    }
    if state.schema_version != MANAGED_CONTROL_PLANE_SCHEMA_VERSION {
        return Err(format!(
            "managed control-plane state {} has unsupported schema version {}",
            path.display(),
            state.schema_version
        ));
    }
    Ok(state)
}

fn persist_state_file(
    path: &Path,
    state: &ManagedControlPlaneStateFile,
    local_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
) -> tsink::Result<()> {
    let encoded = serde_json::to_vec_pretty(state)?;

    if let Some(local_disk_budget) = local_disk_budget {
        return local_disk_budget.write_file_atomically_and_sync_parent(
            path,
            &encoded,
            tsink::DiskCategory::ServerState,
        );
    }

    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, encoded).map_err(|source| tsink::TsinkError::IoWithPath {
        path: temp_path.clone(),
        source,
    })?;
    fs::rename(&temp_path, path).map_err(|source| tsink::TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_resource_id(field: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.len() > MAX_RESOURCE_ID_LEN {
        return Err(format!(
            "{field} must be at most {MAX_RESOURCE_ID_LEN} characters"
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{field} must not contain control characters"));
    }
    Ok(value.to_string())
}

fn validate_non_empty_field(field: &str, value: String) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{field} must not contain control characters"));
    }
    Ok(value.to_string())
}

fn normalize_optional_field(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}

fn normalize_labels(
    labels: BTreeMap<String, String>,
    field: &str,
) -> Result<BTreeMap<String, String>, String> {
    let mut normalized = BTreeMap::new();
    for (key, value) in labels {
        let key = validate_non_empty_field(field, key)?;
        let value = validate_non_empty_field(field, value)?;
        normalized.insert(key, value);
    }
    Ok(normalized)
}

fn normalize_string_list(values: Vec<String>, field: &str) -> Result<Vec<String>, String> {
    let mut normalized = values
        .into_iter()
        .map(|value| validate_non_empty_field(field, value))
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tsink::{
        QueryBudget, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken, QueryLimitReason,
        QueryWorkLimits,
    };

    fn actor() -> ManagedControlPlaneActor {
        ManagedControlPlaneActor {
            id: "test-admin".to_string(),
            scope: "test".to_string(),
        }
    }

    fn deployment_request(deployment_id: &str) -> ManagedDeploymentProvisionRequest {
        ManagedDeploymentProvisionRequest {
            deployment_id: deployment_id.to_string(),
            display_name: Some(format!("Deployment {deployment_id}")),
            region: Some("us-east-1".to_string()),
            plan: Some("ha".to_string()),
            lifecycle: Some(DeploymentLifecycleState::Ready),
            ..ManagedDeploymentProvisionRequest::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn status_projection_test_deployment(
        id: &str,
        region: &str,
        plan: &str,
        lifecycle: DeploymentLifecycleState,
        backup_enabled: bool,
        maintenance_active: bool,
        upgrade_state: UpgradeRolloutState,
        desired_version: Option<&str>,
        observed_version: Option<&str>,
    ) -> ManagedDeployment {
        ManagedDeployment {
            id: id.to_string(),
            display_name: format!("display {id}"),
            region: region.to_string(),
            plan: plan.to_string(),
            control_plane_endpoint: Some(format!("https://cp-{id}.example")),
            data_plane_endpoint: Some(format!("https://dp-{id}.example")),
            object_store_path: Some(format!("s3://bucket/{id}")),
            lifecycle,
            labels: BTreeMap::from([("environment".to_string(), "test".to_string())]),
            created_unix_ms: 101,
            updated_unix_ms: 102,
            backup_policy: ManagedBackupPolicy {
                enabled: backup_enabled,
                ..ManagedBackupPolicy::default()
            },
            maintenance: ManagedMaintenancePolicy {
                active: maintenance_active,
                reason: Some("maintenance diagnostic".to_string()),
                ..ManagedMaintenancePolicy::default()
            },
            upgrade: ManagedUpgradePlan {
                desired_version: desired_version.map(str::to_string),
                observed_version: observed_version.map(str::to_string),
                state: upgrade_state,
                notes: Some("upgrade diagnostic".to_string()),
                ..ManagedUpgradePlan::default()
            },
        }
    }

    fn status_projection_test_store(temp_dir: &TempDir) -> ManagedControlPlane {
        let store =
            ManagedControlPlane::open(Some(temp_dir.path())).expect("test store should open");
        let mut state = store
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        state.updated_unix_ms = 777;
        state.deployments = BTreeMap::from([
            (
                "z-map-key".to_string(),
                status_projection_test_deployment(
                    "deployment-alpha",
                    "region-\"alpha",
                    "plan-alpha\n",
                    DeploymentLifecycleState::Ready,
                    false,
                    false,
                    UpgradeRolloutState::Complete,
                    Some("2.0.0"),
                    None,
                ),
            ),
            (
                "a-map-key".to_string(),
                status_projection_test_deployment(
                    "deployment-zeta",
                    "region-zeta",
                    "plan-zeta",
                    DeploymentLifecycleState::Provisioning,
                    true,
                    true,
                    UpgradeRolloutState::InProgress,
                    Some("3.0.0"),
                    Some("2.9.0"),
                ),
            ),
        ]);
        state.tenants = BTreeMap::from([
            (
                "Current-Tenant-Key".to_string(),
                ManagedTenant {
                    id: "tenant-value-id".to_string(),
                    deployment_id: "deployment-zeta".to_string(),
                    display_name: "Current \"Tenant\"\n".to_string(),
                    lifecycle: TenantLifecycleState::Suspended,
                    retention_days: Some(31),
                    storage_limit_bytes: Some(1_234_567),
                    ingest_rate_limit_per_sec: Some(8_765),
                    query_concurrency_limit: Some(9),
                    labels: BTreeMap::from([
                        ("z-label".to_string(), "z-value\n".to_string()),
                        ("a-label".to_string(), "a-\"value".to_string()),
                    ]),
                    created_unix_ms: 201,
                    updated_unix_ms: 202,
                    lifecycle_note: Some("billing \"hold\"\n".to_string()),
                },
            ),
            (
                "other-tenant".to_string(),
                ManagedTenant {
                    id: "other-tenant".to_string(),
                    deployment_id: "deployment-alpha".to_string(),
                    display_name: "Other Tenant".to_string(),
                    lifecycle: TenantLifecycleState::Active,
                    retention_days: None,
                    storage_limit_bytes: None,
                    ingest_rate_limit_per_sec: None,
                    query_concurrency_limit: None,
                    labels: BTreeMap::new(),
                    created_unix_ms: 301,
                    updated_unix_ms: 302,
                    lifecycle_note: None,
                },
            ),
        ]);
        state.audit_entries = vec![ManagedControlPlaneAuditEntry {
            seq: 1,
            unix_ms: 401,
            actor_id: "status-test".to_string(),
            actor_scope: "test".to_string(),
            operation: "fixture".to_string(),
            target_kind: "control-plane".to_string(),
            target_id: "fixture".to_string(),
            outcome: "success".to_string(),
            detail: Some("not retained by status projection".to_string()),
        }];
        drop(state);
        store
    }

    fn expected_status_projection_string_materializations(
        status: &ManagedControlPlaneStatusSnapshot,
        deployments: &[ManagedDeploymentSummary],
        current_tenant: Option<&ManagedTenant>,
    ) -> u64 {
        let mut count = u64::from(status.state_path.is_some());
        for deployment in deployments {
            count = count
                .saturating_add(3)
                .saturating_add(u64::from(deployment.desired_version.is_some()))
                .saturating_add(u64::from(deployment.observed_version.is_some()));
        }
        if let Some(tenant) = current_tenant {
            count = count
                .saturating_add(3)
                .saturating_add(
                    u64::try_from(tenant.labels.len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(2),
                )
                .saturating_add(u64::from(tenant.lifecycle_note.is_some()));
        }
        count
    }

    #[test]
    fn status_projection_preserves_all_legacy_inputs_and_map_order() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let store = status_projection_test_store(&temp_dir);
        let legacy_status = store.status_snapshot();
        let legacy_deployments = store.deployment_summaries();
        let legacy_current_tenant = store.tenant_snapshot("Current-Tenant-Key");
        let budget = QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let execution = budget.begin_query().expect("query should admit");
        store.reset_status_projection_output_string_materializations();

        let projection = store
            .status_projection_for_with_execution("Current-Tenant-Key", &execution)
            .expect("schema-complete managed status projection should succeed");
        assert_eq!(projection.status, legacy_status);
        assert_eq!(projection.deployments, legacy_deployments);
        assert_eq!(projection.current_tenant, legacy_current_tenant);
        assert_eq!(
            projection
                .deployments
                .iter()
                .map(|deployment| deployment.id.as_str())
                .collect::<Vec<_>>(),
            vec!["deployment-zeta", "deployment-alpha"],
            "deployment summaries must preserve BTreeMap key order rather than sort by value id"
        );
        assert_eq!(
            projection
                .current_tenant
                .as_ref()
                .expect("current tenant should be present")
                .labels
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["a-label", "z-label"]
        );
        assert_eq!(
            projection.accounted_bytes(),
            modeled_managed_status_projection_retained_bytes(&projection)
        );
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            projection.accounted_bytes()
        );
        assert_eq!(
            store.status_projection_output_string_materializations(),
            expected_status_projection_string_materializations(
                &projection.status,
                &projection.deployments,
                projection.current_tenant.as_ref(),
            )
        );

        drop(projection);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let status = budget.snapshot();
        assert_eq!(status.active_queries, 0);
        assert_eq!(status.shared_reserved_memory_bytes, 0);
        assert_eq!(status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn status_projection_enforces_exact_peak_before_any_output_clone() {
        fn budget_with_memory_limit(limit: Option<u64>) -> QueryBudget {
            QueryBudget::new(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: limit,
                per_query: QueryWorkLimits {
                    max_memory_bytes: limit,
                    ..QueryWorkLimits::default()
                },
            })
            .expect("managed status projection budget should build")
        }

        let temp_dir = TempDir::new().expect("temp dir should create");
        let store = status_projection_test_store(&temp_dir);

        let calibration_budget = budget_with_memory_limit(None);
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = store
            .status_projection_for_with_execution("Current-Tenant-Key", &calibration)
            .expect("calibration projection should succeed");
        let exact_bytes = calibrated.accounted_bytes();
        assert!(exact_bytes > 0);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, exact_bytes);
        assert_eq!(
            calibration_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            exact_bytes,
            "the complete retained projection must be its materialization peak"
        );
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibration_status = calibration_budget.snapshot();
        assert_eq!(calibration_status.active_queries, 0);
        assert_eq!(calibration_status.shared_reserved_memory_bytes, 0);
        assert_eq!(calibration_status.accounting_invariant_violations_total, 0);

        let exact_budget = budget_with_memory_limit(Some(exact_bytes));
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        store.reset_status_projection_output_string_materializations();
        let exact_projection = store
            .status_projection_for_with_execution("Current-Tenant-Key", &exact)
            .expect("the exact managed status peak should pass");
        assert_eq!(exact_projection.accounted_bytes(), exact_bytes);
        assert!(store.status_projection_output_string_materializations() > 0);
        drop(exact_projection);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        let exact_status = exact_budget.snapshot();
        assert_eq!(exact_status.active_queries, 0);
        assert_eq!(exact_status.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_status.accounting_invariant_violations_total, 0);

        let one_under_budget = budget_with_memory_limit(Some(exact_bytes.saturating_sub(1)));
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        store.reset_status_projection_output_string_materializations();
        let error = store
            .status_projection_for_with_execution("Current-Tenant-Key", &one_under)
            .expect_err("one byte below the complete managed status peak must reject");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, exact_bytes);
            }
            other => panic!("unexpected managed status projection error: {other}"),
        }
        assert_eq!(
            store.status_projection_output_string_materializations(),
            0,
            "N-1 admission must fail before any retained path, summary, or tenant string clone"
        );
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            one_under_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            0
        );
        drop(one_under);
        let one_under_status = one_under_budget.snapshot();
        assert_eq!(one_under_status.active_queries, 0);
        assert_eq!(one_under_status.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn status_projection_honors_precancellation_without_residual_memory() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let store = status_projection_test_store(&temp_dir);
        let budget = QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("query should admit");
        store.reset_status_projection_output_string_materializations();
        cancellation.cancel();

        let error = store
            .status_projection_for_with_execution("Current-Tenant-Key", &execution)
            .expect_err("a pre-cancelled managed status projection must stop");
        assert!(matches!(error, QueryBudgetError::Cancelled));
        assert_eq!(store.status_projection_output_string_materializations(), 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 0);
        drop(execution);

        let status = budget.snapshot();
        assert_eq!(status.active_queries, 0);
        assert_eq!(status.shared_reserved_memory_bytes, 0);
        assert_eq!(status.cancellations_total, 1);
        assert_eq!(status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn managed_control_plane_persists_hosted_state_and_audit() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let store =
            ManagedControlPlane::open(Some(temp_dir.path())).expect("store should open on disk");

        let deployment = store
            .provision_deployment(
                actor(),
                ManagedDeploymentProvisionRequest {
                    deployment_id: "prod-us-east".to_string(),
                    display_name: Some("Production US East".to_string()),
                    region: Some("us-east-1".to_string()),
                    plan: Some("ha".to_string()),
                    control_plane_endpoint: Some("https://cp.example".to_string()),
                    data_plane_endpoint: Some("https://dp.example".to_string()),
                    object_store_path: Some("s3://tsink-prod-east".to_string()),
                    lifecycle: Some(DeploymentLifecycleState::Ready),
                    labels: Some(BTreeMap::from([(
                        "environment".to_string(),
                        "prod".to_string(),
                    )])),
                },
            )
            .expect("deployment should provision");
        assert_eq!(deployment.lifecycle, DeploymentLifecycleState::Ready);

        let deployment = store
            .apply_backup_policy(
                actor(),
                ManagedBackupPolicyApplyRequest {
                    deployment_id: "prod-us-east".to_string(),
                    enabled: Some(true),
                    schedule: Some("0 */6 * * *".to_string()),
                    retention_copies: Some(14),
                    target: Some("s3://tsink-prod-east/backups".to_string()),
                },
            )
            .expect("backup policy should apply");
        assert!(deployment.backup_policy.enabled);

        let deployment = store
            .record_backup_run(
                actor(),
                ManagedBackupRunRecordRequest {
                    deployment_id: "prod-us-east".to_string(),
                    outcome: BackupRunOutcome::Success,
                    completed_unix_ms: Some(1234),
                    snapshot_path: Some(
                        "/srv/tsink-admin/backups/prod-us-east-20260307".to_string(),
                    ),
                    error: None,
                },
            )
            .expect("backup run should record");
        assert_eq!(deployment.backup_policy.last_success_unix_ms, Some(1234));

        let deployment = store
            .apply_maintenance(
                actor(),
                ManagedMaintenanceApplyRequest {
                    deployment_id: "prod-us-east".to_string(),
                    active: Some(true),
                    reason: Some("kernel rollout".to_string()),
                    window_start_unix_ms: Some(10),
                    window_end_unix_ms: Some(20),
                    allowed_mutations: Some(vec!["backup".to_string(), "upgrade".to_string()]),
                },
            )
            .expect("maintenance should apply");
        assert!(deployment.maintenance.active);

        let deployment = store
            .apply_upgrade(
                actor(),
                ManagedUpgradeApplyRequest {
                    deployment_id: "prod-us-east".to_string(),
                    desired_version: Some("1.2.3".to_string()),
                    observed_version: Some("1.2.2".to_string()),
                    channel: Some("stable".to_string()),
                    strategy: Some("canary".to_string()),
                    state: Some(UpgradeRolloutState::InProgress),
                    notes: Some("begin with canary ring".to_string()),
                },
            )
            .expect("upgrade plan should apply");
        assert_eq!(deployment.upgrade.state, UpgradeRolloutState::InProgress);

        let tenant = store
            .apply_tenant(
                actor(),
                ManagedTenantApplyRequest {
                    tenant_id: "acme".to_string(),
                    deployment_id: Some("prod-us-east".to_string()),
                    display_name: Some("Acme".to_string()),
                    lifecycle: Some(TenantLifecycleState::Active),
                    retention_days: Some(30),
                    storage_limit_bytes: Some(1_000_000),
                    ingest_rate_limit_per_sec: Some(10_000),
                    query_concurrency_limit: Some(8),
                    labels: Some(BTreeMap::from([("tier".to_string(), "gold".to_string())])),
                },
            )
            .expect("tenant should apply");
        assert_eq!(tenant.lifecycle, TenantLifecycleState::Active);

        let tenant = store
            .apply_tenant_lifecycle(
                actor(),
                ManagedTenantLifecycleRequest {
                    tenant_id: "acme".to_string(),
                    lifecycle: TenantLifecycleState::Suspended,
                    note: Some("billing hold".to_string()),
                },
            )
            .expect("tenant lifecycle should update");
        assert_eq!(tenant.lifecycle, TenantLifecycleState::Suspended);

        let snapshot = store.state_snapshot();
        assert_eq!(snapshot.deployments.len(), 1);
        assert_eq!(snapshot.tenants.len(), 1);
        assert_eq!(snapshot.status.audit_records_total, 7);

        let reopened =
            ManagedControlPlane::open(Some(temp_dir.path())).expect("store should reopen");
        let snapshot = reopened.state_snapshot();
        assert_eq!(snapshot.deployments[0].backup_policy.retention_copies, 14);
        assert_eq!(
            snapshot.tenants[0].lifecycle,
            TenantLifecycleState::Suspended
        );
        assert_eq!(snapshot.status.audit_records_total, 7);
    }

    #[test]
    fn managed_control_plane_audits_failed_mutations_without_committing_them() {
        let store = ManagedControlPlane::open(None).expect("in-memory store should open");
        let err = store
            .apply_backup_policy(
                actor(),
                ManagedBackupPolicyApplyRequest {
                    deployment_id: "missing".to_string(),
                    enabled: Some(true),
                    schedule: Some("0 * * * *".to_string()),
                    retention_copies: Some(7),
                    target: Some("s3://missing".to_string()),
                },
            )
            .expect_err("missing deployment should fail");
        assert!(matches!(
            &err,
            ManagedControlPlaneMutationError::Rejected(detail)
                if detail.contains("unknown deployment")
        ));

        let snapshot = store.state_snapshot();
        assert!(snapshot.deployments.is_empty());
        assert_eq!(snapshot.status.audit_records_total, 1);
        let audit = store.query_audit(ManagedControlPlaneAuditFilter::default());
        assert_eq!(audit[0].outcome, "error");
    }

    #[test]
    fn managed_control_plane_disk_quota_rejection_does_not_publish_candidate_state() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let initial_file_bytes = u64::try_from(
            serde_json::to_vec_pretty(&ManagedControlPlaneStateFile::default())
                .expect("default state should encode")
                .len(),
        )
        .expect("encoded state length should fit u64");
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(initial_file_bytes),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = ManagedControlPlane::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("initial state should fit the exact quota");
        let state_path = temp_dir
            .path()
            .join(MANAGED_CONTROL_PLANE_DIR)
            .join(MANAGED_CONTROL_PLANE_STATE_FILE);
        let initial_contents = fs::read(&state_path).expect("initial state should exist");

        let err = store
            .provision_deployment(actor(), deployment_request("quota-rejected"))
            .expect_err("growing the state file should exceed the quota");
        assert!(
            matches!(
                &err,
                ManagedControlPlaneMutationError::Persistence(
                    tsink::TsinkError::DiskQuotaExceeded { .. }
                )
            ),
            "{err}"
        );

        let state = store.state_snapshot();
        assert!(state.deployments.is_empty());
        assert_eq!(state.status.audit_records_total, 0);
        assert_eq!(
            fs::read(&state_path).expect("published state should remain readable"),
            initial_contents
        );
        let disk = budget.snapshot();
        assert_eq!(disk.accounted_bytes, initial_file_bytes);
        assert_eq!(disk.reserved_bytes, 0);
        assert_eq!(disk.active_reservations, 0);
        assert_eq!(disk.rejections_total, 1);
    }

    #[test]
    fn managed_control_plane_disk_accounting_is_exact_across_restart() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let budget =
            tsink::LocalDiskBudget::open(temp_dir.path(), tsink::LocalDiskLimits::default())
                .expect("disk budget should open");
        let store = ManagedControlPlane::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("managed state should open");
        store
            .provision_deployment(actor(), deployment_request("restart-safe"))
            .expect("deployment should persist");

        let state_path = temp_dir
            .path()
            .join(MANAGED_CONTROL_PLANE_DIR)
            .join(MANAGED_CONTROL_PLANE_STATE_FILE);
        let state_bytes = fs::metadata(&state_path)
            .expect("managed state should exist")
            .len();
        let disk = budget.snapshot();
        assert_eq!(disk.accounted_bytes, state_bytes);
        assert_eq!(disk.reserved_bytes, 0);
        assert_eq!(disk.active_reservations, 0);
        assert_eq!(
            disk.categories
                .iter()
                .find(|usage| usage.category == tsink::DiskCategory::ServerState)
                .map(|usage| usage.bytes),
            Some(state_bytes)
        );

        drop(store);
        drop(budget);

        let restarted_budget =
            tsink::LocalDiskBudget::open(temp_dir.path(), tsink::LocalDiskLimits::default())
                .expect("disk budget should reconcile after restart");
        let reopened = ManagedControlPlane::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&restarted_budget)),
        )
        .expect("managed state should reopen");
        assert!(reopened
            .state_snapshot()
            .deployments
            .iter()
            .any(|deployment| deployment.id == "restart-safe"));
        let disk = restarted_budget.snapshot();
        assert_eq!(disk.accounted_bytes, state_bytes);
        assert_eq!(
            disk.categories
                .iter()
                .find(|usage| usage.category == tsink::DiskCategory::ServerState)
                .map(|usage| usage.bytes),
            Some(state_bytes)
        );
    }
}
