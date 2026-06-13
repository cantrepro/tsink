use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MANAGED_CONTROL_PLANE_DIR: &str = "managed-control-plane";
const MANAGED_CONTROL_PLANE_STATE_FILE: &str = "state.json";
const MANAGED_CONTROL_PLANE_MAGIC: &str = "tsink-managed-control-plane";
const MANAGED_CONTROL_PLANE_SCHEMA_VERSION: u16 = 1;
const DEFAULT_BACKUP_RETENTION_COPIES: u32 = 7;
const DEFAULT_AUDIT_QUERY_LIMIT: usize = 100;
const MAX_RESOURCE_ID_LEN: usize = 128;

#[derive(Debug)]
pub struct ManagedControlPlane {
    state_path: Option<PathBuf>,
    state: Mutex<ManagedControlPlaneStateFile>,
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
    pub fn open(data_path: Option<&Path>) -> Result<Self, String> {
        let state_path = match data_path {
            Some(data_path) => {
                let directory = data_path.join(MANAGED_CONTROL_PLANE_DIR);
                fs::create_dir_all(&directory).map_err(|err| {
                    format!(
                        "failed to create managed control-plane directory {}: {err}",
                        directory.display()
                    )
                })?;
                Some(directory.join(MANAGED_CONTROL_PLANE_STATE_FILE))
            }
            None => None,
        };

        let state = if let Some(path) = state_path.as_deref() {
            if path.exists() {
                load_state_file(path)?
            } else {
                let state = ManagedControlPlaneStateFile::default();
                persist_state_file(path, &state)?;
                state
            }
        } else {
            ManagedControlPlaneStateFile::default()
        };

        Ok(Self {
            state_path,
            state: Mutex::new(state),
        })
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

    pub fn status_snapshot(&self) -> ManagedControlPlaneStatusSnapshot {
        let state = self
            .state
            .lock()
            .expect("managed control-plane state mutex should not be poisoned");
        status_snapshot_for_state(&state, self.state_path.as_deref())
    }

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
    ) -> Result<ManagedDeployment, String> {
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
    ) -> Result<ManagedDeployment, String> {
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
    ) -> Result<ManagedDeployment, String> {
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
    ) -> Result<ManagedDeployment, String> {
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
    ) -> Result<ManagedDeployment, String> {
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
    ) -> Result<ManagedTenant, String> {
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
    ) -> Result<ManagedTenant, String> {
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
    ) -> Result<T, String>
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
                    persist_state_file(path, &candidate)?;
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
                    persist_state_file(path, &audited)?;
                }
                *state = audited;
                Err(err)
            }
        }
    }
}

fn status_snapshot_for_state(
    state: &ManagedControlPlaneStateFile,
    state_path: Option<&Path>,
) -> ManagedControlPlaneStatusSnapshot {
    ManagedControlPlaneStatusSnapshot {
        durable: state_path.is_some(),
        state_path: state_path.map(|path| path.display().to_string()),
        deployments_total: u64::try_from(state.deployments.len()).unwrap_or(u64::MAX),
        tenants_total: u64::try_from(state.tenants.len()).unwrap_or(u64::MAX),
        active_tenants_total: u64::try_from(
            state
                .tenants
                .values()
                .filter(|tenant| tenant.lifecycle == TenantLifecycleState::Active)
                .count(),
        )
        .unwrap_or(u64::MAX),
        maintenance_active_total: u64::try_from(
            state
                .deployments
                .values()
                .filter(|deployment| deployment.maintenance.active)
                .count(),
        )
        .unwrap_or(u64::MAX),
        upgrade_rollouts_in_progress_total: u64::try_from(
            state
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
        )
        .unwrap_or(u64::MAX),
        backup_policies_total: u64::try_from(
            state
                .deployments
                .values()
                .filter(|deployment| deployment.backup_policy.enabled)
                .count(),
        )
        .unwrap_or(u64::MAX),
        audit_records_total: u64::try_from(state.audit_entries.len()).unwrap_or(u64::MAX),
        updated_unix_ms: state.updated_unix_ms,
    }
}

fn deployment_summaries_for_state(
    state: &ManagedControlPlaneStateFile,
) -> Vec<ManagedDeploymentSummary> {
    state
        .deployments
        .values()
        .map(|deployment| {
            let tenant_count = state
                .tenants
                .values()
                .filter(|tenant| tenant.deployment_id == deployment.id)
                .count();
            let active_tenant_count = state
                .tenants
                .values()
                .filter(|tenant| {
                    tenant.deployment_id == deployment.id
                        && tenant.lifecycle == TenantLifecycleState::Active
                })
                .count();
            ManagedDeploymentSummary {
                id: deployment.id.clone(),
                region: deployment.region.clone(),
                plan: deployment.plan.clone(),
                lifecycle: deployment.lifecycle,
                tenant_count: u64::try_from(tenant_count).unwrap_or(u64::MAX),
                active_tenant_count: u64::try_from(active_tenant_count).unwrap_or(u64::MAX),
                backup_enabled: deployment.backup_policy.enabled,
                maintenance_active: deployment.maintenance.active,
                upgrade_state: deployment.upgrade.state,
                desired_version: deployment.upgrade.desired_version.clone(),
                observed_version: deployment.upgrade.observed_version.clone(),
            }
        })
        .collect()
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

fn persist_state_file(path: &Path, state: &ManagedControlPlaneStateFile) -> Result<(), String> {
    let encoded = serde_json::to_vec_pretty(state)
        .map_err(|err| format!("failed to encode managed control-plane state: {err}"))?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, encoded).map_err(|err| {
        format!(
            "failed to write managed control-plane state {}: {err}",
            temp_path.display()
        )
    })?;
    fs::rename(&temp_path, path).map_err(|err| {
        format!(
            "failed to publish managed control-plane state {}: {err}",
            path.display()
        )
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

    fn actor() -> ManagedControlPlaneActor {
        ManagedControlPlaneActor {
            id: "test-admin".to_string(),
            scope: "test".to_string(),
        }
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
        assert!(err.contains("unknown deployment"));

        let snapshot = store.state_snapshot();
        assert!(snapshot.deployments.is_empty());
        assert_eq!(snapshot.status.audit_records_total, 1);
        let audit = store.query_audit(ManagedControlPlaneAuditFilter::default());
        assert_eq!(audit[0].outcome, "error");
    }
}
