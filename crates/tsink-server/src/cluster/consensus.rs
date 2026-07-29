use crate::cluster::control::{
    encode_control_state_file, modeled_control_metrics_projection_retained_bytes,
    modeled_control_status_projection_retained_bytes, AccountedControlRebalanceProjection,
    ClusterHandoffSnapshot, ControlHandoffMutationOutcome, ControlHotspotSnapshot,
    ControlMembershipMutationOutcome, ControlMetricsProjection, ControlNodeStatus, ControlState,
    ControlStateStore, ControlStatusProjection,
};
use crate::cluster::membership::MembershipView;
use crate::cluster::rpc::{
    InternalControlAppendRequest, InternalControlAppendResponse, InternalControlCommand,
    InternalControlInstallSnapshotRequest, InternalControlInstallSnapshotResponse,
    InternalControlLogEntry, RpcClient,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::disk_budget::{
    DiskCategory, LocalDiskBudget, ManagedFileReplacement, StagedManagedFileReplacements,
};
use tsink::TsinkError;

const CONTROL_LOG_MAGIC: &str = "tsink-control-log";
const CONTROL_LOG_LEGACY_SCHEMA_VERSION: u16 = 1;
const CONTROL_LOG_SCHEMA_VERSION: u16 = 2;

pub const CLUSTER_CONTROL_TICK_INTERVAL_SECS_ENV: &str = "TSINK_CLUSTER_CONTROL_TICK_INTERVAL_SECS";
pub const CLUSTER_CONTROL_MAX_APPEND_ENTRIES_ENV: &str = "TSINK_CLUSTER_CONTROL_MAX_APPEND_ENTRIES";
pub const CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES_ENV: &str =
    "TSINK_CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES";
pub const CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS_ENV: &str =
    "TSINK_CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS";
pub const CLUSTER_CONTROL_DEAD_TIMEOUT_SECS_ENV: &str = "TSINK_CLUSTER_CONTROL_DEAD_TIMEOUT_SECS";
pub const CLUSTER_CONTROL_LEADER_LEASE_SECS_ENV: &str = "TSINK_CLUSTER_CONTROL_LEADER_LEASE_SECS";

const DEFAULT_CONTROL_TICK_INTERVAL_SECS: u64 = 2;
const DEFAULT_CONTROL_MAX_APPEND_ENTRIES: usize = 64;
const DEFAULT_CONTROL_SNAPSHOT_INTERVAL_ENTRIES: usize = 128;
const DEFAULT_CONTROL_SUSPECT_TIMEOUT_SECS: u64 = 6;
const DEFAULT_CONTROL_DEAD_TIMEOUT_SECS: u64 = 20;
const DEFAULT_CONTROL_LEADER_LEASE_SECS: u64 = 6;
const CONTROL_SYNC_MAX_ATTEMPTS: usize = 4;
const CONTROL_METRICS_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

#[cfg(test)]
struct ControlCheckpointPublishFailureGuard {
    target: PathBuf,
    _serialization_guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for ControlCheckpointPublishFailureGuard {
    fn drop(&mut self) {
        let mut target = control_checkpoint_publish_failure_target()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if target.as_ref() == Some(&self.target) {
            *target = None;
        }
    }
}

#[cfg(test)]
fn control_checkpoint_publish_failure_target() -> &'static std::sync::Mutex<Option<PathBuf>> {
    static TARGET: std::sync::OnceLock<std::sync::Mutex<Option<PathBuf>>> =
        std::sync::OnceLock::new();
    TARGET.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn fail_control_checkpoint_after_log_publish_once(
    target: PathBuf,
) -> ControlCheckpointPublishFailureGuard {
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let serialization_guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *control_checkpoint_publish_failure_target()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(target.clone());
    ControlCheckpointPublishFailureGuard {
        target,
        _serialization_guard: serialization_guard,
    }
}

#[cfg(test)]
fn maybe_fail_control_checkpoint_after_log_publish(log_path: &Path) -> tsink::Result<()> {
    let mut target = control_checkpoint_publish_failure_target()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if target.as_deref() == Some(log_path) {
        *target = None;
        return Err(TsinkError::Other(
            "injected failure after authoritative control-log publication".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
struct ControlPairFinalizationFailureGuard {
    target: PathBuf,
    _serialization_guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for ControlPairFinalizationFailureGuard {
    fn drop(&mut self) {
        let mut target = control_pair_finalization_failure_target()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if target.as_ref() == Some(&self.target) {
            *target = None;
        }
    }
}

#[cfg(test)]
fn control_pair_finalization_failure_target() -> &'static std::sync::Mutex<Option<PathBuf>> {
    static TARGET: std::sync::OnceLock<std::sync::Mutex<Option<PathBuf>>> =
        std::sync::OnceLock::new();
    TARGET.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn fail_control_pair_finalization_once(target: PathBuf) -> ControlPairFinalizationFailureGuard {
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let serialization_guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *control_pair_finalization_failure_target()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(target.clone());
    ControlPairFinalizationFailureGuard {
        target,
        _serialization_guard: serialization_guard,
    }
}

#[cfg(test)]
fn maybe_fail_control_pair_finalization(log_path: &Path) -> tsink::Result<()> {
    let mut target = control_pair_finalization_failure_target()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if target.as_deref() == Some(log_path) {
        *target = None;
        return Err(TsinkError::Other(
            "injected failure after durable control-pair publication".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlDiskResourceLimit {
    DiskQuotaExceeded {
        limit: u64,
        used: u64,
        reserved: u64,
        requested: u64,
    },
    InsufficientDiskSpace {
        required: u64,
        available: u64,
    },
    InsufficientCompactionHeadroom {
        limit: u64,
        used: u64,
        reserved: u64,
        requested: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlCommitPosition {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPersistenceStage {
    LogEncode,
    CheckpointEncode,
    LogPublish,
    CheckpointPublish,
    Repair,
}

impl ControlPersistenceStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::LogEncode => "control-log encoding",
            Self::CheckpointEncode => "control-state checkpoint encoding",
            Self::LogPublish => "control-log publication",
            Self::CheckpointPublish => "control-state checkpoint publication",
            Self::Repair => "control checkpoint repair",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlConsensusError {
    detail: String,
    resource_limit: Option<ControlDiskResourceLimit>,
    committed_checkpoint: Option<ControlCommitPosition>,
    indeterminate: bool,
    candidate_visible: bool,
    persistence_failure: bool,
    cleanup_pending: bool,
}

impl ControlConsensusError {
    fn rejected(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            resource_limit: None,
            committed_checkpoint: None,
            indeterminate: false,
            candidate_visible: false,
            persistence_failure: false,
            cleanup_pending: false,
        }
    }

    fn persistence(stage: ControlPersistenceStage, err: TsinkError) -> Self {
        Self {
            detail: format!("{} failed: {err}", stage.as_str()),
            resource_limit: control_disk_resource_limit(&err),
            committed_checkpoint: None,
            indeterminate: false,
            candidate_visible: false,
            persistence_failure: true,
            cleanup_pending: false,
        }
    }

    fn indeterminate(
        stage: ControlPersistenceStage,
        detail: impl std::fmt::Display,
        candidate_visible: bool,
    ) -> Self {
        Self {
            detail: format!("{} outcome is indeterminate: {detail}", stage.as_str()),
            resource_limit: None,
            committed_checkpoint: None,
            indeterminate: true,
            candidate_visible,
            persistence_failure: true,
            cleanup_pending: false,
        }
    }

    fn committed_checkpoint_pending(
        position: ControlCommitPosition,
        stage: ControlPersistenceStage,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self {
            detail: format!(
                "committed control checkpoint at index {} term {} is pending repair after {} failed: {detail}",
                position.index,
                position.term,
                stage.as_str()
            ),
            resource_limit: None,
            committed_checkpoint: Some(position),
            indeterminate: true,
            candidate_visible: true,
            persistence_failure: true,
            cleanup_pending: false,
        }
    }

    fn durable_candidate_pending(
        stage: ControlPersistenceStage,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self {
            detail: format!(
                "consensus-observed control state is fenced pending durable {}: {detail}",
                stage.as_str()
            ),
            resource_limit: None,
            committed_checkpoint: None,
            indeterminate: true,
            candidate_visible: false,
            persistence_failure: true,
            cleanup_pending: false,
        }
    }

    fn committed_cleanup_pending(
        position: ControlCommitPosition,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self {
            detail: format!(
                "committed control state at index {} term {} has cleanup debt: {detail}",
                position.index, position.term
            ),
            resource_limit: None,
            committed_checkpoint: Some(position),
            indeterminate: false,
            candidate_visible: true,
            persistence_failure: true,
            cleanup_pending: true,
        }
    }

    pub fn resource_limit(&self) -> Option<ControlDiskResourceLimit> {
        self.resource_limit
    }

    pub fn committed_checkpoint(&self) -> Option<ControlCommitPosition> {
        self.committed_checkpoint
    }

    pub fn is_committed_checkpoint_pending(&self) -> bool {
        self.committed_checkpoint.is_some() && !self.cleanup_pending
    }

    pub fn is_committed_cleanup_pending(&self) -> bool {
        self.cleanup_pending
    }

    pub fn is_indeterminate(&self) -> bool {
        self.indeterminate
    }

    pub fn is_persistence_failure(&self) -> bool {
        self.persistence_failure
    }
}

impl std::fmt::Display for ControlConsensusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ControlConsensusError {}

impl Deref for ControlConsensusError {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.detail
    }
}

impl From<ControlConsensusError> for String {
    fn from(err: ControlConsensusError) -> Self {
        err.detail
    }
}

fn control_disk_resource_limit(err: &TsinkError) -> Option<ControlDiskResourceLimit> {
    match err {
        TsinkError::DiskQuotaExceeded {
            limit,
            used,
            reserved,
            requested,
        } => Some(ControlDiskResourceLimit::DiskQuotaExceeded {
            limit: *limit,
            used: *used,
            reserved: *reserved,
            requested: *requested,
        }),
        TsinkError::InsufficientDiskSpace {
            required,
            available,
        } => Some(ControlDiskResourceLimit::InsufficientDiskSpace {
            required: *required,
            available: *available,
        }),
        TsinkError::InsufficientCompactionHeadroom {
            limit,
            used,
            reserved,
            requested,
        } => Some(ControlDiskResourceLimit::InsufficientCompactionHeadroom {
            limit: *limit,
            used: *used,
            reserved: *reserved,
            requested: *requested,
        }),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlConsensusConfig {
    pub tick_interval_secs: u64,
    pub max_append_entries: usize,
    pub snapshot_interval_entries: usize,
    pub suspect_timeout_secs: u64,
    pub dead_timeout_secs: u64,
    pub leader_lease_secs: u64,
}

impl Default for ControlConsensusConfig {
    fn default() -> Self {
        Self {
            tick_interval_secs: DEFAULT_CONTROL_TICK_INTERVAL_SECS,
            max_append_entries: DEFAULT_CONTROL_MAX_APPEND_ENTRIES,
            snapshot_interval_entries: DEFAULT_CONTROL_SNAPSHOT_INTERVAL_ENTRIES,
            suspect_timeout_secs: DEFAULT_CONTROL_SUSPECT_TIMEOUT_SECS,
            dead_timeout_secs: DEFAULT_CONTROL_DEAD_TIMEOUT_SECS,
            leader_lease_secs: DEFAULT_CONTROL_LEADER_LEASE_SECS,
        }
    }
}

impl ControlConsensusConfig {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            tick_interval_secs: parse_env_u64(
                CLUSTER_CONTROL_TICK_INTERVAL_SECS_ENV,
                defaults.tick_interval_secs,
                true,
            )?,
            max_append_entries: parse_env_u64(
                CLUSTER_CONTROL_MAX_APPEND_ENTRIES_ENV,
                defaults.max_append_entries as u64,
                true,
            )? as usize,
            snapshot_interval_entries: parse_env_u64(
                CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES_ENV,
                defaults.snapshot_interval_entries as u64,
                true,
            )? as usize,
            suspect_timeout_secs: parse_env_u64(
                CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS_ENV,
                defaults.suspect_timeout_secs,
                true,
            )?,
            dead_timeout_secs: parse_env_u64(
                CLUSTER_CONTROL_DEAD_TIMEOUT_SECS_ENV,
                defaults.dead_timeout_secs,
                true,
            )?,
            leader_lease_secs: parse_env_u64(
                CLUSTER_CONTROL_LEADER_LEASE_SECS_ENV,
                defaults.leader_lease_secs,
                true,
            )?,
        })
    }

    pub fn tick_interval(self) -> Duration {
        Duration::from_secs(self.tick_interval_secs.max(1))
    }

    pub fn suspect_timeout(self) -> Duration {
        Duration::from_secs(self.suspect_timeout_secs.max(1))
    }

    pub fn dead_timeout(self) -> Duration {
        Duration::from_secs(self.dead_timeout_secs.max(1))
    }

    pub fn leader_lease_timeout(self) -> Duration {
        Duration::from_secs(self.leader_lease_secs.max(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeOutcome {
    Committed {
        index: u64,
        term: u64,
    },
    CommittedCheckpointPending {
        index: u64,
        term: u64,
        detail: String,
    },
    CommittedCleanupPending {
        index: u64,
        term: u64,
        detail: String,
    },
    CommittedPersistencePending {
        index: u64,
        term: u64,
        detail: String,
    },
    Pending {
        required: usize,
        acknowledged: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ControlLogRecoverySnapshot {
    pub current_term: u64,
    #[serde(default)]
    pub stepped_down_term: u64,
    pub commit_index: u64,
    pub snapshot_last_index: u64,
    pub snapshot_last_term: u64,
    #[serde(default)]
    pub entries: Vec<InternalControlLogEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPeerLivenessStatus {
    Unknown,
    Healthy,
    Suspect,
    Dead,
}

impl ControlPeerLivenessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Suspect => "suspect",
            Self::Dead => "dead",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPeerLivenessSnapshot {
    pub node_id: String,
    pub status: ControlPeerLivenessStatus,
    pub last_success_unix_ms: Option<u64>,
    pub last_failure_unix_ms: Option<u64>,
    pub consecutive_failures: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlLivenessSnapshot {
    pub local_node_id: String,
    pub current_term: u64,
    pub commit_index: u64,
    pub leader_node_id: Option<String>,
    pub leader_last_contact_unix_ms: Option<u64>,
    pub leader_contact_age_ms: Option<u64>,
    pub leader_stale: bool,
    pub suspect_peers: usize,
    pub dead_peers: usize,
    pub peers: Vec<ControlPeerLivenessSnapshot>,
}

impl ControlLivenessSnapshot {
    pub fn empty(local_node_id: String) -> Self {
        Self {
            local_node_id,
            current_term: 0,
            commit_index: 0,
            leader_node_id: None,
            leader_last_contact_unix_ms: None,
            leader_contact_age_ms: None,
            leader_stale: false,
            suspect_peers: 0,
            dead_peers: 0,
            peers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ControlConsensusRuntime {
    local_node_id: String,
    state_store: Arc<ControlStateStore>,
    log_path: PathBuf,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    config: ControlConsensusConfig,
    state: Arc<Mutex<ConsensusState>>,
    proposal_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Clone)]
struct ConsensusState {
    current_term: u64,
    stepped_down_term: u64,
    commit_index: u64,
    snapshot_last_index: u64,
    snapshot_last_term: u64,
    last_leader_contact_unix_ms: u64,
    entries: Vec<InternalControlLogEntry>,
    control_state: ControlState,
    peer_next_index: BTreeMap<String, u64>,
    peer_heartbeat: BTreeMap<String, PeerHeartbeatState>,
    persistence_fence: Option<String>,
    checkpoint_pending: Option<ControlCheckpointPending>,
    pending_durable_candidate: Option<ControlPendingDurableCandidate>,
    cleanup_debt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlCheckpointPending {
    position: ControlCommitPosition,
    detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlCheckpointWriteMode {
    Growth,
    AuthoritativeRecovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPendingDurableCandidate {
    LogOnly,
    Checkpoint(ControlCheckpointWriteMode),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPersistenceStatus {
    pub fenced: bool,
    pub pending_checkpoint: Option<ControlCommitPosition>,
    pub cleanup_debt: bool,
    pub detail: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ControlMetricsSnapshot {
    pub liveness: ControlLivenessSnapshot,
    pub persistence: ControlPersistenceStatus,
    pub handoff: ClusterHandoffSnapshot,
    pub hotspot: ControlHotspotSnapshot,
}

impl ControlMetricsSnapshot {
    pub(crate) fn empty(local_node_id: String) -> Self {
        Self {
            liveness: ControlLivenessSnapshot::empty(local_node_id),
            persistence: ControlPersistenceStatus {
                fenced: false,
                pending_checkpoint: None,
                cleanup_debt: false,
                detail: None,
            },
            handoff: ClusterHandoffSnapshot::empty(),
            hotspot: ControlHotspotSnapshot {
                handoff_shards: Vec::new(),
            },
        }
    }
}

/// Consensus metrics output whose dynamic allocations stay charged to the caller's query.
#[derive(Debug)]
pub(crate) struct AccountedControlMetricsSnapshot {
    pub snapshot: ControlMetricsSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

impl AccountedControlMetricsSnapshot {
    #[must_use]
    #[cfg(test)]
    pub(crate) fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

impl Deref for AccountedControlMetricsSnapshot {
    type Target = ControlMetricsSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

/// Schema-complete consensus/control-state input for the TSDB status response.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ControlStatusSnapshot {
    pub liveness: ControlLivenessSnapshot,
    pub persistence: ControlPersistenceStatus,
    pub handoff: ClusterHandoffSnapshot,
    pub hotspot: ControlHotspotSnapshot,
}

/// Status output whose dynamic allocations remain charged until every borrowed field is dropped.
///
/// The inner snapshot is intentionally private and this wrapper has no extraction method, so a
/// caller cannot move the output away from its reservation.
#[derive(Debug)]
#[must_use = "dropping the status snapshot releases its query-memory reservation"]
pub(crate) struct AccountedControlStatusSnapshot {
    snapshot: ControlStatusSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

impl AccountedControlStatusSnapshot {
    #[must_use]
    #[cfg(test)]
    pub(crate) fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

impl Deref for AccountedControlStatusSnapshot {
    type Target = ControlStatusSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PeerHeartbeatState {
    last_success_unix_ms: Option<u64>,
    last_failure_unix_ms: Option<u64>,
    consecutive_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlLogFileV1 {
    magic: String,
    schema_version: u16,
    current_term: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stepped_down_term: Option<u64>,
    commit_index: u64,
    snapshot_last_index: u64,
    snapshot_last_term: u64,
    entries: Vec<InternalControlLogEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_state: Option<ControlState>,
}

impl ControlConsensusRuntime {
    pub fn open(
        membership: MembershipView,
        state_store: Arc<ControlStateStore>,
        bootstrap_state: ControlState,
        log_path: PathBuf,
        config: ControlConsensusConfig,
    ) -> Result<Self, String> {
        bootstrap_state.validate()?;
        if config.max_append_entries == 0 {
            return Err("cluster control max append entries must be greater than zero".to_string());
        }
        if config.snapshot_interval_entries == 0 {
            return Err(
                "cluster control snapshot interval entries must be greater than zero".to_string(),
            );
        }
        if config.dead_timeout_secs < config.suspect_timeout_secs {
            return Err("cluster control dead timeout must be >= suspect timeout".to_string());
        }
        if config.leader_lease_secs < config.tick_interval_secs {
            return Err("cluster control leader lease must be >= tick interval".to_string());
        }
        let log_parent = log_path.parent().ok_or_else(|| {
            format!(
                "control-log path has no parent directory: {}",
                log_path.display()
            )
        })?;
        let local_disk_budget = state_store.local_disk_budget().cloned();
        if let Some(budget) = local_disk_budget.as_ref() {
            budget
                .create_dir_all_and_sync_parents(log_parent)
                .map_err(|err| {
                    format!(
                        "failed to create control-log directory {}: {err}",
                        log_parent.display()
                    )
                })?;
            budget
                .validate_managed_file_path(&log_path)
                .map_err(|err| {
                    format!(
                        "failed to validate control-log path {}: {err}",
                        log_path.display()
                    )
                })?;
            budget
                .cleanup_atomic_write_temps(&log_path)
                .map_err(|err| {
                    format!(
                        "failed to clean control-log temporary files for {}: {err}",
                        log_path.display()
                    )
                })?;
        } else {
            std::fs::create_dir_all(log_parent).map_err(|err| {
                format!(
                    "failed to create control-log directory {}: {err}",
                    log_parent.display()
                )
            })?;
        }

        let log_existed = log_path.try_exists().map_err(|err| {
            format!(
                "failed to inspect control-log path {}: {err}",
                log_path.display()
            )
        })?;
        let mut persisted = if log_existed {
            let persisted = load_log_file(&log_path)?;
            validate_log_file(&persisted, &log_path)?;
            persisted
        } else {
            let mirror = state_store.load()?;
            let seed_state = if let Some(mirror) = mirror {
                let mut normalized_mirror = mirror.clone();
                let mut normalized_bootstrap = bootstrap_state.clone();
                normalized_mirror.updated_unix_ms = 0;
                normalized_bootstrap.updated_unix_ms = 0;
                if normalized_mirror != normalized_bootstrap {
                    return Err(format!(
                        "control-log file {} is missing and the index-0 state mirror is not equivalent to the configured runtime bootstrap; refusing to promote a non-authoritative mirror",
                        log_path.display()
                    ));
                }
                mirror
            } else {
                bootstrap_state.clone()
            };
            if seed_state.applied_log_index != 0 {
                return Err(format!(
                    "control-log file {} is missing while the state mirror is applied through index {}; refusing to make the non-authoritative mirror authoritative",
                    log_path.display(),
                    seed_state.applied_log_index
                ));
            }
            ControlLogFileV1 {
                magic: CONTROL_LOG_MAGIC.to_string(),
                schema_version: CONTROL_LOG_SCHEMA_VERSION,
                current_term: seed_state.applied_log_term.max(1),
                stepped_down_term: Some(0),
                commit_index: seed_state.applied_log_index,
                snapshot_last_index: seed_state.applied_log_index,
                snapshot_last_term: seed_state.applied_log_term,
                entries: Vec::new(),
                checkpoint_state: Some(seed_state),
            }
        };
        validate_log_file(&persisted, &log_path)?;
        let stepped_down_term = persisted.stepped_down_term.unwrap_or(0);

        let embedded_checkpoint = persisted.checkpoint_state.clone();
        let mirror_state = if embedded_checkpoint.is_some() {
            match state_store.load() {
                Ok(state) => state,
                Err(err) => {
                    eprintln!(
                        "cluster control-state mirror {} is invalid and will be repaired from the authoritative schema-v{} log: {err}",
                        state_store.path().display(),
                        CONTROL_LOG_SCHEMA_VERSION
                    );
                    None
                }
            }
        } else {
            Some(state_store.load()?.ok_or_else(|| {
                format!(
                    "legacy schema-v{} control-log file {} requires a valid control-state mirror",
                    persisted.schema_version,
                    log_path.display()
                )
            })?)
        };
        let recovered_state = if let Some(checkpoint) = embedded_checkpoint.as_ref() {
            checkpoint.clone()
        } else {
            let mirror = mirror_state
                .as_ref()
                .expect("legacy control log required a state mirror above");
            if mirror.applied_log_index < persisted.snapshot_last_index {
                return Err(format!(
                    "control state applied_log_index {} is older than control-log snapshot index {}",
                    mirror.applied_log_index, persisted.snapshot_last_index
                ));
            }
            if mirror.applied_log_index > persisted.commit_index {
                return Err(format!(
                    "control state applied_log_index {} exceeds control-log commit index {}",
                    mirror.applied_log_index, persisted.commit_index
                ));
            }
            mirror.clone()
        };

        let last_index = persisted
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(persisted.snapshot_last_index);
        let next_index = last_index.saturating_add(1);
        let peer_next_index = recovered_state
            .nodes
            .iter()
            .filter(|node| {
                node.id != membership.local_node_id && node.status != ControlNodeStatus::Removed
            })
            .map(|node| (node.id.clone(), next_index))
            .collect::<BTreeMap<_, _>>();
        let peer_heartbeat = recovered_state
            .nodes
            .iter()
            .filter(|node| {
                node.id != membership.local_node_id && node.status != ControlNodeStatus::Removed
            })
            .map(|node| (node.id.clone(), PeerHeartbeatState::default()))
            .collect::<BTreeMap<_, _>>();
        let now_ms = unix_timestamp_millis();

        let runtime = Self {
            local_node_id: membership.local_node_id.clone(),
            state_store,
            log_path,
            local_disk_budget,
            config,
            state: Arc::new(Mutex::new(ConsensusState {
                current_term: persisted.current_term.max(1),
                stepped_down_term,
                commit_index: persisted.commit_index,
                snapshot_last_index: persisted.snapshot_last_index,
                snapshot_last_term: persisted.snapshot_last_term,
                last_leader_contact_unix_ms: now_ms,
                entries: std::mem::take(&mut persisted.entries),
                control_state: recovered_state,
                peer_next_index,
                peer_heartbeat,
                persistence_fence: None,
                checkpoint_pending: None,
                pending_durable_candidate: None,
                cleanup_debt: None,
            })),
            proposal_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime.apply_committed_entries_in_memory_locked(&mut state)?;
            runtime.reconcile_dynamic_peers_locked(&mut state);
            ensure_control_state_runtime_compatible(
                &state.control_state,
                &bootstrap_state,
                &membership.local_node_id,
            )?;
            let mirror_matches = mirror_state.as_ref() == Some(&state.control_state);
            if !log_existed || embedded_checkpoint.is_none() {
                runtime
                    .persist_checkpoint_candidate_locked(&state, ControlCheckpointWriteMode::Growth)
                    .map_err(String::from)?;
            } else if !mirror_matches {
                if let Err(err) = runtime.persist_authoritative_mirror_candidate_locked(&state) {
                    if err.is_committed_cleanup_pending() {
                        state.cleanup_debt = Some(err.to_string());
                    } else {
                        let position = err
                            .committed_checkpoint()
                            .expect("authoritative mirror repair reports committed position");
                        state.persistence_fence = Some(err.to_string());
                        state.checkpoint_pending = Some(ControlCheckpointPending {
                            position,
                            detail: err.to_string(),
                        });
                    }
                    eprintln!(
                        "cluster control-state mirror {} remains pending repair from the authoritative log: {err}",
                        runtime.state_store.path().display()
                    );
                }
            }
        }

        Ok(runtime)
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn current_state(&self) -> ControlState {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .control_state
            .clone()
    }

    pub fn persistence_status(&self) -> ControlPersistenceStatus {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ControlPersistenceStatus {
            fenced: state.persistence_fence.is_some()
                || state.checkpoint_pending.is_some()
                || state.pending_durable_candidate.is_some(),
            pending_checkpoint: state
                .checkpoint_pending
                .as_ref()
                .map(|pending| pending.position),
            cleanup_debt: state.cleanup_debt.is_some(),
            detail: state
                .persistence_fence
                .clone()
                .or_else(|| {
                    state
                        .checkpoint_pending
                        .as_ref()
                        .map(|pending| pending.detail.clone())
                })
                .or_else(|| state.cleanup_debt.clone()),
        }
    }

    pub fn recovery_snapshot_bundle(&self) -> (ControlState, ControlLogRecoverySnapshot) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.recovery_snapshot_bundle_locked(&state)
    }

    pub fn exportable_recovery_snapshot_bundle(
        &self,
    ) -> Result<(ControlState, ControlLogRecoverySnapshot), String> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.persistence_fence.is_some()
            || state.checkpoint_pending.is_some()
            || state.pending_durable_candidate.is_some()
        {
            let detail = state
                .persistence_fence
                .as_deref()
                .or_else(|| {
                    state
                        .checkpoint_pending
                        .as_ref()
                        .map(|pending| pending.detail.as_str())
                })
                .unwrap_or("control persistence repair is pending");
            return Err(format!(
                "control recovery snapshot is unavailable while durable authority is fenced: {detail}"
            ));
        }
        Ok(self.recovery_snapshot_bundle_locked(&state))
    }

    fn recovery_snapshot_bundle_locked(
        &self,
        state: &ConsensusState,
    ) -> (ControlState, ControlLogRecoverySnapshot) {
        (
            state.control_state.clone(),
            ControlLogRecoverySnapshot {
                current_term: state.current_term,
                stepped_down_term: state.stepped_down_term,
                commit_index: state.commit_index,
                snapshot_last_index: state.snapshot_last_index,
                snapshot_last_term: state.snapshot_last_term,
                entries: state.entries.clone(),
            },
        )
    }

    #[allow(dead_code)]
    pub fn log_recovery_snapshot(&self) -> ControlLogRecoverySnapshot {
        self.recovery_snapshot_bundle().1
    }

    fn control_peer_nodes_locked(&self, state: &ConsensusState) -> Vec<(String, String)> {
        state
            .control_state
            .nodes
            .iter()
            .filter(|node| {
                node.id != self.local_node_id && node.status != ControlNodeStatus::Removed
            })
            .map(|node| (node.id.clone(), node.endpoint.clone()))
            .collect()
    }

    fn control_voter_node_ids_locked(&self, state: &ConsensusState) -> Vec<String> {
        state
            .control_state
            .nodes
            .iter()
            .filter(|node| node.status == ControlNodeStatus::Active)
            .map(|node| node.id.clone())
            .collect()
    }

    fn reconcile_dynamic_peers_locked(&self, state: &mut ConsensusState) {
        let next_index = self.last_log_index_locked(state).saturating_add(1).max(1);
        let existing_next_index = state.peer_next_index.clone();
        let existing_heartbeat = state.peer_heartbeat.clone();
        let peer_ids = self
            .control_peer_nodes_locked(state)
            .into_iter()
            .map(|(node_id, _)| node_id)
            .collect::<Vec<_>>();
        state.peer_next_index = peer_ids
            .iter()
            .map(|node_id| {
                (
                    node_id.clone(),
                    existing_next_index
                        .get(node_id)
                        .copied()
                        .unwrap_or(next_index)
                        .max(1),
                )
            })
            .collect();
        state.peer_heartbeat = peer_ids
            .into_iter()
            .map(|node_id| {
                (
                    node_id.clone(),
                    existing_heartbeat
                        .get(&node_id)
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect();
    }

    pub fn preflight_recovery_snapshot(
        &self,
        mut control_state: ControlState,
        log_snapshot: &ControlLogRecoverySnapshot,
        force_local_leader: bool,
    ) -> Result<ControlState, String> {
        if force_local_leader {
            control_state.leader_node_id = Some(self.local_node_id.clone());
            control_state.updated_unix_ms = unix_timestamp_millis();
        }
        control_state.validate()?;
        if !control_state
            .nodes
            .iter()
            .any(|node| node.id == self.local_node_id)
        {
            return Err(format!(
                "control recovery snapshot membership does not include local node '{}'",
                self.local_node_id
            ));
        }
        if force_local_leader
            && !control_state.nodes.iter().any(|node| {
                node.id == self.local_node_id && node.status == ControlNodeStatus::Active
            })
        {
            return Err(format!(
                "control recovery snapshot cannot force local node '{}' as leader because it is not an active voter",
                self.local_node_id
            ));
        }
        let current_state = self.current_state();
        ensure_control_state_runtime_compatible(
            &control_state,
            &current_state,
            &self.local_node_id,
        )?;
        validate_recovery_log_snapshot(log_snapshot)?;
        if control_state.applied_log_index < log_snapshot.snapshot_last_index {
            return Err(format!(
                "control recovery state applied_log_index {} is older than log snapshot index {}",
                control_state.applied_log_index, log_snapshot.snapshot_last_index
            ));
        }
        if control_state.applied_log_index > log_snapshot.commit_index {
            return Err(format!(
                "control recovery state applied_log_index {} exceeds log commit index {}",
                control_state.applied_log_index, log_snapshot.commit_index
            ));
        }
        let expected_applied_term =
            recovery_snapshot_term_at(log_snapshot, control_state.applied_log_index).ok_or_else(
                || {
                    format!(
                        "control recovery log is missing term at applied index {}",
                        control_state.applied_log_index
                    )
                },
            )?;
        if control_state.applied_log_term != expected_applied_term {
            return Err(format!(
                "control recovery state applied_log_term {} does not match log term {} at index {}",
                control_state.applied_log_term,
                expected_applied_term,
                control_state.applied_log_index
            ));
        }

        while control_state.applied_log_index < log_snapshot.commit_index {
            let next_index = control_state.applied_log_index.saturating_add(1);
            if next_index <= log_snapshot.snapshot_last_index {
                return Err(format!(
                    "cannot replay compacted control-log index {} (snapshot index {})",
                    next_index, log_snapshot.snapshot_last_index
                ));
            }
            let offset = usize::try_from(
                next_index
                    .saturating_sub(log_snapshot.snapshot_last_index)
                    .saturating_sub(1),
            )
            .map_err(|_| format!("control-log entry index {next_index} exceeds platform limits"))?;
            let entry = log_snapshot.entries.get(offset).ok_or_else(|| {
                format!("missing committed control-log entry at index {next_index}")
            })?;
            self.apply_command_locked(&mut control_state, &entry.command, entry.index, entry.term)?;
        }

        Ok(control_state)
    }

    pub fn restore_recovery_snapshot(
        &self,
        control_state: ControlState,
        log_snapshot: ControlLogRecoverySnapshot,
        force_local_leader: bool,
    ) -> Result<ControlState, ControlConsensusError> {
        let control_state = self
            .preflight_recovery_snapshot(control_state, &log_snapshot, force_local_leader)
            .map_err(ControlConsensusError::rejected)?;

        let last_log_index = log_snapshot
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(log_snapshot.snapshot_last_index);
        let next_peer_index = last_log_index.saturating_add(1);

        let mut live = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.repair_persistence_fence_locked(&mut live)?;
        let mut candidate = live.clone();
        if force_local_leader {
            candidate.current_term = live
                .current_term
                .max(log_snapshot.current_term)
                .checked_add(1)
                .ok_or_else(|| {
                    ControlConsensusError::rejected(
                        "control consensus term exhausted while forcing local restore leadership",
                    )
                })?;
            candidate.stepped_down_term = 0;
        } else {
            candidate.current_term = live.current_term.max(log_snapshot.current_term).max(1);
            candidate.stepped_down_term = live
                .stepped_down_term
                .max(log_snapshot.stepped_down_term)
                .min(candidate.current_term);
        }
        candidate.commit_index = log_snapshot.commit_index;
        candidate.snapshot_last_index = log_snapshot.snapshot_last_index;
        candidate.snapshot_last_term = log_snapshot.snapshot_last_term;
        candidate.entries = log_snapshot.entries;
        candidate.control_state = control_state;
        candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
        candidate.peer_next_index = self
            .control_peer_nodes_locked(&candidate)
            .into_iter()
            .map(|(node_id, _)| (node_id, next_peer_index))
            .collect();

        self.apply_committed_entries_in_memory_locked(&mut candidate)
            .map_err(ControlConsensusError::rejected)?;
        self.reconcile_dynamic_peers_locked(&mut candidate);
        if candidate.current_term < candidate.control_state.applied_log_term {
            candidate.current_term = candidate.control_state.applied_log_term.max(1);
        }
        self.publish_checkpoint_and_install_locked(
            &mut live,
            candidate,
            ControlCheckpointWriteMode::Growth,
        )?;
        Ok(live.control_state.clone())
    }

    pub fn start_reconciler(
        self: &Arc<Self>,
        rpc_client: RpcClient,
    ) -> tokio::task::JoinHandle<()> {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(runtime.config.tick_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = runtime.ensure_leader_established(&rpc_client).await {
                    eprintln!("cluster control leader proposal failed: {err}");
                    continue;
                }
                if !runtime.is_local_control_leader() {
                    continue;
                }
                if let Err(err) = runtime.replicate_to_all_followers(&rpc_client).await {
                    eprintln!("cluster control follower replication failed: {err}");
                }
            }
        })
    }

    pub async fn ensure_leader_established(
        &self,
        rpc_client: &RpcClient,
    ) -> Result<(), ControlConsensusError> {
        let should_propose = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.repair_persistence_fence_locked(&mut state)?;
            if self.local_is_control_leader_locked(&state) {
                state.last_leader_contact_unix_ms = unix_timestamp_millis();
                false
            } else {
                self.should_attempt_leader_establish_locked(&state, unix_timestamp_millis())
            }
        };
        if !should_propose {
            return Ok(());
        }

        let _ = self
            .propose_command(
                rpc_client,
                InternalControlCommand::SetLeader {
                    leader_node_id: self.local_node_id.clone(),
                },
            )
            .await?;
        Ok(())
    }

    pub async fn propose_command(
        &self,
        rpc_client: &RpcClient,
        command: InternalControlCommand,
    ) -> Result<ProposeOutcome, ControlConsensusError> {
        let _proposal_guard = self.proposal_lock.lock().await;
        let (
            request,
            proposal_entry,
            proposal_index,
            proposal_term,
            proposal_leader_term,
            quorum,
            active_voters,
            peers,
        ) = {
            let mut live = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.repair_persistence_fence_locked(&mut live)?;
            let active_voters = self
                .control_voter_node_ids_locked(&live)
                .into_iter()
                .collect::<BTreeSet<_>>();
            let quorum = self.quorum_size_locked(&live);
            let peers = self.control_peer_nodes_locked(&live);
            self.validate_local_proposal_locked(&live, &command, unix_timestamp_millis())
                .map_err(ControlConsensusError::rejected)?;
            let mut candidate = live.clone();
            let (entry, prev_log_index, prev_log_term, leader_commit_before) = self
                .prepare_proposal_locked(&mut candidate, command)
                .map_err(ControlConsensusError::rejected)?;
            let needs_log_publication = candidate.current_term != live.current_term
                || candidate.entries.len() != live.entries.len();
            if matches!(
                &entry.command,
                InternalControlCommand::SetLeader { leader_node_id } if leader_node_id == &self.local_node_id
            ) {
                candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
            }
            if needs_log_publication {
                self.publish_log_and_install_locked(&mut live, candidate)?;
            } else {
                live.last_leader_contact_unix_ms = candidate.last_leader_contact_unix_ms;
            }
            let proposal_leader_term = live.current_term;
            let request = InternalControlAppendRequest {
                term: proposal_leader_term,
                leader_node_id: self.local_node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries: vec![entry.clone()],
                leader_commit: leader_commit_before,
            };
            (
                request,
                entry.clone(),
                entry.index,
                entry.term,
                proposal_leader_term,
                quorum,
                active_voters,
                peers,
            )
        };

        let mut acknowledged = usize::from(active_voters.contains(&self.local_node_id));
        let mut acknowledged_peers = Vec::new();
        let mut highest_remote_term = 0u64;
        let mut tasks = tokio::task::JoinSet::new();
        for (node_id, endpoint) in peers {
            let rpc_client = rpc_client.clone();
            let request = request.clone();
            tasks.spawn(async move {
                let response = rpc_client.control_append(&endpoint, &request).await;
                (node_id, response)
            });
        }

        while let Some(result) = tasks.join_next().await {
            let (node_id, response) = match result {
                Ok(result) => result,
                Err(err) => {
                    eprintln!("control proposal replication task join failed: {err}");
                    continue;
                }
            };
            match response {
                Ok(response) => {
                    if active_voters.contains(&node_id) && response.term > highest_remote_term {
                        highest_remote_term = response.term;
                    }
                    if response.success {
                        if active_voters.contains(&node_id) {
                            acknowledged += 1;
                        }
                        acknowledged_peers.push(node_id);
                    }
                }
                Err(err) => {
                    eprintln!("control proposal replication RPC failed for {node_id}: {err}");
                }
            }
        }

        if highest_remote_term > proposal_leader_term && acknowledged < quorum {
            let mut live = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            live.current_term = live.current_term.max(highest_remote_term);
            live.stepped_down_term = live.stepped_down_term.max(highest_remote_term);
            self.repair_persistence_fence_locked(&mut live)?;
            let mut candidate = live.clone();
            candidate.current_term = candidate.current_term.max(highest_remote_term);
            candidate.stepped_down_term = candidate.stepped_down_term.max(highest_remote_term);
            self.publish_required_log_candidate_and_install_locked(&mut live, candidate)?;
            return Ok(ProposeOutcome::Pending {
                required: quorum,
                acknowledged,
            });
        }

        if acknowledged < quorum {
            return Ok(ProposeOutcome::Pending {
                required: quorum,
                acknowledged,
            });
        }

        let (commit_index, publication_error, send_commit_notices) = {
            let mut live = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if highest_remote_term > proposal_leader_term {
                live.current_term = live.current_term.max(highest_remote_term);
                live.stepped_down_term = live.stepped_down_term.max(highest_remote_term);
            }
            if let Err(err) = self.repair_persistence_fence_locked(&mut live) {
                let indeterminate = ControlConsensusError::indeterminate(
                    ControlPersistenceStage::Repair,
                    format!(
                        "proposal at index {proposal_index} term {proposal_term} reached quorum before local persistence repair failed: {err}"
                    ),
                    false,
                );
                live.persistence_fence = Some(indeterminate.to_string());
                return Err(indeterminate);
            }
            let proposal_still_present = live.entries.iter().any(|entry| entry == &proposal_entry);
            if !proposal_still_present {
                let indeterminate = ControlConsensusError::indeterminate(
                    ControlPersistenceStage::Repair,
                    format!(
                        "control proposal at index {proposal_index} term {proposal_term} is no longer present after quorum acknowledgement"
                    ),
                    false,
                );
                live.persistence_fence = Some(indeterminate.to_string());
                return Err(indeterminate);
            }
            let mut candidate = live.clone();
            if highest_remote_term > candidate.current_term {
                candidate.current_term = highest_remote_term;
                candidate.stepped_down_term = candidate.stepped_down_term.max(highest_remote_term);
            }
            if candidate.commit_index < proposal_index {
                candidate.commit_index = proposal_index;
            }
            for node_id in &acknowledged_peers {
                candidate
                    .peer_next_index
                    .insert(node_id.clone(), proposal_index.saturating_add(1));
            }
            if let Err(err) = self.apply_committed_entries_in_memory_locked(&mut candidate) {
                let indeterminate = ControlConsensusError::indeterminate(
                    ControlPersistenceStage::Repair,
                    format!(
                        "proposal at index {proposal_index} term {proposal_term} reached quorum but local apply failed: {err}"
                    ),
                    false,
                );
                live.persistence_fence = Some(indeterminate.to_string());
                return Err(indeterminate);
            }
            if self.local_is_control_leader_locked(&candidate) {
                candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
            }
            let commit_index = candidate.commit_index;
            let publication_error = self
                .publish_required_checkpoint_candidate_and_install_locked(
                    &mut live,
                    candidate,
                    ControlCheckpointWriteMode::Growth,
                )
                .err();
            let send_commit_notices = highest_remote_term <= proposal_leader_term
                && self.may_send_proposal_commit_notice_locked(
                    &live,
                    proposal_leader_term,
                    proposal_index,
                );
            (commit_index, publication_error, send_commit_notices)
        };

        let commit_notice = InternalControlAppendRequest {
            term: proposal_leader_term,
            leader_node_id: self.local_node_id.clone(),
            prev_log_index: proposal_index,
            prev_log_term: proposal_term,
            entries: Vec::new(),
            leader_commit: commit_index,
        };
        let commit_peers = if send_commit_notices {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.may_send_proposal_commit_notice_locked(
                &state,
                proposal_leader_term,
                proposal_index,
            ) {
                self.control_peer_nodes_locked(&state)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        let mut post_commit_persistence_detail = None;
        for (node_id, endpoint) in commit_peers {
            let still_authorized = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.may_send_proposal_commit_notice_locked(
                    &state,
                    proposal_leader_term,
                    proposal_index,
                )
            };
            if !still_authorized {
                break;
            }
            let response = rpc_client.control_append(&endpoint, &commit_notice).await;
            if let Ok(response) = response {
                let mut live = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if response.term > proposal_leader_term && active_voters.contains(&node_id) {
                    if let Err(err) = self.repair_persistence_fence_locked(&mut live) {
                        live.current_term = live.current_term.max(response.term);
                        live.stepped_down_term = live.stepped_down_term.max(response.term);
                        let detail = format!(
                            "committed proposal observed higher response term {} from '{}' but could not repair existing durable authority before recording it: {err}",
                            response.term, node_id
                        );
                        live.persistence_fence = Some(detail.clone());
                        post_commit_persistence_detail = Some(detail);
                        break;
                    }
                    if response.term > live.current_term || response.term > live.stepped_down_term {
                        let mut candidate = live.clone();
                        candidate.current_term = candidate.current_term.max(response.term);
                        candidate.stepped_down_term =
                            candidate.stepped_down_term.max(response.term);
                        if let Err(err) = self
                            .publish_required_log_candidate_and_install_locked(&mut live, candidate)
                        {
                            post_commit_persistence_detail = Some(format!(
                                "committed proposal could not durably record higher response term {} from '{}': {err}",
                                response.term, node_id
                            ));
                        }
                    }
                    break;
                }
            }
        }

        if let Some(detail) = post_commit_persistence_detail {
            return Ok(ProposeOutcome::CommittedPersistencePending {
                index: proposal_index,
                term: proposal_term,
                detail,
            });
        }

        if let Some(err) = publication_error {
            let persistence = self.persistence_status();
            if !persistence.fenced
                && persistence.pending_checkpoint.is_none()
                && !persistence.cleanup_debt
            {
                return Ok(ProposeOutcome::Committed {
                    index: proposal_index,
                    term: proposal_term,
                });
            }
            if err.is_committed_cleanup_pending() {
                return Ok(ProposeOutcome::CommittedCleanupPending {
                    index: proposal_index,
                    term: proposal_term,
                    detail: err.to_string(),
                });
            }
            if err.is_committed_checkpoint_pending() {
                return Ok(ProposeOutcome::CommittedCheckpointPending {
                    index: proposal_index,
                    term: proposal_term,
                    detail: err.to_string(),
                });
            }
            return Err(err);
        }

        Ok(ProposeOutcome::Committed {
            index: proposal_index,
            term: proposal_term,
        })
    }

    pub async fn replicate_to_all_followers(&self, rpc_client: &RpcClient) -> Result<(), String> {
        let peers = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.repair_persistence_fence_locked(&mut state)
                .map_err(String::from)?;
            if !self.local_is_control_leader_locked(&state) {
                return Err(format!(
                    "node '{}' is not an unfenced control leader",
                    self.local_node_id
                ));
            }
            self.control_peer_nodes_locked(&state)
        };

        let mut failures = Vec::new();
        for (node_id, endpoint) in peers {
            if let Err(err) = self.sync_peer(rpc_client, &node_id, &endpoint).await {
                failures.push(format!("{node_id}: {err}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "control follower replication failed for {} peer(s): {}",
                failures.len(),
                failures.join("; ")
            ))
        }
    }

    pub fn handle_append_request(
        &self,
        request: InternalControlAppendRequest,
    ) -> Result<InternalControlAppendResponse, ControlConsensusError> {
        let leader_node_id = request.leader_node_id.trim();
        if request.term == 0 {
            return Ok(InternalControlAppendResponse {
                term: self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .current_term,
                success: false,
                match_index: 0,
                message: Some("append term must be greater than zero".to_string()),
            });
        }
        if leader_node_id.is_empty() {
            return Ok(InternalControlAppendResponse {
                term: self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .current_term,
                success: false,
                match_index: 0,
                message: Some("leader_node_id must not be empty".to_string()),
            });
        }
        let mut live = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.is_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlAppendResponse {
                term: live.current_term,
                success: false,
                match_index: 0,
                message: Some("unknown_leader_node".to_string()),
            });
        }
        if !self.is_active_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlAppendResponse {
                term: live.current_term,
                success: false,
                match_index: 0,
                message: Some("leader_not_active_voter".to_string()),
            });
        }
        let pre_repair_term = live.current_term;
        let pre_repair_same_term_conflict = request.term == pre_repair_term
            && live
                .control_state
                .leader_node_id
                .as_deref()
                .is_some_and(|leader| leader != leader_node_id);
        if let Err(err) = self.repair_persistence_fence_locked(&mut live) {
            if request.term > pre_repair_term || pre_repair_same_term_conflict {
                live.current_term = live.current_term.max(request.term);
                live.stepped_down_term = live.stepped_down_term.max(request.term);
            }
            return Err(err);
        }
        if !self.is_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlAppendResponse {
                term: live.current_term,
                success: false,
                match_index: 0,
                message: Some("unknown_leader_node".to_string()),
            });
        }
        if !self.is_active_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlAppendResponse {
                term: live.current_term,
                success: false,
                match_index: 0,
                message: Some("leader_not_active_voter".to_string()),
            });
        }
        let previous_term = live.current_term;
        if request.term < previous_term {
            return Ok(InternalControlAppendResponse {
                term: live.current_term,
                success: false,
                match_index: self.last_log_index_locked(&live),
                message: Some("stale_term".to_string()),
            });
        }
        if request.term == previous_term
            && live
                .control_state
                .leader_node_id
                .as_deref()
                .is_some_and(|leader| leader != leader_node_id)
        {
            if self.local_is_control_leader_locked(&live) && leader_node_id != self.local_node_id {
                let mut step_down_candidate = live.clone();
                step_down_candidate.stepped_down_term = request.term;
                step_down_candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
                self.publish_required_log_candidate_and_install_locked(
                    &mut live,
                    step_down_candidate,
                )?;
            }
            return Ok(InternalControlAppendResponse {
                term: live.current_term,
                success: false,
                match_index: self.last_log_index_locked(&live),
                message: Some("conflicting_leader_same_term".to_string()),
            });
        }
        if request.term > previous_term {
            let mut term_candidate = live.clone();
            term_candidate.current_term = request.term;
            term_candidate.stepped_down_term = term_candidate.stepped_down_term.max(request.term);
            term_candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
            if leader_node_id != self.local_node_id {
                self.mark_peer_success_locked(&mut term_candidate, leader_node_id);
            }
            self.publish_required_log_candidate_and_install_locked(&mut live, term_candidate)?;
        }
        let mut state = live.clone();
        state.last_leader_contact_unix_ms = unix_timestamp_millis();
        if leader_node_id != self.local_node_id {
            self.mark_peer_success_locked(&mut state, leader_node_id);
        }

        if request.prev_log_index < state.snapshot_last_index {
            let response = InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: state.snapshot_last_index,
                message: Some("snapshot_required".to_string()),
            };
            self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
            return Ok(response);
        }

        let Some(local_prev_term) = self.term_at_locked(&state, request.prev_log_index) else {
            let response = InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: self.last_log_index_locked(&state),
                message: Some("missing_prev_log_index".to_string()),
            };
            self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
            return Ok(response);
        };
        if local_prev_term != request.prev_log_term {
            let response = InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: self.last_log_index_locked(&state),
                message: Some("prev_log_term_mismatch".to_string()),
            };
            self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
            return Ok(response);
        }

        if !request.entries.is_empty() {
            let mut expected_index = match request.prev_log_index.checked_add(1) {
                Some(index) => index,
                None => {
                    let response = InternalControlAppendResponse {
                        term: state.current_term,
                        success: false,
                        match_index: self.last_log_index_locked(&state),
                        message: Some("entry_index_overflow".to_string()),
                    };
                    self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                    return Ok(response);
                }
            };
            let mut previous_entry_term = local_prev_term;
            for (position, entry) in request.entries.iter().enumerate() {
                let invalid_message = if entry.index != expected_index {
                    Some(format!(
                        "non_contiguous_entry_index: expected {expected_index}, got {}",
                        entry.index
                    ))
                } else if entry.term == 0 {
                    Some("entry_term_must_be_positive".to_string())
                } else if entry.term > request.term {
                    Some("entry_term_exceeds_request_term".to_string())
                } else if entry.term < previous_entry_term {
                    Some("entry_terms_must_not_decrease".to_string())
                } else {
                    None
                };
                if let Some(message) = invalid_message {
                    let response = InternalControlAppendResponse {
                        term: state.current_term,
                        success: false,
                        match_index: self.last_log_index_locked(&state),
                        message: Some(message),
                    };
                    self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                    return Ok(response);
                }
                previous_entry_term = entry.term;
                if position + 1 < request.entries.len() {
                    expected_index = match entry.index.checked_add(1) {
                        Some(index) => index,
                        None => {
                            let response = InternalControlAppendResponse {
                                term: state.current_term,
                                success: false,
                                match_index: self.last_log_index_locked(&state),
                                message: Some("entry_index_overflow".to_string()),
                            };
                            self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                            return Ok(response);
                        }
                    };
                }
            }
        }

        let entries_len = request.entries.len();
        let mut expected_index = request.prev_log_index.checked_add(1).unwrap_or(0);
        for (position, entry) in request.entries.into_iter().enumerate() {
            let entry_index = entry.index;
            if entry.index != expected_index {
                let response = InternalControlAppendResponse {
                    term: state.current_term,
                    success: false,
                    match_index: self.last_log_index_locked(&state),
                    message: Some(format!(
                        "non_contiguous_entry_index: expected {expected_index}, got {}",
                        entry.index
                    )),
                };
                self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                return Ok(response);
            }
            if entry.term == 0 {
                let response = InternalControlAppendResponse {
                    term: state.current_term,
                    success: false,
                    match_index: self.last_log_index_locked(&state),
                    message: Some("entry_term_must_be_positive".to_string()),
                };
                self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                return Ok(response);
            }

            if entry.index <= state.snapshot_last_index {
                let snapshot_term = self.term_at_locked(&state, entry.index).unwrap_or(0);
                if snapshot_term != entry.term {
                    let response = InternalControlAppendResponse {
                        term: state.current_term,
                        success: false,
                        match_index: state.snapshot_last_index,
                        message: Some("snapshot_conflict".to_string()),
                    };
                    self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                    return Ok(response);
                }
                if position + 1 < entries_len {
                    expected_index = entry_index
                        .checked_add(1)
                        .expect("incoming entry indexes were prevalidated");
                }
                continue;
            }

            if let Some(offset) = self.entry_offset_locked(&state, entry.index) {
                let existing = &state.entries[offset];
                if existing.term != entry.term || existing.command != entry.command {
                    if entry.index <= state.commit_index {
                        return Err(ControlConsensusError::rejected(format!(
                            "cannot overwrite committed control-log entry at index {}",
                            entry.index
                        )));
                    }
                    state.entries.truncate(offset);
                    state.entries.push(entry);
                }
            } else {
                let last_index = self.last_log_index_locked(&state);
                if last_index.checked_add(1) != Some(entry.index) {
                    let response = InternalControlAppendResponse {
                        term: state.current_term,
                        success: false,
                        match_index: last_index,
                        message: Some("entry_index_gap".to_string()),
                    };
                    self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
                    return Ok(response);
                }
                state.entries.push(entry);
            }
            if position + 1 < entries_len {
                expected_index = entry_index
                    .checked_add(1)
                    .expect("incoming entry indexes were prevalidated");
            }
        }

        let last_index = self.last_log_index_locked(&state);
        let previous_commit_index = state.commit_index;
        if request.leader_commit > state.commit_index {
            state.commit_index = std::cmp::min(request.leader_commit, last_index);
            self.apply_committed_entries_in_memory_locked(&mut state)
                .map_err(ControlConsensusError::rejected)?;
        }
        let commit_advanced = state.commit_index > previous_commit_index;
        let durable_log_changed = state.current_term != live.current_term
            || state.entries != live.entries
            || state.snapshot_last_index != live.snapshot_last_index
            || state.snapshot_last_term != live.snapshot_last_term;
        let persistence_message = if commit_advanced {
            match self.publish_required_checkpoint_candidate_and_install_locked(
                &mut live,
                state,
                ControlCheckpointWriteMode::Growth,
            ) {
                Ok(()) => None,
                Err(err) if err.is_committed_checkpoint_pending() => {
                    Some("checkpoint_pending".to_string())
                }
                Err(err) if err.is_committed_cleanup_pending() => {
                    Some("cleanup_pending".to_string())
                }
                Err(err) => return Err(err),
            }
        } else if durable_log_changed {
            self.publish_log_and_install_locked(&mut live, state)?;
            None
        } else {
            live.last_leader_contact_unix_ms = state.last_leader_contact_unix_ms;
            live.peer_heartbeat = state.peer_heartbeat;
            None
        };

        Ok(InternalControlAppendResponse {
            term: live.current_term,
            success: true,
            match_index: last_index,
            message: persistence_message,
        })
    }

    pub fn handle_install_snapshot_request(
        &self,
        request: InternalControlInstallSnapshotRequest,
    ) -> Result<InternalControlInstallSnapshotResponse, ControlConsensusError> {
        let leader_node_id = request.leader_node_id.trim();
        if request.term == 0 {
            return Ok(InternalControlInstallSnapshotResponse {
                term: self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .current_term,
                success: false,
                last_index: 0,
                message: Some("snapshot term must be greater than zero".to_string()),
            });
        }
        if request.snapshot_last_index == 0 {
            return Ok(InternalControlInstallSnapshotResponse {
                term: self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .current_term,
                success: false,
                last_index: 0,
                message: Some("snapshot_last_index must be greater than zero".to_string()),
            });
        }
        if leader_node_id.is_empty() {
            return Ok(InternalControlInstallSnapshotResponse {
                term: self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .current_term,
                success: false,
                last_index: 0,
                message: Some("leader_node_id must not be empty".to_string()),
            });
        }
        let mut live = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.is_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: 0,
                message: Some("unknown_leader_node".to_string()),
            });
        }
        if !self.is_active_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: 0,
                message: Some("leader_not_active_voter".to_string()),
            });
        }
        let pre_repair_term = live.current_term;
        let pre_repair_same_term_conflict = request.term == pre_repair_term
            && live
                .control_state
                .leader_node_id
                .as_deref()
                .is_some_and(|leader| leader != leader_node_id);
        if let Err(err) = self.repair_persistence_fence_locked(&mut live) {
            if request.term > pre_repair_term || pre_repair_same_term_conflict {
                live.current_term = live.current_term.max(request.term);
                live.stepped_down_term = live.stepped_down_term.max(request.term);
            }
            return Err(err);
        }
        if !self.is_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: 0,
                message: Some("unknown_leader_node".to_string()),
            });
        }
        if !self.is_active_membership_node_locked(&live, leader_node_id) {
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: 0,
                message: Some("leader_not_active_voter".to_string()),
            });
        }
        let previous_term = live.current_term;
        if request.term < previous_term {
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: self.last_log_index_locked(&live),
                message: Some("stale_term".to_string()),
            });
        }
        if request.term == previous_term
            && live
                .control_state
                .leader_node_id
                .as_deref()
                .is_some_and(|leader| leader != leader_node_id)
        {
            if self.local_is_control_leader_locked(&live) && leader_node_id != self.local_node_id {
                let mut step_down_candidate = live.clone();
                step_down_candidate.stepped_down_term = request.term;
                step_down_candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
                self.publish_required_log_candidate_and_install_locked(
                    &mut live,
                    step_down_candidate,
                )?;
            }
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: self.last_log_index_locked(&live),
                message: Some("conflicting_leader_same_term".to_string()),
            });
        }
        if request.term > previous_term {
            let mut term_candidate = live.clone();
            term_candidate.current_term = request.term;
            term_candidate.stepped_down_term = term_candidate.stepped_down_term.max(request.term);
            term_candidate.last_leader_contact_unix_ms = unix_timestamp_millis();
            if leader_node_id != self.local_node_id {
                self.mark_peer_success_locked(&mut term_candidate, leader_node_id);
            }
            self.publish_required_log_candidate_and_install_locked(&mut live, term_candidate)?;
        }
        if request.snapshot_last_term == 0 || request.snapshot_last_term > request.term {
            return Ok(InternalControlInstallSnapshotResponse {
                term: live.current_term,
                success: false,
                last_index: self.last_log_index_locked(&live),
                message: Some("invalid_snapshot_term".to_string()),
            });
        }

        let mut snapshot_state: ControlState =
            serde_json::from_value(request.state).map_err(|err| {
                ControlConsensusError::rejected(format!(
                    "failed to decode control snapshot payload: {err}"
                ))
            })?;
        snapshot_state.applied_log_index = request.snapshot_last_index;
        snapshot_state.applied_log_term = request.snapshot_last_term;
        snapshot_state.updated_unix_ms = unix_timestamp_millis();
        snapshot_state.leader_node_id = Some(leader_node_id.to_string());
        snapshot_state
            .validate()
            .map_err(ControlConsensusError::rejected)?;

        let mut state = live.clone();
        state.last_leader_contact_unix_ms = unix_timestamp_millis();
        if leader_node_id != self.local_node_id {
            self.mark_peer_success_locked(&mut state, leader_node_id);
        }
        let min_snapshot_index = state
            .commit_index
            .max(state.control_state.applied_log_index);
        if request.snapshot_last_index < min_snapshot_index {
            let response = InternalControlInstallSnapshotResponse {
                term: state.current_term,
                success: false,
                last_index: self.last_log_index_locked(&state),
                message: Some("stale_snapshot".to_string()),
            };
            self.persist_observed_term_before_rejection_locked(&mut live, &state)?;
            return Ok(response);
        }
        let suffix_is_compatible = self.term_at_locked(&state, request.snapshot_last_index)
            == Some(request.snapshot_last_term);
        state.commit_index = request.snapshot_last_index;
        state.snapshot_last_index = request.snapshot_last_index;
        state.snapshot_last_term = request.snapshot_last_term;
        if suffix_is_compatible {
            state
                .entries
                .retain(|entry| entry.index > request.snapshot_last_index);
        } else {
            state.entries.clear();
        }
        state.control_state = snapshot_state;
        self.reconcile_dynamic_peers_locked(&mut state);
        let persistence_message = match self
            .publish_required_checkpoint_candidate_and_install_locked(
                &mut live,
                state,
                ControlCheckpointWriteMode::Growth,
            ) {
            Ok(()) => None,
            Err(err) if err.is_committed_checkpoint_pending() => {
                Some("checkpoint_pending".to_string())
            }
            Err(err) if err.is_committed_cleanup_pending() => Some("cleanup_pending".to_string()),
            Err(err) => return Err(err),
        };

        Ok(InternalControlInstallSnapshotResponse {
            term: live.current_term,
            success: true,
            last_index: self.last_log_index_locked(&live),
            message: persistence_message,
        })
    }

    fn preferred_leader_id_locked(&self, state: &ConsensusState) -> String {
        self.control_voter_node_ids_locked(state)
            .into_iter()
            .min()
            .unwrap_or_default()
    }

    fn quorum_size_locked(&self, state: &ConsensusState) -> usize {
        (self.control_voter_node_ids_locked(state).len() / 2) + 1
    }

    fn is_active_membership_node_locked(&self, state: &ConsensusState, node_id: &str) -> bool {
        state
            .control_state
            .nodes
            .iter()
            .any(|node| node.status == ControlNodeStatus::Active && node.id == node_id)
    }

    fn is_membership_node_locked(&self, state: &ConsensusState, node_id: &str) -> bool {
        state
            .control_state
            .nodes
            .iter()
            .any(|node| node.status != ControlNodeStatus::Removed && node.id == node_id)
    }

    pub fn is_local_control_leader(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.local_is_control_leader_locked(&state)
    }

    #[allow(dead_code)]
    pub fn liveness_snapshot(&self) -> ControlLivenessSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now_ms = unix_timestamp_millis();
        let mut peers = state
            .peer_heartbeat
            .iter()
            .map(|(node_id, heartbeat)| ControlPeerLivenessSnapshot {
                node_id: node_id.clone(),
                status: self.peer_liveness_status(heartbeat, now_ms),
                last_success_unix_ms: heartbeat.last_success_unix_ms,
                last_failure_unix_ms: heartbeat.last_failure_unix_ms,
                consecutive_failures: heartbeat.consecutive_failures,
            })
            .collect::<Vec<_>>();
        peers.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let suspect_peers = peers
            .iter()
            .filter(|peer| peer.status == ControlPeerLivenessStatus::Suspect)
            .count();
        let dead_peers = peers
            .iter()
            .filter(|peer| peer.status == ControlPeerLivenessStatus::Dead)
            .count();
        let leader_node_id = state.control_state.leader_node_id.clone();
        let leader_last_contact_unix_ms = leader_node_id
            .as_ref()
            .map(|_| state.last_leader_contact_unix_ms);
        let leader_contact_age_ms =
            leader_last_contact_unix_ms.map(|contact_ms| now_ms.saturating_sub(contact_ms));

        ControlLivenessSnapshot {
            local_node_id: self.local_node_id.clone(),
            current_term: state.current_term,
            commit_index: state.commit_index,
            leader_node_id,
            leader_last_contact_unix_ms,
            leader_contact_age_ms,
            leader_stale: self.leader_is_stale_locked(&state, now_ms),
            suspect_peers,
            dead_peers,
            peers,
        }
    }

    /// Captures the live, minimal control-state input needed by rebalance status under one lock.
    ///
    /// The control-state projection establishes its reservation while this lock is held, closing
    /// the measurement-to-clone race without copying unrelated consensus/liveness state.
    pub(crate) fn rebalance_projection_with_execution(
        &self,
        execution: &tsink::QueryExecution,
    ) -> Result<AccountedControlRebalanceProjection, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        execution.checkpoint()?;
        state
            .control_state
            .rebalance_projection_with_execution(&self.local_node_id, execution)
    }

    /// Captures every schema-visible consensus/control input needed by TSDB status under one lock.
    ///
    /// The complete retained output is measured and reserved before any peer, leader, persistence,
    /// or handoff diagnostic is cloned. The private accounted wrapper then keeps that reservation
    /// alive for as long as any status field can be borrowed.
    pub(crate) fn status_snapshot_with_execution(
        &self,
        execution: &tsink::QueryExecution,
    ) -> Result<AccountedControlStatusSnapshot, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        execution.checkpoint()?;

        let liveness_bytes =
            self.modeled_metrics_liveness_retained_bytes_with_execution(&state, execution)?;
        let persistence_bytes = control_persistence_detail(&state)
            .map(modeled_control_metrics_str_bytes)
            .unwrap_or(0);
        let control_bytes = state
            .control_state
            .status_projection_retained_bytes_with_execution(execution)?;
        let peak_bytes = liveness_bytes
            .saturating_add(persistence_bytes)
            .saturating_add(control_bytes);
        let mut reservation = execution.reserve_memory(peak_bytes)?;
        execution.checkpoint()?;

        let now_ms = unix_timestamp_millis();
        let mut peers = Vec::with_capacity(state.peer_heartbeat.len());
        let mut suspect_peers = 0usize;
        let mut dead_peers = 0usize;
        for (node_id, heartbeat) in &state.peer_heartbeat {
            execution.checkpoint()?;
            let status = self.peer_liveness_status(heartbeat, now_ms);
            if status == ControlPeerLivenessStatus::Suspect {
                suspect_peers = suspect_peers.saturating_add(1);
            }
            if status == ControlPeerLivenessStatus::Dead {
                dead_peers = dead_peers.saturating_add(1);
            }
            peers.push(ControlPeerLivenessSnapshot {
                node_id: clone_control_metrics_string(node_id),
                status,
                last_success_unix_ms: heartbeat.last_success_unix_ms,
                last_failure_unix_ms: heartbeat.last_failure_unix_ms,
                consecutive_failures: heartbeat.consecutive_failures,
            });
        }
        let leader_node_id = state
            .control_state
            .leader_node_id
            .as_deref()
            .map(clone_control_metrics_string);
        let leader_last_contact_unix_ms = leader_node_id
            .as_ref()
            .map(|_| state.last_leader_contact_unix_ms);
        let leader_contact_age_ms =
            leader_last_contact_unix_ms.map(|contact_ms| now_ms.saturating_sub(contact_ms));
        let liveness = ControlLivenessSnapshot {
            local_node_id: clone_control_metrics_string(&self.local_node_id),
            current_term: state.current_term,
            commit_index: state.commit_index,
            leader_node_id,
            leader_last_contact_unix_ms,
            leader_contact_age_ms,
            leader_stale: self.leader_is_stale_locked(&state, now_ms),
            suspect_peers,
            dead_peers,
            peers,
        };
        let persistence = ControlPersistenceStatus {
            fenced: state.persistence_fence.is_some()
                || state.checkpoint_pending.is_some()
                || state.pending_durable_candidate.is_some(),
            pending_checkpoint: state
                .checkpoint_pending
                .as_ref()
                .map(|pending| pending.position),
            cleanup_debt: state.cleanup_debt.is_some(),
            detail: control_persistence_detail(&state).map(clone_control_metrics_string),
        };
        let projection = state
            .control_state
            .status_projection_with_execution(execution)?;
        let retained_bytes = modeled_control_metrics_liveness_retained_bytes(&liveness)
            .saturating_add(
                persistence
                    .detail
                    .as_ref()
                    .map(modeled_control_metrics_string_bytes)
                    .unwrap_or(0),
            )
            .saturating_add(modeled_control_status_projection_retained_bytes(
                &projection,
            ));
        reservation.resize(retained_bytes)?;
        let ControlStatusProjection { handoff, hotspot } = projection;

        Ok(AccountedControlStatusSnapshot {
            snapshot: ControlStatusSnapshot {
                liveness,
                persistence,
                handoff,
                hotspot,
            },
            _reservation: reservation,
        })
    }

    /// Captures every consensus/control-state input needed by `/metrics` under one state lock.
    ///
    /// Dynamic output is measured and reserved before any peer, leader, or handoff String/Vec is
    /// cloned. The returned reservation keeps the projection charged until the exporter is done
    /// with it.
    pub(crate) fn metrics_snapshot_with_execution(
        &self,
        execution: &tsink::QueryExecution,
    ) -> Result<AccountedControlMetricsSnapshot, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        execution.checkpoint()?;

        let liveness_bytes =
            self.modeled_metrics_liveness_retained_bytes_with_execution(&state, execution)?;
        let control_bytes = state
            .control_state
            .metrics_projection_retained_bytes_with_execution(execution)?;
        let mut reservation =
            execution.reserve_memory(liveness_bytes.saturating_add(control_bytes))?;
        execution.checkpoint()?;

        let now_ms = unix_timestamp_millis();
        let mut peers = Vec::with_capacity(state.peer_heartbeat.len());
        let mut suspect_peers = 0usize;
        let mut dead_peers = 0usize;
        for (node_id, heartbeat) in &state.peer_heartbeat {
            execution.checkpoint()?;
            let status = self.peer_liveness_status(heartbeat, now_ms);
            if status == ControlPeerLivenessStatus::Suspect {
                suspect_peers = suspect_peers.saturating_add(1);
            }
            if status == ControlPeerLivenessStatus::Dead {
                dead_peers = dead_peers.saturating_add(1);
            }
            peers.push(ControlPeerLivenessSnapshot {
                node_id: clone_control_metrics_string(node_id),
                status,
                last_success_unix_ms: heartbeat.last_success_unix_ms,
                last_failure_unix_ms: heartbeat.last_failure_unix_ms,
                consecutive_failures: heartbeat.consecutive_failures,
            });
        }
        let leader_node_id = state
            .control_state
            .leader_node_id
            .as_deref()
            .map(clone_control_metrics_string);
        let leader_last_contact_unix_ms = leader_node_id
            .as_ref()
            .map(|_| state.last_leader_contact_unix_ms);
        let leader_contact_age_ms =
            leader_last_contact_unix_ms.map(|contact_ms| now_ms.saturating_sub(contact_ms));
        let liveness = ControlLivenessSnapshot {
            local_node_id: clone_control_metrics_string(&self.local_node_id),
            current_term: state.current_term,
            commit_index: state.commit_index,
            leader_node_id,
            leader_last_contact_unix_ms,
            leader_contact_age_ms,
            leader_stale: self.leader_is_stale_locked(&state, now_ms),
            suspect_peers,
            dead_peers,
            peers,
        };
        let persistence = ControlPersistenceStatus {
            fenced: state.persistence_fence.is_some()
                || state.checkpoint_pending.is_some()
                || state.pending_durable_candidate.is_some(),
            pending_checkpoint: state
                .checkpoint_pending
                .as_ref()
                .map(|pending| pending.position),
            cleanup_debt: state.cleanup_debt.is_some(),
            detail: None,
        };
        let projection = state
            .control_state
            .metrics_projection_with_execution(execution)?;
        let retained_bytes = modeled_control_metrics_liveness_retained_bytes(&liveness)
            .saturating_add(modeled_control_metrics_projection_retained_bytes(
                &projection,
            ));
        reservation.resize(retained_bytes)?;
        let ControlMetricsProjection { handoff, hotspot } = projection;

        Ok(AccountedControlMetricsSnapshot {
            snapshot: ControlMetricsSnapshot {
                liveness,
                persistence,
                handoff,
                hotspot,
            },
            _reservation: reservation,
        })
    }

    fn modeled_metrics_liveness_retained_bytes_with_execution(
        &self,
        state: &ConsensusState,
        execution: &tsink::QueryExecution,
    ) -> Result<u64, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let mut retained_bytes = modeled_control_metrics_str_bytes(&self.local_node_id)
            .saturating_add(modeled_control_metrics_vec_bytes::<
                ControlPeerLivenessSnapshot,
            >(state.peer_heartbeat.len()));
        if let Some(leader_node_id) = state.control_state.leader_node_id.as_deref() {
            retained_bytes =
                retained_bytes.saturating_add(modeled_control_metrics_str_bytes(leader_node_id));
        }
        for node_id in state.peer_heartbeat.keys() {
            execution.checkpoint()?;
            retained_bytes =
                retained_bytes.saturating_add(modeled_control_metrics_str_bytes(node_id));
        }
        Ok(retained_bytes)
    }

    fn validate_local_proposal_locked(
        &self,
        state: &ConsensusState,
        command: &InternalControlCommand,
        now_ms: u64,
    ) -> Result<(), String> {
        match command {
            InternalControlCommand::SetLeader { leader_node_id } => {
                if leader_node_id != &self.local_node_id {
                    return Err(format!(
                        "local node '{}' cannot propose set_leader for '{}'",
                        self.local_node_id, leader_node_id
                    ));
                }

                if self.can_local_node_propose_locked(state, now_ms) {
                    return Ok(());
                }

                let leader = self.current_leader_id_locked(state).unwrap_or("<none>");
                Err(format!(
                    "node '{}' is not eligible to propose control command; current leader is '{}' and failover preconditions are not met",
                    self.local_node_id, leader
                ))
            }
            InternalControlCommand::LeaveNode { node_id }
                if node_id == &self.local_node_id
                    && self.local_is_control_leader_locked(state) =>
            {
                Err(format!(
                    "active control leader '{}' cannot leave itself before leadership is transferred",
                    self.local_node_id
                ))
            }
            InternalControlCommand::JoinNode { .. }
            | InternalControlCommand::LeaveNode { .. }
            | InternalControlCommand::RecommissionNode { .. }
            | InternalControlCommand::ActivateNode { .. }
            | InternalControlCommand::RemoveNode { .. }
            | InternalControlCommand::BeginShardHandoff { .. }
            | InternalControlCommand::UpdateShardHandoff { .. }
            | InternalControlCommand::CompleteShardHandoff { .. } => {
                if self.local_is_control_leader_locked(state) {
                    return Ok(());
                }
                let leader = self.current_leader_id_locked(state).unwrap_or("<none>");
                Err(format!(
                    "node '{}' is not the active control leader; current leader is '{}'",
                    self.local_node_id, leader
                ))
            }
        }
    }

    fn can_local_node_propose_locked(&self, state: &ConsensusState, now_ms: u64) -> bool {
        if self.local_is_control_leader_locked(state) {
            return true;
        }

        let Some(current_leader) = self.current_leader_id_locked(state) else {
            return self.preferred_leader_id_locked(state) == self.local_node_id;
        };

        if !self.leader_is_stale_locked(state, now_ms) {
            return false;
        }

        self.next_failover_candidate_id_locked(state, Some(current_leader))
            .is_some_and(|candidate| candidate == self.local_node_id)
    }

    fn should_attempt_leader_establish_locked(&self, state: &ConsensusState, now_ms: u64) -> bool {
        self.can_local_node_propose_locked(state, now_ms)
    }

    fn local_is_control_leader_locked(&self, state: &ConsensusState) -> bool {
        state.persistence_fence.is_none()
            && state.checkpoint_pending.is_none()
            && state.pending_durable_candidate.is_none()
            && state.current_term > state.stepped_down_term
            && self.is_active_membership_node_locked(state, &self.local_node_id)
            && state.control_state.leader_node_id.as_deref() == Some(self.local_node_id.as_str())
    }

    fn may_send_proposal_commit_notice_locked(
        &self,
        state: &ConsensusState,
        proposal_leader_term: u64,
        proposal_index: u64,
    ) -> bool {
        let authoritative_checkpoint_pending = state
            .checkpoint_pending
            .as_ref()
            .is_some_and(|pending| pending.position.index >= proposal_index);
        if state.pending_durable_candidate.is_some()
            || ((state.persistence_fence.is_some() || state.checkpoint_pending.is_some())
                && !authoritative_checkpoint_pending)
            || state.current_term != proposal_leader_term
            || state.current_term <= state.stepped_down_term
            || state.commit_index < proposal_index
            || state.control_state.applied_log_index < proposal_index
            || state.control_state.leader_node_id.as_deref() != Some(self.local_node_id.as_str())
        {
            return false;
        }

        self.is_active_membership_node_locked(state, &self.local_node_id)
    }

    fn current_leader_id_locked<'a>(&self, state: &'a ConsensusState) -> Option<&'a str> {
        state.control_state.leader_node_id.as_deref()
    }

    fn leader_is_stale_locked(&self, state: &ConsensusState, now_ms: u64) -> bool {
        if state.control_state.leader_node_id.is_none() {
            return true;
        }
        if self.local_is_control_leader_locked(state) {
            return false;
        }
        let lease_ms = self.config.leader_lease_timeout().as_millis() as u64;
        now_ms.saturating_sub(state.last_leader_contact_unix_ms) >= lease_ms
    }

    fn next_failover_candidate_id_locked(
        &self,
        state: &ConsensusState,
        current_leader: Option<&str>,
    ) -> Option<String> {
        let node_ids = self.control_voter_node_ids_locked(state);
        if node_ids.is_empty() {
            return None;
        }
        let Some(current_leader) = current_leader else {
            return node_ids.first().cloned();
        };
        if node_ids.len() == 1 {
            return node_ids.first().cloned();
        }

        let Some(current_idx) = node_ids
            .iter()
            .position(|node_id| node_id == current_leader)
        else {
            return node_ids.first().cloned();
        };
        for offset in 1..=node_ids.len() {
            let candidate = &node_ids[(current_idx + offset) % node_ids.len()];
            if candidate != current_leader {
                return Some(candidate.clone());
            }
        }
        None
    }

    fn mark_peer_success_locked(&self, state: &mut ConsensusState, node_id: &str) {
        let heartbeat = state.peer_heartbeat.entry(node_id.to_string()).or_default();
        heartbeat.last_success_unix_ms = Some(unix_timestamp_millis());
        heartbeat.consecutive_failures = 0;
    }

    fn mark_peer_failure_locked(&self, state: &mut ConsensusState, node_id: &str) {
        let heartbeat = state.peer_heartbeat.entry(node_id.to_string()).or_default();
        heartbeat.last_failure_unix_ms = Some(unix_timestamp_millis());
        heartbeat.consecutive_failures = heartbeat.consecutive_failures.saturating_add(1);
    }

    fn peer_liveness_status(
        &self,
        heartbeat: &PeerHeartbeatState,
        now_ms: u64,
    ) -> ControlPeerLivenessStatus {
        let dead_ms = self.config.dead_timeout().as_millis() as u64;
        let suspect_ms = self.config.suspect_timeout().as_millis() as u64;
        let Some(last_success) = heartbeat.last_success_unix_ms else {
            let Some(last_failure) = heartbeat.last_failure_unix_ms else {
                return ControlPeerLivenessStatus::Unknown;
            };
            let failure_age_ms = now_ms.saturating_sub(last_failure);
            return if failure_age_ms >= dead_ms {
                ControlPeerLivenessStatus::Dead
            } else {
                ControlPeerLivenessStatus::Suspect
            };
        };

        let age_ms = now_ms.saturating_sub(last_success);
        if age_ms >= dead_ms {
            ControlPeerLivenessStatus::Dead
        } else if age_ms >= suspect_ms || heartbeat.consecutive_failures > 0 {
            ControlPeerLivenessStatus::Suspect
        } else {
            ControlPeerLivenessStatus::Healthy
        }
    }

    fn prepare_proposal_locked(
        &self,
        state: &mut ConsensusState,
        command: InternalControlCommand,
    ) -> Result<(InternalControlLogEntry, u64, u64, u64), String> {
        if let Some(existing) = self.uncommitted_entry_for_command_locked(state, &command) {
            let prev_log_index = existing.index.saturating_sub(1);
            let prev_log_term = self
                .term_at_locked(state, prev_log_index)
                .ok_or_else(|| "missing prev term for existing proposal entry".to_string())?;
            return Ok((existing, prev_log_index, prev_log_term, state.commit_index));
        }
        if let Some(pending) = state
            .entries
            .iter()
            .find(|entry| entry.index > state.commit_index)
        {
            return Err(format!(
                "control proposal is blocked by uncommitted entry {} term {}; retry after it commits or is replaced",
                pending.index, pending.term
            ));
        }

        let term = state
            .current_term
            .checked_add(1)
            .ok_or_else(|| "control consensus term exhausted u64 range".to_string())?
            .max(1);
        let prev_log_index = self.last_log_index_locked(state);
        let prev_log_term = self
            .term_at_locked(state, prev_log_index)
            .ok_or_else(|| "missing prev term for proposal".to_string())?;
        let entry = InternalControlLogEntry {
            index: prev_log_index
                .checked_add(1)
                .ok_or_else(|| "control-log index exhausted u64 range".to_string())?,
            term,
            command,
            created_unix_ms: unix_timestamp_millis(),
        };
        state.current_term = term;
        state.entries.push(entry.clone());
        Ok((entry, prev_log_index, prev_log_term, state.commit_index))
    }

    fn uncommitted_entry_for_command_locked(
        &self,
        state: &ConsensusState,
        command: &InternalControlCommand,
    ) -> Option<InternalControlLogEntry> {
        state
            .entries
            .iter()
            .rfind(|entry| entry.index > state.commit_index && entry.command == *command)
            .cloned()
    }

    fn apply_committed_entries_in_memory_locked(
        &self,
        state: &mut ConsensusState,
    ) -> Result<bool, String> {
        let mut changed = false;
        while state.control_state.applied_log_index < state.commit_index {
            let next_index = state.control_state.applied_log_index.saturating_add(1);
            if next_index <= state.snapshot_last_index {
                return Err(format!(
                    "cannot replay compacted control-log index {} (snapshot index {})",
                    next_index, state.snapshot_last_index
                ));
            }
            let Some(offset) = self.entry_offset_locked(state, next_index) else {
                return Err(format!(
                    "missing committed control-log entry at index {}",
                    next_index
                ));
            };
            let entry = state.entries[offset].clone();
            self.apply_command_locked(
                &mut state.control_state,
                &entry.command,
                entry.index,
                entry.term,
            )?;
            changed = true;
        }

        self.reconcile_dynamic_peers_locked(state);

        if changed {
            self.maybe_compact_locked(state)?;
        }

        Ok(changed)
    }

    fn apply_command_locked(
        &self,
        control_state: &mut ControlState,
        command: &InternalControlCommand,
        index: u64,
        term: u64,
    ) -> Result<(), String> {
        let mut changed = false;
        match command {
            InternalControlCommand::SetLeader { leader_node_id } => {
                if leader_node_id.trim().is_empty() {
                    return Err(
                        "control command set_leader has an empty leader_node_id".to_string()
                    );
                }
                if !control_state.nodes.iter().any(|node| {
                    node.id == *leader_node_id && node.status == ControlNodeStatus::Active
                }) {
                    return Err(format!(
                        "control command set_leader references node '{}' that is not an active voter",
                        leader_node_id
                    ));
                }
                if control_state.leader_node_id.as_deref() != Some(leader_node_id.as_str()) {
                    control_state.leader_node_id = Some(leader_node_id.clone());
                    changed = true;
                }
            }
            InternalControlCommand::JoinNode { node_id, endpoint } => {
                changed = matches!(
                    control_state.apply_join_node(node_id, endpoint)?,
                    ControlMembershipMutationOutcome::Applied
                );
            }
            InternalControlCommand::LeaveNode { node_id } => {
                changed = matches!(
                    control_state.apply_leave_node(node_id)?,
                    ControlMembershipMutationOutcome::Applied
                );
            }
            InternalControlCommand::RecommissionNode { node_id, endpoint } => {
                changed = matches!(
                    control_state.apply_recommission_node(node_id, endpoint.as_deref())?,
                    ControlMembershipMutationOutcome::Applied
                );
            }
            InternalControlCommand::ActivateNode { node_id } => {
                changed = matches!(
                    control_state.apply_activate_node(node_id)?,
                    ControlMembershipMutationOutcome::Applied
                );
            }
            InternalControlCommand::RemoveNode { node_id } => {
                changed = matches!(
                    control_state.apply_remove_node(node_id)?,
                    ControlMembershipMutationOutcome::Applied
                );
            }
            InternalControlCommand::BeginShardHandoff {
                shard,
                from_node_id,
                to_node_id,
                activation_ring_version,
            } => {
                changed = matches!(
                    control_state.apply_begin_shard_handoff(
                        *shard,
                        from_node_id,
                        to_node_id,
                        *activation_ring_version
                    )?,
                    ControlHandoffMutationOutcome::Applied
                );
            }
            InternalControlCommand::UpdateShardHandoff {
                shard,
                phase,
                copied_rows,
                pending_rows,
                last_error,
            } => {
                changed = matches!(
                    control_state.apply_shard_handoff_progress(
                        *shard,
                        *phase,
                        *copied_rows,
                        *pending_rows,
                        last_error.clone()
                    )?,
                    ControlHandoffMutationOutcome::Applied
                );
            }
            InternalControlCommand::CompleteShardHandoff { shard } => {
                changed = matches!(
                    control_state.apply_complete_shard_handoff(*shard)?,
                    ControlHandoffMutationOutcome::Applied
                );
            }
        }
        control_state.applied_log_index = index;
        control_state.applied_log_term = term;
        if changed {
            control_state.updated_unix_ms = unix_timestamp_millis();
        }
        control_state.validate()
    }

    fn maybe_compact_locked(&self, state: &mut ConsensusState) -> Result<(), String> {
        if state.commit_index <= state.snapshot_last_index {
            return Ok(());
        }
        let committed_span = state.commit_index.saturating_sub(state.snapshot_last_index) as usize;
        if committed_span < self.config.snapshot_interval_entries {
            return Ok(());
        }
        let snapshot_term = self
            .term_at_locked(state, state.commit_index)
            .ok_or_else(|| {
                format!(
                    "missing term for control-log snapshot at index {}",
                    state.commit_index
                )
            })?;

        if committed_span > state.entries.len() {
            return Err(format!(
                "control-log compaction span {} exceeds in-memory log length {}",
                committed_span,
                state.entries.len()
            ));
        }
        state.entries.drain(0..committed_span);
        state.snapshot_last_index = state.commit_index;
        state.snapshot_last_term = snapshot_term;
        Ok(())
    }

    fn encode_log_candidate_locked(
        &self,
        state: &ConsensusState,
    ) -> Result<Vec<u8>, ControlConsensusError> {
        let file = ControlLogFileV1 {
            magic: CONTROL_LOG_MAGIC.to_string(),
            schema_version: CONTROL_LOG_SCHEMA_VERSION,
            current_term: state.current_term,
            stepped_down_term: Some(state.stepped_down_term),
            commit_index: state.commit_index,
            snapshot_last_index: state.snapshot_last_index,
            snapshot_last_term: state.snapshot_last_term,
            entries: state.entries.clone(),
            checkpoint_state: Some(state.control_state.clone()),
        };
        validate_log_file(&file, &self.log_path).map_err(ControlConsensusError::rejected)?;
        let mut encoded = serde_json::to_vec_pretty(&file).map_err(|err| {
            ControlConsensusError::rejected(format!(
                "{} failed: {err}",
                ControlPersistenceStage::LogEncode.as_str()
            ))
        })?;
        encoded.push(b'\n');
        Ok(encoded)
    }

    fn persist_log_candidate_locked(
        &self,
        state: &ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        let encoded = self.encode_log_candidate_locked(state)?;
        let replacement = [ManagedFileReplacement::new(&self.log_path, &encoded)];
        let mut log_published = false;
        let mut publication_ambiguous = false;
        let publish = |staged: &mut StagedManagedFileReplacements| {
            let result = staged.publish(0);
            log_published = staged.is_published(0);
            publication_ambiguous = staged.publication_ambiguous();
            result
        };
        let result = if let Some(budget) = self.local_disk_budget.as_ref() {
            budget.with_staged_managed_file_replacements(
                &replacement,
                DiskCategory::Cluster,
                publish,
            )
        } else {
            tsink::with_staged_file_replacements(&replacement, publish)
        };
        match result {
            Ok(()) => Ok(()),
            Err(err) if log_published || publication_ambiguous => {
                let candidate_visible = std::fs::read(&self.log_path)
                    .map(|current| current == encoded)
                    .unwrap_or(false);
                Err(ControlConsensusError::indeterminate(
                    ControlPersistenceStage::LogPublish,
                    err,
                    candidate_visible,
                ))
            }
            Err(err) => Err(ControlConsensusError::persistence(
                ControlPersistenceStage::LogPublish,
                err,
            )),
        }
    }

    fn persist_checkpoint_candidate_locked(
        &self,
        state: &ConsensusState,
        mode: ControlCheckpointWriteMode,
    ) -> Result<(), ControlConsensusError> {
        let log_encoded = self.encode_log_candidate_locked(state)?;
        let checkpoint_encoded =
            encode_control_state_file(&state.control_state).map_err(|err| {
                ControlConsensusError::rejected(format!(
                    "{} failed: {err}",
                    ControlPersistenceStage::CheckpointEncode.as_str()
                ))
            })?;
        let replacements = [
            ManagedFileReplacement::new(&self.log_path, &log_encoded),
            ManagedFileReplacement::new(self.state_store.path(), &checkpoint_encoded),
        ];
        let mut log_published = false;
        let mut log_durable = false;
        let mut checkpoint_published = false;
        let mut checkpoint_durable = false;
        let mut publication_ambiguous = false;
        let publish = |staged: &mut StagedManagedFileReplacements| {
            let log_result = staged.publish(0);
            log_published = staged.is_published(0);
            publication_ambiguous = staged.publication_ambiguous();
            log_result?;
            log_durable = true;

            #[cfg(test)]
            maybe_fail_control_checkpoint_after_log_publish(&self.log_path)?;

            let checkpoint_result = staged.publish(1);
            checkpoint_published = staged.is_published(1);
            publication_ambiguous |= staged.publication_ambiguous();
            checkpoint_result?;
            checkpoint_durable = true;

            #[cfg(test)]
            maybe_fail_control_pair_finalization(&self.log_path)?;

            Ok(())
        };
        let result = if let Some(budget) = self.local_disk_budget.as_ref() {
            match mode {
                ControlCheckpointWriteMode::Growth => budget.with_staged_managed_file_replacements(
                    &replacements,
                    DiskCategory::Cluster,
                    publish,
                ),
                ControlCheckpointWriteMode::AuthoritativeRecovery => budget
                    .with_staged_managed_file_replacements_for_authoritative_recovery(
                        &replacements,
                        DiskCategory::Cluster,
                        publish,
                    ),
            }
        } else {
            tsink::with_staged_file_replacements(&replacements, publish)
        };

        match result {
            Ok(()) => {
                self.state_store
                    .record_persisted_checkpoint(&state.control_state);
                Ok(())
            }
            Err(err) if log_durable && checkpoint_durable => {
                self.state_store
                    .record_persisted_checkpoint(&state.control_state);
                Err(ControlConsensusError::committed_cleanup_pending(
                    ControlCommitPosition {
                        index: state.commit_index,
                        term: state.control_state.applied_log_term,
                    },
                    err,
                ))
            }
            Err(err) if log_durable => {
                let position = ControlCommitPosition {
                    index: state.commit_index,
                    term: state.control_state.applied_log_term,
                };
                let stage = if checkpoint_published {
                    ControlPersistenceStage::Repair
                } else {
                    ControlPersistenceStage::CheckpointPublish
                };
                Err(ControlConsensusError::committed_checkpoint_pending(
                    position, stage, err,
                ))
            }
            Err(err) if log_published || publication_ambiguous => {
                let candidate_visible = std::fs::read(&self.log_path)
                    .map(|current| current == log_encoded)
                    .unwrap_or(false);
                Err(ControlConsensusError::indeterminate(
                    ControlPersistenceStage::LogPublish,
                    err,
                    candidate_visible,
                ))
            }
            Err(err) => Err(ControlConsensusError::persistence(
                ControlPersistenceStage::LogPublish,
                err,
            )),
        }
    }

    fn persist_authoritative_mirror_candidate_locked(
        &self,
        state: &ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        let checkpoint_encoded =
            encode_control_state_file(&state.control_state).map_err(|err| {
                ControlConsensusError::rejected(format!(
                    "{} failed: {err}",
                    ControlPersistenceStage::CheckpointEncode.as_str()
                ))
            })?;
        let replacement = [ManagedFileReplacement::new(
            self.state_store.path(),
            &checkpoint_encoded,
        )];
        let mut checkpoint_published = false;
        let mut checkpoint_durable = false;
        let publish = |staged: &mut StagedManagedFileReplacements| {
            let result = staged.publish(0);
            checkpoint_published = staged.is_published(0);
            result?;
            checkpoint_durable = true;

            #[cfg(test)]
            maybe_fail_control_pair_finalization(&self.log_path)?;

            Ok(())
        };
        let result = if let Some(budget) = self.local_disk_budget.as_ref() {
            budget.with_staged_managed_file_replacements_for_authoritative_recovery(
                &replacement,
                DiskCategory::Cluster,
                publish,
            )
        } else {
            tsink::with_staged_file_replacements(&replacement, publish)
        };
        match result {
            Ok(()) => {
                self.state_store
                    .record_persisted_checkpoint(&state.control_state);
                Ok(())
            }
            Err(err) if checkpoint_durable => {
                self.state_store
                    .record_persisted_checkpoint(&state.control_state);
                Err(ControlConsensusError::committed_cleanup_pending(
                    ControlCommitPosition {
                        index: state.commit_index,
                        term: state.control_state.applied_log_term,
                    },
                    err,
                ))
            }
            Err(err) => Err(ControlConsensusError::committed_checkpoint_pending(
                ControlCommitPosition {
                    index: state.commit_index,
                    term: state.control_state.applied_log_term,
                },
                if checkpoint_published {
                    ControlPersistenceStage::Repair
                } else {
                    ControlPersistenceStage::CheckpointPublish
                },
                err,
            )),
        }
    }

    fn publish_log_and_install_locked(
        &self,
        live: &mut ConsensusState,
        mut candidate: ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        match self.persist_log_candidate_locked(&candidate) {
            Ok(()) => {
                candidate.persistence_fence = None;
                candidate.pending_durable_candidate = None;
                *live = candidate;
                Ok(())
            }
            Err(err) if err.is_indeterminate() => {
                if err.candidate_visible {
                    candidate.persistence_fence = Some(err.to_string());
                    *live = candidate;
                } else {
                    live.persistence_fence = Some(err.to_string());
                }
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    fn publish_required_log_candidate_and_install_locked(
        &self,
        live: &mut ConsensusState,
        mut candidate: ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        match self.publish_log_and_install_locked(live, candidate.clone()) {
            Ok(()) => Ok(()),
            Err(err) if !err.is_persistence_failure() => Err(err),
            Err(err) => {
                let pending = ControlConsensusError::durable_candidate_pending(
                    ControlPersistenceStage::LogPublish,
                    err,
                );
                candidate.persistence_fence = Some(pending.to_string());
                candidate.pending_durable_candidate = Some(ControlPendingDurableCandidate::LogOnly);
                *live = candidate;
                Err(pending)
            }
        }
    }

    fn persist_observed_term_before_rejection_locked(
        &self,
        live: &mut ConsensusState,
        observed: &ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        if observed.current_term > live.current_term
            || observed.stepped_down_term > live.stepped_down_term
        {
            let mut candidate = live.clone();
            candidate.current_term = observed.current_term;
            candidate.stepped_down_term = observed.stepped_down_term;
            candidate.last_leader_contact_unix_ms = observed.last_leader_contact_unix_ms;
            candidate.peer_heartbeat = observed.peer_heartbeat.clone();
            self.publish_required_log_candidate_and_install_locked(live, candidate)
        } else {
            live.last_leader_contact_unix_ms = observed.last_leader_contact_unix_ms;
            live.peer_heartbeat = observed.peer_heartbeat.clone();
            Ok(())
        }
    }

    fn publish_checkpoint_and_install_locked(
        &self,
        live: &mut ConsensusState,
        mut candidate: ConsensusState,
        mode: ControlCheckpointWriteMode,
    ) -> Result<(), ControlConsensusError> {
        match self.persist_checkpoint_candidate_locked(&candidate, mode) {
            Ok(()) => {
                candidate.persistence_fence = None;
                candidate.checkpoint_pending = None;
                candidate.pending_durable_candidate = None;
                candidate.cleanup_debt = None;
                *live = candidate;
                Ok(())
            }
            Err(err) if err.is_committed_cleanup_pending() => {
                candidate.persistence_fence = None;
                candidate.checkpoint_pending = None;
                candidate.pending_durable_candidate = None;
                candidate.cleanup_debt = Some(err.to_string());
                *live = candidate;
                Err(err)
            }
            Err(err) if err.is_committed_checkpoint_pending() => {
                let position = err
                    .committed_checkpoint()
                    .expect("checked committed position");
                candidate.checkpoint_pending = Some(ControlCheckpointPending {
                    position,
                    detail: err.to_string(),
                });
                candidate.persistence_fence = Some(err.to_string());
                candidate.pending_durable_candidate = None;
                *live = candidate;
                Err(err)
            }
            Err(err) if err.is_indeterminate() => {
                live.persistence_fence = Some(err.to_string());
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    fn publish_required_checkpoint_candidate_and_install_locked(
        &self,
        live: &mut ConsensusState,
        mut candidate: ConsensusState,
        mode: ControlCheckpointWriteMode,
    ) -> Result<(), ControlConsensusError> {
        match self.publish_checkpoint_and_install_locked(live, candidate.clone(), mode) {
            Ok(()) => Ok(()),
            Err(err) if err.is_committed_cleanup_pending() => Err(err),
            Err(err) if err.is_committed_checkpoint_pending() => Err(err),
            Err(err) if !err.is_persistence_failure() => Err(err),
            Err(err) => {
                let pending = ControlConsensusError::durable_candidate_pending(
                    ControlPersistenceStage::LogPublish,
                    err,
                );
                candidate.persistence_fence = Some(pending.to_string());
                candidate.checkpoint_pending = None;
                candidate.pending_durable_candidate =
                    Some(ControlPendingDurableCandidate::Checkpoint(mode));
                *live = candidate;
                Err(pending)
            }
        }
    }

    fn repair_persistence_fence_locked(
        &self,
        state: &mut ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        let result = self.try_repair_persistence_fence_locked(state);
        if let Err(err) = &result {
            state.persistence_fence = Some(err.to_string());
        }
        result
    }

    fn try_repair_persistence_fence_locked(
        &self,
        state: &mut ConsensusState,
    ) -> Result<(), ControlConsensusError> {
        let repair_required = state.cleanup_debt.is_some()
            || state.persistence_fence.is_some()
            || state.checkpoint_pending.is_some()
            || state.pending_durable_candidate.is_some();
        if repair_required {
            if let Some(budget) = self.local_disk_budget.as_ref() {
                budget
                    .cleanup_atomic_write_temps(&self.log_path)
                    .map_err(|err| {
                        ControlConsensusError::persistence(ControlPersistenceStage::Repair, err)
                    })?;
                budget
                    .cleanup_atomic_write_temps(self.state_store.path())
                    .map_err(|err| {
                        ControlConsensusError::persistence(ControlPersistenceStage::Repair, err)
                    })?;
                budget.reconcile_when_idle().map_err(|err| {
                    ControlConsensusError::persistence(ControlPersistenceStage::Repair, err)
                })?;
            }
            state.cleanup_debt = None;
        }

        if let Some(pending) = state.pending_durable_candidate {
            let mut candidate = state.clone();
            candidate.persistence_fence = None;
            candidate.checkpoint_pending = None;
            candidate.pending_durable_candidate = None;
            let repair_result = match pending {
                ControlPendingDurableCandidate::LogOnly => {
                    self.persist_log_candidate_locked(&candidate)
                }
                ControlPendingDurableCandidate::Checkpoint(mode) => {
                    self.persist_checkpoint_candidate_locked(&candidate, mode)
                }
            };
            return match repair_result {
                Ok(()) => {
                    *state = candidate;
                    Ok(())
                }
                Err(err) if err.is_committed_cleanup_pending() => {
                    candidate.cleanup_debt = Some(err.to_string());
                    *state = candidate;
                    Ok(())
                }
                Err(err) if err.is_committed_checkpoint_pending() => {
                    let position = err
                        .committed_checkpoint()
                        .expect("checked committed checkpoint position");
                    candidate.persistence_fence = Some(err.to_string());
                    candidate.checkpoint_pending = Some(ControlCheckpointPending {
                        position,
                        detail: err.to_string(),
                    });
                    *state = candidate;
                    Err(err)
                }
                Err(err) => {
                    if err.is_persistence_failure() {
                        let stage = match pending {
                            ControlPendingDurableCandidate::LogOnly => {
                                ControlPersistenceStage::LogPublish
                            }
                            ControlPendingDurableCandidate::Checkpoint(_) => {
                                ControlPersistenceStage::CheckpointPublish
                            }
                        };
                        let pending = ControlConsensusError::durable_candidate_pending(stage, err);
                        state.persistence_fence = Some(pending.to_string());
                        Err(pending)
                    } else {
                        state.persistence_fence = Some(err.to_string());
                        Err(err)
                    }
                }
            };
        }

        if state.persistence_fence.is_none() && state.checkpoint_pending.is_none() {
            return Ok(());
        }

        let persisted = load_log_file(&self.log_path).map_err(|err| {
            ControlConsensusError::indeterminate(ControlPersistenceStage::Repair, err, false)
        })?;
        validate_log_file(&persisted, &self.log_path).map_err(|err| {
            ControlConsensusError::indeterminate(ControlPersistenceStage::Repair, err, false)
        })?;
        let checkpoint = persisted.checkpoint_state.clone().ok_or_else(|| {
            ControlConsensusError::indeterminate(
                ControlPersistenceStage::Repair,
                "authoritative control log does not contain a checkpoint",
                false,
            )
        })?;
        let persisted_stepped_down_term = persisted.stepped_down_term.unwrap_or(0);
        let repair_mirror_only = state.checkpoint_pending.is_some()
            && state.current_term <= persisted.current_term
            && state.stepped_down_term <= persisted_stepped_down_term;
        let mut candidate = state.clone();
        candidate.current_term = state.current_term.max(persisted.current_term);
        candidate.stepped_down_term = state
            .stepped_down_term
            .max(persisted_stepped_down_term)
            .min(candidate.current_term);
        candidate.commit_index = persisted.commit_index;
        candidate.snapshot_last_index = persisted.snapshot_last_index;
        candidate.snapshot_last_term = persisted.snapshot_last_term;
        candidate.entries = persisted.entries;
        candidate.control_state = checkpoint;
        candidate.persistence_fence = None;
        candidate.checkpoint_pending = None;
        candidate.pending_durable_candidate = None;
        self.reconcile_dynamic_peers_locked(&mut candidate);
        if !repair_mirror_only {
            return match self.publish_checkpoint_and_install_locked(
                state,
                candidate,
                ControlCheckpointWriteMode::AuthoritativeRecovery,
            ) {
                Ok(()) => Ok(()),
                Err(err) if err.is_committed_cleanup_pending() => Ok(()),
                Err(err) => Err(err),
            };
        }
        match self.persist_authoritative_mirror_candidate_locked(&candidate) {
            Ok(()) => {
                *state = candidate;
                Ok(())
            }
            Err(err) if err.is_committed_cleanup_pending() => {
                candidate.cleanup_debt = Some(err.to_string());
                *state = candidate;
                Ok(())
            }
            Err(err) => {
                let position = err
                    .committed_checkpoint()
                    .expect("authoritative mirror repair reports committed position");
                candidate.persistence_fence = Some(err.to_string());
                candidate.checkpoint_pending = Some(ControlCheckpointPending {
                    position,
                    detail: err.to_string(),
                });
                *state = candidate;
                Err(err)
            }
        }
    }

    fn last_log_index_locked(&self, state: &ConsensusState) -> u64 {
        state
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(state.snapshot_last_index)
    }

    fn entry_offset_locked(&self, state: &ConsensusState, index: u64) -> Option<usize> {
        if index <= state.snapshot_last_index {
            return None;
        }
        let offset = index
            .checked_sub(state.snapshot_last_index)?
            .checked_sub(1)?;
        let offset = usize::try_from(offset).ok()?;
        (offset < state.entries.len()).then_some(offset)
    }

    fn term_at_locked(&self, state: &ConsensusState, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == state.snapshot_last_index {
            return Some(state.snapshot_last_term);
        }
        let offset = self.entry_offset_locked(state, index)?;
        Some(state.entries[offset].term)
    }

    async fn sync_peer(
        &self,
        rpc_client: &RpcClient,
        node_id: &str,
        endpoint: &str,
    ) -> Result<(), String> {
        for attempt in 0..CONTROL_SYNC_MAX_ATTEMPTS {
            let (plan, peer_was_active) = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !self.local_is_control_leader_locked(&state) {
                    return Err(format!(
                        "node '{}' stepped down before synchronizing peer '{node_id}'",
                        self.local_node_id
                    ));
                }
                (
                    self.build_peer_plan_locked(&state, node_id)?,
                    self.is_active_membership_node_locked(&state, node_id),
                )
            };

            match plan {
                PeerPlan::Append(request) => {
                    match rpc_client.control_append(endpoint, &request).await {
                        Ok(response) => {
                            {
                                let mut state = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                self.mark_peer_success_locked(&mut state, node_id);
                            }
                            if response.term > request.term && peer_was_active {
                                let mut live = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                self.persist_observed_higher_peer_term_locked(
                                    &mut live,
                                    response.term,
                                    node_id,
                                )?;
                                return Ok(());
                            }
                            if response.success {
                                let mut state = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                state.peer_next_index.insert(
                                    node_id.to_string(),
                                    response.match_index.saturating_add(1),
                                );
                                return Ok(());
                            }

                            let mut state = self
                                .state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let next_index = self
                                .peer_next_index_after_append_reject(&state, node_id, &response);
                            state
                                .peer_next_index
                                .insert(node_id.to_string(), next_index);
                        }
                        Err(err) => {
                            let mut state = self
                                .state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            self.mark_peer_failure_locked(&mut state, node_id);
                            if attempt + 1 < CONTROL_SYNC_MAX_ATTEMPTS {
                                continue;
                            }
                            return Err(format!("control append RPC to {node_id} failed: {err}"));
                        }
                    }
                }
                PeerPlan::InstallSnapshot(request) => {
                    match rpc_client
                        .control_install_snapshot(endpoint, &request)
                        .await
                    {
                        Ok(response) => {
                            {
                                let mut state = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                self.mark_peer_success_locked(&mut state, node_id);
                            }
                            if response.term > request.term && peer_was_active {
                                let mut live = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                self.persist_observed_higher_peer_term_locked(
                                    &mut live,
                                    response.term,
                                    node_id,
                                )?;
                                return Ok(());
                            }
                            if response.success {
                                let mut state = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                state.peer_next_index.insert(
                                    node_id.to_string(),
                                    response.last_index.saturating_add(1),
                                );
                                continue;
                            }
                            return Ok(());
                        }
                        Err(err) => {
                            let mut state = self
                                .state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            self.mark_peer_failure_locked(&mut state, node_id);
                            if attempt + 1 < CONTROL_SYNC_MAX_ATTEMPTS {
                                continue;
                            }
                            return Err(format!("control snapshot RPC to {node_id} failed: {err}"));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn persist_observed_higher_peer_term_locked(
        &self,
        live: &mut ConsensusState,
        observed_term: u64,
        node_id: &str,
    ) -> Result<(), String> {
        if observed_term <= live.current_term && observed_term <= live.stepped_down_term {
            return Ok(());
        }
        if let Err(err) = self.repair_persistence_fence_locked(live) {
            live.current_term = live.current_term.max(observed_term);
            live.stepped_down_term = live.stepped_down_term.max(observed_term);
            let detail = format!(
                "observed higher control term {observed_term} from active peer '{node_id}' but could not repair existing durable authority before recording it: {err}"
            );
            live.persistence_fence = Some(detail.clone());
            return Err(detail);
        }
        if observed_term <= live.current_term && observed_term <= live.stepped_down_term {
            return Ok(());
        }

        let mut candidate = live.clone();
        candidate.current_term = candidate.current_term.max(observed_term);
        candidate.stepped_down_term = candidate.stepped_down_term.max(observed_term);
        self.publish_required_log_candidate_and_install_locked(live, candidate)
            .map_err(String::from)
    }

    fn peer_next_index_after_append_reject(
        &self,
        state: &ConsensusState,
        node_id: &str,
        response: &InternalControlAppendResponse,
    ) -> u64 {
        let current_next = state
            .peer_next_index
            .get(node_id)
            .copied()
            .unwrap_or(1)
            .max(1);
        let hinted_next = response.match_index.saturating_add(1).max(1);
        match response.message.as_deref() {
            Some("snapshot_required") => hinted_next,
            _ => std::cmp::min(current_next.saturating_sub(1).max(1), hinted_next),
        }
    }

    fn build_peer_plan_locked(
        &self,
        state: &ConsensusState,
        node_id: &str,
    ) -> Result<PeerPlan, String> {
        let last_index = self.last_log_index_locked(state);
        let next_index = state
            .peer_next_index
            .get(node_id)
            .copied()
            .unwrap_or(last_index.saturating_add(1));
        let current_term = state.current_term.max(1);

        if next_index <= state.snapshot_last_index {
            let snapshot_payload = serde_json::to_value(&state.control_state)
                .map_err(|err| format!("failed to encode control snapshot payload: {err}"))?;
            return Ok(PeerPlan::InstallSnapshot(
                InternalControlInstallSnapshotRequest {
                    term: current_term,
                    leader_node_id: self.local_node_id.clone(),
                    snapshot_last_index: state.commit_index,
                    snapshot_last_term: state.control_state.applied_log_term,
                    state: snapshot_payload,
                },
            ));
        }

        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = self
            .term_at_locked(state, prev_log_index)
            .ok_or_else(|| format!("missing prev log term for index {}", prev_log_index))?;
        let entries = state
            .entries
            .iter()
            .filter(|entry| entry.index >= next_index)
            .take(self.config.max_append_entries)
            .cloned()
            .collect::<Vec<_>>();

        Ok(PeerPlan::Append(InternalControlAppendRequest {
            term: current_term,
            leader_node_id: self.local_node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: state.commit_index,
        }))
    }

    #[cfg(test)]
    fn log_snapshot_position(&self) -> (u64, u64, usize) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.snapshot_last_index,
            state.snapshot_last_term,
            state.entries.len(),
        )
    }
}

enum PeerPlan {
    Append(InternalControlAppendRequest),
    InstallSnapshot(InternalControlInstallSnapshotRequest),
}

fn ensure_control_state_runtime_compatible(
    recovered: &ControlState,
    runtime_bootstrap: &ControlState,
    local_node_id: &str,
) -> Result<(), String> {
    recovered.validate()?;
    runtime_bootstrap.validate()?;
    let runtime_local = runtime_bootstrap
        .node_record(local_node_id)
        .ok_or_else(|| {
            format!("runtime bootstrap is missing local cluster node '{local_node_id}'")
        })?;
    let recovered_local = recovered.node_record(local_node_id).ok_or_else(|| {
        format!("persisted control membership is missing local cluster node '{local_node_id}'")
    })?;
    if recovered_local.endpoint != runtime_local.endpoint {
        return Err(format!(
            "persisted control membership endpoint mismatch for local node '{}': '{}' != '{}'",
            local_node_id, recovered_local.endpoint, runtime_local.endpoint
        ));
    }
    if recovered.ring.shard_count != runtime_bootstrap.ring.shard_count {
        return Err(format!(
            "persisted control ring shard_count {} does not match runtime shard_count {}",
            recovered.ring.shard_count, runtime_bootstrap.ring.shard_count
        ));
    }
    if recovered.ring.hash_version != runtime_bootstrap.ring.hash_version {
        return Err(format!(
            "persisted control ring hash_version {} does not match runtime hash_version {}",
            recovered.ring.hash_version, runtime_bootstrap.ring.hash_version
        ));
    }
    if recovered.ring.replication_factor != runtime_bootstrap.ring.replication_factor {
        return Err(format!(
            "persisted control ring replication_factor {} does not match runtime replication_factor {}",
            recovered.ring.replication_factor, runtime_bootstrap.ring.replication_factor
        ));
    }
    if recovered.ring.virtual_nodes_per_node != runtime_bootstrap.ring.virtual_nodes_per_node {
        return Err(format!(
            "persisted control ring virtual_nodes_per_node {} does not match runtime virtual_nodes_per_node {}",
            recovered.ring.virtual_nodes_per_node,
            runtime_bootstrap.ring.virtual_nodes_per_node
        ));
    }
    Ok(())
}

fn validate_log_file(file: &ControlLogFileV1, path: &Path) -> Result<(), String> {
    if file.magic != CONTROL_LOG_MAGIC {
        return Err(format!(
            "control-log file {} has unsupported magic '{}'",
            path.display(),
            file.magic
        ));
    }
    if !matches!(
        file.schema_version,
        CONTROL_LOG_LEGACY_SCHEMA_VERSION | CONTROL_LOG_SCHEMA_VERSION
    ) {
        return Err(format!(
            "control-log file {} has unsupported schema version {}",
            path.display(),
            file.schema_version
        ));
    }
    if file.current_term == 0 {
        return Err(format!(
            "control-log file {} has invalid current_term=0",
            path.display()
        ));
    }
    match (file.schema_version, file.stepped_down_term) {
        (CONTROL_LOG_SCHEMA_VERSION, Some(term)) if term <= file.current_term => {}
        (CONTROL_LOG_SCHEMA_VERSION, Some(term)) => {
            return Err(format!(
                "control-log file {} has steppedDownTerm {} greater than current term {}",
                path.display(),
                term,
                file.current_term
            ));
        }
        (CONTROL_LOG_SCHEMA_VERSION, None) => {
            return Err(format!(
                "control-log schema v{} file {} is missing steppedDownTerm",
                CONTROL_LOG_SCHEMA_VERSION,
                path.display()
            ));
        }
        (CONTROL_LOG_LEGACY_SCHEMA_VERSION, None) => {}
        (CONTROL_LOG_LEGACY_SCHEMA_VERSION, Some(_)) => {
            return Err(format!(
                "legacy control-log schema v{} file {} unexpectedly contains steppedDownTerm",
                CONTROL_LOG_LEGACY_SCHEMA_VERSION,
                path.display()
            ));
        }
        _ => unreachable!("schema version was validated above"),
    }
    if file.snapshot_last_index > file.commit_index {
        return Err(format!(
            "control-log file {} has snapshot_last_index {} greater than commit_index {}",
            path.display(),
            file.snapshot_last_index,
            file.commit_index
        ));
    }
    if file.snapshot_last_index == 0 && file.snapshot_last_term != 0 {
        return Err(format!(
            "control-log file {} has snapshot term {} at index 0",
            path.display(),
            file.snapshot_last_term
        ));
    }
    if file.snapshot_last_index > 0 && file.snapshot_last_term == 0 {
        return Err(format!(
            "control-log file {} has term 0 at nonzero snapshot index {}",
            path.display(),
            file.snapshot_last_index
        ));
    }
    if file.snapshot_last_term > file.current_term {
        return Err(format!(
            "control-log file {} has snapshot term {} greater than current term {}",
            path.display(),
            file.snapshot_last_term,
            file.current_term
        ));
    }

    let mut expected_index = file.snapshot_last_index.checked_add(1).ok_or_else(|| {
        format!(
            "control-log file {} cannot contain entries after index {}",
            path.display(),
            file.snapshot_last_index
        )
    })?;
    let mut previous_term = file.snapshot_last_term;
    for entry in &file.entries {
        if entry.index != expected_index {
            return Err(format!(
                "control-log file {} has non-contiguous entry index {}, expected {}",
                path.display(),
                entry.index,
                expected_index
            ));
        }
        if entry.term == 0 {
            return Err(format!(
                "control-log file {} has entry {} with term=0",
                path.display(),
                entry.index
            ));
        }
        if entry.term < previous_term {
            return Err(format!(
                "control-log file {} has decreasing term {} at entry {} after term {}",
                path.display(),
                entry.term,
                entry.index,
                previous_term
            ));
        }
        if entry.term > file.current_term {
            return Err(format!(
                "control-log file {} has entry {} term {} greater than current term {}",
                path.display(),
                entry.index,
                entry.term,
                file.current_term
            ));
        }
        previous_term = entry.term;
        expected_index = expected_index.checked_add(1).ok_or_else(|| {
            format!(
                "control-log file {} entry indexes exceed the supported range",
                path.display()
            )
        })?;
    }

    let last_index = file
        .entries
        .last()
        .map(|entry| entry.index)
        .unwrap_or(file.snapshot_last_index);
    if file.commit_index > last_index {
        return Err(format!(
            "control-log file {} has commit_index {} beyond last log index {}",
            path.display(),
            file.commit_index,
            last_index
        ));
    }

    if file.schema_version == CONTROL_LOG_SCHEMA_VERSION {
        let checkpoint = file.checkpoint_state.as_ref().ok_or_else(|| {
            format!(
                "control-log schema v{} file {} is missing checkpointState",
                CONTROL_LOG_SCHEMA_VERSION,
                path.display()
            )
        })?;
        checkpoint.validate().map_err(|err| {
            format!(
                "control-log checkpointState validation failed for {}: {err}",
                path.display()
            )
        })?;
        if checkpoint.applied_log_index != file.commit_index {
            return Err(format!(
                "control-log checkpointState in {} is applied through index {}, expected commit index {}",
                path.display(),
                checkpoint.applied_log_index,
                file.commit_index
            ));
        }
        let committed_term = if file.commit_index == 0 {
            0
        } else if file.commit_index == file.snapshot_last_index {
            file.snapshot_last_term
        } else {
            let offset = usize::try_from(
                file.commit_index
                    .saturating_sub(file.snapshot_last_index)
                    .saturating_sub(1),
            )
            .map_err(|_| {
                format!(
                    "control-log commit index {} exceeds platform limits in {}",
                    file.commit_index,
                    path.display()
                )
            })?;
            file.entries
                .get(offset)
                .map(|entry| entry.term)
                .ok_or_else(|| {
                    format!(
                        "control-log file {} is missing committed entry {}",
                        path.display(),
                        file.commit_index
                    )
                })?
        };
        if checkpoint.applied_log_term != committed_term {
            return Err(format!(
                "control-log checkpointState in {} has applied term {}, expected {} at commit index {}",
                path.display(),
                checkpoint.applied_log_term,
                committed_term,
                file.commit_index
            ));
        }
    } else if file.checkpoint_state.is_some() {
        return Err(format!(
            "legacy control-log schema v{} file {} unexpectedly contains checkpointState",
            CONTROL_LOG_LEGACY_SCHEMA_VERSION,
            path.display()
        ));
    }

    Ok(())
}

fn validate_recovery_log_snapshot(snapshot: &ControlLogRecoverySnapshot) -> Result<(), String> {
    if snapshot.stepped_down_term > snapshot.current_term {
        return Err(format!(
            "control recovery snapshot steppedDownTerm {} exceeds currentTerm {}",
            snapshot.stepped_down_term, snapshot.current_term
        ));
    }
    let path = Path::new("<recovery-snapshot>");
    let file = ControlLogFileV1 {
        magic: CONTROL_LOG_MAGIC.to_string(),
        schema_version: CONTROL_LOG_LEGACY_SCHEMA_VERSION,
        current_term: snapshot.current_term,
        stepped_down_term: None,
        commit_index: snapshot.commit_index,
        snapshot_last_index: snapshot.snapshot_last_index,
        snapshot_last_term: snapshot.snapshot_last_term,
        entries: snapshot.entries.clone(),
        checkpoint_state: None,
    };
    validate_log_file(&file, path)
}

fn recovery_snapshot_term_at(snapshot: &ControlLogRecoverySnapshot, index: u64) -> Option<u64> {
    if index == 0 {
        return Some(0);
    }
    if index == snapshot.snapshot_last_index {
        return Some(snapshot.snapshot_last_term);
    }
    let offset = index
        .checked_sub(snapshot.snapshot_last_index)?
        .checked_sub(1)?;
    let offset = usize::try_from(offset).ok()?;
    snapshot.entries.get(offset).map(|entry| entry.term)
}

fn load_log_file(path: &Path) -> Result<ControlLogFileV1, String> {
    let raw = std::fs::read(path)
        .map_err(|err| format!("failed to read control-log file {}: {err}", path.display()))?;
    serde_json::from_slice(&raw)
        .map_err(|err| format!("failed to parse control-log file {}: {err}", path.display()))
}

fn control_persistence_detail(state: &ConsensusState) -> Option<&str> {
    state
        .persistence_fence
        .as_deref()
        .or_else(|| {
            state
                .checkpoint_pending
                .as_ref()
                .map(|pending| pending.detail.as_str())
        })
        .or(state.cleanup_debt.as_deref())
}

fn modeled_control_metrics_liveness_retained_bytes(snapshot: &ControlLivenessSnapshot) -> u64 {
    modeled_control_metrics_string_bytes(&snapshot.local_node_id)
        .saturating_add(
            snapshot
                .leader_node_id
                .as_ref()
                .map(modeled_control_metrics_string_bytes)
                .unwrap_or(0),
        )
        .saturating_add(modeled_control_metrics_vec_bytes::<
            ControlPeerLivenessSnapshot,
        >(snapshot.peers.capacity()))
        .saturating_add(snapshot.peers.iter().fold(0u64, |retained_bytes, peer| {
            retained_bytes.saturating_add(modeled_control_metrics_string_bytes(&peer.node_id))
        }))
}

fn modeled_control_metrics_vec_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
        .saturating_add(CONTROL_METRICS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_control_metrics_str_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        u64::try_from(value.len())
            .unwrap_or(u64::MAX)
            .saturating_add(CONTROL_METRICS_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_control_metrics_string_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        u64::try_from(value.capacity())
            .unwrap_or(u64::MAX)
            .saturating_add(CONTROL_METRICS_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn clone_control_metrics_string(value: &str) -> String {
    let mut cloned = String::with_capacity(value.len());
    cloned.push_str(value);
    cloned
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::control::{
        ControlState, ShardHandoffPhase, ShardHandoffProgress, ShardOwnershipTransition,
    };
    use crate::cluster::membership::{ClusterNode, MembershipView};
    use crate::cluster::ring::ShardRing;
    use crate::cluster::rpc::{
        derive_shared_internal_token, RpcClientConfig, INTERNAL_RPC_PROTOCOL_VERSION,
    };
    use crate::http::{read_http_request, write_http_response, HttpResponse};
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tsink::disk_budget::LocalDiskLimits;
    use tsink::{
        QueryBudget, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken, QueryLimitReason,
        QueryWorkLimits,
    };

    fn sample_membership_and_state() -> (MembershipView, ControlState) {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            shards: 16,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&config).expect("membership should build");
        let ring = ShardRing::build(16, 2, &membership).expect("ring should build");
        (
            membership.clone(),
            ControlState::from_runtime(&membership, &ring),
        )
    }

    fn open_runtime_for_node(
        temp_dir: &TempDir,
        node_id: &str,
        bind: &str,
        seeds: &[&str],
        state_file_stem: &str,
        snapshot_interval_entries: usize,
    ) -> ControlConsensusRuntime {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some(node_id.to_string()),
            bind: Some(bind.to_string()),
            seeds: seeds.iter().map(|seed| (*seed).to_string()).collect(),
            shards: 16,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&config).expect("membership should build");
        let ring = ShardRing::build(16, 2, &membership).expect("ring should build");
        let bootstrap_state = ControlState::from_runtime(&membership, &ring);
        let state_store = Arc::new(
            ControlStateStore::open(
                temp_dir
                    .path()
                    .join(format!("{state_file_stem}.control-state.json")),
            )
            .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            temp_dir
                .path()
                .join(format!("{state_file_stem}.control-log.json")),
            ControlConsensusConfig {
                snapshot_interval_entries,
                ..ControlConsensusConfig::default()
            },
        )
        .expect("runtime should open")
    }

    fn force_local_leader(runtime: &ControlConsensusRuntime) {
        let state = runtime.current_state();
        let log = runtime.log_recovery_snapshot();
        runtime
            .restore_recovery_snapshot(state, log, true)
            .expect("local leader fixture should persist");
        assert!(runtime.is_local_control_leader());
    }

    fn configure_metrics_projection_fixture(runtime: &ControlConsensusRuntime) {
        let now_ms = unix_timestamp_millis();
        let mut state = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.control_state.leader_node_id = Some("node-b".to_string());
        state.last_leader_contact_unix_ms = now_ms;
        state.persistence_fence = Some("diagnostic fence detail".repeat(256));
        state.checkpoint_pending = Some(ControlCheckpointPending {
            position: ControlCommitPosition { index: 17, term: 4 },
            detail: "diagnostic checkpoint detail".repeat(256),
        });
        state.cleanup_debt = Some("diagnostic cleanup detail".repeat(256));
        state.control_state.transitions = vec![
            ShardOwnershipTransition {
                shard: 0,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version: 2,
                handoff: ShardHandoffProgress {
                    phase: ShardHandoffPhase::Warmup,
                    copied_rows: 41,
                    pending_rows: 37,
                    resumed_count: 1,
                    started_unix_ms: now_ms.saturating_sub(10),
                    updated_unix_ms: now_ms,
                    last_error: Some("diagnostic handoff detail".repeat(256)),
                },
            },
            ShardOwnershipTransition {
                shard: 1,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version: 2,
                handoff: ShardHandoffProgress {
                    phase: ShardHandoffPhase::Completed,
                    copied_rows: 73,
                    pending_rows: 0,
                    resumed_count: 0,
                    started_unix_ms: now_ms.saturating_sub(20),
                    updated_unix_ms: now_ms.saturating_sub(1),
                    last_error: Some("completed diagnostic handoff detail".repeat(256)),
                },
            },
        ];
        state.peer_heartbeat.insert(
            "node-b".to_string(),
            PeerHeartbeatState {
                last_success_unix_ms: Some(now_ms),
                last_failure_unix_ms: None,
                consecutive_failures: 0,
            },
        );
    }

    fn set_in_memory_node_status(
        runtime: &ControlConsensusRuntime,
        node_id: &str,
        status: ControlNodeStatus,
    ) {
        let mut state = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .control_state
            .nodes
            .iter_mut()
            .find(|node| node.id == node_id)
            .unwrap_or_else(|| panic!("node '{node_id}' should exist in the test membership"))
            .status = status;
    }

    fn test_rpc_client(local_node_id: &str) -> RpcClient {
        RpcClient::new(crate::cluster::rpc::RpcClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 0,
            protocol_version: crate::cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "test-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: local_node_id.to_string(),
            compatibility: crate::cluster::rpc::CompatibilityProfile::default(),
            internal_mtls: None,
        })
    }

    fn single_node_membership_and_state() -> (MembershipView, ControlState) {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            shards: 16,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&config).expect("membership should build");
        let ring = ShardRing::build(16, 1, &membership).expect("ring should build");
        (
            membership.clone(),
            ControlState::from_runtime(&membership, &ring),
        )
    }

    #[test]
    fn append_entries_commit_after_heartbeat() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");

        let runtime = ControlConsensusRuntime::open(
            membership,
            Arc::clone(&state_store),
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig {
                snapshot_interval_entries: 64,
                ..ControlConsensusConfig::default()
            },
        )
        .expect("runtime should open");

        let append = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![InternalControlLogEntry {
                index: 1,
                term: 2,
                command: InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
                created_unix_ms: 1,
            }],
            leader_commit: 0,
        };
        let response = runtime
            .handle_append_request(append)
            .expect("append should succeed");
        assert!(response.success);
        assert_eq!(response.match_index, 1);
        assert_eq!(runtime.current_state().applied_log_index, 0);

        let heartbeat = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: Vec::new(),
            leader_commit: 1,
        };
        let heartbeat_response = runtime
            .handle_append_request(heartbeat)
            .expect("heartbeat should succeed");
        assert!(heartbeat_response.success);

        let state = runtime.current_state();
        assert_eq!(state.applied_log_index, 1);
        assert_eq!(state.applied_log_term, 2);
        assert_eq!(state.leader_node_id.as_deref(), Some("node-a"));
    }

    #[test]
    fn membership_mutation_proposals_require_local_control_leader() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "leader-check",
            64,
        );

        let mut state = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.control_state.leader_node_id = Some("node-b".to_string());
        let err = runtime
            .validate_local_proposal_locked(
                &state,
                &InternalControlCommand::JoinNode {
                    node_id: "node-c".to_string(),
                    endpoint: "127.0.0.1:9303".to_string(),
                },
                unix_timestamp_millis(),
            )
            .expect_err("non-leader membership mutation should be rejected");
        assert!(err.contains("not the active control leader"));
        let handoff_err = runtime
            .validate_local_proposal_locked(
                &state,
                &InternalControlCommand::BeginShardHandoff {
                    shard: 0,
                    from_node_id: "node-a".to_string(),
                    to_node_id: "node-b".to_string(),
                    activation_ring_version: 2,
                },
                unix_timestamp_millis(),
            )
            .expect_err("non-leader handoff mutation should be rejected");
        assert!(handoff_err.contains("not the active control leader"));

        state.control_state.leader_node_id = Some("node-a".to_string());
        runtime
            .validate_local_proposal_locked(
                &state,
                &InternalControlCommand::JoinNode {
                    node_id: "node-c".to_string(),
                    endpoint: "127.0.0.1:9303".to_string(),
                },
                unix_timestamp_millis(),
            )
            .expect("leader membership mutation proposal should pass");
        runtime
            .validate_local_proposal_locked(
                &state,
                &InternalControlCommand::BeginShardHandoff {
                    shard: 0,
                    from_node_id: "node-a".to_string(),
                    to_node_id: "node-b".to_string(),
                    activation_ring_version: 2,
                },
                unix_timestamp_millis(),
            )
            .expect("leader handoff mutation proposal should pass");
        let self_leave_err = runtime
            .validate_local_proposal_locked(
                &state,
                &InternalControlCommand::LeaveNode {
                    node_id: "node-a".to_string(),
                },
                unix_timestamp_millis(),
            )
            .expect_err("an active leader must transfer leadership before leaving itself");
        assert!(self_leave_err.contains("cannot leave itself"));
    }

    #[test]
    fn append_handoff_commands_apply_state_transitions() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let initial_ring_version = bootstrap_state.ring_version;
        let activation_ring_version = initial_ring_version.saturating_add(1);
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let begin = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::BeginShardHandoff {
                        shard: 0,
                        from_node_id: "node-a".to_string(),
                        to_node_id: "node-b".to_string(),
                        activation_ring_version,
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 1,
            })
            .expect("begin handoff append should succeed");
        assert!(begin.success);
        let after_begin = runtime.current_state();
        assert_eq!(after_begin.ring_version, initial_ring_version);
        let transition = after_begin
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("shard transition should exist after begin");
        assert_eq!(transition.handoff.phase.as_str(), "warmup");
        assert_eq!(transition.handoff.copied_rows, 0);
        assert_eq!(transition.handoff.pending_rows, 0);

        let progress = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 3,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: vec![InternalControlLogEntry {
                    index: 2,
                    term: 3,
                    command: InternalControlCommand::UpdateShardHandoff {
                        shard: 0,
                        phase: crate::cluster::control::ShardHandoffPhase::Cutover,
                        copied_rows: Some(125),
                        pending_rows: Some(24),
                        last_error: None,
                    },
                    created_unix_ms: 2,
                }],
                leader_commit: 2,
            })
            .expect("handoff progress append should succeed");
        assert!(progress.success);
        let after_progress = runtime.current_state();
        assert_eq!(after_progress.ring_version, activation_ring_version);
        let transition = after_progress
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("shard transition should exist after progress");
        assert_eq!(transition.handoff.phase.as_str(), "cutover");
        assert_eq!(transition.handoff.copied_rows, 125);
        assert_eq!(transition.handoff.pending_rows, 24);

        let final_sync = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 4,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 2,
                prev_log_term: 3,
                entries: vec![InternalControlLogEntry {
                    index: 3,
                    term: 4,
                    command: InternalControlCommand::UpdateShardHandoff {
                        shard: 0,
                        phase: crate::cluster::control::ShardHandoffPhase::FinalSync,
                        copied_rows: Some(150),
                        pending_rows: Some(3),
                        last_error: None,
                    },
                    created_unix_ms: 3,
                }],
                leader_commit: 3,
            })
            .expect("handoff final-sync append should succeed");
        assert!(final_sync.success);
        let after_final_sync = runtime.current_state();
        let transition = after_final_sync
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("shard transition should exist after final-sync");
        assert_eq!(transition.handoff.phase.as_str(), "final_sync");
        assert_eq!(transition.handoff.copied_rows, 150);
        assert_eq!(transition.handoff.pending_rows, 3);

        let complete = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 5,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 3,
                prev_log_term: 4,
                entries: vec![InternalControlLogEntry {
                    index: 4,
                    term: 5,
                    command: InternalControlCommand::CompleteShardHandoff { shard: 0 },
                    created_unix_ms: 4,
                }],
                leader_commit: 4,
            })
            .expect("handoff completion append should succeed");
        assert!(complete.success);
        let after_complete = runtime.current_state();
        let transition = after_complete
            .transitions
            .iter()
            .find(|transition| transition.shard == 0)
            .expect("shard transition should exist after completion");
        assert_eq!(transition.handoff.phase.as_str(), "completed");
        assert_eq!(transition.handoff.copied_rows, 150);
        assert_eq!(transition.handoff.pending_rows, 0);
    }

    #[test]
    fn append_membership_commands_apply_idempotent_state_transitions() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let join = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::JoinNode {
                        node_id: "node-c".to_string(),
                        endpoint: "127.0.0.1:9303".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 1,
            })
            .expect("join append should succeed");
        assert!(join.success);
        let joined_state = runtime.current_state();
        assert_eq!(joined_state.membership_epoch, 2);
        assert_eq!(
            joined_state
                .node_record("node-c")
                .map(|node| node.status.as_str()),
            Some("joining")
        );

        let join_noop = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 3,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: vec![InternalControlLogEntry {
                    index: 2,
                    term: 3,
                    command: InternalControlCommand::JoinNode {
                        node_id: "node-c".to_string(),
                        endpoint: "127.0.0.1:9303".to_string(),
                    },
                    created_unix_ms: 2,
                }],
                leader_commit: 2,
            })
            .expect("idempotent join append should succeed");
        assert!(join_noop.success);
        let join_noop_state = runtime.current_state();
        assert_eq!(join_noop_state.membership_epoch, 2);

        let leave = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 4,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 2,
                prev_log_term: 3,
                entries: vec![InternalControlLogEntry {
                    index: 3,
                    term: 4,
                    command: InternalControlCommand::LeaveNode {
                        node_id: "node-c".to_string(),
                    },
                    created_unix_ms: 3,
                }],
                leader_commit: 3,
            })
            .expect("leave append should succeed");
        assert!(leave.success);
        let leaving_state = runtime.current_state();
        assert_eq!(leaving_state.membership_epoch, 3);
        assert_eq!(
            leaving_state
                .node_record("node-c")
                .map(|node| node.status.as_str()),
            Some("leaving")
        );

        let recommission = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 5,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 3,
                prev_log_term: 4,
                entries: vec![InternalControlLogEntry {
                    index: 4,
                    term: 5,
                    command: InternalControlCommand::RecommissionNode {
                        node_id: "node-c".to_string(),
                        endpoint: None,
                    },
                    created_unix_ms: 4,
                }],
                leader_commit: 4,
            })
            .expect("recommission append should succeed");
        assert!(recommission.success);
        let recommissioned_state = runtime.current_state();
        assert_eq!(recommissioned_state.membership_epoch, 4);
        assert_eq!(
            recommissioned_state
                .node_record("node-c")
                .map(|node| node.status.as_str()),
            Some("active")
        );
    }

    #[test]
    fn removed_recommission_target_enters_postcommit_fanout_without_proposal_entry() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "removed-recommission",
            64,
        );
        force_local_leader(&runtime);
        set_in_memory_node_status(&runtime, "node-b", ControlNodeStatus::Removed);
        let mut candidate = {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime.reconcile_dynamic_peers_locked(&mut state);
            assert_eq!(
                runtime.control_voter_node_ids_locked(&state),
                vec!["node-a".to_string()]
            );
            assert!(
                runtime.control_peer_nodes_locked(&state).is_empty(),
                "a Removed target is omitted from the proposal fan-out"
            );
            state.clone()
        };

        let (entry, _, _, _) = runtime
            .prepare_proposal_locked(
                &mut candidate,
                InternalControlCommand::RecommissionNode {
                    node_id: "node-b".to_string(),
                    endpoint: None,
                },
            )
            .expect("recommission proposal should prepare");
        candidate.commit_index = entry.index;
        runtime
            .apply_committed_entries_in_memory_locked(&mut candidate)
            .expect("old single-node quorum should apply recommission");
        assert_eq!(
            candidate
                .control_state
                .node_record("node-b")
                .map(|node| node.status),
            Some(ControlNodeStatus::Active),
            "the old single-node quorum activates the target"
        );
        assert_eq!(
            runtime.control_peer_nodes_locked(&candidate),
            vec![("node-b".to_string(), "127.0.0.1:9302".to_string())],
            "the activated target joins the post-commit fan-out"
        );

        let observed = match runtime
            .build_peer_plan_locked(&candidate, "node-b")
            .expect("post-commit target plan should build")
        {
            PeerPlan::Append(request) => request,
            PeerPlan::InstallSnapshot(_) => {
                panic!("optimistic post-commit progress should produce an append notice")
            }
        };
        assert!(
            observed.entries.is_empty(),
            "the Removed target must not receive the recommission proposal entry in the initial fan-out"
        );
        assert_eq!(observed.prev_log_index, entry.index);
        assert_eq!(observed.leader_commit, entry.index);
    }

    #[test]
    fn append_rejects_unknown_leader_node() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let response = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 1,
                leader_node_id: "unknown-node".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .expect("append should return");
        assert!(!response.success);
        assert_eq!(response.message.as_deref(), Some("unknown_leader_node"));
    }

    #[test]
    fn append_and_snapshot_from_non_active_members_cannot_advance_term() {
        for status in [ControlNodeStatus::Joining, ControlNodeStatus::Leaving] {
            let temp_dir = TempDir::new().expect("temp dir should create");
            let runtime = open_runtime_for_node(
                &temp_dir,
                "node-a",
                "127.0.0.1:9301",
                &["node-b@127.0.0.1:9302"],
                status.as_str(),
                64,
            );
            set_in_memory_node_status(&runtime, "node-b", status);
            let before = runtime.log_recovery_snapshot();

            let append = runtime
                .handle_append_request(InternalControlAppendRequest {
                    term: before.current_term.checked_add(10).unwrap(),
                    leader_node_id: "node-b".to_string(),
                    prev_log_index: before.snapshot_last_index,
                    prev_log_term: before.snapshot_last_term,
                    entries: Vec::new(),
                    leader_commit: before.commit_index,
                })
                .expect("non-active append should return a protocol rejection");
            assert!(!append.success, "{status:?} leader append was accepted");
            assert_eq!(append.message.as_deref(), Some("leader_not_active_voter"));
            assert_eq!(append.term, before.current_term);

            let snapshot = runtime
                .handle_install_snapshot_request(InternalControlInstallSnapshotRequest {
                    term: before.current_term.checked_add(11).unwrap(),
                    leader_node_id: "node-b".to_string(),
                    snapshot_last_index: 1,
                    snapshot_last_term: 1,
                    state: serde_json::to_value(runtime.current_state())
                        .expect("snapshot fixture should encode"),
                })
                .expect("non-active snapshot should return a protocol rejection");
            assert!(!snapshot.success, "{status:?} leader snapshot was accepted");
            assert_eq!(snapshot.message.as_deref(), Some("leader_not_active_voter"));
            assert_eq!(snapshot.term, before.current_term);

            let after = runtime.log_recovery_snapshot();
            assert_eq!(after.current_term, before.current_term);
            assert_eq!(after.stepped_down_term, before.stepped_down_term);
            assert_eq!(after.entries, before.entries);
        }
    }

    #[test]
    fn divergent_membership_view_rejects_proofless_newly_active_leader_repair() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let new_leader = open_runtime_for_node(
            &temp_dir,
            "node-b",
            "127.0.0.1:9302",
            &["node-a@127.0.0.1:9301", "node-c@127.0.0.1:9303"],
            "newly-active-leader",
            64,
        );
        let lagging_voter = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302", "node-c@127.0.0.1:9303"],
            "lagging-voter",
            64,
        );
        force_local_leader(&new_leader);
        set_in_memory_node_status(&lagging_voter, "node-b", ControlNodeStatus::Joining);

        let before = lagging_voter.log_recovery_snapshot();
        {
            let mut state = new_leader
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.current_term = before.current_term.saturating_add(10);
            assert!(new_leader.local_is_control_leader_locked(&state));
            assert!(new_leader.is_active_membership_node_locked(&state, "node-b"));
        }
        assert_eq!(
            lagging_voter
                .current_state()
                .node_record("node-b")
                .map(|node| node.status),
            Some(ControlNodeStatus::Joining),
            "the receiver fixture must retain its pre-activation membership view"
        );

        let append_request = {
            let state = new_leader
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match new_leader
                .build_peer_plan_locked(&state, "node-a")
                .expect("new leader append plan should build")
            {
                PeerPlan::Append(request) => request,
                PeerPlan::InstallSnapshot(_) => {
                    panic!("a fresh peer should receive an append plan")
                }
            }
        };
        let append = lagging_voter
            .handle_append_request(append_request)
            .expect("proof-less append should return a protocol rejection");
        assert!(!append.success);
        assert_eq!(append.message.as_deref(), Some("leader_not_active_voter"));
        assert_eq!(append.term, before.current_term);

        let snapshot = lagging_voter
            .handle_install_snapshot_request(InternalControlInstallSnapshotRequest {
                term: before.current_term.saturating_add(10),
                leader_node_id: "node-b".to_string(),
                snapshot_last_index: 1,
                snapshot_last_term: before.current_term.saturating_add(10),
                state: serde_json::to_value(new_leader.current_state())
                    .expect("new leader snapshot fixture should encode"),
            })
            .expect("proof-less snapshot should return a protocol rejection");
        assert!(!snapshot.success);
        assert_eq!(snapshot.message.as_deref(), Some("leader_not_active_voter"));
        assert_eq!(snapshot.term, before.current_term);

        let after = lagging_voter.log_recovery_snapshot();
        assert_eq!(after.current_term, before.current_term);
        assert_eq!(after.stepped_down_term, before.stepped_down_term);
        assert_eq!(after.commit_index, before.commit_index);
        assert_eq!(after.entries, before.entries);
    }

    #[test]
    fn append_rejects_conflicting_leader_for_same_term() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let first = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 1,
            })
            .expect("append should succeed");
        assert!(first.success);

        let conflicting = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-b".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: Vec::new(),
                leader_commit: 1,
            })
            .expect("append should return");
        assert!(!conflicting.success);
        assert_eq!(
            conflicting.message.as_deref(),
            Some("conflicting_leader_same_term")
        );
    }

    #[test]
    fn higher_term_append_allows_leader_failover() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let set_node_a = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 1,
            })
            .expect("append should succeed");
        assert!(set_node_a.success);
        assert_eq!(
            runtime.current_state().leader_node_id.as_deref(),
            Some("node-a")
        );

        let takeover_append = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 3,
                leader_node_id: "node-b".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: vec![InternalControlLogEntry {
                    index: 2,
                    term: 3,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-b".to_string(),
                    },
                    created_unix_ms: 2,
                }],
                leader_commit: 2,
            })
            .expect("takeover append should succeed");
        assert!(takeover_append.success);
        assert_eq!(
            runtime.current_state().leader_node_id.as_deref(),
            Some("node-b")
        );

        let old_leader = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 3,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 2,
                prev_log_term: 3,
                entries: Vec::new(),
                leader_commit: 2,
            })
            .expect("old leader heartbeat should return");
        assert!(!old_leader.success);
        assert_eq!(
            old_leader.message.as_deref(),
            Some("conflicting_leader_same_term")
        );
    }

    #[test]
    fn stale_leader_failover_candidate_is_deterministic() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let node_b_runtime = open_runtime_for_node(
            &temp_dir,
            "node-b",
            "127.0.0.1:9302",
            &["node-a@127.0.0.1:9301", "node-c@127.0.0.1:9303"],
            "node-b",
            64,
        );
        let node_c_runtime = open_runtime_for_node(
            &temp_dir,
            "node-c",
            "127.0.0.1:9303",
            &["node-a@127.0.0.1:9301", "node-b@127.0.0.1:9302"],
            "node-c",
            64,
        );

        let now_ms = unix_timestamp_millis();
        {
            let mut state = node_b_runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.control_state.leader_node_id = Some("node-a".to_string());
            state.last_leader_contact_unix_ms = 0;
            assert!(node_b_runtime.can_local_node_propose_locked(&state, now_ms));
        }

        {
            let mut state = node_c_runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.control_state.leader_node_id = Some("node-a".to_string());
            state.last_leader_contact_unix_ms = 0;
            assert!(!node_c_runtime.can_local_node_propose_locked(&state, now_ms));
        }
    }

    #[test]
    fn inactive_local_member_is_neither_control_leader_nor_voter() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "inactive-local",
            64,
        );
        force_local_leader(&runtime);
        set_in_memory_node_status(&runtime, "node-a", ControlNodeStatus::Leaving);

        let state = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            state.control_state.leader_node_id.as_deref(),
            Some("node-a"),
            "the regression fixture must retain the recorded local leader"
        );
        assert!(!runtime.local_is_control_leader_locked(&state));
        let voters = runtime.control_voter_node_ids_locked(&state);
        assert_eq!(voters, vec!["node-b".to_string()]);
        assert!(!voters.iter().any(|node_id| node_id == "node-a"));
        assert!(!runtime.can_local_node_propose_locked(&state, u64::MAX));
        drop(state);
        assert!(!runtime.is_local_control_leader());
    }

    #[test]
    fn failover_with_recorded_non_voter_leader_selects_first_active_voter() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-b",
            "127.0.0.1:9302",
            &["node-a@127.0.0.1:9301", "node-c@127.0.0.1:9303"],
            "non-voter-leader",
            64,
        );
        set_in_memory_node_status(&runtime, "node-a", ControlNodeStatus::Leaving);

        let mut state = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.control_state.leader_node_id = Some("node-a".to_string());
        state.last_leader_contact_unix_ms = 0;
        let voters = runtime.control_voter_node_ids_locked(&state);
        assert_eq!(voters, vec!["node-b".to_string(), "node-c".to_string()]);
        assert_eq!(
            runtime.next_failover_candidate_id_locked(&state, Some("node-a")),
            Some("node-b".to_string())
        );
        assert!(runtime.can_local_node_propose_locked(&state, u64::MAX));
    }

    #[test]
    fn stale_leader_failover_matrix_selects_single_non_leader_candidate() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let node_a_runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302", "node-c@127.0.0.1:9303"],
            "node-a",
            64,
        );
        let node_b_runtime = open_runtime_for_node(
            &temp_dir,
            "node-b",
            "127.0.0.1:9302",
            &["node-a@127.0.0.1:9301", "node-c@127.0.0.1:9303"],
            "node-b",
            64,
        );
        let node_c_runtime = open_runtime_for_node(
            &temp_dir,
            "node-c",
            "127.0.0.1:9303",
            &["node-a@127.0.0.1:9301", "node-b@127.0.0.1:9302"],
            "node-c",
            64,
        );
        let runtimes = [
            ("node-a", &node_a_runtime),
            ("node-b", &node_b_runtime),
            ("node-c", &node_c_runtime),
        ];
        let cases = [
            ("node-a", "node-b"),
            ("node-b", "node-c"),
            ("node-c", "node-a"),
        ];
        let now_ms = unix_timestamp_millis();

        for (stale_leader, expected_candidate) in cases {
            let mut eligible_non_leaders = Vec::new();
            for (node_id, runtime) in runtimes {
                let mut state = runtime
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.control_state.leader_node_id = Some(stale_leader.to_string());
                state.last_leader_contact_unix_ms = 0;
                if node_id != stale_leader && runtime.can_local_node_propose_locked(&state, now_ms)
                {
                    eligible_non_leaders.push(node_id.to_string());
                }
            }
            assert_eq!(
                eligible_non_leaders,
                vec![expected_candidate.to_string()],
                "expected exactly one failover candidate when stale leader is {stale_leader}"
            );
        }
    }

    #[test]
    fn partition_failover_rejects_delayed_old_leader_and_converges_to_single_leader() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let node_a_runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302", "node-c@127.0.0.1:9303"],
            "node-a",
            64,
        );
        let node_b_runtime = open_runtime_for_node(
            &temp_dir,
            "node-b",
            "127.0.0.1:9302",
            &["node-a@127.0.0.1:9301", "node-c@127.0.0.1:9303"],
            "node-b",
            64,
        );
        let node_c_runtime = open_runtime_for_node(
            &temp_dir,
            "node-c",
            "127.0.0.1:9303",
            &["node-a@127.0.0.1:9301", "node-b@127.0.0.1:9302"],
            "node-c",
            64,
        );

        let establish_node_a = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![InternalControlLogEntry {
                index: 1,
                term: 2,
                command: InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
                created_unix_ms: 1,
            }],
            leader_commit: 1,
        };
        for runtime in [&node_a_runtime, &node_b_runtime, &node_c_runtime] {
            let response = runtime
                .handle_append_request(establish_node_a.clone())
                .expect("leader establish append should succeed");
            assert!(response.success);
            assert_eq!(
                runtime.current_state().leader_node_id.as_deref(),
                Some("node-a")
            );
        }

        let failover_to_node_b = InternalControlAppendRequest {
            term: 3,
            leader_node_id: "node-b".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![InternalControlLogEntry {
                index: 2,
                term: 3,
                command: InternalControlCommand::SetLeader {
                    leader_node_id: "node-b".to_string(),
                },
                created_unix_ms: 2,
            }],
            leader_commit: 2,
        };
        for runtime in [&node_b_runtime, &node_c_runtime] {
            let response = runtime
                .handle_append_request(failover_to_node_b.clone())
                .expect("failover append should succeed on majority partition");
            assert!(response.success);
            assert_eq!(
                runtime.current_state().leader_node_id.as_deref(),
                Some("node-b")
            );
        }

        let delayed_old_leader = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: Vec::new(),
            leader_commit: 1,
        };
        for runtime in [&node_b_runtime, &node_c_runtime] {
            let response = runtime
                .handle_append_request(delayed_old_leader.clone())
                .expect("delayed heartbeat should return");
            assert!(!response.success);
            assert_eq!(response.message.as_deref(), Some("stale_term"));
        }

        let catch_up_response = node_a_runtime
            .handle_append_request(failover_to_node_b)
            .expect("recovery append to old leader should succeed");
        assert!(catch_up_response.success);

        for (node_id, runtime) in [
            ("node-a", &node_a_runtime),
            ("node-b", &node_b_runtime),
            ("node-c", &node_c_runtime),
        ] {
            let state = runtime.current_state();
            assert_eq!(
                state.leader_node_id.as_deref(),
                Some("node-b"),
                "node {node_id} did not converge to the surviving leader"
            );
            assert_eq!(
                state.applied_log_index, 2,
                "node {node_id} has stale log index"
            );
            assert_eq!(
                state.applied_log_term, 3,
                "node {node_id} has stale log term"
            );
        }
    }

    #[tokio::test]
    async fn minority_partition_set_leader_proposal_stays_pending_and_preserves_leader() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-c",
            "127.0.0.1:9393",
            &["node-a@127.0.0.1:9391", "node-b@127.0.0.1:9392"],
            "node-c",
            64,
        );
        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.control_state.leader_node_id = Some("node-b".to_string());
            state.last_leader_contact_unix_ms = 0;
        }
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: derive_shared_internal_token(&MembershipView {
                local_node_id: runtime.local_node_id.clone(),
                nodes: runtime
                    .current_state()
                    .nodes
                    .iter()
                    .map(|node| ClusterNode {
                        id: node.id.clone(),
                        endpoint: node.endpoint.clone(),
                    })
                    .collect(),
            }),
            internal_auth_runtime: None,
            local_node_id: "node-c".to_string(),
            compatibility: crate::cluster::rpc::CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let outcome = runtime
            .propose_command(
                &rpc_client,
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-c".to_string(),
                },
            )
            .await
            .expect("minority proposal should return pending");
        assert!(matches!(
            outcome,
            ProposeOutcome::Pending {
                required: 2,
                acknowledged: 1
            }
        ));
        assert!(!runtime.is_local_control_leader());
        assert_eq!(
            runtime.current_state().leader_node_id.as_deref(),
            Some("node-b")
        );
    }

    #[tokio::test]
    async fn successful_non_active_peer_does_not_count_for_quorum_or_raise_observed_term() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("joining-peer listener should bind");
        let joining_endpoint = listener
            .local_addr()
            .expect("joining-peer listener should have an address")
            .to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("proposal RPC expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("proposal request should parse");
            assert_eq!(request.path_without_query(), "/internal/v1/control/append");
            let append: InternalControlAppendRequest =
                serde_json::from_slice(&request.body).expect("proposal body should decode");
            assert_eq!(append.entries.len(), 1);
            let response = InternalControlAppendResponse {
                term: append.term.checked_add(100).unwrap(),
                success: true,
                match_index: append.entries[0].index,
                message: None,
            };
            write_http_response(
                &mut stream,
                &HttpResponse::new(
                    200,
                    serde_json::to_vec(&response).expect("response should encode"),
                )
                .with_header("Content-Type", "application/json"),
            )
            .await
            .expect("proposal response should write");
        });

        let temp_dir = TempDir::new().expect("temp dir should create");
        let joining_seed = format!("node-c@{joining_endpoint}");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:1", joining_seed.as_str()],
            "non-active-quorum",
            64,
        );
        force_local_leader(&runtime);
        set_in_memory_node_status(&runtime, "node-c", ControlNodeStatus::Joining);
        let before = runtime.log_recovery_snapshot();

        let outcome = runtime
            .propose_command(
                &test_rpc_client("node-a"),
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .await
            .expect("proposal should remain pending without an active remote vote");
        assert!(matches!(
            outcome,
            ProposeOutcome::Pending {
                required: 2,
                acknowledged: 1
            }
        ));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("joining-peer server should finish")
            .expect("joining-peer server task should succeed");

        let after = runtime.log_recovery_snapshot();
        assert_eq!(
            after.current_term,
            before.current_term.checked_add(1).unwrap()
        );
        assert!(after.current_term < before.current_term.checked_add(100).unwrap());
        assert_eq!(after.stepped_down_term, before.stepped_down_term);
        assert_eq!(after.commit_index, before.commit_index);
    }

    #[tokio::test]
    async fn higher_term_commit_notice_response_durably_revokes_local_leadership() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("active-peer listener should bind");
        let active_endpoint = listener
            .local_addr()
            .expect("active-peer listener should have an address")
            .to_string();
        let server = tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("control RPC expected");
                let mut read_buffer = Vec::new();
                let request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("control request should parse");
                assert_eq!(request.path_without_query(), "/internal/v1/control/append");
                let append: InternalControlAppendRequest =
                    serde_json::from_slice(&request.body).expect("control body should decode");
                let (term, success, match_index, message) = if request_number == 0 {
                    assert_eq!(append.entries.len(), 1);
                    (append.term, true, append.entries[0].index, None)
                } else {
                    assert!(append.entries.is_empty());
                    assert!(append.leader_commit >= 1);
                    (
                        append.term.checked_add(1).unwrap(),
                        false,
                        append.prev_log_index,
                        Some("stale_term".to_string()),
                    )
                };
                let response = InternalControlAppendResponse {
                    term,
                    success,
                    match_index,
                    message,
                };
                write_http_response(
                    &mut stream,
                    &HttpResponse::new(
                        200,
                        serde_json::to_vec(&response).expect("response should encode"),
                    )
                    .with_header("Content-Type", "application/json"),
                )
                .await
                .expect("control response should write");
            }
        });

        let temp_dir = TempDir::new().expect("temp dir should create");
        let active_seed = format!("node-b@{active_endpoint}");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &[active_seed.as_str()],
            "commit-notice-term",
            64,
        );
        force_local_leader(&runtime);
        let outcome = runtime
            .propose_command(
                &test_rpc_client("node-a"),
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .await
            .expect("quorum proposal should commit before the higher-term notice response");
        let proposal_term = match outcome {
            ProposeOutcome::Committed { term, .. } => term,
            other => panic!("expected committed proposal, got {other:?}"),
        };
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("active-peer server should receive proposal and commit notice")
            .expect("active-peer server task should succeed");

        let after = runtime.log_recovery_snapshot();
        assert_eq!(after.current_term, proposal_term.checked_add(1).unwrap());
        assert_eq!(after.stepped_down_term, after.current_term);
        assert!(!runtime.is_local_control_leader());
        let persisted = load_log_file(runtime.log_path()).expect("durable log should load");
        assert_eq!(persisted.current_term, after.current_term);
        assert_eq!(persisted.stepped_down_term, Some(after.stepped_down_term));
    }

    #[tokio::test]
    async fn committed_proposal_remains_success_when_higher_term_persistence_is_pending() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let pressure_path = temp_dir.path().join("external-pressure.bin");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(1024 * 1024),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("active-peer listener should bind");
        let active_endpoint = listener
            .local_addr()
            .expect("active-peer listener should have an address")
            .to_string();
        let server_budget = Arc::clone(&budget);
        let server_pressure_path = pressure_path.clone();
        let server = tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("control RPC expected");
                let mut read_buffer = Vec::new();
                let request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("control request should parse");
                let append: InternalControlAppendRequest =
                    serde_json::from_slice(&request.body).expect("control body should decode");
                let response = if request_number == 0 {
                    InternalControlAppendResponse {
                        term: append.term,
                        success: true,
                        match_index: append.entries[0].index,
                        message: None,
                    }
                } else {
                    assert!(append.entries.is_empty());
                    std::fs::write(&server_pressure_path, vec![0u8; 2 * 1024 * 1024])
                        .expect("external pressure fixture should write");
                    let snapshot = server_budget
                        .reconcile()
                        .expect("external pressure should reconcile");
                    assert!(snapshot.over_limit);
                    InternalControlAppendResponse {
                        term: append.term.checked_add(1).unwrap(),
                        success: false,
                        match_index: append.prev_log_index,
                        message: Some("stale_term".to_string()),
                    }
                };
                write_http_response(
                    &mut stream,
                    &HttpResponse::new(
                        200,
                        serde_json::to_vec(&response).expect("response should encode"),
                    )
                    .with_header("Content-Type", "application/json"),
                )
                .await
                .expect("control response should write");
            }
        });

        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{active_endpoint}")],
            shards: 16,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&config).expect("membership should build");
        let ring = ShardRing::build(16, 2, &membership).expect("ring should build");
        let bootstrap_state = ControlState::from_runtime(&membership, &ring);
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let state_store = Arc::new(
            ControlStateStore::open_with_disk_budget(state_path, Some(Arc::clone(&budget)))
                .expect("budgeted state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");
        force_local_leader(&runtime);

        let outcome = runtime
            .propose_command(
                &test_rpc_client("node-a"),
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .await
            .expect("a quorum-committed proposal must remain a success");
        let (proposal_term, detail) = match outcome {
            ProposeOutcome::CommittedPersistencePending { term, detail, .. } => (term, detail),
            other => panic!("expected committed persistence debt, got {other:?}"),
        };
        assert!(detail.contains("higher response term"));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("active-peer server should finish")
            .expect("active-peer server task should succeed");

        assert_eq!(runtime.current_state().applied_log_index, 1);
        assert!(runtime.persistence_status().fenced);
        assert!(!runtime.is_local_control_leader());
        let in_memory = runtime.log_recovery_snapshot();
        assert_eq!(
            in_memory.current_term,
            proposal_term.checked_add(1).unwrap()
        );
        assert_eq!(in_memory.stepped_down_term, in_memory.current_term);
        let before_repair = load_log_file(&log_path).expect("committed log should remain readable");
        assert_eq!(before_repair.commit_index, 1);
        assert_eq!(before_repair.current_term, proposal_term);

        std::fs::remove_file(&pressure_path).expect("pressure fixture should remove");
        budget
            .reconcile()
            .expect("released pressure should reconcile");
        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime
                .repair_persistence_fence_locked(&mut state)
                .expect("pending higher term should repair after pressure clears");
        }
        let repaired = load_log_file(&log_path).expect("repaired log should load");
        assert_eq!(repaired.commit_index, 1);
        assert_eq!(repaired.current_term, in_memory.current_term);
        assert_eq!(
            repaired.stepped_down_term,
            Some(in_memory.stepped_down_term)
        );
        assert!(!runtime.persistence_status().fenced);
        assert!(!runtime.is_local_control_leader());
    }

    #[test]
    fn peer_without_success_transitions_from_suspect_to_dead() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        let now_ms = unix_timestamp_millis();
        let dead_ms = runtime.config.dead_timeout().as_millis() as u64;

        let mut heartbeat = PeerHeartbeatState {
            last_success_unix_ms: None,
            last_failure_unix_ms: Some(now_ms.saturating_sub(dead_ms.saturating_sub(1))),
            consecutive_failures: 1,
        };
        assert_eq!(
            runtime.peer_liveness_status(&heartbeat, now_ms),
            ControlPeerLivenessStatus::Suspect
        );

        heartbeat.last_failure_unix_ms = Some(now_ms.saturating_sub(dead_ms));
        assert_eq!(
            runtime.peer_liveness_status(&heartbeat, now_ms),
            ControlPeerLivenessStatus::Dead
        );
    }

    #[test]
    fn peer_liveness_recovers_to_healthy_after_successful_heartbeat() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        let now_ms = unix_timestamp_millis();
        let heartbeat = PeerHeartbeatState {
            last_success_unix_ms: Some(now_ms),
            last_failure_unix_ms: Some(now_ms),
            consecutive_failures: 1,
        };
        assert_eq!(
            runtime.peer_liveness_status(&heartbeat, now_ms),
            ControlPeerLivenessStatus::Suspect
        );

        let recovered = PeerHeartbeatState {
            last_success_unix_ms: Some(now_ms),
            last_failure_unix_ms: Some(now_ms),
            consecutive_failures: 0,
        };
        assert_eq!(
            runtime.peer_liveness_status(&recovered, now_ms),
            ControlPeerLivenessStatus::Healthy
        );
    }

    #[test]
    fn leader_liveness_snapshot_clamps_future_contact_age_under_clock_skew() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        let future_contact_ms = unix_timestamp_millis().saturating_add(
            (runtime.config.leader_lease_timeout().as_millis() as u64).saturating_mul(4),
        );
        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.control_state.leader_node_id = Some("node-b".to_string());
            state.last_leader_contact_unix_ms = future_contact_ms;
        }

        let snapshot = runtime.liveness_snapshot();
        assert_eq!(snapshot.leader_node_id.as_deref(), Some("node-b"));
        assert_eq!(
            snapshot.leader_last_contact_unix_ms,
            Some(future_contact_ms)
        );
        assert_eq!(snapshot.leader_contact_age_ms, Some(0));
        assert!(!snapshot.leader_stale);
    }

    #[test]
    fn control_metrics_projection_reserves_exact_output_before_materialization() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        configure_metrics_projection_fixture(&runtime);

        let calibration_budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = runtime
            .metrics_snapshot_with_execution(&calibration)
            .expect("calibration projection should succeed");
        let exact_bytes = calibrated.accounted_bytes();
        assert!(exact_bytes > 0);
        assert_eq!(
            calibration.snapshot().memory_reserved_bytes,
            exact_bytes,
            "the accounted wrapper must retain the entire projection reservation"
        );
        assert_eq!(calibrated.liveness.local_node_id, "node-a");
        assert_eq!(
            calibrated.liveness.leader_node_id.as_deref(),
            Some("node-b")
        );
        assert_eq!(
            calibrated.persistence.pending_checkpoint,
            Some(ControlCommitPosition { index: 17, term: 4 })
        );
        assert!(calibrated.persistence.fenced);
        assert!(calibrated.persistence.cleanup_debt);
        assert_eq!(
            calibrated.persistence.detail, None,
            "the metrics projection must not copy persistence diagnostics"
        );
        assert_eq!(calibrated.handoff.shards.len(), 2);
        assert!(calibrated
            .handoff
            .shards
            .iter()
            .all(|shard| shard.last_error.is_none()));
        assert_eq!(
            calibrated.hotspot.handoff_shards,
            vec![crate::cluster::control::ControlHotspotShardSnapshot {
                shard: 0,
                pending_rows: 37,
            }]
        );
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        assert_eq!(calibration_budget.snapshot().active_queries, 0);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(exact_bytes),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let exact_snapshot = runtime
            .metrics_snapshot_with_execution(&exact)
            .expect("the exact metrics projection limit should pass");
        assert_eq!(exact_snapshot.accounted_bytes(), exact_bytes);
        drop(exact_snapshot);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(exact_bytes.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = runtime
            .metrics_snapshot_with_execution(&one_under)
            .expect_err("one byte below the projection model must reject");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, exact_bytes);
            }
            other => panic!("unexpected metrics projection error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            one_under_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            0,
            "a failed reservation must happen before projection materialization"
        );
        drop(one_under);
        let one_under_status = one_under_budget.snapshot();
        assert_eq!(one_under_status.active_queries, 0);
        assert_eq!(one_under_status.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn control_metrics_projection_honors_precancellation_without_residual_memory() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        configure_metrics_projection_fixture(&runtime);
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("test budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("test query should admit");
        cancellation.cancel();

        let error = runtime
            .metrics_snapshot_with_execution(&execution)
            .expect_err("a pre-cancelled projection must stop");
        assert!(matches!(error, QueryBudgetError::Cancelled));
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
    fn control_status_projection_preserves_legacy_snapshot_values() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        configure_metrics_projection_fixture(&runtime);
        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.last_leader_contact_unix_ms = u64::MAX;
            state
                .peer_heartbeat
                .get_mut("node-b")
                .expect("peer fixture should exist")
                .last_success_unix_ms = Some(u64::MAX);
            state.control_state.transitions.reverse();
        }

        let legacy_liveness = runtime.liveness_snapshot();
        let legacy_persistence = runtime.persistence_status();
        let legacy_handoff = runtime.current_state().handoff_snapshot();
        let budget = QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let execution = budget.begin_query().expect("query should admit");
        let status = runtime
            .status_snapshot_with_execution(&execution)
            .expect("status projection should succeed");

        assert_eq!(&status.liveness, &legacy_liveness);
        assert_eq!(&status.persistence, &legacy_persistence);
        assert_eq!(&status.handoff, &legacy_handoff);
        assert_eq!(
            status.hotspot.handoff_shards,
            vec![crate::cluster::control::ControlHotspotShardSnapshot {
                shard: 0,
                pending_rows: 37,
            }]
        );
        assert_eq!(
            status.persistence.detail.as_deref(),
            Some("diagnostic fence detail".repeat(256).as_str())
        );
        assert!(status
            .handoff
            .shards
            .iter()
            .all(|shard| shard.last_error.is_some()));
        assert_eq!(
            status
                .handoff
                .shards
                .iter()
                .map(|shard| shard.shard)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "status handoff ordering must match the sorted legacy snapshot"
        );

        drop(status);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let budget_status = budget.snapshot();
        assert_eq!(budget_status.active_queries, 0);
        assert_eq!(budget_status.shared_reserved_memory_bytes, 0);
        assert_eq!(budget_status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn control_status_projection_preserves_persistence_detail_precedence() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        let cases = [
            (
                Some("fence"),
                Some("checkpoint"),
                Some("cleanup"),
                Some("fence"),
            ),
            (
                None,
                Some("checkpoint"),
                Some("cleanup"),
                Some("checkpoint"),
            ),
            (None, None, Some("cleanup"), Some("cleanup")),
            (None, None, None, None),
        ];
        let budget = QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let execution = budget.begin_query().expect("query should admit");

        for (fence, checkpoint, cleanup, expected) in cases {
            {
                let mut state = runtime
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.persistence_fence = fence.map(str::to_string);
                state.checkpoint_pending = checkpoint.map(|detail| ControlCheckpointPending {
                    position: ControlCommitPosition { index: 17, term: 4 },
                    detail: detail.to_string(),
                });
                state.cleanup_debt = cleanup.map(str::to_string);
            }
            let legacy = runtime.persistence_status();
            let status = runtime
                .status_snapshot_with_execution(&execution)
                .expect("status projection should succeed");
            assert_eq!(&status.persistence, &legacy);
            assert_eq!(status.persistence.detail.as_deref(), expected);
            drop(status);
            assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        }

        drop(execution);
        let budget_status = budget.snapshot();
        assert_eq!(budget_status.active_queries, 0);
        assert_eq!(budget_status.shared_reserved_memory_bytes, 0);
        assert_eq!(budget_status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn control_status_projection_enforces_exact_peak_before_materialization() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        configure_metrics_projection_fixture(&runtime);

        let calibration_budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = runtime
            .status_snapshot_with_execution(&calibration)
            .expect("calibration status projection should succeed");
        let exact_bytes = calibrated.accounted_bytes();
        assert!(exact_bytes > 0);
        assert_eq!(
            calibration.snapshot().memory_reserved_bytes,
            exact_bytes,
            "the wrapper must retain the complete status projection reservation"
        );
        assert_eq!(
            calibration_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            exact_bytes,
            "the measured retained output is the complete materialization peak"
        );
        assert!(calibrated.persistence.detail.is_some());
        assert!(calibrated
            .handoff
            .shards
            .iter()
            .all(|shard| shard.last_error.is_some()));
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        assert_eq!(calibration_budget.snapshot().active_queries, 0);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(exact_bytes),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let exact_status = runtime
            .status_snapshot_with_execution(&exact)
            .expect("the exact status projection limit should pass");
        assert_eq!(exact_status.accounted_bytes(), exact_bytes);
        drop(exact_status);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        let exact_budget_status = exact_budget.snapshot();
        assert_eq!(exact_budget_status.active_queries, 0);
        assert_eq!(exact_budget_status.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_budget_status.accounting_invariant_violations_total, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(exact_bytes.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = runtime
            .status_snapshot_with_execution(&one_under)
            .expect_err("one byte below the status projection peak must reject");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, exact_bytes);
            }
            other => panic!("unexpected status projection error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            one_under_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            0,
            "the N-1 rejection must happen before any status field is cloned"
        );
        drop(one_under);
        let one_under_status = one_under_budget.snapshot();
        assert_eq!(one_under_status.active_queries, 0);
        assert_eq!(one_under_status.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn control_status_projection_honors_precancellation_without_residual_memory() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        configure_metrics_projection_fixture(&runtime);
        let budget = QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("query should admit");
        cancellation.cancel();

        let error = runtime
            .status_snapshot_with_execution(&execution)
            .expect_err("a pre-cancelled status projection must stop");
        assert!(matches!(error, QueryBudgetError::Cancelled));
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 0);
        drop(execution);

        let budget_status = budget.snapshot();
        assert_eq!(budget_status.active_queries, 0);
        assert_eq!(budget_status.shared_reserved_memory_bytes, 0);
        assert_eq!(budget_status.cancellations_total, 1);
        assert_eq!(budget_status.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn install_snapshot_replaces_state_and_compacts_log() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");

        let runtime = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&state_store),
            bootstrap_state.clone(),
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let mut snapshot_state = bootstrap_state;
        snapshot_state.leader_node_id = Some("node-a".to_string());
        snapshot_state.applied_log_index = 5;
        snapshot_state.applied_log_term = 7;

        let install = InternalControlInstallSnapshotRequest {
            term: 7,
            leader_node_id: "node-a".to_string(),
            snapshot_last_index: 5,
            snapshot_last_term: 7,
            state: serde_json::to_value(&snapshot_state).expect("state should encode"),
        };
        let response = runtime
            .handle_install_snapshot_request(install)
            .expect("snapshot should install");
        assert!(response.success);
        assert_eq!(response.last_index, 5);

        let state = runtime.current_state();
        assert_eq!(state.applied_log_index, 5);
        assert_eq!(state.applied_log_term, 7);
        assert_eq!(state.leader_node_id.as_deref(), Some("node-a"));
    }

    #[test]
    fn install_snapshot_rejects_stale_snapshot_index_without_rollback() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "stale-snapshot",
            64,
        );

        let set_node_a = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 1,
            })
            .expect("append should succeed");
        assert!(set_node_a.success);

        let set_node_b = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 3,
                leader_node_id: "node-b".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: vec![InternalControlLogEntry {
                    index: 2,
                    term: 3,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-b".to_string(),
                    },
                    created_unix_ms: 2,
                }],
                leader_commit: 2,
            })
            .expect("append should succeed");
        assert!(set_node_b.success);
        assert_eq!(
            runtime.current_state().leader_node_id.as_deref(),
            Some("node-b")
        );

        let mut stale_snapshot_state = runtime.current_state();
        stale_snapshot_state.leader_node_id = Some("node-a".to_string());
        stale_snapshot_state.applied_log_index = 1;
        stale_snapshot_state.applied_log_term = 2;

        let response = runtime
            .handle_install_snapshot_request(InternalControlInstallSnapshotRequest {
                term: 3,
                leader_node_id: "node-b".to_string(),
                snapshot_last_index: 1,
                snapshot_last_term: 2,
                state: serde_json::to_value(&stale_snapshot_state)
                    .expect("snapshot state should encode"),
            })
            .expect("snapshot install should return");
        assert!(!response.success);
        assert_eq!(response.message.as_deref(), Some("stale_snapshot"));

        let state = runtime.current_state();
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(state.applied_log_term, 3);
        assert_eq!(state.leader_node_id.as_deref(), Some("node-b"));
    }

    #[test]
    fn committed_entries_are_compacted_at_snapshot_interval() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");

        let runtime = ControlConsensusRuntime::open(
            membership,
            Arc::clone(&state_store),
            bootstrap_state,
            temp_dir.path().join("control-log.json"),
            ControlConsensusConfig {
                snapshot_interval_entries: 1,
                ..ControlConsensusConfig::default()
            },
        )
        .expect("runtime should open");

        let append = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![InternalControlLogEntry {
                index: 1,
                term: 2,
                command: InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
                created_unix_ms: 1,
            }],
            leader_commit: 1,
        };
        let response = runtime
            .handle_append_request(append)
            .expect("append should succeed");
        assert!(response.success);

        let (snapshot_index, snapshot_term, entries_len) = runtime.log_snapshot_position();
        assert_eq!(snapshot_index, 1);
        assert_eq!(snapshot_term, 2);
        assert_eq!(entries_len, 0);
    }

    #[test]
    fn append_rejection_uses_match_hint_for_peer_backtracking() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "leader",
            2,
        );

        let mut state = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.peer_next_index.insert("node-b".to_string(), 20);
        let mismatch_response = InternalControlAppendResponse {
            term: state.current_term,
            success: false,
            match_index: 5,
            message: Some("prev_log_term_mismatch".to_string()),
        };
        let next_after_mismatch =
            runtime.peer_next_index_after_append_reject(&state, "node-b", &mismatch_response);
        assert_eq!(next_after_mismatch, 6);

        let snapshot_required_response = InternalControlAppendResponse {
            term: state.current_term,
            success: false,
            match_index: 11,
            message: Some("snapshot_required".to_string()),
        };
        let next_after_snapshot_required = runtime.peer_next_index_after_append_reject(
            &state,
            "node-b",
            &snapshot_required_response,
        );
        assert_eq!(next_after_snapshot_required, 12);
    }

    #[test]
    fn follower_catch_up_uses_snapshot_then_append() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let leader = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "leader",
            2,
        );
        let follower = open_runtime_for_node(
            &temp_dir,
            "node-b",
            "127.0.0.1:9302",
            &["node-a@127.0.0.1:9301"],
            "follower",
            64,
        );

        {
            let mut leader_state = leader
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for _ in 0..3 {
                let (entry, _, _, _) = leader
                    .prepare_proposal_locked(
                        &mut leader_state,
                        InternalControlCommand::SetLeader {
                            leader_node_id: "node-a".to_string(),
                        },
                    )
                    .expect("proposal should prepare");
                leader_state.commit_index = entry.index;
                leader
                    .apply_committed_entries_in_memory_locked(&mut leader_state)
                    .expect("committed entries should apply");
            }
            leader
                .persist_checkpoint_candidate_locked(
                    &leader_state,
                    ControlCheckpointWriteMode::Growth,
                )
                .expect("leader checkpoint should persist");
            leader_state.peer_next_index.insert("node-b".to_string(), 1);
        }

        let snapshot_plan = {
            let leader_state = leader
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            leader
                .build_peer_plan_locked(&leader_state, "node-b")
                .expect("snapshot plan should build")
        };
        let snapshot_response = match snapshot_plan {
            PeerPlan::InstallSnapshot(request) => follower
                .handle_install_snapshot_request(request)
                .expect("snapshot install should return"),
            PeerPlan::Append(_) => panic!("expected snapshot plan for lagging follower"),
        };
        assert!(snapshot_response.success);
        {
            let mut leader_state = leader
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            leader_state.peer_next_index.insert(
                "node-b".to_string(),
                snapshot_response.last_index.saturating_add(1),
            );
        }

        let append_plan = {
            let leader_state = leader
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            leader
                .build_peer_plan_locked(&leader_state, "node-b")
                .expect("append plan should build")
        };
        let append_response = match append_plan {
            PeerPlan::Append(request) => follower
                .handle_append_request(request)
                .expect("append should return"),
            PeerPlan::InstallSnapshot(_) => panic!("expected append plan after snapshot"),
        };
        assert!(append_response.success);

        let leader_state = leader.current_state();
        let follower_state = follower.current_state();
        assert_eq!(
            follower_state.applied_log_index,
            leader_state.applied_log_index
        );
        assert_eq!(
            follower_state.applied_log_term,
            leader_state.applied_log_term
        );
        assert_eq!(follower_state.leader_node_id, leader_state.leader_node_id);
    }

    #[test]
    fn restore_recovery_snapshot_rehydrates_control_state_and_log() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );

        let append_leader = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![InternalControlLogEntry {
                index: 1,
                term: 2,
                command: InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
                created_unix_ms: 1,
            }],
            leader_commit: 1,
        };
        let response = runtime
            .handle_append_request(append_leader)
            .expect("leader append should succeed");
        assert!(response.success);

        let append_join = InternalControlAppendRequest {
            term: 3,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![InternalControlLogEntry {
                index: 2,
                term: 3,
                command: InternalControlCommand::JoinNode {
                    node_id: "node-c".to_string(),
                    endpoint: "127.0.0.1:9303".to_string(),
                },
                created_unix_ms: 2,
            }],
            leader_commit: 2,
        };
        let response = runtime
            .handle_append_request(append_join)
            .expect("join append should succeed");
        assert!(response.success);

        let saved_state = runtime.current_state();
        let saved_log = runtime.log_recovery_snapshot();
        assert!(saved_state.node_record("node-c").is_some());
        assert_eq!(saved_log.commit_index, 2);

        let append_leave = InternalControlAppendRequest {
            term: 4,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 2,
            prev_log_term: 3,
            entries: vec![InternalControlLogEntry {
                index: 3,
                term: 4,
                command: InternalControlCommand::LeaveNode {
                    node_id: "node-c".to_string(),
                },
                created_unix_ms: 3,
            }],
            leader_commit: 3,
        };
        let response = runtime
            .handle_append_request(append_leave)
            .expect("leave append should succeed");
        assert!(response.success);
        let changed_state = runtime.current_state();
        assert_eq!(
            changed_state
                .node_record("node-c")
                .expect("node-c should exist")
                .status
                .as_str(),
            "leaving"
        );

        let restored_state = runtime
            .restore_recovery_snapshot(saved_state.clone(), saved_log.clone(), false)
            .expect("restore should succeed");
        assert_eq!(restored_state, saved_state);
        assert_eq!(runtime.current_state(), saved_state);
        assert_eq!(
            runtime
                .state_store
                .load()
                .expect("persisted state should load")
                .expect("persisted state should exist"),
            saved_state
        );

        let restored_log = runtime.log_recovery_snapshot();
        assert_eq!(restored_log.commit_index, saved_log.commit_index);
        assert_eq!(restored_log.entries, saved_log.entries);
    }

    #[test]
    fn restore_recovery_snapshot_can_force_local_leader() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "node-a",
            64,
        );
        let mut recovered_state = runtime.current_state();
        recovered_state.leader_node_id = Some("node-b".to_string());
        recovered_state.updated_unix_ms = recovered_state.updated_unix_ms.saturating_add(1);
        recovered_state
            .validate()
            .expect("recovery state should validate");

        let log_snapshot = runtime.log_recovery_snapshot();
        let restored_state = runtime
            .restore_recovery_snapshot(recovered_state, log_snapshot, true)
            .expect("forced leader restore should succeed");
        assert_eq!(restored_state.leader_node_id.as_deref(), Some("node-a"));
        assert_eq!(
            runtime.current_state().leader_node_id.as_deref(),
            Some("node-a")
        );
        assert_eq!(
            runtime
                .state_store
                .load()
                .expect("persisted state should load")
                .expect("persisted state should exist")
                .leader_node_id
                .as_deref(),
            Some("node-a")
        );
    }

    #[test]
    fn checkpoint_failure_after_log_publication_is_committed_and_repairs_on_reopen() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let state_store =
            Arc::new(ControlStateStore::open(state_path.clone()).expect("state store should open"));
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&state_store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let append = InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![InternalControlLogEntry {
                index: 1,
                term: 2,
                command: InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
                created_unix_ms: 1,
            }],
            leader_commit: 0,
        };
        assert!(
            runtime
                .handle_append_request(append)
                .expect("uncommitted append should persist")
                .success
        );

        let _failure_guard = fail_control_checkpoint_after_log_publish_once(log_path.clone());
        let response = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: Vec::new(),
                leader_commit: 1,
            })
            .expect("durable log commit should still be acknowledged");
        assert!(response.success);
        assert_eq!(response.message.as_deref(), Some("checkpoint_pending"));
        assert_eq!(runtime.current_state().applied_log_index, 1);
        let persistence = runtime.persistence_status();
        assert!(persistence.fenced);
        assert_eq!(persistence.pending_checkpoint.unwrap().index, 1);
        assert_eq!(
            state_store
                .load()
                .expect("state mirror should load")
                .expect("state mirror should exist")
                .applied_log_index,
            0,
            "the injected failure must leave the mirror behind the authoritative log"
        );
        drop(runtime);
        drop(state_store);

        let reopened_store =
            Arc::new(ControlStateStore::open(state_path).expect("state mirror should reopen"));
        let stale_state = reopened_store
            .load()
            .expect("state mirror should load")
            .expect("state mirror should exist");
        let reopened = ControlConsensusRuntime::open(
            membership,
            Arc::clone(&reopened_store),
            stale_state,
            log_path,
            ControlConsensusConfig::default(),
        )
        .expect("authoritative log should repair the stale mirror");
        assert_eq!(reopened.current_state().applied_log_index, 1);
        assert!(!reopened.persistence_status().fenced);
        assert_eq!(
            reopened_store
                .load()
                .expect("repaired mirror should load")
                .expect("repaired mirror should exist")
                .applied_log_index,
            1
        );
    }

    #[test]
    fn durable_pair_finalization_failure_records_cleanup_debt_and_repairs_owned_temps() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");
        let state_store = Arc::new(
            ControlStateStore::open_with_disk_budget(state_path.clone(), Some(Arc::clone(&budget)))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            Arc::clone(&state_store),
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should open");

        let append = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 0,
            })
            .expect("uncommitted append should persist");
        assert!(append.success);

        let _failure_guard = fail_control_pair_finalization_once(log_path.clone());
        let commit = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: Vec::new(),
                leader_commit: 1,
            })
            .expect("both durable files should allow a successful commit response");
        assert!(commit.success);
        assert_eq!(commit.message.as_deref(), Some("cleanup_pending"));

        let persistence = runtime.persistence_status();
        assert!(!persistence.fenced);
        assert!(persistence.pending_checkpoint.is_none());
        assert!(persistence.cleanup_debt);
        assert!(runtime.exportable_recovery_snapshot_bundle().is_ok());

        let authoritative_state = runtime.current_state();
        assert_eq!(authoritative_state.applied_log_index, 1);
        assert_eq!(
            state_store
                .load()
                .expect("state mirror should load")
                .expect("state mirror should exist"),
            authoritative_state
        );
        let authoritative_log = load_log_file(&log_path).expect("control log should load");
        assert_eq!(authoritative_log.commit_index, 1);
        assert_eq!(
            authoritative_log.checkpoint_state.as_ref(),
            Some(&authoritative_state)
        );

        let owned_orphan = temp_dir
            .path()
            .join(".control-log.json.tmp-123-0000000000000001");
        std::fs::write(&owned_orphan, b"owned orphan")
            .expect("owned temporary fixture should write");
        assert!(owned_orphan.is_file());

        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime
                .repair_persistence_fence_locked(&mut state)
                .expect("cleanup-only repair should succeed");
            assert!(state.cleanup_debt.is_none());
            assert!(state.persistence_fence.is_none());
            assert!(state.checkpoint_pending.is_none());
        }
        assert!(!owned_orphan.exists());
        let repaired = runtime.persistence_status();
        assert!(!repaired.fenced);
        assert!(!repaired.cleanup_debt);
    }

    #[test]
    fn exportable_recovery_snapshot_blocks_authority_fences_but_allows_cleanup_debt() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "exportability",
            64,
        );

        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.cleanup_debt = Some("post-durable cleanup remains".to_string());
        }
        runtime
            .exportable_recovery_snapshot_bundle()
            .expect("cleanup debt must not hide an authoritative durable pair");

        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.cleanup_debt = None;
            state.persistence_fence = Some("publication authority is ambiguous".to_string());
        }
        let fenced = runtime
            .exportable_recovery_snapshot_bundle()
            .expect_err("a persistence fence must block recovery export");
        assert!(fenced.contains("publication authority is ambiguous"));

        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.persistence_fence = None;
            state.checkpoint_pending = Some(ControlCheckpointPending {
                position: ControlCommitPosition { index: 1, term: 1 },
                detail: "checkpoint mirror is behind".to_string(),
            });
        }
        let checkpoint_pending = runtime
            .exportable_recovery_snapshot_bundle()
            .expect_err("checkpoint-pending authority must block recovery export");
        assert!(checkpoint_pending.contains("checkpoint mirror is behind"));
    }

    #[test]
    fn consensus_required_pair_quota_fences_without_definitive_rejection() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let initial_budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("initial budget should open");
        let initial_store = Arc::new(
            ControlStateStore::open_with_disk_budget(
                state_path.clone(),
                Some(Arc::clone(&initial_budget)),
            )
            .expect("budgeted state store should open"),
        );
        initial_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let initial_runtime = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&initial_store),
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("initial runtime should open");
        initial_runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 0,
            })
            .expect("uncommitted entry should persist before quota restart");
        drop(initial_runtime);
        drop(initial_store);
        drop(initial_budget);

        let exact_bytes = std::fs::metadata(&state_path)
            .expect("state metadata should load")
            .len()
            .checked_add(
                std::fs::metadata(&log_path)
                    .expect("log metadata should load")
                    .len(),
            )
            .expect("fixture size should fit");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(exact_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("exact budget should open");
        let state_store = Arc::new(
            ControlStateStore::open_with_disk_budget(state_path, Some(Arc::clone(&budget)))
                .expect("state store should reopen"),
        );
        let recovered = state_store
            .load()
            .expect("state should load")
            .expect("state should exist");
        let runtime = ControlConsensusRuntime::open(
            membership,
            state_store,
            recovered,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("runtime should reopen without rewriting a coherent pair");
        let before_state = runtime.current_state();
        let before_log = std::fs::read(&log_path).expect("log should read");

        let err = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: Vec::new(),
                leader_commit: 1,
            })
            .expect_err("the exact logical quota should reject paired checkpoint staging");
        assert!(err.resource_limit().is_none());
        assert!(err.is_indeterminate());
        assert_eq!(before_state.applied_log_index, 0);
        assert_eq!(runtime.current_state().applied_log_index, 1);
        let persistence = runtime.persistence_status();
        assert!(persistence.fenced);
        assert_eq!(persistence.pending_checkpoint, None);
        assert_eq!(
            std::fs::read(&log_path).expect("log should remain readable"),
            before_log
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, exact_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reservation_overruns_total, 0);
    }

    #[test]
    fn authoritative_checkpoint_repairs_stale_mirror_at_logical_quota() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let initial_budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("initial budget should open");
        let initial_store = Arc::new(
            ControlStateStore::open_with_disk_budget(
                state_path.clone(),
                Some(Arc::clone(&initial_budget)),
            )
            .expect("budgeted state store should open"),
        );
        initial_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let runtime = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&initial_store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("initial runtime should open");
        runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 2,
                leader_node_id: "node-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![InternalControlLogEntry {
                    index: 1,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: 1,
            })
            .expect("committed append should persist");
        assert_eq!(runtime.current_state().applied_log_index, 1);
        drop(runtime);
        drop(initial_store);
        drop(initial_budget);

        tsink::engine::fs_utils::write_file_atomically_and_sync_parent(
            &state_path,
            &encode_control_state_file(&bootstrap_state).expect("stale state should encode"),
        )
        .expect("stale mirror should replace the checkpoint");
        let stale_pair_bytes = std::fs::metadata(&state_path)
            .expect("state metadata should load")
            .len()
            .checked_add(
                std::fs::metadata(&log_path)
                    .expect("log metadata should load")
                    .len(),
            )
            .expect("fixture size should fit");
        let exact_budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(stale_pair_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("exact budget should open");
        let reopened_store = Arc::new(
            ControlStateStore::open_with_disk_budget(state_path, Some(Arc::clone(&exact_budget)))
                .expect("stale state store should reopen"),
        );
        let stale_state = reopened_store
            .load()
            .expect("stale mirror should load")
            .expect("stale mirror should exist");
        assert_eq!(stale_state.applied_log_index, 0);

        let reopened = ControlConsensusRuntime::open(
            membership,
            Arc::clone(&reopened_store),
            stale_state,
            log_path,
            ControlConsensusConfig::default(),
        )
        .expect("authoritative recovery admission should repair at the logical quota");
        assert_eq!(reopened.current_state().applied_log_index, 1);
        assert_eq!(
            reopened_store
                .load()
                .expect("repaired mirror should load")
                .expect("repaired mirror should exist")
                .applied_log_index,
            1
        );
        let snapshot = exact_budget.snapshot();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reservation_overruns_total, 0);
    }

    #[test]
    fn higher_term_is_durable_even_when_append_is_rejected() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "higher-term-reject",
            64,
        );
        let response = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: 9,
                leader_node_id: "node-b".to_string(),
                prev_log_index: 99,
                prev_log_term: 9,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .expect("rejection should persist the observed term");
        assert!(!response.success);
        assert_eq!(response.message.as_deref(), Some("missing_prev_log_index"));
        let persisted = load_log_file(runtime.log_path()).expect("log should load");
        assert_eq!(persisted.current_term, 9);
        assert!(persisted.entries.is_empty());
    }

    #[test]
    fn proposal_term_exhaustion_does_not_mutate_candidate() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "term-exhaustion",
            64,
        );
        let mut candidate = runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        candidate.current_term = u64::MAX;
        let entries_before = candidate.entries.clone();
        let err = runtime
            .prepare_proposal_locked(
                &mut candidate,
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .expect_err("term exhaustion must reject the proposal");
        assert!(err.contains("term exhausted"));
        assert_eq!(candidate.current_term, u64::MAX);
        assert_eq!(candidate.entries, entries_before);

        candidate.current_term = 7;
        candidate.commit_index = u64::MAX;
        candidate.snapshot_last_index = u64::MAX;
        candidate.snapshot_last_term = 7;
        candidate.control_state.applied_log_index = u64::MAX;
        candidate.control_state.applied_log_term = 7;
        let err = runtime
            .prepare_proposal_locked(
                &mut candidate,
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .expect_err("index exhaustion must reject the proposal");
        assert!(err.contains("index exhausted"));
        assert_eq!(candidate.current_term, 7);
        assert_eq!(candidate.entries, entries_before);
    }

    #[test]
    fn schema_v2_log_repairs_missing_and_invalid_state_mirrors() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let store =
            Arc::new(ControlStateStore::open(state_path.clone()).expect("state store should open"));
        let runtime = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("fresh paired runtime should open");
        force_local_leader(&runtime);
        let authoritative = runtime.current_state();
        drop(runtime);
        drop(store);

        std::fs::remove_file(&state_path).expect("state mirror should be removable");
        let missing_store = Arc::new(
            ControlStateStore::open(state_path.clone()).expect("missing mirror store should open"),
        );
        let reopened = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&missing_store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("authoritative log should repair a missing mirror");
        assert_eq!(reopened.current_state(), authoritative);
        assert_eq!(missing_store.load().unwrap(), Some(authoritative.clone()));
        drop(reopened);
        drop(missing_store);

        std::fs::write(&state_path, b"{invalid-json").expect("invalid mirror fixture should write");
        let invalid_store = Arc::new(
            ControlStateStore::open(state_path.clone()).expect("invalid mirror store should open"),
        );
        let reopened = ControlConsensusRuntime::open(
            membership,
            Arc::clone(&invalid_store),
            bootstrap_state,
            log_path,
            ControlConsensusConfig::default(),
        )
        .expect("authoritative log should repair an invalid mirror");
        assert_eq!(reopened.current_state(), authoritative);
        assert_eq!(invalid_store.load().unwrap(), Some(authoritative));
    }

    #[test]
    fn legacy_log_requires_valid_mirror_and_corrupt_v2_log_never_uses_mirror_authority() {
        let legacy_dir = TempDir::new().expect("legacy temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = legacy_dir.path().join("control-state.json");
        let log_path = legacy_dir.path().join("control-log.json");
        let legacy = ControlLogFileV1 {
            magic: CONTROL_LOG_MAGIC.to_string(),
            schema_version: CONTROL_LOG_LEGACY_SCHEMA_VERSION,
            current_term: 1,
            stepped_down_term: None,
            commit_index: 0,
            snapshot_last_index: 0,
            snapshot_last_term: 0,
            entries: Vec::new(),
            checkpoint_state: None,
        };
        tsink::engine::fs_utils::write_file_atomically_and_sync_parent(
            &log_path,
            &serde_json::to_vec_pretty(&legacy).expect("legacy log should encode"),
        )
        .expect("legacy log should write");
        let missing_store =
            Arc::new(ControlStateStore::open(state_path.clone()).expect("state store should open"));
        let err = ControlConsensusRuntime::open(
            membership.clone(),
            missing_store,
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect_err("legacy log without a mirror must fail closed");
        assert!(err.contains("requires a valid control-state mirror"));

        std::fs::write(&state_path, b"not-json").expect("corrupt mirror should write");
        let corrupt_store = Arc::new(
            ControlStateStore::open(state_path).expect("corrupt mirror store should open"),
        );
        let err = ControlConsensusRuntime::open(
            membership.clone(),
            corrupt_store,
            bootstrap_state.clone(),
            log_path,
            ControlConsensusConfig::default(),
        )
        .expect_err("legacy log with corrupt mirror must fail closed");
        assert!(err.contains("failed to parse control-state file"));

        let v2_dir = TempDir::new().expect("v2 temp dir should create");
        let state_path = v2_dir.path().join("control-state.json");
        let log_path = v2_dir.path().join("control-log.json");
        let store = Arc::new(
            ControlStateStore::open(state_path.clone()).expect("v2 state store should open"),
        );
        let runtime = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("v2 runtime should open");
        drop(runtime);
        drop(store);
        let mirror_before = std::fs::read(&state_path).expect("mirror should read");
        std::fs::write(&log_path, b"not-json").expect("corrupt log should write");
        let store = Arc::new(
            ControlStateStore::open(state_path.clone()).expect("state store should reopen"),
        );
        let err = ControlConsensusRuntime::open(
            membership,
            store,
            bootstrap_state,
            log_path,
            ControlConsensusConfig::default(),
        )
        .expect_err("corrupt authoritative log must fail closed");
        assert!(err.contains("failed to parse control-log file"));
        assert_eq!(std::fs::read(state_path).unwrap(), mirror_before);
    }

    #[test]
    fn missing_log_refuses_non_bootstrap_index_zero_mirror_without_mutation() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_path = temp_dir.path().join("control-state.json");
        let log_path = temp_dir.path().join("control-log.json");
        let store =
            Arc::new(ControlStateStore::open(state_path.clone()).expect("state store should open"));
        let mut mutated = bootstrap_state.clone();
        mutated.leader_node_id = Some("node-a".to_string());
        mutated.updated_unix_ms = mutated.updated_unix_ms.saturating_add(1);
        store
            .persist(&mutated)
            .expect("mutated mirror should persist");
        let mirror_before = std::fs::read(&state_path).expect("mirror should read");

        let err = ControlConsensusRuntime::open(
            membership,
            store,
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect_err("non-bootstrap mirror must not become authority");
        assert!(err.contains("not equivalent to the configured runtime bootstrap"));
        assert!(!log_path.exists());
        assert_eq!(std::fs::read(state_path).unwrap(), mirror_before);
    }

    #[test]
    fn log_validation_rejects_invalid_term_relationships() {
        let path = Path::new("<term-validation>");
        let baseline = ControlLogFileV1 {
            magic: CONTROL_LOG_MAGIC.to_string(),
            schema_version: CONTROL_LOG_LEGACY_SCHEMA_VERSION,
            current_term: 3,
            stepped_down_term: None,
            commit_index: 3,
            snapshot_last_index: 1,
            snapshot_last_term: 1,
            entries: vec![
                InternalControlLogEntry {
                    index: 2,
                    term: 2,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 1,
                },
                InternalControlLogEntry {
                    index: 3,
                    term: 3,
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                    created_unix_ms: 2,
                },
            ],
            checkpoint_state: None,
        };
        validate_log_file(&baseline, path).expect("baseline log should validate");

        let mut zero_snapshot_term = baseline.clone();
        zero_snapshot_term.snapshot_last_term = 0;
        assert!(validate_log_file(&zero_snapshot_term, path)
            .unwrap_err()
            .contains("term 0 at nonzero snapshot index"));

        let mut future_snapshot_term = baseline.clone();
        future_snapshot_term.snapshot_last_term = 4;
        assert!(validate_log_file(&future_snapshot_term, path)
            .unwrap_err()
            .contains("greater than current term"));

        let mut decreasing_terms = baseline;
        decreasing_terms.entries[0].term = 3;
        decreasing_terms.entries[1].term = 2;
        assert!(validate_log_file(&decreasing_terms, path)
            .unwrap_err()
            .contains("decreasing term"));
    }

    #[test]
    fn malformed_follower_entries_step_down_term_without_installing_invalid_candidate() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9302"],
            "malformed-entry",
            64,
        );
        force_local_leader(&runtime);
        let before = runtime.log_recovery_snapshot();
        let response = runtime
            .handle_append_request(InternalControlAppendRequest {
                term: before.current_term.saturating_add(1),
                leader_node_id: "node-b".to_string(),
                prev_log_index: before.commit_index,
                prev_log_term: runtime.current_state().applied_log_term,
                entries: vec![InternalControlLogEntry {
                    index: before.commit_index.saturating_add(1),
                    term: before.current_term.saturating_add(2),
                    command: InternalControlCommand::SetLeader {
                        leader_node_id: "node-b".to_string(),
                    },
                    created_unix_ms: 1,
                }],
                leader_commit: before.commit_index.saturating_add(1),
            })
            .expect("malformed append should return a protocol rejection");
        assert!(!response.success);
        assert_eq!(
            response.message.as_deref(),
            Some("entry_term_exceeds_request_term")
        );
        let after = runtime.log_recovery_snapshot();
        assert_eq!(after.entries, before.entries);
        assert_eq!(after.commit_index, before.commit_index);
        assert!(after.current_term > before.current_term);
        assert_eq!(after.stepped_down_term, after.current_term);
        assert!(!runtime.persistence_status().fenced);
        assert!(!runtime.is_local_control_leader());
    }

    #[test]
    fn higher_term_heartbeat_revokes_local_leadership_across_restart_and_restore() {
        let source_dir = TempDir::new().expect("source temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let source_state_path = source_dir.path().join("control-state.json");
        let source_log_path = source_dir.path().join("control-log.json");
        let source_store = Arc::new(
            ControlStateStore::open(source_state_path.clone()).expect("state store should open"),
        );
        let source = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&source_store),
            bootstrap_state.clone(),
            source_log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("source runtime should open");
        force_local_leader(&source);
        let before = source.log_recovery_snapshot();
        let response = source
            .handle_append_request(InternalControlAppendRequest {
                term: before.current_term.checked_add(1).unwrap(),
                leader_node_id: "node-b".to_string(),
                prev_log_index: before.snapshot_last_index,
                prev_log_term: before.snapshot_last_term,
                entries: Vec::new(),
                leader_commit: before.commit_index,
            })
            .expect("higher-term heartbeat should persist");
        assert!(response.success);
        assert!(!source.is_local_control_leader());
        let (saved_state, saved_log) = source.recovery_snapshot_bundle();
        assert_eq!(saved_log.stepped_down_term, saved_log.current_term);
        drop(source);
        drop(source_store);

        let reopened_store = Arc::new(
            ControlStateStore::open(source_state_path).expect("state store should reopen"),
        );
        let reopened = ControlConsensusRuntime::open(
            membership.clone(),
            reopened_store,
            bootstrap_state.clone(),
            source_log_path,
            ControlConsensusConfig::default(),
        )
        .expect("runtime should reopen");
        assert!(!reopened.is_local_control_leader());

        let target_dir = TempDir::new().expect("target temp dir should create");
        let target_store = Arc::new(
            ControlStateStore::open(target_dir.path().join("control-state.json"))
                .expect("target store should open"),
        );
        let target = ControlConsensusRuntime::open(
            membership,
            target_store,
            bootstrap_state,
            target_dir.path().join("control-log.json"),
            ControlConsensusConfig::default(),
        )
        .expect("target runtime should open");
        target
            .restore_recovery_snapshot(saved_state.clone(), saved_log.clone(), false)
            .expect("ordinary restore should preserve stepdown");
        assert!(!target.is_local_control_leader());
        target
            .restore_recovery_snapshot(saved_state, saved_log, true)
            .expect("forced restore should establish a fresh local term");
        assert!(target.is_local_control_leader());
    }

    #[tokio::test]
    async fn pre_quorum_quota_is_typed_but_post_quorum_quota_is_indeterminate() {
        let pre_dir = TempDir::new().expect("pre-quorum temp dir should create");
        let (membership, bootstrap_state) = single_node_membership_and_state();
        let state_path = pre_dir.path().join("control-state.json");
        let log_path = pre_dir.path().join("control-log.json");
        let initial_budget = LocalDiskBudget::open(pre_dir.path(), LocalDiskLimits::default())
            .expect("initial budget should open");
        let initial_store = Arc::new(
            ControlStateStore::open_with_disk_budget(
                state_path.clone(),
                Some(Arc::clone(&initial_budget)),
            )
            .expect("initial store should open"),
        );
        let initial = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&initial_store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("initial runtime should open");
        force_local_leader(&initial);
        drop(initial);
        drop(initial_store);
        drop(initial_budget);
        let exact_bytes = std::fs::metadata(&state_path).unwrap().len()
            + std::fs::metadata(&log_path).unwrap().len();
        let exact_budget = LocalDiskBudget::open(
            pre_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(exact_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("exact budget should open");
        let exact_store = Arc::new(
            ControlStateStore::open_with_disk_budget(state_path, Some(Arc::clone(&exact_budget)))
                .expect("exact store should open"),
        );
        let runtime = ControlConsensusRuntime::open(
            membership,
            exact_store,
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("exact runtime should open");
        let before_state = runtime.current_state();
        let before_log = std::fs::read(&log_path).unwrap();
        let err = runtime
            .propose_command(
                &test_rpc_client("node-a"),
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .await
            .expect_err("uncommitted log admission should fail before quorum");
        assert!(matches!(
            err.resource_limit(),
            Some(ControlDiskResourceLimit::DiskQuotaExceeded { .. })
        ));
        assert!(!err.is_indeterminate());
        assert!(!runtime.persistence_status().fenced);
        assert_eq!(runtime.current_state(), before_state);
        assert_eq!(std::fs::read(log_path).unwrap(), before_log);

        let post_dir = TempDir::new().expect("post-quorum temp dir should create");
        let (membership, bootstrap_state) = single_node_membership_and_state();
        let state_path = post_dir.path().join("control-state.json");
        let log_path = post_dir.path().join("control-log.json");
        let initial_budget = LocalDiskBudget::open(post_dir.path(), LocalDiskLimits::default())
            .expect("initial budget should open");
        let initial_store = Arc::new(
            ControlStateStore::open_with_disk_budget(
                state_path.clone(),
                Some(Arc::clone(&initial_budget)),
            )
            .expect("initial store should open"),
        );
        let initial = ControlConsensusRuntime::open(
            membership.clone(),
            Arc::clone(&initial_store),
            bootstrap_state.clone(),
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("initial runtime should open");
        force_local_leader(&initial);
        {
            let mut live = initial
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut candidate = live.clone();
            initial
                .prepare_proposal_locked(
                    &mut candidate,
                    InternalControlCommand::SetLeader {
                        leader_node_id: "node-a".to_string(),
                    },
                )
                .expect("uncommitted fixture should prepare");
            initial
                .publish_log_and_install_locked(&mut live, candidate)
                .expect("uncommitted fixture should persist");
        }
        drop(initial);
        drop(initial_store);
        drop(initial_budget);
        let exact_bytes = std::fs::metadata(&state_path).unwrap().len()
            + std::fs::metadata(&log_path).unwrap().len();
        let exact_budget = LocalDiskBudget::open(
            post_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(exact_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("exact budget should open");
        let exact_store = Arc::new(
            ControlStateStore::open_with_disk_budget(state_path, Some(Arc::clone(&exact_budget)))
                .expect("exact store should open"),
        );
        let runtime = ControlConsensusRuntime::open(
            membership,
            exact_store,
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("exact runtime should open");
        let before_log = std::fs::read(&log_path).unwrap();
        let err = runtime
            .propose_command(
                &test_rpc_client("node-a"),
                InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
            )
            .await
            .expect_err("paired publication after quorum should be indeterminate");
        assert!(err.resource_limit().is_none());
        assert!(err.is_indeterminate());
        assert_eq!(runtime.current_state().applied_log_index, 1);
        assert!(runtime.persistence_status().fenced);
        assert_eq!(std::fs::read(log_path).unwrap(), before_log);
    }

    #[test]
    fn legacy_v1_log_is_upgraded_to_required_v2_checkpoint() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let (membership, bootstrap_state) = sample_membership_and_state();
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("state store should open"),
        );
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap state should persist");
        let log_path = temp_dir.path().join("control-log.json");
        let legacy = ControlLogFileV1 {
            magic: CONTROL_LOG_MAGIC.to_string(),
            schema_version: CONTROL_LOG_LEGACY_SCHEMA_VERSION,
            current_term: 1,
            stepped_down_term: None,
            commit_index: 0,
            snapshot_last_index: 0,
            snapshot_last_term: 0,
            entries: Vec::new(),
            checkpoint_state: None,
        };
        tsink::engine::fs_utils::write_file_atomically_and_sync_parent(
            &log_path,
            &serde_json::to_vec_pretty(&legacy).expect("legacy log should encode"),
        )
        .expect("legacy log should persist");

        ControlConsensusRuntime::open(
            membership,
            state_store,
            bootstrap_state,
            log_path.clone(),
            ControlConsensusConfig::default(),
        )
        .expect("legacy pair should migrate");
        let upgraded: serde_json::Value =
            serde_json::from_slice(&std::fs::read(log_path).expect("upgraded log should read"))
                .expect("upgraded log should parse");
        assert_eq!(
            upgraded["schemaVersion"],
            serde_json::json!(CONTROL_LOG_SCHEMA_VERSION)
        );
        assert_eq!(upgraded["checkpointState"]["appliedLogIndex"], 0);
    }

    #[tokio::test]
    async fn replicate_to_all_followers_collects_all_peer_failures() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let runtime = open_runtime_for_node(
            &temp_dir,
            "node-a",
            "127.0.0.1:9301",
            &["node-b@127.0.0.1:9392", "node-c@127.0.0.1:9393"],
            "node-a",
            64,
        );
        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.control_state.leader_node_id = Some("node-a".to_string());
            state.current_term = 2;
            state.stepped_down_term = 1;
        }
        assert!(runtime.is_local_control_leader());
        let rpc_client = RpcClient::new(crate::cluster::rpc::RpcClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 0,
            protocol_version: crate::cluster::rpc::INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "test-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: crate::cluster::rpc::CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let err = runtime
            .replicate_to_all_followers(&rpc_client)
            .await
            .expect_err("replication should fail for unreachable peers");
        assert!(
            err.contains("node-b"),
            "missing node-b failure context: {err}"
        );
        assert!(
            err.contains("node-c"),
            "missing node-c failure context: {err}"
        );
    }
}
