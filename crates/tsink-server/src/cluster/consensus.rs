use crate::cluster::control::{
    ControlHandoffMutationOutcome, ControlMembershipMutationOutcome, ControlNodeStatus,
    ControlState, ControlStateStore,
};
use crate::cluster::membership::MembershipView;
use crate::cluster::rpc::{
    InternalControlAppendRequest, InternalControlAppendResponse, InternalControlCommand,
    InternalControlInstallSnapshotRequest, InternalControlInstallSnapshotResponse,
    InternalControlLogEntry, RpcClient,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONTROL_LOG_MAGIC: &str = "tsink-control-log";
const CONTROL_LOG_SCHEMA_VERSION: u16 = 1;

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
    Pending {
        required: usize,
        acknowledged: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ControlLogRecoverySnapshot {
    pub current_term: u64,
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
    config: ControlConsensusConfig,
    state: Arc<Mutex<ConsensusState>>,
}

#[derive(Debug, Clone)]
struct ConsensusState {
    current_term: u64,
    commit_index: u64,
    snapshot_last_index: u64,
    snapshot_last_term: u64,
    last_leader_contact_unix_ms: u64,
    entries: Vec<InternalControlLogEntry>,
    control_state: ControlState,
    peer_next_index: BTreeMap<String, u64>,
    peer_heartbeat: BTreeMap<String, PeerHeartbeatState>,
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
    commit_index: u64,
    snapshot_last_index: u64,
    snapshot_last_term: u64,
    entries: Vec<InternalControlLogEntry>,
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
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create control-log directory {}: {err}",
                    parent.display()
                )
            })?;
        }

        let mut persisted = if log_path.exists() {
            load_log_file(&log_path)?
        } else {
            ControlLogFileV1 {
                magic: CONTROL_LOG_MAGIC.to_string(),
                schema_version: CONTROL_LOG_SCHEMA_VERSION,
                current_term: bootstrap_state.applied_log_term.max(1),
                commit_index: bootstrap_state.applied_log_index,
                snapshot_last_index: bootstrap_state.applied_log_index,
                snapshot_last_term: bootstrap_state.applied_log_term,
                entries: Vec::new(),
            }
        };
        validate_log_file(&persisted, &log_path)?;

        if bootstrap_state.applied_log_index < persisted.snapshot_last_index {
            return Err(format!(
                "control state applied_log_index {} is older than control-log snapshot index {}",
                bootstrap_state.applied_log_index, persisted.snapshot_last_index
            ));
        }
        if bootstrap_state.applied_log_index > persisted.commit_index {
            return Err(format!(
                "control state applied_log_index {} exceeds control-log commit index {}",
                bootstrap_state.applied_log_index, persisted.commit_index
            ));
        }

        let last_index = persisted
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(persisted.snapshot_last_index);
        let next_index = last_index.saturating_add(1);
        let peer_next_index = bootstrap_state
            .nodes
            .iter()
            .filter(|node| {
                node.id != membership.local_node_id && node.status != ControlNodeStatus::Removed
            })
            .map(|node| (node.id.clone(), next_index))
            .collect::<BTreeMap<_, _>>();
        let peer_heartbeat = bootstrap_state
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
            config,
            state: Arc::new(Mutex::new(ConsensusState {
                current_term: persisted.current_term.max(1),
                commit_index: persisted.commit_index,
                snapshot_last_index: persisted.snapshot_last_index,
                snapshot_last_term: persisted.snapshot_last_term,
                last_leader_contact_unix_ms: now_ms,
                entries: std::mem::take(&mut persisted.entries),
                control_state: bootstrap_state,
                peer_next_index,
                peer_heartbeat,
            })),
        };

        {
            let mut state = runtime
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime.apply_committed_entries_locked(&mut state)?;
            runtime.reconcile_dynamic_peers_locked(&mut state);
            runtime.persist_log_locked(&state)?;
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

    pub fn recovery_snapshot_bundle(&self) -> (ControlState, ControlLogRecoverySnapshot) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.control_state.clone(),
            ControlLogRecoverySnapshot {
                current_term: state.current_term,
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

    pub fn restore_recovery_snapshot(
        &self,
        mut control_state: ControlState,
        log_snapshot: ControlLogRecoverySnapshot,
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
        validate_recovery_log_snapshot(&log_snapshot)?;
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

        let last_log_index = log_snapshot
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(log_snapshot.snapshot_last_index);
        let next_peer_index = last_log_index.saturating_add(1);

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.current_term = log_snapshot.current_term.max(1);
        state.commit_index = log_snapshot.commit_index;
        state.snapshot_last_index = log_snapshot.snapshot_last_index;
        state.snapshot_last_term = log_snapshot.snapshot_last_term;
        state.entries = log_snapshot.entries;
        state.control_state = control_state;
        state.last_leader_contact_unix_ms = unix_timestamp_millis();
        state.peer_next_index = self
            .control_peer_nodes_locked(&state)
            .into_iter()
            .map(|(node_id, _)| (node_id, next_peer_index))
            .collect();

        self.apply_committed_entries_locked(&mut state)?;
        self.reconcile_dynamic_peers_locked(&mut state);
        if state.current_term < state.control_state.applied_log_term {
            state.current_term = state.control_state.applied_log_term.max(1);
        }
        self.persist_log_locked(&state)?;
        Ok(state.control_state.clone())
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

    pub async fn ensure_leader_established(&self, rpc_client: &RpcClient) -> Result<(), String> {
        let should_propose = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<ProposeOutcome, String> {
        let (request, proposal_index, proposal_term, quorum, peers) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let quorum = self.quorum_size_locked(&state);
            let peers = self.control_peer_nodes_locked(&state);
            self.validate_local_proposal_locked(&state, &command, unix_timestamp_millis())?;
            let (entry, prev_log_index, prev_log_term, leader_commit_before) =
                self.prepare_proposal_locked(&mut state, command)?;
            if matches!(
                &entry.command,
                InternalControlCommand::SetLeader { leader_node_id } if leader_node_id == &self.local_node_id
            ) {
                state.last_leader_contact_unix_ms = unix_timestamp_millis();
            }
            let request = InternalControlAppendRequest {
                term: entry.term,
                leader_node_id: self.local_node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries: vec![entry.clone()],
                leader_commit: leader_commit_before,
            };
            (request, entry.index, entry.term, quorum, peers)
        };

        let mut acknowledged = 1usize;
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
            let (node_id, response) = result
                .map_err(|err| format!("control proposal replication task join failed: {err}"))?;
            match response {
                Ok(response) => {
                    if response.term > highest_remote_term {
                        highest_remote_term = response.term;
                    }
                    if response.success {
                        acknowledged += 1;
                        acknowledged_peers.push(node_id);
                    }
                }
                Err(err) => {
                    eprintln!("control proposal replication RPC failed for {node_id}: {err}");
                }
            }
        }

        if highest_remote_term > proposal_term {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.current_term = highest_remote_term;
            self.persist_log_locked(&state)?;
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

        let commit_index = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.commit_index < proposal_index {
                state.commit_index = proposal_index;
            }
            for node_id in &acknowledged_peers {
                state
                    .peer_next_index
                    .insert(node_id.clone(), proposal_index.saturating_add(1));
            }
            self.apply_committed_entries_locked(&mut state)?;
            if self.local_is_control_leader_locked(&state) {
                state.last_leader_contact_unix_ms = unix_timestamp_millis();
            }
            self.persist_log_locked(&state)?;
            state.commit_index
        };

        let commit_notice = InternalControlAppendRequest {
            term: proposal_term,
            leader_node_id: self.local_node_id.clone(),
            prev_log_index: proposal_index,
            prev_log_term: proposal_term,
            entries: Vec::new(),
            leader_commit: commit_index,
        };
        let mut tasks = tokio::task::JoinSet::new();
        let commit_peers = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.control_peer_nodes_locked(&state)
        };
        for (_node_id, endpoint) in commit_peers {
            let rpc_client = rpc_client.clone();
            let request = commit_notice.clone();
            tasks.spawn(async move {
                let _ = rpc_client.control_append(&endpoint, &request).await;
            });
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                eprintln!("control commit-notice task join failed: {err}");
            }
        }

        Ok(ProposeOutcome::Committed {
            index: proposal_index,
            term: proposal_term,
        })
    }

    pub async fn replicate_to_all_followers(&self, rpc_client: &RpcClient) -> Result<(), String> {
        let peers = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<InternalControlAppendResponse, String> {
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
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.is_membership_node_locked(&state, leader_node_id) {
            return Ok(InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: 0,
                message: Some("unknown_leader_node".to_string()),
            });
        }
        let previous_term = state.current_term;
        if request.term < previous_term {
            return Ok(InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: self.last_log_index_locked(&state),
                message: Some("stale_term".to_string()),
            });
        }
        if request.term == previous_term
            && state
                .control_state
                .leader_node_id
                .as_deref()
                .is_some_and(|leader| leader != leader_node_id)
        {
            return Ok(InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: self.last_log_index_locked(&state),
                message: Some("conflicting_leader_same_term".to_string()),
            });
        }
        if request.term > state.current_term {
            state.current_term = request.term;
        }
        state.last_leader_contact_unix_ms = unix_timestamp_millis();
        if leader_node_id != self.local_node_id {
            self.mark_peer_success_locked(&mut state, leader_node_id);
        }

        if request.prev_log_index < state.snapshot_last_index {
            return Ok(InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: state.snapshot_last_index,
                message: Some("snapshot_required".to_string()),
            });
        }

        let Some(local_prev_term) = self.term_at_locked(&state, request.prev_log_index) else {
            return Ok(InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: self.last_log_index_locked(&state),
                message: Some("missing_prev_log_index".to_string()),
            });
        };
        if local_prev_term != request.prev_log_term {
            return Ok(InternalControlAppendResponse {
                term: state.current_term,
                success: false,
                match_index: self.last_log_index_locked(&state),
                message: Some("prev_log_term_mismatch".to_string()),
            });
        }

        let mut expected_index = request.prev_log_index.saturating_add(1);
        for entry in request.entries {
            if entry.index != expected_index {
                return Ok(InternalControlAppendResponse {
                    term: state.current_term,
                    success: false,
                    match_index: self.last_log_index_locked(&state),
                    message: Some(format!(
                        "non_contiguous_entry_index: expected {expected_index}, got {}",
                        entry.index
                    )),
                });
            }
            if entry.term == 0 {
                return Ok(InternalControlAppendResponse {
                    term: state.current_term,
                    success: false,
                    match_index: self.last_log_index_locked(&state),
                    message: Some("entry_term_must_be_positive".to_string()),
                });
            }

            if entry.index <= state.snapshot_last_index {
                let snapshot_term = self.term_at_locked(&state, entry.index).unwrap_or(0);
                if snapshot_term != entry.term {
                    return Ok(InternalControlAppendResponse {
                        term: state.current_term,
                        success: false,
                        match_index: state.snapshot_last_index,
                        message: Some("snapshot_conflict".to_string()),
                    });
                }
                expected_index = expected_index.saturating_add(1);
                continue;
            }

            if let Some(offset) = self.entry_offset_locked(&state, entry.index) {
                let existing = &state.entries[offset];
                if existing.term != entry.term || existing.command != entry.command {
                    if entry.index <= state.commit_index {
                        return Err(format!(
                            "cannot overwrite committed control-log entry at index {}",
                            entry.index
                        ));
                    }
                    state.entries.truncate(offset);
                    state.entries.push(entry);
                }
            } else {
                let last_index = self.last_log_index_locked(&state);
                if entry.index != last_index.saturating_add(1) {
                    return Ok(InternalControlAppendResponse {
                        term: state.current_term,
                        success: false,
                        match_index: last_index,
                        message: Some("entry_index_gap".to_string()),
                    });
                }
                state.entries.push(entry);
            }
            expected_index = expected_index.saturating_add(1);
        }

        let last_index = self.last_log_index_locked(&state);
        if request.leader_commit > state.commit_index {
            state.commit_index = std::cmp::min(request.leader_commit, last_index);
            self.apply_committed_entries_locked(&mut state)?;
        }
        self.persist_log_locked(&state)?;

        Ok(InternalControlAppendResponse {
            term: state.current_term,
            success: true,
            match_index: last_index,
            message: None,
        })
    }

    pub fn handle_install_snapshot_request(
        &self,
        request: InternalControlInstallSnapshotRequest,
    ) -> Result<InternalControlInstallSnapshotResponse, String> {
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
        let mut snapshot_state: ControlState = serde_json::from_value(request.state)
            .map_err(|err| format!("failed to decode control snapshot payload: {err}"))?;
        snapshot_state.applied_log_index = request.snapshot_last_index;
        snapshot_state.applied_log_term = request.snapshot_last_term;
        snapshot_state.updated_unix_ms = unix_timestamp_millis();
        snapshot_state.leader_node_id = Some(leader_node_id.to_string());
        snapshot_state.validate()?;

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.is_membership_node_locked(&state, leader_node_id) {
            return Ok(InternalControlInstallSnapshotResponse {
                term: state.current_term,
                success: false,
                last_index: 0,
                message: Some("unknown_leader_node".to_string()),
            });
        }
        let previous_term = state.current_term;
        if request.term < previous_term {
            return Ok(InternalControlInstallSnapshotResponse {
                term: state.current_term,
                success: false,
                last_index: self.last_log_index_locked(&state),
                message: Some("stale_term".to_string()),
            });
        }
        if request.term == previous_term
            && state
                .control_state
                .leader_node_id
                .as_deref()
                .is_some_and(|leader| leader != leader_node_id)
        {
            return Ok(InternalControlInstallSnapshotResponse {
                term: state.current_term,
                success: false,
                last_index: self.last_log_index_locked(&state),
                message: Some("conflicting_leader_same_term".to_string()),
            });
        }

        state.current_term = request.term;
        state.last_leader_contact_unix_ms = unix_timestamp_millis();
        if leader_node_id != self.local_node_id {
            self.mark_peer_success_locked(&mut state, leader_node_id);
        }
        let min_snapshot_index = state
            .commit_index
            .max(state.control_state.applied_log_index);
        if request.snapshot_last_index < min_snapshot_index {
            return Ok(InternalControlInstallSnapshotResponse {
                term: state.current_term,
                success: false,
                last_index: self.last_log_index_locked(&state),
                message: Some("stale_snapshot".to_string()),
            });
        }
        state.commit_index = state.commit_index.max(request.snapshot_last_index);
        state.snapshot_last_index = request.snapshot_last_index;
        state.snapshot_last_term = request.snapshot_last_term;
        state
            .entries
            .retain(|entry| entry.index > request.snapshot_last_index);
        state.control_state = snapshot_state;
        self.reconcile_dynamic_peers_locked(&mut state);
        self.state_store.persist(&state.control_state)?;
        self.persist_log_locked(&state)?;

        Ok(InternalControlInstallSnapshotResponse {
            term: state.current_term,
            success: true,
            last_index: self.last_log_index_locked(&state),
            message: None,
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
        state.control_state.leader_node_id.as_deref() == Some(self.local_node_id.as_str())
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

        let current_idx = node_ids
            .iter()
            .position(|node_id| node_id == current_leader)?;
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

        let term = state.current_term.saturating_add(1).max(1);
        state.current_term = term;
        let prev_log_index = self.last_log_index_locked(state);
        let prev_log_term = self
            .term_at_locked(state, prev_log_index)
            .ok_or_else(|| "missing prev term for proposal".to_string())?;
        let entry = InternalControlLogEntry {
            index: prev_log_index.saturating_add(1),
            term,
            command,
            created_unix_ms: unix_timestamp_millis(),
        };
        state.entries.push(entry.clone());
        self.persist_log_locked(state)?;
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

    fn apply_committed_entries_locked(&self, state: &mut ConsensusState) -> Result<(), String> {
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
            self.state_store.persist(&state.control_state)?;
            self.maybe_compact_locked(state)?;
        }

        Ok(())
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
                if !control_state
                    .nodes
                    .iter()
                    .any(|node| node.id == *leader_node_id)
                {
                    return Err(format!(
                        "control command set_leader references unknown node '{}'",
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

    fn persist_log_locked(&self, state: &ConsensusState) -> Result<(), String> {
        let encoded = serde_json::to_vec_pretty(&ControlLogFileV1 {
            magic: CONTROL_LOG_MAGIC.to_string(),
            schema_version: CONTROL_LOG_SCHEMA_VERSION,
            current_term: state.current_term,
            commit_index: state.commit_index,
            snapshot_last_index: state.snapshot_last_index,
            snapshot_last_term: state.snapshot_last_term,
            entries: state.entries.clone(),
        })
        .map_err(|err| format!("failed to serialize control-log state: {err}"))?;
        write_atomically(&self.log_path, &encoded)
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
        let offset = (index - state.snapshot_last_index - 1) as usize;
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
            let plan = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.build_peer_plan_locked(&state, node_id)?
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
                            if response.term > request.term {
                                let mut state = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                if response.term > state.current_term {
                                    state.current_term = response.term;
                                    self.persist_log_locked(&state)?;
                                }
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
                                self.persist_log_locked(&state)?;
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
                            self.persist_log_locked(&state)?;
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
                            if response.term > request.term {
                                let mut state = self
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                if response.term > state.current_term {
                                    state.current_term = response.term;
                                    self.persist_log_locked(&state)?;
                                }
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
                                self.persist_log_locked(&state)?;
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
                    snapshot_last_index: state.snapshot_last_index,
                    snapshot_last_term: state.snapshot_last_term,
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

fn validate_log_file(file: &ControlLogFileV1, path: &Path) -> Result<(), String> {
    if file.magic != CONTROL_LOG_MAGIC {
        return Err(format!(
            "control-log file {} has unsupported magic '{}'",
            path.display(),
            file.magic
        ));
    }
    if file.schema_version != CONTROL_LOG_SCHEMA_VERSION {
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
    if file.snapshot_last_index > file.commit_index {
        return Err(format!(
            "control-log file {} has snapshot_last_index {} greater than commit_index {}",
            path.display(),
            file.snapshot_last_index,
            file.commit_index
        ));
    }

    let mut expected_index = file.snapshot_last_index.saturating_add(1);
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
        expected_index = expected_index.saturating_add(1);
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

    Ok(())
}

fn validate_recovery_log_snapshot(snapshot: &ControlLogRecoverySnapshot) -> Result<(), String> {
    let path = Path::new("<recovery-snapshot>");
    let file = ControlLogFileV1 {
        magic: CONTROL_LOG_MAGIC.to_string(),
        schema_version: CONTROL_LOG_SCHEMA_VERSION,
        current_term: snapshot.current_term,
        commit_index: snapshot.commit_index,
        snapshot_last_index: snapshot.snapshot_last_index,
        snapshot_last_term: snapshot.snapshot_last_term,
        entries: snapshot.entries.clone(),
    };
    validate_log_file(&file, path)
}

fn load_log_file(path: &Path) -> Result<ControlLogFileV1, String> {
    let raw = std::fs::read(path)
        .map_err(|err| format!("failed to read control-log file {}: {err}", path.display()))?;
    serde_json::from_slice(&raw)
        .map_err(|err| format!("failed to parse control-log file {}: {err}", path.display()))
}

fn write_atomically(path: &Path, encoded: &[u8]) -> Result<(), String> {
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|err| {
            format!(
                "failed to open temporary control-log file {}: {err}",
                tmp_path.display()
            )
        })?;
    file.write_all(encoded).map_err(|err| {
        format!(
            "failed to write temporary control-log file {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to fsync temporary control-log file {}: {err}",
            tmp_path.display()
        )
    })?;
    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to replace control-log file {} with {}: {err}",
            path.display(),
            tmp_path.display()
        )
    })
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
    use crate::cluster::control::ControlState;
    use crate::cluster::membership::{ClusterNode, MembershipView};
    use crate::cluster::ring::ShardRing;
    use crate::cluster::rpc::{
        derive_shared_internal_token, RpcClientConfig, INTERNAL_RPC_PROTOCOL_VERSION,
    };
    use tempfile::TempDir;

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
                    .apply_committed_entries_locked(&mut leader_state)
                    .expect("committed entries should apply");
            }
            leader
                .persist_log_locked(&leader_state)
                .expect("leader log should persist");
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
