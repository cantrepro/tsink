use crate::cluster::membership::MembershipView;
use crate::cluster::ring::{ring_hash_version_name, ShardRing, ShardRingSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CONTROL_STATE_SCHEMA_VERSION: u16 = 1;
const CONTROL_STATE_MAGIC: &str = "tsink-control-state";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ControlNodeStatus {
    Joining,
    #[default]
    Active,
    Leaving,
    Removed,
}

impl ControlNodeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Active => "active",
            Self::Leaving => "leaving",
            Self::Removed => "removed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlMembershipMutationOutcome {
    Noop,
    Applied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlHandoffMutationOutcome {
    Noop,
    Applied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShardHandoffPhase {
    #[default]
    Warmup,
    Cutover,
    FinalSync,
    Completed,
    Failed,
}

impl ShardHandoffPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Cutover => "cutover",
            Self::FinalSync => "final_sync",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Warmup | Self::Cutover | Self::FinalSync)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            (current, target) if current == target => true,
            (Self::Warmup, Self::Cutover | Self::Failed) => true,
            (Self::Cutover, Self::FinalSync | Self::Failed) => true,
            (Self::FinalSync, Self::Completed | Self::Failed) => true,
            (Self::Failed, Self::Warmup) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardHandoffProgress {
    #[serde(default)]
    pub phase: ShardHandoffPhase,
    #[serde(default)]
    pub copied_rows: u64,
    #[serde(default)]
    pub pending_rows: u64,
    #[serde(default)]
    pub resumed_count: u64,
    #[serde(default)]
    pub started_unix_ms: u64,
    #[serde(default)]
    pub updated_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl Default for ShardHandoffProgress {
    fn default() -> Self {
        Self {
            phase: ShardHandoffPhase::Warmup,
            copied_rows: 0,
            pending_rows: 0,
            resumed_count: 0,
            started_unix_ms: 0,
            updated_unix_ms: 0,
            last_error: None,
        }
    }
}

impl ShardHandoffProgress {
    fn bootstrap(now_ms: u64) -> Self {
        Self {
            started_unix_ms: now_ms,
            updated_unix_ms: now_ms,
            ..Self::default()
        }
    }

    fn advance(
        &mut self,
        next_phase: ShardHandoffPhase,
        copied_rows: Option<u64>,
        pending_rows: Option<u64>,
        last_error: Option<String>,
    ) -> Result<bool, String> {
        if !self.phase.can_transition_to(next_phase) {
            return Err(format!(
                "invalid handoff phase transition: {} -> {}",
                self.phase.as_str(),
                next_phase.as_str()
            ));
        }

        let mut changed = false;
        if self.phase != next_phase {
            if self.phase == ShardHandoffPhase::Failed && next_phase == ShardHandoffPhase::Warmup {
                self.resumed_count = self.resumed_count.saturating_add(1);
            }
            self.phase = next_phase;
            changed = true;
        }
        if let Some(value) = copied_rows {
            if self.copied_rows != value {
                self.copied_rows = value;
                changed = true;
            }
        }
        if let Some(value) = pending_rows {
            if self.pending_rows != value {
                self.pending_rows = value;
                changed = true;
            }
        }
        let normalized_error = last_error.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        if self.last_error != normalized_error {
            self.last_error = normalized_error;
            changed = true;
        }
        if changed {
            let now_ms = unix_timestamp_millis();
            if self.started_unix_ms == 0 {
                self.started_unix_ms = now_ms;
            }
            self.updated_unix_ms = now_ms;
        }
        Ok(changed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardHandoffSnapshot {
    pub shard: u32,
    pub from_node_id: String,
    pub to_node_id: String,
    pub activation_ring_version: u64,
    pub phase: ShardHandoffPhase,
    pub copied_rows: u64,
    pub pending_rows: u64,
    pub resumed_count: u64,
    pub started_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHandoffSnapshot {
    pub total_shards: usize,
    pub in_progress_shards: usize,
    pub warmup_shards: usize,
    pub cutover_shards: usize,
    pub final_sync_shards: usize,
    pub completed_shards: usize,
    pub failed_shards: usize,
    pub resumed_shards: usize,
    pub copied_rows_total: u64,
    pub pending_rows_total: u64,
    pub shards: Vec<ShardHandoffSnapshot>,
}

impl ClusterHandoffSnapshot {
    pub fn empty() -> Self {
        Self {
            total_shards: 0,
            in_progress_shards: 0,
            warmup_shards: 0,
            cutover_shards: 0,
            final_sync_shards: 0,
            completed_shards: 0,
            failed_shards: 0,
            resumed_shards: 0,
            copied_rows_total: 0,
            pending_rows_total: 0,
            shards: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlNodeRecord {
    pub id: String,
    pub endpoint: String,
    #[serde(default)]
    pub membership_generation: u64,
    #[serde(default)]
    pub status: ControlNodeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardOwnershipTransition {
    pub shard: u32,
    pub from_node_id: String,
    pub to_node_id: String,
    pub activation_ring_version: u64,
    #[serde(default)]
    pub handoff: ShardHandoffProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlState {
    pub membership_epoch: u64,
    pub ring_version: u64,
    #[serde(default)]
    pub applied_log_index: u64,
    #[serde(default)]
    pub applied_log_term: u64,
    #[serde(default)]
    pub leader_node_id: Option<String>,
    pub nodes: Vec<ControlNodeRecord>,
    pub ring: ShardRingSnapshot,
    #[serde(default)]
    pub transitions: Vec<ShardOwnershipTransition>,
    pub updated_unix_ms: u64,
}

impl ControlState {
    pub fn from_runtime(membership: &MembershipView, ring: &ShardRing) -> Self {
        let mut nodes = membership
            .nodes
            .iter()
            .map(|node| ControlNodeRecord {
                id: node.id.clone(),
                endpoint: node.endpoint.clone(),
                membership_generation: 1,
                status: ControlNodeStatus::Active,
            })
            .collect::<Vec<_>>();
        nodes.sort_by(|a, b| {
            a.id.cmp(&b.id)
                .then_with(|| a.endpoint.cmp(&b.endpoint))
                .then_with(|| a.membership_generation.cmp(&b.membership_generation))
        });

        Self {
            membership_epoch: 1,
            ring_version: 1,
            applied_log_index: 0,
            applied_log_term: 0,
            leader_node_id: None,
            nodes,
            ring: ring.snapshot(),
            transitions: Vec::new(),
            updated_unix_ms: unix_timestamp_millis(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.membership_epoch == 0 {
            return Err("control state requires membership_epoch > 0".to_string());
        }
        if self.ring_version == 0 {
            return Err("control state requires ring_version > 0".to_string());
        }
        if self.nodes.is_empty() {
            return Err("control state requires at least one membership node".to_string());
        }

        let mut node_ids = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for node in &self.nodes {
            let node_id = node.id.trim();
            if node_id.is_empty() {
                return Err("control membership contains an empty node id".to_string());
            }
            let endpoint = node.endpoint.trim();
            if endpoint.is_empty() {
                return Err(format!(
                    "control membership node '{}' has an empty endpoint",
                    node.id
                ));
            }
            if !node_ids.insert(node_id.to_string()) {
                return Err(format!(
                    "control membership contains duplicate node id '{node_id}'"
                ));
            }
            if !endpoints.insert(endpoint.to_string()) {
                return Err(format!(
                    "control membership contains duplicate endpoint '{endpoint}'"
                ));
            }
        }

        if let Some(leader) = self.leader_node_id.as_deref() {
            if !node_ids.contains(leader) {
                return Err(format!(
                    "control state leader '{}' is not part of membership",
                    leader
                ));
            }
        }

        ShardRing::from_snapshot(self.ring.clone())
            .map_err(|err| format!("invalid control ring snapshot: {err}"))?;
        for (shard, owners) in self.ring.assignments.iter().enumerate() {
            for owner in owners {
                if !node_ids.contains(owner) {
                    return Err(format!(
                        "control ring owner '{}' for shard {} is not present in membership",
                        owner, shard
                    ));
                }
            }
        }

        let mut transitioning_shards = BTreeSet::new();
        for transition in &self.transitions {
            if transition.shard >= self.ring.shard_count {
                return Err(format!(
                    "control transition shard {} exceeds ring shard_count {}",
                    transition.shard, self.ring.shard_count
                ));
            }
            if transition.from_node_id.trim().is_empty() {
                return Err(format!(
                    "control transition for shard {} has an empty from_node_id",
                    transition.shard
                ));
            }
            if transition.to_node_id.trim().is_empty() {
                return Err(format!(
                    "control transition for shard {} has an empty to_node_id",
                    transition.shard
                ));
            }
            if transition.from_node_id == transition.to_node_id {
                return Err(format!(
                    "control transition for shard {} must change ownership",
                    transition.shard
                ));
            }
            if !node_ids.contains(transition.from_node_id.as_str()) {
                return Err(format!(
                    "control transition from_node_id '{}' is not part of membership",
                    transition.from_node_id
                ));
            }
            if !node_ids.contains(transition.to_node_id.as_str()) {
                return Err(format!(
                    "control transition to_node_id '{}' is not part of membership",
                    transition.to_node_id
                ));
            }
            if transition.activation_ring_version < self.ring_version
                && !transition.handoff.phase.is_terminal()
            {
                return Err(format!(
                    "control transition shard {} activation_ring_version {} is older than ring_version {}",
                    transition.shard, transition.activation_ring_version, self.ring_version
                ));
            }
            if !transitioning_shards.insert(transition.shard) {
                return Err(format!(
                    "control state contains multiple transitions for shard {}",
                    transition.shard
                ));
            }
            if transition.handoff.updated_unix_ms > 0
                && transition.handoff.started_unix_ms > transition.handoff.updated_unix_ms
            {
                return Err(format!(
                    "control transition shard {} has started_unix_ms {} newer than updated_unix_ms {}",
                    transition.shard,
                    transition.handoff.started_unix_ms,
                    transition.handoff.updated_unix_ms
                ));
            }
        }

        Ok(())
    }

    pub fn ensure_runtime_compatible(
        &self,
        membership: &MembershipView,
        ring: &ShardRing,
    ) -> Result<(), String> {
        self.validate()?;
        let local_node_id = membership.local_node_id.as_str();
        let Some(local_runtime) = membership
            .nodes
            .iter()
            .find(|node| node.id == local_node_id)
        else {
            return Err(format!(
                "cluster runtime membership is missing local node '{}'",
                local_node_id
            ));
        };
        let Some(local_record) = self.node_record(local_node_id) else {
            return Err(format!(
                "persisted control membership is missing local cluster node '{}'",
                local_node_id
            ));
        };
        if local_record.endpoint != local_runtime.endpoint {
            return Err(format!(
                "persisted control membership endpoint mismatch for local node '{}': '{}' != '{}'",
                local_node_id, local_record.endpoint, local_runtime.endpoint
            ));
        }
        if self.ring.shard_count != ring.shard_count() {
            return Err(format!(
                "persisted control ring shard_count {} does not match runtime shard_count {}",
                self.ring.shard_count,
                ring.shard_count()
            ));
        }
        if self.ring.hash_version != ring.hash_version() {
            let persisted_hash_name =
                ring_hash_version_name(self.ring.hash_version).unwrap_or("unknown");
            let runtime_hash_name =
                ring_hash_version_name(ring.hash_version()).unwrap_or("unknown");
            return Err(format!(
                "persisted control ring hash_version {} ({}) does not match runtime hash_version {} ({}); rebuild or migrate the control state before startup",
                self.ring.hash_version,
                persisted_hash_name,
                ring.hash_version(),
                runtime_hash_name
            ));
        }
        if self.ring.replication_factor != ring.replication_factor() {
            return Err(format!(
                "persisted control ring replication_factor {} does not match runtime replication_factor {}",
                self.ring.replication_factor,
                ring.replication_factor()
            ));
        }
        if self.ring.virtual_nodes_per_node != ring.virtual_nodes_per_node() {
            return Err(format!(
                "persisted control ring virtual_nodes_per_node {} does not match runtime virtual_nodes_per_node {}",
                self.ring.virtual_nodes_per_node,
                ring.virtual_nodes_per_node()
            ));
        }
        Ok(())
    }

    pub fn shard_for_series_id(&self, series_id: u64) -> u32 {
        (series_id % u64::from(self.ring.shard_count)) as u32
    }

    pub fn owners_for_shard_at_ring_version(&self, shard: u32, ring_version: u64) -> Vec<String> {
        let Some(base_owners) = self.ring.assignments.get(shard as usize) else {
            return Vec::new();
        };
        let mut effective = base_owners.clone();
        if let Some(transition) = self.transitions.iter().find(|item| item.shard == shard) {
            if ring_version < transition.activation_ring_version {
                rewrite_shard_owner(
                    &mut effective,
                    transition.to_node_id.as_str(),
                    transition.from_node_id.as_str(),
                );
            } else {
                rewrite_shard_owner(
                    &mut effective,
                    transition.from_node_id.as_str(),
                    transition.to_node_id.as_str(),
                );
            }
        }
        dedupe_preserving_order(&mut effective);
        effective
    }

    pub fn effective_ring_snapshot_at_ring_version(&self, ring_version: u64) -> ShardRingSnapshot {
        let mut snapshot = self.ring.clone();
        snapshot.assignments = (0..snapshot.shard_count)
            .map(|shard| self.owners_for_shard_at_ring_version(shard, ring_version))
            .collect();
        snapshot
    }

    pub fn node_is_owner_for_shard_at_ring_version(
        &self,
        shard: u32,
        node_id: &str,
        ring_version: u64,
    ) -> bool {
        self.owners_for_shard_at_ring_version(shard, ring_version)
            .iter()
            .any(|owner| owner == node_id)
    }

    pub fn node_record(&self, node_id: &str) -> Option<&ControlNodeRecord> {
        self.nodes.iter().find(|node| node.id == node_id)
    }

    pub fn handoff_snapshot(&self) -> ClusterHandoffSnapshot {
        let mut snapshot = ClusterHandoffSnapshot::empty();
        for transition in &self.transitions {
            let handoff = &transition.handoff;
            snapshot.total_shards = snapshot.total_shards.saturating_add(1);
            snapshot.copied_rows_total = snapshot
                .copied_rows_total
                .saturating_add(handoff.copied_rows);
            snapshot.pending_rows_total = snapshot
                .pending_rows_total
                .saturating_add(handoff.pending_rows);
            if handoff.phase.is_active() {
                snapshot.in_progress_shards = snapshot.in_progress_shards.saturating_add(1);
            }
            if handoff.resumed_count > 0 {
                snapshot.resumed_shards = snapshot.resumed_shards.saturating_add(1);
            }
            match handoff.phase {
                ShardHandoffPhase::Warmup => {
                    snapshot.warmup_shards = snapshot.warmup_shards.saturating_add(1)
                }
                ShardHandoffPhase::Cutover => {
                    snapshot.cutover_shards = snapshot.cutover_shards.saturating_add(1)
                }
                ShardHandoffPhase::FinalSync => {
                    snapshot.final_sync_shards = snapshot.final_sync_shards.saturating_add(1)
                }
                ShardHandoffPhase::Completed => {
                    snapshot.completed_shards = snapshot.completed_shards.saturating_add(1)
                }
                ShardHandoffPhase::Failed => {
                    snapshot.failed_shards = snapshot.failed_shards.saturating_add(1)
                }
            }
            snapshot.shards.push(ShardHandoffSnapshot {
                shard: transition.shard,
                from_node_id: transition.from_node_id.clone(),
                to_node_id: transition.to_node_id.clone(),
                activation_ring_version: transition.activation_ring_version,
                phase: handoff.phase,
                copied_rows: handoff.copied_rows,
                pending_rows: handoff.pending_rows,
                resumed_count: handoff.resumed_count,
                started_unix_ms: handoff.started_unix_ms,
                updated_unix_ms: handoff.updated_unix_ms,
                last_error: handoff.last_error.clone(),
            });
        }
        snapshot.shards.sort_by(|left, right| {
            left.shard
                .cmp(&right.shard)
                .then_with(|| left.from_node_id.cmp(&right.from_node_id))
                .then_with(|| left.to_node_id.cmp(&right.to_node_id))
        });
        snapshot
    }

    pub fn apply_begin_shard_handoff(
        &mut self,
        shard: u32,
        from_node_id: &str,
        to_node_id: &str,
        activation_ring_version: u64,
    ) -> Result<ControlHandoffMutationOutcome, String> {
        if shard >= self.ring.shard_count {
            return Err(format!(
                "control command begin_shard_handoff shard {} exceeds ring shard_count {}",
                shard, self.ring.shard_count
            ));
        }
        let from_node_id =
            normalize_non_empty_membership_field(from_node_id, "from_node_id", "begin_handoff")?;
        let to_node_id =
            normalize_non_empty_membership_field(to_node_id, "to_node_id", "begin_handoff")?;
        if from_node_id == to_node_id {
            return Err("control command begin_shard_handoff requires distinct owners".to_string());
        }
        if activation_ring_version < self.ring_version {
            return Err(format!(
                "control command begin_shard_handoff activation_ring_version {} is older than ring_version {}",
                activation_ring_version, self.ring_version
            ));
        }
        if !self.nodes.iter().any(|node| node.id == from_node_id) {
            return Err(format!(
                "control command begin_shard_handoff references unknown from_node_id '{}'",
                from_node_id
            ));
        }
        if !self.nodes.iter().any(|node| node.id == to_node_id) {
            return Err(format!(
                "control command begin_shard_handoff references unknown to_node_id '{}'",
                to_node_id
            ));
        }
        let from_is_current_owner = self.node_is_owner_for_shard_at_ring_version(
            shard,
            from_node_id.as_str(),
            self.ring_version,
        );

        if let Some(transition) = self.transitions.iter_mut().find(|item| item.shard == shard) {
            if transition.handoff.phase == ShardHandoffPhase::Completed {
                if transition.from_node_id == from_node_id
                    && transition.to_node_id == to_node_id
                    && transition.activation_ring_version == activation_ring_version
                {
                    return Ok(ControlHandoffMutationOutcome::Noop);
                }
                if !from_is_current_owner {
                    return Err(format!(
                        "control command begin_shard_handoff requires from_node_id '{}' to own shard {} at ring_version {}",
                        from_node_id, shard, self.ring_version
                    ));
                }
                *transition = ShardOwnershipTransition {
                    shard,
                    from_node_id,
                    to_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress::bootstrap(unix_timestamp_millis()),
                };
                self.updated_unix_ms = unix_timestamp_millis();
                return Ok(ControlHandoffMutationOutcome::Applied);
            }
            if !from_is_current_owner {
                return Err(format!(
                    "control command begin_shard_handoff requires from_node_id '{}' to own shard {} at ring_version {}",
                    from_node_id, shard, self.ring_version
                ));
            }
            if transition.from_node_id != from_node_id
                || transition.to_node_id != to_node_id
                || transition.activation_ring_version != activation_ring_version
            {
                return Err(format!(
                    "control command begin_shard_handoff for shard {} conflicts with existing transition {} -> {} @ ring_version {}",
                    shard,
                    transition.from_node_id,
                    transition.to_node_id,
                    transition.activation_ring_version
                ));
            }
            let changed =
                transition
                    .handoff
                    .advance(ShardHandoffPhase::Warmup, None, None, None)?;
            if changed {
                self.updated_unix_ms = unix_timestamp_millis();
                return Ok(ControlHandoffMutationOutcome::Applied);
            }
            return Ok(ControlHandoffMutationOutcome::Noop);
        }
        if !from_is_current_owner {
            return Err(format!(
                "control command begin_shard_handoff requires from_node_id '{}' to own shard {} at ring_version {}",
                from_node_id, shard, self.ring_version
            ));
        }

        self.transitions.push(ShardOwnershipTransition {
            shard,
            from_node_id,
            to_node_id,
            activation_ring_version,
            handoff: ShardHandoffProgress::bootstrap(unix_timestamp_millis()),
        });
        sort_transitions(&mut self.transitions);
        self.updated_unix_ms = unix_timestamp_millis();
        Ok(ControlHandoffMutationOutcome::Applied)
    }

    pub fn apply_shard_handoff_progress(
        &mut self,
        shard: u32,
        phase: ShardHandoffPhase,
        copied_rows: Option<u64>,
        pending_rows: Option<u64>,
        last_error: Option<String>,
    ) -> Result<ControlHandoffMutationOutcome, String> {
        let Some(transition_index) = self.transitions.iter().position(|item| item.shard == shard)
        else {
            return Err(format!(
                "control command shard_handoff_progress references unknown shard transition {}",
                shard
            ));
        };

        let activation_ring_version = self.transitions[transition_index].activation_ring_version;
        let mut changed = self.transitions[transition_index].handoff.advance(
            phase,
            copied_rows,
            pending_rows,
            last_error,
        )?;
        if matches!(
            phase,
            ShardHandoffPhase::Cutover
                | ShardHandoffPhase::FinalSync
                | ShardHandoffPhase::Completed
        ) && self.ring_version < activation_ring_version
        {
            self.ring_version = activation_ring_version;
            changed = true;
        }
        if phase == ShardHandoffPhase::Completed {
            let transition = &self.transitions[transition_index];
            let Some(owners) = self.ring.assignments.get_mut(shard as usize) else {
                return Err(format!(
                    "control command shard_handoff_progress references unknown ring shard {}",
                    shard
                ));
            };
            if materialize_shard_owner(
                owners,
                transition.from_node_id.as_str(),
                transition.to_node_id.as_str(),
            ) {
                changed = true;
            }
        }
        if self.prune_stale_terminal_transitions() {
            changed = true;
        }
        if changed {
            self.updated_unix_ms = unix_timestamp_millis();
            return Ok(ControlHandoffMutationOutcome::Applied);
        }
        Ok(ControlHandoffMutationOutcome::Noop)
    }

    pub fn apply_complete_shard_handoff(
        &mut self,
        shard: u32,
    ) -> Result<ControlHandoffMutationOutcome, String> {
        self.apply_shard_handoff_progress(shard, ShardHandoffPhase::Completed, None, Some(0), None)
    }

    pub fn apply_join_node(
        &mut self,
        node_id: &str,
        endpoint: &str,
    ) -> Result<ControlMembershipMutationOutcome, String> {
        let node_id = normalize_non_empty_membership_field(node_id, "node_id", "join_node")?;
        let endpoint = normalize_non_empty_membership_field(endpoint, "endpoint", "join_node")?;

        let mut changed = false;
        if let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) {
            if node.endpoint != endpoint {
                if node.status != ControlNodeStatus::Removed {
                    return Err(format!(
                        "control command join_node endpoint mismatch for existing node '{}': '{}' != '{}'",
                        node_id, node.endpoint, endpoint
                    ));
                }
                node.endpoint = endpoint.clone();
                changed = true;
            }

            if !matches!(
                node.status,
                ControlNodeStatus::Joining | ControlNodeStatus::Active
            ) {
                node.status = ControlNodeStatus::Joining;
                changed = true;
            }

            if changed {
                node.membership_generation = bump_membership_generation(node.membership_generation);
            }
        } else {
            self.nodes.push(ControlNodeRecord {
                id: node_id,
                endpoint,
                membership_generation: next_membership_generation(&self.nodes),
                status: ControlNodeStatus::Joining,
            });
            changed = true;
        }

        if !changed {
            return Ok(ControlMembershipMutationOutcome::Noop);
        }

        self.membership_epoch = self.membership_epoch.saturating_add(1);
        sort_control_nodes(&mut self.nodes);
        Ok(ControlMembershipMutationOutcome::Applied)
    }

    pub fn apply_leave_node(
        &mut self,
        node_id: &str,
    ) -> Result<ControlMembershipMutationOutcome, String> {
        let node_id = normalize_non_empty_membership_field(node_id, "node_id", "leave_node")?;
        let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) else {
            return Err(format!(
                "control command leave_node references unknown node '{}'",
                node_id
            ));
        };

        if matches!(
            node.status,
            ControlNodeStatus::Leaving | ControlNodeStatus::Removed
        ) {
            return Ok(ControlMembershipMutationOutcome::Noop);
        }

        node.status = ControlNodeStatus::Leaving;
        node.membership_generation = bump_membership_generation(node.membership_generation);
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        Ok(ControlMembershipMutationOutcome::Applied)
    }

    pub fn apply_recommission_node(
        &mut self,
        node_id: &str,
        endpoint: Option<&str>,
    ) -> Result<ControlMembershipMutationOutcome, String> {
        let node_id =
            normalize_non_empty_membership_field(node_id, "node_id", "recommission_node")?;
        let endpoint = endpoint
            .map(|value| normalize_non_empty_membership_field(value, "endpoint", "recommission"))
            .transpose()?;

        let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) else {
            return Err(format!(
                "control command recommission_node references unknown node '{}'",
                node_id
            ));
        };

        let mut changed = false;
        if let Some(endpoint) = endpoint {
            if node.endpoint != endpoint {
                node.endpoint = endpoint;
                changed = true;
            }
        }

        if node.status != ControlNodeStatus::Active {
            node.status = ControlNodeStatus::Active;
            changed = true;
        }

        if !changed {
            return Ok(ControlMembershipMutationOutcome::Noop);
        }

        node.membership_generation = bump_membership_generation(node.membership_generation);
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        Ok(ControlMembershipMutationOutcome::Applied)
    }

    pub fn apply_activate_node(
        &mut self,
        node_id: &str,
    ) -> Result<ControlMembershipMutationOutcome, String> {
        let node_id = normalize_non_empty_membership_field(node_id, "node_id", "activate_node")?;
        let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) else {
            return Err(format!(
                "control command activate_node references unknown node '{}'",
                node_id
            ));
        };
        if node.status == ControlNodeStatus::Removed {
            return Err(format!(
                "control command activate_node cannot reactivate removed node '{}'; use join_node",
                node_id
            ));
        }
        if node.status == ControlNodeStatus::Active {
            return Ok(ControlMembershipMutationOutcome::Noop);
        }

        node.status = ControlNodeStatus::Active;
        node.membership_generation = bump_membership_generation(node.membership_generation);
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        Ok(ControlMembershipMutationOutcome::Applied)
    }

    pub fn apply_remove_node(
        &mut self,
        node_id: &str,
    ) -> Result<ControlMembershipMutationOutcome, String> {
        let node_id = normalize_non_empty_membership_field(node_id, "node_id", "remove_node")?;
        let Some(node_idx) = self.nodes.iter().position(|node| node.id == node_id) else {
            return Err(format!(
                "control command remove_node references unknown node '{}'",
                node_id
            ));
        };
        if self.nodes[node_idx].status == ControlNodeStatus::Removed {
            return Ok(ControlMembershipMutationOutcome::Noop);
        }
        if self.nodes[node_idx].status != ControlNodeStatus::Leaving {
            return Err(format!(
                "control command remove_node requires node '{}' to be in leaving state",
                node_id
            ));
        }
        if self
            .ring
            .assignments
            .iter()
            .any(|owners| owners.iter().any(|owner| owner == &node_id))
        {
            return Err(format!(
                "control command remove_node requires node '{}' to own no shards in the current ring",
                node_id
            ));
        }
        if self.transitions.iter().any(|transition| {
            transition.from_node_id == node_id || transition.to_node_id == node_id
        }) {
            return Err(format!(
                "control command remove_node requires node '{}' to have no in-flight shard transitions",
                node_id
            ));
        }

        self.nodes[node_idx].status = ControlNodeStatus::Removed;
        self.nodes[node_idx].membership_generation =
            bump_membership_generation(self.nodes[node_idx].membership_generation);
        if self.leader_node_id.as_deref() == Some(node_id.as_str()) {
            self.leader_node_id = None;
        }
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        Ok(ControlMembershipMutationOutcome::Applied)
    }

    fn prune_stale_terminal_transitions(&mut self) -> bool {
        let original_len = self.transitions.len();
        self.transitions.retain(|transition| {
            !transition.handoff.phase.is_terminal()
                || transition.activation_ring_version >= self.ring_version
        });
        original_len != self.transitions.len()
    }
}

fn normalize_non_empty_membership_field(
    value: &str,
    field_name: &str,
    command_name: &str,
) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "control command {command_name} has an empty {field_name}"
        ));
    }
    Ok(trimmed.to_string())
}

fn sort_control_nodes(nodes: &mut [ControlNodeRecord]) {
    nodes.sort_by(|a, b| {
        a.id.cmp(&b.id)
            .then_with(|| a.endpoint.cmp(&b.endpoint))
            .then_with(|| a.membership_generation.cmp(&b.membership_generation))
    });
}

fn next_membership_generation(nodes: &[ControlNodeRecord]) -> u64 {
    nodes
        .iter()
        .map(|node| node.membership_generation)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1)
}

fn bump_membership_generation(current: u64) -> u64 {
    if current == 0 {
        1
    } else {
        current.saturating_add(1)
    }
}

fn sort_transitions(transitions: &mut [ShardOwnershipTransition]) {
    transitions.sort_by(|left, right| {
        left.shard
            .cmp(&right.shard)
            .then_with(|| left.from_node_id.cmp(&right.from_node_id))
            .then_with(|| left.to_node_id.cmp(&right.to_node_id))
            .then_with(|| {
                left.activation_ring_version
                    .cmp(&right.activation_ring_version)
            })
    });
}

fn rewrite_shard_owner(owners: &mut Vec<String>, from_node_id: &str, to_node_id: &str) {
    if let Some(idx) = owners.iter().position(|owner| owner == from_node_id) {
        owners[idx] = to_node_id.to_string();
        return;
    }
    if !owners.iter().any(|owner| owner == to_node_id) {
        owners.push(to_node_id.to_string());
    }
}

fn dedupe_preserving_order(owners: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    owners.retain(|owner| seen.insert(owner.clone()));
}

fn materialize_shard_owner(owners: &mut Vec<String>, from_node_id: &str, to_node_id: &str) -> bool {
    let before = owners.clone();
    rewrite_shard_owner(owners, from_node_id, to_node_id);
    dedupe_preserving_order(owners);
    *owners != before
}

#[derive(Debug, Clone)]
pub struct ControlStateStore {
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct LoadedControlState {
    state: ControlState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlStateEnvelopeV1 {
    magic: String,
    schema_version: u16,
    state: ControlState,
}

impl ControlStateStore {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create control-state directory {}: {err}",
                    parent.display()
                )
            })?;
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub fn load(&self) -> Result<Option<ControlState>, String> {
        Ok(self.load_internal()?.map(|loaded| loaded.state))
    }

    pub fn load_or_bootstrap(&self, bootstrap_state: ControlState) -> Result<ControlState, String> {
        bootstrap_state.validate()?;

        if let Some(loaded) = self.load_internal()? {
            return Ok(loaded.state);
        }

        self.persist(&bootstrap_state)?;
        Ok(bootstrap_state)
    }

    pub fn persist(&self, state: &ControlState) -> Result<(), String> {
        state.validate()?;

        let envelope = ControlStateEnvelopeV1 {
            magic: CONTROL_STATE_MAGIC.to_string(),
            schema_version: CONTROL_STATE_SCHEMA_VERSION,
            state: state.clone(),
        };
        let mut encoded = serde_json::to_vec_pretty(&envelope)
            .map_err(|err| format!("failed to serialize control state: {err}"))?;
        encoded.push(b'\n');
        write_atomically(&self.path, &encoded)?;
        Ok(())
    }

    fn load_internal(&self) -> Result<Option<LoadedControlState>, String> {
        if !self.path.exists() {
            return Ok(None);
        }

        let raw = std::fs::read(&self.path).map_err(|err| {
            format!(
                "failed to read control-state file {}: {err}",
                self.path.display()
            )
        })?;

        let json: serde_json::Value = serde_json::from_slice(&raw).map_err(|err| {
            format!(
                "failed to parse control-state file {}: {err}",
                self.path.display()
            )
        })?;

        let magic = json
            .get("magic")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                format!(
                    "control-state file {} is missing 'magic' header",
                    self.path.display()
                )
            })?;
        if magic != CONTROL_STATE_MAGIC {
            return Err(format!(
                "control-state file {} has unsupported magic header '{}'",
                self.path.display(),
                magic
            ));
        }

        let schema_version = json
            .get("schemaVersion")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                format!(
                    "control-state file {} is missing numeric 'schemaVersion'",
                    self.path.display()
                )
            })?;
        if schema_version > u16::MAX as u64 {
            return Err(format!(
                "control-state file {} has schemaVersion {} which exceeds supported range",
                self.path.display(),
                schema_version
            ));
        }
        let schema_version = schema_version as u16;

        if schema_version != CONTROL_STATE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported control-state schema version {} in {} (expected {})",
                schema_version,
                self.path.display(),
                CONTROL_STATE_SCHEMA_VERSION
            ));
        }
        let mut state: ControlState = {
            let envelope: ControlStateEnvelopeV1 = serde_json::from_value(json).map_err(|err| {
                format!(
                    "failed to decode control-state schema v{} from {}: {err}",
                    CONTROL_STATE_SCHEMA_VERSION,
                    self.path.display()
                )
            })?;
            envelope.state
        };
        state.prune_stale_terminal_transitions();
        state.validate().map_err(|err| {
            format!(
                "control-state validation failed for {}: {err}",
                self.path.display()
            )
        })?;

        Ok(Some(LoadedControlState { state }))
    }
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
                "failed to open temporary control-state file {}: {err}",
                tmp_path.display()
            )
        })?;
    file.write_all(encoded).map_err(|err| {
        format!(
            "failed to write temporary control-state file {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to fsync temporary control-state file {}: {err}",
            tmp_path.display()
        )
    })?;
    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to replace control-state file {} with {}: {err}",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::membership::MembershipView;
    use crate::cluster::ring::{ShardRingSnapshot, CURRENT_RING_HASH_VERSION};
    use tempfile::TempDir;

    fn sample_membership_and_ring() -> (MembershipView, ShardRing) {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            shards: 32,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&config).expect("membership should build");
        let ring = ShardRing::build(32, 2, &membership).expect("ring should build");
        (membership, ring)
    }

    #[test]
    fn bootstrap_roundtrip_persists_and_recovers_state() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let state_path = temp_dir.path().join("cluster").join("control-state.json");
        let store = ControlStateStore::open(state_path.clone()).expect("store should open");
        let (membership, ring) = sample_membership_and_ring();

        let bootstrap = ControlState::from_runtime(&membership, &ring);
        let loaded = store
            .load_or_bootstrap(bootstrap.clone())
            .expect("bootstrap load should succeed");
        assert_eq!(loaded, bootstrap);

        let reopened = ControlStateStore::open(state_path).expect("store should reopen");
        let recovered = reopened
            .load()
            .expect("load should succeed")
            .expect("state should exist");
        assert_eq!(recovered, bootstrap);
    }

    #[test]
    fn load_rejects_legacy_schema_v0() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let state_path = temp_dir.path().join("cluster").join("control-state.json");
        let legacy = serde_json::json!({
            "magic": CONTROL_STATE_MAGIC,
            "schemaVersion": 0,
        });
        let mut encoded = serde_json::to_vec_pretty(&legacy).expect("legacy json should encode");
        encoded.push(b'\n');
        std::fs::create_dir_all(
            state_path
                .parent()
                .expect("control state path should have a parent"),
        )
        .expect("parent directory should create");
        std::fs::write(&state_path, encoded).expect("legacy file should write");

        let store = ControlStateStore::open(state_path).expect("store should open");
        let err = store.load().expect_err("legacy schema must fail");
        assert!(err.contains("unsupported control-state schema version 0"));
    }

    #[test]
    fn load_rejects_unsupported_schema_version() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let state_path = temp_dir.path().join("cluster").join("control-state.json");
        let file = serde_json::json!({
            "magic": CONTROL_STATE_MAGIC,
            "schemaVersion": 9
        });
        std::fs::create_dir_all(
            state_path
                .parent()
                .expect("control state path should have a parent"),
        )
        .expect("parent directory should create");
        std::fs::write(
            &state_path,
            serde_json::to_vec_pretty(&file).expect("json should encode"),
        )
        .expect("file should write");

        let store = ControlStateStore::open(state_path).expect("store should open");
        let err = store.load().expect_err("unsupported schema must fail");
        assert!(err.contains("unsupported control-state schema version"));
    }

    #[test]
    fn load_rejects_corrupt_control_record() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let state_path = temp_dir.path().join("cluster").join("control-state.json");
        std::fs::create_dir_all(
            state_path
                .parent()
                .expect("control state path should have a parent"),
        )
        .expect("parent directory should create");
        std::fs::write(&state_path, b"{not-json").expect("file should write");

        let store = ControlStateStore::open(state_path).expect("store should open");
        let err = store.load().expect_err("corrupt json must fail");
        assert!(err.contains("failed to parse control-state file"));
    }

    #[test]
    fn validate_rejects_ring_owner_missing_from_membership() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        state.ring.assignments[0][0] = "node-z".to_string();
        let err = state.validate().expect_err("validation should fail");
        assert!(err.contains("not present in membership"));
    }

    #[test]
    fn runtime_compatibility_rejects_ring_geometry_mismatch() {
        let (membership, ring) = sample_membership_and_ring();
        let state = ControlState::from_runtime(&membership, &ring);
        let mismatched_ring = ShardRing::build(64, 2, &membership).expect("ring should build");
        let err = state
            .ensure_runtime_compatible(&membership, &mismatched_ring)
            .expect_err("runtime mismatch must fail");
        assert!(err.contains("shard_count"));
    }

    #[test]
    fn runtime_compatibility_rejects_ring_hash_version_mismatch() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        state.ring.hash_version = CURRENT_RING_HASH_VERSION.saturating_add(1);

        let err = state
            .ensure_runtime_compatible(&membership, &ring)
            .expect_err("ring hash version mismatch must fail");
        assert!(err.contains("hash_version"));
        assert!(err.contains("unsupported"));
        assert!(err.contains("xxh64_seed0"));
    }

    #[test]
    fn runtime_compatibility_allows_additional_joined_nodes() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        state
            .apply_join_node("node-c", "127.0.0.1:9303")
            .expect("join should apply");

        state
            .ensure_runtime_compatible(&membership, &ring)
            .expect("joined control membership should still be runtime-compatible");
    }

    #[test]
    fn runtime_compatibility_rejects_local_node_endpoint_mismatch() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        let node = state
            .nodes
            .iter_mut()
            .find(|node| node.id == "node-a")
            .expect("node-a should exist");
        node.endpoint = "127.0.0.1:9999".to_string();

        let err = state
            .ensure_runtime_compatible(&membership, &ring)
            .expect_err("local endpoint mismatch must fail");
        assert!(err.contains("endpoint mismatch for local node 'node-a'"));
    }

    #[test]
    fn owners_for_shard_respects_transition_activation_barrier() {
        let state = ControlState {
            membership_epoch: 1,
            ring_version: 1,
            applied_log_index: 0,
            applied_log_term: 0,
            leader_node_id: None,
            nodes: vec![
                ControlNodeRecord {
                    id: "node-a".to_string(),
                    endpoint: "127.0.0.1:9301".to_string(),
                    membership_generation: 1,
                    status: ControlNodeStatus::Active,
                },
                ControlNodeRecord {
                    id: "node-b".to_string(),
                    endpoint: "127.0.0.1:9302".to_string(),
                    membership_generation: 1,
                    status: ControlNodeStatus::Active,
                },
            ],
            ring: ShardRingSnapshot {
                hash_version: CURRENT_RING_HASH_VERSION,
                shard_count: 1,
                replication_factor: 1,
                virtual_nodes_per_node: 1,
                assignments: vec![vec!["node-b".to_string()]],
            },
            transitions: vec![ShardOwnershipTransition {
                shard: 0,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version: 2,
                handoff: ShardHandoffProgress::default(),
            }],
            updated_unix_ms: 1,
        };
        state.validate().expect("state should validate");

        let before_cutover = state.owners_for_shard_at_ring_version(0, 1);
        let after_cutover = state.owners_for_shard_at_ring_version(0, 2);
        assert_eq!(before_cutover, vec!["node-a".to_string()]);
        assert_eq!(after_cutover, vec!["node-b".to_string()]);
    }

    #[test]
    fn node_is_owner_uses_effective_transition_owner() {
        let state = ControlState {
            membership_epoch: 1,
            ring_version: 1,
            applied_log_index: 0,
            applied_log_term: 0,
            leader_node_id: None,
            nodes: vec![
                ControlNodeRecord {
                    id: "node-a".to_string(),
                    endpoint: "127.0.0.1:9301".to_string(),
                    membership_generation: 1,
                    status: ControlNodeStatus::Active,
                },
                ControlNodeRecord {
                    id: "node-b".to_string(),
                    endpoint: "127.0.0.1:9302".to_string(),
                    membership_generation: 1,
                    status: ControlNodeStatus::Active,
                },
            ],
            ring: ShardRingSnapshot {
                hash_version: CURRENT_RING_HASH_VERSION,
                shard_count: 1,
                replication_factor: 1,
                virtual_nodes_per_node: 1,
                assignments: vec![vec!["node-b".to_string()]],
            },
            transitions: vec![ShardOwnershipTransition {
                shard: 0,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version: 2,
                handoff: ShardHandoffProgress::default(),
            }],
            updated_unix_ms: 1,
        };
        state.validate().expect("state should validate");

        assert!(state.node_is_owner_for_shard_at_ring_version(0, "node-a", 1));
        assert!(!state.node_is_owner_for_shard_at_ring_version(0, "node-b", 1));
        assert!(!state.node_is_owner_for_shard_at_ring_version(0, "node-a", 2));
        assert!(state.node_is_owner_for_shard_at_ring_version(0, "node-b", 2));
    }

    #[test]
    fn effective_ring_snapshot_uses_transition_owner_before_activation() {
        let state = ControlState {
            membership_epoch: 1,
            ring_version: 1,
            applied_log_index: 0,
            applied_log_term: 0,
            leader_node_id: None,
            nodes: vec![
                ControlNodeRecord {
                    id: "node-a".to_string(),
                    endpoint: "127.0.0.1:9301".to_string(),
                    membership_generation: 1,
                    status: ControlNodeStatus::Active,
                },
                ControlNodeRecord {
                    id: "node-b".to_string(),
                    endpoint: "127.0.0.1:9302".to_string(),
                    membership_generation: 1,
                    status: ControlNodeStatus::Active,
                },
            ],
            ring: ShardRingSnapshot {
                hash_version: CURRENT_RING_HASH_VERSION,
                shard_count: 1,
                replication_factor: 1,
                virtual_nodes_per_node: 1,
                assignments: vec![vec!["node-b".to_string()]],
            },
            transitions: vec![ShardOwnershipTransition {
                shard: 0,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version: 2,
                handoff: ShardHandoffProgress::default(),
            }],
            updated_unix_ms: 1,
        };
        state.validate().expect("state should validate");

        let effective_before = state.effective_ring_snapshot_at_ring_version(1);
        let effective_after = state.effective_ring_snapshot_at_ring_version(2);
        assert_eq!(effective_before.assignments[0], vec!["node-a".to_string()]);
        assert_eq!(effective_after.assignments[0], vec!["node-b".to_string()]);
    }

    #[test]
    fn membership_mutations_are_idempotent() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);

        let join = state
            .apply_join_node("node-c", "127.0.0.1:9303")
            .expect("join should succeed");
        assert_eq!(join, ControlMembershipMutationOutcome::Applied);
        state.validate().expect("joined state should validate");
        let epoch_after_join = state.membership_epoch;
        assert_eq!(
            state.node_record("node-c").map(|node| node.status),
            Some(ControlNodeStatus::Joining)
        );

        let join_noop = state
            .apply_join_node("node-c", "127.0.0.1:9303")
            .expect("idempotent join should succeed");
        assert_eq!(join_noop, ControlMembershipMutationOutcome::Noop);
        assert_eq!(state.membership_epoch, epoch_after_join);

        let leave = state
            .apply_leave_node("node-c")
            .expect("leave should succeed");
        assert_eq!(leave, ControlMembershipMutationOutcome::Applied);
        state.validate().expect("leaving state should validate");
        let epoch_after_leave = state.membership_epoch;
        assert_eq!(
            state.node_record("node-c").map(|node| node.status),
            Some(ControlNodeStatus::Leaving)
        );

        let leave_noop = state
            .apply_leave_node("node-c")
            .expect("idempotent leave should succeed");
        assert_eq!(leave_noop, ControlMembershipMutationOutcome::Noop);
        assert_eq!(state.membership_epoch, epoch_after_leave);

        let recommission = state
            .apply_recommission_node("node-c", None)
            .expect("recommission should succeed");
        assert_eq!(recommission, ControlMembershipMutationOutcome::Applied);
        state
            .validate()
            .expect("recommissioned state should validate");
        let epoch_after_recommission = state.membership_epoch;
        assert_eq!(
            state.node_record("node-c").map(|node| node.status),
            Some(ControlNodeStatus::Active)
        );

        let recommission_noop = state
            .apply_recommission_node("node-c", None)
            .expect("idempotent recommission should succeed");
        assert_eq!(recommission_noop, ControlMembershipMutationOutcome::Noop);
        assert_eq!(state.membership_epoch, epoch_after_recommission);
    }

    #[test]
    fn activate_and_remove_membership_transitions_are_supported() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);

        state
            .apply_join_node("node-c", "127.0.0.1:9303")
            .expect("join should succeed");
        assert_eq!(
            state.node_record("node-c").map(|node| node.status),
            Some(ControlNodeStatus::Joining)
        );

        state
            .apply_activate_node("node-c")
            .expect("activate should succeed");
        assert_eq!(
            state.node_record("node-c").map(|node| node.status),
            Some(ControlNodeStatus::Active)
        );

        state
            .apply_leave_node("node-c")
            .expect("leave should succeed");
        state
            .apply_remove_node("node-c")
            .expect("remove should succeed when node owns no shards");
        assert_eq!(
            state.node_record("node-c").map(|node| node.status),
            Some(ControlNodeStatus::Removed)
        );
        state.validate().expect("state should remain valid");
    }

    #[test]
    fn shard_handoff_state_machine_tracks_progress_and_resume() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        let base_ring_version = state.ring_version;
        let activation_ring_version = base_ring_version.saturating_add(1);

        let begin = state
            .apply_begin_shard_handoff(0, "node-a", "node-b", activation_ring_version)
            .expect("begin handoff should succeed");
        assert_eq!(begin, ControlHandoffMutationOutcome::Applied);
        assert_eq!(state.ring_version, base_ring_version);

        let initial_snapshot = state.handoff_snapshot();
        assert_eq!(initial_snapshot.total_shards, 1);
        assert_eq!(initial_snapshot.warmup_shards, 1);
        assert_eq!(initial_snapshot.in_progress_shards, 1);

        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Cutover, Some(120), Some(40), None)
            .expect("cutover should succeed");
        assert_eq!(state.ring_version, activation_ring_version);
        state
            .apply_shard_handoff_progress(
                0,
                ShardHandoffPhase::FinalSync,
                Some(180),
                Some(12),
                None,
            )
            .expect("final sync should succeed");
        state
            .apply_shard_handoff_progress(
                0,
                ShardHandoffPhase::Failed,
                Some(180),
                Some(12),
                Some("transient peer timeout".to_string()),
            )
            .expect("failed handoff state should be tracked");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Warmup, None, None, None)
            .expect("failed handoff should be resumable");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Cutover, Some(200), Some(8), None)
            .expect("resumed cutover should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::FinalSync, Some(225), Some(2), None)
            .expect("resumed final-sync should succeed");
        state
            .apply_complete_shard_handoff(0)
            .expect("completion should succeed");

        let snapshot = state.handoff_snapshot();
        assert_eq!(snapshot.total_shards, 1);
        assert_eq!(snapshot.completed_shards, 1);
        assert_eq!(snapshot.in_progress_shards, 0);
        assert_eq!(snapshot.resumed_shards, 1);
        assert_eq!(snapshot.copied_rows_total, 225);
        assert_eq!(snapshot.pending_rows_total, 0);
        assert_eq!(snapshot.shards[0].phase, ShardHandoffPhase::Completed);
        assert_eq!(snapshot.shards[0].resumed_count, 1);
        assert_eq!(state.ring_version, activation_ring_version);
    }

    #[test]
    fn shard_handoff_progress_persists_in_control_store() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let state_path = temp_dir.path().join("cluster").join("control-state.json");
        let store = ControlStateStore::open(state_path).expect("store should open");
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        let activation_ring_version = state.ring_version.saturating_add(1);

        state
            .apply_begin_shard_handoff(0, "node-a", "node-b", activation_ring_version)
            .expect("begin handoff should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Cutover, Some(75), Some(15), None)
            .expect("cutover should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::FinalSync, Some(90), Some(4), None)
            .expect("final sync should succeed");
        store.persist(&state).expect("state should persist");

        let recovered = store
            .load()
            .expect("load should succeed")
            .expect("state should exist");
        assert_eq!(recovered.ring_version, activation_ring_version);
        let snapshot = recovered.handoff_snapshot();
        assert_eq!(snapshot.total_shards, 1);
        assert_eq!(snapshot.final_sync_shards, 1);
        assert_eq!(snapshot.copied_rows_total, 90);
        assert_eq!(snapshot.pending_rows_total, 4);
    }

    #[test]
    fn shard_handoff_rejects_invalid_phase_transition() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        state
            .apply_begin_shard_handoff(0, "node-a", "node-b", state.ring_version.saturating_add(1))
            .expect("begin handoff should succeed");

        let err = state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Completed, None, None, None)
            .expect_err("warmup -> completed should be rejected");
        assert!(err.contains("invalid handoff phase transition"));
    }

    #[test]
    fn begin_handoff_rejects_when_source_is_not_current_owner() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        state
            .apply_join_node("node-c", "127.0.0.1:9303")
            .expect("join should succeed");

        let err = state
            .apply_begin_shard_handoff(0, "node-c", "node-a", state.ring_version.saturating_add(1))
            .expect_err("begin handoff should reject non-owner source node");
        assert!(err.contains("requires from_node_id 'node-c' to own shard 0"));
    }

    #[test]
    fn begin_handoff_restarts_completed_shard_transition() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);

        let first_activation = state.ring_version.saturating_add(1);
        state
            .apply_begin_shard_handoff(0, "node-a", "node-b", first_activation)
            .expect("first begin should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Cutover, Some(10), Some(2), None)
            .expect("first cutover should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::FinalSync, Some(12), Some(0), None)
            .expect("first final sync should succeed");
        state
            .apply_complete_shard_handoff(0)
            .expect("first completion should succeed");

        let second_activation = state.ring_version.saturating_add(1);
        let outcome = state
            .apply_begin_shard_handoff(0, "node-b", "node-a", second_activation)
            .expect("second begin should replace completed transition");
        assert_eq!(outcome, ControlHandoffMutationOutcome::Applied);
        state.validate().expect("state should remain valid");

        let transition = state
            .transitions
            .iter()
            .find(|item| item.shard == 0)
            .expect("replacement transition should exist");
        assert_eq!(transition.from_node_id, "node-b");
        assert_eq!(transition.to_node_id, "node-a");
        assert_eq!(transition.activation_ring_version, second_activation);
        assert_eq!(transition.handoff.phase, ShardHandoffPhase::Warmup);
    }

    #[test]
    fn ring_version_advance_prunes_older_completed_transitions() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);

        let first_activation = state.ring_version.saturating_add(1);
        state
            .apply_begin_shard_handoff(0, "node-a", "node-b", first_activation)
            .expect("first begin should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::Cutover, Some(15), Some(3), None)
            .expect("first cutover should succeed");
        state
            .apply_shard_handoff_progress(0, ShardHandoffPhase::FinalSync, Some(18), Some(0), None)
            .expect("first final sync should succeed");
        state
            .apply_complete_shard_handoff(0)
            .expect("first completion should succeed");
        assert!(
            state.ring.assignments[0]
                .iter()
                .any(|owner| owner == "node-b"),
            "completed handoff should materialize new owner into the base ring"
        );
        assert!(state
            .transitions
            .iter()
            .any(|item| item.shard == 0 && item.handoff.phase == ShardHandoffPhase::Completed));

        let second_activation = state.ring_version.saturating_add(1);
        state
            .apply_begin_shard_handoff(1, "node-a", "node-b", second_activation)
            .expect("second begin should succeed");
        state
            .apply_shard_handoff_progress(1, ShardHandoffPhase::Cutover, Some(20), Some(5), None)
            .expect("second cutover should succeed");

        state.validate().expect("state should remain valid");
        assert_eq!(state.ring_version, second_activation);
        assert!(
            state.transitions.iter().all(|item| item.shard != 0),
            "older completed transition should be pruned"
        );
        assert!(state.node_is_owner_for_shard_at_ring_version(0, "node-b", state.ring_version));
    }

    #[test]
    fn join_rejects_endpoint_mismatch_for_active_node() {
        let (membership, ring) = sample_membership_and_ring();
        let mut state = ControlState::from_runtime(&membership, &ring);
        let err = state
            .apply_join_node("node-a", "127.0.0.1:9999")
            .expect_err("join should reject endpoint mismatch for active node");
        assert!(err.contains("endpoint mismatch"));
    }
}
