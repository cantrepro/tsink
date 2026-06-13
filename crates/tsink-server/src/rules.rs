use crate::admission::{self, ReadAdmissionError, WriteAdmissionError};
use crate::cluster::control::{ControlNodeStatus, ControlState};
use crate::cluster::distributed_storage::{DistributedPromqlReadBridge, DistributedStorageAdapter};
use crate::cluster::membership::{ClusterNode, MembershipView};
use crate::cluster::query::ReadFanoutExecutor;
use crate::cluster::replication::WriteRouter;
use crate::cluster::ring::ShardRing;
use crate::cluster::ClusterRequestContext;
use crate::tenant;
use crate::usage::{UsageAccounting, UsageCategory, UsageRecordInput};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tsink::label::{
    canonical_series_identity_key, MAX_LABEL_NAME_LEN, MAX_LABEL_VALUE_LEN, MAX_METRIC_NAME_LEN,
};
use tsink::promql::types::{histogram_count_value, PromqlValue, Sample};
use tsink::promql::Engine;
use tsink::{DataPoint, Label, Row, Storage, TimestampPrecision, Value};

const RULES_STORE_FILE_NAME: &str = "rules-store.json";
const RULES_STORE_MAGIC: &str = "tsink-rules-store";
const RULES_STORE_SCHEMA_VERSION: u16 = 1;
const DEFAULT_GROUP_INTERVAL_SECS: u64 = 60;
const RULES_SCHEDULER_TICK_MS_ENV: &str = "TSINK_RULES_SCHEDULER_TICK_MS";
const RULES_MAX_RECORDING_ROWS_PER_EVAL_ENV: &str = "TSINK_RULES_MAX_RECORDING_ROWS_PER_EVAL";
const RULES_MAX_ALERT_INSTANCES_PER_RULE_ENV: &str = "TSINK_RULES_MAX_ALERT_INSTANCES_PER_RULE";
const DEFAULT_RULES_SCHEDULER_TICK_MS: u64 = 1_000;
const DEFAULT_MAX_RECORDING_ROWS_PER_EVAL: usize = 10_000;
const DEFAULT_MAX_ALERT_INSTANCES_PER_RULE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RulesRuntimeConfig {
    pub scheduler_tick: Duration,
    pub max_recording_rows_per_eval: usize,
    pub max_alert_instances_per_rule: usize,
}

impl Default for RulesRuntimeConfig {
    fn default() -> Self {
        Self {
            scheduler_tick: Duration::from_millis(DEFAULT_RULES_SCHEDULER_TICK_MS),
            max_recording_rows_per_eval: DEFAULT_MAX_RECORDING_ROWS_PER_EVAL,
            max_alert_instances_per_rule: DEFAULT_MAX_ALERT_INSTANCES_PER_RULE,
        }
    }
}

impl RulesRuntimeConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            scheduler_tick: Duration::from_millis(parse_env_u64(
                RULES_SCHEDULER_TICK_MS_ENV,
                DEFAULT_RULES_SCHEDULER_TICK_MS,
                true,
            )?),
            max_recording_rows_per_eval: parse_env_usize(
                RULES_MAX_RECORDING_ROWS_PER_EVAL_ENV,
                DEFAULT_MAX_RECORDING_ROWS_PER_EVAL,
                true,
            )?,
            max_alert_instances_per_rule: parse_env_usize(
                RULES_MAX_ALERT_INSTANCES_PER_RULE_ENV,
                DEFAULT_MAX_ALERT_INSTANCES_PER_RULE,
                true,
            )?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RulesApplyRequest {
    #[serde(default)]
    pub groups: Vec<RuleGroupInput>,
}

impl RulesApplyRequest {
    pub fn into_groups(self) -> Result<Vec<RuleGroupSpec>, String> {
        self.groups
            .into_iter()
            .map(RuleGroupInput::into_group_spec)
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuleGroupInput {
    pub name: String,
    pub tenant_id: String,
    #[serde(default)]
    pub interval: Option<DurationInput>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub rules: Vec<RuleInput>,
}

impl RuleGroupInput {
    fn into_group_spec(self) -> Result<RuleGroupSpec, String> {
        let interval_secs = self
            .interval
            .map(DurationInput::into_secs)
            .transpose()?
            .unwrap_or(DEFAULT_GROUP_INTERVAL_SECS);
        let rules = self
            .rules
            .into_iter()
            .map(RuleInput::into_rule_spec)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RuleGroupSpec {
            name: self.name,
            tenant_id: self.tenant_id,
            interval_secs,
            labels: self.labels,
            rules,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum RuleInput {
    Recording {
        record: String,
        expr: String,
        #[serde(default)]
        interval: Option<DurationInput>,
        #[serde(default)]
        labels: BTreeMap<String, String>,
    },
    Alert {
        alert: String,
        expr: String,
        #[serde(default)]
        interval: Option<DurationInput>,
        #[serde(default, rename = "for", alias = "forDuration")]
        for_duration: Option<DurationInput>,
        #[serde(default)]
        labels: BTreeMap<String, String>,
        #[serde(default)]
        annotations: BTreeMap<String, String>,
    },
}

impl RuleInput {
    fn into_rule_spec(self) -> Result<RuleSpec, String> {
        match self {
            Self::Recording {
                record,
                expr,
                interval,
                labels,
            } => Ok(RuleSpec::Recording(RecordingRuleSpec {
                record,
                expr,
                interval_secs: interval.map(DurationInput::into_secs).transpose()?,
                labels,
            })),
            Self::Alert {
                alert,
                expr,
                interval,
                for_duration,
                labels,
                annotations,
            } => Ok(RuleSpec::Alert(AlertRuleSpec {
                alert,
                expr,
                interval_secs: interval.map(DurationInput::into_secs).transpose()?,
                for_secs: for_duration
                    .map(DurationInput::into_secs)
                    .transpose()?
                    .unwrap_or(0),
                labels,
                annotations,
            })),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DurationInput {
    String(String),
    Integer(u64),
}

impl DurationInput {
    fn into_secs(self) -> Result<u64, String> {
        match self {
            Self::String(value) => parse_duration_secs(&value),
            Self::Integer(value) => {
                if value == 0 {
                    return Err("duration must be greater than zero".to_string());
                }
                Ok(value)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuleGroupSpec {
    pub name: String,
    pub tenant_id: String,
    pub interval_secs: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub rules: Vec<RuleSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RuleSpec {
    Recording(RecordingRuleSpec),
    Alert(AlertRuleSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RecordingRuleSpec {
    pub record: String,
    pub expr: String,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlertRuleSpec {
    pub alert: String,
    pub expr: String,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub for_secs: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AlertInstanceStatus {
    Pending,
    Firing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlertInstanceState {
    pub key: String,
    pub source_metric: String,
    #[serde(default)]
    pub labels: Vec<Label>,
    pub active_since_timestamp: i64,
    pub last_seen_timestamp: i64,
    #[serde(default)]
    pub firing_since_timestamp: Option<i64>,
    pub state: AlertInstanceStatus,
    #[serde(default)]
    pub sample_type: String,
    #[serde(default)]
    pub sample_value: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum RuleEvaluationOutcome {
    Success,
    Error,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedRuleRuntimeState {
    fingerprint: u64,
    #[serde(default)]
    last_eval_timestamp: Option<i64>,
    #[serde(default)]
    last_eval_unix_ms: Option<u64>,
    #[serde(default)]
    last_success_unix_ms: Option<u64>,
    #[serde(default)]
    last_duration_ms: u64,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    last_sample_count: u64,
    #[serde(default)]
    last_recorded_rows: u64,
    #[serde(default)]
    last_outcome: Option<RuleEvaluationOutcome>,
    #[serde(default)]
    alert_instances: Vec<AlertInstanceState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedRulesStoreState {
    #[serde(default)]
    groups: Vec<RuleGroupSpec>,
    #[serde(default)]
    runtime: BTreeMap<String, PersistedRuleRuntimeState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedRulesStore {
    magic: String,
    schema_version: u16,
    state: PersistedRulesStoreState,
}

struct RulesStore {
    path: Option<PathBuf>,
    state: RwLock<PersistedRulesStoreState>,
}

impl RulesStore {
    fn open(data_path: Option<&Path>) -> Result<Self, String> {
        let path = data_path.map(|path| path.join(RULES_STORE_FILE_NAME));
        let state = if let Some(path) = path.as_ref() {
            load_rules_store_state(path)?
        } else {
            PersistedRulesStoreState::default()
        };
        Ok(Self {
            path,
            state: RwLock::new(state),
        })
    }

    fn snapshot(&self) -> Result<PersistedRulesStoreState, String> {
        Ok(self.read_state()?.clone())
    }

    fn apply_groups(&self, groups: Vec<RuleGroupSpec>) -> Result<(), String> {
        validate_groups(&groups)?;
        let fingerprints = configured_rule_fingerprints(&groups)?;
        let mut state = self.write_state()?;
        let mut retained = BTreeMap::new();
        for (rule_id, fingerprint) in fingerprints {
            if let Some(existing) = state.runtime.get(&rule_id) {
                if existing.fingerprint == fingerprint {
                    retained.insert(rule_id, existing.clone());
                    continue;
                }
            }
            retained.insert(
                rule_id,
                PersistedRuleRuntimeState {
                    fingerprint,
                    ..PersistedRuleRuntimeState::default()
                },
            );
        }
        state.groups = groups;
        state.runtime = retained;
        self.persist_state(&state)
    }

    fn apply_runtime_updates(
        &self,
        updates: Vec<(String, PersistedRuleRuntimeState)>,
    ) -> Result<(), String> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut state = self.write_state()?;
        let configured = configured_rule_fingerprints(&state.groups)?;
        let mut changed = false;
        for (rule_id, runtime_state) in updates {
            let Some(expected_fingerprint) = configured.get(&rule_id).copied() else {
                continue;
            };
            if expected_fingerprint != runtime_state.fingerprint {
                continue;
            }
            state.runtime.insert(rule_id, runtime_state);
            changed = true;
        }
        if changed {
            self.persist_state(&state)?;
        }
        Ok(())
    }

    fn read_state(&self) -> Result<RwLockReadGuard<'_, PersistedRulesStoreState>, String> {
        self.state
            .read()
            .map_err(|_| "rules store read lock poisoned".to_string())
    }

    fn write_state(&self) -> Result<RwLockWriteGuard<'_, PersistedRulesStoreState>, String> {
        self.state
            .write()
            .map_err(|_| "rules store write lock poisoned".to_string())
    }

    fn persist_state(&self, state: &PersistedRulesStoreState) -> Result<(), String> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        write_rules_store_state(path, state)
    }

    fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), String> {
        let snapshot_file = snapshot_path.join(RULES_STORE_FILE_NAME);
        let state = self.read_state()?;
        write_rules_store_state(&snapshot_file, &state)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleStatusSnapshot {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub expr: String,
    pub interval_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_secs: Option<u64>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_eval_timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_eval_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix_ms: Option<u64>,
    pub last_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub last_sample_count: u64,
    pub last_recorded_rows: u64,
    pub state: String,
    #[serde(default)]
    pub alert_instances: Vec<AlertInstanceState>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleGroupStatusSnapshot {
    pub name: String,
    pub tenant_id: String,
    pub interval_secs: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub rules: Vec<RuleStatusSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RulesMetricsSnapshot {
    pub scheduler_runs_total: u64,
    pub scheduler_skipped_not_leader_total: u64,
    pub scheduler_skipped_inflight_total: u64,
    pub evaluated_rules_total: u64,
    pub evaluation_failures_total: u64,
    pub recording_rows_written_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub configured_groups: u64,
    pub configured_rules: u64,
    pub pending_alerts: u64,
    pub firing_alerts: u64,
    pub local_scheduler_active: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RulesStatusSnapshot {
    pub scheduler_tick_ms: u64,
    pub max_recording_rows_per_eval: usize,
    pub max_alert_instances_per_rule: usize,
    pub cluster_enabled: bool,
    pub cluster_leader: bool,
    pub metrics: RulesMetricsSnapshot,
    #[serde(default)]
    pub groups: Vec<RuleGroupStatusSnapshot>,
}

#[derive(Debug, Default)]
struct RulesRuntimeMetrics {
    scheduler_runs_total: u64,
    scheduler_skipped_not_leader_total: u64,
    scheduler_skipped_inflight_total: u64,
    evaluated_rules_total: u64,
    evaluation_failures_total: u64,
    recording_rows_written_total: u64,
    last_run_unix_ms: Option<u64>,
    last_error: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RulesRunTriggerError {
    AlreadyRunning,
    Snapshot(String),
}

pub struct RulesRuntime {
    store: Arc<RulesStore>,
    storage: Arc<dyn Storage>,
    precision: TimestampPrecision,
    cluster_context: Option<Arc<ClusterRequestContext>>,
    usage_accounting: Option<Arc<UsageAccounting>>,
    config: RulesRuntimeConfig,
    metrics: Arc<Mutex<RulesRuntimeMetrics>>,
    run_inflight: Arc<Mutex<bool>>,
}

impl RulesRuntime {
    pub fn open(
        data_path: Option<&Path>,
        storage: Arc<dyn Storage>,
        precision: TimestampPrecision,
        cluster_context: Option<Arc<ClusterRequestContext>>,
        usage_accounting: Option<Arc<UsageAccounting>>,
    ) -> Result<Arc<Self>, String> {
        Ok(Arc::new(Self {
            store: Arc::new(RulesStore::open(data_path)?),
            storage,
            precision,
            cluster_context,
            usage_accounting,
            config: RulesRuntimeConfig::from_env()?,
            metrics: Arc::new(Mutex::new(RulesRuntimeMetrics::default())),
            run_inflight: Arc::new(Mutex::new(false)),
        }))
    }

    pub fn start_worker(self: &Arc<Self>) -> JoinHandle<()> {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(runtime.config.scheduler_tick);
            loop {
                interval.tick().await;
                runtime.run_due_now().await;
            }
        })
    }

    pub fn apply_groups(&self, groups: Vec<RuleGroupSpec>) -> Result<RulesStatusSnapshot, String> {
        self.store.apply_groups(groups)?;
        self.snapshot()
    }

    pub fn snapshot(&self) -> Result<RulesStatusSnapshot, String> {
        let store = self.store.snapshot()?;
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cluster_leader = self.scheduler_enabled_here();
        let pending_alerts = store
            .runtime
            .values()
            .flat_map(|runtime| runtime.alert_instances.iter())
            .filter(|instance| instance.state == AlertInstanceStatus::Pending)
            .count() as u64;
        let firing_alerts = store
            .runtime
            .values()
            .flat_map(|runtime| runtime.alert_instances.iter())
            .filter(|instance| instance.state == AlertInstanceStatus::Firing)
            .count() as u64;
        let configured_groups = store.groups.len() as u64;
        let configured_rules = store
            .groups
            .iter()
            .map(|group| group.rules.len() as u64)
            .sum::<u64>();
        let group_snapshots = store
            .groups
            .iter()
            .map(|group| build_group_snapshot(group, &store.runtime))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RulesStatusSnapshot {
            scheduler_tick_ms: u64::try_from(self.config.scheduler_tick.as_millis())
                .unwrap_or(u64::MAX),
            max_recording_rows_per_eval: self.config.max_recording_rows_per_eval,
            max_alert_instances_per_rule: self.config.max_alert_instances_per_rule,
            cluster_enabled: self.cluster_context.is_some(),
            cluster_leader,
            metrics: RulesMetricsSnapshot {
                scheduler_runs_total: metrics.scheduler_runs_total,
                scheduler_skipped_not_leader_total: metrics.scheduler_skipped_not_leader_total,
                scheduler_skipped_inflight_total: metrics.scheduler_skipped_inflight_total,
                evaluated_rules_total: metrics.evaluated_rules_total,
                evaluation_failures_total: metrics.evaluation_failures_total,
                recording_rows_written_total: metrics.recording_rows_written_total,
                last_run_unix_ms: metrics.last_run_unix_ms,
                last_error: metrics.last_error.clone(),
                configured_groups,
                configured_rules,
                pending_alerts,
                firing_alerts,
                local_scheduler_active: cluster_leader,
            },
            groups: group_snapshots,
        })
    }

    pub fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), String> {
        self.store.snapshot_into(snapshot_path)
    }

    pub async fn trigger_run(&self) -> Result<RulesStatusSnapshot, RulesRunTriggerError> {
        let Some(_guard) = RunInflightGuard::try_acquire(&self.run_inflight) else {
            return Err(RulesRunTriggerError::AlreadyRunning);
        };
        self.run_due_at(current_timestamp(self.precision)).await;
        self.snapshot().map_err(RulesRunTriggerError::Snapshot)
    }

    async fn run_due_now(&self) {
        let Some(_guard) = RunInflightGuard::try_acquire(&self.run_inflight) else {
            let mut metrics = self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            metrics.scheduler_skipped_inflight_total =
                metrics.scheduler_skipped_inflight_total.saturating_add(1);
            return;
        };
        self.run_due_at(current_timestamp(self.precision)).await;
    }

    async fn run_due_at(&self, now_timestamp: i64) {
        let now_unix_ms = unix_timestamp_millis();
        {
            let mut metrics = self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            metrics.scheduler_runs_total = metrics.scheduler_runs_total.saturating_add(1);
            metrics.last_run_unix_ms = Some(now_unix_ms);
        }

        if !self.scheduler_enabled_here() {
            let mut metrics = self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            metrics.scheduler_skipped_not_leader_total =
                metrics.scheduler_skipped_not_leader_total.saturating_add(1);
            return;
        }

        let snapshot = match self.store.snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let mut metrics = self
                    .metrics
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                metrics.last_error = Some(format!("rules snapshot failed: {err}"));
                return;
            }
        };

        let mut updates = Vec::new();
        let mut evaluated_rules = 0u64;
        let mut evaluation_failures = 0u64;
        let mut recording_rows_written = 0u64;
        let mut last_error = None::<String>;

        for group in &snapshot.groups {
            for rule in &group.rules {
                let rule_id = rule_id(group, rule);
                let fingerprint = match rule_fingerprint(group, rule) {
                    Ok(fingerprint) => fingerprint,
                    Err(err) => {
                        evaluation_failures = evaluation_failures.saturating_add(1);
                        last_error = Some(err);
                        continue;
                    }
                };
                let previous =
                    snapshot
                        .runtime
                        .get(&rule_id)
                        .cloned()
                        .unwrap_or(PersistedRuleRuntimeState {
                            fingerprint,
                            ..PersistedRuleRuntimeState::default()
                        });
                let interval_secs = rule_interval_secs(group, rule);
                let interval = duration_units(interval_secs, self.precision);
                let aligned_eval_ts = align_eval_timestamp(now_timestamp, interval);
                if previous
                    .last_eval_timestamp
                    .is_some_and(|last| last >= aligned_eval_ts)
                {
                    continue;
                }

                evaluated_rules = evaluated_rules.saturating_add(1);
                let started = Instant::now();
                let result = self
                    .evaluate_rule(group, rule, aligned_eval_ts, previous.clone())
                    .await;
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                let operation = match rule {
                    RuleSpec::Recording(_) => "recording_rule",
                    RuleSpec::Alert(_) => "alert_rule",
                };
                match result {
                    Ok(mut state) => {
                        state.fingerprint = fingerprint;
                        state.last_duration_ms = duration_ms;
                        recording_rows_written =
                            recording_rows_written.saturating_add(state.last_recorded_rows);
                        if let Some(accounting) = self.usage_accounting.as_ref() {
                            let mut record = UsageRecordInput::success(
                                &group.tenant_id,
                                UsageCategory::Background,
                                operation,
                                "rules",
                            );
                            record.request_units = 1;
                            record.result_units = state.last_sample_count;
                            record.rows = state.last_recorded_rows;
                            record.duration_nanos = duration_ms.saturating_mul(1_000_000);
                            let _ = accounting.record(record);
                        }
                        updates.push((rule_id, state));
                    }
                    Err(err) => {
                        let mut state = previous;
                        state.fingerprint = fingerprint;
                        state.last_eval_timestamp = Some(aligned_eval_ts);
                        state.last_eval_unix_ms = Some(now_unix_ms);
                        state.last_duration_ms = duration_ms;
                        state.last_error = Some(err.clone());
                        state.last_outcome = Some(RuleEvaluationOutcome::Error);
                        updates.push((rule_id, state));
                        evaluation_failures = evaluation_failures.saturating_add(1);
                        if let Some(accounting) = self.usage_accounting.as_ref() {
                            let _ = accounting.record(UsageRecordInput {
                                tenant_id: &group.tenant_id,
                                category: UsageCategory::Background,
                                operation,
                                source: "rules",
                                status: "error",
                                request_units: 1,
                                result_units: 0,
                                rows: 0,
                                metadata_updates: 0,
                                exemplars_accepted: 0,
                                exemplars_dropped: 0,
                                histogram_series: 0,
                                matched_series: 0,
                                tombstones_applied: 0,
                                duration_nanos: duration_ms.saturating_mul(1_000_000),
                                request_bytes: 0,
                                logical_storage_series: 0,
                                logical_storage_samples: 0,
                                logical_storage_bytes: 0,
                            });
                        }
                        last_error = Some(err);
                    }
                }
            }
        }

        if let Err(err) = self.store.apply_runtime_updates(updates) {
            evaluation_failures = evaluation_failures.saturating_add(1);
            last_error = Some(format!("rules state persist failed: {err}"));
        }

        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        metrics.evaluated_rules_total = metrics
            .evaluated_rules_total
            .saturating_add(evaluated_rules);
        metrics.evaluation_failures_total = metrics
            .evaluation_failures_total
            .saturating_add(evaluation_failures);
        metrics.recording_rows_written_total = metrics
            .recording_rows_written_total
            .saturating_add(recording_rows_written);
        metrics.last_error = last_error;
    }

    async fn evaluate_rule(
        &self,
        group: &RuleGroupSpec,
        rule: &RuleSpec,
        eval_timestamp: i64,
        previous: PersistedRuleRuntimeState,
    ) -> Result<PersistedRuleRuntimeState, String> {
        let read_admission = admission::global_public_read_admission()
            .map_err(|err| format!("read admission unavailable: {err}"))?;
        let _read_lease = read_admission
            .admit_request(1)
            .await
            .map_err(format_read_admission_error)?;
        let read_storage = self.promql_storage_for_tenant(&group.tenant_id)?;
        let engine = Engine::with_precision(read_storage, self.precision);
        let expr = rule_expr(rule).to_string();
        let value =
            tokio::task::spawn_blocking(move || engine.instant_query(&expr, eval_timestamp))
                .await
                .map_err(|err| format!("rule query task failed: {err}"))?
                .map_err(|err| err.to_string())?;

        match rule {
            RuleSpec::Recording(spec) => {
                self.evaluate_recording_rule(group, spec, eval_timestamp, value)
                    .await
            }
            RuleSpec::Alert(spec) => {
                Ok(self.evaluate_alert_rule(group, spec, eval_timestamp, value, previous)?)
            }
        }
    }

    async fn evaluate_recording_rule(
        &self,
        group: &RuleGroupSpec,
        spec: &RecordingRuleSpec,
        eval_timestamp: i64,
        value: PromqlValue,
    ) -> Result<PersistedRuleRuntimeState, String> {
        let rows = recording_rows_from_value(group, spec, eval_timestamp, value)?;
        let scoped_rows = tenant::scope_rows_for_tenant(rows, &group.tenant_id)?;
        let rows_len = scoped_rows.len();
        if rows_len > self.config.max_recording_rows_per_eval {
            return Err(format!(
                "recording rule '{}' produced {} rows which exceeds the configured limit {}",
                spec.record, rows_len, self.config.max_recording_rows_per_eval
            ));
        }

        if rows_len > 0 {
            let write_admission = admission::global_public_write_admission()
                .map_err(|err| format!("write admission unavailable: {err}"))?;
            let request_slot = write_admission
                .acquire_request_slot()
                .await
                .map_err(format_write_admission_error)?;
            let _write_lease = write_admission
                .reserve_rows(request_slot, rows_len)
                .await
                .map_err(format_write_admission_error)?;
            if let Some(cluster_context) = self.cluster_context.as_ref() {
                let router = effective_write_router(cluster_context)?;
                let ring_version = current_cluster_ring_version(cluster_context);
                router
                    .route_and_write_with_consistency_and_ring_version(
                        &self.storage,
                        &cluster_context.rpc_client,
                        scoped_rows.clone(),
                        None,
                        ring_version,
                    )
                    .await
                    .map_err(|err| err.to_string())?;
            } else {
                let storage = Arc::clone(&self.storage);
                tokio::task::spawn_blocking(move || storage.insert_rows(&scoped_rows))
                    .await
                    .map_err(|err| format!("recording rule write task failed: {err}"))?
                    .map_err(|err| format!("recording rule write failed: {err}"))?;
            }
        }

        Ok(PersistedRuleRuntimeState {
            fingerprint: 0,
            last_eval_timestamp: Some(eval_timestamp),
            last_eval_unix_ms: Some(unix_timestamp_millis()),
            last_success_unix_ms: Some(unix_timestamp_millis()),
            last_duration_ms: 0,
            last_error: None,
            last_sample_count: rows_len as u64,
            last_recorded_rows: rows_len as u64,
            last_outcome: Some(RuleEvaluationOutcome::Success),
            alert_instances: Vec::new(),
        })
    }

    fn evaluate_alert_rule(
        &self,
        group: &RuleGroupSpec,
        spec: &AlertRuleSpec,
        eval_timestamp: i64,
        value: PromqlValue,
        previous: PersistedRuleRuntimeState,
    ) -> Result<PersistedRuleRuntimeState, String> {
        let samples = alert_samples_from_value(value)?;
        if samples.len() > self.config.max_alert_instances_per_rule {
            return Err(format!(
                "alert rule '{}' produced {} active alerts which exceeds the configured limit {}",
                spec.alert,
                samples.len(),
                self.config.max_alert_instances_per_rule
            ));
        }

        let previous_instances = previous
            .alert_instances
            .into_iter()
            .map(|instance| {
                (
                    alert_instance_key(&instance.source_metric, &instance.labels),
                    instance,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let for_units = duration_units(spec.for_secs, self.precision);
        let mut instances = Vec::with_capacity(samples.len());
        for sample in samples {
            let (labels, sample_type, sample_value) = alert_instance_fields(group, spec, &sample)?;
            let key = alert_instance_key(&sample.metric, &labels);
            let previous = previous_instances.get(&key);
            let active_since = previous
                .map(|instance| instance.active_since_timestamp)
                .unwrap_or(eval_timestamp);
            let firing = for_units == 0 || eval_timestamp.saturating_sub(active_since) >= for_units;
            let firing_since = if firing {
                previous
                    .and_then(|instance| instance.firing_since_timestamp)
                    .or(Some(eval_timestamp))
            } else {
                None
            };
            instances.push(AlertInstanceState {
                key,
                source_metric: sample.metric,
                labels,
                active_since_timestamp: active_since,
                last_seen_timestamp: eval_timestamp,
                firing_since_timestamp: firing_since,
                state: if firing {
                    AlertInstanceStatus::Firing
                } else {
                    AlertInstanceStatus::Pending
                },
                sample_type,
                sample_value,
            });
        }
        instances.sort_by(|left, right| left.key.cmp(&right.key));

        Ok(PersistedRuleRuntimeState {
            fingerprint: 0,
            last_eval_timestamp: Some(eval_timestamp),
            last_eval_unix_ms: Some(unix_timestamp_millis()),
            last_success_unix_ms: Some(unix_timestamp_millis()),
            last_duration_ms: 0,
            last_error: None,
            last_sample_count: instances.len() as u64,
            last_recorded_rows: 0,
            last_outcome: Some(RuleEvaluationOutcome::Success),
            alert_instances: instances,
        })
    }

    fn scheduler_enabled_here(&self) -> bool {
        let Some(cluster_context) = self.cluster_context.as_ref() else {
            return true;
        };
        cluster_context
            .control_consensus
            .as_ref()
            .map(|consensus| consensus.is_local_control_leader())
            .unwrap_or(true)
    }

    fn promql_storage_for_tenant(&self, tenant_id: &str) -> Result<Arc<dyn Storage>, String> {
        if let Some(cluster_context) = self.cluster_context.as_ref() {
            let ring_version = current_cluster_ring_version(cluster_context);
            let read_fanout = effective_read_fanout(cluster_context)?;
            // Rules evaluate PromQL inside `spawn_blocking`, so cluster reads use the same
            // dedicated sync-to-async bridge as the public PromQL handlers.
            let distributed_storage = Arc::new(DistributedStorageAdapter::new(
                Arc::clone(&self.storage),
                cluster_context.rpc_client.clone(),
                read_fanout,
                ring_version,
                DistributedPromqlReadBridge::from_current_runtime(),
            ));
            let storage: Arc<dyn Storage> = distributed_storage;
            Ok(tenant::scoped_storage(storage, tenant_id))
        } else {
            Ok(tenant::scoped_storage(Arc::clone(&self.storage), tenant_id))
        }
    }
}

fn build_group_snapshot(
    group: &RuleGroupSpec,
    runtime: &BTreeMap<String, PersistedRuleRuntimeState>,
) -> Result<RuleGroupStatusSnapshot, String> {
    let mut rules = Vec::with_capacity(group.rules.len());
    for rule in &group.rules {
        let rule_id = rule_id(group, rule);
        let state = runtime
            .get(&rule_id)
            .cloned()
            .unwrap_or_else(PersistedRuleRuntimeState::default);
        rules.push(RuleStatusSnapshot {
            id: rule_id,
            name: rule_name(rule).to_string(),
            kind: rule_kind(rule).to_string(),
            expr: rule_expr(rule).to_string(),
            interval_secs: rule_interval_secs(group, rule),
            for_secs: rule_for_secs(rule),
            labels: rule_labels(rule).clone(),
            annotations: rule_annotations(rule).clone(),
            last_eval_timestamp: state.last_eval_timestamp,
            last_eval_unix_ms: state.last_eval_unix_ms,
            last_success_unix_ms: state.last_success_unix_ms,
            last_duration_ms: state.last_duration_ms,
            last_error: state.last_error.clone(),
            last_sample_count: state.last_sample_count,
            last_recorded_rows: state.last_recorded_rows,
            state: summarize_rule_state(rule, &state),
            alert_instances: state.alert_instances,
        });
    }
    Ok(RuleGroupStatusSnapshot {
        name: group.name.clone(),
        tenant_id: group.tenant_id.clone(),
        interval_secs: group.interval_secs,
        labels: group.labels.clone(),
        rules,
    })
}

fn summarize_rule_state(rule: &RuleSpec, state: &PersistedRuleRuntimeState) -> String {
    if state.last_error.is_some()
        && matches!(state.last_outcome, Some(RuleEvaluationOutcome::Error))
    {
        return "error".to_string();
    }
    match rule {
        RuleSpec::Recording(_) => {
            if state.last_eval_timestamp.is_some() {
                "ok".to_string()
            } else {
                "inactive".to_string()
            }
        }
        RuleSpec::Alert(_) => {
            if state
                .alert_instances
                .iter()
                .any(|instance| instance.state == AlertInstanceStatus::Firing)
            {
                "firing".to_string()
            } else if state
                .alert_instances
                .iter()
                .any(|instance| instance.state == AlertInstanceStatus::Pending)
            {
                "pending".to_string()
            } else {
                "inactive".to_string()
            }
        }
    }
}

fn validate_groups(groups: &[RuleGroupSpec]) -> Result<(), String> {
    let mut seen_rule_ids = BTreeSet::new();
    for group in groups {
        if group.name.trim().is_empty() {
            return Err("rule group name must not be empty".to_string());
        }
        if group.interval_secs == 0 {
            return Err(format!(
                "rule group '{}' interval must be greater than zero",
                group.name
            ));
        }
        tenant::scope_rows_for_tenant(Vec::new(), &group.tenant_id)?;
        validate_rule_label_map(
            &group.labels,
            &format!("rule group '{}' labels", group.name),
            true,
        )?;
        if group.rules.is_empty() {
            return Err(format!(
                "rule group '{}' must contain at least one rule",
                group.name
            ));
        }
        for rule in &group.rules {
            let rule_id = rule_id(group, rule);
            if !seen_rule_ids.insert(rule_id.clone()) {
                return Err(format!("duplicate rule identifier '{rule_id}'"));
            }
            let context = format!("rule group '{}' rule '{}'", group.name, rule_name(rule));
            match rule {
                RuleSpec::Recording(spec) => {
                    validate_metric_name(&spec.record, &format!("{context} record name"))?;
                    validate_expr(&spec.expr, &format!("{context} expr"))?;
                    if spec.interval_secs == Some(0) {
                        return Err(format!("{context} interval must be greater than zero"));
                    }
                    validate_rule_label_map(&spec.labels, &format!("{context} labels"), true)?;
                }
                RuleSpec::Alert(spec) => {
                    validate_label_value(&spec.alert, &format!("{context} alert name"))?;
                    validate_expr(&spec.expr, &format!("{context} expr"))?;
                    if spec.interval_secs == Some(0) {
                        return Err(format!("{context} interval must be greater than zero"));
                    }
                    validate_rule_label_map(&spec.labels, &format!("{context} labels"), true)?;
                    validate_rule_label_map(
                        &spec.annotations,
                        &format!("{context} annotations"),
                        false,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_expr(expr: &str, context: &str) -> Result<(), String> {
    if expr.trim().is_empty() {
        return Err(format!("{context} must not be empty"));
    }
    tsink::promql::parse(expr)
        .map(|_| ())
        .map_err(|err| format!("{context} is invalid: {err}"))
}

fn validate_metric_name(metric: &str, context: &str) -> Result<(), String> {
    if metric.trim().is_empty() {
        return Err(format!("{context} must not be empty"));
    }
    if metric.len() > MAX_METRIC_NAME_LEN {
        return Err(format!("{context} must be <= {MAX_METRIC_NAME_LEN} bytes"));
    }
    Ok(())
}

fn validate_label_name(name: &str, context: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err(format!("{context} label name must not be empty"));
    }
    if name == tenant::TENANT_LABEL {
        return Err(format!(
            "{context} label '{}' is reserved for server-managed tenant isolation",
            tenant::TENANT_LABEL
        ));
    }
    if name == "__name__" {
        return Err(format!("{context} label '__name__' is not supported"));
    }
    if name.len() > MAX_LABEL_NAME_LEN {
        return Err(format!(
            "{context} label '{name}' exceeds the {MAX_LABEL_NAME_LEN}-byte name limit"
        ));
    }
    Ok(())
}

fn validate_label_value(value: &str, context: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{context} must not be empty"));
    }
    if value.len() > MAX_LABEL_VALUE_LEN {
        return Err(format!("{context} must be <= {MAX_LABEL_VALUE_LEN} bytes"));
    }
    Ok(())
}

fn validate_rule_label_map(
    labels: &BTreeMap<String, String>,
    context: &str,
    reject_reserved_labels: bool,
) -> Result<(), String> {
    for (name, value) in labels {
        if reject_reserved_labels {
            validate_label_name(name, context)?;
        } else if name.trim().is_empty() {
            return Err(format!("{context} label name must not be empty"));
        }
        validate_label_value(value, &format!("{context} label '{name}' value"))?;
    }
    Ok(())
}

fn configured_rule_fingerprints(groups: &[RuleGroupSpec]) -> Result<BTreeMap<String, u64>, String> {
    let mut out = BTreeMap::new();
    for group in groups {
        for rule in &group.rules {
            out.insert(rule_id(group, rule), rule_fingerprint(group, rule)?);
        }
    }
    Ok(out)
}

fn rule_fingerprint(group: &RuleGroupSpec, rule: &RuleSpec) -> Result<u64, String> {
    let encoded = serde_json::to_vec(&(group, rule))
        .map_err(|err| format!("failed to encode rule fingerprint: {err}"))?;
    Ok(fnv1a64(&encoded))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn rule_id(group: &RuleGroupSpec, rule: &RuleSpec) -> String {
    let kind = rule_kind(rule);
    format!(
        "{}/{}/{}/{}",
        group.tenant_id,
        group.name,
        kind,
        rule_name(rule)
    )
}

fn rule_name(rule: &RuleSpec) -> &str {
    match rule {
        RuleSpec::Recording(spec) => &spec.record,
        RuleSpec::Alert(spec) => &spec.alert,
    }
}

fn rule_kind(rule: &RuleSpec) -> &'static str {
    match rule {
        RuleSpec::Recording(_) => "recording",
        RuleSpec::Alert(_) => "alert",
    }
}

fn rule_expr(rule: &RuleSpec) -> &str {
    match rule {
        RuleSpec::Recording(spec) => &spec.expr,
        RuleSpec::Alert(spec) => &spec.expr,
    }
}

fn rule_interval_secs(group: &RuleGroupSpec, rule: &RuleSpec) -> u64 {
    match rule {
        RuleSpec::Recording(spec) => spec.interval_secs.unwrap_or(group.interval_secs),
        RuleSpec::Alert(spec) => spec.interval_secs.unwrap_or(group.interval_secs),
    }
}

fn rule_for_secs(rule: &RuleSpec) -> Option<u64> {
    match rule {
        RuleSpec::Recording(_) => None,
        RuleSpec::Alert(spec) => Some(spec.for_secs),
    }
}

fn rule_labels(rule: &RuleSpec) -> &BTreeMap<String, String> {
    match rule {
        RuleSpec::Recording(spec) => &spec.labels,
        RuleSpec::Alert(spec) => &spec.labels,
    }
}

fn rule_annotations(rule: &RuleSpec) -> &BTreeMap<String, String> {
    match rule {
        RuleSpec::Recording(_) => &EMPTY_MAP,
        RuleSpec::Alert(spec) => &spec.annotations,
    }
}

static EMPTY_MAP: BTreeMap<String, String> = BTreeMap::new();

fn recording_rows_from_value(
    group: &RuleGroupSpec,
    spec: &RecordingRuleSpec,
    eval_timestamp: i64,
    value: PromqlValue,
) -> Result<Vec<Row>, String> {
    match value {
        PromqlValue::Scalar(value, _) => Ok(vec![Row::with_labels(
            spec.record.clone(),
            merged_rule_labels(&[], &group.labels, &spec.labels)?,
            DataPoint::new(eval_timestamp, value),
        )]),
        PromqlValue::InstantVector(samples) => samples
            .into_iter()
            .map(|sample| recording_row_from_sample(group, spec, eval_timestamp, sample))
            .collect(),
        PromqlValue::RangeVector(_) => Err(format!(
            "recording rule '{}' must evaluate to a scalar or instant vector",
            spec.record
        )),
        PromqlValue::String(_, _) => Err(format!(
            "recording rule '{}' cannot record string results",
            spec.record
        )),
    }
}

fn recording_row_from_sample(
    group: &RuleGroupSpec,
    spec: &RecordingRuleSpec,
    eval_timestamp: i64,
    sample: Sample,
) -> Result<Row, String> {
    let labels = merged_rule_labels(&sample.labels, &group.labels, &spec.labels)?;
    let point = if let Some(histogram) = sample.histogram {
        DataPoint::new(eval_timestamp, Value::from(*histogram))
    } else {
        DataPoint::new(eval_timestamp, sample.value)
    };
    Ok(Row::with_labels(spec.record.clone(), labels, point))
}

fn alert_samples_from_value(value: PromqlValue) -> Result<Vec<Sample>, String> {
    match value {
        PromqlValue::Scalar(value, timestamp) => Ok(vec![Sample {
            metric: String::new(),
            labels: Vec::new(),
            timestamp,
            value,
            histogram: None,
        }]),
        PromqlValue::InstantVector(samples) => Ok(samples),
        PromqlValue::RangeVector(_) => {
            Err("alert rule must evaluate to a scalar or instant vector".to_string())
        }
        PromqlValue::String(_, _) => Err("alert rule cannot evaluate to a string".to_string()),
    }
}

fn alert_instance_fields(
    group: &RuleGroupSpec,
    spec: &AlertRuleSpec,
    sample: &Sample,
) -> Result<(Vec<Label>, String, Option<String>), String> {
    let mut merged = merged_rule_labels(&sample.labels, &group.labels, &spec.labels)?;
    merged.retain(|label| label.name != "alertname");
    merged.push(Label::new("alertname", spec.alert.clone()));
    merged.sort();
    let (sample_type, sample_value) = if let Some(histogram) = sample.histogram.as_deref() {
        (
            "histogram".to_string(),
            Some(histogram_count_value(histogram).to_string()),
        )
    } else {
        ("scalar".to_string(), Some(sample.value.to_string()))
    };
    Ok((merged, sample_type, sample_value))
}

fn merged_rule_labels(
    sample_labels: &[Label],
    group_labels: &BTreeMap<String, String>,
    rule_labels: &BTreeMap<String, String>,
) -> Result<Vec<Label>, String> {
    let mut merged = BTreeMap::new();
    for label in sample_labels {
        merged.insert(label.name.clone(), label.value.clone());
    }
    for (name, value) in group_labels {
        merged.insert(name.clone(), value.clone());
    }
    for (name, value) in rule_labels {
        merged.insert(name.clone(), value.clone());
    }
    let mut out = Vec::with_capacity(merged.len());
    for (name, value) in merged {
        validate_label_name(&name, "rule output")?;
        validate_label_value(&value, &format!("rule output label '{name}'"))?;
        out.push(Label::new(name, value));
    }
    out.sort();
    Ok(out)
}

fn alert_instance_key(metric: &str, labels: &[Label]) -> String {
    canonical_series_identity_key(metric, labels)
}

fn parse_duration_secs(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty duration value".to_string());
    }
    if let Ok(secs) = value.parse::<u64>() {
        if secs == 0 {
            return Err("duration must be greater than zero".to_string());
        }
        return Ok(secs);
    }
    let (num_str, unit) = if let Some(stripped) = value.strip_suffix("ms") {
        (stripped, "ms")
    } else if value.len() > 1 {
        (&value[..value.len() - 1], &value[value.len() - 1..])
    } else {
        return Err(format!("invalid duration: '{value}'"));
    };
    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration: '{value}'"))?;
    let secs = match unit {
        "ms" => num / 1_000.0,
        "s" => num,
        "m" => num * 60.0,
        "h" => num * 3_600.0,
        "d" => num * 86_400.0,
        "w" => num * 604_800.0,
        "y" => num * 365.25 * 86_400.0,
        _ => return Err(format!("invalid duration unit: '{unit}'")),
    };
    if !secs.is_finite() || secs <= 0.0 {
        return Err(format!("invalid duration: '{value}'"));
    }
    Ok(secs.ceil() as u64)
}

fn duration_units(secs: u64, precision: TimestampPrecision) -> i64 {
    match precision {
        TimestampPrecision::Seconds => secs as i64,
        TimestampPrecision::Milliseconds => secs.saturating_mul(1_000) as i64,
        TimestampPrecision::Microseconds => secs.saturating_mul(1_000_000) as i64,
        TimestampPrecision::Nanoseconds => secs.saturating_mul(1_000_000_000) as i64,
    }
    .max(1)
}

fn current_timestamp(precision: TimestampPrecision) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    match precision {
        TimestampPrecision::Seconds => now.as_secs() as i64,
        TimestampPrecision::Milliseconds => now.as_millis() as i64,
        TimestampPrecision::Microseconds => now.as_micros() as i64,
        TimestampPrecision::Nanoseconds => now.as_nanos() as i64,
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn align_eval_timestamp(timestamp: i64, interval: i64) -> i64 {
    if interval <= 1 {
        return timestamp;
    }
    timestamp - timestamp.rem_euclid(interval)
}

fn current_cluster_ring_version(cluster_context: &ClusterRequestContext) -> u64 {
    cluster_context
        .control_consensus
        .as_ref()
        .map(|consensus| consensus.current_state().ring_version)
        .unwrap_or(1)
        .max(1)
}

fn current_control_state(cluster_context: &ClusterRequestContext) -> Option<ControlState> {
    cluster_context
        .control_consensus
        .as_ref()
        .map(|consensus| consensus.current_state())
}

fn membership_from_control_state(
    cluster_context: &ClusterRequestContext,
    state: &ControlState,
) -> Result<MembershipView, String> {
    let local_node_id = cluster_context.runtime.membership.local_node_id.clone();
    let mut nodes = state
        .nodes
        .iter()
        .filter(|node| node.status != ControlNodeStatus::Removed)
        .map(|node| ClusterNode {
            id: node.id.clone(),
            endpoint: node.endpoint.clone(),
        })
        .collect::<Vec<_>>();
    nodes.sort();
    if !nodes.iter().any(|node| node.id == local_node_id) {
        return Err(format!(
            "local node '{local_node_id}' missing from control-state membership"
        ));
    }
    Ok(MembershipView {
        local_node_id,
        nodes,
    })
}

fn effective_write_router(cluster_context: &ClusterRequestContext) -> Result<WriteRouter, String> {
    let Some(state) = current_control_state(cluster_context) else {
        return Ok(cluster_context.write_router.clone());
    };
    let membership = membership_from_control_state(cluster_context, &state)?;
    let ring =
        ShardRing::from_snapshot(state.effective_ring_snapshot_at_ring_version(state.ring_version))
            .map_err(|err| {
                format!("failed to restore control-state ring for write routing: {err}")
            })?;
    cluster_context
        .write_router
        .reconfigured_for_topology(ring, &membership)
}

fn effective_read_fanout(
    cluster_context: &ClusterRequestContext,
) -> Result<ReadFanoutExecutor, String> {
    let Some(state) = current_control_state(cluster_context) else {
        return Ok(cluster_context.read_fanout.clone());
    };
    let membership = membership_from_control_state(cluster_context, &state)?;
    let ring =
        ShardRing::from_snapshot(state.effective_ring_snapshot_at_ring_version(state.ring_version))
            .map_err(|err| {
                format!("failed to restore control-state ring for read fanout: {err}")
            })?;
    cluster_context
        .read_fanout
        .reconfigured_for_topology(ring, &membership)
}

fn format_read_admission_error(err: ReadAdmissionError) -> String {
    err.to_string()
}

fn format_write_admission_error(err: WriteAdmissionError) -> String {
    err.to_string()
}

fn parse_env_u64(var: &str, default: u64, enforce_positive: bool) -> Result<u64, String> {
    match std::env::var(var) {
        Ok(value) => {
            let parsed = value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("{var} must be a positive integer"))?;
            if enforce_positive && parsed == 0 {
                return Err(format!("{var} must be greater than zero"));
            }
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{var} must be valid UTF-8")),
    }
}

fn parse_env_usize(var: &str, default: usize, enforce_positive: bool) -> Result<usize, String> {
    match std::env::var(var) {
        Ok(value) => {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("{var} must be a positive integer"))?;
            if enforce_positive && parsed == 0 {
                return Err(format!("{var} must be greater than zero"));
            }
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{var} must be valid UTF-8")),
    }
}

fn load_rules_store_state(path: &Path) -> Result<PersistedRulesStoreState, String> {
    if !path.exists() {
        return Ok(PersistedRulesStoreState::default());
    }
    let raw = std::fs::read(path)
        .map_err(|err| format!("failed to read rules store {}: {err}", path.display()))?;
    let persisted: PersistedRulesStore = serde_json::from_slice(&raw)
        .map_err(|err| format!("failed to parse rules store {}: {err}", path.display()))?;
    if persisted.magic != RULES_STORE_MAGIC {
        return Err(format!(
            "rules store {} has unsupported magic '{}'",
            path.display(),
            persisted.magic
        ));
    }
    if persisted.schema_version != RULES_STORE_SCHEMA_VERSION {
        return Err(format!(
            "rules store {} has unsupported schema version {}",
            path.display(),
            persisted.schema_version
        ));
    }
    Ok(persisted.state)
}

fn write_rules_store_state(path: &Path, state: &PersistedRulesStoreState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create rules store directory {}: {err}",
                parent.display()
            )
        })?;
    }
    let persisted = PersistedRulesStore {
        magic: RULES_STORE_MAGIC.to_string(),
        schema_version: RULES_STORE_SCHEMA_VERSION,
        state: state.clone(),
    };
    let mut encoded = serde_json::to_vec_pretty(&persisted)
        .map_err(|err| format!("failed to serialize rules store: {err}"))?;
    encoded.push(b'\n');
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|err| {
            format!(
                "failed to open temporary rules store {}: {err}",
                tmp_path.display()
            )
        })?;
    file.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write temporary rules store {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to sync temporary rules store {}: {err}",
            tmp_path.display()
        )
    })?;
    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to replace rules store {} from {}: {err}",
            path.display(),
            tmp_path.display()
        )
    })
}

pub fn empty_rules_snapshot() -> RulesStatusSnapshot {
    RulesStatusSnapshot {
        scheduler_tick_ms: DEFAULT_RULES_SCHEDULER_TICK_MS,
        max_recording_rows_per_eval: DEFAULT_MAX_RECORDING_ROWS_PER_EVAL,
        max_alert_instances_per_rule: DEFAULT_MAX_ALERT_INSTANCES_PER_RULE,
        cluster_enabled: false,
        cluster_leader: true,
        metrics: RulesMetricsSnapshot {
            scheduler_runs_total: 0,
            scheduler_skipped_not_leader_total: 0,
            scheduler_skipped_inflight_total: 0,
            evaluated_rules_total: 0,
            evaluation_failures_total: 0,
            recording_rows_written_total: 0,
            last_run_unix_ms: None,
            last_error: None,
            configured_groups: 0,
            configured_rules: 0,
            pending_alerts: 0,
            firing_alerts: 0,
            local_scheduler_active: true,
        },
        groups: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::{ClusterConfig, DEFAULT_CLUSTER_SHARDS};
    use crate::cluster::{ClusterRequestContext, ClusterRuntime};
    use tempfile::tempdir;
    use tsink::StorageBuilder;

    fn make_storage_with_path(path: &Path) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_data_path(path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
    }

    fn make_cluster_storage_with_path(path: &Path) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_data_path(path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn make_cluster_context() -> Arc<ClusterRequestContext> {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9311".to_string()),
            seeds: Vec::new(),
            internal_auth_token: Some("cluster-test-token".to_string()),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&cfg)
            .expect("cluster runtime should bootstrap")
            .expect("cluster runtime should exist");
        Arc::new(
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build"),
        )
    }

    #[tokio::test]
    async fn recording_rule_writes_rows_once_per_aligned_timestamp_and_persists_state() {
        let temp_dir = tempdir().expect("temp dir should build");
        let storage_path = temp_dir.path().join("data");
        let storage = make_storage_with_path(&storage_path);
        storage
            .insert_rows(&[Row::with_labels(
                "source_metric",
                vec![
                    Label::new("host", "a"),
                    Label::new(tenant::TENANT_LABEL, "team-a"),
                ],
                DataPoint::new(60_000, 2.5),
            )])
            .expect("seed write should succeed");

        let runtime = RulesRuntime::open(
            Some(&storage_path),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("runtime should open");
        runtime
            .apply_groups(vec![RuleGroupSpec {
                name: "recording".to_string(),
                tenant_id: "team-a".to_string(),
                interval_secs: 60,
                labels: BTreeMap::new(),
                rules: vec![RuleSpec::Recording(RecordingRuleSpec {
                    record: "recorded_metric".to_string(),
                    expr: "source_metric{host=\"a\"}".to_string(),
                    interval_secs: None,
                    labels: BTreeMap::new(),
                })],
            }])
            .expect("rule config should apply");

        runtime.run_due_at(60_000).await;
        runtime.run_due_at(60_001).await;

        let scoped = tenant::scoped_storage(Arc::clone(&storage), "team-a");
        let points = scoped
            .select("recorded_metric", &[Label::new("host", "a")], 0, 120_000)
            .expect("recorded points should exist");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, 60_000);
        assert_eq!(points[0].value_as_f64(), Some(2.5));

        drop(runtime);
        let reopened = RulesRuntime::open(
            Some(&storage_path),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("runtime should reopen");
        reopened.run_due_at(60_500).await;
        let points = scoped
            .select("recorded_metric", &[Label::new("host", "a")], 0, 120_000)
            .expect("recorded points should still exist");
        assert_eq!(points.len(), 1);
    }

    #[tokio::test]
    async fn rules_runtime_records_background_usage_per_tenant() {
        let temp_dir = tempdir().expect("temp dir should build");
        let storage_path = temp_dir.path().join("data");
        let storage = make_storage_with_path(&storage_path);
        let usage_accounting = UsageAccounting::open(None).expect("usage store should open");
        storage
            .insert_rows(&[Row::with_labels(
                "source_metric",
                vec![
                    Label::new("host", "a"),
                    Label::new(tenant::TENANT_LABEL, "team-a"),
                ],
                DataPoint::new(60_000, 5.0),
            )])
            .expect("seed write should succeed");

        let runtime = RulesRuntime::open(
            Some(&storage_path),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            Some(Arc::clone(&usage_accounting)),
        )
        .expect("runtime should open");
        runtime
            .apply_groups(vec![RuleGroupSpec {
                name: "recording".to_string(),
                tenant_id: "team-a".to_string(),
                interval_secs: 60,
                labels: BTreeMap::new(),
                rules: vec![RuleSpec::Recording(RecordingRuleSpec {
                    record: "recorded_metric".to_string(),
                    expr: "source_metric{host=\"a\"}".to_string(),
                    interval_secs: None,
                    labels: BTreeMap::new(),
                })],
            }])
            .expect("rule config should apply");

        runtime.run_due_at(60_000).await;

        let summary = usage_accounting.tenant_summary("team-a");
        assert_eq!(summary.background.events_total, 1);
        assert_eq!(summary.background.rows, 1);
        assert_eq!(summary.background.result_units, 1);
    }

    #[tokio::test]
    async fn recording_rule_evaluates_under_cluster_mode() {
        let temp_dir = tempdir().expect("temp dir should build");
        let storage_path = temp_dir.path().join("data");
        let storage = make_cluster_storage_with_path(&storage_path);
        storage
            .insert_rows(&[Row::with_labels(
                "source_metric",
                vec![
                    Label::new("host", "a"),
                    Label::new(tenant::TENANT_LABEL, "team-a"),
                ],
                DataPoint::new(60_000, 3.5),
            )])
            .expect("seed write should succeed");

        let runtime = RulesRuntime::open(
            Some(&storage_path),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            Some(make_cluster_context()),
            None,
        )
        .expect("runtime should open");
        runtime
            .apply_groups(vec![RuleGroupSpec {
                name: "recording".to_string(),
                tenant_id: "team-a".to_string(),
                interval_secs: 60,
                labels: BTreeMap::new(),
                rules: vec![RuleSpec::Recording(RecordingRuleSpec {
                    record: "recorded_metric".to_string(),
                    expr: "source_metric{host=\"a\"}".to_string(),
                    interval_secs: None,
                    labels: BTreeMap::new(),
                })],
            }])
            .expect("rule config should apply");

        runtime.run_due_at(60_000).await;

        let scoped = tenant::scoped_storage(Arc::clone(&storage), "team-a");
        let points = scoped
            .select("recorded_metric", &[Label::new("host", "a")], 0, 120_000)
            .expect("recorded points should exist");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, 60_000);
        assert_eq!(points[0].value_as_f64(), Some(3.5));
    }

    #[tokio::test]
    async fn alert_rule_tracks_pending_and_firing_then_clears() {
        let temp_dir = tempdir().expect("temp dir should build");
        let storage_path = temp_dir.path().join("data");
        let storage = make_storage_with_path(&storage_path);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("host", "a"),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(60_000, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("host", "a"),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(180_000, 0.0),
                ),
            ])
            .expect("seed writes should succeed");

        let runtime = RulesRuntime::open(
            Some(&storage_path),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("runtime should open");
        runtime
            .apply_groups(vec![RuleGroupSpec {
                name: "alerts".to_string(),
                tenant_id: "team-a".to_string(),
                interval_secs: 60,
                labels: BTreeMap::new(),
                rules: vec![RuleSpec::Alert(AlertRuleSpec {
                    alert: "HighCpu".to_string(),
                    expr: "cpu_usage{host=\"a\"} > 0.5".to_string(),
                    interval_secs: None,
                    for_secs: 60,
                    labels: BTreeMap::from([("severity".to_string(), "page".to_string())]),
                    annotations: BTreeMap::new(),
                })],
            }])
            .expect("alert config should apply");

        runtime.run_due_at(60_000).await;
        let pending = runtime.snapshot().expect("snapshot should load");
        assert_eq!(pending.groups[0].rules[0].state, "pending");
        assert_eq!(pending.groups[0].rules[0].alert_instances.len(), 1);
        assert_eq!(
            pending.groups[0].rules[0].alert_instances[0].state,
            AlertInstanceStatus::Pending
        );

        runtime.run_due_at(120_000).await;
        let firing = runtime.snapshot().expect("snapshot should load");
        assert_eq!(firing.groups[0].rules[0].state, "firing");
        assert_eq!(
            firing.groups[0].rules[0].alert_instances[0].state,
            AlertInstanceStatus::Firing
        );

        runtime.run_due_at(180_000).await;
        let cleared = runtime.snapshot().expect("snapshot should load");
        assert_eq!(cleared.groups[0].rules[0].state, "inactive");
        assert!(cleared.groups[0].rules[0].alert_instances.is_empty());
    }

    #[tokio::test]
    async fn alert_rule_tracks_delimiter_collision_instances_independently() {
        let temp_dir = tempdir().expect("temp dir should build");
        let storage_path = temp_dir.path().join("data");
        let storage = make_storage_with_path(&storage_path);
        let alert_delimiter_job = "api|zone=west";
        storage
            .insert_rows(&[
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("job", alert_delimiter_job),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(60_000, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("job", alert_delimiter_job),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(120_000, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("job", alert_delimiter_job),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(180_000, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("zone", "west"),
                        Label::new("job", "api"),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(60_000, 0.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("zone", "west"),
                        Label::new("job", "api"),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(120_000, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![
                        Label::new("zone", "west"),
                        Label::new("job", "api"),
                        Label::new(tenant::TENANT_LABEL, "team-a"),
                    ],
                    DataPoint::new(180_000, 1.0),
                ),
            ])
            .expect("seed writes should succeed");

        let runtime = RulesRuntime::open(
            Some(&storage_path),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("runtime should open");
        runtime
            .apply_groups(vec![RuleGroupSpec {
                name: "alerts".to_string(),
                tenant_id: "team-a".to_string(),
                interval_secs: 60,
                labels: BTreeMap::new(),
                rules: vec![RuleSpec::Alert(AlertRuleSpec {
                    alert: "HighCpu".to_string(),
                    expr: "cpu_usage > 0.5".to_string(),
                    interval_secs: None,
                    for_secs: 120,
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                })],
            }])
            .expect("alert config should apply");

        runtime.run_due_at(60_000).await;
        let first = runtime.snapshot().expect("snapshot should load");
        assert_eq!(first.groups[0].rules[0].state, "pending");
        assert_eq!(first.groups[0].rules[0].alert_instances.len(), 1);
        assert_eq!(
            first.groups[0].rules[0].alert_instances[0].active_since_timestamp,
            60_000
        );

        runtime.run_due_at(120_000).await;
        let second = runtime.snapshot().expect("snapshot should load");
        assert_eq!(second.groups[0].rules[0].state, "pending");
        assert_eq!(second.groups[0].rules[0].alert_instances.len(), 2);
        assert!(second.groups[0].rules[0]
            .alert_instances
            .iter()
            .all(|instance| instance.state == AlertInstanceStatus::Pending));

        runtime.run_due_at(180_000).await;
        let third = runtime.snapshot().expect("snapshot should load");
        let instances = &third.groups[0].rules[0].alert_instances;
        assert_eq!(third.groups[0].rules[0].state, "firing");
        assert_eq!(instances.len(), 2);

        let delimiter_instance = instances
            .iter()
            .find(|instance| {
                instance
                    .labels
                    .iter()
                    .any(|label| label.name == "job" && label.value == alert_delimiter_job)
            })
            .expect("delimiter instance should exist");
        assert_eq!(delimiter_instance.state, AlertInstanceStatus::Firing);
        assert_eq!(delimiter_instance.active_since_timestamp, 60_000);
        assert_eq!(delimiter_instance.firing_since_timestamp, Some(180_000));

        let plain_instance = instances
            .iter()
            .find(|instance| {
                instance
                    .labels
                    .iter()
                    .any(|label| label.name == "job" && label.value == "api")
            })
            .expect("plain instance should exist");
        assert_eq!(plain_instance.state, AlertInstanceStatus::Pending);
        assert_eq!(plain_instance.active_since_timestamp, 120_000);
        assert_eq!(plain_instance.firing_since_timestamp, None);
        assert!(plain_instance
            .labels
            .iter()
            .any(|label| label.name == "zone" && label.value == "west"));
    }

    #[test]
    fn alert_rule_recomputes_previous_instance_keys_from_labels() {
        fn legacy_alert_instance_key(metric: &str, labels: &[Label]) -> String {
            let mut key = metric.to_string();
            for label in labels {
                key.push('|');
                key.push_str(&label.name);
                key.push('=');
                key.push_str(&label.value);
            }
            key
        }

        let temp_dir = tempdir().expect("temp dir should build");
        let storage_path = temp_dir.path().join("data");
        let storage = make_storage_with_path(&storage_path);
        let runtime = RulesRuntime::open(
            Some(&storage_path),
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("runtime should open");
        let group = RuleGroupSpec {
            name: "alerts".to_string(),
            tenant_id: "team-a".to_string(),
            interval_secs: 60,
            labels: BTreeMap::new(),
            rules: Vec::new(),
        };
        let spec = AlertRuleSpec {
            alert: "HighCpu".to_string(),
            expr: "cpu_usage > 0.5".to_string(),
            interval_secs: None,
            for_secs: 120,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        };
        let previous_labels = vec![
            Label::new("alertname", "HighCpu"),
            Label::new("job", "api|zone=west"),
        ];
        let previous = PersistedRuleRuntimeState {
            fingerprint: 0,
            last_eval_timestamp: Some(120_000),
            last_eval_unix_ms: Some(120_000),
            last_success_unix_ms: Some(120_000),
            last_duration_ms: 0,
            last_error: None,
            last_sample_count: 1,
            last_recorded_rows: 0,
            last_outcome: Some(RuleEvaluationOutcome::Success),
            alert_instances: vec![AlertInstanceState {
                key: legacy_alert_instance_key("cpu_usage", &previous_labels),
                source_metric: "cpu_usage".to_string(),
                labels: previous_labels,
                active_since_timestamp: 60_000,
                last_seen_timestamp: 120_000,
                firing_since_timestamp: None,
                state: AlertInstanceStatus::Pending,
                sample_type: "scalar".to_string(),
                sample_value: Some("1".to_string()),
            }],
        };

        let state = runtime
            .evaluate_alert_rule(
                &group,
                &spec,
                180_000,
                PromqlValue::InstantVector(vec![Sample::from_float(
                    "cpu_usage".to_string(),
                    vec![Label::new("job", "api|zone=west")],
                    180_000,
                    1.0,
                )]),
                previous,
            )
            .expect("evaluation should succeed");

        assert_eq!(state.alert_instances.len(), 1);
        assert_eq!(state.alert_instances[0].state, AlertInstanceStatus::Firing);
        assert_eq!(state.alert_instances[0].active_since_timestamp, 60_000);
        assert_eq!(
            state.alert_instances[0].firing_since_timestamp,
            Some(180_000)
        );
        assert_ne!(
            state.alert_instances[0].key,
            "cpu_usage|alertname=HighCpu|job=api|zone=west"
        );
    }

    #[test]
    fn rules_apply_request_parses_duration_strings() {
        let request = RulesApplyRequest {
            groups: vec![RuleGroupInput {
                name: "example".to_string(),
                tenant_id: "team-a".to_string(),
                interval: Some(DurationInput::String("30s".to_string())),
                labels: BTreeMap::new(),
                rules: vec![RuleInput::Alert {
                    alert: "HighUsage".to_string(),
                    expr: "up == 0".to_string(),
                    interval: None,
                    for_duration: Some(DurationInput::String("5m".to_string())),
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                }],
            }],
        };
        let groups = request.into_groups().expect("request should parse");
        assert_eq!(groups[0].interval_secs, 30);
        match &groups[0].rules[0] {
            RuleSpec::Alert(spec) => assert_eq!(spec.for_secs, 300),
            other => panic!("expected alert rule, got {other:?}"),
        }
    }
}
