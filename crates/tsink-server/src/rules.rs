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
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tsink::label::{
    canonical_series_identity_key, MAX_LABEL_NAME_LEN, MAX_LABEL_VALUE_LEN, MAX_METRIC_NAME_LEN,
};
use tsink::promql::types::{histogram_count_value, PromqlValue, Sample};
use tsink::promql::Engine;
use tsink::{
    DataPoint, DiskCategory, Label, LocalDiskBudget, Row, Storage, TimestampPrecision, Value,
};

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
const RULES_ALLOCATION_ALLOWANCE_BYTES: usize = 64;
const RULES_STATUS_BTREE_ENTRY_ALLOWANCE_BYTES: usize = 1_024;
const RULES_STARTUP_MAX_JSON_DEPTH: usize = 64;
const RULES_MAX_DIAGNOSTIC_BYTES: usize = 512;
const RECORDING_RULE_ATTEMPT_PENDING: &str =
    "recording rule evaluation attempt checkpointed; final outcome pending";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RulesStoreLimits {
    pub max_groups: usize,
    pub max_rules_per_group: usize,
    pub max_rules_total: usize,
    pub max_alert_instances_per_rule: usize,
    pub max_labels_per_set: usize,
    pub max_label_set_bytes: usize,
    pub max_name_bytes: usize,
    pub max_expression_bytes: usize,
    pub max_annotation_bytes: usize,
    pub max_total_retained_state_bytes: usize,
    pub max_durable_file_bytes: usize,
    pub max_startup_transient_bytes: usize,
    pub max_replacement_transient_bytes: usize,
    pub max_runtime_update_transient_bytes: usize,
    pub max_snapshot_status_bytes: usize,
}

impl Default for RulesStoreLimits {
    fn default() -> Self {
        Self {
            max_groups: 256,
            max_rules_per_group: 256,
            max_rules_total: 4_096,
            max_alert_instances_per_rule: DEFAULT_MAX_ALERT_INSTANCES_PER_RULE,
            max_labels_per_set: 64,
            max_label_set_bytes: 64 * 1024,
            max_name_bytes: 1024,
            max_expression_bytes: 256 * 1024,
            max_annotation_bytes: 64 * 1024,
            max_total_retained_state_bytes: 16 * 1024 * 1024,
            max_durable_file_bytes: 32 * 1024 * 1024,
            max_startup_transient_bytes: 128 * 1024 * 1024,
            max_replacement_transient_bytes: 64 * 1024 * 1024,
            max_runtime_update_transient_bytes: 48 * 1024 * 1024,
            max_snapshot_status_bytes: 64 * 1024 * 1024,
        }
    }
}

impl RulesStoreLimits {
    pub fn validate(self) -> Result<Self, String> {
        let positive = [
            self.max_groups,
            self.max_rules_per_group,
            self.max_rules_total,
            self.max_alert_instances_per_rule,
            self.max_labels_per_set,
            self.max_label_set_bytes,
            self.max_name_bytes,
            self.max_expression_bytes,
            self.max_annotation_bytes,
            self.max_total_retained_state_bytes,
            self.max_durable_file_bytes,
            self.max_startup_transient_bytes,
            self.max_replacement_transient_bytes,
            self.max_runtime_update_transient_bytes,
            self.max_snapshot_status_bytes,
        ];
        if positive.contains(&0) {
            return Err("rules store limits must all be greater than zero".to_string());
        }
        if self.max_rules_per_group > self.max_rules_total {
            return Err("rules max_rules_per_group must not exceed max_rules_total".to_string());
        }
        if self.max_snapshot_status_bytes > crate::http::MAX_BODY_BYTES {
            return Err(
                "rules snapshot/status limit must not exceed the HTTP body limit".to_string(),
            );
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RulesRuntimeConfig {
    pub scheduler_tick: Duration,
    pub max_recording_rows_per_eval: usize,
    pub max_alert_instances_per_rule: usize,
    pub store_limits: RulesStoreLimits,
}

impl Default for RulesRuntimeConfig {
    fn default() -> Self {
        Self {
            scheduler_tick: Duration::from_millis(DEFAULT_RULES_SCHEDULER_TICK_MS),
            max_recording_rows_per_eval: DEFAULT_MAX_RECORDING_ROWS_PER_EVAL,
            max_alert_instances_per_rule: DEFAULT_MAX_ALERT_INSTANCES_PER_RULE,
            store_limits: RulesStoreLimits::default(),
        }
    }
}

impl RulesRuntimeConfig {
    pub fn from_env() -> Result<Self, String> {
        Self {
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
            store_limits: RulesStoreLimits::default(),
        }
        .validate()
    }

    pub fn validate(self) -> Result<Self, String> {
        if self.scheduler_tick.is_zero()
            || self.max_recording_rows_per_eval == 0
            || self.max_alert_instances_per_rule == 0
        {
            return Err("rules runtime limits must all be greater than zero".to_string());
        }
        self.store_limits.validate()?;
        if self.max_alert_instances_per_rule > self.store_limits.max_alert_instances_per_rule {
            return Err(
                "rules runtime alert limit exceeds the rules store alert limit".to_string(),
            );
        }
        Ok(self)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RulesLimitSurface {
    Configuration,
    RetainedState,
    DurableFile,
    Startup,
    Replacement,
    RuntimeUpdate,
    SnapshotStatus,
    SnapshotFile,
}

impl RulesLimitSurface {
    fn message(self) -> &'static str {
        match self {
            Self::Configuration => "rules configuration exceeds a finite store limit",
            Self::RetainedState => "rules retained state exceeds its finite byte limit",
            Self::DurableFile => "rules durable state exceeds its finite file limit",
            Self::Startup => "rules startup state exceeds its finite transient limit",
            Self::Replacement => "rules replacement exceeds its finite transient limit",
            Self::RuntimeUpdate => "rules runtime update exceeds its finite transient limit",
            Self::SnapshotStatus => "rules snapshot/status exceeds its finite output limit",
            Self::SnapshotFile => "rules snapshot file exceeds its finite transient limit",
        }
    }
}

#[derive(Debug)]
enum RulesStoreError {
    Limit(RulesLimitSurface),
    Invalid(&'static str),
    Persistence(tsink::TsinkError),
    Internal(&'static str),
}

impl std::fmt::Display for RulesStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(surface) => formatter.write_str(surface.message()),
            Self::Invalid(message) | Self::Internal(message) => formatter.write_str(message),
            Self::Persistence(_) => formatter.write_str("rules state persistence failed"),
        }
    }
}

impl std::error::Error for RulesStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Persistence(source) => Some(source),
            Self::Limit(_) | Self::Invalid(_) | Self::Internal(_) => None,
        }
    }
}

impl From<tsink::TsinkError> for RulesStoreError {
    fn from(error: tsink::TsinkError) -> Self {
        Self::Persistence(error)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RulesStoreAccountingSnapshot {
    retained_state_bytes: u64,
    peak_retained_state_bytes: u64,
    durable_file_bytes: u64,
    peak_startup_transient_bytes: u64,
    peak_replacement_transient_bytes: u64,
    peak_runtime_update_transient_bytes: u64,
    peak_snapshot_status_bytes: u64,
    peak_snapshot_file_bytes: u64,
    limit_rejections_total: u64,
    startup_rejections_total: u64,
    replacement_rejections_total: u64,
    runtime_update_rejections_total: u64,
    snapshot_rejections_total: u64,
    persistence_failures_total: u64,
}

#[derive(Debug, Default)]
struct RulesStoreAccounting {
    state: Mutex<RulesStoreAccountingSnapshot>,
}

impl RulesStoreAccounting {
    fn snapshot(&self) -> RulesStoreAccountingSnapshot {
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn initialize(&self, retained_state_bytes: usize, durable_file_bytes: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.retained_state_bytes = saturating_u64(retained_state_bytes);
        state.peak_retained_state_bytes = state
            .peak_retained_state_bytes
            .max(state.retained_state_bytes);
        state.durable_file_bytes = saturating_u64(durable_file_bytes);
    }

    fn publish_state(&self, retained_state_bytes: usize, durable_file_bytes: Option<usize>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.retained_state_bytes = saturating_u64(retained_state_bytes);
        state.peak_retained_state_bytes = state
            .peak_retained_state_bytes
            .max(state.retained_state_bytes);
        if let Some(durable_file_bytes) = durable_file_bytes {
            state.durable_file_bytes = saturating_u64(durable_file_bytes);
        }
    }

    fn observe_peak(&self, surface: RulesLimitSurface, bytes: usize) {
        let bytes = saturating_u64(bytes);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match surface {
            RulesLimitSurface::Startup => {
                state.peak_startup_transient_bytes = state.peak_startup_transient_bytes.max(bytes);
            }
            RulesLimitSurface::Replacement => {
                state.peak_replacement_transient_bytes =
                    state.peak_replacement_transient_bytes.max(bytes);
            }
            RulesLimitSurface::RuntimeUpdate => {
                state.peak_runtime_update_transient_bytes =
                    state.peak_runtime_update_transient_bytes.max(bytes);
            }
            RulesLimitSurface::SnapshotStatus => {
                state.peak_snapshot_status_bytes = state.peak_snapshot_status_bytes.max(bytes);
            }
            RulesLimitSurface::SnapshotFile => {
                state.peak_snapshot_file_bytes = state.peak_snapshot_file_bytes.max(bytes);
            }
            RulesLimitSurface::Configuration
            | RulesLimitSurface::RetainedState
            | RulesLimitSurface::DurableFile => {}
        }
    }

    fn reject(&self, surface: RulesLimitSurface) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.limit_rejections_total = state.limit_rejections_total.saturating_add(1);
        match surface {
            RulesLimitSurface::Startup => {
                state.startup_rejections_total = state.startup_rejections_total.saturating_add(1);
            }
            RulesLimitSurface::Replacement
            | RulesLimitSurface::Configuration
            | RulesLimitSurface::RetainedState
            | RulesLimitSurface::DurableFile => {
                state.replacement_rejections_total =
                    state.replacement_rejections_total.saturating_add(1);
            }
            RulesLimitSurface::RuntimeUpdate => {
                state.runtime_update_rejections_total =
                    state.runtime_update_rejections_total.saturating_add(1);
            }
            RulesLimitSurface::SnapshotStatus | RulesLimitSurface::SnapshotFile => {
                state.snapshot_rejections_total = state.snapshot_rejections_total.saturating_add(1);
            }
        }
    }

    fn persistence_failure(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.persistence_failures_total = state.persistence_failures_total.saturating_add(1);
    }

    fn observe_snapshot_file_peak(&self, bytes: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.peak_snapshot_file_bytes = state.peak_snapshot_file_bytes.max(saturating_u64(bytes));
    }
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn modeled_allocation_bytes(payload_bytes: usize) -> usize {
    if payload_bytes == 0 {
        0
    } else {
        payload_bytes.saturating_add(RULES_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_vec_len_bytes<T>(len: usize) -> usize {
    modeled_allocation_bytes(len.saturating_mul(std::mem::size_of::<T>()))
}

fn modeled_owned_string_bytes(value: &String) -> usize {
    modeled_allocation_bytes(value.capacity())
}

fn modeled_owned_vec_bytes<T>(value: &Vec<T>) -> usize {
    modeled_allocation_bytes(value.capacity().saturating_mul(std::mem::size_of::<T>()))
}

fn modeled_label_heap_bytes(label: &Label) -> usize {
    modeled_owned_string_bytes(&label.name).saturating_add(modeled_owned_string_bytes(&label.value))
}

fn modeled_labels_bytes(labels: &Vec<Label>) -> usize {
    modeled_owned_vec_bytes(labels).saturating_add(labels.iter().fold(0usize, |bytes, label| {
        bytes.saturating_add(modeled_label_heap_bytes(label))
    }))
}

fn modeled_string_map_bytes(values: &BTreeMap<String, String>) -> usize {
    let node_inline = std::mem::size_of::<String>()
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(4));
    values.iter().fold(0usize, |bytes, (name, value)| {
        bytes
            .saturating_add(modeled_allocation_bytes(node_inline))
            .saturating_add(modeled_owned_string_bytes(name))
            .saturating_add(modeled_owned_string_bytes(value))
    })
}

fn modeled_rule_heap_bytes(rule: &RuleSpec) -> usize {
    match rule {
        RuleSpec::Recording(spec) => modeled_owned_string_bytes(&spec.record)
            .saturating_add(modeled_owned_string_bytes(&spec.expr))
            .saturating_add(modeled_string_map_bytes(&spec.labels)),
        RuleSpec::Alert(spec) => modeled_owned_string_bytes(&spec.alert)
            .saturating_add(modeled_owned_string_bytes(&spec.expr))
            .saturating_add(modeled_string_map_bytes(&spec.labels))
            .saturating_add(modeled_string_map_bytes(&spec.annotations)),
    }
}

fn modeled_group_heap_bytes(group: &RuleGroupSpec) -> usize {
    modeled_owned_string_bytes(&group.name)
        .saturating_add(modeled_owned_string_bytes(&group.tenant_id))
        .saturating_add(modeled_string_map_bytes(&group.labels))
        .saturating_add(modeled_owned_vec_bytes(&group.rules))
        .saturating_add(group.rules.iter().fold(0usize, |bytes, rule| {
            bytes.saturating_add(modeled_rule_heap_bytes(rule))
        }))
}

fn modeled_groups_bytes(groups: &Vec<RuleGroupSpec>) -> usize {
    modeled_owned_vec_bytes(groups).saturating_add(groups.iter().fold(0usize, |bytes, group| {
        bytes.saturating_add(modeled_group_heap_bytes(group))
    }))
}

fn modeled_alert_instance_heap_bytes(instance: &AlertInstanceState) -> usize {
    modeled_owned_string_bytes(&instance.key)
        .saturating_add(modeled_owned_string_bytes(&instance.source_metric))
        .saturating_add(modeled_labels_bytes(&instance.labels))
        .saturating_add(modeled_owned_string_bytes(&instance.sample_type))
        .saturating_add(
            instance
                .sample_value
                .as_ref()
                .map(modeled_owned_string_bytes)
                .unwrap_or(0),
        )
}

fn modeled_runtime_state_heap_bytes(state: &PersistedRuleRuntimeState) -> usize {
    state
        .last_error
        .as_ref()
        .map(modeled_owned_string_bytes)
        .unwrap_or(0)
        .saturating_add(modeled_owned_vec_bytes(&state.alert_instances))
        .saturating_add(
            state
                .alert_instances
                .iter()
                .fold(0usize, |bytes, instance| {
                    bytes.saturating_add(modeled_alert_instance_heap_bytes(instance))
                }),
        )
}

fn modeled_runtime_entry_bytes(rule_id: &String, state: &PersistedRuleRuntimeState) -> usize {
    modeled_runtime_map_node_bytes()
        .saturating_add(modeled_owned_string_bytes(rule_id))
        .saturating_add(modeled_runtime_state_heap_bytes(state))
}

fn modeled_runtime_map_node_bytes() -> usize {
    let node_inline = std::mem::size_of::<String>()
        .saturating_add(std::mem::size_of::<PersistedRuleRuntimeState>())
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(4));
    modeled_allocation_bytes(node_inline)
}

fn modeled_runtime_map_bytes(runtime: &BTreeMap<String, PersistedRuleRuntimeState>) -> usize {
    runtime.iter().fold(0usize, |bytes, (rule_id, state)| {
        bytes.saturating_add(modeled_runtime_entry_bytes(rule_id, state))
    })
}

fn modeled_rules_state_bytes(state: &PersistedRulesStoreState) -> usize {
    std::mem::size_of::<PersistedRulesStoreState>()
        .saturating_add(modeled_groups_bytes(&state.groups))
        .saturating_add(modeled_runtime_map_bytes(&state.runtime))
}

struct RulesStore {
    path: Option<PathBuf>,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    limits: RulesStoreLimits,
    accounting: RulesStoreAccounting,
    state: RwLock<PersistedRulesStoreState>,
}

impl RulesStore {
    #[cfg_attr(not(test), allow(dead_code))]
    fn open(data_path: Option<&Path>) -> Result<Self, String> {
        Self::open_with_disk_budget(data_path, None)
    }

    fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Self, String> {
        Self::open_with_limits(data_path, local_disk_budget, RulesStoreLimits::default())
    }

    fn open_with_limits(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
        limits: RulesStoreLimits,
    ) -> Result<Self, String> {
        let limits = limits.validate()?;
        let path = data_path.map(|path| path.join(RULES_STORE_FILE_NAME));
        match (path.as_deref(), local_disk_budget.as_ref()) {
            (Some(path), Some(budget)) => {
                budget
                    .cleanup_atomic_write_temps(path)
                    .map_err(|_| "failed to clean rules temporary files".to_string())?;
                budget
                    .validate_managed_file_path(path)
                    .map_err(|_| "failed to validate rules store path".to_string())?;
            }
            (None, Some(_)) => {
                return Err("rules cannot use a local disk budget without a data path".to_string())
            }
            _ => {}
        }
        let accounting = RulesStoreAccounting::default();
        let loaded = if let Some(path) = path.as_ref() {
            load_rules_store_state_bounded(path, &limits, &accounting)
                .map_err(|error| error.to_string())?
        } else {
            LoadedRulesStoreState {
                state: PersistedRulesStoreState::default(),
                durable_file_bytes: 0,
                startup_transient_bytes: 0,
            }
        };
        let retained_state_bytes = modeled_rules_state_bytes(&loaded.state);
        accounting.initialize(retained_state_bytes, loaded.durable_file_bytes);
        accounting.observe_peak(RulesLimitSurface::Startup, loaded.startup_transient_bytes);
        Ok(Self {
            path,
            local_disk_budget,
            limits,
            accounting,
            state: RwLock::new(loaded.state),
        })
    }

    fn snapshot(&self) -> Result<PersistedRulesStoreState, String> {
        let state = self.read_state()?;
        let retained_bytes = modeled_rules_state_bytes(&state);
        let peak_bytes = retained_bytes.saturating_mul(2);
        self.enforce_limit(
            RulesLimitSurface::SnapshotStatus,
            peak_bytes,
            self.limits.max_snapshot_status_bytes,
        )
        .map_err(|error| error.to_string())?;
        let snapshot = state.clone();
        self.enforce_limit(
            RulesLimitSurface::SnapshotStatus,
            retained_bytes.saturating_add(modeled_rules_state_bytes(&snapshot)),
            self.limits.max_snapshot_status_bytes,
        )
        .map_err(|error| error.to_string())?;
        Ok(snapshot)
    }

    fn apply_groups(&self, groups: Vec<RuleGroupSpec>) -> Result<(), RulesApplyError> {
        let mut state = self.write_state().map_err(RulesApplyError::Internal)?;
        let old_state_bytes = modeled_rules_state_bytes(&state);
        let rule_id_scratch_bytes = groups.iter().fold(0usize, |bytes, group| {
            group.rules.iter().fold(bytes, |bytes, rule| {
                let rule_id_len = group
                    .tenant_id
                    .len()
                    .saturating_add(group.name.len())
                    .saturating_add(rule_kind(rule).len())
                    .saturating_add(rule_name(rule).len())
                    .saturating_add(3);
                bytes
                    .saturating_add(modeled_allocation_bytes(
                        std::mem::size_of::<String>()
                            .saturating_add(std::mem::size_of::<usize>().saturating_mul(4)),
                    ))
                    .saturating_add(modeled_allocation_bytes(rule_id_len))
            })
        });
        self.enforce_limit(
            RulesLimitSurface::Replacement,
            old_state_bytes
                .saturating_add(modeled_groups_bytes(&groups))
                .saturating_add(rule_id_scratch_bytes),
            self.limits.max_replacement_transient_bytes,
        )
        .map_err(|error| RulesApplyError::Rejected(error.to_string()))?;
        if let Err(error) = validate_groups_with_limits(&groups, &self.limits) {
            if let RulesStoreError::Limit(surface) = error {
                self.accounting.reject(surface);
            }
            return Err(RulesApplyError::Rejected(error.to_string()));
        }
        let mut candidate_runtime_bytes = 0usize;
        let mut max_rule_id_scratch_bytes = 0usize;
        for group in &groups {
            for rule in &group.rules {
                let rule_id = rule_id(group, rule);
                max_rule_id_scratch_bytes =
                    max_rule_id_scratch_bytes.max(modeled_owned_string_bytes(&rule_id));
                let fingerprint =
                    rule_fingerprint(group, rule).map_err(RulesApplyError::Rejected)?;
                let default;
                let runtime_state = match state.runtime.get(&rule_id) {
                    Some(existing) if existing.fingerprint == fingerprint => existing,
                    _ => {
                        default = PersistedRuleRuntimeState {
                            fingerprint,
                            ..PersistedRuleRuntimeState::default()
                        };
                        &default
                    }
                };
                candidate_runtime_bytes = candidate_runtime_bytes
                    .saturating_add(modeled_runtime_entry_bytes(&rule_id, runtime_state));
            }
        }
        let candidate_state_upper = std::mem::size_of::<PersistedRulesStoreState>()
            .saturating_add(modeled_groups_bytes(&groups))
            .saturating_add(candidate_runtime_bytes);
        self.enforce_limit(
            RulesLimitSurface::RetainedState,
            candidate_state_upper,
            self.limits.max_total_retained_state_bytes,
        )
        .map_err(|error| RulesApplyError::Rejected(error.to_string()))?;
        self.enforce_limit(
            RulesLimitSurface::Replacement,
            old_state_bytes
                .saturating_add(candidate_state_upper)
                .saturating_add(max_rule_id_scratch_bytes),
            self.limits.max_replacement_transient_bytes,
        )
        .map_err(|error| RulesApplyError::Rejected(error.to_string()))?;

        let mut retained = BTreeMap::new();
        for group in &groups {
            for rule in &group.rules {
                let rule_id = rule_id(group, rule);
                let fingerprint =
                    rule_fingerprint(group, rule).map_err(RulesApplyError::Rejected)?;
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
        }
        let candidate = PersistedRulesStoreState {
            groups,
            runtime: retained,
        };
        let candidate_state_bytes = modeled_rules_state_bytes(&candidate);
        self.enforce_limit(
            RulesLimitSurface::RetainedState,
            candidate_state_bytes,
            self.limits.max_total_retained_state_bytes,
        )
        .map_err(|error| RulesApplyError::Rejected(error.to_string()))?;
        self.enforce_limit(
            RulesLimitSurface::Replacement,
            old_state_bytes.saturating_add(candidate_state_bytes),
            self.limits.max_replacement_transient_bytes,
        )
        .map_err(|error| RulesApplyError::Rejected(error.to_string()))?;
        let durable_file_bytes = self
            .persist_serializable_state(
                &candidate,
                RulesLimitSurface::Replacement,
                old_state_bytes.saturating_add(candidate_state_bytes),
            )
            .map_err(|error| match error {
                RulesStoreError::Persistence(source) => RulesApplyError::Persistence(source),
                other => RulesApplyError::Rejected(other.to_string()),
            })?;
        *state = candidate;
        self.accounting
            .publish_state(candidate_state_bytes, durable_file_bytes);
        Ok(())
    }

    fn apply_runtime_updates(
        &self,
        updates: Vec<(String, PersistedRuleRuntimeState)>,
    ) -> Result<usize, RulesStoreError> {
        if updates.is_empty() {
            return Ok(0);
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| RulesStoreError::Internal("rules store write lock poisoned"))?;
        let current_state_bytes = modeled_rules_state_bytes(&state);
        let update_input_bytes = modeled_owned_vec_bytes(&updates).saturating_add(
            updates.iter().fold(0usize, |bytes, (rule_id, runtime)| {
                bytes
                    .saturating_add(modeled_owned_string_bytes(rule_id))
                    .saturating_add(modeled_runtime_state_heap_bytes(runtime))
            }),
        );
        let accepted_node_upper = modeled_runtime_map_node_bytes().saturating_mul(updates.len());
        self.enforce_limit(
            RulesLimitSurface::RuntimeUpdate,
            current_state_bytes
                .saturating_add(update_input_bytes)
                .saturating_add(accepted_node_upper),
            self.limits.max_runtime_update_transient_bytes,
        )?;
        let mut accepted = BTreeMap::new();
        for (rule_id, runtime_state) in updates {
            let Some(existing) = state.runtime.get(&rule_id) else {
                continue;
            };
            if existing.fingerprint != runtime_state.fingerprint {
                continue;
            }
            validate_runtime_state_with_limits(&runtime_state, &self.limits).map_err(|error| {
                if let RulesStoreError::Limit(surface) = error {
                    self.accounting.reject(surface);
                }
                error
            })?;
            accepted.insert(rule_id, runtime_state);
        }
        if accepted.is_empty() {
            return Ok(0);
        }
        let accepted_bytes = accepted.iter().fold(0usize, |bytes, (rule_id, runtime)| {
            bytes.saturating_add(modeled_runtime_entry_bytes(rule_id, runtime))
        });
        let final_state_bytes = modeled_rules_state_bytes_with_runtime_overlay(&state, &accepted);
        self.enforce_limit(
            RulesLimitSurface::RuntimeUpdate,
            final_state_bytes,
            self.limits.max_total_retained_state_bytes,
        )?;
        let update_peak = current_state_bytes
            .saturating_add(accepted_bytes)
            .saturating_add(
                modeled_vec_len_bytes::<(String, PersistedRuleRuntimeState)>(accepted.len()),
            );
        self.enforce_limit(
            RulesLimitSurface::RuntimeUpdate,
            update_peak,
            self.limits.max_runtime_update_transient_bytes,
        )?;
        let overlay = PersistedRulesStateOverlay {
            groups: &state.groups,
            runtime: RuntimeStateOverlay {
                current: &state.runtime,
                replacements: &accepted,
            },
        };
        let durable_file_bytes = self.persist_serializable_state(
            &overlay,
            RulesLimitSurface::RuntimeUpdate,
            update_peak,
        )?;
        let applied = accepted.len();
        for (rule_id, runtime_state) in accepted {
            if let Some(existing) = state.runtime.get_mut(&rule_id) {
                *existing = runtime_state;
            }
        }
        self.accounting
            .publish_state(final_state_bytes, durable_file_bytes);
        Ok(applied)
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

    fn persist_serializable_state<S: Serialize>(
        &self,
        state: &S,
        transient_surface: RulesLimitSurface,
        resident_transient_bytes: usize,
    ) -> Result<Option<usize>, RulesStoreError> {
        let Some(path) = self.path.as_ref() else {
            return Ok(None);
        };
        let encoded_len = measure_rules_store_state(state)?;
        self.enforce_limit_for_operation(
            RulesLimitSurface::DurableFile,
            transient_surface,
            encoded_len,
            self.limits.max_durable_file_bytes,
        )?;
        self.enforce_limit(
            transient_surface,
            resident_transient_bytes.saturating_add(modeled_allocation_bytes(encoded_len)),
            match transient_surface {
                RulesLimitSurface::Replacement => self.limits.max_replacement_transient_bytes,
                RulesLimitSurface::RuntimeUpdate => self.limits.max_runtime_update_transient_bytes,
                RulesLimitSurface::SnapshotStatus => self.limits.max_snapshot_status_bytes,
                _ => self.limits.max_replacement_transient_bytes,
            },
        )?;
        let encoded = encode_rules_store_state_exact(state, encoded_len)?;
        self.enforce_limit(
            transient_surface,
            resident_transient_bytes.saturating_add(modeled_owned_vec_bytes(&encoded)),
            match transient_surface {
                RulesLimitSurface::Replacement => self.limits.max_replacement_transient_bytes,
                RulesLimitSurface::RuntimeUpdate => self.limits.max_runtime_update_transient_bytes,
                RulesLimitSurface::SnapshotStatus => self.limits.max_snapshot_status_bytes,
                _ => self.limits.max_replacement_transient_bytes,
            },
        )?;
        let result =
            write_encoded_rules_store_state(path, &encoded, self.local_disk_budget.as_ref());
        if let Err(error) = result {
            self.accounting.persistence_failure();
            return Err(RulesStoreError::Persistence(error));
        }
        Ok(Some(encoded_len))
    }

    fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), String> {
        let snapshot_file = snapshot_path.join(RULES_STORE_FILE_NAME);
        let state = self.read_state()?;
        let state_bytes = modeled_rules_state_bytes(&state);
        let encoded_len = measure_rules_store_state(&*state).map_err(|error| error.to_string())?;
        self.enforce_limit_for_operation(
            RulesLimitSurface::DurableFile,
            RulesLimitSurface::SnapshotFile,
            encoded_len,
            self.limits.max_durable_file_bytes,
        )
        .map_err(|error| error.to_string())?;
        self.accounting.observe_snapshot_file_peak(
            state_bytes.saturating_add(modeled_allocation_bytes(encoded_len)),
        );
        let encoded = encode_rules_store_state_exact(&*state, encoded_len)
            .map_err(|error| error.to_string())?;
        self.accounting.observe_snapshot_file_peak(
            state_bytes.saturating_add(modeled_owned_vec_bytes(&encoded)),
        );
        write_encoded_rules_store_state(&snapshot_file, &encoded, None).map_err(|_| {
            self.accounting.persistence_failure();
            "failed to write rules snapshot".to_string()
        })
    }

    fn enforce_limit(
        &self,
        surface: RulesLimitSurface,
        observed: usize,
        limit: usize,
    ) -> Result<(), RulesStoreError> {
        self.enforce_limit_for_operation(surface, surface, observed, limit)
    }

    fn enforce_limit_for_operation(
        &self,
        limit_surface: RulesLimitSurface,
        operation_surface: RulesLimitSurface,
        observed: usize,
        limit: usize,
    ) -> Result<(), RulesStoreError> {
        self.accounting.observe_peak(operation_surface, observed);
        if observed > limit {
            self.accounting.reject(operation_surface);
            return Err(RulesStoreError::Limit(limit_surface));
        }
        Ok(())
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
    pub retained_state_bytes: u64,
    pub peak_retained_state_bytes: u64,
    pub durable_file_bytes: u64,
    pub peak_startup_transient_bytes: u64,
    pub peak_replacement_transient_bytes: u64,
    pub peak_runtime_update_transient_bytes: u64,
    pub peak_snapshot_status_bytes: u64,
    pub peak_snapshot_file_bytes: u64,
    pub limit_rejections_total: u64,
    pub startup_rejections_total: u64,
    pub replacement_rejections_total: u64,
    pub runtime_update_rejections_total: u64,
    pub snapshot_rejections_total: u64,
    pub persistence_failures_total: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RulesStatusSnapshot {
    pub scheduler_tick_ms: u64,
    pub max_recording_rows_per_eval: usize,
    pub max_alert_instances_per_rule: usize,
    pub store_limits: RulesStoreLimits,
    pub cluster_enabled: bool,
    pub cluster_leader: bool,
    pub metrics: RulesMetricsSnapshot,
    #[serde(default)]
    pub groups: Vec<RuleGroupStatusSnapshot>,
}

#[derive(Debug)]
#[must_use = "dropping the rules status releases its query-memory reservation"]
pub(crate) struct AccountedRulesStatusSnapshot {
    snapshot: RulesStatusSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

impl std::ops::Deref for AccountedRulesStatusSnapshot {
    type Target = RulesStatusSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl AccountedRulesStatusSnapshot {
    #[cfg(test)]
    fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

#[derive(Debug)]
pub(crate) enum RulesStatusProjectionError {
    QueryBudget(tsink::QueryBudgetError),
    StoreReadPoisoned,
    StatusLimit,
    Serialization(&'static str),
}

impl std::fmt::Display for RulesStatusProjectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueryBudget(error) => write!(formatter, "{error}"),
            Self::StoreReadPoisoned => formatter.write_str("rules store read lock poisoned"),
            Self::StatusLimit => formatter.write_str(RulesLimitSurface::SnapshotStatus.message()),
            Self::Serialization(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for RulesStatusProjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QueryBudget(error) => Some(error),
            Self::StoreReadPoisoned | Self::StatusLimit | Self::Serialization(_) => None,
        }
    }
}

impl From<tsink::QueryBudgetError> for RulesStatusProjectionError {
    fn from(error: tsink::QueryBudgetError) -> Self {
        Self::QueryBudget(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RulesExpositionMetrics {
    pub scheduler_runs_total: u64,
    pub scheduler_skipped_not_leader_total: u64,
    pub scheduler_skipped_inflight_total: u64,
    pub evaluated_rules_total: u64,
    pub evaluation_failures_total: u64,
    pub recording_rows_written_total: u64,
    pub configured_groups: u64,
    pub configured_rules: u64,
    pub pending_alerts: u64,
    pub firing_alerts: u64,
    pub local_scheduler_active: bool,
    pub retained_state_bytes: u64,
    pub peak_retained_state_bytes: u64,
    pub durable_file_bytes: u64,
    pub peak_startup_transient_bytes: u64,
    pub peak_replacement_transient_bytes: u64,
    pub peak_runtime_update_transient_bytes: u64,
    pub peak_snapshot_status_bytes: u64,
    pub peak_snapshot_file_bytes: u64,
    pub limit_rejections_total: u64,
    pub startup_rejections_total: u64,
    pub replacement_rejections_total: u64,
    pub runtime_update_rejections_total: u64,
    pub snapshot_rejections_total: u64,
    pub persistence_failures_total: u64,
}

impl Default for RulesExpositionMetrics {
    fn default() -> Self {
        Self {
            scheduler_runs_total: 0,
            scheduler_skipped_not_leader_total: 0,
            scheduler_skipped_inflight_total: 0,
            evaluated_rules_total: 0,
            evaluation_failures_total: 0,
            recording_rows_written_total: 0,
            configured_groups: 0,
            configured_rules: 0,
            pending_alerts: 0,
            firing_alerts: 0,
            local_scheduler_active: true,
            retained_state_bytes: 0,
            peak_retained_state_bytes: 0,
            durable_file_bytes: 0,
            peak_startup_transient_bytes: 0,
            peak_replacement_transient_bytes: 0,
            peak_runtime_update_transient_bytes: 0,
            peak_snapshot_status_bytes: 0,
            peak_snapshot_file_bytes: 0,
            limit_rejections_total: 0,
            startup_rejections_total: 0,
            replacement_rejections_total: 0,
            runtime_update_rejections_total: 0,
            snapshot_rejections_total: 0,
            persistence_failures_total: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RulesExpositionSnapshot {
    pub scheduler_tick_ms: u64,
    pub max_recording_rows_per_eval: usize,
    pub max_alert_instances_per_rule: usize,
    pub store_limits: RulesStoreLimits,
    pub metrics: RulesExpositionMetrics,
}

impl Default for RulesExpositionSnapshot {
    fn default() -> Self {
        Self {
            scheduler_tick_ms: DEFAULT_RULES_SCHEDULER_TICK_MS,
            max_recording_rows_per_eval: DEFAULT_MAX_RECORDING_ROWS_PER_EVAL,
            max_alert_instances_per_rule: DEFAULT_MAX_ALERT_INSTANCES_PER_RULE,
            store_limits: RulesStoreLimits::default(),
            metrics: RulesExpositionMetrics::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum RulesExpositionError {
    QueryBudget(tsink::QueryBudgetError),
    StoreReadPoisoned,
}

impl std::fmt::Display for RulesExpositionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueryBudget(error) => write!(formatter, "{error}"),
            Self::StoreReadPoisoned => formatter.write_str("rules store read lock poisoned"),
        }
    }
}

impl std::error::Error for RulesExpositionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QueryBudget(error) => Some(error),
            Self::StoreReadPoisoned => None,
        }
    }
}

impl From<tsink::QueryBudgetError> for RulesExpositionError {
    fn from(error: tsink::QueryBudgetError) -> Self {
        Self::QueryBudget(error)
    }
}

#[derive(Serialize)]
struct RulesSuccessEnvelope<'a> {
    status: &'static str,
    data: &'a RulesStatusSnapshot,
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug)]
pub enum RulesApplyError {
    Rejected(String),
    Persistence(tsink::TsinkError),
    Internal(String),
}

impl std::fmt::Display for RulesApplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(detail) | Self::Internal(detail) => formatter.write_str(detail),
            Self::Persistence(source) => {
                write!(formatter, "rules state persistence failed: {source}")
            }
        }
    }
}

impl std::error::Error for RulesApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Persistence(source) => Some(source),
            Self::Rejected(_) | Self::Internal(_) => None,
        }
    }
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(
        data_path: Option<&Path>,
        storage: Arc<dyn Storage>,
        precision: TimestampPrecision,
        cluster_context: Option<Arc<ClusterRequestContext>>,
        usage_accounting: Option<Arc<UsageAccounting>>,
    ) -> Result<Arc<Self>, String> {
        Self::open_with_disk_budget(
            data_path,
            storage,
            precision,
            cluster_context,
            usage_accounting,
            None,
        )
    }

    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        storage: Arc<dyn Storage>,
        precision: TimestampPrecision,
        cluster_context: Option<Arc<ClusterRequestContext>>,
        usage_accounting: Option<Arc<UsageAccounting>>,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Arc<Self>, String> {
        Self::open_with_config(
            data_path,
            storage,
            precision,
            cluster_context,
            usage_accounting,
            local_disk_budget,
            RulesRuntimeConfig::from_env()?,
        )
    }

    pub fn open_with_config(
        data_path: Option<&Path>,
        storage: Arc<dyn Storage>,
        precision: TimestampPrecision,
        cluster_context: Option<Arc<ClusterRequestContext>>,
        usage_accounting: Option<Arc<UsageAccounting>>,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
        config: RulesRuntimeConfig,
    ) -> Result<Arc<Self>, String> {
        let config = config.validate()?;
        Ok(Arc::new(Self {
            store: Arc::new(RulesStore::open_with_limits(
                data_path,
                local_disk_budget,
                config.store_limits,
            )?),
            storage,
            precision,
            cluster_context,
            usage_accounting,
            config,
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

    pub fn apply_groups(
        &self,
        groups: Vec<RuleGroupSpec>,
    ) -> Result<RulesStatusSnapshot, RulesApplyError> {
        self.store.apply_groups(groups)?;
        self.snapshot().map_err(RulesApplyError::Internal)
    }

    /// Returns a bounded, caller-owned status snapshot.
    ///
    /// The returned allocations are no longer store-owned after this method returns; callers
    /// that serialize or retain multiple snapshots must enforce their own aggregate envelope.
    pub fn snapshot(&self) -> Result<RulesStatusSnapshot, String> {
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let store = self.store.read_state()?;
        let output_upper = modeled_rules_status_output_upper_bytes(&store, &metrics);
        self.store
            .enforce_limit(
                RulesLimitSurface::SnapshotStatus,
                output_upper,
                self.config.store_limits.max_snapshot_status_bytes,
            )
            .map_err(|error| error.to_string())?;
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
        let store_accounting = self.store.accounting.snapshot();
        let mut snapshot = RulesStatusSnapshot {
            scheduler_tick_ms: u64::try_from(self.config.scheduler_tick.as_millis())
                .unwrap_or(u64::MAX),
            max_recording_rows_per_eval: self.config.max_recording_rows_per_eval,
            max_alert_instances_per_rule: self.config.max_alert_instances_per_rule,
            store_limits: self.config.store_limits,
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
                retained_state_bytes: store_accounting.retained_state_bytes,
                peak_retained_state_bytes: store_accounting.peak_retained_state_bytes,
                durable_file_bytes: store_accounting.durable_file_bytes,
                peak_startup_transient_bytes: store_accounting.peak_startup_transient_bytes,
                peak_replacement_transient_bytes: store_accounting.peak_replacement_transient_bytes,
                peak_runtime_update_transient_bytes: store_accounting
                    .peak_runtime_update_transient_bytes,
                peak_snapshot_status_bytes: store_accounting.peak_snapshot_status_bytes,
                peak_snapshot_file_bytes: store_accounting.peak_snapshot_file_bytes,
                limit_rejections_total: store_accounting.limit_rejections_total,
                startup_rejections_total: store_accounting.startup_rejections_total,
                replacement_rejections_total: store_accounting.replacement_rejections_total,
                runtime_update_rejections_total: store_accounting.runtime_update_rejections_total,
                snapshot_rejections_total: store_accounting.snapshot_rejections_total,
                persistence_failures_total: store_accounting.persistence_failures_total,
            },
            groups: group_snapshots,
        };
        let actual_output_bytes = modeled_rules_status_actual_bytes(&snapshot);
        self.store
            .enforce_limit(
                RulesLimitSurface::SnapshotStatus,
                actual_output_bytes,
                self.config.store_limits.max_snapshot_status_bytes,
            )
            .map_err(|error| error.to_string())?;
        let latest_accounting = self.store.accounting.snapshot();
        snapshot.metrics.peak_snapshot_status_bytes = latest_accounting.peak_snapshot_status_bytes;
        snapshot.metrics.snapshot_rejections_total = latest_accounting.snapshot_rejections_total;
        snapshot.metrics.limit_rejections_total = latest_accounting.limit_rejections_total;
        Ok(snapshot)
    }

    /// Builds the complete rules status tree under the caller's query-memory budget.
    ///
    /// Sampling intentionally matches `snapshot`: runtime metrics are captured first, followed by
    /// the later rules-store generation and its accounting counters. Every owned output byte is
    /// modeled and reserved before group, rule, label, diagnostic, or alert-instance cloning.
    pub(crate) fn status_snapshot_with_execution(
        &self,
        execution: &tsink::QueryExecution,
    ) -> Result<AccountedRulesStatusSnapshot, RulesStatusProjectionError> {
        execution.checkpoint()?;
        let metrics = {
            let metrics = self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let initial_bytes = metrics
                .last_error
                .as_deref()
                .map(modeled_string_clone_upper_bytes)
                .unwrap_or(0);
            let reservation = execution.reserve_memory(saturating_u64(initial_bytes))?;
            execution.checkpoint()?;
            (reservation, metrics.clone())
        };
        let (mut reservation, metrics) = metrics;
        execution.checkpoint()?;

        let store = self
            .store
            .state
            .read()
            .map_err(|_| RulesStatusProjectionError::StoreReadPoisoned)?;
        execution.checkpoint()?;
        let (legacy_output_upper, query_output_upper) =
            modeled_rules_status_output_upper_bytes_with_execution(&store, &metrics, execution)?;
        self.store
            .enforce_limit(
                RulesLimitSurface::SnapshotStatus,
                legacy_output_upper,
                self.config.store_limits.max_snapshot_status_bytes,
            )
            .map_err(|_| RulesStatusProjectionError::StatusLimit)?;
        reservation.resize(saturating_u64(query_output_upper))?;
        execution.checkpoint()?;

        let cluster_leader = self.scheduler_enabled_here();
        execution.checkpoint()?;
        let mut pending_alerts = 0u64;
        let mut firing_alerts = 0u64;
        for runtime in store.runtime.values() {
            execution.checkpoint()?;
            for instance in &runtime.alert_instances {
                execution.checkpoint()?;
                match instance.state {
                    AlertInstanceStatus::Pending => {
                        pending_alerts = pending_alerts.saturating_add(1);
                    }
                    AlertInstanceStatus::Firing => {
                        firing_alerts = firing_alerts.saturating_add(1);
                    }
                }
            }
        }
        let configured_groups = store.groups.len() as u64;
        let mut configured_rules = 0u64;
        for group in &store.groups {
            execution.checkpoint()?;
            configured_rules = configured_rules.saturating_add(group.rules.len() as u64);
        }
        let mut group_snapshots = Vec::with_capacity(store.groups.len());
        for group in &store.groups {
            execution.checkpoint()?;
            group_snapshots.push(build_group_snapshot_with_execution(
                group,
                &store.runtime,
                execution,
            )?);
        }
        let store_accounting = self.store.accounting.snapshot();
        execution.checkpoint()?;
        let mut snapshot = RulesStatusSnapshot {
            scheduler_tick_ms: u64::try_from(self.config.scheduler_tick.as_millis())
                .unwrap_or(u64::MAX),
            max_recording_rows_per_eval: self.config.max_recording_rows_per_eval,
            max_alert_instances_per_rule: self.config.max_alert_instances_per_rule,
            store_limits: self.config.store_limits,
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
                last_error: metrics.last_error,
                configured_groups,
                configured_rules,
                pending_alerts,
                firing_alerts,
                local_scheduler_active: cluster_leader,
                retained_state_bytes: store_accounting.retained_state_bytes,
                peak_retained_state_bytes: store_accounting.peak_retained_state_bytes,
                durable_file_bytes: store_accounting.durable_file_bytes,
                peak_startup_transient_bytes: store_accounting.peak_startup_transient_bytes,
                peak_replacement_transient_bytes: store_accounting.peak_replacement_transient_bytes,
                peak_runtime_update_transient_bytes: store_accounting
                    .peak_runtime_update_transient_bytes,
                peak_snapshot_status_bytes: store_accounting.peak_snapshot_status_bytes,
                peak_snapshot_file_bytes: store_accounting.peak_snapshot_file_bytes,
                limit_rejections_total: store_accounting.limit_rejections_total,
                startup_rejections_total: store_accounting.startup_rejections_total,
                replacement_rejections_total: store_accounting.replacement_rejections_total,
                runtime_update_rejections_total: store_accounting.runtime_update_rejections_total,
                snapshot_rejections_total: store_accounting.snapshot_rejections_total,
                persistence_failures_total: store_accounting.persistence_failures_total,
            },
            groups: group_snapshots,
        };
        let (legacy_actual_output_bytes, query_actual_output_bytes) =
            modeled_rules_status_actual_bytes_with_execution(&snapshot, execution)?;
        assert!(
            query_actual_output_bytes <= query_output_upper,
            "rules status retained-memory model exceeded its pre-allocation reservation"
        );
        self.store
            .enforce_limit(
                RulesLimitSurface::SnapshotStatus,
                legacy_actual_output_bytes,
                self.config.store_limits.max_snapshot_status_bytes,
            )
            .map_err(|_| RulesStatusProjectionError::StatusLimit)?;
        let latest_accounting = self.store.accounting.snapshot();
        execution.checkpoint()?;
        snapshot.metrics.peak_snapshot_status_bytes = latest_accounting.peak_snapshot_status_bytes;
        snapshot.metrics.snapshot_rejections_total = latest_accounting.snapshot_rejections_total;
        snapshot.metrics.limit_rejections_total = latest_accounting.limit_rejections_total;
        reservation.resize(saturating_u64(query_actual_output_bytes))?;
        drop(store);
        execution.checkpoint()?;

        Ok(AccountedRulesStatusSnapshot {
            snapshot,
            _reservation: reservation,
        })
    }

    /// Stabilizes the self-observing rules status counters without allocating the HTTP body.
    ///
    /// The direct endpoint reserves the same conservative encoded-vector upper bound before its
    /// exact allocation. Replaying that fixed-point loop here preserves its visible counters while
    /// the support-bundle serializer owns the separately query-accounted body allocation.
    pub(crate) fn prepare_success_snapshot_iteration_with_execution(
        &self,
        snapshot: &mut AccountedRulesStatusSnapshot,
        execution: &tsink::QueryExecution,
    ) -> Result<Option<usize>, RulesStatusProjectionError> {
        execution.checkpoint()?;
        let accounting = self.store.accounting.snapshot();
        execution.checkpoint()?;
        snapshot.snapshot.metrics.peak_snapshot_status_bytes =
            accounting.peak_snapshot_status_bytes;
        snapshot.snapshot.metrics.snapshot_rejections_total = accounting.snapshot_rejections_total;
        snapshot.snapshot.metrics.limit_rejections_total = accounting.limit_rejections_total;

        let (snapshot_bytes, _) =
            modeled_rules_status_actual_bytes_with_execution(&snapshot.snapshot, execution)?;
        let encoded_len =
            measure_rules_success_snapshot_with_execution(&snapshot.snapshot, execution)?;
        if encoded_len > crate::http::MAX_BODY_BYTES {
            self.store
                .accounting
                .reject(RulesLimitSurface::SnapshotStatus);
            return Err(RulesStatusProjectionError::StatusLimit);
        }
        self.store
            .enforce_limit(
                RulesLimitSurface::SnapshotStatus,
                snapshot_bytes.saturating_add(modeled_vec_clone_upper_bytes::<u8>(encoded_len)),
                self.config.store_limits.max_snapshot_status_bytes,
            )
            .map_err(|_| RulesStatusProjectionError::StatusLimit)?;
        let latest = self.store.accounting.snapshot();
        let stable = latest.peak_snapshot_status_bytes
            == snapshot.snapshot.metrics.peak_snapshot_status_bytes;
        Ok(stable.then_some(encoded_len))
    }

    pub(crate) fn finalize_success_snapshot_with_execution(
        &self,
        snapshot: &AccountedRulesStatusSnapshot,
        encoded_capacity: usize,
        execution: &tsink::QueryExecution,
    ) -> Result<bool, RulesStatusProjectionError> {
        execution.checkpoint()?;
        let (snapshot_bytes, _) =
            modeled_rules_status_actual_bytes_with_execution(&snapshot.snapshot, execution)?;
        self.store
            .enforce_limit(
                RulesLimitSurface::SnapshotStatus,
                snapshot_bytes.saturating_add(modeled_allocation_bytes(encoded_capacity)),
                self.config.store_limits.max_snapshot_status_bytes,
            )
            .map_err(|_| RulesStatusProjectionError::StatusLimit)?;
        let latest = self.store.accounting.snapshot();
        Ok(latest.peak_snapshot_status_bytes
            == snapshot.snapshot.metrics.peak_snapshot_status_bytes)
    }

    pub(crate) fn metrics_snapshot_with_execution(
        &self,
        execution: &tsink::QueryExecution,
    ) -> Result<RulesExpositionSnapshot, RulesExpositionError> {
        execution.checkpoint()?;
        let local_scheduler_active = self.scheduler_enabled_here();
        execution.checkpoint()?;

        let mut metrics = {
            let metrics = self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            RulesExpositionMetrics {
                scheduler_runs_total: metrics.scheduler_runs_total,
                scheduler_skipped_not_leader_total: metrics.scheduler_skipped_not_leader_total,
                scheduler_skipped_inflight_total: metrics.scheduler_skipped_inflight_total,
                evaluated_rules_total: metrics.evaluated_rules_total,
                evaluation_failures_total: metrics.evaluation_failures_total,
                recording_rows_written_total: metrics.recording_rows_written_total,
                local_scheduler_active,
                ..RulesExpositionMetrics::default()
            }
        };
        execution.checkpoint()?;

        let store = self
            .store
            .state
            .read()
            .map_err(|_| RulesExpositionError::StoreReadPoisoned)?;
        metrics.configured_groups = u64::try_from(store.groups.len()).unwrap_or(u64::MAX);
        for group in &store.groups {
            execution.checkpoint()?;
            metrics.configured_rules = metrics
                .configured_rules
                .saturating_add(u64::try_from(group.rules.len()).unwrap_or(u64::MAX));
        }
        for runtime in store.runtime.values() {
            execution.checkpoint()?;
            for instance in &runtime.alert_instances {
                execution.checkpoint()?;
                match instance.state {
                    AlertInstanceStatus::Pending => {
                        metrics.pending_alerts = metrics.pending_alerts.saturating_add(1);
                    }
                    AlertInstanceStatus::Firing => {
                        metrics.firing_alerts = metrics.firing_alerts.saturating_add(1);
                    }
                }
            }
        }
        drop(store);

        let store_accounting = self.store.accounting.snapshot();
        metrics.retained_state_bytes = store_accounting.retained_state_bytes;
        metrics.peak_retained_state_bytes = store_accounting.peak_retained_state_bytes;
        metrics.durable_file_bytes = store_accounting.durable_file_bytes;
        metrics.peak_startup_transient_bytes = store_accounting.peak_startup_transient_bytes;
        metrics.peak_replacement_transient_bytes =
            store_accounting.peak_replacement_transient_bytes;
        metrics.peak_runtime_update_transient_bytes =
            store_accounting.peak_runtime_update_transient_bytes;
        metrics.peak_snapshot_status_bytes = store_accounting.peak_snapshot_status_bytes;
        metrics.peak_snapshot_file_bytes = store_accounting.peak_snapshot_file_bytes;
        metrics.limit_rejections_total = store_accounting.limit_rejections_total;
        metrics.startup_rejections_total = store_accounting.startup_rejections_total;
        metrics.replacement_rejections_total = store_accounting.replacement_rejections_total;
        metrics.runtime_update_rejections_total = store_accounting.runtime_update_rejections_total;
        metrics.snapshot_rejections_total = store_accounting.snapshot_rejections_total;
        metrics.persistence_failures_total = store_accounting.persistence_failures_total;
        execution.checkpoint()?;

        Ok(RulesExpositionSnapshot {
            scheduler_tick_ms: u64::try_from(self.config.scheduler_tick.as_millis())
                .unwrap_or(u64::MAX),
            max_recording_rows_per_eval: self.config.max_recording_rows_per_eval,
            max_alert_instances_per_rule: self.config.max_alert_instances_per_rule,
            store_limits: self.config.store_limits,
            metrics,
        })
    }

    pub(crate) fn encode_success_snapshot(
        &self,
        snapshot: &mut RulesStatusSnapshot,
    ) -> Result<Vec<u8>, String> {
        // The peak is itself present in the response. Re-measure until updating that fixed-width
        // scalar no longer changes the encoded length at a decimal boundary.
        for _ in 0..8 {
            let accounting = self.store.accounting.snapshot();
            snapshot.metrics.peak_snapshot_status_bytes = accounting.peak_snapshot_status_bytes;
            snapshot.metrics.snapshot_rejections_total = accounting.snapshot_rejections_total;
            snapshot.metrics.limit_rejections_total = accounting.limit_rejections_total;

            let snapshot_bytes = modeled_rules_status_actual_bytes(snapshot);
            let envelope = RulesSuccessEnvelope {
                status: "success",
                data: snapshot,
            };
            let encoded_len = measure_json_value(&envelope).map_err(|error| error.to_string())?;
            if encoded_len > crate::http::MAX_BODY_BYTES {
                self.store
                    .accounting
                    .reject(RulesLimitSurface::SnapshotStatus);
                return Err(RulesLimitSurface::SnapshotStatus.message().to_string());
            }
            self.store
                .enforce_limit(
                    RulesLimitSurface::SnapshotStatus,
                    snapshot_bytes.saturating_add(modeled_vec_clone_upper_bytes::<u8>(encoded_len)),
                    self.config.store_limits.max_snapshot_status_bytes,
                )
                .map_err(|error| error.to_string())?;
            if self.store.accounting.snapshot().peak_snapshot_status_bytes
                == snapshot.metrics.peak_snapshot_status_bytes
            {
                let encoded = encode_json_value_exact(&envelope, encoded_len)
                    .map_err(|error| error.to_string())?;
                self.store
                    .enforce_limit(
                        RulesLimitSurface::SnapshotStatus,
                        snapshot_bytes.saturating_add(modeled_owned_vec_bytes(&encoded)),
                        self.config.store_limits.max_snapshot_status_bytes,
                    )
                    .map_err(|error| error.to_string())?;
                if self.store.accounting.snapshot().peak_snapshot_status_bytes
                    != snapshot.metrics.peak_snapshot_status_bytes
                {
                    continue;
                }
                return Ok(encoded);
            }
        }
        Err("rules snapshot/status accounting did not stabilize".to_string())
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
            Err(_err) => {
                let mut metrics = self
                    .metrics
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                metrics.last_error = Some("rules state snapshot failed".to_string());
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
                    Err(_err) => {
                        evaluation_failures = evaluation_failures.saturating_add(1);
                        last_error = Some("rules fingerprint calculation failed".to_string());
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

                // Recording evaluation has externally visible row and usage side effects. Claim
                // the aligned interval durably before either can occur. If the final outcome
                // checkpoint later fails, this conservative marker survives restart and prevents
                // replaying the interval (at the cost of at-most-once behavior after a crash).
                let evaluation_previous = if matches!(rule, RuleSpec::Recording(_)) {
                    let attempt = recording_rule_attempt_state(
                        previous.clone(),
                        fingerprint,
                        aligned_eval_ts,
                        now_unix_ms,
                    );
                    match self
                        .store
                        .apply_runtime_updates(vec![(rule_id.clone(), attempt.clone())])
                    {
                        Ok(1) => attempt,
                        Ok(_) => {
                            evaluation_failures = evaluation_failures.saturating_add(1);
                            last_error =
                                Some("recording rule attempt checkpoint was skipped".to_string());
                            continue;
                        }
                        Err(_err) => {
                            evaluation_failures = evaluation_failures.saturating_add(1);
                            last_error =
                                Some("recording rule attempt checkpoint failed".to_string());
                            continue;
                        }
                    }
                } else {
                    previous.clone()
                };

                evaluated_rules = evaluated_rules.saturating_add(1);
                let started = Instant::now();
                let result = self
                    .evaluate_rule(group, rule, aligned_eval_ts, evaluation_previous.clone())
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
                            accounting.record_best_effort(record).await;
                        }
                        updates.push((rule_id, state));
                    }
                    Err(err) => {
                        let mut state = evaluation_previous;
                        state.fingerprint = fingerprint;
                        state.last_eval_timestamp = Some(aligned_eval_ts);
                        state.last_eval_unix_ms = Some(now_unix_ms);
                        state.last_duration_ms = duration_ms;
                        state.last_error = Some(err.clone());
                        state.last_outcome = Some(RuleEvaluationOutcome::Error);
                        updates.push((rule_id, state));
                        evaluation_failures = evaluation_failures.saturating_add(1);
                        if let Some(accounting) = self.usage_accounting.as_ref() {
                            accounting
                                .record_best_effort(UsageRecordInput {
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
                                })
                                .await;
                        }
                        last_error = Some(err);
                    }
                }
            }
        }

        if let Err(_err) = self.store.apply_runtime_updates(updates) {
            evaluation_failures = evaluation_failures.saturating_add(1);
            last_error = Some("rules state persist failed".to_string());
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
            .map_err(|_| "rules read admission is unavailable".to_string())?;
        let _read_lease = read_admission
            .admit_request(1)
            .await
            .map_err(format_read_admission_error)?;
        let read_storage = self
            .promql_storage_for_tenant(&group.tenant_id)
            .map_err(|_| "rules query backend is unavailable".to_string())?;
        let engine = Engine::with_precision(read_storage, self.precision);
        let expr = rule_expr(rule).to_string();
        let value =
            tokio::task::spawn_blocking(move || engine.instant_query(&expr, eval_timestamp))
                .await
                .map_err(|_| "rules query task failed".to_string())?
                .map_err(|_| "rules query evaluation failed".to_string())?;

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
        let result_count = match &value {
            PromqlValue::Scalar(_, _) => 1,
            PromqlValue::InstantVector(samples) => samples.len(),
            PromqlValue::RangeVector(_) | PromqlValue::String(_, _) => 0,
        };
        if result_count > self.config.max_recording_rows_per_eval {
            return Err("recording rule result exceeds its finite row limit".to_string());
        }
        let rows = recording_rows_from_value(group, spec, eval_timestamp, value)?;
        let scoped_rows = tenant::scope_rows_for_tenant(rows, &group.tenant_id)?;
        let rows_len = scoped_rows.len();
        if rows_len > self.config.max_recording_rows_per_eval {
            return Err("recording rule result exceeds its finite row limit".to_string());
        }

        if rows_len > 0 {
            let write_admission = admission::global_public_write_admission()
                .map_err(|_| "rules write admission is unavailable".to_string())?;
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
                    .map_err(|_| "recording rule distributed write failed".to_string())?;
            } else {
                let storage = Arc::clone(&self.storage);
                tokio::task::spawn_blocking(move || storage.insert_rows(&scoped_rows))
                    .await
                    .map_err(|_| "recording rule write task failed".to_string())?
                    .map_err(|_| "recording rule write failed".to_string())?;
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
            return Err("alert rule result exceeds its finite instance limit".to_string());
        }
        let limits = self.config.store_limits;
        let mut projected_runtime_bytes =
            modeled_vec_clone_upper_bytes::<AlertInstanceState>(samples.len());
        for sample in &samples {
            projected_runtime_bytes = projected_runtime_bytes.saturating_add(
                preflight_alert_sample_state_bytes(group, spec, sample, &limits)?,
            );
        }
        let previous_runtime_bytes = modeled_runtime_state_heap_bytes(&previous);
        let previous_key_scratch_bytes = previous
            .alert_instances
            .iter()
            .map(modeled_alert_key_recompute_scratch_bytes)
            .max()
            .unwrap_or(0);
        self.store
            .enforce_limit(
                RulesLimitSurface::RuntimeUpdate,
                previous_runtime_bytes
                    .saturating_add(projected_runtime_bytes)
                    .saturating_add(previous_key_scratch_bytes),
                limits.max_runtime_update_transient_bytes,
            )
            .map_err(|error| error.to_string())?;

        let mut previous_instances = previous.alert_instances;
        for instance in &mut previous_instances {
            instance.key = alert_instance_key(&instance.source_metric, &instance.labels);
        }
        previous_instances.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        let for_units = duration_units(spec.for_secs, self.precision);
        let mut instances = Vec::new();
        instances
            .try_reserve_exact(samples.len())
            .map_err(|_| "failed to allocate bounded alert runtime state".to_string())?;
        for sample in samples {
            let (labels, sample_type, sample_value) = alert_instance_fields(group, spec, &sample)?;
            let key = alert_instance_key(&sample.metric, &labels);
            let previous = previous_instances
                .binary_search_by(|instance| instance.key.cmp(&key))
                .ok()
                .map(|index| &previous_instances[index]);
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
        instances.sort_unstable_by(|left, right| left.key.cmp(&right.key));

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

fn clone_vec_capacity_upper(len: usize) -> usize {
    if len == 0 {
        0
    } else if len <= 4 {
        4
    } else {
        len.checked_next_power_of_two().unwrap_or(usize::MAX)
    }
}

fn modeled_vec_clone_upper_bytes<T>(len: usize) -> usize {
    modeled_allocation_bytes(clone_vec_capacity_upper(len).saturating_mul(std::mem::size_of::<T>()))
}

fn modeled_string_clone_upper_bytes(value: &str) -> usize {
    modeled_allocation_bytes(value.len())
}

fn modeled_string_map_clone_upper_bytes(values: &BTreeMap<String, String>) -> usize {
    let node_inline = std::mem::size_of::<String>()
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(4));
    values.iter().fold(0usize, |bytes, (name, value)| {
        bytes
            .saturating_add(modeled_allocation_bytes(node_inline))
            .saturating_add(modeled_string_clone_upper_bytes(name))
            .saturating_add(modeled_string_clone_upper_bytes(value))
    })
}

fn modeled_labels_clone_upper_bytes(labels: &[Label]) -> usize {
    modeled_vec_clone_upper_bytes::<Label>(labels.len()).saturating_add(labels.iter().fold(
        0usize,
        |bytes, label| {
            bytes
                .saturating_add(modeled_string_clone_upper_bytes(&label.name))
                .saturating_add(modeled_string_clone_upper_bytes(&label.value))
        },
    ))
}

fn modeled_alert_instance_clone_upper_bytes(instance: &AlertInstanceState) -> usize {
    modeled_string_clone_upper_bytes(&instance.key)
        .saturating_add(modeled_string_clone_upper_bytes(&instance.source_metric))
        .saturating_add(modeled_labels_clone_upper_bytes(&instance.labels))
        .saturating_add(modeled_string_clone_upper_bytes(&instance.sample_type))
        .saturating_add(
            instance
                .sample_value
                .as_deref()
                .map(modeled_string_clone_upper_bytes)
                .unwrap_or(0),
        )
}

fn modeled_rules_status_output_upper_bytes(
    state: &PersistedRulesStoreState,
    metrics: &RulesRuntimeMetrics,
) -> usize {
    let mut bytes = std::mem::size_of::<RulesStatusSnapshot>()
        .saturating_add(modeled_vec_clone_upper_bytes::<RuleGroupStatusSnapshot>(
            state.groups.len(),
        ))
        .saturating_add(
            metrics
                .last_error
                .as_deref()
                .map(modeled_string_clone_upper_bytes)
                .unwrap_or(0),
        );
    for group in &state.groups {
        bytes = bytes
            .saturating_add(modeled_string_clone_upper_bytes(&group.name))
            .saturating_add(modeled_string_clone_upper_bytes(&group.tenant_id))
            .saturating_add(modeled_string_map_clone_upper_bytes(&group.labels))
            .saturating_add(modeled_vec_clone_upper_bytes::<RuleStatusSnapshot>(
                group.rules.len(),
            ));
        for rule in &group.rules {
            let rule_id_len = group
                .tenant_id
                .len()
                .saturating_add(group.name.len())
                .saturating_add(rule_kind(rule).len())
                .saturating_add(rule_name(rule).len())
                .saturating_add(3);
            bytes = bytes
                .saturating_add(modeled_allocation_bytes(rule_id_len))
                .saturating_add(modeled_string_clone_upper_bytes(rule_name(rule)))
                .saturating_add(modeled_string_clone_upper_bytes(rule_kind(rule)))
                .saturating_add(modeled_string_clone_upper_bytes(rule_expr(rule)))
                .saturating_add(modeled_string_map_clone_upper_bytes(rule_labels(rule)))
                .saturating_add(modeled_string_map_clone_upper_bytes(rule_annotations(rule)))
                .saturating_add(modeled_string_clone_upper_bytes("inactive"));
        }
    }
    for runtime in state.runtime.values() {
        bytes = bytes
            .saturating_add(
                runtime
                    .last_error
                    .as_deref()
                    .map(modeled_string_clone_upper_bytes)
                    .unwrap_or(0),
            )
            .saturating_add(modeled_vec_clone_upper_bytes::<AlertInstanceState>(
                runtime.alert_instances.len(),
            ))
            .saturating_add(
                runtime
                    .alert_instances
                    .iter()
                    .fold(0usize, |bytes, instance| {
                        bytes.saturating_add(modeled_alert_instance_clone_upper_bytes(instance))
                    }),
            );
    }
    bytes
}

fn modeled_rules_status_output_upper_bytes_with_execution(
    state: &PersistedRulesStoreState,
    metrics: &RulesRuntimeMetrics,
    execution: &tsink::QueryExecution,
) -> Result<(usize, usize), RulesStatusProjectionError> {
    let mut bytes = std::mem::size_of::<RulesStatusSnapshot>()
        .saturating_add(modeled_vec_clone_upper_bytes::<RuleGroupStatusSnapshot>(
            state.groups.len(),
        ))
        .saturating_add(
            metrics
                .last_error
                .as_deref()
                .map(modeled_string_clone_upper_bytes)
                .unwrap_or(0),
        );
    let mut configured_rules = 0usize;
    let mut map_entries = 0usize;
    for group in &state.groups {
        execution.checkpoint()?;
        configured_rules = configured_rules.saturating_add(group.rules.len());
        map_entries = map_entries.saturating_add(group.labels.len());
        bytes = bytes
            .saturating_add(modeled_string_clone_upper_bytes(&group.name))
            .saturating_add(modeled_string_clone_upper_bytes(&group.tenant_id))
            .saturating_add(modeled_string_map_clone_upper_bytes(&group.labels))
            .saturating_add(modeled_vec_clone_upper_bytes::<RuleStatusSnapshot>(
                group.rules.len(),
            ));
        for rule in &group.rules {
            execution.checkpoint()?;
            map_entries = map_entries
                .saturating_add(rule_labels(rule).len())
                .saturating_add(rule_annotations(rule).len());
            let rule_id_len = group
                .tenant_id
                .len()
                .saturating_add(group.name.len())
                .saturating_add(rule_kind(rule).len())
                .saturating_add(rule_name(rule).len())
                .saturating_add(3);
            bytes = bytes
                .saturating_add(modeled_allocation_bytes(rule_id_len))
                .saturating_add(modeled_string_clone_upper_bytes(rule_name(rule)))
                .saturating_add(modeled_string_clone_upper_bytes(rule_kind(rule)))
                .saturating_add(modeled_string_clone_upper_bytes(rule_expr(rule)))
                .saturating_add(modeled_string_map_clone_upper_bytes(rule_labels(rule)))
                .saturating_add(modeled_string_map_clone_upper_bytes(rule_annotations(rule)))
                .saturating_add(modeled_string_clone_upper_bytes("inactive"));
        }
    }
    for runtime in state.runtime.values() {
        execution.checkpoint()?;
        bytes = bytes
            .saturating_add(
                runtime
                    .last_error
                    .as_deref()
                    .map(modeled_string_clone_upper_bytes)
                    .unwrap_or(0),
            )
            .saturating_add(modeled_vec_clone_upper_bytes::<AlertInstanceState>(
                runtime.alert_instances.len(),
            ));
        for instance in &runtime.alert_instances {
            execution.checkpoint()?;
            bytes = bytes.saturating_add(modeled_alert_instance_clone_upper_bytes(instance));
        }
    }
    let query_upper = bytes
        .saturating_add(configured_rules.saturating_mul(std::mem::size_of::<RuleStatusSnapshot>()))
        .saturating_add(map_entries.saturating_mul(RULES_STATUS_BTREE_ENTRY_ALLOWANCE_BYTES));
    Ok((bytes, query_upper))
}

fn modeled_rule_status_actual_bytes(rule: &RuleStatusSnapshot) -> usize {
    std::mem::size_of::<RuleStatusSnapshot>()
        .saturating_add(modeled_owned_string_bytes(&rule.id))
        .saturating_add(modeled_owned_string_bytes(&rule.name))
        .saturating_add(modeled_owned_string_bytes(&rule.kind))
        .saturating_add(modeled_owned_string_bytes(&rule.expr))
        .saturating_add(modeled_string_map_bytes(&rule.labels))
        .saturating_add(modeled_string_map_bytes(&rule.annotations))
        .saturating_add(
            rule.last_error
                .as_ref()
                .map(modeled_owned_string_bytes)
                .unwrap_or(0),
        )
        .saturating_add(modeled_owned_string_bytes(&rule.state))
        .saturating_add(modeled_owned_vec_bytes(&rule.alert_instances))
        .saturating_add(rule.alert_instances.iter().fold(0usize, |bytes, instance| {
            bytes.saturating_add(modeled_alert_instance_heap_bytes(instance))
        }))
}

fn modeled_rules_status_actual_bytes(snapshot: &RulesStatusSnapshot) -> usize {
    std::mem::size_of::<RulesStatusSnapshot>()
        .saturating_add(
            snapshot
                .metrics
                .last_error
                .as_ref()
                .map(modeled_owned_string_bytes)
                .unwrap_or(0),
        )
        .saturating_add(modeled_owned_vec_bytes(&snapshot.groups))
        .saturating_add(snapshot.groups.iter().fold(0usize, |bytes, group| {
            bytes
                .saturating_add(modeled_owned_string_bytes(&group.name))
                .saturating_add(modeled_owned_string_bytes(&group.tenant_id))
                .saturating_add(modeled_string_map_bytes(&group.labels))
                .saturating_add(modeled_owned_vec_bytes(&group.rules))
                .saturating_add(group.rules.iter().fold(0usize, |bytes, rule| {
                    bytes.saturating_add(modeled_rule_status_actual_bytes(rule))
                }))
        }))
}

fn modeled_rules_status_actual_bytes_with_execution(
    snapshot: &RulesStatusSnapshot,
    execution: &tsink::QueryExecution,
) -> Result<(usize, usize), RulesStatusProjectionError> {
    let mut bytes = std::mem::size_of::<RulesStatusSnapshot>()
        .saturating_add(
            snapshot
                .metrics
                .last_error
                .as_ref()
                .map(modeled_owned_string_bytes)
                .unwrap_or(0),
        )
        .saturating_add(modeled_owned_vec_bytes(&snapshot.groups));
    let mut map_entries = 0usize;
    for group in &snapshot.groups {
        execution.checkpoint()?;
        map_entries = map_entries.saturating_add(group.labels.len());
        bytes = bytes
            .saturating_add(modeled_owned_string_bytes(&group.name))
            .saturating_add(modeled_owned_string_bytes(&group.tenant_id))
            .saturating_add(modeled_string_map_bytes(&group.labels))
            .saturating_add(modeled_owned_vec_bytes(&group.rules));
        for rule in &group.rules {
            execution.checkpoint()?;
            map_entries = map_entries
                .saturating_add(rule.labels.len())
                .saturating_add(rule.annotations.len());
            bytes = bytes
                .saturating_add(std::mem::size_of::<RuleStatusSnapshot>())
                .saturating_add(modeled_owned_string_bytes(&rule.id))
                .saturating_add(modeled_owned_string_bytes(&rule.name))
                .saturating_add(modeled_owned_string_bytes(&rule.kind))
                .saturating_add(modeled_owned_string_bytes(&rule.expr))
                .saturating_add(modeled_string_map_bytes(&rule.labels))
                .saturating_add(modeled_string_map_bytes(&rule.annotations))
                .saturating_add(
                    rule.last_error
                        .as_ref()
                        .map(modeled_owned_string_bytes)
                        .unwrap_or(0),
                )
                .saturating_add(modeled_owned_string_bytes(&rule.state))
                .saturating_add(modeled_owned_vec_bytes(&rule.alert_instances));
            for instance in &rule.alert_instances {
                execution.checkpoint()?;
                bytes = bytes.saturating_add(modeled_alert_instance_heap_bytes(instance));
            }
        }
    }
    let query_bytes =
        bytes.saturating_add(map_entries.saturating_mul(RULES_STATUS_BTREE_ENTRY_ALLOWANCE_BYTES));
    Ok((bytes, query_bytes))
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

fn clone_string_map_with_execution(
    values: &BTreeMap<String, String>,
    execution: &tsink::QueryExecution,
) -> Result<BTreeMap<String, String>, RulesStatusProjectionError> {
    let mut cloned = BTreeMap::new();
    for (name, value) in values {
        execution.checkpoint()?;
        cloned.insert(name.clone(), value.clone());
    }
    Ok(cloned)
}

fn clone_labels_with_execution(
    labels: &[Label],
    execution: &tsink::QueryExecution,
) -> Result<Vec<Label>, RulesStatusProjectionError> {
    let mut cloned = Vec::with_capacity(labels.len());
    for label in labels {
        execution.checkpoint()?;
        cloned.push(label.clone());
    }
    Ok(cloned)
}

fn clone_alert_instances_with_execution(
    instances: &[AlertInstanceState],
    execution: &tsink::QueryExecution,
) -> Result<Vec<AlertInstanceState>, RulesStatusProjectionError> {
    let mut cloned = Vec::with_capacity(instances.len());
    for instance in instances {
        execution.checkpoint()?;
        cloned.push(AlertInstanceState {
            key: instance.key.clone(),
            source_metric: instance.source_metric.clone(),
            labels: clone_labels_with_execution(&instance.labels, execution)?,
            active_since_timestamp: instance.active_since_timestamp,
            last_seen_timestamp: instance.last_seen_timestamp,
            firing_since_timestamp: instance.firing_since_timestamp,
            state: instance.state,
            sample_type: instance.sample_type.clone(),
            sample_value: instance.sample_value.clone(),
        });
    }
    Ok(cloned)
}

fn build_group_snapshot_with_execution(
    group: &RuleGroupSpec,
    runtime: &BTreeMap<String, PersistedRuleRuntimeState>,
    execution: &tsink::QueryExecution,
) -> Result<RuleGroupStatusSnapshot, RulesStatusProjectionError> {
    let mut rules = Vec::with_capacity(group.rules.len());
    for rule in &group.rules {
        execution.checkpoint()?;
        let rule_id = rule_id(group, rule);
        let state = runtime.get(&rule_id);
        rules.push(RuleStatusSnapshot {
            id: rule_id,
            name: rule_name(rule).to_string(),
            kind: rule_kind(rule).to_string(),
            expr: rule_expr(rule).to_string(),
            interval_secs: rule_interval_secs(group, rule),
            for_secs: rule_for_secs(rule),
            labels: clone_string_map_with_execution(rule_labels(rule), execution)?,
            annotations: clone_string_map_with_execution(rule_annotations(rule), execution)?,
            last_eval_timestamp: state.and_then(|state| state.last_eval_timestamp),
            last_eval_unix_ms: state.and_then(|state| state.last_eval_unix_ms),
            last_success_unix_ms: state.and_then(|state| state.last_success_unix_ms),
            last_duration_ms: state.map_or(0, |state| state.last_duration_ms),
            last_error: state.and_then(|state| state.last_error.clone()),
            last_sample_count: state.map_or(0, |state| state.last_sample_count),
            last_recorded_rows: state.map_or(0, |state| state.last_recorded_rows),
            state: state
                .map(|state| summarize_rule_state_str(rule, state))
                .unwrap_or("inactive")
                .to_string(),
            alert_instances: match state {
                Some(state) => {
                    clone_alert_instances_with_execution(&state.alert_instances, execution)?
                }
                None => Vec::new(),
            },
        });
    }
    Ok(RuleGroupStatusSnapshot {
        name: group.name.clone(),
        tenant_id: group.tenant_id.clone(),
        interval_secs: group.interval_secs,
        labels: clone_string_map_with_execution(&group.labels, execution)?,
        rules,
    })
}

fn summarize_rule_state(rule: &RuleSpec, state: &PersistedRuleRuntimeState) -> String {
    summarize_rule_state_str(rule, state).to_string()
}

fn summarize_rule_state_str(rule: &RuleSpec, state: &PersistedRuleRuntimeState) -> &'static str {
    if state.last_error.is_some()
        && matches!(state.last_outcome, Some(RuleEvaluationOutcome::Error))
    {
        return "error";
    }
    match rule {
        RuleSpec::Recording(_) => {
            if state.last_eval_timestamp.is_some() {
                "ok"
            } else {
                "inactive"
            }
        }
        RuleSpec::Alert(_) => {
            if state
                .alert_instances
                .iter()
                .any(|instance| instance.state == AlertInstanceStatus::Firing)
            {
                "firing"
            } else if state
                .alert_instances
                .iter()
                .any(|instance| instance.state == AlertInstanceStatus::Pending)
            {
                "pending"
            } else {
                "inactive"
            }
        }
    }
}

fn recording_rule_attempt_state(
    mut previous: PersistedRuleRuntimeState,
    fingerprint: u64,
    aligned_eval_timestamp: i64,
    attempt_unix_ms: u64,
) -> PersistedRuleRuntimeState {
    previous.fingerprint = fingerprint;
    previous.last_eval_timestamp = Some(aligned_eval_timestamp);
    previous.last_eval_unix_ms = Some(attempt_unix_ms);
    previous.last_duration_ms = 0;
    previous.last_error = Some(RECORDING_RULE_ATTEMPT_PENDING.to_string());
    previous.last_sample_count = 0;
    previous.last_recorded_rows = 0;
    previous.last_outcome = Some(RuleEvaluationOutcome::Error);
    previous.alert_instances.clear();
    previous
}

fn validate_label_set_limits(
    labels: &BTreeMap<String, String>,
    limits: &RulesStoreLimits,
    annotations: bool,
) -> Result<(), RulesStoreError> {
    if labels.len() > limits.max_labels_per_set {
        return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
    }
    let mut cumulative_bytes = 0usize;
    for (name, value) in labels {
        if name.is_empty()
            || name.len() > limits.max_name_bytes
            || name.len() > MAX_LABEL_NAME_LEN
            || (!annotations && (name == tenant::TENANT_LABEL || name == "__name__"))
        {
            return Err(RulesStoreError::Invalid(
                "rules contain an invalid label name",
            ));
        }
        if value.is_empty() || value.len() > MAX_LABEL_VALUE_LEN {
            return Err(RulesStoreError::Invalid(
                "rules contain an invalid label value",
            ));
        }
        cumulative_bytes = cumulative_bytes
            .saturating_add(name.len())
            .saturating_add(value.len());
    }
    let limit = if annotations {
        limits.max_annotation_bytes
    } else {
        limits.max_label_set_bytes
    };
    if cumulative_bytes > limit {
        return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
    }
    Ok(())
}

fn validate_groups_with_limits(
    groups: &Vec<RuleGroupSpec>,
    limits: &RulesStoreLimits,
) -> Result<(), RulesStoreError> {
    if groups.len() > limits.max_groups {
        return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
    }
    let mut rules_total = 0usize;
    for group in groups {
        if group.name.is_empty() || group.tenant_id.is_empty() || group.interval_secs == 0 {
            return Err(RulesStoreError::Invalid("rules contain an invalid group"));
        }
        if group.name.len() > limits.max_name_bytes || group.tenant_id.len() > limits.max_name_bytes
        {
            return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
        }
        tenant::scope_rows_for_tenant(Vec::new(), &group.tenant_id)
            .map_err(|_| RulesStoreError::Invalid("rules contain an invalid tenant identifier"))?;
        if group.rules.is_empty() {
            return Err(RulesStoreError::Invalid("rules groups must not be empty"));
        }
        if group.rules.len() > limits.max_rules_per_group {
            return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
        }
        rules_total = rules_total.saturating_add(group.rules.len());
        if rules_total > limits.max_rules_total {
            return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
        }
        validate_label_set_limits(&group.labels, limits, false)?;
        for rule in &group.rules {
            let name = rule_name(rule);
            let expression = rule_expr(rule);
            if name.is_empty()
                || name.len() > limits.max_name_bytes
                || expression.is_empty()
                || expression.len() > limits.max_expression_bytes
            {
                return Err(RulesStoreError::Limit(RulesLimitSurface::Configuration));
            }
            if expression.trim().is_empty() {
                return Err(RulesStoreError::Invalid(
                    "rules contain an empty expression",
                ));
            }
            match rule {
                RuleSpec::Recording(spec) => {
                    if spec.record.len() > MAX_METRIC_NAME_LEN || spec.interval_secs == Some(0) {
                        return Err(RulesStoreError::Invalid(
                            "rules contain an invalid recording rule",
                        ));
                    }
                    validate_label_set_limits(&spec.labels, limits, false)?;
                }
                RuleSpec::Alert(spec) => {
                    if spec.alert.len() > MAX_LABEL_VALUE_LEN || spec.interval_secs == Some(0) {
                        return Err(RulesStoreError::Invalid(
                            "rules contain an invalid alert rule",
                        ));
                    }
                    validate_label_set_limits(&spec.labels, limits, false)?;
                    validate_label_set_limits(&spec.annotations, limits, true)?;
                }
            }
        }
    }

    let mut seen_rule_ids = BTreeSet::new();
    for group in groups {
        for rule in &group.rules {
            if tsink::promql::parse(rule_expr(rule)).is_err() {
                return Err(RulesStoreError::Invalid(
                    "rules contain an invalid expression",
                ));
            }
            if !seen_rule_ids.insert(rule_id(group, rule)) {
                return Err(RulesStoreError::Invalid(
                    "rules contain a duplicate rule identifier",
                ));
            }
        }
    }
    Ok(())
}

fn validate_runtime_state_with_limits(
    runtime: &PersistedRuleRuntimeState,
    limits: &RulesStoreLimits,
) -> Result<(), RulesStoreError> {
    if runtime
        .last_error
        .as_ref()
        .is_some_and(|error| error.len() > RULES_MAX_DIAGNOSTIC_BYTES)
    {
        return Err(RulesStoreError::Limit(RulesLimitSurface::RuntimeUpdate));
    }
    if runtime.alert_instances.len() > limits.max_alert_instances_per_rule {
        return Err(RulesStoreError::Limit(RulesLimitSurface::RuntimeUpdate));
    }
    for instance in &runtime.alert_instances {
        if instance.key.len()
            > limits
                .max_label_set_bytes
                .saturating_add(limits.max_name_bytes)
            || instance.source_metric.len() > limits.max_name_bytes
            || instance.sample_type.len() > limits.max_name_bytes
            || instance
                .sample_value
                .as_ref()
                .is_some_and(|value| value.len() > limits.max_annotation_bytes)
            || instance.labels.len() > limits.max_labels_per_set
        {
            return Err(RulesStoreError::Limit(RulesLimitSurface::RuntimeUpdate));
        }
        let mut label_bytes = 0usize;
        for label in &instance.labels {
            if label.name.is_empty()
                || label.name.len() > limits.max_name_bytes
                || label.name.len() > MAX_LABEL_NAME_LEN
                || label.value.len() > MAX_LABEL_VALUE_LEN
            {
                return Err(RulesStoreError::Invalid(
                    "rules runtime contains an invalid alert label",
                ));
            }
            label_bytes = label_bytes
                .saturating_add(label.name.len())
                .saturating_add(label.value.len());
        }
        if label_bytes > limits.max_label_set_bytes {
            return Err(RulesStoreError::Limit(RulesLimitSurface::RuntimeUpdate));
        }
    }
    Ok(())
}

fn validate_persisted_state_with_limits(
    state: &PersistedRulesStoreState,
    limits: &RulesStoreLimits,
) -> Result<(), RulesStoreError> {
    validate_groups_with_limits(&state.groups, limits)?;
    if state.runtime.len() > limits.max_rules_total {
        return Err(RulesStoreError::Limit(RulesLimitSurface::Startup));
    }
    for (rule_id, runtime) in &state.runtime {
        if rule_id.len() > limits.max_name_bytes.saturating_mul(3).saturating_add(32) {
            return Err(RulesStoreError::Limit(RulesLimitSurface::Startup));
        }
        validate_runtime_state_with_limits(runtime, limits)?;
    }
    if modeled_rules_state_bytes(state) > limits.max_total_retained_state_bytes {
        return Err(RulesStoreError::Limit(RulesLimitSurface::RetainedState));
    }
    Ok(())
}

fn modeled_rules_state_bytes_with_runtime_overlay(
    state: &PersistedRulesStoreState,
    replacements: &BTreeMap<String, PersistedRuleRuntimeState>,
) -> usize {
    let runtime_bytes = state
        .runtime
        .iter()
        .fold(0usize, |bytes, (rule_id, runtime)| {
            bytes.saturating_add(modeled_runtime_entry_bytes(
                rule_id,
                replacements.get(rule_id).unwrap_or(runtime),
            ))
        });
    std::mem::size_of::<PersistedRulesStoreState>()
        .saturating_add(modeled_groups_bytes(&state.groups))
        .saturating_add(runtime_bytes)
}

fn rule_fingerprint(group: &RuleGroupSpec, rule: &RuleSpec) -> Result<u64, String> {
    let mut writer = Fnv1aWriter {
        hash: 0xcbf29ce484222325,
    };
    serde_json::to_writer(&mut writer, &(group, rule))
        .map_err(|_| "failed to encode bounded rule fingerprint".to_string())?;
    Ok(writer.hash)
}

struct Fnv1aWriter {
    hash: u64,
}

impl Write for Fnv1aWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(0x100000001b3);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn rule_id(group: &RuleGroupSpec, rule: &RuleSpec) -> String {
    let kind = rule_kind(rule);
    let rule_name = rule_name(rule);
    let capacity = group
        .tenant_id
        .len()
        .saturating_add(group.name.len())
        .saturating_add(kind.len())
        .saturating_add(rule_name.len())
        .saturating_add(3);
    let mut id = String::with_capacity(capacity);
    id.push_str(&group.tenant_id);
    id.push('/');
    id.push_str(&group.name);
    id.push('/');
    id.push_str(kind);
    id.push('/');
    id.push_str(rule_name);
    id
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
        PromqlValue::RangeVector(_) => {
            Err("recording rule must evaluate to a scalar or instant vector".to_string())
        }
        PromqlValue::String(_, _) => Err("recording rule cannot record string results".to_string()),
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

fn modeled_alert_key_recompute_scratch_bytes(instance: &AlertInstanceState) -> usize {
    let label_bytes = instance.labels.iter().fold(0usize, |bytes, label| {
        bytes
            .saturating_add(label.name.len())
            .saturating_add(label.value.len())
    });
    let key_bytes_upper = instance
        .source_metric
        .len()
        .saturating_add(label_bytes.saturating_mul(2))
        .saturating_add(instance.labels.len().saturating_mul(32))
        .max(instance.key.len());
    modeled_allocation_bytes(key_bytes_upper)
}

fn preflight_alert_sample_state_bytes(
    group: &RuleGroupSpec,
    spec: &AlertRuleSpec,
    sample: &Sample,
    limits: &RulesStoreLimits,
) -> Result<usize, String> {
    if sample.metric.len() > limits.max_name_bytes
        || sample.labels.len() > limits.max_labels_per_set
    {
        return Err("alert rule output exceeds a finite state limit".to_string());
    }
    let merged_count_upper = sample
        .labels
        .len()
        .saturating_add(group.labels.len())
        .saturating_add(spec.labels.len())
        .saturating_add(1);
    if merged_count_upper > limits.max_labels_per_set {
        return Err("alert rule output exceeds a finite label-count limit".to_string());
    }
    let mut label_bytes = "alertname".len().saturating_add(spec.alert.len());
    let mut clone_heap_bytes = modeled_string_clone_upper_bytes("alertname")
        .saturating_add(modeled_string_clone_upper_bytes(&spec.alert));
    for label in &sample.labels {
        if label.name.is_empty()
            || label.name.len() > limits.max_name_bytes
            || label.name.len() > MAX_LABEL_NAME_LEN
            || label.value.len() > MAX_LABEL_VALUE_LEN
        {
            return Err("alert rule output contains an invalid label".to_string());
        }
        label_bytes = label_bytes
            .saturating_add(label.name.len())
            .saturating_add(label.value.len());
        clone_heap_bytes = clone_heap_bytes
            .saturating_add(modeled_string_clone_upper_bytes(&label.name))
            .saturating_add(modeled_string_clone_upper_bytes(&label.value));
    }
    for labels in [&group.labels, &spec.labels] {
        for (name, value) in labels {
            label_bytes = label_bytes
                .saturating_add(name.len())
                .saturating_add(value.len());
            clone_heap_bytes = clone_heap_bytes
                .saturating_add(modeled_string_clone_upper_bytes(name))
                .saturating_add(modeled_string_clone_upper_bytes(value));
        }
    }
    if label_bytes > limits.max_label_set_bytes {
        return Err("alert rule output exceeds a finite label-byte limit".to_string());
    }
    let key_bytes_upper = sample
        .metric
        .len()
        .saturating_add(label_bytes.saturating_mul(2))
        .saturating_add(merged_count_upper.saturating_mul(32));
    Ok(modeled_allocation_bytes(key_bytes_upper)
        .saturating_add(modeled_string_clone_upper_bytes(&sample.metric))
        .saturating_add(modeled_vec_clone_upper_bytes::<Label>(merged_count_upper))
        .saturating_add(clone_heap_bytes)
        .saturating_add(modeled_allocation_bytes(32))
        .saturating_add(modeled_allocation_bytes(64)))
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
        if name.trim().is_empty()
            || name == tenant::TENANT_LABEL
            || name == "__name__"
            || name.len() > MAX_LABEL_NAME_LEN
        {
            return Err("rule output contains an invalid label name".to_string());
        }
        if value.trim().is_empty() || value.len() > MAX_LABEL_VALUE_LEN {
            return Err("rule output contains an invalid label value".to_string());
        }
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
        return Err("invalid rules duration".to_string());
    };
    let num: f64 = num_str
        .parse()
        .map_err(|_| "invalid rules duration".to_string())?;
    let secs = match unit {
        "ms" => num / 1_000.0,
        "s" => num,
        "m" => num * 60.0,
        "h" => num * 3_600.0,
        "d" => num * 86_400.0,
        "w" => num * 604_800.0,
        "y" => num * 365.25 * 86_400.0,
        _ => return Err("invalid rules duration unit".to_string()),
    };
    if !secs.is_finite() || secs <= 0.0 {
        return Err("invalid rules duration".to_string());
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

struct LoadedRulesStoreState {
    state: PersistedRulesStoreState,
    durable_file_bytes: usize,
    startup_transient_bytes: usize,
}

#[derive(Serialize)]
struct PersistedRulesStoreRef<'a, S: Serialize + ?Sized> {
    magic: &'static str,
    schema_version: u16,
    state: &'a S,
}

#[derive(Serialize)]
struct PersistedRulesStateOverlay<'a> {
    groups: &'a [RuleGroupSpec],
    runtime: RuntimeStateOverlay<'a>,
}

struct RuntimeStateOverlay<'a> {
    current: &'a BTreeMap<String, PersistedRuleRuntimeState>,
    replacements: &'a BTreeMap<String, PersistedRuleRuntimeState>,
}

impl Serialize for RuntimeStateOverlay<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.current.len()))?;
        for (rule_id, current) in self.current {
            map.serialize_entry(rule_id, self.replacements.get(rule_id).unwrap_or(current))?;
        }
        map.end()
    }
}

struct JsonLengthWriter {
    bytes: usize,
}

impl Write for JsonLengthWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("rules JSON length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn measure_json_value<S: Serialize + ?Sized>(value: &S) -> Result<usize, RulesStoreError> {
    let mut counter = JsonLengthWriter { bytes: 0 };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| RulesStoreError::Internal("failed to measure bounded rules JSON"))?;
    Ok(counter.bytes)
}

struct RulesExecutionJsonLengthWriter<'a> {
    bytes: usize,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl Write for RulesExecutionJsonLengthWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "rules status JSON measurement was canceled",
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("rules status JSON length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn measure_rules_success_snapshot_with_execution(
    snapshot: &RulesStatusSnapshot,
    execution: &tsink::QueryExecution,
) -> Result<usize, RulesStatusProjectionError> {
    let envelope = RulesSuccessEnvelope {
        status: "success",
        data: snapshot,
    };
    let mut counter = RulesExecutionJsonLengthWriter {
        bytes: 0,
        execution,
        control_error: None,
    };
    if serde_json::to_writer(&mut counter, &envelope).is_err() {
        return Err(match counter.control_error {
            Some(error) => RulesStatusProjectionError::QueryBudget(error),
            None => {
                RulesStatusProjectionError::Serialization("failed to measure bounded rules JSON")
            }
        });
    }
    Ok(counter.bytes)
}

fn encode_json_value_exact<S: Serialize + ?Sized>(
    value: &S,
    encoded_len: usize,
) -> Result<Vec<u8>, RulesStoreError> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| RulesStoreError::Internal("failed to allocate bounded rules JSON"))?;
    serde_json::to_writer(&mut encoded, value)
        .map_err(|_| RulesStoreError::Internal("failed to encode bounded rules JSON"))?;
    if encoded.len() != encoded_len {
        return Err(RulesStoreError::Internal(
            "bounded rules JSON length changed during encoding",
        ));
    }
    Ok(encoded)
}

fn measure_rules_store_state<S: Serialize + ?Sized>(state: &S) -> Result<usize, RulesStoreError> {
    let persisted = PersistedRulesStoreRef {
        magic: RULES_STORE_MAGIC,
        schema_version: RULES_STORE_SCHEMA_VERSION,
        state,
    };
    let mut counter = JsonLengthWriter { bytes: 0 };
    serde_json::to_writer_pretty(&mut counter, &persisted)
        .map_err(|_| RulesStoreError::Internal("failed to measure rules durable state"))?;
    counter
        .bytes
        .checked_add(1)
        .ok_or(RulesStoreError::Internal(
            "rules durable state length overflowed",
        ))
}

fn encode_rules_store_state_exact<S: Serialize + ?Sized>(
    state: &S,
    encoded_len: usize,
) -> Result<Vec<u8>, RulesStoreError> {
    let persisted = PersistedRulesStoreRef {
        magic: RULES_STORE_MAGIC,
        schema_version: RULES_STORE_SCHEMA_VERSION,
        state,
    };
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| RulesStoreError::Internal("failed to allocate rules durable state"))?;
    serde_json::to_writer_pretty(&mut encoded, &persisted)
        .map_err(|_| RulesStoreError::Internal("failed to encode rules durable state"))?;
    encoded.push(b'\n');
    if encoded.len() != encoded_len {
        return Err(RulesStoreError::Internal(
            "rules durable state length changed during encoding",
        ));
    }
    Ok(encoded)
}

fn write_encoded_rules_store_state(
    path: &Path,
    encoded: &[u8],
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
) -> tsink::Result<()> {
    if let Some(local_disk_budget) = local_disk_budget {
        return local_disk_budget.write_file_atomically_and_sync_parent(
            path,
            encoded,
            DiskCategory::ServerState,
        );
    }
    tsink::engine::fs_utils::write_file_atomically_and_sync_parent(path, encoded)
}

fn preflight_rules_json(raw: &[u8]) -> Result<usize, RulesStoreError> {
    let mut depth = 0usize;
    let mut structural_values = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in raw {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                structural_values = structural_values.saturating_add(1);
                if depth > RULES_STARTUP_MAX_JSON_DEPTH {
                    return Err(RulesStoreError::Invalid(
                        "rules store JSON exceeds its nesting-depth limit",
                    ));
                }
            }
            b'}' | b']' => {
                depth = depth.checked_sub(1).ok_or(RulesStoreError::Invalid(
                    "rules store contains malformed JSON",
                ))?;
            }
            b',' | b':' => {
                structural_values = structural_values.saturating_add(1);
            }
            _ => {}
        }
    }
    if in_string || escaped || depth != 0 {
        return Err(RulesStoreError::Invalid(
            "rules store contains malformed JSON",
        ));
    }
    Ok(raw
        .len()
        .saturating_mul(2)
        .saturating_add(
            structural_values.saturating_mul(
                RULES_ALLOCATION_ALLOWANCE_BYTES
                    .saturating_add(std::mem::size_of::<serde_json::Value>()),
            ),
        ))
}

fn read_rules_file_bounded(
    path: &Path,
    max_file_bytes: usize,
    max_startup_transient_bytes: usize,
) -> Result<Option<Vec<u8>>, RulesStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(RulesStoreError::Persistence(error.into())),
    };
    let file_len = usize::try_from(file.metadata().map_err(tsink::TsinkError::from)?.len())
        .unwrap_or(usize::MAX);
    if file_len > max_file_bytes {
        return Err(RulesStoreError::Limit(RulesLimitSurface::DurableFile));
    }
    if file_len > max_startup_transient_bytes {
        return Err(RulesStoreError::Limit(RulesLimitSurface::Startup));
    }
    let mut raw = Vec::new();
    raw.try_reserve_exact(file_len)
        .map_err(|_| RulesStoreError::Internal("failed to allocate bounded rules startup state"))?;
    raw.resize(file_len, 0);
    file.read_exact(&mut raw).map_err(tsink::TsinkError::from)?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing).map_err(tsink::TsinkError::from)? != 0 {
        return Err(RulesStoreError::Limit(RulesLimitSurface::DurableFile));
    }
    Ok(Some(raw))
}

fn load_rules_store_state_bounded(
    path: &Path,
    limits: &RulesStoreLimits,
    accounting: &RulesStoreAccounting,
) -> Result<LoadedRulesStoreState, RulesStoreError> {
    let Some(raw) = read_rules_file_bounded(
        path,
        limits.max_durable_file_bytes,
        limits.max_startup_transient_bytes,
    )?
    else {
        return Ok(LoadedRulesStoreState {
            state: PersistedRulesStoreState::default(),
            durable_file_bytes: 0,
            startup_transient_bytes: 0,
        });
    };
    let decoded_upper = preflight_rules_json(&raw)?;
    let preflight_peak = raw.len().saturating_add(decoded_upper);
    accounting.observe_peak(RulesLimitSurface::Startup, preflight_peak);
    if preflight_peak > limits.max_startup_transient_bytes {
        accounting.reject(RulesLimitSurface::Startup);
        return Err(RulesStoreError::Limit(RulesLimitSurface::Startup));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(&raw);
    let persisted = PersistedRulesStore::deserialize(&mut deserializer)
        .map_err(|_| RulesStoreError::Invalid("rules store contains invalid durable state"))?;
    deserializer
        .end()
        .map_err(|_| RulesStoreError::Invalid("rules store contains trailing data"))?;
    if persisted.magic != RULES_STORE_MAGIC
        || persisted.schema_version != RULES_STORE_SCHEMA_VERSION
    {
        return Err(RulesStoreError::Invalid(
            "rules store has an unsupported durable format",
        ));
    }
    validate_persisted_state_with_limits(&persisted.state, limits).map_err(|error| {
        if matches!(error, RulesStoreError::Limit(_)) {
            accounting.reject(RulesLimitSurface::Startup);
            RulesStoreError::Limit(RulesLimitSurface::Startup)
        } else {
            RulesStoreError::Invalid("rules store contains invalid bounded state")
        }
    })?;
    let retained_state_bytes = modeled_rules_state_bytes(&persisted.state);
    if retained_state_bytes > limits.max_total_retained_state_bytes {
        accounting.reject(RulesLimitSurface::Startup);
        return Err(RulesStoreError::Limit(RulesLimitSurface::Startup));
    }
    let actual_peak = raw.len().saturating_add(retained_state_bytes);
    if actual_peak > limits.max_startup_transient_bytes {
        accounting.reject(RulesLimitSurface::Startup);
        return Err(RulesStoreError::Limit(RulesLimitSurface::Startup));
    }
    Ok(LoadedRulesStoreState {
        state: persisted.state,
        durable_file_bytes: raw.len(),
        startup_transient_bytes: preflight_peak.max(actual_peak),
    })
}

#[cfg(test)]
fn load_rules_store_state(path: &Path) -> Result<PersistedRulesStoreState, String> {
    let accounting = RulesStoreAccounting::default();
    load_rules_store_state_bounded(path, &RulesStoreLimits::default(), &accounting)
        .map(|loaded| loaded.state)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::{ClusterConfig, DEFAULT_CLUSTER_SHARDS};
    use crate::cluster::{ClusterRequestContext, ClusterRuntime};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Barrier;
    use tempfile::tempdir;
    use tsink::StorageBuilder;

    struct CountingInsertStorage {
        inner: Arc<dyn Storage>,
        inserted_rows: Arc<AtomicU64>,
    }

    impl Storage for CountingInsertStorage {
        fn insert_rows(&self, rows: &[Row]) -> tsink::Result<()> {
            self.inner.insert_rows(rows)?;
            self.inserted_rows.fetch_add(
                u64::try_from(rows.len()).unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
            Ok(())
        }

        fn select(
            &self,
            metric: &str,
            labels: &[Label],
            start: i64,
            end: i64,
        ) -> tsink::Result<Vec<DataPoint>> {
            self.inner.select(metric, labels, start, end)
        }

        fn select_with_options(
            &self,
            metric: &str,
            opts: tsink::QueryOptions,
        ) -> tsink::Result<Vec<DataPoint>> {
            self.inner.select_with_options(metric, opts)
        }

        fn select_all(
            &self,
            metric: &str,
            start: i64,
            end: i64,
        ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.inner.select_all(metric, start, end)
        }

        fn list_metrics(&self) -> tsink::Result<Vec<tsink::MetricSeries>> {
            self.inner.list_metrics()
        }

        fn list_metrics_with_wal(&self) -> tsink::Result<Vec<tsink::MetricSeries>> {
            self.inner.list_metrics_with_wal()
        }

        fn select_series(
            &self,
            selection: &tsink::SeriesSelection,
        ) -> tsink::Result<Vec<tsink::MetricSeries>> {
            self.inner.select_series(selection)
        }

        fn close(&self) -> tsink::Result<()> {
            self.inner.close()
        }
    }

    fn encoded_rules_store_len(state: &PersistedRulesStoreState) -> u64 {
        u64::try_from(
            measure_rules_store_state(state).expect("rules state length should be measurable"),
        )
        .expect("encoded rules state length should fit u64")
    }

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

    fn sample_recording_group(name: &str, record: &str) -> RuleGroupSpec {
        RuleGroupSpec {
            name: name.to_string(),
            tenant_id: "team-a".to_string(),
            interval_secs: 60,
            labels: BTreeMap::new(),
            rules: vec![RuleSpec::Recording(RecordingRuleSpec {
                record: record.to_string(),
                expr: "source_metric".to_string(),
                interval_secs: None,
                labels: BTreeMap::new(),
            })],
        }
    }

    fn sample_alert_group(name: &str, alert: &str) -> RuleGroupSpec {
        RuleGroupSpec {
            name: name.to_string(),
            tenant_id: "team-a".to_string(),
            interval_secs: 60,
            labels: BTreeMap::new(),
            rules: vec![RuleSpec::Alert(AlertRuleSpec {
                alert: alert.to_string(),
                expr: "source_metric > 0".to_string(),
                interval_secs: None,
                for_secs: 0,
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
            })],
        }
    }

    fn test_store_limits() -> RulesStoreLimits {
        RulesStoreLimits {
            max_groups: 8,
            max_rules_per_group: 8,
            max_rules_total: 32,
            max_alert_instances_per_rule: 32,
            max_labels_per_set: 8,
            max_label_set_bytes: 8 * 1024,
            max_name_bytes: 1024,
            max_expression_bytes: 8 * 1024,
            max_annotation_bytes: 8 * 1024,
            max_total_retained_state_bytes: 8 * 1024 * 1024,
            max_durable_file_bytes: 8 * 1024 * 1024,
            max_startup_transient_bytes: 32 * 1024 * 1024,
            max_replacement_transient_bytes: 32 * 1024 * 1024,
            max_runtime_update_transient_bytes: 16 * 1024 * 1024,
            max_snapshot_status_bytes: 16 * 1024 * 1024,
        }
    }

    fn rich_status_projection_runtime() -> Arc<RulesRuntime> {
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let runtime = RulesRuntime::open_with_config(
            None,
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            RulesRuntimeConfig {
                store_limits: test_store_limits(),
                max_alert_instances_per_rule: test_store_limits().max_alert_instances_per_rule,
                ..RulesRuntimeConfig::default()
            },
        )
        .expect("rules runtime should open");
        let group = RuleGroupSpec {
            name: "status-\u{2603}-group".to_string(),
            tenant_id: "team-\"quoted\"".to_string(),
            interval_secs: 45,
            labels: BTreeMap::from([
                ("environment".to_string(), "staging\\blue".to_string()),
                ("region".to_string(), "central".to_string()),
            ]),
            rules: vec![
                RuleSpec::Recording(RecordingRuleSpec {
                    record: "status_recording_one".to_string(),
                    expr: "sum(source_metric{path=\"a\\\\b\"})".to_string(),
                    interval_secs: Some(15),
                    labels: BTreeMap::from([(
                        "recording_label".to_string(),
                        "value-one".to_string(),
                    )]),
                }),
                RuleSpec::Alert(AlertRuleSpec {
                    alert: "StatusAlertOne".to_string(),
                    expr: "source_metric > 0".to_string(),
                    interval_secs: None,
                    for_secs: 30,
                    labels: BTreeMap::from([("severity".to_string(), "warning".to_string())]),
                    annotations: BTreeMap::from([(
                        "summary".to_string(),
                        "snowman \u{2603} says \"hello\"".to_string(),
                    )]),
                }),
                RuleSpec::Recording(RecordingRuleSpec {
                    record: "status_recording_two".to_string(),
                    expr: "max(secondary_metric)".to_string(),
                    interval_secs: None,
                    labels: BTreeMap::from([("zone".to_string(), "z2".to_string())]),
                }),
                RuleSpec::Alert(AlertRuleSpec {
                    alert: "StatusAlertTwo".to_string(),
                    expr: "secondary_metric == 0".to_string(),
                    interval_secs: Some(10),
                    for_secs: 0,
                    labels: BTreeMap::from([("severity".to_string(), "critical".to_string())]),
                    annotations: BTreeMap::from([(
                        "runbook".to_string(),
                        "https://example.invalid/runbook?q=\"status\"".to_string(),
                    )]),
                }),
            ],
        };
        runtime
            .store
            .apply_groups(vec![group.clone()])
            .expect("rich rules group should apply");
        let alert_rule_id = rule_id(&group, &group.rules[1]);
        let recording_rule_id = rule_id(&group, &group.rules[0]);
        {
            let mut state = runtime
                .store
                .state
                .write()
                .expect("rules state should remain writable");
            state.runtime.insert(
                alert_rule_id,
                PersistedRuleRuntimeState {
                    fingerprint: 41,
                    last_eval_timestamp: Some(1_700_000_001_000),
                    last_eval_unix_ms: Some(1_700_000_001_111),
                    last_success_unix_ms: Some(1_700_000_001_222),
                    last_duration_ms: 17,
                    last_error: Some("diagnostic: escaped \\\"value\\\" \u{2603}".to_string()),
                    last_sample_count: 2,
                    last_recorded_rows: 0,
                    last_outcome: Some(RuleEvaluationOutcome::Error),
                    alert_instances: vec![
                        AlertInstanceState {
                            key: "pending-key".to_string(),
                            source_metric: "source_metric".to_string(),
                            labels: vec![Label::new("host", "alpha")],
                            active_since_timestamp: 1_700_000_000_000,
                            last_seen_timestamp: 1_700_000_001_000,
                            firing_since_timestamp: None,
                            state: AlertInstanceStatus::Pending,
                            sample_type: "float".to_string(),
                            sample_value: Some("1.5".to_string()),
                        },
                        AlertInstanceState {
                            key: "firing-key".to_string(),
                            source_metric: "source_metric".to_string(),
                            labels: vec![Label::new("host", "beta")],
                            active_since_timestamp: 1_699_999_999_000,
                            last_seen_timestamp: 1_700_000_001_000,
                            firing_since_timestamp: Some(1_700_000_000_500),
                            state: AlertInstanceStatus::Firing,
                            sample_type: "histogram".to_string(),
                            sample_value: None,
                        },
                    ],
                },
            );
            state.runtime.insert(
                recording_rule_id,
                PersistedRuleRuntimeState {
                    fingerprint: 42,
                    last_eval_timestamp: Some(1_700_000_002_000),
                    last_eval_unix_ms: Some(1_700_000_002_111),
                    last_success_unix_ms: Some(1_700_000_002_222),
                    last_duration_ms: 9,
                    last_sample_count: 4,
                    last_recorded_rows: 3,
                    last_outcome: Some(RuleEvaluationOutcome::Success),
                    ..PersistedRuleRuntimeState::default()
                },
            );
        }
        {
            let mut metrics = runtime
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            metrics.scheduler_runs_total = 11;
            metrics.evaluated_rules_total = 19;
            metrics.evaluation_failures_total = 3;
            metrics.recording_rows_written_total = 7;
            metrics.last_run_unix_ms = Some(1_700_000_003_000);
            metrics.last_error = Some("scheduler diagnostic \u{2603}".to_string());
        }
        runtime
    }

    #[test]
    fn accounted_status_projection_preserves_rich_legacy_snapshot_bytes() {
        let runtime = rich_status_projection_runtime();
        let legacy = runtime.snapshot().expect("legacy status should build");
        let expected = serde_json::to_vec(&legacy).expect("legacy status should serialize");
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let execution = budget
            .begin_query()
            .expect("rules status query should admit");

        let projected = runtime
            .status_snapshot_with_execution(&execution)
            .expect("accounted rules status should build");
        assert_eq!(
            serde_json::to_vec(&*projected).expect("accounted status should serialize"),
            expected
        );
        assert_eq!(projected.groups[0].rules.len(), 4);
        assert_eq!(projected.metrics.pending_alerts, 1);
        assert_eq!(projected.metrics.firing_alerts, 1);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            projected.accounted_bytes()
        );
        assert!(projected.accounted_bytes() > 0);
        drop(projected);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn accounted_status_projection_enforces_exact_query_memory_peak() {
        let runtime = rich_status_projection_runtime();
        let calibration_budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = runtime
            .status_snapshot_with_execution(&calibration)
            .expect("calibration status should build");
        let retained = calibrated.accounted_bytes();
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required >= retained);
        assert!(retained > 0);
        drop(calibrated);
        drop(calibration);

        let exact_budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(required),
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("exact budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let exact_snapshot = runtime
            .status_snapshot_with_execution(&exact)
            .expect("the exact status projection peak should pass");
        assert_eq!(exact_snapshot.accounted_bytes(), retained);
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        drop(exact_snapshot);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);

        let one_under_budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(required.saturating_sub(1)),
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("one-under budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        assert!(matches!(
            runtime.status_snapshot_with_execution(&one_under),
            Err(RulesStatusProjectionError::QueryBudget(
                tsink::QueryBudgetError::LimitExceeded(_)
            ))
        ));
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);

        for budget in [calibration_budget, exact_budget, one_under_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn accounted_status_projection_honors_precancellation_without_residue() {
        let runtime = rich_status_projection_runtime();
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let cancellation = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(cancellation.clone())
            .expect("rules status query should admit");
        cancellation.cancel();

        assert!(matches!(
            runtime.status_snapshot_with_execution(&execution),
            Err(RulesStatusProjectionError::QueryBudget(
                tsink::QueryBudgetError::Cancelled
            ))
        ));
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn accounted_status_projection_releases_memory_when_store_lock_is_poisoned() {
        let runtime = rich_status_projection_runtime();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = runtime
                .store
                .state
                .write()
                .expect("rules state should initially be writable");
            panic!("poison rules status source lock");
        }));
        assert!(poisoned.is_err());
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let execution = budget
            .begin_query()
            .expect("rules status query should admit");

        assert!(matches!(
            runtime.status_snapshot_with_execution(&execution),
            Err(RulesStatusProjectionError::StoreReadPoisoned)
        ));
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn metrics_exposition_snapshot_is_copy_and_bypasses_status_tree_limit() {
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let limits = RulesStoreLimits {
            max_snapshot_status_bytes: 1,
            ..test_store_limits()
        };
        let runtime = RulesRuntime::open_with_config(
            None,
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            RulesRuntimeConfig {
                store_limits: limits,
                max_alert_instances_per_rule: limits.max_alert_instances_per_rule,
                ..RulesRuntimeConfig::default()
            },
        )
        .expect("runtime should open");
        runtime
            .store
            .apply_groups(vec![sample_recording_group(
                "metrics-copy",
                "metrics_copy_recording",
            )])
            .expect("rule state should fit independently of the status-tree limit");
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let execution = budget.begin_query().expect("query should admit");
        let before = runtime.store.accounting.snapshot();

        let snapshot = runtime
            .metrics_snapshot_with_execution(&execution)
            .expect("copy-only metrics projection should bypass the status-tree allocation limit");
        assert_eq!(snapshot.metrics.configured_groups, 1);
        assert_eq!(snapshot.metrics.configured_rules, 1);
        assert!(!std::mem::needs_drop::<RulesExpositionSnapshot>());
        let after = runtime.store.accounting.snapshot();
        assert_eq!(
            after.peak_snapshot_status_bytes,
            before.peak_snapshot_status_bytes
        );
        assert_eq!(
            after.snapshot_rejections_total,
            before.snapshot_rejections_total
        );
        assert!(
            runtime.snapshot().is_err(),
            "the ordinary owned status tree must still enforce its one-byte limit"
        );
    }

    #[test]
    fn metrics_exposition_snapshot_uses_forwarded_execution_and_honors_cancellation() {
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let runtime =
            RulesRuntime::open(None, storage, TimestampPrecision::Milliseconds, None, None)
                .expect("runtime should open");
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("query budget should build");
        let cancellation = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(cancellation.clone())
            .expect("one root execution should admit");

        runtime
            .metrics_snapshot_with_execution(&execution)
            .expect("the projection must reuse the forwarded execution");
        let active = budget.snapshot();
        assert_eq!(active.queries_started_total, 1);
        assert_eq!(active.active_queries, 1);
        assert_eq!(active.concurrency_rejections_total, 0);

        cancellation.cancel();
        assert!(matches!(
            runtime.metrics_snapshot_with_execution(&execution),
            Err(RulesExpositionError::QueryBudget(
                tsink::QueryBudgetError::Cancelled
            ))
        ));
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.queries_completed_total, 1);
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    fn write_rules_fixture(path: &Path, state: &PersistedRulesStoreState) -> Vec<u8> {
        let encoded_len =
            measure_rules_store_state(state).expect("fixture state should be measurable");
        let encoded = encode_rules_store_state_exact(state, encoded_len)
            .expect("fixture state should encode");
        std::fs::write(path.join(RULES_STORE_FILE_NAME), &encoded)
            .expect("rules fixture should write");
        encoded
    }

    fn alert_runtime_update(fingerprint: u64, instance_count: usize) -> PersistedRuleRuntimeState {
        let mut alert_instances = Vec::new();
        alert_instances.reserve_exact(instance_count);
        for index in 0..instance_count {
            let labels = vec![
                Label::new("alertname", "HighUsage"),
                Label::new("instance", format!("node-{index:02}")),
            ];
            alert_instances.push(AlertInstanceState {
                key: alert_instance_key("source_metric", &labels),
                source_metric: "source_metric".to_string(),
                labels,
                active_since_timestamp: 60_000,
                last_seen_timestamp: 60_000,
                firing_since_timestamp: Some(60_000),
                state: AlertInstanceStatus::Firing,
                sample_type: "scalar".to_string(),
                sample_value: Some("1".to_string()),
            });
        }
        PersistedRuleRuntimeState {
            fingerprint,
            last_eval_timestamp: Some(60_000),
            last_eval_unix_ms: Some(60_000),
            last_success_unix_ms: Some(60_000),
            last_duration_ms: 1,
            last_error: None,
            last_sample_count: instance_count as u64,
            last_recorded_rows: 0,
            last_outcome: Some(RuleEvaluationOutcome::Success),
            alert_instances,
        }
    }

    fn configured_runtime_identity(store: &RulesStore) -> (String, u64) {
        let state = store.read_state().expect("state should be readable");
        let (rule_id, runtime) = state
            .runtime
            .iter()
            .next()
            .expect("configured rule runtime should exist");
        (rule_id.clone(), runtime.fingerprint)
    }

    fn assert_rules_limit_rejection(error: RulesApplyError, expected: &'static str) {
        match error {
            RulesApplyError::Rejected(message) => assert_eq!(message, expected),
            other => panic!("expected bounded rules rejection, got {other:?}"),
        }
    }

    #[test]
    fn rules_store_count_limits_are_exact_at_n_and_reject_n_plus_one() {
        let one_group = sample_recording_group("group-a", "record_a");
        let mut limits = test_store_limits();
        limits.max_groups = 1;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![one_group.clone()])
            .expect("group count N should pass");
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![
                    one_group.clone(),
                    sample_recording_group("group-b", "record_b"),
                ])
                .expect_err("group count N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );

        let mut two_rules = one_group;
        two_rules.rules.push(RuleSpec::Recording(RecordingRuleSpec {
            record: "record_b".to_string(),
            expr: "source_metric".to_string(),
            interval_secs: None,
            labels: BTreeMap::new(),
        }));
        let mut limits = test_store_limits();
        limits.max_rules_per_group = 1;
        limits.max_rules_total = 1;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![sample_recording_group("group-a", "record_a")])
            .expect("rule count N should pass");
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![two_rules])
                .expect_err("rule count N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );

        let mut limits = test_store_limits();
        limits.max_groups = 2;
        limits.max_rules_per_group = 1;
        limits.max_rules_total = 1;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![sample_recording_group("group-a", "record_a")])
            .expect("total rule count N should pass");
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![
                    sample_recording_group("group-a", "record_a"),
                    sample_recording_group("group-b", "record_b"),
                ])
                .expect_err("total rule count N+1 across groups should fail"),
            RulesLimitSurface::Configuration.message(),
        );
    }

    #[test]
    fn rules_label_expression_and_annotation_byte_limits_are_exact() {
        let exact_name = sample_recording_group("12345678", "record_a");
        let mut limits = test_store_limits();
        limits.max_name_bytes = 8;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![exact_name.clone()])
            .expect("name bytes at N should pass");
        let mut one_more_name_byte = exact_name;
        one_more_name_byte.name.push('9');
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![one_more_name_byte])
                .expect_err("name bytes N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );

        let mut exact_labels = sample_alert_group("alerts", "HighUsage");
        exact_labels.labels = BTreeMap::from([("a".to_string(), "b".to_string())]);
        let mut limits = test_store_limits();
        limits.max_labels_per_set = 1;
        limits.max_label_set_bytes = 2;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![exact_labels.clone()])
            .expect("label count and bytes at N should pass");
        let mut one_more_label = exact_labels.clone();
        one_more_label
            .labels
            .insert("c".to_string(), "d".to_string());
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![one_more_label])
                .expect_err("label count N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );
        let mut one_more_byte = exact_labels;
        one_more_byte
            .labels
            .insert("a".to_string(), "bc".to_string());
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![one_more_byte])
                .expect_err("label bytes N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );

        let exact_expression = sample_alert_group("alerts", "HighUsage");
        let expression_len = rule_expr(&exact_expression.rules[0]).len();
        let mut limits = test_store_limits();
        limits.max_expression_bytes = expression_len;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![exact_expression.clone()])
            .expect("expression bytes at N should pass");
        let mut one_more_expression_byte = exact_expression;
        match &mut one_more_expression_byte.rules[0] {
            RuleSpec::Alert(spec) => spec.expr.push(' '),
            RuleSpec::Recording(_) => unreachable!("sample group is an alert"),
        }
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![one_more_expression_byte])
                .expect_err("expression bytes N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );

        let mut exact_annotations = sample_alert_group("alerts", "HighUsage");
        match &mut exact_annotations.rules[0] {
            RuleSpec::Alert(spec) => {
                spec.annotations = BTreeMap::from([("note".to_string(), "ok".to_string())]);
            }
            RuleSpec::Recording(_) => unreachable!("sample group is an alert"),
        }
        let mut limits = test_store_limits();
        limits.max_annotation_bytes = 6;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![exact_annotations.clone()])
            .expect("annotation bytes at N should pass");
        match &mut exact_annotations.rules[0] {
            RuleSpec::Alert(spec) => {
                spec.annotations
                    .insert("note".to_string(), "okay".to_string());
            }
            RuleSpec::Recording(_) => unreachable!("sample group is an alert"),
        }
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![exact_annotations])
                .expect_err("annotation bytes N+1 should fail"),
            RulesLimitSurface::Configuration.message(),
        );
    }

    #[test]
    fn rules_limits_validate_and_hostile_configuration_errors_do_not_echo_input() {
        let mut zero = test_store_limits();
        zero.max_groups = 0;
        assert!(zero.validate().is_err());

        let mut inconsistent = test_store_limits();
        inconsistent.max_rules_per_group = 2;
        inconsistent.max_rules_total = 1;
        assert!(inconsistent.validate().is_err());

        let mut oversized_status = test_store_limits();
        oversized_status.max_snapshot_status_bytes = crate::http::MAX_BODY_BYTES + 1;
        assert!(oversized_status.validate().is_err());

        let invalid_runtime = RulesRuntimeConfig {
            scheduler_tick: Duration::ZERO,
            store_limits: test_store_limits(),
            max_alert_instances_per_rule: test_store_limits().max_alert_instances_per_rule,
            ..RulesRuntimeConfig::default()
        };
        assert!(invalid_runtime.validate().is_err());
        let inconsistent_runtime = RulesRuntimeConfig {
            store_limits: RulesStoreLimits {
                max_alert_instances_per_rule: 1,
                ..test_store_limits()
            },
            max_alert_instances_per_rule: 2,
            ..RulesRuntimeConfig::default()
        };
        assert!(inconsistent_runtime.validate().is_err());

        let attacker = "hostile-expression-secret-marker{";
        let mut hostile = sample_recording_group("hostile", "hostile_recording");
        match &mut hostile.rules[0] {
            RuleSpec::Recording(spec) => spec.expr = attacker.to_string(),
            RuleSpec::Alert(_) => unreachable!("sample group is a recording rule"),
        }
        let store = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        let error = store
            .apply_groups(vec![hostile])
            .expect_err("invalid hostile expression should fail")
            .to_string();
        assert!(!error.contains(attacker));
        assert!(error.len() <= RULES_MAX_DIAGNOSTIC_BYTES);
        assert!(store
            .read_state()
            .expect("state should remain readable")
            .groups
            .is_empty());
    }

    #[test]
    fn rules_retained_and_durable_byte_limits_are_exact() {
        let group = sample_recording_group("bounded", "bounded_recording");
        let measured = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        measured
            .apply_groups(vec![group.clone()])
            .expect("measurement state should apply");
        let exact_retained = usize::try_from(measured.accounting.snapshot().retained_state_bytes)
            .expect("retained byte measurement should fit usize");
        assert!(exact_retained > 1);

        let mut exact_limits = test_store_limits();
        exact_limits.max_total_retained_state_bytes = exact_retained;
        RulesStore::open_with_limits(None, None, exact_limits)
            .expect("exact retained store should open")
            .apply_groups(vec![group.clone()])
            .expect("retained bytes N should pass");

        let mut one_under_limits = exact_limits;
        one_under_limits.max_total_retained_state_bytes = exact_retained - 1;
        let one_under =
            RulesStore::open_with_limits(None, None, one_under_limits).expect("store should open");
        assert_rules_limit_rejection(
            one_under
                .apply_groups(vec![group.clone()])
                .expect_err("retained bytes N-1 should fail"),
            RulesLimitSurface::RetainedState.message(),
        );
        assert!(one_under
            .read_state()
            .expect("state should remain readable")
            .groups
            .is_empty());

        let measured_dir = tempdir().expect("measurement directory should build");
        let measured =
            RulesStore::open_with_limits(Some(measured_dir.path()), None, test_store_limits())
                .expect("durable measurement store should open");
        measured
            .apply_groups(vec![group.clone()])
            .expect("durable measurement should apply");
        let exact_durable = usize::try_from(measured.accounting.snapshot().durable_file_bytes)
            .expect("durable byte measurement should fit usize");
        assert!(exact_durable > 1);

        let exact_dir = tempdir().expect("exact directory should build");
        let mut exact_limits = test_store_limits();
        exact_limits.max_durable_file_bytes = exact_durable;
        RulesStore::open_with_limits(Some(exact_dir.path()), None, exact_limits)
            .expect("exact durable store should open")
            .apply_groups(vec![group.clone()])
            .expect("durable bytes N should pass");

        let one_under_dir = tempdir().expect("one-under directory should build");
        let mut one_under_limits = exact_limits;
        one_under_limits.max_durable_file_bytes = exact_durable - 1;
        let one_under =
            RulesStore::open_with_limits(Some(one_under_dir.path()), None, one_under_limits)
                .expect("one-under durable store should open");
        assert_rules_limit_rejection(
            one_under
                .apply_groups(vec![group])
                .expect_err("durable bytes N-1 should fail"),
            RulesLimitSurface::DurableFile.message(),
        );
        assert!(one_under
            .read_state()
            .expect("state should remain readable")
            .groups
            .is_empty());
    }

    #[test]
    fn replacement_peak_is_larger_than_new_state_and_fails_without_publication() {
        let initial = sample_recording_group("initial", "initial_recording");
        let replacement = sample_recording_group("replacement", "replacement_recording");

        let new_store = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        new_store
            .apply_groups(vec![initial.clone()])
            .expect("new state should apply");
        let new_peak = usize::try_from(
            new_store
                .accounting
                .snapshot()
                .peak_replacement_transient_bytes,
        )
        .expect("new peak should fit usize");

        let measured = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        measured
            .apply_groups(vec![initial.clone()])
            .expect("initial state should apply");
        measured
            .apply_groups(vec![replacement.clone()])
            .expect("replacement should apply");
        let replacement_peak = usize::try_from(
            measured
                .accounting
                .snapshot()
                .peak_replacement_transient_bytes,
        )
        .expect("replacement peak should fit usize");
        assert!(replacement_peak > new_peak);

        let mut limits = test_store_limits();
        limits.max_replacement_transient_bytes = replacement_peak - 1;
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        store
            .apply_groups(vec![initial.clone()])
            .expect("new state must fit below replacement peak");
        assert_rules_limit_rejection(
            store
                .apply_groups(vec![replacement])
                .expect_err("replacement peak N-1 should fail"),
            RulesLimitSurface::Replacement.message(),
        );
        let state = store.read_state().expect("state should remain readable");
        assert_eq!(state.groups[0].name, initial.name);
    }

    #[test]
    fn retained_accounting_uses_owned_string_and_vector_capacities() {
        let tight = sample_recording_group("capacity", "capacity_recording");
        let tight_store = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        tight_store
            .apply_groups(vec![tight])
            .expect("tight state should apply");
        let tight_bytes = tight_store.accounting.snapshot().retained_state_bytes;

        let mut name = String::with_capacity(4 * 1024);
        name.push_str("capacity");
        let mut record = String::with_capacity(4 * 1024);
        record.push_str("capacity_recording");
        let mut rules = Vec::with_capacity(8);
        rules.push(RuleSpec::Recording(RecordingRuleSpec {
            record,
            expr: "source_metric".to_string(),
            interval_secs: None,
            labels: BTreeMap::new(),
        }));
        let spare_group = RuleGroupSpec {
            name,
            tenant_id: "team-a".to_string(),
            interval_secs: 60,
            labels: BTreeMap::new(),
            rules,
        };
        let mut groups = Vec::with_capacity(8);
        groups.push(spare_group);
        let spare_bytes = modeled_groups_bytes(&groups)
            .saturating_add(std::mem::size_of::<PersistedRulesStoreState>());
        assert!(saturating_u64(spare_bytes) > tight_bytes);

        let mut limits = test_store_limits();
        limits.max_total_retained_state_bytes =
            usize::try_from(tight_bytes).expect("tight bytes should fit usize");
        let store =
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open");
        assert_rules_limit_rejection(
            store
                .apply_groups(groups)
                .expect_err("spare caller capacity must be retained-accounted"),
            RulesLimitSurface::RetainedState.message(),
        );
    }

    #[test]
    fn startup_read_decode_and_retained_peaks_are_bounded_exactly() {
        let source = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        source
            .apply_groups(vec![sample_recording_group("startup", "startup_recording")])
            .expect("fixture state should apply");
        let fixture_state = source
            .read_state()
            .expect("fixture state should be readable")
            .clone();
        let fixture_dir = tempdir().expect("fixture directory should build");
        let raw = write_rules_fixture(fixture_dir.path(), &fixture_state);
        let decoded_upper =
            preflight_rules_json(&raw).expect("fixture JSON should pass structural preflight");
        let exact_startup_peak = raw.len().saturating_add(decoded_upper);

        let mut exact_limits = test_store_limits();
        exact_limits.max_startup_transient_bytes = exact_startup_peak;
        let exact = RulesStore::open_with_limits(Some(fixture_dir.path()), None, exact_limits)
            .expect("startup transient bytes N should pass");
        assert_eq!(
            exact.accounting.snapshot().peak_startup_transient_bytes,
            saturating_u64(exact_startup_peak)
        );
        assert_eq!(
            exact
                .read_state()
                .expect("reopened state should be readable")
                .groups
                .len(),
            1
        );

        let mut one_under_limits = exact_limits;
        one_under_limits.max_startup_transient_bytes = exact_startup_peak - 1;
        let error = RulesStore::open_with_limits(Some(fixture_dir.path()), None, one_under_limits)
            .err()
            .expect("startup transient bytes N-1 should fail");
        assert_eq!(error, RulesLimitSurface::Startup.message());
    }

    #[test]
    fn corrupt_oversized_and_deep_rules_startup_inputs_fail_closed_without_echo() {
        let corrupt = tempdir().expect("corrupt directory should build");
        let attacker = "startup-secret-marker";
        std::fs::write(
            corrupt.path().join(RULES_STORE_FILE_NAME),
            format!("{{\"{attacker}\":"),
        )
        .expect("corrupt fixture should write");
        let error = RulesStore::open_with_limits(Some(corrupt.path()), None, test_store_limits())
            .err()
            .expect("corrupt startup should fail");
        assert!(!error.contains(attacker));
        assert!(error.len() <= RULES_MAX_DIAGNOSTIC_BYTES);

        let oversized = tempdir().expect("oversized directory should build");
        std::fs::write(oversized.path().join(RULES_STORE_FILE_NAME), vec![b'x'; 65])
            .expect("oversized fixture should write");
        let mut limits = test_store_limits();
        limits.max_durable_file_bytes = 64;
        let error = RulesStore::open_with_limits(Some(oversized.path()), None, limits)
            .err()
            .expect("oversized startup should fail");
        assert_eq!(error, RulesLimitSurface::DurableFile.message());

        let deep = tempdir().expect("deep directory should build");
        let mut deeply_nested = vec![b'['; RULES_STARTUP_MAX_JSON_DEPTH + 1];
        deeply_nested.extend(std::iter::repeat_n(b']', RULES_STARTUP_MAX_JSON_DEPTH + 1));
        std::fs::write(deep.path().join(RULES_STORE_FILE_NAME), deeply_nested)
            .expect("deep fixture should write");
        let error = RulesStore::open_with_limits(Some(deep.path()), None, test_store_limits())
            .err()
            .expect("deep startup should fail");
        assert_eq!(error, "rules store JSON exceeds its nesting-depth limit");
    }

    #[test]
    fn runtime_update_and_alert_growth_limits_are_exact_and_atomic() {
        let group = sample_alert_group("alerts", "HighUsage");
        let measured = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        measured
            .apply_groups(vec![group.clone()])
            .expect("alert group should apply");
        let (rule_id, fingerprint) = configured_runtime_identity(&measured);
        measured
            .apply_runtime_updates(vec![(rule_id, alert_runtime_update(fingerprint, 1))])
            .expect("measurement update should apply");
        let exact_peak = usize::try_from(
            measured
                .accounting
                .snapshot()
                .peak_runtime_update_transient_bytes,
        )
        .expect("runtime peak should fit usize");
        assert!(exact_peak > 1);

        let mut exact_limits = test_store_limits();
        exact_limits.max_runtime_update_transient_bytes = exact_peak;
        let exact =
            RulesStore::open_with_limits(None, None, exact_limits).expect("store should open");
        exact
            .apply_groups(vec![group.clone()])
            .expect("alert group should apply");
        let (rule_id, fingerprint) = configured_runtime_identity(&exact);
        assert_eq!(
            exact
                .apply_runtime_updates(vec![(rule_id, alert_runtime_update(fingerprint, 1),)])
                .expect("runtime update bytes N should pass"),
            1
        );

        let mut one_under_limits = exact_limits;
        one_under_limits.max_runtime_update_transient_bytes = exact_peak - 1;
        let one_under =
            RulesStore::open_with_limits(None, None, one_under_limits).expect("store should open");
        one_under
            .apply_groups(vec![group.clone()])
            .expect("alert group should apply");
        let (rule_id, fingerprint) = configured_runtime_identity(&one_under);
        let error = one_under
            .apply_runtime_updates(vec![(
                rule_id.clone(),
                alert_runtime_update(fingerprint, 1),
            )])
            .expect_err("runtime update bytes N-1 should fail");
        assert_eq!(
            error.to_string(),
            RulesLimitSurface::RuntimeUpdate.message()
        );
        assert!(one_under
            .read_state()
            .expect("state should remain readable")
            .runtime
            .get(&rule_id)
            .expect("runtime should remain configured")
            .alert_instances
            .is_empty());

        let mut count_limits = test_store_limits();
        count_limits.max_alert_instances_per_rule = 1;
        let count_store =
            RulesStore::open_with_limits(None, None, count_limits).expect("store should open");
        count_store
            .apply_groups(vec![group])
            .expect("alert group should apply");
        let (rule_id, fingerprint) = configured_runtime_identity(&count_store);
        assert_eq!(
            count_store
                .apply_runtime_updates(vec![(
                    rule_id.clone(),
                    alert_runtime_update(fingerprint, 1),
                )])
                .expect("alert count N should pass"),
            1
        );
        let error = count_store
            .apply_runtime_updates(vec![(
                rule_id.clone(),
                alert_runtime_update(fingerprint, 2),
            )])
            .expect_err("alert count N+1 should fail");
        assert_eq!(
            error.to_string(),
            RulesLimitSurface::RuntimeUpdate.message()
        );
        assert_eq!(
            count_store
                .read_state()
                .expect("state should remain readable")
                .runtime
                .get(&rule_id)
                .expect("runtime should remain configured")
                .alert_instances
                .len(),
            1
        );
    }

    #[test]
    fn concurrent_runtime_updates_cannot_spend_the_same_final_retained_bytes() {
        let mut group = sample_alert_group("alerts", "HighUsage");
        group.rules.push(RuleSpec::Alert(AlertRuleSpec {
            alert: "HighLatency".to_string(),
            expr: "source_metric > 0".to_string(),
            interval_secs: None,
            for_secs: 0,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        }));
        group.rules.shrink_to_fit();

        let measured = RulesStore::open_with_limits(None, None, test_store_limits())
            .expect("store should open");
        measured
            .apply_groups(vec![group.clone()])
            .expect("measurement group should apply");
        let state = measured.read_state().expect("state should be readable");
        let identities = state
            .runtime
            .iter()
            .map(|(rule_id, runtime)| (rule_id.clone(), runtime.fingerprint))
            .collect::<Vec<_>>();
        assert_eq!(identities.len(), 2);
        drop(state);
        measured
            .apply_runtime_updates(vec![(
                identities[0].0.clone(),
                alert_runtime_update(identities[0].1, 1),
            )])
            .expect("first measurement update should apply");
        let first_update_bytes = modeled_rules_state_bytes(
            &measured
                .read_state()
                .expect("first measurement state should be readable"),
        );
        measured
            .apply_runtime_updates(vec![(
                identities[1].0.clone(),
                alert_runtime_update(identities[1].1, 1),
            )])
            .expect("second measurement update should apply");
        let both_update_bytes = modeled_rules_state_bytes(
            &measured
                .read_state()
                .expect("combined measurement state should be readable"),
        );
        assert!(both_update_bytes > first_update_bytes);

        let mut limits = test_store_limits();
        limits.max_total_retained_state_bytes = first_update_bytes;
        let store = Arc::new(
            RulesStore::open_with_limits(None, None, limits).expect("bounded store should open"),
        );
        store
            .apply_groups(vec![group])
            .expect("baseline group should apply");
        let barrier = Arc::new(Barrier::new(3));
        let handles = identities
            .into_iter()
            .map(|(rule_id, fingerprint)| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    store.apply_runtime_updates(vec![(
                        rule_id,
                        alert_runtime_update(fingerprint, 1),
                    )])
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("runtime update thread should join"))
            .collect::<Vec<_>>();
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "concurrent update results: {results:?}"
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    result.as_ref().is_err_and(|error| {
                        error.to_string() == RulesLimitSurface::RuntimeUpdate.message()
                    })
                })
                .count(),
            1
        );
        let state = store.read_state().expect("state should remain readable");
        assert_eq!(
            state
                .runtime
                .values()
                .map(|runtime| runtime.alert_instances.len())
                .sum::<usize>(),
            1
        );
        assert!(modeled_rules_state_bytes(&state) <= limits.max_total_retained_state_bytes);
        let accounting = store.accounting.snapshot();
        assert_eq!(accounting.runtime_update_rejections_total, 1);
        assert_eq!(
            accounting.retained_state_bytes,
            saturating_u64(first_update_bytes)
        );
    }

    #[test]
    fn status_output_limit_is_exact_and_caller_owned() {
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let config = RulesRuntimeConfig {
            store_limits: test_store_limits(),
            max_alert_instances_per_rule: test_store_limits().max_alert_instances_per_rule,
            ..RulesRuntimeConfig::default()
        };
        let measured = RulesRuntime::open_with_config(
            None,
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            config,
        )
        .expect("runtime should open");
        let mut measured_snapshot = measured
            .apply_groups(vec![sample_recording_group("status", "status_recording")])
            .expect("measurement group should apply");
        let measured_body = measured
            .encode_success_snapshot(&mut measured_snapshot)
            .expect("measurement status should encode");
        assert!(measured_body.len() <= crate::http::MAX_BODY_BYTES);
        let measured_json: serde_json::Value = serde_json::from_slice(&measured_body)
            .expect("measurement status should be valid JSON");
        assert_eq!(measured_json["status"], "success");
        let exact_output = usize::try_from(
            measured
                .store
                .accounting
                .snapshot()
                .peak_snapshot_status_bytes,
        )
        .expect("status output should fit usize");

        let exact_config = RulesRuntimeConfig {
            store_limits: RulesStoreLimits {
                max_snapshot_status_bytes: exact_output,
                ..test_store_limits()
            },
            max_alert_instances_per_rule: test_store_limits().max_alert_instances_per_rule,
            ..RulesRuntimeConfig::default()
        };
        let exact = RulesRuntime::open_with_config(
            None,
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            exact_config,
        )
        .expect("runtime should open");
        let mut snapshot = exact
            .apply_groups(vec![sample_recording_group("status", "status_recording")])
            .expect("status snapshot allocation should pass");
        let body = exact
            .encode_success_snapshot(&mut snapshot)
            .expect("snapshot plus encoded response bytes N should pass");
        assert!(body.len() <= crate::http::MAX_BODY_BYTES);
        assert_eq!(
            snapshot.metrics.peak_snapshot_status_bytes,
            saturating_u64(exact_output)
        );

        let one_under_config = RulesRuntimeConfig {
            store_limits: RulesStoreLimits {
                max_snapshot_status_bytes: exact_output - 1,
                ..test_store_limits()
            },
            max_alert_instances_per_rule: test_store_limits().max_alert_instances_per_rule,
            ..RulesRuntimeConfig::default()
        };
        let one_under = RulesRuntime::open_with_config(
            None,
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            one_under_config,
        )
        .expect("runtime should open");
        one_under
            .store
            .apply_groups(vec![sample_recording_group("status", "status_recording")])
            .expect("state publication should fit independently");
        let mut snapshot = one_under
            .snapshot()
            .expect("snapshot allocation should fit below the combined response peak");
        let error = one_under
            .encode_success_snapshot(&mut snapshot)
            .expect_err("snapshot plus encoded response bytes N-1 should fail");
        assert_eq!(error, RulesLimitSurface::SnapshotStatus.message());
        assert_eq!(
            one_under
                .store
                .accounting
                .snapshot()
                .snapshot_rejections_total,
            1
        );
    }

    #[test]
    fn disk_quota_failure_does_not_publish_candidate_rules() {
        let temp_dir = tempdir().expect("temp dir should build");
        let initial_group = sample_recording_group("initial", "initial_recording");
        let store = RulesStore::open(Some(temp_dir.path())).expect("rules store should open");
        store
            .apply_groups(vec![initial_group.clone()])
            .expect("initial rules should persist");
        drop(store);

        let store_path = temp_dir.path().join(RULES_STORE_FILE_NAME);
        let initial_bytes = std::fs::read(&store_path).expect("initial rules should exist");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(initial_bytes.len() as u64 + 1),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let runtime = RulesRuntime::open_with_disk_budget(
            Some(temp_dir.path()),
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
            Some(Arc::clone(&budget)),
        )
        .expect("runtime should open");

        let err = runtime
            .apply_groups(vec![sample_recording_group(
                "replacement",
                "replacement_recording",
            )])
            .expect_err("replacement should exceed the atomic-write peak quota");
        assert!(matches!(
            err,
            RulesApplyError::Persistence(tsink::TsinkError::DiskQuotaExceeded { .. })
        ));

        let snapshot = runtime
            .snapshot()
            .expect("live state should remain readable");
        assert_eq!(snapshot.groups.len(), 1);
        assert_eq!(snapshot.groups[0].name, initial_group.name);
        assert_eq!(
            std::fs::read(&store_path).expect("persisted rules should remain readable"),
            initial_bytes
        );
        let rules_accounting = runtime.store.accounting.snapshot();
        assert_eq!(rules_accounting.persistence_failures_total, 1);
        assert_eq!(
            rules_accounting.durable_file_bytes,
            u64::try_from(initial_bytes.len()).expect("initial file length should fit u64")
        );
        let disk = budget.snapshot();
        assert_eq!(disk.active_reservations, 0);
        assert_eq!(disk.reserved_bytes, 0);
        assert_eq!(disk.rejections_total, 1);
    }

    #[test]
    fn runtime_persistence_failure_stays_typed_and_does_not_publish() {
        let temp_dir = tempdir().expect("temp dir should build");
        let group = sample_alert_group("alerts", "HighUsage");
        let bootstrap =
            RulesStore::open(Some(temp_dir.path())).expect("bootstrap store should open");
        bootstrap
            .apply_groups(vec![group])
            .expect("initial alert rule should persist");
        drop(bootstrap);

        let store_path = temp_dir.path().join(RULES_STORE_FILE_NAME);
        let initial_bytes = std::fs::read(&store_path).expect("initial state should be readable");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(
                    u64::try_from(initial_bytes.len())
                        .expect("initial length should fit u64")
                        .saturating_add(1),
                ),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = RulesStore::open_with_limits(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
            test_store_limits(),
        )
        .expect("bounded store should reopen");
        let before = store.accounting.snapshot();
        let (rule_id, fingerprint) = configured_runtime_identity(&store);
        let error = store
            .apply_runtime_updates(vec![(
                rule_id.clone(),
                alert_runtime_update(fingerprint, 1),
            )])
            .expect_err("runtime persistence should exceed atomic-write quota");
        assert!(matches!(
            error,
            RulesStoreError::Persistence(tsink::TsinkError::DiskQuotaExceeded { .. })
        ));
        let state = store.read_state().expect("state should remain readable");
        assert!(state
            .runtime
            .get(&rule_id)
            .expect("runtime should remain configured")
            .alert_instances
            .is_empty());
        drop(state);
        assert_eq!(
            std::fs::read(&store_path).expect("durable state should remain readable"),
            initial_bytes
        );
        let after = store.accounting.snapshot();
        assert_eq!(after.retained_state_bytes, before.retained_state_bytes);
        assert_eq!(after.durable_file_bytes, before.durable_file_bytes);
        assert_eq!(after.persistence_failures_total, 1);
        assert_eq!(budget.snapshot().active_reservations, 0);
    }

    #[test]
    fn budgeted_rules_are_accounted_exactly_and_reopen() {
        let temp_dir = tempdir().expect("temp dir should build");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(128 * 1024),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let expected_group = sample_recording_group("persisted", "persisted_recording");
        let runtime = RulesRuntime::open_with_disk_budget(
            Some(temp_dir.path()),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
            Some(Arc::clone(&budget)),
        )
        .expect("runtime should open");
        runtime
            .apply_groups(vec![expected_group.clone()])
            .expect("rules should persist");

        let store_path = temp_dir.path().join(RULES_STORE_FILE_NAME);
        let store_bytes = std::fs::metadata(&store_path)
            .expect("rules store should exist")
            .len();
        let disk = budget.snapshot();
        assert_eq!(disk.accounted_bytes, store_bytes);
        assert_eq!(
            disk.categories
                .iter()
                .find(|usage| usage.category == DiskCategory::ServerState)
                .map(|usage| usage.bytes),
            Some(store_bytes)
        );

        let snapshot_dir = tempdir().expect("snapshot dir should build");
        runtime
            .snapshot_into(snapshot_dir.path())
            .expect("external snapshot should succeed");
        let snapshot_file = snapshot_dir.path().join(RULES_STORE_FILE_NAME);
        assert!(snapshot_file.exists());
        assert_eq!(
            std::fs::metadata(&snapshot_file)
                .expect("snapshot file should be readable")
                .len(),
            store_bytes
        );
        let live_status = runtime.snapshot().expect("live status should be readable");
        assert_eq!(live_status.metrics.durable_file_bytes, store_bytes);
        assert!(live_status.metrics.peak_snapshot_file_bytes > store_bytes);
        assert_eq!(budget.snapshot().accounted_bytes, store_bytes);

        drop(runtime);
        drop(budget);
        let reopened_budget = LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(128 * 1024),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should reopen");
        let reopened = RulesRuntime::open_with_disk_budget(
            Some(temp_dir.path()),
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
            Some(Arc::clone(&reopened_budget)),
        )
        .expect("rules runtime should reopen");
        let snapshot = reopened.snapshot().expect("reopened state should load");
        assert_eq!(snapshot.groups.len(), 1);
        assert_eq!(snapshot.groups[0].name, expected_group.name);
        assert_eq!(snapshot.metrics.durable_file_bytes, store_bytes);
        assert!(snapshot.metrics.retained_state_bytes > 0);
        assert!(snapshot.metrics.peak_startup_transient_bytes >= store_bytes);
        assert_eq!(reopened_budget.snapshot().accounted_bytes, store_bytes);
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
    async fn recording_rule_final_checkpoint_quota_failure_does_not_replay_rows_or_usage_after_reopen(
    ) {
        let rules_dir = tempdir().expect("rules temp dir should build");
        let group = sample_recording_group("recording", "recorded_metric");
        let bootstrap_store =
            RulesStore::open(Some(rules_dir.path())).expect("rules store should open");
        bootstrap_store
            .apply_groups(vec![group.clone()])
            .expect("initial rules should persist");
        let initial_state = bootstrap_store
            .snapshot()
            .expect("initial rules state should be readable");
        let rule = &group.rules[0];
        let rule_id = rule_id(&group, rule);
        let fingerprint = rule_fingerprint(&group, rule).expect("rule should fingerprint");
        let initial_runtime = initial_state
            .runtime
            .get(&rule_id)
            .cloned()
            .expect("initial runtime state should exist");
        let attempt = recording_rule_attempt_state(
            initial_runtime,
            fingerprint,
            60_000,
            unix_timestamp_millis(),
        );
        let mut attempt_candidate = initial_state.clone();
        attempt_candidate
            .runtime
            .insert(rule_id.clone(), attempt.clone());
        let initial_len = encoded_rules_store_len(&initial_state);
        let attempt_len = encoded_rules_store_len(&attempt_candidate);

        let mut successful_candidate = attempt_candidate;
        let successful_runtime = successful_candidate
            .runtime
            .get_mut(&rule_id)
            .expect("candidate runtime should exist");
        successful_runtime.last_success_unix_ms = Some(unix_timestamp_millis());
        successful_runtime.last_error = None;
        successful_runtime.last_sample_count = 1;
        successful_runtime.last_recorded_rows = 1;
        successful_runtime.last_outcome = Some(RuleEvaluationOutcome::Success);
        assert!(
            encoded_rules_store_len(&successful_candidate) > initial_len,
            "the final checkpoint must require more than the post-attempt quota headroom"
        );
        drop(bootstrap_store);

        let budget_limit = initial_len
            .checked_add(attempt_len)
            .expect("rules quota should fit u64");
        let budget = LocalDiskBudget::open(
            rules_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(budget_limit),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");

        let inner_storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        inner_storage
            .insert_rows(&[Row::with_labels(
                "source_metric",
                vec![
                    Label::new("host", "a"),
                    Label::new(tenant::TENANT_LABEL, "team-a"),
                ],
                DataPoint::new(60_000, 7.0),
            )])
            .expect("seed write should succeed");
        let inserted_rows = Arc::new(AtomicU64::new(0));
        let storage: Arc<dyn Storage> = Arc::new(CountingInsertStorage {
            inner: inner_storage,
            inserted_rows: Arc::clone(&inserted_rows),
        });
        let usage_accounting = UsageAccounting::open(None).expect("usage store should open");
        let runtime = RulesRuntime::open_with_disk_budget(
            Some(rules_dir.path()),
            Arc::clone(&storage),
            TimestampPrecision::Milliseconds,
            None,
            Some(Arc::clone(&usage_accounting)),
            Some(Arc::clone(&budget)),
        )
        .expect("runtime should open");

        runtime.run_due_at(60_000).await;

        assert_eq!(inserted_rows.load(Ordering::SeqCst), 1);
        let usage = usage_accounting.tenant_summary("team-a");
        assert_eq!(usage.background.events_total, 1);
        assert_eq!(usage.background.rows, 1);
        let live = runtime.snapshot().expect("live status should be readable");
        assert_eq!(live.metrics.evaluated_rules_total, 1);
        assert_eq!(live.metrics.evaluation_failures_total, 1);
        assert_eq!(live.metrics.recording_rows_written_total, 1);
        assert!(
            live.metrics
                .last_error
                .as_deref()
                .is_some_and(|err| err.contains("rules state persist failed")),
            "final checkpoint failure should be visible in runtime status: {:?}",
            live.metrics.last_error
        );
        let live_rule = &live.groups[0].rules[0];
        assert_eq!(live_rule.last_eval_timestamp, Some(60_000));
        assert_eq!(live_rule.state, "error");
        assert_eq!(
            live_rule.last_error.as_deref(),
            Some(RECORDING_RULE_ATTEMPT_PENDING)
        );

        let persisted = load_rules_store_state(&rules_dir.path().join(RULES_STORE_FILE_NAME))
            .expect("attempt checkpoint should remain readable");
        let persisted_attempt = persisted
            .runtime
            .get(&rule_id)
            .expect("attempt checkpoint should remain persisted");
        assert_eq!(persisted_attempt.last_eval_timestamp, Some(60_000));
        assert_eq!(
            persisted_attempt.last_error.as_deref(),
            Some(RECORDING_RULE_ATTEMPT_PENDING)
        );
        assert_eq!(budget.snapshot().rejections_total, 1);

        drop(runtime);
        drop(budget);
        let reopened_budget = LocalDiskBudget::open(
            rules_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(budget_limit),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should reopen");
        let reopened = RulesRuntime::open_with_disk_budget(
            Some(rules_dir.path()),
            storage,
            TimestampPrecision::Milliseconds,
            None,
            Some(Arc::clone(&usage_accounting)),
            Some(reopened_budget),
        )
        .expect("rules runtime should reopen");

        reopened.run_due_at(60_500).await;

        assert_eq!(inserted_rows.load(Ordering::SeqCst), 1);
        let usage = usage_accounting.tenant_summary("team-a");
        assert_eq!(usage.background.events_total, 1);
        assert_eq!(usage.background.rows, 1);
        let reopened_status = reopened
            .snapshot()
            .expect("reopened status should be readable");
        let reopened_rule = &reopened_status.groups[0].rules[0];
        assert_eq!(reopened_rule.last_eval_timestamp, Some(60_000));
        assert_eq!(reopened_rule.state, "error");
        assert_eq!(
            reopened_rule.last_error.as_deref(),
            Some(RECORDING_RULE_ATTEMPT_PENDING)
        );
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
