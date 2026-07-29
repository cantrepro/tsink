use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tsink::{
    DiskCategory, LocalDiskBudget, QueryBudgetError, QueryExecution, QueryMemoryReservation,
    TsinkError,
};

pub const CLUSTER_AUDIT_RETENTION_SECS_ENV: &str = "TSINK_CLUSTER_AUDIT_RETENTION_SECS";
pub const CLUSTER_AUDIT_MAX_LOG_BYTES_ENV: &str = "TSINK_CLUSTER_AUDIT_MAX_LOG_BYTES";
pub const CLUSTER_AUDIT_MAX_QUERY_LIMIT_ENV: &str = "TSINK_CLUSTER_AUDIT_MAX_QUERY_LIMIT";

const DEFAULT_AUDIT_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_AUDIT_MAX_LOG_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_AUDIT_MAX_QUERY_LIMIT: usize = 1000;
const DEFAULT_AUDIT_QUERY_LIMIT: usize = 100;
const AUDIT_STATUS_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterAuditConfig {
    pub retention_secs: u64,
    pub max_log_bytes: u64,
    pub max_query_limit: usize,
}

impl Default for ClusterAuditConfig {
    fn default() -> Self {
        Self {
            retention_secs: DEFAULT_AUDIT_RETENTION_SECS,
            max_log_bytes: DEFAULT_AUDIT_MAX_LOG_BYTES,
            max_query_limit: DEFAULT_AUDIT_MAX_QUERY_LIMIT,
        }
    }
}

impl ClusterAuditConfig {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            retention_secs: parse_env_u64(
                CLUSTER_AUDIT_RETENTION_SECS_ENV,
                defaults.retention_secs,
                true,
            )?,
            max_log_bytes: parse_env_u64(
                CLUSTER_AUDIT_MAX_LOG_BYTES_ENV,
                defaults.max_log_bytes,
                true,
            )?,
            max_query_limit: parse_env_u64(
                CLUSTER_AUDIT_MAX_QUERY_LIMIT_ENV,
                defaults.max_query_limit as u64,
                true,
            )? as usize,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterAuditActor {
    pub id: String,
    pub auth_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterAuditOutcome {
    pub status: String,
    pub http_status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterAuditRecord {
    pub id: u64,
    pub timestamp_unix_ms: u64,
    pub operation: String,
    pub actor: ClusterAuditActor,
    pub target: JsonValue,
    pub outcome: ClusterAuditOutcome,
}

#[derive(Debug, Clone)]
pub struct ClusterAuditEntryInput {
    pub timestamp_unix_ms: Option<u64>,
    pub operation: String,
    pub actor: ClusterAuditActor,
    pub target: JsonValue,
    pub outcome: ClusterAuditOutcome,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterAuditQuery {
    pub operation: Option<String>,
    pub actor_id: Option<String>,
    pub status: Option<String>,
    pub since_unix_ms: Option<u64>,
    pub until_unix_ms: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterAuditHealthSnapshot {
    pub enabled: bool,
    pub retained_entries: u64,
    pub log_bytes: u64,
    pub cleanup_pending: bool,
    pub last_cleanup_error: Option<String>,
    pub persistence_fenced: bool,
    pub persistence_fence_reason: Option<String>,
    pub degraded: bool,
}

#[derive(Debug)]
#[must_use = "dropping the audit-health snapshot releases its query-memory reservation"]
pub struct AccountedClusterAuditHealthSnapshot {
    snapshot: ClusterAuditHealthSnapshot,
    _reservation: QueryMemoryReservation,
}

impl AccountedClusterAuditHealthSnapshot {
    #[cfg(test)]
    fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

impl std::ops::Deref for AccountedClusterAuditHealthSnapshot {
    type Target = ClusterAuditHealthSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClusterAuditMetricsSnapshot {
    pub enabled: bool,
    pub retained_entries: u64,
    pub log_bytes: u64,
    pub cleanup_pending: bool,
    pub persistence_fenced: bool,
    pub degraded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterAuditPersistenceStage {
    Encode,
    Append,
    Compact,
}

impl ClusterAuditPersistenceStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Encode => "record encoding",
            Self::Append => "record append",
            Self::Compact => "log compaction",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterAuditDiskResourceLimit {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterAuditAppendError {
    stage: ClusterAuditPersistenceStage,
    detail: String,
    resource_limit: Option<ClusterAuditDiskResourceLimit>,
    indeterminate: bool,
}

impl ClusterAuditAppendError {
    fn new(stage: ClusterAuditPersistenceStage, detail: impl std::fmt::Display) -> Self {
        Self {
            stage,
            detail: detail.to_string(),
            resource_limit: None,
            indeterminate: false,
        }
    }

    fn indeterminate(stage: ClusterAuditPersistenceStage, detail: impl std::fmt::Display) -> Self {
        Self {
            stage,
            detail: detail.to_string(),
            resource_limit: None,
            indeterminate: true,
        }
    }

    fn from_tsink(stage: ClusterAuditPersistenceStage, err: TsinkError) -> Self {
        let resource_limit = match &err {
            TsinkError::DiskQuotaExceeded {
                limit,
                used,
                reserved,
                requested,
            } => Some(ClusterAuditDiskResourceLimit::DiskQuotaExceeded {
                limit: *limit,
                used: *used,
                reserved: *reserved,
                requested: *requested,
            }),
            TsinkError::InsufficientDiskSpace {
                required,
                available,
            } => Some(ClusterAuditDiskResourceLimit::InsufficientDiskSpace {
                required: *required,
                available: *available,
            }),
            TsinkError::InsufficientCompactionHeadroom {
                limit,
                used,
                reserved,
                requested,
            } => Some(
                ClusterAuditDiskResourceLimit::InsufficientCompactionHeadroom {
                    limit: *limit,
                    used: *used,
                    reserved: *reserved,
                    requested: *requested,
                },
            ),
            _ => None,
        };
        let indeterminate =
            stage == ClusterAuditPersistenceStage::Append && resource_limit.is_none();
        Self {
            stage,
            detail: err.to_string(),
            resource_limit,
            indeterminate,
        }
    }

    #[cfg(test)]
    pub(crate) fn stage(&self) -> ClusterAuditPersistenceStage {
        self.stage
    }

    pub fn resource_limit(&self) -> Option<ClusterAuditDiskResourceLimit> {
        self.resource_limit
    }

    pub fn is_indeterminate(&self) -> bool {
        self.indeterminate
    }
}

impl std::fmt::Display for ClusterAuditAppendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cluster audit persistence failed during {}: {}",
            self.stage.as_str(),
            self.detail
        )
    }
}

impl std::error::Error for ClusterAuditAppendError {}

#[derive(Debug, Clone)]
pub struct ClusterAuditLog {
    path: PathBuf,
    config: ClusterAuditConfig,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    state: Arc<Mutex<ClusterAuditState>>,
    #[cfg(test)]
    health_projection_string_clones: Arc<AtomicU64>,
}

#[derive(Debug)]
struct ClusterAuditState {
    entries: VecDeque<ClusterAuditRecord>,
    log_bytes: u64,
    next_id: u64,
    persistence_fenced: Option<String>,
    last_cleanup_error: Option<String>,
    #[cfg(test)]
    fail_next_compaction: bool,
    #[cfg(test)]
    fail_next_append_indeterminate: bool,
}

impl ClusterAuditLog {
    pub fn open(path: PathBuf, config: ClusterAuditConfig) -> Result<Self, String> {
        Self::open_with_disk_budget(path, config, None)
    }

    pub fn open_with_disk_budget(
        path: PathBuf,
        config: ClusterAuditConfig,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Self, String> {
        if config.retention_secs == 0 {
            return Err("cluster audit retention must be greater than zero seconds".to_string());
        }
        if config.max_log_bytes == 0 {
            return Err("cluster audit max log bytes must be greater than zero".to_string());
        }
        if config.max_query_limit == 0 {
            return Err("cluster audit max query limit must be greater than zero".to_string());
        }
        if let Some(parent) = path.parent() {
            if let Some(local_disk_budget) = local_disk_budget.as_ref() {
                local_disk_budget
                    .create_dir_all_and_sync_parents(parent)
                    .map_err(|err| {
                        format!(
                            "failed to create managed cluster audit directory {}: {err}",
                            parent.display()
                        )
                    })?;
                local_disk_budget
                    .cleanup_atomic_write_temps(&path)
                    .map_err(|err| {
                        format!(
                            "failed to clean managed cluster audit temporaries for {}: {err}",
                            path.display()
                        )
                    })?;
                let legacy_compaction_temp = path.with_extension("tmp");
                if legacy_compaction_temp != path {
                    local_disk_budget
                        .remove_managed_file_if_exists_and_sync_parent(
                            &legacy_compaction_temp,
                            DiskCategory::Temporary,
                        )
                        .map_err(|err| {
                            format!(
                                "failed to clean legacy cluster audit compaction file {}: {err}",
                                legacy_compaction_temp.display()
                            )
                        })?;
                }
                local_disk_budget
                    .validate_managed_file_path(&path)
                    .map_err(|err| {
                        format!(
                            "invalid managed cluster audit path {}: {err}",
                            path.display()
                        )
                    })?;
            } else {
                std::fs::create_dir_all(parent).map_err(|err| {
                    format!(
                        "failed to create cluster audit directory {}: {err}",
                        parent.display()
                    )
                })?;
            }
        }

        if !path.exists() {
            if let Some(local_disk_budget) = local_disk_budget.as_ref() {
                local_disk_budget
                    .append_file_and_sync_parent(&path, &[], DiskCategory::Cluster)
                    .map_err(|err| {
                        format!(
                            "failed to initialize managed cluster audit log {}: {err}",
                            path.display()
                        )
                    })?;
            } else {
                tsink::engine::fs_utils::write_file_atomically_and_sync_parent(&path, &[])
                    .map_err(|err| {
                        format!(
                            "failed to initialize cluster audit log {}: {err}",
                            path.display()
                        )
                    })?;
            }
        }

        let now_ms = unix_timestamp_millis();
        let cutoff_ms = now_ms.saturating_sub(config.retention_secs.saturating_mul(1000));
        let mut entries = VecDeque::new();
        let mut next_id = 1_u64;
        let mut skipped_expired = false;
        if path.exists() {
            let mut reader = BufReader::new(File::open(&path).map_err(|err| {
                format!("failed to open cluster audit log {}: {err}", path.display())
            })?);
            let mut line = Vec::new();
            let mut line_number = 0_u64;
            loop {
                line.clear();
                let read = reader.read_until(b'\n', &mut line).map_err(|err| {
                    format!("failed to read cluster audit log {}: {err}", path.display(),)
                })?;
                if read == 0 {
                    break;
                }
                line_number = line_number
                    .checked_add(1)
                    .ok_or_else(|| "cluster audit log line count overflow".to_string())?;
                if line.last() != Some(&b'\n') {
                    return Err(format!(
                        "cluster audit log {} ends with an incomplete record at line {line_number}",
                        path.display()
                    ));
                }
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let record: ClusterAuditRecord = serde_json::from_slice(&line).map_err(|err| {
                    format!(
                        "failed to parse cluster audit log {} line {}: {err}",
                        path.display(),
                        line_number
                    )
                })?;
                let following_id = record.id.checked_add(1).ok_or_else(|| {
                    format!(
                        "cluster audit log {} exhausted its record id space at line {line_number}",
                        path.display()
                    )
                })?;
                next_id = next_id.max(following_id);
                if record.timestamp_unix_ms < cutoff_ms {
                    skipped_expired = true;
                    continue;
                }
                entries.push_back(record);
            }
        }

        let log_bytes = std::fs::metadata(&path)
            .map_err(|err| {
                format!(
                    "failed to inspect cluster audit log {}: {err}",
                    path.display()
                )
            })?
            .len();

        let store = Self {
            path,
            config,
            local_disk_budget,
            state: Arc::new(Mutex::new(ClusterAuditState {
                entries,
                log_bytes,
                next_id,
                persistence_fenced: None,
                last_cleanup_error: None,
                #[cfg(test)]
                fail_next_compaction: false,
                #[cfg(test)]
                fail_next_append_indeterminate: false,
            })),
            #[cfg(test)]
            health_projection_string_clones: Arc::new(AtomicU64::new(0)),
        };

        {
            let mut state = store
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut changed = skipped_expired
                || prune_retention_locked(
                    &mut state.entries,
                    now_ms.saturating_sub(store.config.retention_secs.saturating_mul(1000)),
                );
            changed |= enforce_size_bound_locked(&mut state.entries, store.config.max_log_bytes)?;
            if changed || state.log_bytes > store.config.max_log_bytes {
                compact_locked(&store.path, store.local_disk_budget.as_ref(), &mut state)
                    .map_err(|err| format!("failed to initialize cluster audit log: {err}"))?;
            }
        }

        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(
        &self,
        input: ClusterAuditEntryInput,
    ) -> Result<ClusterAuditRecord, ClusterAuditAppendError> {
        let timestamp_unix_ms = input
            .timestamp_unix_ms
            .unwrap_or_else(unix_timestamp_millis);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(reason) = state.persistence_fenced.as_deref() {
            return Err(ClusterAuditAppendError::new(
                ClusterAuditPersistenceStage::Append,
                format!(
                    "cluster audit log is fenced after an indeterminate persistence failure: {reason}"
                ),
            ));
        }
        let now_ms = unix_timestamp_millis();
        let mut cleanup_needed = state.last_cleanup_error.is_some()
            || prune_retention_locked(
                &mut state.entries,
                now_ms.saturating_sub(self.config.retention_secs.saturating_mul(1000)),
            );
        cleanup_needed |= enforce_size_bound_locked(&mut state.entries, self.config.max_log_bytes)
            .map_err(|err| {
                ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Encode, err)
            })?;
        cleanup_needed |= state.log_bytes > self.config.max_log_bytes;
        if cleanup_needed {
            if let Err(err) =
                compact_locked(&self.path, self.local_disk_budget.as_ref(), &mut state)
            {
                state.last_cleanup_error = Some(err.to_string());
                return Err(err);
            }
        }
        let next_id = state.next_id.checked_add(1).ok_or_else(|| {
            ClusterAuditAppendError::new(
                ClusterAuditPersistenceStage::Encode,
                "cluster audit record id space exhausted",
            )
        })?;
        let record = ClusterAuditRecord {
            id: state.next_id,
            timestamp_unix_ms,
            operation: input.operation,
            actor: input.actor,
            target: input.target,
            outcome: input.outcome,
        };
        let encoded = serialize_record_line(&record).map_err(|err| {
            ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Encode, err)
        })?;
        #[cfg(test)]
        let append_result = if std::mem::take(&mut state.fail_next_append_indeterminate) {
            Err(ClusterAuditAppendError::indeterminate(
                ClusterAuditPersistenceStage::Append,
                "injected append failure with failed rollback",
            ))
        } else {
            append_record(&self.path, self.local_disk_budget.as_ref(), &encoded)
        };
        #[cfg(not(test))]
        let append_result = append_record(&self.path, self.local_disk_budget.as_ref(), &encoded);
        if let Err(err) = append_result {
            if err.is_indeterminate() {
                state.persistence_fenced = Some(err.to_string());
            }
            return Err(err);
        }
        state.log_bytes = std::fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or_else(|err| {
                eprintln!(
                    "failed to inspect cluster audit log after durable append; deferring exact reconciliation: {err}"
                );
                state.log_bytes.saturating_add(encoded.len() as u64)
            });
        state.next_id = next_id;
        state.entries.push_back(record.clone());

        let now_ms = unix_timestamp_millis();
        let mut changed = prune_retention_locked(
            &mut state.entries,
            now_ms.saturating_sub(self.config.retention_secs.saturating_mul(1000)),
        );
        changed |= enforce_size_bound_locked(&mut state.entries, self.config.max_log_bytes)
            .map_err(|err| {
                ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Encode, err)
            })?;
        if changed || state.log_bytes > self.config.max_log_bytes {
            if let Err(err) =
                compact_locked(&self.path, self.local_disk_budget.as_ref(), &mut state)
            {
                // The appended record is already durable and visible in memory. Compaction is
                // cleanup debt and must not turn that committed append into a false rejection.
                state.last_cleanup_error = Some(err.to_string());
                eprintln!("cluster audit compaction deferred after durable append: {err}");
            }
        }
        Ok(record)
    }

    #[cfg(test)]
    fn fail_next_compaction(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_next_compaction = true;
    }

    #[cfg(test)]
    fn fail_next_append_indeterminate(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_next_append_indeterminate = true;
    }

    #[allow(dead_code)]
    pub fn health_snapshot(&self) -> ClusterAuditHealthSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cleanup_pending = state.last_cleanup_error.is_some();
        let persistence_fenced = state.persistence_fenced.is_some();
        ClusterAuditHealthSnapshot {
            enabled: true,
            retained_entries: u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            log_bytes: state.log_bytes,
            cleanup_pending,
            last_cleanup_error: state.last_cleanup_error.clone(),
            persistence_fenced,
            persistence_fence_reason: state.persistence_fenced.clone(),
            degraded: cleanup_pending || persistence_fenced,
        }
    }

    /// Captures the full audit-health status after reserving both optional diagnostics.
    pub fn health_snapshot_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> Result<AccountedClusterAuditHealthSnapshot, QueryBudgetError> {
        execution.checkpoint()?;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        execution.checkpoint()?;
        let peak_bytes = state
            .last_cleanup_error
            .as_deref()
            .map(modeled_audit_status_str_bytes)
            .unwrap_or(0)
            .saturating_add(
                state
                    .persistence_fenced
                    .as_deref()
                    .map(modeled_audit_status_str_bytes)
                    .unwrap_or(0),
            );
        let mut reservation = execution.reserve_memory(peak_bytes)?;
        execution.checkpoint()?;

        let cleanup_pending = state.last_cleanup_error.is_some();
        let persistence_fenced = state.persistence_fenced.is_some();
        let snapshot = ClusterAuditHealthSnapshot {
            enabled: true,
            retained_entries: u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            log_bytes: state.log_bytes,
            cleanup_pending,
            last_cleanup_error: state
                .last_cleanup_error
                .as_deref()
                .map(|value| self.clone_health_projection_string(value)),
            persistence_fenced,
            persistence_fence_reason: state
                .persistence_fenced
                .as_deref()
                .map(|value| self.clone_health_projection_string(value)),
            degraded: cleanup_pending || persistence_fenced,
        };
        drop(state);
        execution.checkpoint()?;
        let retained_bytes = modeled_audit_health_snapshot_bytes(&snapshot);
        debug_assert!(retained_bytes <= peak_bytes);
        reservation.resize(retained_bytes)?;
        Ok(AccountedClusterAuditHealthSnapshot {
            snapshot,
            _reservation: reservation,
        })
    }

    fn clone_health_projection_string(&self, value: &str) -> String {
        #[cfg(test)]
        self.health_projection_string_clones
            .fetch_add(1, Ordering::Relaxed);
        let mut cloned = String::with_capacity(value.len());
        cloned.push_str(value);
        cloned
    }

    #[cfg(test)]
    fn reset_health_projection_string_clones(&self) {
        self.health_projection_string_clones
            .store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn health_projection_string_clones(&self) -> u64 {
        self.health_projection_string_clones.load(Ordering::Relaxed)
    }

    pub fn metrics_snapshot(&self) -> ClusterAuditMetricsSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cleanup_pending = state.last_cleanup_error.is_some();
        let persistence_fenced = state.persistence_fenced.is_some();
        ClusterAuditMetricsSnapshot {
            enabled: true,
            retained_entries: u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            log_bytes: state.log_bytes,
            cleanup_pending,
            persistence_fenced,
            degraded: cleanup_pending || persistence_fenced,
        }
    }

    pub fn query(&self, query: &ClusterAuditQuery) -> Vec<ClusterAuditRecord> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let limit = query
            .limit
            .unwrap_or(DEFAULT_AUDIT_QUERY_LIMIT)
            .max(1)
            .min(self.config.max_query_limit);
        let mut entries = Vec::with_capacity(limit);
        for record in state.entries.iter().rev() {
            if !record_matches_query(record, query) {
                continue;
            }
            entries.push(record.clone());
            if entries.len() >= limit {
                break;
            }
        }
        entries
    }

    pub fn export_jsonl(&self, query: &ClusterAuditQuery) -> Result<Vec<u8>, String> {
        let mut records = self.query(query);
        records.reverse();
        let mut encoded = Vec::new();
        for record in records {
            let line = serialize_record_line(&record)?;
            encoded.extend_from_slice(&line);
        }
        Ok(encoded)
    }
}

fn modeled_audit_status_str_bytes(value: &str) -> u64 {
    if value.is_empty() {
        return 0;
    }
    u64::try_from(value.len())
        .unwrap_or(u64::MAX)
        .saturating_add(AUDIT_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_audit_status_string_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        return 0;
    }
    u64::try_from(value.capacity())
        .unwrap_or(u64::MAX)
        .saturating_add(AUDIT_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_audit_health_snapshot_bytes(snapshot: &ClusterAuditHealthSnapshot) -> u64 {
    snapshot
        .last_cleanup_error
        .as_ref()
        .map(modeled_audit_status_string_bytes)
        .unwrap_or(0)
        .saturating_add(
            snapshot
                .persistence_fence_reason
                .as_ref()
                .map(modeled_audit_status_string_bytes)
                .unwrap_or(0),
        )
}

fn serialize_record_line(record: &ClusterAuditRecord) -> Result<Vec<u8>, String> {
    let mut encoded = serde_json::to_vec(record)
        .map_err(|err| format!("failed to serialize audit record: {err}"))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn prune_retention_locked(entries: &mut VecDeque<ClusterAuditRecord>, cutoff_ms: u64) -> bool {
    let previous_len = entries.len();
    entries.retain(|record| record.timestamp_unix_ms >= cutoff_ms);
    entries.len() != previous_len
}

fn enforce_size_bound_locked(
    entries: &mut VecDeque<ClusterAuditRecord>,
    max_log_bytes: u64,
) -> Result<bool, String> {
    let mut total_bytes = estimate_entries_size(entries)?;
    let mut changed = false;
    while total_bytes > max_log_bytes && entries.len() > 1 {
        let removed = entries
            .pop_front()
            .ok_or_else(|| "cluster audit size trim underflow".to_string())?;
        total_bytes = total_bytes.saturating_sub(serialize_record_line(&removed)?.len() as u64);
        changed = true;
    }
    Ok(changed)
}

fn estimate_entries_size(entries: &VecDeque<ClusterAuditRecord>) -> Result<u64, String> {
    let mut total = 0_u64;
    for entry in entries {
        let encoded_bytes = u64::try_from(serialize_record_line(entry)?.len())
            .map_err(|_| "encoded cluster audit record exceeds the supported byte range")?;
        total = total
            .checked_add(encoded_bytes)
            .ok_or_else(|| "cluster audit log exceeds the supported byte range".to_string())?;
    }
    Ok(total)
}

fn append_record(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    encoded: &[u8],
) -> Result<(), ClusterAuditAppendError> {
    if let Some(local_disk_budget) = local_disk_budget {
        return local_disk_budget
            .append_file_and_sync_parent(path, encoded, DiskCategory::Cluster)
            .map_err(|err| {
                ClusterAuditAppendError::from_tsink(ClusterAuditPersistenceStage::Append, err)
            });
    }

    let initial_len = std::fs::metadata(path)
        .map_err(|err| ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Append, err))?
        .len();
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|err| ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Append, err))?;
    let append_result = (|| -> std::io::Result<()> {
        file.write_all(encoded)?;
        file.flush()?;
        file.sync_all()?;
        sync_parent_directory(path)?;
        Ok(())
    })();
    let Err(append_err) = append_result else {
        return Ok(());
    };

    let rollback_result = file
        .set_len(initial_len)
        .and_then(|()| file.sync_all())
        .and_then(|()| sync_parent_directory(path));
    match rollback_result {
        Ok(()) => Err(ClusterAuditAppendError::new(
            ClusterAuditPersistenceStage::Append,
            append_err,
        )),
        Err(rollback_err) => Err(ClusterAuditAppendError::indeterminate(
            ClusterAuditPersistenceStage::Append,
            format!("append failed: {append_err}; rollback failed: {rollback_err}"),
        )),
    }
}

fn compact_locked(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    state: &mut ClusterAuditState,
) -> Result<(), ClusterAuditAppendError> {
    #[cfg(test)]
    if std::mem::take(&mut state.fail_next_compaction) {
        return Err(ClusterAuditAppendError::new(
            ClusterAuditPersistenceStage::Compact,
            "injected cluster audit compaction failure",
        ));
    }

    let compacted_bytes = estimate_entries_size(&state.entries)
        .map_err(|err| ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Compact, err))?;
    if let Some(local_disk_budget) = local_disk_budget {
        local_disk_budget
            .rewrite_file_atomically_and_sync_parent_for_cleanup_with(
                path,
                compacted_bytes,
                DiskCategory::Cluster,
                |writer| write_compacted_log(&state.entries, writer).map_err(TsinkError::Other),
            )
            .map_err(|err| {
                ClusterAuditAppendError::from_tsink(ClusterAuditPersistenceStage::Compact, err)
            })?;
    } else {
        tsink::engine::fs_utils::write_file_atomically_and_sync_parent_with(
            path,
            compacted_bytes,
            |writer| write_compacted_log(&state.entries, writer).map_err(TsinkError::Other),
        )
        .map_err(|err| {
            ClusterAuditAppendError::from_tsink(ClusterAuditPersistenceStage::Compact, err)
        })?;
    }

    let actual_bytes = std::fs::metadata(path)
        .map_err(|err| ClusterAuditAppendError::new(ClusterAuditPersistenceStage::Compact, err))?
        .len();
    if actual_bytes != compacted_bytes {
        return Err(ClusterAuditAppendError::new(
            ClusterAuditPersistenceStage::Compact,
            format!(
                "compacted cluster audit log length mismatch: expected {compacted_bytes} bytes, found {actual_bytes} bytes"
            ),
        ));
    }
    state.log_bytes = actual_bytes;
    state.last_cleanup_error = None;
    Ok(())
}

fn write_compacted_log(
    entries: &VecDeque<ClusterAuditRecord>,
    writer: &mut dyn Write,
) -> Result<(), String> {
    for entry in entries {
        let encoded = serialize_record_line(entry)?;
        writer
            .write_all(&encoded)
            .map_err(|err| format!("failed to write compacted cluster audit log: {err}"))?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn record_matches_query(record: &ClusterAuditRecord, query: &ClusterAuditQuery) -> bool {
    if let Some(operation) = query.operation.as_deref() {
        if !record.operation.eq_ignore_ascii_case(operation) {
            return false;
        }
    }
    if let Some(actor_id) = query.actor_id.as_deref() {
        if record.actor.id != actor_id {
            return false;
        }
    }
    if let Some(status) = query.status.as_deref() {
        if !record.outcome.status.eq_ignore_ascii_case(status) {
            return false;
        }
    }
    if let Some(since_unix_ms) = query.since_unix_ms {
        if record.timestamp_unix_ms < since_unix_ms {
            return false;
        }
    }
    if let Some(until_unix_ms) = query.until_unix_ms {
        if record.timestamp_unix_ms > until_unix_ms {
            return false;
        }
    }
    true
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

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Barrier;
    use std::thread;
    use tempfile::TempDir;
    use tsink::{
        LocalDiskLimits, QueryBudget, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken,
        QueryLimitReason, QueryWorkLimits,
    };

    fn actor(id: &str) -> ClusterAuditActor {
        ClusterAuditActor {
            id: id.to_string(),
            auth_scope: "admin".to_string(),
        }
    }

    fn outcome(status: &str, http_status: u16) -> ClusterAuditOutcome {
        ClusterAuditOutcome {
            status: status.to_string(),
            http_status,
            result: None,
            error_type: None,
        }
    }

    fn input(timestamp_unix_ms: u64, operation: &str) -> ClusterAuditEntryInput {
        ClusterAuditEntryInput {
            timestamp_unix_ms: Some(timestamp_unix_ms),
            operation: operation.to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/api/v1/admin/cluster/test"}),
            outcome: outcome("success", 200),
        }
    }

    fn category_bytes(snapshot: &tsink::LocalDiskBudgetSnapshot, category: DiskCategory) -> u64 {
        snapshot
            .categories
            .iter()
            .find(|usage| usage.category == category)
            .map(|usage| usage.bytes)
            .unwrap_or_default()
    }

    #[test]
    fn metrics_snapshot_matches_health_without_owned_diagnostics() {
        assert!(!std::mem::needs_drop::<ClusterAuditMetricsSnapshot>());

        let temp_dir = TempDir::new().expect("tempdir should build");
        let log = ClusterAuditLog::open(
            temp_dir.path().join("audit.log"),
            ClusterAuditConfig::default(),
        )
        .expect("audit log should open");
        log.append(input(unix_timestamp_millis(), "metrics"))
            .expect("audit record should append");
        {
            let mut state = log
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.last_cleanup_error = Some("cleanup diagnostic".repeat(64));
            state.persistence_fenced = Some("fence diagnostic".repeat(64));
        }

        let metrics = log.metrics_snapshot();
        let health = log.health_snapshot();
        assert_eq!(metrics.enabled, health.enabled);
        assert_eq!(metrics.retained_entries, health.retained_entries);
        assert_eq!(metrics.log_bytes, health.log_bytes);
        assert_eq!(metrics.cleanup_pending, health.cleanup_pending);
        assert_eq!(metrics.persistence_fenced, health.persistence_fenced);
        assert_eq!(metrics.degraded, health.degraded);
        assert!(health
            .last_cleanup_error
            .as_deref()
            .is_some_and(|error| error.starts_with("cleanup diagnostic")));
        assert!(health
            .persistence_fence_reason
            .as_deref()
            .is_some_and(|error| error.starts_with("fence diagnostic")));
    }

    #[test]
    fn accounted_health_snapshot_preserves_values_and_enforces_exact_preclone_peak() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let log = ClusterAuditLog::open(
            temp_dir.path().join("accounted-health.log"),
            ClusterAuditConfig::default(),
        )
        .expect("audit log should open");
        log.append(input(unix_timestamp_millis(), "accounted-health"))
            .expect("audit record should append");
        {
            let mut state = log
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.last_cleanup_error = Some("cleanup diagnostic".repeat(32));
            state.persistence_fenced = Some("fence diagnostic".repeat(48));
        }
        let expected = log.health_snapshot();
        let peak_bytes = modeled_audit_health_snapshot_bytes(&expected);
        assert!(peak_bytes > 0);

        log.reset_health_projection_string_clones();
        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(peak_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(peak_bytes),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact audit-health budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let projected = log
            .health_snapshot_with_execution(&exact)
            .expect("the exact modeled peak should pass");
        assert_eq!(&*projected, &expected);
        assert_eq!(projected.accounted_bytes(), peak_bytes);
        assert_eq!(log.health_projection_string_clones(), 2);
        drop(projected);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        let exact_released = exact_budget.snapshot();
        assert_eq!(exact_released.active_queries, 0);
        assert_eq!(exact_released.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_released.accounting_invariant_violations_total, 0);

        log.reset_health_projection_string_clones();
        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(peak_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(peak_bytes - 1),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under audit-health budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = log
            .health_snapshot_with_execution(&one_under)
            .expect_err("one byte below the modeled peak must reject");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, peak_bytes);
            }
            other => panic!("unexpected audit-health projection error: {other}"),
        }
        assert_eq!(log.health_projection_string_clones(), 0);
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let one_under_released = one_under_budget.snapshot();
        assert_eq!(one_under_released.active_queries, 0);
        assert_eq!(one_under_released.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_released.accounting_invariant_violations_total, 0);

        log.reset_health_projection_string_clones();
        let cancelled_budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let cancellation = QueryCancellationToken::new();
        let cancelled = cancelled_budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("cancelled query should admit");
        cancellation.cancel();
        let error = log
            .health_snapshot_with_execution(&cancelled)
            .expect_err("a pre-cancelled audit-health projection must stop");
        assert!(matches!(error, QueryBudgetError::Cancelled));
        assert_eq!(log.health_projection_string_clones(), 0);
        assert_eq!(cancelled.snapshot().memory_reserved_bytes, 0);
        drop(cancelled);
        let cancelled_released = cancelled_budget.snapshot();
        assert_eq!(cancelled_released.active_queries, 0);
        assert_eq!(cancelled_released.shared_reserved_memory_bytes, 0);
        assert_eq!(cancelled_released.cancellations_total, 1);
        assert_eq!(cancelled_released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn append_and_query_returns_most_recent_entries() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let log = ClusterAuditLog::open(path, ClusterAuditConfig::default())
            .expect("audit log should open");
        let now_ms = unix_timestamp_millis();
        log.append(ClusterAuditEntryInput {
            timestamp_unix_ms: Some(now_ms),
            operation: "join".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/api/v1/admin/cluster/join", "nodeId": "node-b"}),
            outcome: outcome("success", 200),
        })
        .expect("first append should succeed");
        log.append(ClusterAuditEntryInput {
            timestamp_unix_ms: Some(now_ms.saturating_add(1)),
            operation: "leave".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/api/v1/admin/cluster/leave", "nodeId": "node-b"}),
            outcome: outcome("error", 409),
        })
        .expect("second append should succeed");

        let queried = log.query(&ClusterAuditQuery {
            actor_id: Some("operator-a".to_string()),
            limit: Some(1),
            ..ClusterAuditQuery::default()
        });
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].operation, "leave");
        assert_eq!(queried[0].outcome.http_status, 409);
    }

    #[test]
    fn retention_prunes_expired_records() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let config = ClusterAuditConfig {
            retention_secs: 1,
            max_log_bytes: 1024 * 1024,
            max_query_limit: 100,
        };
        let log = ClusterAuditLog::open(path, config).expect("audit log should open");
        let now_ms = unix_timestamp_millis();

        log.append(ClusterAuditEntryInput {
            timestamp_unix_ms: Some(now_ms.saturating_sub(20_000)),
            operation: "join".to_string(),
            actor: actor("operator-old"),
            target: json!({"path": "/api/v1/admin/cluster/join"}),
            outcome: outcome("success", 200),
        })
        .expect("old append should succeed");

        log.append(ClusterAuditEntryInput {
            timestamp_unix_ms: Some(now_ms),
            operation: "join".to_string(),
            actor: actor("operator-new"),
            target: json!({"path": "/api/v1/admin/cluster/join"}),
            outcome: outcome("success", 200),
        })
        .expect("new append should succeed");

        let queried = log.query(&ClusterAuditQuery::default());
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].actor.id, "operator-new");
    }

    #[test]
    fn export_jsonl_returns_newline_delimited_records() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let log = ClusterAuditLog::open(path, ClusterAuditConfig::default())
            .expect("audit log should open");
        let now_ms = unix_timestamp_millis();

        log.append(ClusterAuditEntryInput {
            timestamp_unix_ms: Some(now_ms),
            operation: "pause_repair".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/api/v1/admin/cluster/repair/pause"}),
            outcome: outcome("success", 200),
        })
        .expect("append should succeed");
        log.append(ClusterAuditEntryInput {
            timestamp_unix_ms: Some(now_ms.saturating_add(1)),
            operation: "resume_repair".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/api/v1/admin/cluster/repair/resume"}),
            outcome: outcome("success", 200),
        })
        .expect("append should succeed");

        let exported = log
            .export_jsonl(&ClusterAuditQuery::default())
            .expect("export should succeed");
        let lines = std::str::from_utf8(&exported)
            .expect("utf8")
            .lines()
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        let first: ClusterAuditRecord =
            serde_json::from_str(lines[0]).expect("line should decode as record");
        let second: ClusterAuditRecord =
            serde_json::from_str(lines[1]).expect("line should decode as record");
        assert_eq!(first.operation, "pause_repair");
        assert_eq!(second.operation, "resume_repair");
    }

    #[test]
    fn query_limit_is_capped_by_config() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let log = ClusterAuditLog::open(
            path,
            ClusterAuditConfig {
                retention_secs: DEFAULT_AUDIT_RETENTION_SECS,
                max_log_bytes: DEFAULT_AUDIT_MAX_LOG_BYTES,
                max_query_limit: 2,
            },
        )
        .expect("audit log should open");
        let now_ms = unix_timestamp_millis();
        for idx in 0..5 {
            log.append(ClusterAuditEntryInput {
                timestamp_unix_ms: Some(now_ms.saturating_add(idx)),
                operation: "join".to_string(),
                actor: actor("operator"),
                target: json!({"index": idx}),
                outcome: outcome("success", 200),
            })
            .expect("append should succeed");
        }

        let queried = log.query(&ClusterAuditQuery {
            limit: Some(10),
            ..ClusterAuditQuery::default()
        });
        assert_eq!(queried.len(), 2);
    }

    #[test]
    fn budgeted_append_rejects_tiny_quota_without_publication() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("cluster/audit/node-a.audit.log");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(1),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let log = ClusterAuditLog::open_with_disk_budget(
            path.clone(),
            ClusterAuditConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("audit log should open");

        let err = log
            .append(input(unix_timestamp_millis(), "tiny_quota"))
            .expect_err("record should exceed the tiny quota");
        assert_eq!(err.stage(), ClusterAuditPersistenceStage::Append);
        assert!(matches!(
            err.resource_limit(),
            Some(ClusterAuditDiskResourceLimit::DiskQuotaExceeded {
                limit: 1,
                used: 0,
                reserved: 0,
                requested,
            }) if requested > 1
        ));
        assert!(log.query(&ClusterAuditQuery::default()).is_empty());
        assert_eq!(std::fs::metadata(path).expect("audit metadata").len(), 0);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(category_bytes(&snapshot, DiskCategory::Cluster), 0);
    }

    #[test]
    fn physical_headroom_failure_preserves_typed_resource_detail() {
        let err = ClusterAuditAppendError::from_tsink(
            ClusterAuditPersistenceStage::Append,
            TsinkError::InsufficientDiskSpace {
                required: 4_096,
                available: 1_024,
            },
        );

        assert_eq!(err.stage(), ClusterAuditPersistenceStage::Append);
        assert_eq!(
            err.resource_limit(),
            Some(ClusterAuditDiskResourceLimit::InsufficientDiskSpace {
                required: 4_096,
                available: 1_024,
            })
        );
    }

    #[test]
    fn budgeted_audit_has_exact_cluster_accounting_across_restart() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("cluster/audit/node-a.audit.log");
        let limits = LocalDiskLimits {
            max_bytes: Some(8 * 1024 * 1024),
            ..LocalDiskLimits::default()
        };
        let timestamp = unix_timestamp_millis();

        let budget = LocalDiskBudget::open(temp_dir.path(), limits).expect("budget should open");
        let log = ClusterAuditLog::open_with_disk_budget(
            path.clone(),
            ClusterAuditConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("audit log should open");
        let appended = log
            .append(input(timestamp, "restart_accounting"))
            .expect("append should succeed");
        let physical_bytes = std::fs::metadata(&path)
            .expect("audit metadata should load")
            .len();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, physical_bytes);
        assert_eq!(
            category_bytes(&snapshot, DiskCategory::Cluster),
            physical_bytes
        );
        assert_eq!(snapshot.reserved_bytes, 0);
        drop(log);
        drop(budget);

        let restarted_budget =
            LocalDiskBudget::open(temp_dir.path(), limits).expect("restarted budget should open");
        let restarted = ClusterAuditLog::open_with_disk_budget(
            path,
            ClusterAuditConfig::default(),
            Some(Arc::clone(&restarted_budget)),
        )
        .expect("audit log should reopen");
        assert_eq!(
            restarted.query(&ClusterAuditQuery::default()),
            vec![appended]
        );
        let restarted_snapshot = restarted_budget.snapshot();
        assert_eq!(restarted_snapshot.accounted_bytes, physical_bytes);
        assert_eq!(
            category_bytes(&restarted_snapshot, DiskCategory::Cluster),
            physical_bytes
        );
        assert_eq!(restarted_snapshot.reserved_bytes, 0);
    }

    #[test]
    fn concurrent_budgeted_audits_cannot_share_the_final_record_bytes() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let timestamp = unix_timestamp_millis();
        let record_bytes = u64::try_from(
            serialize_record_line(&ClusterAuditRecord {
                id: 1,
                timestamp_unix_ms: timestamp,
                operation: "concurrent".to_string(),
                actor: actor("operator-a"),
                target: json!({"path": "/api/v1/admin/cluster/test"}),
                outcome: outcome("success", 200),
            })
            .expect("test record should encode")
            .len(),
        )
        .expect("record length should fit u64");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(record_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("budget should open");
        let logs = ["a", "b"].map(|name| {
            Arc::new(
                ClusterAuditLog::open_with_disk_budget(
                    temp_dir
                        .path()
                        .join(format!("cluster/audit/{name}.audit.log")),
                    ClusterAuditConfig::default(),
                    Some(Arc::clone(&budget)),
                )
                .expect("audit log should open"),
            )
        });
        let barrier = Arc::new(Barrier::new(3));
        let handles = logs
            .into_iter()
            .map(|log| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    log.append(input(timestamp, "concurrent"))
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("append thread should finish"))
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(err) if matches!(
                        err.resource_limit(),
                        Some(ClusterAuditDiskResourceLimit::DiskQuotaExceeded { .. })
                    )
                ))
                .count(),
            1
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, record_bytes);
        assert_eq!(
            category_bytes(&snapshot, DiskCategory::Cluster),
            record_bytes
        );
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn quota_full_open_compacts_non_growing_state_with_recovery_admission() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("cluster/audit/node-a.audit.log");
        std::fs::create_dir_all(path.parent().expect("audit parent"))
            .expect("audit parent should build");
        let now = unix_timestamp_millis();
        let expired = ClusterAuditRecord {
            id: 1,
            timestamp_unix_ms: now.saturating_sub(20_000),
            operation: "expired".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/expired"}),
            outcome: outcome("success", 200),
        };
        let retained = ClusterAuditRecord {
            id: 2,
            timestamp_unix_ms: now,
            operation: "retained".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/retained"}),
            outcome: outcome("success", 200),
        };
        let mut fixture = serialize_record_line(&expired).expect("expired record should encode");
        let retained_line =
            serialize_record_line(&retained).expect("retained record should encode");
        fixture.extend_from_slice(&retained_line);
        std::fs::write(&path, &fixture).expect("audit fixture should write");
        let initial_bytes = u64::try_from(fixture.len()).expect("fixture length should fit u64");
        let retained_bytes =
            u64::try_from(retained_line.len()).expect("line length should fit u64");
        let budget = LocalDiskBudget::open(
            temp_dir.path(),
            LocalDiskLimits {
                max_bytes: Some(initial_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("budget should open at its quota");
        let log = ClusterAuditLog::open_with_disk_budget(
            path.clone(),
            ClusterAuditConfig {
                retention_secs: 1,
                max_log_bytes: retained_bytes,
                max_query_limit: 100,
            },
            Some(Arc::clone(&budget)),
        )
        .expect("recovery compaction should open the audit log");

        assert_eq!(log.query(&ClusterAuditQuery::default()), vec![retained]);
        assert_eq!(
            std::fs::metadata(path).expect("audit metadata").len(),
            retained_bytes
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, retained_bytes);
        assert_eq!(
            category_bytes(&snapshot, DiskCategory::Cluster),
            retained_bytes
        );
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn budgeted_open_cleans_only_owned_atomic_and_legacy_temporaries() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("cluster/audit/node-a.audit.log");
        let parent = path.parent().expect("audit path should have a parent");
        std::fs::create_dir_all(parent).expect("audit directory should build");
        let target_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("audit file name should be UTF-8");
        let generated_temp = parent.join(format!(".{target_name}.tmp-123-0000000000000000"));
        let generated_lookalike = parent.join(format!(".{target_name}.tmp-123-000000000000000G"));
        let legacy_temp = path.with_extension("tmp");
        let legacy_lookalike = parent.join("node.audit.tmp.keep");
        std::fs::write(&generated_temp, b"generated").expect("generated temp should write");
        std::fs::write(&generated_lookalike, b"generated-lookalike")
            .expect("generated lookalike should write");
        std::fs::write(&legacy_temp, b"legacy").expect("legacy temp should write");
        std::fs::write(&legacy_lookalike, b"legacy-lookalike")
            .expect("legacy lookalike should write");
        let budget = LocalDiskBudget::open(temp_dir.path(), LocalDiskLimits::default())
            .expect("budget should open");

        let _log = ClusterAuditLog::open_with_disk_budget(
            path.clone(),
            ClusterAuditConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("audit log should open");

        assert!(!generated_temp.exists());
        assert!(!legacy_temp.exists());
        assert!(generated_lookalike.exists());
        assert!(legacy_lookalike.exists());
        assert_eq!(std::fs::metadata(path).expect("audit metadata").len(), 0);
        let expected_bytes = std::fs::metadata(&generated_lookalike)
            .expect("generated lookalike metadata")
            .len()
            .saturating_add(
                std::fs::metadata(&legacy_lookalike)
                    .expect("legacy lookalike metadata")
                    .len(),
            );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, expected_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn post_append_compaction_failure_is_cleanup_debt() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let config = ClusterAuditConfig {
            retention_secs: DEFAULT_AUDIT_RETENTION_SECS,
            max_log_bytes: 1,
            max_query_limit: 100,
        };
        let log = ClusterAuditLog::open(path.clone(), config).expect("audit log should open");
        log.fail_next_compaction();

        let appended = log
            .append(input(unix_timestamp_millis(), "cleanup_debt"))
            .expect("durable append must not fail when cleanup is deferred");
        assert_eq!(
            log.query(&ClusterAuditQuery::default()),
            vec![appended.clone()]
        );
        let health = log.health_snapshot();
        assert!(health.cleanup_pending);
        assert!(health.degraded);
        assert!(!health.persistence_fenced);
        assert!(health
            .last_cleanup_error
            .as_deref()
            .is_some_and(|error| error.contains("injected")));
        drop(log);

        let restarted = ClusterAuditLog::open(path, config).expect("audit log should reopen");
        assert_eq!(
            restarted.query(&ClusterAuditQuery::default()),
            vec![appended]
        );
    }

    #[test]
    fn retention_only_cleanup_debt_is_observable_and_retried() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let config = ClusterAuditConfig {
            retention_secs: 1,
            max_log_bytes: 1024 * 1024,
            max_query_limit: 100,
        };
        let log = ClusterAuditLog::open(path.clone(), config).expect("audit log should open");
        log.fail_next_compaction();

        log.append(input(
            unix_timestamp_millis().saturating_sub(20_000),
            "expired_cleanup_debt",
        ))
        .expect("durable expired append should survive deferred cleanup");
        assert!(log.query(&ClusterAuditQuery::default()).is_empty());
        assert!(log.health_snapshot().cleanup_pending);
        assert!(std::fs::metadata(&path).expect("metadata").len() > 0);

        let retained = log
            .append(input(unix_timestamp_millis(), "cleanup_retry"))
            .expect("next append should retry retention-only cleanup");
        let health = log.health_snapshot();
        assert!(!health.cleanup_pending);
        assert!(!health.degraded);
        assert!(health.last_cleanup_error.is_none());
        assert_eq!(log.query(&ClusterAuditQuery::default()), vec![retained]);

        drop(log);
        let reopened = ClusterAuditLog::open(path, config).expect("cleaned log should reopen");
        assert_eq!(reopened.query(&ClusterAuditQuery::default()).len(), 1);
    }

    #[test]
    fn indeterminate_append_failure_fences_future_audit_appends() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let log = ClusterAuditLog::open(path.clone(), ClusterAuditConfig::default())
            .expect("audit log should open");
        log.fail_next_append_indeterminate();

        let error = log
            .append(input(unix_timestamp_millis(), "indeterminate"))
            .expect_err("indeterminate persistence must fail the audit append");
        assert!(error.is_indeterminate());
        let health = log.health_snapshot();
        assert!(health.persistence_fenced);
        assert!(health.degraded);
        assert!(!health.cleanup_pending);
        assert!(health
            .persistence_fence_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("failed rollback")));
        assert!(log.query(&ClusterAuditQuery::default()).is_empty());
        assert_eq!(std::fs::metadata(&path).expect("metadata").len(), 0);

        let second = log
            .append(input(unix_timestamp_millis(), "must_remain_fenced"))
            .expect_err("fenced audit log must reject later appends");
        assert!(!second.is_indeterminate());
        assert!(second.to_string().contains("is fenced"));
        assert_eq!(std::fs::metadata(path).expect("metadata").len(), 0);
    }

    #[test]
    fn open_rejects_an_unterminated_final_record() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let record = ClusterAuditRecord {
            id: 1,
            timestamp_unix_ms: unix_timestamp_millis(),
            operation: "unterminated".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/unterminated"}),
            outcome: outcome("success", 200),
        };
        let mut encoded = serialize_record_line(&record).expect("record should encode");
        assert_eq!(encoded.pop(), Some(b'\n'));
        std::fs::write(&path, encoded).expect("fixture should write");

        let err = ClusterAuditLog::open(path, ClusterAuditConfig::default())
            .expect_err("unterminated record should fail closed");
        assert!(err.contains("incomplete record"), "unexpected error: {err}");
    }

    #[test]
    fn expired_records_advance_ids_before_startup_compaction() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let expired = ClusterAuditRecord {
            id: 41,
            timestamp_unix_ms: unix_timestamp_millis().saturating_sub(20_000),
            operation: "expired".to_string(),
            actor: actor("operator-a"),
            target: json!({"path": "/expired"}),
            outcome: outcome("success", 200),
        };
        std::fs::write(
            &path,
            serialize_record_line(&expired).expect("expired record should encode"),
        )
        .expect("fixture should write");
        let config = ClusterAuditConfig {
            retention_secs: 1,
            ..ClusterAuditConfig::default()
        };

        let log = ClusterAuditLog::open(path.clone(), config).expect("audit log should open");
        assert_eq!(std::fs::metadata(&path).expect("metadata").len(), 0);
        let appended = log
            .append(input(unix_timestamp_millis(), "after_expiry"))
            .expect("append should succeed");
        assert_eq!(appended.id, 42);
    }

    #[test]
    fn retention_prunes_out_of_order_expired_records() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let path = temp_dir.path().join("audit.log");
        let config = ClusterAuditConfig {
            retention_secs: 1,
            max_log_bytes: 1024 * 1024,
            max_query_limit: 100,
        };
        let log = ClusterAuditLog::open(path, config).expect("audit log should open");
        let now = unix_timestamp_millis();
        let retained = log
            .append(input(now, "retained"))
            .expect("recent append should succeed");
        log.append(input(now.saturating_sub(20_000), "expired"))
            .expect("out-of-order append should persist before retention cleanup");

        assert_eq!(log.query(&ClusterAuditQuery::default()), vec![retained]);
    }
}
