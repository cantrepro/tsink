use crate::cluster::dedupe::{
    dedupe_metrics_snapshot, validate_idempotency_key, DedupeConfig, DedupeWindowStore,
};
use crate::cluster::replication::validate_confirmed_atomic_ingest_response;
use crate::cluster::rpc::{
    normalize_capabilities, required_capabilities_for_rows, CompatibilityProfile,
    InternalApiConfig, InternalIngestRowsRequest, InternalRow, RpcClient, RpcClientConfig,
    INTERNAL_RPC_PROTOCOL_VERSION, MAX_INTERNAL_INGEST_ROWS,
};
use crate::tenant;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::{DiskCategory, Label, LocalDiskBudget, Row, TsinkError, WriteAcknowledgement};

pub const EDGE_SYNC_MAX_ENTRIES_ENV: &str = "TSINK_EDGE_SYNC_MAX_ENTRIES";
pub const EDGE_SYNC_MAX_BYTES_ENV: &str = "TSINK_EDGE_SYNC_MAX_BYTES";
pub const EDGE_SYNC_MAX_LOG_BYTES_ENV: &str = "TSINK_EDGE_SYNC_MAX_LOG_BYTES";
pub const EDGE_SYNC_MAX_RECORD_BYTES_ENV: &str = "TSINK_EDGE_SYNC_MAX_RECORD_BYTES";
pub const EDGE_SYNC_REPLAY_INTERVAL_SECS_ENV: &str = "TSINK_EDGE_SYNC_REPLAY_INTERVAL_SECS";
pub const EDGE_SYNC_REPLAY_BATCH_SIZE_ENV: &str = "TSINK_EDGE_SYNC_REPLAY_BATCH_SIZE";
pub const EDGE_SYNC_MAX_BACKOFF_SECS_ENV: &str = "TSINK_EDGE_SYNC_MAX_BACKOFF_SECS";
pub const EDGE_SYNC_CLEANUP_INTERVAL_SECS_ENV: &str = "TSINK_EDGE_SYNC_CLEANUP_INTERVAL_SECS";
pub const EDGE_SYNC_PRE_ACK_RETENTION_SECS_ENV: &str = "TSINK_EDGE_SYNC_PRE_ACK_RETENTION_SECS";
pub const EDGE_SYNC_DEDUPE_WINDOW_SECS_ENV: &str = "TSINK_EDGE_SYNC_DEDUPE_WINDOW_SECS";
pub const EDGE_SYNC_DEDUPE_MAX_ENTRIES_ENV: &str = "TSINK_EDGE_SYNC_DEDUPE_MAX_ENTRIES";
pub const EDGE_SYNC_DEDUPE_MAX_LOG_BYTES_ENV: &str = "TSINK_EDGE_SYNC_DEDUPE_MAX_LOG_BYTES";
pub const EDGE_SYNC_DEDUPE_CLEANUP_INTERVAL_SECS_ENV: &str =
    "TSINK_EDGE_SYNC_DEDUPE_CLEANUP_INTERVAL_SECS";

const DEFAULT_EDGE_SYNC_MAX_ENTRIES: usize = 100_000;
const DEFAULT_EDGE_SYNC_MAX_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_EDGE_SYNC_MAX_LOG_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_EDGE_SYNC_MAX_RECORD_BYTES: u64 = 2 * 1024 * 1024;
const DEFAULT_EDGE_SYNC_REPLAY_INTERVAL_SECS: u64 = 2;
const DEFAULT_EDGE_SYNC_REPLAY_BATCH_SIZE: usize = 256;
const DEFAULT_EDGE_SYNC_MAX_BACKOFF_SECS: u64 = 30;
const DEFAULT_EDGE_SYNC_CLEANUP_INTERVAL_SECS: u64 = 30;
const DEFAULT_EDGE_SYNC_PRE_ACK_RETENTION_SECS: u64 = 24 * 3600;
const DEFAULT_EDGE_SYNC_DEDUPE_WINDOW_SECS: u64 = 24 * 3600;

const EDGE_SYNC_DIR_NAME: &str = "edge_sync";
const EDGE_SYNC_QUEUE_FILE_NAME: &str = "queue.log";
const EDGE_SYNC_DEDUPE_FILE_NAME: &str = "dedupe.log";

#[cfg(test)]
fn injected_append_failure_paths() -> &'static Mutex<BTreeSet<PathBuf>> {
    static PATHS: std::sync::OnceLock<Mutex<BTreeSet<PathBuf>>> = std::sync::OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
fn injected_compaction_failure_paths() -> &'static Mutex<BTreeSet<PathBuf>> {
    static PATHS: std::sync::OnceLock<Mutex<BTreeSet<PathBuf>>> = std::sync::OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
pub(crate) struct InjectedFailureGuard {
    path: PathBuf,
    kind: InjectedFailureKind,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum InjectedFailureKind {
    Append,
    Compaction,
}

#[cfg(test)]
impl Drop for InjectedFailureGuard {
    fn drop(&mut self) {
        let paths = match self.kind {
            InjectedFailureKind::Append => injected_append_failure_paths(),
            InjectedFailureKind::Compaction => injected_compaction_failure_paths(),
        };
        paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.path);
    }
}

#[cfg(test)]
pub(crate) fn inject_append_failure(path: &Path) -> InjectedFailureGuard {
    injected_append_failure_paths()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(path.to_path_buf());
    InjectedFailureGuard {
        path: path.to_path_buf(),
        kind: InjectedFailureKind::Append,
    }
}

#[cfg(test)]
fn inject_compaction_failure(path: &Path) -> InjectedFailureGuard {
    injected_compaction_failure_paths()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(path.to_path_buf());
    InjectedFailureGuard {
        path: path.to_path_buf(),
        kind: InjectedFailureKind::Compaction,
    }
}

#[cfg(test)]
fn append_failure_injected(path: &Path) -> bool {
    injected_append_failure_paths()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(path)
}

#[cfg(test)]
fn compaction_failure_injected(path: &Path) -> bool {
    injected_compaction_failure_paths()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeSyncTenantMappingMode {
    Preserve,
    Static,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdgeSyncTenantMapping {
    pub mode: EdgeSyncTenantMappingMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub static_tenant_id: Option<String>,
}

impl Default for EdgeSyncTenantMapping {
    fn default() -> Self {
        Self {
            mode: EdgeSyncTenantMappingMode::Preserve,
            static_tenant_id: None,
        }
    }
}

impl EdgeSyncTenantMapping {
    pub fn preserve() -> Self {
        Self::default()
    }

    pub fn static_tenant(tenant_id: impl Into<String>) -> Self {
        Self {
            mode: EdgeSyncTenantMappingMode::Static,
            static_tenant_id: Some(tenant_id.into()),
        }
    }

    pub fn static_tenant_id(&self) -> Option<&str> {
        self.static_tenant_id
            .as_deref()
            .map(str::trim)
            .filter(|tenant_id| !tenant_id.is_empty())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeSyncQueueConfig {
    pub max_entries: usize,
    pub max_bytes: u64,
    pub max_log_bytes: u64,
    pub max_record_bytes: u64,
    pub replay_interval_secs: u64,
    pub replay_batch_size: usize,
    pub max_backoff_secs: u64,
    pub cleanup_interval_secs: u64,
    pub pre_ack_retention_secs: u64,
}

impl Default for EdgeSyncQueueConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_EDGE_SYNC_MAX_ENTRIES,
            max_bytes: DEFAULT_EDGE_SYNC_MAX_BYTES,
            max_log_bytes: DEFAULT_EDGE_SYNC_MAX_LOG_BYTES,
            max_record_bytes: DEFAULT_EDGE_SYNC_MAX_RECORD_BYTES,
            replay_interval_secs: DEFAULT_EDGE_SYNC_REPLAY_INTERVAL_SECS,
            replay_batch_size: DEFAULT_EDGE_SYNC_REPLAY_BATCH_SIZE,
            max_backoff_secs: DEFAULT_EDGE_SYNC_MAX_BACKOFF_SECS,
            cleanup_interval_secs: DEFAULT_EDGE_SYNC_CLEANUP_INTERVAL_SECS,
            pre_ack_retention_secs: DEFAULT_EDGE_SYNC_PRE_ACK_RETENTION_SECS,
        }
    }
}

impl EdgeSyncQueueConfig {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            max_entries: parse_env_u64(
                EDGE_SYNC_MAX_ENTRIES_ENV,
                defaults.max_entries as u64,
                true,
            )? as usize,
            max_bytes: parse_env_u64(EDGE_SYNC_MAX_BYTES_ENV, defaults.max_bytes, true)?,
            max_log_bytes: parse_env_u64(
                EDGE_SYNC_MAX_LOG_BYTES_ENV,
                defaults.max_log_bytes,
                true,
            )?,
            max_record_bytes: parse_env_u64(
                EDGE_SYNC_MAX_RECORD_BYTES_ENV,
                defaults.max_record_bytes,
                true,
            )?,
            replay_interval_secs: parse_env_u64(
                EDGE_SYNC_REPLAY_INTERVAL_SECS_ENV,
                defaults.replay_interval_secs,
                true,
            )?,
            replay_batch_size: parse_env_u64(
                EDGE_SYNC_REPLAY_BATCH_SIZE_ENV,
                defaults.replay_batch_size as u64,
                true,
            )? as usize,
            max_backoff_secs: parse_env_u64(
                EDGE_SYNC_MAX_BACKOFF_SECS_ENV,
                defaults.max_backoff_secs,
                true,
            )?,
            cleanup_interval_secs: parse_env_u64(
                EDGE_SYNC_CLEANUP_INTERVAL_SECS_ENV,
                defaults.cleanup_interval_secs,
                true,
            )?,
            pre_ack_retention_secs: parse_env_u64(
                EDGE_SYNC_PRE_ACK_RETENTION_SECS_ENV,
                defaults.pre_ack_retention_secs,
                true,
            )?,
        })
    }

    pub fn validate(self) -> Result<(), String> {
        if self.max_entries == 0 {
            return Err("edge sync max entries must be greater than zero".to_string());
        }
        if self.max_bytes == 0 {
            return Err("edge sync max bytes must be greater than zero".to_string());
        }
        if self.max_log_bytes == 0 {
            return Err("edge sync max log bytes must be greater than zero".to_string());
        }
        if self.max_record_bytes == 0 {
            return Err("edge sync max record bytes must be greater than zero".to_string());
        }
        if self.replay_interval_secs == 0 {
            return Err("edge sync replay interval must be greater than zero".to_string());
        }
        if self.replay_batch_size == 0 {
            return Err("edge sync replay batch size must be greater than zero".to_string());
        }
        if self.max_backoff_secs == 0 {
            return Err("edge sync replay backoff must be greater than zero".to_string());
        }
        if self.cleanup_interval_secs == 0 {
            return Err("edge sync cleanup interval must be greater than zero".to_string());
        }
        if self.pre_ack_retention_secs == 0 {
            return Err("edge sync pre-ack retention must be greater than zero".to_string());
        }
        Ok(())
    }

    pub fn replay_interval(self) -> Duration {
        Duration::from_secs(self.replay_interval_secs.max(1))
    }

    pub fn cleanup_interval(self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs.max(1))
    }
}

#[derive(Debug, Clone)]
pub struct EdgeSyncSourceBootstrap {
    pub source_id: String,
    pub upstream_endpoint: String,
    pub shared_auth_token: String,
    pub tenant_mapping: EdgeSyncTenantMapping,
}

#[derive(Debug)]
pub struct EdgeSyncSourceRuntime {
    source_id: String,
    upstream_endpoint: String,
    tenant_mapping: EdgeSyncTenantMapping,
    config: EdgeSyncQueueConfig,
    queue: Arc<EdgeSyncQueue>,
    rpc_client: RpcClient,
    enqueued_total: AtomicU64,
    enqueue_rejected_total: AtomicU64,
    replay_attempts_total: AtomicU64,
    replay_success_total: AtomicU64,
    replay_failures_total: AtomicU64,
    cleanup_runs_total: AtomicU64,
    expired_entries_total: AtomicU64,
    expired_bytes_total: AtomicU64,
    last_successful_replay_unix_ms: AtomicU64,
    last_enqueue_error: RwLock<Option<String>>,
    last_replay_error: RwLock<Option<String>>,
    last_upstream_acknowledgement: RwLock<Option<WriteAcknowledgement>>,
    replayed_rows_total: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct EdgeSyncRuntimeContext {
    pub source: Option<Arc<EdgeSyncSourceRuntime>>,
    pub accept_dedupe_store: Option<Arc<DedupeWindowStore>>,
    pub accept_dedupe_config: Option<DedupeConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeSyncEnqueueCause {
    InvalidIdempotencyKey {
        message: String,
    },
    RecordTooLarge {
        bytes: u64,
        max_bytes: u64,
    },
    TotalEntriesLimit {
        max_entries: usize,
    },
    TotalBytesLimit {
        max_bytes: u64,
        queued_bytes: u64,
        record_bytes: u64,
    },
    IdExhausted,
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
    Persistence {
        message: String,
    },
    PersistenceFenced {
        message: String,
    },
}

impl EdgeSyncEnqueueCause {
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Persistence { .. })
    }

    pub fn is_disk_resource_limit(&self) -> bool {
        matches!(
            self,
            Self::DiskQuotaExceeded { .. }
                | Self::InsufficientDiskSpace { .. }
                | Self::InsufficientCompactionHeadroom { .. }
        )
    }
}

impl std::fmt::Display for EdgeSyncEnqueueCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIdempotencyKey { message } => {
                write!(formatter, "invalid edge-sync idempotency key: {message}")
            }
            Self::RecordTooLarge { bytes, max_bytes } => write!(
                formatter,
                "edge sync record exceeds max size: {bytes} bytes > {max_bytes} bytes"
            ),
            Self::TotalEntriesLimit { max_entries } => write!(
                formatter,
                "edge sync queue entry limit reached: {max_entries}"
            ),
            Self::TotalBytesLimit {
                max_bytes,
                queued_bytes,
                record_bytes,
            } => write!(
                formatter,
                "edge sync queue byte limit reached: queued {queued_bytes} + record {record_bytes} > {max_bytes}"
            ),
            Self::IdExhausted => write!(formatter, "edge sync queue entry id space exhausted"),
            Self::DiskQuotaExceeded {
                limit,
                used,
                reserved,
                requested,
            } => write!(
                formatter,
                "edge sync local disk quota exceeded: limit {limit} bytes, used {used} bytes, reserved {reserved} bytes, requested {requested} bytes"
            ),
            Self::InsufficientDiskSpace {
                required,
                available,
            } => write!(
                formatter,
                "edge sync local disk headroom exhausted: required {required} bytes, available {available} bytes"
            ),
            Self::InsufficientCompactionHeadroom {
                limit,
                used,
                reserved,
                requested,
            } => write!(
                formatter,
                "edge sync maintenance headroom exhausted: limit {limit} bytes, used {used} bytes, reserved {reserved} bytes, requested {requested} bytes"
            ),
            Self::Persistence { message } => {
                write!(formatter, "edge sync queue persistence failure: {message}")
            }
            Self::PersistenceFenced { message } => write!(
                formatter,
                "edge sync queue fenced after an indeterminate persistence failure: {message}"
            ),
        }
    }
}

impl std::error::Error for EdgeSyncEnqueueCause {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeSyncTypedEnqueueError {
    pub submitted_rows: usize,
    pub queued_rows: usize,
    pub cause: EdgeSyncEnqueueCause,
}

impl std::fmt::Display for EdgeSyncTypedEnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "queued {} of {} rows before edge-sync enqueue failed: {}",
            self.queued_rows, self.submitted_rows, self.cause
        )
    }
}

impl std::error::Error for EdgeSyncTypedEnqueueError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeSyncQueueSnapshot {
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub log_bytes: u64,
    pub oldest_enqueued_unix_ms: Option<u64>,
    pub persistence_fenced: bool,
    pub persistence_fence_reason: Option<String>,
    pub cleanup_pending: bool,
    pub last_cleanup_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeSyncSourceStatusSnapshot {
    pub enabled: bool,
    pub source_id: Option<String>,
    pub upstream_endpoint: Option<String>,
    pub tenant_mapping_mode: String,
    pub static_tenant_id: Option<String>,
    pub conflict_semantics: String,
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub log_bytes: u64,
    pub oldest_queued_age_ms: Option<u64>,
    pub max_entries: usize,
    pub max_bytes: u64,
    pub max_log_bytes: u64,
    pub max_record_bytes: u64,
    pub replay_interval_secs: u64,
    pub replay_batch_size: usize,
    pub max_backoff_secs: u64,
    pub cleanup_interval_secs: u64,
    pub pre_ack_retention_secs: u64,
    pub enqueued_total: u64,
    pub enqueue_rejected_total: u64,
    pub replay_attempts_total: u64,
    pub replay_success_total: u64,
    pub replay_failures_total: u64,
    pub replayed_rows_total: u64,
    pub cleanup_runs_total: u64,
    pub expired_entries_total: u64,
    pub expired_bytes_total: u64,
    pub last_successful_replay_unix_ms: Option<u64>,
    pub last_enqueue_error: Option<String>,
    pub last_replay_error: Option<String>,
    pub last_upstream_acknowledgement: Option<WriteAcknowledgement>,
    pub persistence_fenced: bool,
    pub persistence_fence_reason: Option<String>,
    pub cleanup_pending: bool,
    pub last_cleanup_error: Option<String>,
    pub degraded: bool,
}

impl Default for EdgeSyncSourceStatusSnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            source_id: None,
            upstream_endpoint: None,
            tenant_mapping_mode: "preserve".to_string(),
            static_tenant_id: None,
            conflict_semantics: "idempotent_batch_only".to_string(),
            queued_entries: 0,
            queued_bytes: 0,
            log_bytes: 0,
            oldest_queued_age_ms: None,
            max_entries: 0,
            max_bytes: 0,
            max_log_bytes: 0,
            max_record_bytes: 0,
            replay_interval_secs: 0,
            replay_batch_size: 0,
            max_backoff_secs: 0,
            cleanup_interval_secs: 0,
            pre_ack_retention_secs: 0,
            enqueued_total: 0,
            enqueue_rejected_total: 0,
            replay_attempts_total: 0,
            replay_success_total: 0,
            replay_failures_total: 0,
            replayed_rows_total: 0,
            cleanup_runs_total: 0,
            expired_entries_total: 0,
            expired_bytes_total: 0,
            last_successful_replay_unix_ms: None,
            last_enqueue_error: None,
            last_replay_error: None,
            last_upstream_acknowledgement: None,
            persistence_fenced: false,
            persistence_fence_reason: None,
            cleanup_pending: false,
            last_cleanup_error: None,
            degraded: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeSyncAcceptStatusSnapshot {
    pub enabled: bool,
    pub dedupe_window_secs: u64,
    pub max_entries: usize,
    pub max_log_bytes: u64,
    pub cleanup_interval_secs: u64,
    pub requests_total: u64,
    pub accepted_total: u64,
    pub duplicates_total: u64,
    pub inflight_rejections_total: u64,
    pub commits_total: u64,
    pub aborts_total: u64,
    pub cleanup_runs_total: u64,
    pub expired_keys_total: u64,
    pub evicted_keys_total: u64,
    pub persistence_failures_total: u64,
    pub active_keys: u64,
    pub inflight_keys: u64,
    pub log_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdgeSyncEntry {
    id: u64,
    idempotency_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_capabilities: Vec<String>,
    rows: Vec<InternalRow>,
    #[serde(default)]
    queue_bytes: u64,
    enqueued_unix_ms: u64,
    next_attempt_unix_ms: u64,
    attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum EdgeSyncLogRecord {
    Put { entry: EdgeSyncEntry },
    Ack { id: u64 },
}

#[derive(Debug)]
struct EdgeSyncQueue {
    path: PathBuf,
    config: EdgeSyncQueueConfig,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    state: Mutex<EdgeSyncQueueState>,
}

#[derive(Debug)]
struct EdgeSyncQueueState {
    pending: BTreeMap<u64, EdgeSyncEntry>,
    queued_bytes: u64,
    log_bytes: u64,
    log_records: u64,
    next_id: Option<u64>,
    persistence_fenced: Option<String>,
    last_cleanup_error: Option<String>,
}

impl EdgeSyncSourceRuntime {
    pub fn open(base_data_path: &Path, bootstrap: EdgeSyncSourceBootstrap) -> Result<Self, String> {
        Self::open_with_disk_budget(base_data_path, bootstrap, None)
    }

    pub fn open_with_disk_budget(
        base_data_path: &Path,
        bootstrap: EdgeSyncSourceBootstrap,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Self, String> {
        let config = EdgeSyncQueueConfig::from_env()?;
        Self::open_with_config_and_disk_budget(base_data_path, bootstrap, config, local_disk_budget)
    }

    #[cfg(test)]
    fn open_with_config(
        base_data_path: &Path,
        bootstrap: EdgeSyncSourceBootstrap,
        config: EdgeSyncQueueConfig,
    ) -> Result<Self, String> {
        Self::open_with_config_and_disk_budget(base_data_path, bootstrap, config, None)
    }

    fn open_with_config_and_disk_budget(
        base_data_path: &Path,
        bootstrap: EdgeSyncSourceBootstrap,
        config: EdgeSyncQueueConfig,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Self, String> {
        config.validate()?;

        let queue_path = edge_sync_dir(base_data_path).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue = Arc::new(EdgeSyncQueue::open_with_disk_budget(
            queue_path,
            config,
            local_disk_budget,
        )?);
        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(crate::cluster::rpc::DEFAULT_RPC_TIMEOUT_MS),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: bootstrap.shared_auth_token,
            internal_auth_runtime: None,
            local_node_id: bootstrap.source_id.clone(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });

        Ok(Self {
            source_id: bootstrap.source_id,
            upstream_endpoint: bootstrap.upstream_endpoint,
            tenant_mapping: bootstrap.tenant_mapping,
            config,
            queue,
            rpc_client,
            enqueued_total: AtomicU64::new(0),
            enqueue_rejected_total: AtomicU64::new(0),
            replay_attempts_total: AtomicU64::new(0),
            replay_success_total: AtomicU64::new(0),
            replay_failures_total: AtomicU64::new(0),
            cleanup_runs_total: AtomicU64::new(0),
            expired_entries_total: AtomicU64::new(0),
            expired_bytes_total: AtomicU64::new(0),
            last_successful_replay_unix_ms: AtomicU64::new(0),
            last_enqueue_error: RwLock::new(None),
            last_replay_error: RwLock::new(None),
            last_upstream_acknowledgement: RwLock::new(None),
            replayed_rows_total: AtomicU64::new(0),
        })
    }

    pub fn start_replay_worker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(runtime.config.replay_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = runtime.replay_due_once().await {
                    runtime.set_last_replay_error(Some(err));
                }
            }
        })
    }

    pub fn start_cleanup_worker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(runtime.config.cleanup_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = runtime.cleanup_once() {
                    runtime.set_last_replay_error(Some(err));
                }
            }
        })
    }

    pub fn enqueue_rows_typed(&self, rows: &[Row]) -> Result<usize, EdgeSyncTypedEnqueueError> {
        if rows.is_empty() {
            return Ok(0);
        }

        match self.prepare_rows(rows) {
            Ok(mapped_rows) => {
                let mut queued_rows = 0usize;
                for chunk in mapped_rows.chunks(MAX_INTERNAL_INGEST_ROWS) {
                    if let Err(err) = self.queue.enqueue_rows(&self.source_id, chunk) {
                        self.enqueue_rejected_total.fetch_add(1, Ordering::Relaxed);
                        self.set_last_enqueue_error(Some(err.to_string()));
                        return Err(EdgeSyncTypedEnqueueError {
                            submitted_rows: rows.len(),
                            queued_rows,
                            cause: err,
                        });
                    }
                    queued_rows = queued_rows.saturating_add(chunk.len());
                    self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                    self.set_last_enqueue_error(None);
                }
                Ok(queued_rows)
            }
            Err(err) => {
                self.enqueue_rejected_total.fetch_add(1, Ordering::Relaxed);
                self.set_last_enqueue_error(Some(err.clone()));
                Err(EdgeSyncTypedEnqueueError {
                    submitted_rows: rows.len(),
                    queued_rows: 0,
                    cause: EdgeSyncEnqueueCause::Persistence { message: err },
                })
            }
        }
    }

    pub fn status_snapshot(&self) -> EdgeSyncSourceStatusSnapshot {
        let queue = self.queue.snapshot();
        let now = unix_timestamp_millis();
        let oldest_queued_age_ms = queue
            .oldest_enqueued_unix_ms
            .map(|oldest| now.saturating_sub(oldest));
        let last_successful_replay_unix_ms =
            option_atomic_millis(&self.last_successful_replay_unix_ms);
        let last_enqueue_error = self
            .last_enqueue_error
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let last_replay_error = self
            .last_replay_error
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let last_upstream_acknowledgement = *self
            .last_upstream_acknowledgement
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        EdgeSyncSourceStatusSnapshot {
            enabled: true,
            source_id: Some(self.source_id.clone()),
            upstream_endpoint: Some(self.upstream_endpoint.clone()),
            tenant_mapping_mode: match self.tenant_mapping.mode {
                EdgeSyncTenantMappingMode::Preserve => "preserve",
                EdgeSyncTenantMappingMode::Static => "static",
            }
            .to_string(),
            static_tenant_id: self.tenant_mapping.static_tenant_id.clone(),
            conflict_semantics: "idempotent_batch_only".to_string(),
            queued_entries: queue.queued_entries,
            queued_bytes: queue.queued_bytes,
            log_bytes: queue.log_bytes,
            oldest_queued_age_ms,
            max_entries: self.config.max_entries,
            max_bytes: self.config.max_bytes,
            max_log_bytes: self.config.max_log_bytes,
            max_record_bytes: self.config.max_record_bytes,
            replay_interval_secs: self.config.replay_interval_secs,
            replay_batch_size: self.config.replay_batch_size,
            max_backoff_secs: self.config.max_backoff_secs,
            cleanup_interval_secs: self.config.cleanup_interval_secs,
            pre_ack_retention_secs: self.config.pre_ack_retention_secs,
            enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            enqueue_rejected_total: self.enqueue_rejected_total.load(Ordering::Relaxed),
            replay_attempts_total: self.replay_attempts_total.load(Ordering::Relaxed),
            replay_success_total: self.replay_success_total.load(Ordering::Relaxed),
            replay_failures_total: self.replay_failures_total.load(Ordering::Relaxed),
            replayed_rows_total: self.replayed_rows_total.load(Ordering::Relaxed),
            cleanup_runs_total: self.cleanup_runs_total.load(Ordering::Relaxed),
            expired_entries_total: self.expired_entries_total.load(Ordering::Relaxed),
            expired_bytes_total: self.expired_bytes_total.load(Ordering::Relaxed),
            last_successful_replay_unix_ms,
            last_enqueue_error: last_enqueue_error.clone(),
            last_replay_error: last_replay_error.clone(),
            last_upstream_acknowledgement,
            persistence_fenced: queue.persistence_fenced,
            persistence_fence_reason: queue.persistence_fence_reason,
            cleanup_pending: queue.cleanup_pending,
            last_cleanup_error: queue.last_cleanup_error,
            degraded: queue.persistence_fenced
                || queue.cleanup_pending
                || (queue.queued_entries > 0
                    && (last_enqueue_error.is_some() || last_replay_error.is_some())),
        }
    }

    async fn replay_due_once(&self) -> Result<(), String> {
        let entries = self
            .queue
            .collect_due_entries(self.config.replay_batch_size);
        for entry in entries {
            self.replay_attempts_total.fetch_add(1, Ordering::Relaxed);
            let request = InternalIngestRowsRequest {
                ring_version: 1,
                idempotency_key: Some(entry.idempotency_key.clone()),
                required_capabilities: entry.required_capabilities.clone(),
                rows: entry.rows.clone(),
            };
            match self
                .rpc_client
                .ingest_rows(&self.upstream_endpoint, &request)
                .await
            {
                Ok(response) => {
                    match validate_confirmed_atomic_ingest_response(request.rows.len(), &response) {
                        Ok(acknowledgement) => {
                            *self
                                .last_upstream_acknowledgement
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                Some(acknowledgement);
                            self.queue.ack_entry(entry.id)?;
                            self.replay_success_total.fetch_add(1, Ordering::Relaxed);
                            self.replayed_rows_total.fetch_add(
                                u64::try_from(request.rows.len()).unwrap_or(u64::MAX),
                                Ordering::Relaxed,
                            );
                            self.last_successful_replay_unix_ms
                                .store(unix_timestamp_millis(), Ordering::Relaxed);
                            self.set_last_replay_error(None);
                        }
                        Err(err) => {
                            self.replay_failures_total.fetch_add(1, Ordering::Relaxed);
                            self.queue
                                .reschedule_entry(entry.id, self.config.max_backoff_secs)?;
                            self.set_last_replay_error(Some(err));
                        }
                    }
                }
                Err(err) => {
                    self.replay_failures_total.fetch_add(1, Ordering::Relaxed);
                    self.queue
                        .reschedule_entry(entry.id, self.config.max_backoff_secs)?;
                    self.set_last_replay_error(Some(err.to_string()));
                }
            }
        }
        Ok(())
    }

    fn cleanup_once(&self) -> Result<(), String> {
        self.cleanup_runs_total.fetch_add(1, Ordering::Relaxed);
        let cutoff = unix_timestamp_millis()
            .saturating_sub(self.config.pre_ack_retention_secs.saturating_mul(1_000));
        let expired = self.queue.expire_before(cutoff)?;
        self.expired_entries_total
            .fetch_add(expired.0, Ordering::Relaxed);
        self.expired_bytes_total
            .fetch_add(expired.1, Ordering::Relaxed);
        Ok(())
    }

    fn prepare_rows(&self, rows: &[Row]) -> Result<Vec<Row>, String> {
        match self.tenant_mapping.mode {
            EdgeSyncTenantMappingMode::Preserve => Ok(rows.to_vec()),
            EdgeSyncTenantMappingMode::Static => {
                let tenant_id = self.tenant_mapping.static_tenant_id().ok_or_else(|| {
                    "edge sync static tenant mapping is missing a tenant id".to_string()
                })?;
                Ok(rows
                    .iter()
                    .map(|row| override_row_tenant(row, tenant_id))
                    .collect())
            }
        }
    }

    fn set_last_enqueue_error(&self, error: Option<String>) {
        *self
            .last_enqueue_error
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = error;
    }

    fn set_last_replay_error(&self, error: Option<String>) {
        *self
            .last_replay_error
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = error;
    }
}

impl EdgeSyncRuntimeContext {
    pub fn source_status_snapshot(&self) -> EdgeSyncSourceStatusSnapshot {
        self.source
            .as_ref()
            .map(|runtime| runtime.status_snapshot())
            .unwrap_or_default()
    }

    pub fn accept_status_snapshot(&self) -> EdgeSyncAcceptStatusSnapshot {
        let Some(config) = self.accept_dedupe_config else {
            return EdgeSyncAcceptStatusSnapshot::default();
        };
        let snapshot = dedupe_metrics_snapshot();
        EdgeSyncAcceptStatusSnapshot {
            enabled: self.accept_dedupe_store.is_some(),
            dedupe_window_secs: config.window_secs,
            max_entries: config.max_entries,
            max_log_bytes: config.max_log_bytes,
            cleanup_interval_secs: config.cleanup_interval_secs,
            requests_total: snapshot.requests_total,
            accepted_total: snapshot.accepted_total,
            duplicates_total: snapshot.duplicates_total,
            inflight_rejections_total: snapshot.inflight_rejections_total,
            commits_total: snapshot.commits_total,
            aborts_total: snapshot.aborts_total,
            cleanup_runs_total: snapshot.cleanup_runs_total,
            expired_keys_total: snapshot.expired_keys_total,
            evicted_keys_total: snapshot.evicted_keys_total,
            persistence_failures_total: snapshot.persistence_failures_total,
            active_keys: snapshot.active_keys,
            inflight_keys: snapshot.inflight_keys,
            log_bytes: snapshot.log_bytes,
        }
    }
}

impl EdgeSyncQueue {
    #[cfg(test)]
    fn open(path: PathBuf, config: EdgeSyncQueueConfig) -> Result<Self, String> {
        Self::open_with_disk_budget(path, config, None)
    }

    fn open_with_disk_budget(
        path: PathBuf,
        config: EdgeSyncQueueConfig,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            if let Some(local_disk_budget) = local_disk_budget.as_ref() {
                local_disk_budget
                    .create_dir_all_and_sync_parents(parent)
                    .map_err(|err| {
                        format!(
                            "failed to create managed edge sync queue directory {}: {err}",
                            parent.display()
                        )
                    })?;
                local_disk_budget
                    .cleanup_atomic_write_temps(&path)
                    .map_err(|err| {
                        format!(
                            "failed to clean managed edge sync queue temporaries for {}: {err}",
                            path.display()
                        )
                    })?;
                let legacy_compaction_temp = path.with_extension("tmp");
                local_disk_budget
                    .remove_managed_file_if_exists_and_sync_parent(
                        &legacy_compaction_temp,
                        DiskCategory::Temporary,
                    )
                    .map_err(|err| {
                        format!(
                            "failed to clean legacy edge sync compaction file {}: {err}",
                            legacy_compaction_temp.display()
                        )
                    })?;
                local_disk_budget
                    .validate_managed_file_path(&path)
                    .map_err(|err| {
                        format!(
                            "invalid managed edge sync queue path {}: {err}",
                            path.display()
                        )
                    })?;
            } else {
                std::fs::create_dir_all(parent).map_err(|err| {
                    format!(
                        "failed to create edge sync queue directory {}: {err}",
                        parent.display()
                    )
                })?;
                let legacy_compaction_temp = path.with_extension("tmp");
                match std::fs::remove_file(&legacy_compaction_temp) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(format!(
                            "failed to clean legacy edge sync compaction file {}: {err}",
                            legacy_compaction_temp.display()
                        ));
                    }
                }
            }
        }

        if !path.exists() {
            if let Some(local_disk_budget) = local_disk_budget.as_ref() {
                local_disk_budget
                    .append_file_and_sync_parent(&path, &[], DiskCategory::EdgeSync)
                    .map_err(|err| {
                        format!(
                            "failed to initialize managed edge sync queue log {}: {err}",
                            path.display()
                        )
                    })?;
            } else {
                tsink::engine::fs_utils::write_file_atomically_and_sync_parent(&path, &[])
                    .map_err(|err| {
                        format!(
                            "failed to initialize edge sync queue log {}: {err}",
                            path.display()
                        )
                    })?;
            }
        }

        let mut pending = BTreeMap::new();
        let mut next_id = Some(1u64);
        let mut log_records = 0u64;
        load_existing_records(&path, &mut pending, &mut next_id, &mut log_records)?;

        let mut queued_bytes = 0u64;
        for entry in pending.values_mut() {
            if entry.queue_bytes == 0 {
                entry.queue_bytes = estimate_queue_bytes(entry);
            }
            queued_bytes = queued_bytes.checked_add(entry.queue_bytes).ok_or_else(|| {
                format!(
                    "edge sync queued byte accounting overflow while opening {}",
                    path.display()
                )
            })?;
        }
        let log_bytes = std::fs::metadata(&path)
            .map_err(|err| {
                format!(
                    "failed to inspect edge sync queue log {}: {err}",
                    path.display()
                )
            })?
            .len();
        let queue = Self {
            path,
            config,
            local_disk_budget,
            state: Mutex::new(EdgeSyncQueueState {
                pending,
                queued_bytes,
                log_bytes,
                log_records,
                next_id,
                persistence_fenced: None,
                last_cleanup_error: None,
            }),
        };
        {
            let mut state = queue
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.log_bytes > queue.config.max_log_bytes {
                try_compact_after_durable_record_locked(
                    &queue.path,
                    queue.local_disk_budget.as_ref(),
                    &mut state,
                    "startup recovery",
                );
            }
        }
        Ok(queue)
    }

    fn enqueue_rows(&self, source_id: &str, rows: &[Row]) -> Result<(), EdgeSyncEnqueueCause> {
        let idempotency_key = build_edge_sync_idempotency_key(source_id, unix_timestamp_millis());
        validate_idempotency_key(&idempotency_key)
            .map_err(|message| EdgeSyncEnqueueCause::InvalidIdempotencyKey { message })?;

        let mut entry = EdgeSyncEntry {
            id: 0,
            idempotency_key,
            required_capabilities: required_capabilities_for_rows(rows),
            rows: rows.iter().map(InternalRow::from).collect(),
            queue_bytes: 0,
            enqueued_unix_ms: unix_timestamp_millis(),
            next_attempt_unix_ms: unix_timestamp_millis(),
            attempts: 0,
        };
        entry.required_capabilities = normalize_capabilities(entry.required_capabilities);
        entry.queue_bytes = estimate_queue_bytes(&entry);

        if entry.queue_bytes > self.config.max_record_bytes {
            return Err(EdgeSyncEnqueueCause::RecordTooLarge {
                bytes: entry.queue_bytes,
                max_bytes: self.config.max_record_bytes,
            });
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_queue_not_fenced(&state)
            .map_err(|message| EdgeSyncEnqueueCause::PersistenceFenced { message })?;
        if state.pending.len() >= self.config.max_entries {
            return Err(EdgeSyncEnqueueCause::TotalEntriesLimit {
                max_entries: self.config.max_entries,
            });
        }
        let next_queued_bytes = state
            .queued_bytes
            .checked_add(entry.queue_bytes)
            .ok_or_else(|| EdgeSyncEnqueueCause::TotalBytesLimit {
                max_bytes: self.config.max_bytes,
                queued_bytes: state.queued_bytes,
                record_bytes: entry.queue_bytes,
            })?;
        if next_queued_bytes > self.config.max_bytes {
            return Err(EdgeSyncEnqueueCause::TotalBytesLimit {
                max_bytes: self.config.max_bytes,
                queued_bytes: state.queued_bytes,
                record_bytes: entry.queue_bytes,
            });
        }

        entry.id = state.next_id.ok_or(EdgeSyncEnqueueCause::IdExhausted)?;
        let next_id = entry.id.checked_add(1);
        let encoded = encode_log_record(&EdgeSyncLogRecord::Put {
            entry: entry.clone(),
        })
        .map_err(|message| EdgeSyncEnqueueCause::Persistence { message })?;
        let encoded_bytes =
            u64::try_from(encoded.len()).map_err(|_| EdgeSyncEnqueueCause::RecordTooLarge {
                bytes: u64::MAX,
                max_bytes: self.config.max_record_bytes,
            })?;
        if encoded_bytes > self.config.max_record_bytes {
            return Err(EdgeSyncEnqueueCause::RecordTooLarge {
                bytes: encoded_bytes,
                max_bytes: self.config.max_record_bytes,
            });
        }
        if let Err(err) = append_encoded_log_locked(
            &self.path,
            self.local_disk_budget.as_ref(),
            &mut state,
            &encoded,
            1,
            false,
        ) {
            fence_after_indeterminate_append_error(&mut state, &err);
            return Err(edge_sync_enqueue_persistence_error(err));
        }
        state.next_id = next_id;
        state.queued_bytes = next_queued_bytes;
        state.pending.insert(entry.id, entry);

        if state.log_bytes > self.config.max_log_bytes {
            try_compact_after_durable_record_locked(
                &self.path,
                self.local_disk_budget.as_ref(),
                &mut state,
                "enqueue",
            );
        }
        Ok(())
    }

    fn snapshot(&self) -> EdgeSyncQueueSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        EdgeSyncQueueSnapshot {
            queued_entries: state.pending.len() as u64,
            queued_bytes: state.queued_bytes,
            log_bytes: state.log_bytes,
            oldest_enqueued_unix_ms: state
                .pending
                .values()
                .map(|entry| entry.enqueued_unix_ms)
                .min(),
            persistence_fenced: state.persistence_fenced.is_some(),
            persistence_fence_reason: state.persistence_fenced.clone(),
            cleanup_pending: state.last_cleanup_error.is_some(),
            last_cleanup_error: state.last_cleanup_error.clone(),
        }
    }

    fn collect_due_entries(&self, limit: usize) -> Vec<EdgeSyncEntry> {
        let now = unix_timestamp_millis();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .pending
            .values()
            .filter(|entry| entry.next_attempt_unix_ms <= now)
            .take(limit.max(1))
            .cloned()
            .collect()
    }

    fn ack_entry(&self, id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_queue_not_fenced(&state)?;
        let Some(entry) = state.pending.get(&id).cloned() else {
            return Ok(());
        };
        let encoded = encode_log_record(&EdgeSyncLogRecord::Ack { id })?;
        let used_recovery_admission = match append_encoded_log_locked(
            &self.path,
            self.local_disk_budget.as_ref(),
            &mut state,
            &encoded,
            1,
            true,
        ) {
            Ok(used_recovery_admission) => used_recovery_admission,
            Err(err) => {
                fence_after_indeterminate_append_error(&mut state, &err);
                return Err(err.to_string());
            }
        };
        state.pending.remove(&id);
        state.queued_bytes = state.queued_bytes.saturating_sub(entry.queue_bytes);
        let disk_growth_capacity_exhausted = self
            .local_disk_budget
            .as_deref()
            .is_some_and(local_disk_growth_capacity_exhausted);
        if state.log_bytes > self.config.max_log_bytes
            || used_recovery_admission
            || disk_growth_capacity_exhausted
        {
            try_compact_after_durable_record_locked(
                &self.path,
                self.local_disk_budget.as_ref(),
                &mut state,
                "acknowledgement",
            );
        }
        Ok(())
    }

    fn reschedule_entry(&self, id: u64, max_backoff_secs: u64) -> Result<(), String> {
        let now = unix_timestamp_millis();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = state.pending.get_mut(&id) {
            entry.attempts = entry.attempts.saturating_add(1);
            let backoff_secs = 1u64
                .checked_shl(entry.attempts.min(16))
                .unwrap_or(u64::MAX)
                .min(max_backoff_secs.max(1));
            entry.next_attempt_unix_ms = now.saturating_add(backoff_secs.saturating_mul(1_000));
        }
        Ok(())
    }

    fn expire_before(&self, cutoff_unix_ms: u64) -> Result<(u64, u64), String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_queue_not_fenced(&state)?;
        let to_remove = state
            .pending
            .iter()
            .filter(|(_, entry)| entry.enqueued_unix_ms < cutoff_unix_ms)
            .map(|(id, entry)| (*id, entry.queue_bytes))
            .collect::<Vec<_>>();
        if to_remove.is_empty() {
            if state.last_cleanup_error.is_some()
                || (stale_record_count(&state) > 0
                    && (state.log_bytes > self.config.max_log_bytes
                        || self
                            .local_disk_budget
                            .as_deref()
                            .is_some_and(local_disk_growth_capacity_exhausted)))
            {
                try_compact_after_durable_record_locked(
                    &self.path,
                    self.local_disk_budget.as_ref(),
                    &mut state,
                    "cleanup retry",
                );
            }
            return Ok((0, 0));
        }

        let mut encoded = Vec::new();
        for (id, _) in &to_remove {
            let record = encode_log_record(&EdgeSyncLogRecord::Ack { id: *id })?;
            encoded
                .try_reserve(record.len())
                .map_err(|_| "edge sync expiry acknowledgement batch is too large".to_string())?;
            encoded.extend_from_slice(&record);
        }
        let record_count = u64::try_from(to_remove.len())
            .map_err(|_| "edge sync expiry record count exceeds u64".to_string())?;
        let used_recovery_admission = match append_encoded_log_locked(
            &self.path,
            self.local_disk_budget.as_ref(),
            &mut state,
            &encoded,
            record_count,
            true,
        ) {
            Ok(used_recovery_admission) => used_recovery_admission,
            Err(err) => {
                fence_after_indeterminate_append_error(&mut state, &err);
                return Err(err.to_string());
            }
        };

        let mut expired_bytes = 0u64;
        for (id, queue_bytes) in &to_remove {
            state.pending.remove(id);
            state.queued_bytes = state.queued_bytes.saturating_sub(*queue_bytes);
            expired_bytes = expired_bytes.saturating_add(*queue_bytes);
        }
        let expired_entries = record_count;

        if state.log_bytes > self.config.max_log_bytes
            || used_recovery_admission
            || self
                .local_disk_budget
                .as_deref()
                .is_some_and(local_disk_growth_capacity_exhausted)
        {
            try_compact_after_durable_record_locked(
                &self.path,
                self.local_disk_budget.as_ref(),
                &mut state,
                "expiry",
            );
        }
        Ok((expired_entries, expired_bytes))
    }
}

pub fn edge_sync_dir(base_data_path: &Path) -> PathBuf {
    base_data_path.join(EDGE_SYNC_DIR_NAME)
}

pub fn edge_sync_accept_internal_api(auth_token: &str) -> InternalApiConfig {
    InternalApiConfig::new(
        auth_token.to_string(),
        INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        false,
        Vec::new(),
    )
    .with_compatibility(CompatibilityProfile::default())
}

pub fn edge_sync_accept_dedupe_config() -> Result<DedupeConfig, String> {
    let defaults = DedupeConfig::default();
    Ok(DedupeConfig {
        window_secs: parse_env_u64(
            EDGE_SYNC_DEDUPE_WINDOW_SECS_ENV,
            DEFAULT_EDGE_SYNC_DEDUPE_WINDOW_SECS.max(defaults.window_secs),
            true,
        )?,
        max_entries: parse_env_u64(
            EDGE_SYNC_DEDUPE_MAX_ENTRIES_ENV,
            defaults.max_entries as u64,
            true,
        )? as usize,
        max_log_bytes: parse_env_u64(
            EDGE_SYNC_DEDUPE_MAX_LOG_BYTES_ENV,
            defaults.max_log_bytes,
            true,
        )?,
        cleanup_interval_secs: parse_env_u64(
            EDGE_SYNC_DEDUPE_CLEANUP_INTERVAL_SECS_ENV,
            defaults.cleanup_interval_secs,
            true,
        )?,
    })
}

pub fn open_edge_sync_accept_dedupe_store(
    base_data_path: &Path,
    config: DedupeConfig,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
) -> Result<Arc<DedupeWindowStore>, String> {
    let path = edge_sync_dir(base_data_path).join(EDGE_SYNC_DEDUPE_FILE_NAME);
    match local_disk_budget {
        Some(local_disk_budget) => DedupeWindowStore::open_with_disk_budget(
            path,
            config,
            Some(local_disk_budget),
            DiskCategory::EdgeSync,
        ),
        None => DedupeWindowStore::open(path, config),
    }
    .map(Arc::new)
}

fn override_row_tenant(row: &Row, tenant_id: &str) -> Row {
    let mut labels = row
        .labels()
        .iter()
        .filter(|label| label.name != tenant::TENANT_LABEL)
        .cloned()
        .collect::<Vec<_>>();
    labels.push(Label::new(tenant::TENANT_LABEL, tenant_id));
    Row::with_labels(row.metric(), labels, row.data_point().clone())
}

fn build_edge_sync_idempotency_key(source_id: &str, unix_ms: u64) -> String {
    static NEXT_KEY: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    format!("tsink:edge:{source_id}:{unix_ms}:{seq}")
}

fn load_existing_records(
    path: &Path,
    pending: &mut BTreeMap<u64, EdgeSyncEntry>,
    next_id: &mut Option<u64>,
    log_records: &mut u64,
) -> Result<(), String> {
    let file = File::open(path).map_err(|err| {
        format!(
            "failed to open edge sync queue log {}: {err}",
            path.display()
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut line_number = 0u64;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line).map_err(|err| {
            format!(
                "failed to read edge sync queue log {}: {err}",
                path.display()
            )
        })?;
        if read == 0 {
            break;
        }
        line_number = line_number
            .checked_add(1)
            .ok_or_else(|| "edge sync queue line number overflow".to_string())?;
        if line.last() != Some(&b'\n') {
            return Err(format!(
                "edge sync queue log {} ends with an incomplete record at line {line_number}",
                path.display()
            ));
        }
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            return Err(format!(
                "edge sync queue log {} contains a blank record at line {line_number}",
                path.display()
            ));
        }
        let record: EdgeSyncLogRecord = serde_json::from_slice(&line).map_err(|err| {
            format!(
                "failed to decode edge sync queue record in {} at line {line_number}: {err}",
                path.display(),
            )
        })?;
        *log_records = log_records
            .checked_add(1)
            .ok_or_else(|| "edge sync queue record count overflow".to_string())?;
        match record {
            EdgeSyncLogRecord::Put { mut entry } => {
                if entry.queue_bytes == 0 {
                    entry.queue_bytes = estimate_queue_bytes(&entry);
                }
                advance_next_id(next_id, entry.id);
                pending.insert(entry.id, entry);
            }
            EdgeSyncLogRecord::Ack { id } => {
                pending.remove(&id);
                advance_next_id(next_id, id);
            }
        }
    }
    Ok(())
}

fn advance_next_id(next_id: &mut Option<u64>, observed_id: u64) {
    let Some(current) = *next_id else {
        return;
    };
    *next_id = observed_id
        .checked_add(1)
        .map(|candidate| current.max(candidate));
}

fn encode_log_record(record: &EdgeSyncLogRecord) -> Result<Vec<u8>, String> {
    let mut encoded = serde_json::to_vec(record)
        .map_err(|err| format!("failed to encode edge sync queue record: {err}"))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn append_encoded_log_locked(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    state: &mut EdgeSyncQueueState,
    encoded: &[u8],
    record_count: u64,
    allow_recovery: bool,
) -> Result<bool, TsinkError> {
    let encoded_bytes = u64::try_from(encoded.len())
        .map_err(|_| TsinkError::Other("edge sync append exceeds u64 bytes".to_string()))?;
    let next_log_bytes = state
        .log_bytes
        .checked_add(encoded_bytes)
        .ok_or_else(|| TsinkError::Other("edge sync log byte accounting overflow".to_string()))?;
    let next_log_records = state
        .log_records
        .checked_add(record_count)
        .ok_or_else(|| TsinkError::Other("edge sync log record accounting overflow".to_string()))?;

    #[cfg(test)]
    if append_failure_injected(path) {
        return Err(TsinkError::Other(
            "injected edge sync queue append failure".to_string(),
        ));
    }

    let used_recovery_admission = if let Some(local_disk_budget) = local_disk_budget {
        if allow_recovery {
            match local_disk_budget.append_file_and_sync_parent(
                path,
                encoded,
                DiskCategory::EdgeSync,
            ) {
                Ok(()) => false,
                Err(
                    TsinkError::DiskQuotaExceeded { .. }
                    | TsinkError::InsufficientDiskSpace { .. }
                    | TsinkError::InsufficientCompactionHeadroom { .. },
                ) => {
                    local_disk_budget.append_file_and_sync_parent_for_recovery(
                        path,
                        encoded,
                        DiskCategory::EdgeSync,
                    )?;
                    true
                }
                Err(err) => return Err(err),
            }
        } else {
            local_disk_budget.append_file_and_sync_parent(path, encoded, DiskCategory::EdgeSync)?;
            false
        }
    } else {
        let mut file = OpenOptions::new().append(true).open(path).map_err(|err| {
            TsinkError::Other(format!(
                "failed to open edge sync queue log for append: {err}"
            ))
        })?;
        file.write_all(encoded).map_err(|err| {
            TsinkError::Other(format!("failed to append edge sync queue record: {err}"))
        })?;
        file.flush().map_err(|err| {
            TsinkError::Other(format!("failed to flush edge sync queue record: {err}"))
        })?;
        file.sync_all().map_err(|err| {
            TsinkError::Other(format!("failed to sync edge sync queue record: {err}"))
        })?;
        false
    };

    state.log_bytes = next_log_bytes;
    state.log_records = next_log_records;
    Ok(used_recovery_admission)
}

fn compacted_log_len(pending: &BTreeMap<u64, EdgeSyncEntry>) -> Result<u64, String> {
    pending.values().try_fold(0u64, |total, entry| {
        let encoded = encode_log_record(&EdgeSyncLogRecord::Put {
            entry: entry.clone(),
        })?;
        let record_bytes = u64::try_from(encoded.len()).map_err(|_| {
            "encoded edge sync compaction record exceeds the supported byte range".to_string()
        })?;
        total
            .checked_add(record_bytes)
            .ok_or_else(|| "compacted edge sync log exceeds the supported byte range".to_string())
    })
}

fn write_compacted_log(
    pending: &BTreeMap<u64, EdgeSyncEntry>,
    writer: &mut dyn Write,
) -> Result<(), String> {
    for entry in pending.values() {
        let encoded = encode_log_record(&EdgeSyncLogRecord::Put {
            entry: entry.clone(),
        })?;
        writer
            .write_all(&encoded)
            .map_err(|err| format!("failed to write edge sync queue compaction record: {err}"))?;
    }
    Ok(())
}

fn compact_locked(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    state: &mut EdgeSyncQueueState,
) -> Result<(), String> {
    #[cfg(test)]
    if compaction_failure_injected(path) {
        return Err("injected edge sync queue compaction failure".to_string());
    }

    let compacted_bytes = compacted_log_len(&state.pending)?;
    if let Some(local_disk_budget) = local_disk_budget {
        local_disk_budget
            .rewrite_file_atomically_and_sync_parent_for_cleanup_with(
                path,
                compacted_bytes,
                DiskCategory::EdgeSync,
                |writer| write_compacted_log(&state.pending, writer).map_err(TsinkError::Other),
            )
            .map_err(|err| {
                format!(
                    "failed to compact managed edge sync queue log {}: {err}",
                    path.display()
                )
            })?;
    } else {
        tsink::engine::fs_utils::write_file_atomically_and_sync_parent_with(
            path,
            compacted_bytes,
            |writer| write_compacted_log(&state.pending, writer).map_err(TsinkError::Other),
        )
        .map_err(|err| {
            format!(
                "failed to compact edge sync queue log {}: {err}",
                path.display()
            )
        })?;
    }

    state.log_bytes = std::fs::metadata(path)
        .map_err(|err| {
            format!(
                "failed to inspect compacted edge sync queue log {}: {err}",
                path.display()
            )
        })?
        .len();
    state.log_records = u64::try_from(state.pending.len())
        .map_err(|_| "edge sync pending entry count exceeds u64".to_string())?;
    Ok(())
}

fn try_compact_after_durable_record_locked(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    state: &mut EdgeSyncQueueState,
    operation: &str,
) {
    match compact_locked(path, local_disk_budget, state) {
        Ok(()) => state.last_cleanup_error = None,
        Err(err) => {
            state.last_cleanup_error = Some(err.clone());
            eprintln!(
                "edge sync queue compaction deferred after durable {operation} record: {err}"
            );
        }
    }
}

fn ensure_queue_not_fenced(state: &EdgeSyncQueueState) -> Result<(), String> {
    match state.persistence_fenced.as_ref() {
        Some(reason) => Err(format!(
            "edge sync queue is fenced after an indeterminate persistence failure: {reason}"
        )),
        None => Ok(()),
    }
}

fn fence_after_indeterminate_append_error(state: &mut EdgeSyncQueueState, error: &TsinkError) {
    if !is_disk_resource_error(error) {
        state.persistence_fenced = Some(error.to_string());
    }
}

fn is_disk_resource_error(error: &TsinkError) -> bool {
    matches!(
        error,
        TsinkError::DiskQuotaExceeded { .. }
            | TsinkError::InsufficientDiskSpace { .. }
            | TsinkError::InsufficientCompactionHeadroom { .. }
    )
}

fn edge_sync_enqueue_persistence_error(error: TsinkError) -> EdgeSyncEnqueueCause {
    match error {
        TsinkError::DiskQuotaExceeded {
            limit,
            used,
            reserved,
            requested,
        } => EdgeSyncEnqueueCause::DiskQuotaExceeded {
            limit,
            used,
            reserved,
            requested,
        },
        TsinkError::InsufficientDiskSpace {
            required,
            available,
        } => EdgeSyncEnqueueCause::InsufficientDiskSpace {
            required,
            available,
        },
        TsinkError::InsufficientCompactionHeadroom {
            limit,
            used,
            reserved,
            requested,
        } => EdgeSyncEnqueueCause::InsufficientCompactionHeadroom {
            limit,
            used,
            reserved,
            requested,
        },
        other => EdgeSyncEnqueueCause::PersistenceFenced {
            message: other.to_string(),
        },
    }
}

fn stale_record_count(state: &EdgeSyncQueueState) -> u64 {
    state.log_records.saturating_sub(state.pending.len() as u64)
}

fn local_disk_growth_capacity_exhausted(budget: &LocalDiskBudget) -> bool {
    let snapshot = budget.snapshot();
    let Some(max_bytes) = snapshot.limits.max_bytes else {
        return false;
    };
    let growth_limit = max_bytes.saturating_sub(snapshot.limits.maintenance_temp_reserve_bytes);
    snapshot
        .accounted_bytes
        .saturating_add(snapshot.reserved_bytes)
        >= growth_limit
}

fn estimate_queue_bytes(entry: &EdgeSyncEntry) -> u64 {
    serde_json::to_vec(entry)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or(u64::MAX)
}

fn parse_env_u64(var: &str, default: u64, positive_only: bool) -> Result<u64, String> {
    match std::env::var(var) {
        Ok(value) => {
            let parsed = value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("environment variable {var} must be a positive integer"))?;
            if positive_only && parsed == 0 {
                return Err(format!(
                    "environment variable {var} must be greater than zero"
                ));
            }
            Ok(parsed)
        }
        Err(_) => Ok(default),
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn option_atomic_millis(value: &AtomicU64) -> Option<u64> {
    match value.load(Ordering::Relaxed) {
        0 => None,
        millis => Some(millis),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::rpc::InternalIngestRowsResponse;
    use crate::http::read_http_request;
    use tokio::io::AsyncWriteExt;
    use tsink::LocalDiskLimits;

    fn tempdir() -> tempfile::TempDir {
        tempfile::TempDir::new().expect("tempdir")
    }

    fn test_rows() -> Vec<Row> {
        vec![
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("host", "edge-a"),
                    Label::new(tenant::TENANT_LABEL, "tenant-a"),
                ],
                tsink::DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("host", "edge-a"),
                    Label::new(tenant::TENANT_LABEL, "tenant-a"),
                ],
                tsink::DataPoint::new(2, 2.0),
            ),
        ]
    }

    fn category_bytes(snapshot: &tsink::LocalDiskBudgetSnapshot, category: DiskCategory) -> u64 {
        snapshot
            .categories
            .iter()
            .find(|usage| usage.category == category)
            .map_or(0, |usage| usage.bytes)
    }

    #[test]
    fn static_tenant_mapping_rewrites_reserved_label() {
        let row = override_row_tenant(&test_rows()[0], "central");
        assert!(row
            .labels()
            .iter()
            .any(|label| label.name == tenant::TENANT_LABEL && label.value == "central"));
        assert!(!row
            .labels()
            .iter()
            .any(|label| label.name == tenant::TENANT_LABEL && label.value == "tenant-a"));
    }

    #[test]
    fn queue_recovers_pending_entries_after_restart() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue = EdgeSyncQueue::open(path.clone(), EdgeSyncQueueConfig::default())
            .expect("queue should open");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("enqueue should succeed");
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.queued_entries, 1);
        drop(queue);

        let reopened =
            EdgeSyncQueue::open(path, EdgeSyncQueueConfig::default()).expect("queue should reopen");
        let snapshot = reopened.snapshot();
        assert_eq!(snapshot.queued_entries, 1);
        assert!(snapshot.queued_bytes > 0);
    }

    #[test]
    fn queue_open_rejects_blank_corrupt_and_unterminated_records() {
        let dir = tempdir();
        let parent = edge_sync_dir(dir.path());
        std::fs::create_dir_all(&parent).expect("edge sync directory should build");
        let mut unterminated =
            encode_log_record(&EdgeSyncLogRecord::Ack { id: 1 }).expect("Ack should encode");
        assert_eq!(unterminated.pop(), Some(b'\n'));
        let cases = [
            ("blank.log", b"\n".as_slice(), "blank record"),
            (
                "corrupt.log",
                b"{not-json}\n".as_slice(),
                "failed to decode",
            ),
            (
                "unterminated.log",
                unterminated.as_slice(),
                "incomplete record",
            ),
        ];

        for (name, bytes, expected) in cases {
            let path = parent.join(name);
            std::fs::write(&path, bytes).expect("invalid queue fixture should write");
            let err = EdgeSyncQueue::open(path, EdgeSyncQueueConfig::default())
                .expect_err("invalid queue log should fail closed");
            assert!(err.contains(expected), "unexpected error: {err}");
        }
    }

    #[test]
    fn observed_maximum_id_fences_future_enqueue_without_reuse() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        std::fs::create_dir_all(path.parent().expect("queue parent"))
            .expect("queue parent should build");
        std::fs::write(
            &path,
            encode_log_record(&EdgeSyncLogRecord::Ack { id: u64::MAX })
                .expect("maximum-id Ack should encode"),
        )
        .expect("queue fixture should write");

        let queue = EdgeSyncQueue::open(path, EdgeSyncQueueConfig::default())
            .expect("maximum-id history should remain readable");
        assert_eq!(
            queue
                .enqueue_rows("edge-a", &test_rows())
                .expect_err("maximum id must not be reused"),
            EdgeSyncEnqueueCause::IdExhausted
        );
        assert_eq!(queue.snapshot().queued_entries, 0);
    }

    #[test]
    fn budgeted_enqueue_rejects_quota_without_publishing_queue_state() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(1),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let queue = EdgeSyncQueue::open_with_disk_budget(
            path.clone(),
            EdgeSyncQueueConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("budgeted queue should open");

        let error = queue
            .enqueue_rows("edge-a", &test_rows())
            .expect_err("enqueue should exceed the shared disk quota");

        assert!(matches!(
            error,
            EdgeSyncEnqueueCause::DiskQuotaExceeded {
                limit: 1,
                used: 0,
                reserved: 0,
                requested,
            } if requested > 1
        ));
        assert_eq!(queue.snapshot().queued_entries, 0);
        assert_eq!(queue.snapshot().log_bytes, 0);
        assert_eq!(
            queue
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next_id,
            Some(1)
        );
        assert_eq!(std::fs::metadata(&path).expect("queue metadata").len(), 0);
        assert_eq!(budget.snapshot().reserved_bytes, 0);
        assert_eq!(
            category_bytes(&budget.snapshot(), DiskCategory::EdgeSync),
            0
        );

        drop(queue);
        let reopened = EdgeSyncQueue::open_with_disk_budget(
            path,
            EdgeSyncQueueConfig::default(),
            Some(budget),
        )
        .expect("budgeted queue should reopen");
        assert_eq!(reopened.snapshot().queued_entries, 0);
    }

    #[test]
    fn budgeted_enqueue_reconciles_edge_sync_bytes_across_restart() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(8 * 1024 * 1024),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let queue = EdgeSyncQueue::open_with_disk_budget(
            path.clone(),
            EdgeSyncQueueConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("budgeted queue should open");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("enqueue should succeed");
        let log_bytes = queue.snapshot().log_bytes;
        assert!(log_bytes > 0);
        assert_eq!(
            category_bytes(&budget.snapshot(), DiskCategory::EdgeSync),
            log_bytes
        );
        drop(queue);
        drop(budget);

        let reopened_budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(8 * 1024 * 1024),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should reconcile on restart");
        let reopened = EdgeSyncQueue::open_with_disk_budget(
            path,
            EdgeSyncQueueConfig::default(),
            Some(Arc::clone(&reopened_budget)),
        )
        .expect("budgeted queue should reopen");
        assert_eq!(reopened.snapshot().queued_entries, 1);
        assert_eq!(reopened.snapshot().log_bytes, log_bytes);
        assert_eq!(
            category_bytes(&reopened_budget.snapshot(), DiskCategory::EdgeSync),
            log_bytes
        );
    }

    #[test]
    fn budgeted_open_cleans_only_owned_atomic_and_legacy_temporaries() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let parent = path.parent().expect("queue parent");
        std::fs::create_dir_all(parent).expect("queue parent should build");
        let target_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("queue name should be UTF-8");
        let generated_temp = parent.join(format!(".{target_name}.tmp-123-0000000000000000"));
        let generated_lookalike = parent.join(format!(".{target_name}.tmp-123-000000000000000G"));
        let legacy_temp = path.with_extension("tmp");
        let legacy_lookalike = parent.join("queue.tmp.keep");
        std::fs::write(&generated_temp, b"generated").expect("generated temp should write");
        std::fs::write(&generated_lookalike, b"generated-lookalike")
            .expect("generated lookalike should write");
        std::fs::write(&legacy_temp, b"legacy").expect("legacy temp should write");
        std::fs::write(&legacy_lookalike, b"legacy-lookalike")
            .expect("legacy lookalike should write");
        let budget = LocalDiskBudget::open(dir.path(), LocalDiskLimits::default())
            .expect("budget should open");

        let _queue = EdgeSyncQueue::open_with_disk_budget(
            path.clone(),
            EdgeSyncQueueConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("queue should open");

        assert!(!generated_temp.exists());
        assert!(!legacy_temp.exists());
        assert!(generated_lookalike.exists());
        assert!(legacy_lookalike.exists());
        assert_eq!(std::fs::metadata(path).expect("queue metadata").len(), 0);
        let expected_bytes = std::fs::metadata(generated_lookalike)
            .expect("generated lookalike metadata")
            .len()
            .saturating_add(
                std::fs::metadata(legacy_lookalike)
                    .expect("legacy lookalike metadata")
                    .len(),
            );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, expected_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn compaction_failure_after_durable_put_is_cleanup_debt_not_enqueue_failure() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let config = EdgeSyncQueueConfig {
            max_log_bytes: 1,
            ..EdgeSyncQueueConfig::default()
        };
        let queue = EdgeSyncQueue::open(path.clone(), config).expect("queue should open");
        let compaction_failure = inject_compaction_failure(&path);

        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("durable Put should remain successful when compaction is deferred");
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.queued_entries, 1);
        assert!(snapshot.cleanup_pending);
        assert!(snapshot
            .last_cleanup_error
            .as_deref()
            .is_some_and(|error| error.contains("injected")));
        drop(compaction_failure);
        queue
            .expire_before(0)
            .expect("cleanup worker should retry deferred compaction");
        let snapshot = queue.snapshot();
        assert!(!snapshot.cleanup_pending);
        assert!(snapshot.last_cleanup_error.is_none());
        drop(queue);

        let reopened = EdgeSyncQueue::open(path, config).expect("queue should reopen");
        assert_eq!(reopened.snapshot().queued_entries, 1);
    }

    #[test]
    fn source_status_exposes_cleanup_debt_and_persistence_fencing() {
        let cleanup_dir = tempdir();
        let cleanup_path = edge_sync_dir(cleanup_dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let cleanup_runtime = EdgeSyncSourceRuntime::open_with_config(
            cleanup_dir.path(),
            EdgeSyncSourceBootstrap {
                source_id: "edge-a".to_string(),
                upstream_endpoint: "127.0.0.1:1".to_string(),
                shared_auth_token: "secret".to_string(),
                tenant_mapping: EdgeSyncTenantMapping::preserve(),
            },
            EdgeSyncQueueConfig {
                max_log_bytes: 1,
                ..EdgeSyncQueueConfig::default()
            },
        )
        .expect("cleanup runtime should open");
        let cleanup_failure = inject_compaction_failure(&cleanup_path);
        cleanup_runtime
            .enqueue_rows_typed(&test_rows())
            .expect("durable enqueue should survive deferred compaction");
        let status = cleanup_runtime.status_snapshot();
        assert!(status.cleanup_pending);
        assert!(status.degraded);
        assert!(!status.persistence_fenced);
        assert!(status
            .last_cleanup_error
            .as_deref()
            .is_some_and(|error| error.contains("injected")));
        drop(cleanup_failure);
        cleanup_runtime
            .cleanup_once()
            .expect("cleanup should retry deferred compaction");
        let status = cleanup_runtime.status_snapshot();
        assert!(!status.cleanup_pending);
        assert!(!status.degraded);
        assert!(status.last_cleanup_error.is_none());

        let fenced_dir = tempdir();
        let fenced_path = edge_sync_dir(fenced_dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let fenced_runtime = EdgeSyncSourceRuntime::open_with_config(
            fenced_dir.path(),
            EdgeSyncSourceBootstrap {
                source_id: "edge-b".to_string(),
                upstream_endpoint: "127.0.0.1:1".to_string(),
                shared_auth_token: "secret".to_string(),
                tenant_mapping: EdgeSyncTenantMapping::preserve(),
            },
            EdgeSyncQueueConfig::default(),
        )
        .expect("fenced runtime should open");
        let _append_failure = inject_append_failure(&fenced_path);
        fenced_runtime
            .enqueue_rows_typed(&test_rows())
            .expect_err("indeterminate append failure should reject and fence enqueue");
        let status = fenced_runtime.status_snapshot();
        assert!(status.persistence_fenced);
        assert!(status.degraded);
        assert!(!status.cleanup_pending);
        assert!(status
            .persistence_fence_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("injected")));
    }

    #[test]
    fn acknowledgement_uses_recovery_admission_at_growth_limit() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue = EdgeSyncQueue::open(path.clone(), EdgeSyncQueueConfig::default())
            .expect("queue should open");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("enqueue should succeed");
        let id = queue.collect_due_entries(1)[0].id;
        let put_bytes = queue.snapshot().log_bytes;
        drop(queue);

        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(put_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("full disk budget should reopen existing queue");
        let queue = EdgeSyncQueue::open_with_disk_budget(
            path.clone(),
            EdgeSyncQueueConfig::default(),
            Some(Arc::clone(&budget)),
        )
        .expect("budgeted queue should open at its growth limit");

        queue
            .ack_entry(id)
            .expect("acknowledgement cleanup should use recovery admission");
        assert_eq!(queue.snapshot().queued_entries, 0);
        assert_eq!(queue.snapshot().log_bytes, 0);
        assert_eq!(
            category_bytes(&budget.snapshot(), DiskCategory::EdgeSync),
            0
        );
        drop(queue);

        let reopened = EdgeSyncQueue::open_with_disk_budget(
            path,
            EdgeSyncQueueConfig::default(),
            Some(budget),
        )
        .expect("queue should reopen after recovered acknowledgement");
        assert_eq!(reopened.snapshot().queued_entries, 0);
    }

    #[test]
    fn ack_append_failure_keeps_entry_pending_in_memory_and_after_restart() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue = EdgeSyncQueue::open(path.clone(), EdgeSyncQueueConfig::default())
            .expect("queue should open");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("enqueue should succeed");
        let entry = queue
            .collect_due_entries(1)
            .into_iter()
            .next()
            .expect("entry should be pending");
        let before = queue.snapshot();
        let _append_failure = inject_append_failure(&path);

        let error = queue
            .ack_entry(entry.id)
            .expect_err("injected queue failure should reject the ack append");
        assert!(error.contains("injected edge sync queue append failure"));
        let after = queue.snapshot();
        assert_eq!(after.queued_entries, before.queued_entries);
        assert_eq!(after.queued_bytes, before.queued_bytes);
        assert_eq!(after.log_bytes, before.log_bytes);
        assert_eq!(
            after.oldest_enqueued_unix_ms,
            before.oldest_enqueued_unix_ms
        );
        assert!(after.persistence_fenced);
        assert!(after
            .persistence_fence_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("injected")));
        drop(queue);

        let reopened =
            EdgeSyncQueue::open(path, EdgeSyncQueueConfig::default()).expect("queue should reopen");
        assert_eq!(reopened.snapshot().queued_entries, 1);
        assert_eq!(reopened.collect_due_entries(1)[0].id, entry.id);
    }

    #[test]
    fn expiry_ack_append_failure_keeps_entry_pending_in_memory_and_after_restart() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue = EdgeSyncQueue::open(path.clone(), EdgeSyncQueueConfig::default())
            .expect("queue should open");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("enqueue should succeed");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("second enqueue should succeed");
        let before = queue.snapshot();
        assert_eq!(before.queued_entries, 2);
        let _append_failure = inject_append_failure(&path);

        let error = queue
            .expire_before(u64::MAX)
            .expect_err("injected queue failure should reject the expiry ack append");
        assert!(error.contains("injected edge sync queue append failure"));
        let after = queue.snapshot();
        assert_eq!(after.queued_entries, before.queued_entries);
        assert_eq!(after.queued_bytes, before.queued_bytes);
        assert_eq!(after.log_bytes, before.log_bytes);
        assert_eq!(
            after.oldest_enqueued_unix_ms,
            before.oldest_enqueued_unix_ms
        );
        assert!(after.persistence_fenced);
        drop(queue);

        let reopened =
            EdgeSyncQueue::open(path, EdgeSyncQueueConfig::default()).expect("queue should reopen");
        assert_eq!(reopened.snapshot().queued_entries, 2);
    }

    #[test]
    fn queue_expires_stale_entries() {
        let dir = tempdir();
        let path = edge_sync_dir(dir.path()).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue =
            EdgeSyncQueue::open(path, EdgeSyncQueueConfig::default()).expect("queue should open");
        queue
            .enqueue_rows("edge-a", &test_rows())
            .expect("enqueue should succeed");
        let removed = queue
            .expire_before(u64::MAX)
            .expect("cleanup should succeed");
        assert_eq!(removed.0, 1);
        assert!(removed.1 > 0);
        assert_eq!(queue.snapshot().queued_entries, 0);
    }

    async fn spawn_success_ingest_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener.local_addr().expect("local addr").to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request_buffer = Vec::new();
            let _ = read_http_request(&mut stream, &mut request_buffer)
                .await
                .expect("request should parse");
            let response = serde_json::to_vec(&InternalIngestRowsResponse {
                inserted_rows: 2,
                write_result: Some(tsink::BatchWriteResult::from_outcomes(
                    Some(tsink::WriteAcknowledgement::Volatile),
                    (0..2).map(tsink::RowWriteOutcome::accepted).collect(),
                )),
            })
            .expect("response encode");
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("headers");
            stream.write_all(&response).await.expect("body");
        });
        (endpoint, task)
    }

    async fn spawn_count_only_ingest_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener.local_addr().expect("local addr").to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request_buffer = Vec::new();
            let _ = read_http_request(&mut stream, &mut request_buffer)
                .await
                .expect("request should parse");
            let response = serde_json::to_vec(&InternalIngestRowsResponse {
                inserted_rows: 2,
                write_result: None,
            })
            .expect("response encode");
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("headers");
            stream.write_all(&response).await.expect("body");
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn source_runtime_replays_pending_entries() {
        let dir = tempdir();
        let (endpoint, server) = spawn_success_ingest_server().await;
        let runtime = EdgeSyncSourceRuntime::open(
            dir.path(),
            EdgeSyncSourceBootstrap {
                source_id: "edge-a".to_string(),
                upstream_endpoint: endpoint,
                shared_auth_token: "secret".to_string(),
                tenant_mapping: EdgeSyncTenantMapping::preserve(),
            },
        )
        .expect("runtime should open");
        runtime
            .enqueue_rows_typed(&test_rows())
            .expect("rows should enqueue");
        runtime
            .replay_due_once()
            .await
            .expect("replay should succeed");
        let snapshot = runtime.status_snapshot();
        assert_eq!(snapshot.queued_entries, 0);
        assert_eq!(snapshot.replay_success_total, 1);
        assert_eq!(
            snapshot.last_upstream_acknowledgement,
            Some(WriteAcknowledgement::Volatile)
        );
        server.abort();
    }

    #[tokio::test]
    async fn source_runtime_preserves_entry_without_canonical_result() {
        let dir = tempdir();
        let (endpoint, server) = spawn_count_only_ingest_server().await;
        let runtime = EdgeSyncSourceRuntime::open(
            dir.path(),
            EdgeSyncSourceBootstrap {
                source_id: "edge-a".to_string(),
                upstream_endpoint: endpoint,
                shared_auth_token: "secret".to_string(),
                tenant_mapping: EdgeSyncTenantMapping::preserve(),
            },
        )
        .expect("runtime should open");
        runtime
            .enqueue_rows_typed(&test_rows())
            .expect("rows should enqueue");

        runtime
            .replay_due_once()
            .await
            .expect("replay iteration should complete");

        let snapshot = runtime.status_snapshot();
        assert_eq!(snapshot.queued_entries, 1);
        assert_eq!(snapshot.replay_success_total, 0);
        assert_eq!(snapshot.replay_failures_total, 1);
        assert!(snapshot
            .last_replay_error
            .as_deref()
            .is_some_and(|message| message.contains("omitted its canonical write result")));
        server.await.expect("server should finish");
    }

    #[test]
    fn source_enqueue_reports_rows_persisted_before_chunk_failure() {
        let dir = tempdir();
        let runtime = EdgeSyncSourceRuntime::open_with_config(
            dir.path(),
            EdgeSyncSourceBootstrap {
                source_id: "edge-a".to_string(),
                upstream_endpoint: "127.0.0.1:1".to_string(),
                shared_auth_token: "secret".to_string(),
                tenant_mapping: EdgeSyncTenantMapping::preserve(),
            },
            EdgeSyncQueueConfig {
                max_entries: 1,
                ..EdgeSyncQueueConfig::default()
            },
        )
        .expect("runtime should open");
        let rows = (0..=MAX_INTERNAL_INGEST_ROWS)
            .map(|index| {
                Row::with_labels(
                    "edge_partial",
                    vec![Label::new(tenant::TENANT_LABEL, "tenant-a")],
                    tsink::DataPoint::new(index as i64, index as f64),
                )
            })
            .collect::<Vec<_>>();

        let error = runtime
            .enqueue_rows_typed(&rows)
            .expect_err("second queue chunk should exceed the entry limit");

        assert_eq!(error.submitted_rows, MAX_INTERNAL_INGEST_ROWS + 1);
        assert_eq!(error.queued_rows, MAX_INTERNAL_INGEST_ROWS);
        let snapshot = runtime.status_snapshot();
        assert_eq!(snapshot.queued_entries, 1);
        assert_eq!(snapshot.enqueued_total, 1);
        assert_eq!(snapshot.enqueue_rejected_total, 1);
    }
}
