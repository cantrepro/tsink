use crate::cluster::dedupe::{
    dedupe_metrics_snapshot, validate_idempotency_key, DedupeConfig, DedupeWindowStore,
};
use crate::cluster::rpc::{
    normalize_capabilities, required_capabilities_for_rows, CompatibilityProfile,
    InternalApiConfig, InternalIngestRowsRequest, InternalRow, RpcClient, RpcClientConfig,
    INTERNAL_RPC_PROTOCOL_VERSION, MAX_INTERNAL_INGEST_ROWS,
};
use crate::tenant;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::{Label, Row};

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
    replayed_rows_total: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct EdgeSyncRuntimeContext {
    pub source: Option<Arc<EdgeSyncSourceRuntime>>,
    pub accept_dedupe_store: Option<Arc<DedupeWindowStore>>,
    pub accept_dedupe_config: Option<DedupeConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeSyncQueueSnapshot {
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub log_bytes: u64,
    pub oldest_enqueued_unix_ms: Option<u64>,
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
    state: Mutex<EdgeSyncQueueState>,
}

#[derive(Debug)]
struct EdgeSyncQueueState {
    pending: BTreeMap<u64, EdgeSyncEntry>,
    file: File,
    queued_bytes: u64,
    log_bytes: u64,
    log_records: u64,
    next_id: u64,
}

impl EdgeSyncSourceRuntime {
    pub fn open(base_data_path: &Path, bootstrap: EdgeSyncSourceBootstrap) -> Result<Self, String> {
        let config = EdgeSyncQueueConfig::from_env()?;
        config.validate()?;

        let queue_path = edge_sync_dir(base_data_path).join(EDGE_SYNC_QUEUE_FILE_NAME);
        let queue = Arc::new(EdgeSyncQueue::open(queue_path, config)?);
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

    pub fn enqueue_rows(&self, rows: &[Row]) {
        if rows.is_empty() {
            return;
        }

        match self.prepare_rows(rows) {
            Ok(mapped_rows) => {
                for chunk in mapped_rows.chunks(MAX_INTERNAL_INGEST_ROWS) {
                    if let Err(err) = self.queue.enqueue_rows(&self.source_id, chunk) {
                        self.enqueue_rejected_total.fetch_add(1, Ordering::Relaxed);
                        self.set_last_enqueue_error(Some(err));
                        return;
                    }
                    self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                    self.set_last_enqueue_error(None);
                }
            }
            Err(err) => {
                self.enqueue_rejected_total.fetch_add(1, Ordering::Relaxed);
                self.set_last_enqueue_error(Some(err));
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
            degraded: queue.queued_entries > 0
                && (last_enqueue_error.is_some() || last_replay_error.is_some()),
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
                Ok(response) if response.inserted_rows == request.rows.len() => {
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
                Ok(response) => {
                    self.replay_failures_total.fetch_add(1, Ordering::Relaxed);
                    let err = format!(
                        "edge sync replay inserted {} rows but expected {}",
                        response.inserted_rows,
                        request.rows.len()
                    );
                    self.queue
                        .reschedule_entry(entry.id, self.config.max_backoff_secs)?;
                    self.set_last_replay_error(Some(err));
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
    fn open(path: PathBuf, config: EdgeSyncQueueConfig) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create edge sync queue directory {}: {err}",
                    parent.display()
                )
            })?;
        }

        let mut pending = BTreeMap::new();
        let mut next_id = 1u64;
        let mut log_records = 0u64;
        if path.exists() {
            load_existing_records(&path, &mut pending, &mut next_id, &mut log_records)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|err| {
                format!(
                    "failed to open edge sync queue log {}: {err}",
                    path.display()
                )
            })?;
        let mut queued_bytes = 0u64;
        for entry in pending.values_mut() {
            if entry.queue_bytes == 0 {
                entry.queue_bytes = estimate_queue_bytes(entry);
            }
            queued_bytes = queued_bytes.saturating_add(entry.queue_bytes);
        }
        let log_bytes = file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let queue = Self {
            path,
            config,
            state: Mutex::new(EdgeSyncQueueState {
                pending,
                file,
                queued_bytes,
                log_bytes,
                log_records,
                next_id,
            }),
        };
        {
            let mut state = queue
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.log_bytes > queue.config.max_log_bytes {
                compact_locked(&queue.path, &mut state)?;
            }
        }
        Ok(queue)
    }

    fn enqueue_rows(&self, source_id: &str, rows: &[Row]) -> Result<(), String> {
        let idempotency_key = build_edge_sync_idempotency_key(source_id, unix_timestamp_millis());
        validate_idempotency_key(&idempotency_key)?;

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
            return Err(format!(
                "edge sync record exceeds max size: {} bytes > {} bytes",
                entry.queue_bytes, self.config.max_record_bytes
            ));
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.len() >= self.config.max_entries {
            return Err(format!(
                "edge sync queue entry limit reached: {}",
                self.config.max_entries
            ));
        }
        if state.queued_bytes.saturating_add(entry.queue_bytes) > self.config.max_bytes {
            return Err(format!(
                "edge sync queue byte limit reached: queued {} + record {} > {}",
                state.queued_bytes, entry.queue_bytes, self.config.max_bytes
            ));
        }

        entry.id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        append_log_record_locked(
            &mut state,
            &EdgeSyncLogRecord::Put {
                entry: entry.clone(),
            },
        )?;
        state.queued_bytes = state.queued_bytes.saturating_add(entry.queue_bytes);
        state.pending.insert(entry.id, entry);

        if state.log_bytes > self.config.max_log_bytes {
            compact_locked(&self.path, &mut state)?;
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
        let Some(entry) = state.pending.remove(&id) else {
            return Ok(());
        };
        append_log_record_locked(&mut state, &EdgeSyncLogRecord::Ack { id })?;
        state.queued_bytes = state.queued_bytes.saturating_sub(entry.queue_bytes);
        if state.log_bytes > self.config.max_log_bytes {
            compact_locked(&self.path, &mut state)?;
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
        let to_remove = state
            .pending
            .iter()
            .filter(|(_, entry)| entry.enqueued_unix_ms < cutoff_unix_ms)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let mut expired_entries = 0u64;
        let mut expired_bytes = 0u64;
        for id in to_remove {
            let Some(entry) = state.pending.remove(&id) else {
                continue;
            };
            append_log_record_locked(&mut state, &EdgeSyncLogRecord::Ack { id })?;
            state.queued_bytes = state.queued_bytes.saturating_sub(entry.queue_bytes);
            expired_entries = expired_entries.saturating_add(1);
            expired_bytes = expired_bytes.saturating_add(entry.queue_bytes);
        }
        if state.log_bytes > self.config.max_log_bytes {
            compact_locked(&self.path, &mut state)?;
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
) -> Result<Arc<DedupeWindowStore>, String> {
    let path = edge_sync_dir(base_data_path).join(EDGE_SYNC_DEDUPE_FILE_NAME);
    DedupeWindowStore::open(path, config).map(Arc::new)
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
    next_id: &mut u64,
    log_records: &mut u64,
) -> Result<(), String> {
    let file = File::open(path).map_err(|err| {
        format!(
            "failed to open edge sync queue log {}: {err}",
            path.display()
        )
    })?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.map_err(|err| {
            format!(
                "failed to read edge sync queue log {}: {err}",
                path.display()
            )
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record: EdgeSyncLogRecord = serde_json::from_str(trimmed).map_err(|err| {
            format!(
                "failed to decode edge sync queue record in {}: {err}",
                path.display()
            )
        })?;
        *log_records = log_records.saturating_add(1);
        match record {
            EdgeSyncLogRecord::Put { mut entry } => {
                if entry.queue_bytes == 0 {
                    entry.queue_bytes = estimate_queue_bytes(&entry);
                }
                *next_id = (*next_id).max(entry.id.saturating_add(1));
                pending.insert(entry.id, entry);
            }
            EdgeSyncLogRecord::Ack { id } => {
                pending.remove(&id);
            }
        }
    }
    Ok(())
}

fn append_log_record_locked(
    state: &mut EdgeSyncQueueState,
    record: &EdgeSyncLogRecord,
) -> Result<(), String> {
    let serialized = serde_json::to_vec(record)
        .map_err(|err| format!("failed to encode edge sync queue record: {err}"))?;
    state
        .file
        .write_all(&serialized)
        .and_then(|_| state.file.write_all(b"\n"))
        .and_then(|_| state.file.flush())
        .map_err(|err| format!("failed to append edge sync queue record: {err}"))?;
    state.log_bytes = state
        .log_bytes
        .saturating_add(u64::try_from(serialized.len() + 1).unwrap_or(u64::MAX));
    state.log_records = state.log_records.saturating_add(1);
    Ok(())
}

fn compact_locked(path: &Path, state: &mut EdgeSyncQueueState) -> Result<(), String> {
    let temp_path = path.with_extension("tmp");
    let mut temp = File::create(&temp_path).map_err(|err| {
        format!(
            "failed to create edge sync compaction temp file {}: {err}",
            temp_path.display()
        )
    })?;
    let mut log_bytes = 0u64;
    for entry in state.pending.values() {
        let record = EdgeSyncLogRecord::Put {
            entry: entry.clone(),
        };
        let encoded = serde_json::to_vec(&record)
            .map_err(|err| format!("failed to encode edge sync compaction record: {err}"))?;
        temp.write_all(&encoded)
            .and_then(|_| temp.write_all(b"\n"))
            .map_err(|err| {
                format!(
                    "failed to write edge sync compaction temp file {}: {err}",
                    temp_path.display()
                )
            })?;
        log_bytes = log_bytes.saturating_add(u64::try_from(encoded.len() + 1).unwrap_or(u64::MAX));
    }
    temp.flush().map_err(|err| {
        format!(
            "failed to flush edge sync compaction temp file {}: {err}",
            temp_path.display()
        )
    })?;
    std::fs::rename(&temp_path, path).map_err(|err| {
        format!(
            "failed to replace edge sync queue log {} with compacted file {}: {err}",
            path.display(),
            temp_path.display()
        )
    })?;
    state.file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .map_err(|err| format!("failed to reopen compacted edge sync queue log: {err}"))?;
    state.log_bytes = log_bytes;
    state.log_records = state.pending.len() as u64;
    Ok(())
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.expect("read");
            let response = serde_json::to_vec(&InternalIngestRowsResponse { inserted_rows: 2 })
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
        runtime.enqueue_rows(&test_rows());
        runtime
            .replay_due_once()
            .await
            .expect("replay should succeed");
        let snapshot = runtime.status_snapshot();
        assert_eq!(snapshot.queued_entries, 0);
        assert_eq!(snapshot.replay_success_total, 1);
        server.abort();
    }
}
