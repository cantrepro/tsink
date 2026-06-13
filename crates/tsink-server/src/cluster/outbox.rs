use crate::cluster::dedupe::validate_idempotency_key;
use crate::cluster::rpc::{
    normalize_capabilities, InternalIngestRowsRequest, InternalRow, RpcClient,
    DEFAULT_INTERNAL_RING_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::Row;

pub const CLUSTER_OUTBOX_MAX_ENTRIES_ENV: &str = "TSINK_CLUSTER_OUTBOX_MAX_ENTRIES";
pub const CLUSTER_OUTBOX_MAX_BYTES_ENV: &str = "TSINK_CLUSTER_OUTBOX_MAX_BYTES";
pub const CLUSTER_OUTBOX_MAX_PEER_BYTES_ENV: &str = "TSINK_CLUSTER_OUTBOX_MAX_PEER_BYTES";
pub const CLUSTER_OUTBOX_MAX_LOG_BYTES_ENV: &str = "TSINK_CLUSTER_OUTBOX_MAX_LOG_BYTES";
pub const CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS_ENV: &str =
    "TSINK_CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS";
pub const CLUSTER_OUTBOX_REPLAY_BATCH_SIZE_ENV: &str = "TSINK_CLUSTER_OUTBOX_REPLAY_BATCH_SIZE";
pub const CLUSTER_OUTBOX_MAX_BACKOFF_SECS_ENV: &str = "TSINK_CLUSTER_OUTBOX_MAX_BACKOFF_SECS";
pub const CLUSTER_OUTBOX_MAX_RECORD_BYTES_ENV: &str = "TSINK_CLUSTER_OUTBOX_MAX_RECORD_BYTES";
pub const CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS_ENV: &str =
    "TSINK_CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS";
pub const CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS_ENV: &str =
    "TSINK_CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS";
pub const CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS_ENV: &str =
    "TSINK_CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS";
pub const CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES_ENV: &str =
    "TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES";
pub const CLUSTER_OUTBOX_STALLED_PEER_MIN_BYTES_ENV: &str =
    "TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_BYTES";

const DEFAULT_OUTBOX_MAX_ENTRIES: usize = 100_000;
const DEFAULT_OUTBOX_MAX_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_OUTBOX_MAX_PEER_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_OUTBOX_MAX_LOG_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_OUTBOX_REPLAY_INTERVAL_SECS: u64 = 2;
const DEFAULT_OUTBOX_REPLAY_BATCH_SIZE: usize = 256;
const DEFAULT_OUTBOX_MAX_BACKOFF_SECS: u64 = 30;
const DEFAULT_OUTBOX_MAX_RECORD_BYTES: u64 = 2 * 1024 * 1024;
const DEFAULT_OUTBOX_CLEANUP_INTERVAL_SECS: u64 = 30;
const DEFAULT_OUTBOX_CLEANUP_MIN_STALE_RECORDS: usize = 1024;
const DEFAULT_OUTBOX_STALLED_PEER_AGE_SECS: u64 = 300;
const DEFAULT_OUTBOX_STALLED_PEER_MIN_ENTRIES: u64 = 1;
const DEFAULT_OUTBOX_STALLED_PEER_MIN_BYTES: u64 = 1;

static CLUSTER_OUTBOX_ENQUEUED_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_ENQUEUE_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_REPLAY_ATTEMPTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_REPLAY_SUCCESS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_REPLAY_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_QUEUED_ENTRIES: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_QUEUED_BYTES: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_LOG_BYTES: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_STALE_RECORDS: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_CLEANUP_RUNS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_CLEANUP_COMPACTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_CLEANUP_RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_CLEANUP_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_STALLED_ALERTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_STALLED_PEERS: AtomicU64 = AtomicU64::new(0);
static CLUSTER_OUTBOX_STALLED_OLDEST_AGE_MS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxConfig {
    pub max_entries: usize,
    pub max_bytes: u64,
    pub max_peer_bytes: u64,
    pub max_log_bytes: u64,
    pub replay_interval_secs: u64,
    pub replay_batch_size: usize,
    pub max_backoff_secs: u64,
    pub max_record_bytes: u64,
    pub cleanup_interval_secs: u64,
    pub cleanup_min_stale_records: usize,
    pub stalled_peer_age_secs: u64,
    pub stalled_peer_min_entries: u64,
    pub stalled_peer_min_bytes: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_OUTBOX_MAX_ENTRIES,
            max_bytes: DEFAULT_OUTBOX_MAX_BYTES,
            max_peer_bytes: DEFAULT_OUTBOX_MAX_PEER_BYTES,
            max_log_bytes: DEFAULT_OUTBOX_MAX_LOG_BYTES,
            replay_interval_secs: DEFAULT_OUTBOX_REPLAY_INTERVAL_SECS,
            replay_batch_size: DEFAULT_OUTBOX_REPLAY_BATCH_SIZE,
            max_backoff_secs: DEFAULT_OUTBOX_MAX_BACKOFF_SECS,
            max_record_bytes: DEFAULT_OUTBOX_MAX_RECORD_BYTES,
            cleanup_interval_secs: DEFAULT_OUTBOX_CLEANUP_INTERVAL_SECS,
            cleanup_min_stale_records: DEFAULT_OUTBOX_CLEANUP_MIN_STALE_RECORDS,
            stalled_peer_age_secs: DEFAULT_OUTBOX_STALLED_PEER_AGE_SECS,
            stalled_peer_min_entries: DEFAULT_OUTBOX_STALLED_PEER_MIN_ENTRIES,
            stalled_peer_min_bytes: DEFAULT_OUTBOX_STALLED_PEER_MIN_BYTES,
        }
    }
}

impl OutboxConfig {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            max_entries: parse_env_u64(
                CLUSTER_OUTBOX_MAX_ENTRIES_ENV,
                defaults.max_entries as u64,
                true,
            )? as usize,
            max_bytes: parse_env_u64(CLUSTER_OUTBOX_MAX_BYTES_ENV, defaults.max_bytes, true)?,
            max_peer_bytes: parse_env_u64(
                CLUSTER_OUTBOX_MAX_PEER_BYTES_ENV,
                defaults.max_peer_bytes,
                true,
            )?,
            max_log_bytes: parse_env_u64(
                CLUSTER_OUTBOX_MAX_LOG_BYTES_ENV,
                defaults.max_log_bytes,
                true,
            )?,
            replay_interval_secs: parse_env_u64(
                CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS_ENV,
                defaults.replay_interval_secs,
                true,
            )?,
            replay_batch_size: parse_env_u64(
                CLUSTER_OUTBOX_REPLAY_BATCH_SIZE_ENV,
                defaults.replay_batch_size as u64,
                true,
            )? as usize,
            max_backoff_secs: parse_env_u64(
                CLUSTER_OUTBOX_MAX_BACKOFF_SECS_ENV,
                defaults.max_backoff_secs,
                true,
            )?,
            max_record_bytes: parse_env_u64(
                CLUSTER_OUTBOX_MAX_RECORD_BYTES_ENV,
                defaults.max_record_bytes,
                true,
            )?,
            cleanup_interval_secs: parse_env_u64(
                CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS_ENV,
                defaults.cleanup_interval_secs,
                true,
            )?,
            cleanup_min_stale_records: parse_env_u64(
                CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS_ENV,
                defaults.cleanup_min_stale_records as u64,
                true,
            )? as usize,
            stalled_peer_age_secs: parse_env_u64(
                CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS_ENV,
                defaults.stalled_peer_age_secs,
                true,
            )?,
            stalled_peer_min_entries: parse_env_u64(
                CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES_ENV,
                defaults.stalled_peer_min_entries,
                true,
            )?,
            stalled_peer_min_bytes: parse_env_u64(
                CLUSTER_OUTBOX_STALLED_PEER_MIN_BYTES_ENV,
                defaults.stalled_peer_min_bytes,
                true,
            )?,
        })
    }

    pub fn replay_interval(self) -> Duration {
        Duration::from_secs(self.replay_interval_secs.max(1))
    }

    pub fn cleanup_interval(self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxMetricsSnapshot {
    pub enqueued_total: u64,
    pub enqueue_rejected_total: u64,
    pub persistence_failures_total: u64,
    pub replay_attempts_total: u64,
    pub replay_success_total: u64,
    pub replay_failures_total: u64,
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub log_bytes: u64,
    pub stale_records: u64,
    pub cleanup_runs_total: u64,
    pub cleanup_compactions_total: u64,
    pub cleanup_reclaimed_bytes_total: u64,
    pub cleanup_failures_total: u64,
    pub stalled_alerts_total: u64,
    pub stalled_peers: u64,
    pub stalled_oldest_age_ms: u64,
}

pub fn outbox_metrics_snapshot() -> OutboxMetricsSnapshot {
    OutboxMetricsSnapshot {
        enqueued_total: CLUSTER_OUTBOX_ENQUEUED_TOTAL.load(Ordering::Relaxed),
        enqueue_rejected_total: CLUSTER_OUTBOX_ENQUEUE_REJECTED_TOTAL.load(Ordering::Relaxed),
        persistence_failures_total: CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL
            .load(Ordering::Relaxed),
        replay_attempts_total: CLUSTER_OUTBOX_REPLAY_ATTEMPTS_TOTAL.load(Ordering::Relaxed),
        replay_success_total: CLUSTER_OUTBOX_REPLAY_SUCCESS_TOTAL.load(Ordering::Relaxed),
        replay_failures_total: CLUSTER_OUTBOX_REPLAY_FAILURES_TOTAL.load(Ordering::Relaxed),
        queued_entries: CLUSTER_OUTBOX_QUEUED_ENTRIES.load(Ordering::Relaxed),
        queued_bytes: CLUSTER_OUTBOX_QUEUED_BYTES.load(Ordering::Relaxed),
        log_bytes: CLUSTER_OUTBOX_LOG_BYTES.load(Ordering::Relaxed),
        stale_records: CLUSTER_OUTBOX_STALE_RECORDS.load(Ordering::Relaxed),
        cleanup_runs_total: CLUSTER_OUTBOX_CLEANUP_RUNS_TOTAL.load(Ordering::Relaxed),
        cleanup_compactions_total: CLUSTER_OUTBOX_CLEANUP_COMPACTIONS_TOTAL.load(Ordering::Relaxed),
        cleanup_reclaimed_bytes_total: CLUSTER_OUTBOX_CLEANUP_RECLAIMED_BYTES_TOTAL
            .load(Ordering::Relaxed),
        cleanup_failures_total: CLUSTER_OUTBOX_CLEANUP_FAILURES_TOTAL.load(Ordering::Relaxed),
        stalled_alerts_total: CLUSTER_OUTBOX_STALLED_ALERTS_TOTAL.load(Ordering::Relaxed),
        stalled_peers: CLUSTER_OUTBOX_STALLED_PEERS.load(Ordering::Relaxed),
        stalled_oldest_age_ms: CLUSTER_OUTBOX_STALLED_OLDEST_AGE_MS.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxPeerBacklogSnapshot {
    pub node_id: String,
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub oldest_enqueued_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub struct OutboxBacklogSnapshot {
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub log_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxStalledPeerSnapshot {
    pub node_id: String,
    pub queued_entries: u64,
    pub queued_bytes: u64,
    pub oldest_enqueued_unix_ms: u64,
    pub oldest_age_ms: u64,
    pub first_stalled_unix_ms: u64,
}

#[derive(Debug, Clone)]
pub struct HintedHandoffOutbox {
    path: PathBuf,
    config: OutboxConfig,
    state: Arc<Mutex<OutboxState>>,
}

#[derive(Debug)]
struct OutboxState {
    pending: BTreeMap<u64, OutboxEntry>,
    peer_queued_bytes: BTreeMap<String, u64>,
    file: File,
    queued_bytes: u64,
    log_bytes: u64,
    log_records: u64,
    next_id: u64,
    active_stalled_peers: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxEntry {
    id: u64,
    peer_node_id: String,
    endpoint: String,
    idempotency_key: String,
    #[serde(default = "default_internal_ring_version")]
    ring_version: u64,
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
enum OutboxLogRecord {
    Put { entry: OutboxEntry },
    Ack { id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxEnqueueError {
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
    PeerBytesLimit {
        node_id: String,
        max_peer_bytes: u64,
        peer_queued_bytes: u64,
        record_bytes: u64,
    },
    Persistence {
        message: String,
    },
}

impl OutboxEnqueueError {
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Persistence { .. })
    }
}

impl std::fmt::Display for OutboxEnqueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIdempotencyKey { message } => {
                write!(f, "invalid idempotency key for outbox enqueue: {message}")
            }
            Self::RecordTooLarge { bytes, max_bytes } => {
                write!(
                    f,
                    "outbox record exceeds max size: {bytes} bytes > {max_bytes} bytes"
                )
            }
            Self::TotalEntriesLimit { max_entries } => {
                write!(f, "outbox queue entry limit reached: max {max_entries}")
            }
            Self::TotalBytesLimit {
                max_bytes,
                queued_bytes,
                record_bytes,
            } => {
                write!(
                    f,
                    "outbox queue byte limit reached: queued {queued_bytes} + record {record_bytes} > max {max_bytes}"
                )
            }
            Self::PeerBytesLimit {
                node_id,
                max_peer_bytes,
                peer_queued_bytes,
                record_bytes,
            } => {
                write!(
                    f,
                    "outbox peer byte limit reached for node '{node_id}': queued {peer_queued_bytes} + record {record_bytes} > max {max_peer_bytes}"
                )
            }
            Self::Persistence { message } => {
                write!(f, "outbox persistence failure: {message}")
            }
        }
    }
}

impl std::error::Error for OutboxEnqueueError {}

impl HintedHandoffOutbox {
    pub fn open(path: PathBuf, config: OutboxConfig) -> Result<Self, String> {
        if config.max_entries == 0 {
            return Err("cluster outbox max entries must be greater than zero".to_string());
        }
        if config.max_bytes == 0 {
            return Err("cluster outbox max bytes must be greater than zero".to_string());
        }
        if config.max_peer_bytes == 0 {
            return Err("cluster outbox max peer bytes must be greater than zero".to_string());
        }
        if config.max_log_bytes == 0 {
            return Err("cluster outbox max log bytes must be greater than zero".to_string());
        }
        if config.replay_batch_size == 0 {
            return Err("cluster outbox replay batch size must be greater than zero".to_string());
        }
        if config.max_record_bytes == 0 {
            return Err("cluster outbox max record bytes must be greater than zero".to_string());
        }
        if config.cleanup_interval_secs == 0 {
            return Err("cluster outbox cleanup interval must be greater than zero".to_string());
        }
        if config.cleanup_min_stale_records == 0 {
            return Err(
                "cluster outbox cleanup stale-record threshold must be greater than zero"
                    .to_string(),
            );
        }
        if config.stalled_peer_age_secs == 0 {
            return Err(
                "cluster outbox stalled peer age threshold must be greater than zero".to_string(),
            );
        }
        if config.stalled_peer_min_entries == 0 {
            return Err(
                "cluster outbox stalled peer minimum entries must be greater than zero".to_string(),
            );
        }
        if config.stalled_peer_min_bytes == 0 {
            return Err(
                "cluster outbox stalled peer minimum bytes must be greater than zero".to_string(),
            );
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create cluster outbox directory {}: {err}",
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
                    "failed to open cluster outbox log {}: {err}",
                    path.display()
                )
            })?;

        let mut queued_bytes = 0u64;
        let mut peer_queued_bytes = BTreeMap::new();
        for entry in pending.values_mut() {
            if entry.queue_bytes == 0 {
                entry.queue_bytes = estimate_queue_bytes(entry);
            }
            queued_bytes = queued_bytes.saturating_add(entry.queue_bytes);
            let peer = peer_queued_bytes
                .entry(entry.peer_node_id.clone())
                .or_insert(0u64);
            *peer = peer.saturating_add(entry.queue_bytes);
        }

        let log_bytes = file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();

        let outbox = Self {
            path,
            config,
            state: Arc::new(Mutex::new(OutboxState {
                pending,
                peer_queued_bytes,
                file,
                queued_bytes,
                log_bytes,
                log_records,
                next_id,
                active_stalled_peers: BTreeMap::new(),
            })),
        };

        {
            let mut state = outbox
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.log_bytes > outbox.config.max_log_bytes {
                if let Err(err) = compact_locked(&outbox.path, &mut state) {
                    CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    eprintln!("cluster outbox compaction failed during open: {err}");
                }
            }
            refresh_observability_locked(&mut state, &outbox.config, unix_timestamp_millis());
        }

        Ok(outbox)
    }

    pub fn outbox_path(&self) -> &Path {
        &self.path
    }

    pub fn config(&self) -> OutboxConfig {
        self.config
    }

    #[allow(dead_code)]
    pub fn enqueue_replica_write(
        &self,
        peer_node_id: &str,
        endpoint: &str,
        idempotency_key: &str,
        rows: &[Row],
    ) -> Result<(), OutboxEnqueueError> {
        self.enqueue_replica_write_with_ring_version(
            peer_node_id,
            endpoint,
            idempotency_key,
            DEFAULT_INTERNAL_RING_VERSION,
            rows,
        )
    }

    pub fn enqueue_replica_write_with_ring_version(
        &self,
        peer_node_id: &str,
        endpoint: &str,
        idempotency_key: &str,
        ring_version: u64,
        rows: &[Row],
    ) -> Result<(), OutboxEnqueueError> {
        self.enqueue_replica_write_with_capabilities(
            peer_node_id,
            endpoint,
            idempotency_key,
            ring_version,
            &[],
            rows,
        )
    }

    pub fn enqueue_replica_write_with_capabilities(
        &self,
        peer_node_id: &str,
        endpoint: &str,
        idempotency_key: &str,
        ring_version: u64,
        required_capabilities: &[String],
        rows: &[Row],
    ) -> Result<(), OutboxEnqueueError> {
        validate_idempotency_key(idempotency_key)
            .map_err(|message| OutboxEnqueueError::InvalidIdempotencyKey { message })?;

        let now = unix_timestamp_millis();
        let mut entry = OutboxEntry {
            id: 0,
            peer_node_id: peer_node_id.to_string(),
            endpoint: endpoint.to_string(),
            idempotency_key: idempotency_key.to_string(),
            ring_version: ring_version.max(1),
            required_capabilities: normalize_capabilities(required_capabilities.iter().cloned()),
            rows: rows.iter().map(InternalRow::from).collect(),
            queue_bytes: 0,
            enqueued_unix_ms: now,
            next_attempt_unix_ms: now,
            attempts: 0,
        };
        entry.queue_bytes = estimate_queue_bytes(&entry);

        if entry.queue_bytes > self.config.max_record_bytes {
            CLUSTER_OUTBOX_ENQUEUE_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(OutboxEnqueueError::RecordTooLarge {
                bytes: entry.queue_bytes,
                max_bytes: self.config.max_record_bytes,
            });
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if state.pending.len() >= self.config.max_entries {
            CLUSTER_OUTBOX_ENQUEUE_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(OutboxEnqueueError::TotalEntriesLimit {
                max_entries: self.config.max_entries,
            });
        }

        if state.queued_bytes.saturating_add(entry.queue_bytes) > self.config.max_bytes {
            CLUSTER_OUTBOX_ENQUEUE_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(OutboxEnqueueError::TotalBytesLimit {
                max_bytes: self.config.max_bytes,
                queued_bytes: state.queued_bytes,
                record_bytes: entry.queue_bytes,
            });
        }

        let peer_queued_bytes = state
            .peer_queued_bytes
            .get(peer_node_id)
            .copied()
            .unwrap_or_default();
        if peer_queued_bytes.saturating_add(entry.queue_bytes) > self.config.max_peer_bytes {
            CLUSTER_OUTBOX_ENQUEUE_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(OutboxEnqueueError::PeerBytesLimit {
                node_id: peer_node_id.to_string(),
                max_peer_bytes: self.config.max_peer_bytes,
                peer_queued_bytes,
                record_bytes: entry.queue_bytes,
            });
        }

        entry.id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);

        if let Err(err) = append_log_record_locked(
            &mut state,
            &OutboxLogRecord::Put {
                entry: entry.clone(),
            },
        ) {
            CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(OutboxEnqueueError::Persistence { message: err });
        }

        state.queued_bytes = state.queued_bytes.saturating_add(entry.queue_bytes);
        let peer = state
            .peer_queued_bytes
            .entry(entry.peer_node_id.clone())
            .or_insert(0u64);
        *peer = peer.saturating_add(entry.queue_bytes);
        state.pending.insert(entry.id, entry);

        if state.log_bytes > self.config.max_log_bytes {
            if let Err(err) = compact_locked(&self.path, &mut state) {
                CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                eprintln!("cluster outbox compaction failed after enqueue: {err}");
            }
        }

        CLUSTER_OUTBOX_ENQUEUED_TOTAL.fetch_add(1, Ordering::Relaxed);
        refresh_observability_locked(&mut state, &self.config, now);

        Ok(())
    }

    pub async fn replay_due_once(&self, rpc_client: &RpcClient) -> Result<(), String> {
        let due = self.take_due_entries();
        for entry in due {
            CLUSTER_OUTBOX_REPLAY_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);

            let request = InternalIngestRowsRequest {
                ring_version: entry.ring_version.max(1),
                idempotency_key: Some(entry.idempotency_key.clone()),
                required_capabilities: entry.required_capabilities.clone(),
                rows: entry.rows.clone(),
            };

            match rpc_client.ingest_rows(&entry.endpoint, &request).await {
                Ok(response) if response.inserted_rows == request.rows.len() => {
                    match self.ack_entry(entry.id) {
                        Ok(()) => {
                            CLUSTER_OUTBOX_REPLAY_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            CLUSTER_OUTBOX_REPLAY_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                            CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL
                                .fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "cluster outbox ack persistence failed for entry {}: {err}",
                                entry.id
                            );
                        }
                    }
                }
                Ok(response) => {
                    CLUSTER_OUTBOX_REPLAY_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    let err = format!(
                        "replayed ingest inserted {} rows but expected {}",
                        response.inserted_rows,
                        request.rows.len()
                    );
                    if let Err(update_err) = self.reschedule_entry(entry, &err) {
                        CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                        eprintln!("cluster outbox replay reschedule failed: {update_err}");
                    }
                }
                Err(err) => {
                    CLUSTER_OUTBOX_REPLAY_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    if let Err(update_err) = self.reschedule_entry(entry, &err.to_string()) {
                        CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                        eprintln!("cluster outbox replay reschedule failed: {update_err}");
                    }
                }
            }
        }

        Ok(())
    }

    pub fn start_replay_worker(
        self: &Arc<Self>,
        rpc_client: RpcClient,
    ) -> tokio::task::JoinHandle<()> {
        let outbox = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(outbox.config.replay_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = outbox.replay_due_once(&rpc_client).await {
                    eprintln!("cluster outbox replay worker iteration failed: {err}");
                }
            }
        })
    }

    pub fn start_cleanup_worker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let outbox = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(outbox.config.cleanup_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(err) = outbox.cleanup_once() {
                    eprintln!("cluster outbox cleanup worker iteration failed: {err}");
                }
            }
        })
    }

    #[allow(dead_code)]
    pub fn backlog_snapshot(&self) -> OutboxBacklogSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        OutboxBacklogSnapshot {
            queued_entries: state.pending.len() as u64,
            queued_bytes: state.queued_bytes,
            log_bytes: state.log_bytes,
        }
    }

    pub fn peer_backlog_snapshot(&self) -> Vec<OutboxPeerBacklogSnapshot> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut oldest_by_peer: BTreeMap<String, u64> = BTreeMap::new();
        let mut entries_by_peer: BTreeMap<String, u64> = BTreeMap::new();
        for entry in state.pending.values() {
            let oldest = oldest_by_peer
                .entry(entry.peer_node_id.clone())
                .or_insert(entry.enqueued_unix_ms);
            *oldest = (*oldest).min(entry.enqueued_unix_ms);
            let count = entries_by_peer
                .entry(entry.peer_node_id.clone())
                .or_insert(0u64);
            *count = count.saturating_add(1);
        }

        let mut peers = Vec::with_capacity(state.peer_queued_bytes.len());
        for (node_id, queued_bytes) in &state.peer_queued_bytes {
            peers.push(OutboxPeerBacklogSnapshot {
                node_id: node_id.clone(),
                queued_entries: entries_by_peer.get(node_id).copied().unwrap_or_default(),
                queued_bytes: *queued_bytes,
                oldest_enqueued_unix_ms: oldest_by_peer.get(node_id).copied(),
            });
        }
        peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        peers
    }

    pub fn stalled_peer_snapshot(&self) -> Vec<OutboxStalledPeerSnapshot> {
        let now = unix_timestamp_millis();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        refresh_observability_locked(&mut state, &self.config, now)
    }

    fn cleanup_once(&self) -> Result<(), String> {
        CLUSTER_OUTBOX_CLEANUP_RUNS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let now = unix_timestamp_millis();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let stale_records = stale_record_count(&state);
        let should_compact = stale_records > 0
            && (stale_records >= self.config.cleanup_min_stale_records as u64
                || state.log_bytes > self.config.max_log_bytes);
        if should_compact {
            let previous_log_bytes = state.log_bytes;
            if let Err(err) = compact_locked(&self.path, &mut state) {
                CLUSTER_OUTBOX_CLEANUP_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                CLUSTER_OUTBOX_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                refresh_observability_locked(&mut state, &self.config, now);
                return Err(err);
            }
            CLUSTER_OUTBOX_CLEANUP_COMPACTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reclaimed = previous_log_bytes.saturating_sub(state.log_bytes);
            if reclaimed > 0 {
                CLUSTER_OUTBOX_CLEANUP_RECLAIMED_BYTES_TOTAL
                    .fetch_add(reclaimed, Ordering::Relaxed);
            }
        }

        refresh_observability_locked(&mut state, &self.config, now);
        Ok(())
    }

    fn take_due_entries(&self) -> Vec<OutboxEntry> {
        let now = unix_timestamp_millis();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut due = Vec::new();
        for entry in state.pending.values() {
            if entry.next_attempt_unix_ms > now {
                continue;
            }
            due.push(entry.clone());
            if due.len() >= self.config.replay_batch_size {
                break;
            }
        }
        due
    }

    fn ack_entry(&self, entry_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !state.pending.contains_key(&entry_id) {
            return Ok(());
        }

        append_log_record_locked(&mut state, &OutboxLogRecord::Ack { id: entry_id })?;
        let entry = state.pending.remove(&entry_id);
        if let Some(entry) = entry {
            state.queued_bytes = state.queued_bytes.saturating_sub(entry.queue_bytes);
            if let Some(peer_bytes) = state.peer_queued_bytes.get_mut(&entry.peer_node_id) {
                *peer_bytes = peer_bytes.saturating_sub(entry.queue_bytes);
                if *peer_bytes == 0 {
                    state.peer_queued_bytes.remove(&entry.peer_node_id);
                }
            }
        }

        if state.log_bytes > self.config.max_log_bytes {
            compact_locked(&self.path, &mut state)?;
        }

        refresh_observability_locked(&mut state, &self.config, unix_timestamp_millis());
        Ok(())
    }

    fn reschedule_entry(&self, mut entry: OutboxEntry, reason: &str) -> Result<(), String> {
        let now = unix_timestamp_millis();
        entry.attempts = entry.attempts.saturating_add(1);
        let exponent = entry.attempts.saturating_sub(1).min(30);
        let backoff_secs = 1u64
            .checked_shl(exponent)
            .unwrap_or(u64::MAX)
            .min(self.config.max_backoff_secs.max(1));
        entry.next_attempt_unix_ms = now.saturating_add(backoff_secs.saturating_mul(1_000));

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !state.pending.contains_key(&entry.id) {
            return Ok(());
        }

        append_log_record_locked(
            &mut state,
            &OutboxLogRecord::Put {
                entry: entry.clone(),
            },
        )?;
        state.pending.insert(entry.id, entry);

        if state.log_bytes > self.config.max_log_bytes {
            compact_locked(&self.path, &mut state)?;
        }

        refresh_observability_locked(&mut state, &self.config, now);
        eprintln!("cluster outbox replay deferred: {reason}");
        Ok(())
    }
}

fn load_existing_records(
    path: &Path,
    pending: &mut BTreeMap<u64, OutboxEntry>,
    next_id: &mut u64,
    log_records: &mut u64,
) -> Result<(), String> {
    let file = File::open(path).map_err(|err| {
        format!(
            "failed to open cluster outbox log {}: {err}",
            path.display()
        )
    })?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line.map_err(|err| {
            format!(
                "failed to read cluster outbox log {}: {err}",
                path.display()
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }

        let record: OutboxLogRecord = serde_json::from_str(&line).map_err(|err| {
            format!(
                "failed to decode cluster outbox log {}: {err}",
                path.display()
            )
        })?;
        *log_records = log_records.saturating_add(1);

        match record {
            OutboxLogRecord::Put { mut entry } => {
                if entry.queue_bytes == 0 {
                    entry.queue_bytes = estimate_queue_bytes(&entry);
                }
                *next_id = (*next_id).max(entry.id.saturating_add(1));
                pending.insert(entry.id, entry);
            }
            OutboxLogRecord::Ack { id } => {
                pending.remove(&id);
                *next_id = (*next_id).max(id.saturating_add(1));
            }
        }
    }

    Ok(())
}

fn append_log_record_locked(
    state: &mut OutboxState,
    record: &OutboxLogRecord,
) -> Result<(), String> {
    let mut encoded = serde_json::to_vec(record)
        .map_err(|err| format!("failed to encode outbox log record: {err}"))?;
    encoded.push(b'\n');

    state
        .file
        .write_all(&encoded)
        .map_err(|err| format!("failed to append outbox log record: {err}"))?;
    state
        .file
        .flush()
        .map_err(|err| format!("failed to flush outbox log record: {err}"))?;
    state
        .file
        .sync_data()
        .map_err(|err| format!("failed to sync outbox log record: {err}"))?;

    state.log_bytes = state.log_bytes.saturating_add(encoded.len() as u64);
    state.log_records = state.log_records.saturating_add(1);
    Ok(())
}

fn compact_locked(path: &Path, state: &mut OutboxState) -> Result<(), String> {
    let temp_path = path.with_extension("compact.tmp");
    let mut temp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(|err| {
            format!(
                "failed to open cluster outbox compaction file {}: {err}",
                temp_path.display()
            )
        })?;

    for entry in state.pending.values() {
        let mut encoded = serde_json::to_vec(&OutboxLogRecord::Put {
            entry: entry.clone(),
        })
        .map_err(|err| format!("failed to encode outbox entry during compaction: {err}"))?;
        encoded.push(b'\n');
        temp_file
            .write_all(&encoded)
            .map_err(|err| format!("failed to write outbox compaction record: {err}"))?;
    }

    temp_file
        .flush()
        .map_err(|err| format!("failed to flush outbox compaction file: {err}"))?;
    temp_file
        .sync_data()
        .map_err(|err| format!("failed to sync outbox compaction file: {err}"))?;
    drop(temp_file);

    std::fs::rename(&temp_path, path).map_err(|err| {
        format!(
            "failed to replace outbox log {} from {}: {err}",
            path.display(),
            temp_path.display()
        )
    })?;

    let reopened = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .map_err(|err| {
            format!(
                "failed to reopen compacted outbox log {}: {err}",
                path.display()
            )
        })?;

    state.log_bytes = reopened
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    state.log_records = state.pending.len() as u64;
    state.file = reopened;

    Ok(())
}

fn estimate_queue_bytes(entry: &OutboxEntry) -> u64 {
    serde_json::to_vec(entry)
        .map(|encoded| encoded.len() as u64)
        .unwrap_or(0)
}

fn update_outbox_gauges(state: &OutboxState) {
    CLUSTER_OUTBOX_QUEUED_ENTRIES.store(state.pending.len() as u64, Ordering::Relaxed);
    CLUSTER_OUTBOX_QUEUED_BYTES.store(state.queued_bytes, Ordering::Relaxed);
    CLUSTER_OUTBOX_LOG_BYTES.store(state.log_bytes, Ordering::Relaxed);
    CLUSTER_OUTBOX_STALE_RECORDS.store(stale_record_count(state), Ordering::Relaxed);
}

fn stale_record_count(state: &OutboxState) -> u64 {
    state.log_records.saturating_sub(state.pending.len() as u64)
}

fn refresh_observability_locked(
    state: &mut OutboxState,
    config: &OutboxConfig,
    now_unix_ms: u64,
) -> Vec<OutboxStalledPeerSnapshot> {
    update_outbox_gauges(state);
    let stalled_peers = collect_stalled_peers_locked(state, config, now_unix_ms);
    let mut next_active = BTreeMap::new();
    let mut max_age_ms = 0u64;
    let mut snapshots = Vec::with_capacity(stalled_peers.len());
    for stalled in stalled_peers {
        let first_stalled_unix_ms = state
            .active_stalled_peers
            .get(&stalled.node_id)
            .copied()
            .unwrap_or_else(|| {
                CLUSTER_OUTBOX_STALLED_ALERTS_TOTAL.fetch_add(1, Ordering::Relaxed);
                now_unix_ms
            });
        max_age_ms = max_age_ms.max(stalled.oldest_age_ms);
        next_active.insert(stalled.node_id.clone(), first_stalled_unix_ms);
        snapshots.push(OutboxStalledPeerSnapshot {
            node_id: stalled.node_id,
            queued_entries: stalled.queued_entries,
            queued_bytes: stalled.queued_bytes,
            oldest_enqueued_unix_ms: stalled.oldest_enqueued_unix_ms,
            oldest_age_ms: stalled.oldest_age_ms,
            first_stalled_unix_ms,
        });
    }
    state.active_stalled_peers = next_active;
    CLUSTER_OUTBOX_STALLED_PEERS.store(snapshots.len() as u64, Ordering::Relaxed);
    CLUSTER_OUTBOX_STALLED_OLDEST_AGE_MS.store(max_age_ms, Ordering::Relaxed);
    snapshots
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StalledPeerCandidate {
    node_id: String,
    queued_entries: u64,
    queued_bytes: u64,
    oldest_enqueued_unix_ms: u64,
    oldest_age_ms: u64,
}

fn collect_stalled_peers_locked(
    state: &OutboxState,
    config: &OutboxConfig,
    now_unix_ms: u64,
) -> Vec<StalledPeerCandidate> {
    let mut oldest_by_peer: BTreeMap<String, u64> = BTreeMap::new();
    let mut entries_by_peer: BTreeMap<String, u64> = BTreeMap::new();
    for entry in state.pending.values() {
        let oldest = oldest_by_peer
            .entry(entry.peer_node_id.clone())
            .or_insert(entry.enqueued_unix_ms);
        *oldest = (*oldest).min(entry.enqueued_unix_ms);
        let entries = entries_by_peer
            .entry(entry.peer_node_id.clone())
            .or_insert(0u64);
        *entries = entries.saturating_add(1);
    }

    let age_threshold_ms = config.stalled_peer_age_secs.saturating_mul(1_000);
    let mut stalled = Vec::new();
    for (node_id, queued_bytes) in &state.peer_queued_bytes {
        let queued_entries = entries_by_peer.get(node_id).copied().unwrap_or_default();
        if queued_entries == 0 {
            continue;
        }
        if queued_entries < config.stalled_peer_min_entries
            || *queued_bytes < config.stalled_peer_min_bytes
        {
            continue;
        }
        let Some(oldest_enqueued_unix_ms) = oldest_by_peer.get(node_id).copied() else {
            continue;
        };
        let oldest_age_ms = now_unix_ms.saturating_sub(oldest_enqueued_unix_ms);
        if oldest_age_ms < age_threshold_ms {
            continue;
        }

        stalled.push(StalledPeerCandidate {
            node_id: node_id.clone(),
            queued_entries,
            queued_bytes: *queued_bytes,
            oldest_enqueued_unix_ms,
            oldest_age_ms,
        });
    }
    stalled.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    stalled
}

fn default_internal_ring_version() -> u64 {
    DEFAULT_INTERNAL_RING_VERSION
}

fn parse_env_u64(name: &str, default: u64, allow_zero: bool) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(format!("{name} must not be empty when set"));
            }
            let parsed = trimmed
                .parse::<u64>()
                .map_err(|_| format!("{name} must be an integer, got '{raw}'"))?;
            if !allow_zero && parsed == 0 {
                return Err(format!("{name} must be greater than zero when set"));
            }
            if allow_zero && parsed == 0 && default > 0 {
                return Err(format!("{name} must be greater than zero when set"));
            }
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("{name} must be valid UTF-8 when set"))
        }
    }
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
    use crate::http::{read_http_request, write_http_response, HttpResponse};
    use crate::{cluster::rpc::InternalIngestRowsResponse, cluster::rpc::RpcClientConfig};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tsink::{DataPoint, Label, Row};

    fn sample_rows() -> Vec<Row> {
        vec![
            Row::with_labels(
                "outbox_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(1_700_000_000_000, 1.0),
            ),
            Row::with_labels(
                "outbox_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(1_700_000_000_100, 2.0),
            ),
        ]
    }

    fn open_outbox(path: PathBuf) -> HintedHandoffOutbox {
        HintedHandoffOutbox::open(
            path,
            OutboxConfig {
                max_entries: 8,
                max_bytes: 8 * 1024 * 1024,
                max_peer_bytes: 8 * 1024 * 1024,
                max_log_bytes: 8 * 1024 * 1024,
                replay_interval_secs: 1,
                replay_batch_size: 8,
                max_backoff_secs: 1,
                max_record_bytes: 1024 * 1024,
                cleanup_interval_secs: 1,
                cleanup_min_stale_records: 1,
                stalled_peer_age_secs: 1,
                stalled_peer_min_entries: 1,
                stalled_peer_min_bytes: 1,
            },
        )
        .expect("outbox should open")
    }

    async fn spawn_success_ingest_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener.local_addr().expect("local address").to_string();

        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should decode");
            assert_eq!(request.path_without_query(), "/internal/v1/ingest_rows");

            let ingest_request: InternalIngestRowsRequest =
                serde_json::from_slice(&request.body).expect("request body should decode");
            assert_eq!(
                ingest_request.idempotency_key.as_deref(),
                Some("tsink:test:outbox:1")
            );
            let response = HttpResponse::new(
                200,
                serde_json::to_vec(&InternalIngestRowsResponse {
                    inserted_rows: ingest_request.rows.len(),
                })
                .expect("response should encode"),
            )
            .with_header("Content-Type", "application/json");
            write_http_response(&mut stream, &response)
                .await
                .expect("response should write");
        });

        (endpoint, task)
    }

    #[test]
    fn enqueue_persists_across_restart() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("outbox.log");
        {
            let outbox = open_outbox(path.clone());
            outbox
                .enqueue_replica_write(
                    "node-b",
                    "127.0.0.1:9302",
                    "tsink:test:outbox:1",
                    &sample_rows(),
                )
                .expect("enqueue should succeed");
            let backlog = outbox.backlog_snapshot();
            assert_eq!(backlog.queued_entries, 1);
            assert!(backlog.queued_bytes > 0);
        }

        let reopened = open_outbox(path);
        let backlog = reopened.backlog_snapshot();
        assert_eq!(backlog.queued_entries, 1);
        assert!(backlog.queued_bytes > 0);
        let peers = reopened.peer_backlog_snapshot();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "node-b");
        assert_eq!(peers[0].queued_entries, 1);
    }

    #[test]
    fn enqueue_rejects_when_entry_limit_is_reached() {
        let temp = TempDir::new().expect("temp dir");
        let outbox = HintedHandoffOutbox::open(
            temp.path().join("outbox.log"),
            OutboxConfig {
                max_entries: 1,
                max_bytes: 8 * 1024 * 1024,
                max_peer_bytes: 8 * 1024 * 1024,
                max_log_bytes: 8 * 1024 * 1024,
                replay_interval_secs: 1,
                replay_batch_size: 8,
                max_backoff_secs: 1,
                max_record_bytes: 1024 * 1024,
                cleanup_interval_secs: 1,
                cleanup_min_stale_records: 1,
                stalled_peer_age_secs: 1,
                stalled_peer_min_entries: 1,
                stalled_peer_min_bytes: 1,
            },
        )
        .expect("outbox should open");

        outbox
            .enqueue_replica_write(
                "node-b",
                "127.0.0.1:9302",
                "tsink:test:outbox:1",
                &sample_rows(),
            )
            .expect("first enqueue should succeed");

        let err = outbox
            .enqueue_replica_write(
                "node-b",
                "127.0.0.1:9302",
                "tsink:test:outbox:2",
                &sample_rows(),
            )
            .expect_err("second enqueue should fail");

        assert!(matches!(err, OutboxEnqueueError::TotalEntriesLimit { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replay_due_once_drains_successful_entries() {
        let temp = TempDir::new().expect("temp dir");
        let outbox = open_outbox(temp.path().join("outbox.log"));
        let (endpoint, server) = spawn_success_ingest_server().await;

        outbox
            .enqueue_replica_write("node-b", &endpoint, "tsink:test:outbox:1", &sample_rows())
            .expect("enqueue should succeed");
        assert_eq!(outbox.backlog_snapshot().queued_entries, 1);

        let rpc_client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(250),
            max_retries: 0,
            internal_auth_token: "cluster-test-token".to_string(),
            ..RpcClientConfig::default()
        });

        outbox
            .replay_due_once(&rpc_client)
            .await
            .expect("replay should run");
        assert_eq!(outbox.backlog_snapshot().queued_entries, 0);

        server.await.expect("server should finish without panic");
    }

    #[test]
    fn cleanup_compacts_stale_log_records_without_dropping_pending_entries() {
        let temp = TempDir::new().expect("temp dir");
        let outbox = open_outbox(temp.path().join("outbox.log"));
        outbox
            .enqueue_replica_write(
                "node-b",
                "127.0.0.1:9302",
                "tsink:test:outbox:cleanup",
                &sample_rows(),
            )
            .expect("enqueue should succeed");

        let initial_entry = {
            let state = outbox
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .pending
                .values()
                .next()
                .cloned()
                .expect("pending entry should exist")
        };
        outbox
            .reschedule_entry(initial_entry, "test-reschedule")
            .expect("reschedule should succeed");

        let before_cleanup = outbox.backlog_snapshot();
        let metrics_before = outbox_metrics_snapshot();
        assert_eq!(before_cleanup.queued_entries, 1);
        assert!(metrics_before.stale_records > 0);

        outbox.cleanup_once().expect("cleanup should succeed");

        let after_cleanup = outbox.backlog_snapshot();
        let metrics_after = outbox_metrics_snapshot();
        assert_eq!(after_cleanup.queued_entries, 1);
        assert!(after_cleanup.log_bytes <= before_cleanup.log_bytes);
        assert_eq!(metrics_after.stale_records, 0);
        assert!(metrics_after.cleanup_runs_total > metrics_before.cleanup_runs_total);
        assert!(
            metrics_after.cleanup_compactions_total >= metrics_before.cleanup_compactions_total
        );
    }

    #[test]
    fn stalled_peer_snapshot_reports_stalled_backlog_with_context() {
        let temp = TempDir::new().expect("temp dir");
        let outbox = open_outbox(temp.path().join("outbox.log"));
        outbox
            .enqueue_replica_write(
                "node-b",
                "127.0.0.1:9302",
                "tsink:test:outbox:stalled",
                &sample_rows(),
            )
            .expect("enqueue should succeed");

        let stale_age_ms = outbox.config().stalled_peer_age_secs.saturating_mul(1_000) + 1_000;
        {
            let mut state = outbox
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for entry in state.pending.values_mut() {
                entry.enqueued_unix_ms = entry.enqueued_unix_ms.saturating_sub(stale_age_ms);
            }
        }

        let stalled = outbox.stalled_peer_snapshot();
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].node_id, "node-b");
        assert_eq!(stalled[0].queued_entries, 1);
        assert!(stalled[0].oldest_age_ms >= stale_age_ms);
        assert!(stalled[0].first_stalled_unix_ms > 0);

        let metrics = outbox_metrics_snapshot();
        assert!(metrics.stalled_peers >= 1);
        assert!(metrics.stalled_oldest_age_ms >= stale_age_ms);
    }
}
