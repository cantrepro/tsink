use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::{DiskCategory, LocalDiskBudget, TsinkError};

pub const CLUSTER_DEDUPE_WINDOW_SECS_ENV: &str = "TSINK_CLUSTER_DEDUPE_WINDOW_SECS";
pub const CLUSTER_DEDUPE_MAX_ENTRIES_ENV: &str = "TSINK_CLUSTER_DEDUPE_MAX_ENTRIES";
pub const CLUSTER_DEDUPE_MAX_LOG_BYTES_ENV: &str = "TSINK_CLUSTER_DEDUPE_MAX_LOG_BYTES";
pub const CLUSTER_DEDUPE_CLEANUP_INTERVAL_SECS_ENV: &str =
    "TSINK_CLUSTER_DEDUPE_CLEANUP_INTERVAL_SECS";

const DEFAULT_DEDUPE_WINDOW_SECS: u64 = 15 * 60;
const DEFAULT_DEDUPE_MAX_ENTRIES: usize = 250_000;
const DEFAULT_DEDUPE_MAX_LOG_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_DEDUPE_CLEANUP_INTERVAL_SECS: u64 = 30;
const MAX_IDEMPOTENCY_KEY_LEN: usize = 160;
const MAX_PERSISTENCE_ERROR_DETAIL_BYTES: usize = 256;

static CLUSTER_DEDUPE_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_DUPLICATES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_INFLIGHT_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_COMMITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_ABORTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_CLEANUP_RUNS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_EXPIRED_KEYS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_EVICTED_KEYS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_ACTIVE_KEYS: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_INFLIGHT_KEYS: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DEDUPE_LOG_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupeConfig {
    pub window_secs: u64,
    pub max_entries: usize,
    pub max_log_bytes: u64,
    pub cleanup_interval_secs: u64,
}

impl Default for DedupeConfig {
    fn default() -> Self {
        Self {
            window_secs: DEFAULT_DEDUPE_WINDOW_SECS,
            max_entries: DEFAULT_DEDUPE_MAX_ENTRIES,
            max_log_bytes: DEFAULT_DEDUPE_MAX_LOG_BYTES,
            cleanup_interval_secs: DEFAULT_DEDUPE_CLEANUP_INTERVAL_SECS,
        }
    }
}

impl DedupeConfig {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            window_secs: parse_env_u64(CLUSTER_DEDUPE_WINDOW_SECS_ENV, defaults.window_secs, true)?,
            max_entries: parse_env_u64(
                CLUSTER_DEDUPE_MAX_ENTRIES_ENV,
                defaults.max_entries as u64,
                true,
            )? as usize,
            max_log_bytes: parse_env_u64(
                CLUSTER_DEDUPE_MAX_LOG_BYTES_ENV,
                defaults.max_log_bytes,
                true,
            )?,
            cleanup_interval_secs: parse_env_u64(
                CLUSTER_DEDUPE_CLEANUP_INTERVAL_SECS_ENV,
                defaults.cleanup_interval_secs,
                true,
            )?,
        })
    }

    pub fn cleanup_interval(self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs.max(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "response_type", rename_all = "snake_case")]
pub enum DedupeCompletion {
    IngestRows {
        inserted_rows: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        write_result: Option<tsink::BatchWriteResult>,
    },
    IngestWrite {
        inserted_rows: usize,
        accepted_metadata_updates: usize,
        accepted_exemplars: usize,
        dropped_exemplars: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        acknowledgement: Option<tsink::WriteAcknowledgement>,
    },
}

#[derive(Debug)]
pub enum DedupeBeginOutcome<'a> {
    Accepted(DedupeReservation<'a>),
    Duplicate {
        completion: Option<DedupeCompletion>,
    },
    InFlight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupePersistenceStage {
    Encode,
    Append,
    Flush,
    Sync,
    Compact,
}

impl DedupePersistenceStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Encode => "record encoding",
            Self::Append => "record append",
            Self::Flush => "log flush",
            Self::Sync => "log fsync",
            Self::Compact => "log compaction",
        }
    }
}

/// A bounded error identifying the durability stage that failed.
///
/// The underlying diagnostic is retained for local logs, but `Display` intentionally exposes only
/// a fixed-size message suitable for an internal RPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupePersistenceError {
    stage: DedupePersistenceStage,
    detail: String,
    resource_limit: Option<DedupeDiskResourceLimit>,
}

impl DedupePersistenceError {
    fn new(stage: DedupePersistenceStage, detail: impl std::fmt::Display) -> Self {
        Self {
            stage,
            detail: truncate_error_detail(&detail.to_string()),
            resource_limit: None,
        }
    }

    fn from_tsink(stage: DedupePersistenceStage, err: TsinkError) -> Self {
        let resource_limit = match &err {
            TsinkError::DiskQuotaExceeded {
                limit,
                used,
                reserved,
                requested,
            } => Some(DedupeDiskResourceLimit::DiskQuotaExceeded {
                limit: *limit,
                used: *used,
                reserved: *reserved,
                requested: *requested,
            }),
            TsinkError::InsufficientDiskSpace {
                required,
                available,
            } => Some(DedupeDiskResourceLimit::InsufficientDiskSpace {
                required: *required,
                available: *available,
            }),
            _ => None,
        };
        Self {
            stage,
            detail: truncate_error_detail(&err.to_string()),
            resource_limit,
        }
    }

    #[cfg(test)]
    pub(crate) fn stage(&self) -> DedupePersistenceStage {
        self.stage
    }

    pub fn resource_limit(&self) -> Option<DedupeDiskResourceLimit> {
        self.resource_limit
    }
}

impl std::fmt::Display for DedupePersistenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cluster dedupe completion marker persistence failed during {}",
            self.stage.as_str()
        )
    }
}

impl std::error::Error for DedupePersistenceError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupeDiskResourceLimit {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupeBeginError {
    InvalidIdempotencyKey { message: String },
    Persistence(DedupePersistenceError),
}

impl std::fmt::Display for DedupeBeginError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIdempotencyKey { message } => formatter.write_str(message),
            Self::Persistence(err) => err.fmt(formatter),
        }
    }
}

impl std::error::Error for DedupeBeginError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidIdempotencyKey { .. } => None,
            Self::Persistence(err) => Some(err),
        }
    }
}

/// An accepted dedupe reservation that is automatically aborted unless committed.
///
/// This keeps request handlers exception-safe across every early return after `begin`: dropping
/// the reservation releases the key so a retry cannot remain permanently stuck as in-flight.
#[derive(Debug)]
pub struct DedupeReservation<'a> {
    store: &'a DedupeWindowStore,
    key: String,
    active: bool,
}

impl DedupeReservation<'_> {
    pub fn commit(mut self, completion: DedupeCompletion) -> Result<(), DedupePersistenceError> {
        let result = self.store.commit_with_result(&self.key, completion);
        // Even on a persistence failure the completion remains in memory so an immediate retry
        // receives the exact result instead of applying the already-committed write again.
        self.active = false;
        result
    }
}

impl Drop for DedupeReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.store.abort(&self.key);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupeMetricsSnapshot {
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

pub fn dedupe_metrics_snapshot() -> DedupeMetricsSnapshot {
    DedupeMetricsSnapshot {
        requests_total: CLUSTER_DEDUPE_REQUESTS_TOTAL.load(Ordering::Relaxed),
        accepted_total: CLUSTER_DEDUPE_ACCEPTED_TOTAL.load(Ordering::Relaxed),
        duplicates_total: CLUSTER_DEDUPE_DUPLICATES_TOTAL.load(Ordering::Relaxed),
        inflight_rejections_total: CLUSTER_DEDUPE_INFLIGHT_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        commits_total: CLUSTER_DEDUPE_COMMITS_TOTAL.load(Ordering::Relaxed),
        aborts_total: CLUSTER_DEDUPE_ABORTS_TOTAL.load(Ordering::Relaxed),
        cleanup_runs_total: CLUSTER_DEDUPE_CLEANUP_RUNS_TOTAL.load(Ordering::Relaxed),
        expired_keys_total: CLUSTER_DEDUPE_EXPIRED_KEYS_TOTAL.load(Ordering::Relaxed),
        evicted_keys_total: CLUSTER_DEDUPE_EVICTED_KEYS_TOTAL.load(Ordering::Relaxed),
        persistence_failures_total: CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL
            .load(Ordering::Relaxed),
        active_keys: CLUSTER_DEDUPE_ACTIVE_KEYS.load(Ordering::Relaxed),
        inflight_keys: CLUSTER_DEDUPE_INFLIGHT_KEYS.load(Ordering::Relaxed),
        log_bytes: CLUSTER_DEDUPE_LOG_BYTES.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone)]
pub struct DedupeWindowStore {
    path: PathBuf,
    config: DedupeConfig,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    disk_category: DiskCategory,
    state: Arc<Mutex<DedupeState>>,
}

#[derive(Debug)]
struct DedupeState {
    entries: HashMap<String, DedupeEntry>,
    entries_by_expiry: BTreeMap<u64, BTreeSet<String>>,
    in_flight: HashSet<String>,
    log_bytes: u64,
    next_cleanup_unix_secs: u64,
    persistence_error: Option<DedupePersistenceError>,
    #[cfg(test)]
    append_fault: Option<DedupePersistenceStage>,
}

#[derive(Debug, Clone)]
struct DedupeEntry {
    expires_at_unix_secs: u64,
    completion: Option<DedupeCompletion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DedupeRecord {
    key: String,
    expires_at_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion: Option<DedupeCompletion>,
}

impl DedupeWindowStore {
    pub fn open(path: PathBuf, config: DedupeConfig) -> Result<Self, String> {
        Self::open_with_disk_budget(path, config, None, DiskCategory::Cluster)
    }

    pub fn open_with_disk_budget(
        path: PathBuf,
        config: DedupeConfig,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
        disk_category: DiskCategory,
    ) -> Result<Self, String> {
        if config.window_secs == 0 {
            return Err("cluster dedupe window must be greater than zero seconds".to_string());
        }
        if config.max_entries == 0 {
            return Err("cluster dedupe max entries must be greater than zero".to_string());
        }
        if config.max_log_bytes == 0 {
            return Err("cluster dedupe max log bytes must be greater than zero".to_string());
        }

        if let Some(parent) = path.parent() {
            if let Some(local_disk_budget) = local_disk_budget.as_ref() {
                local_disk_budget
                    .create_dir_all_and_sync_parents(parent)
                    .map_err(|err| {
                        format!(
                            "failed to create managed cluster dedupe directory {}: {err}",
                            parent.display()
                        )
                    })?;
                local_disk_budget
                    .cleanup_atomic_write_temps(&path)
                    .map_err(|err| {
                        format!(
                            "failed to clean managed cluster dedupe temporaries for {}: {err}",
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
                                "failed to clean legacy cluster dedupe compaction file {}: {err}",
                                legacy_compaction_temp.display()
                            )
                        })?;
                }
                local_disk_budget
                    .validate_managed_file_path(&path)
                    .map_err(|err| {
                        format!(
                            "invalid managed cluster dedupe path {}: {err}",
                            path.display()
                        )
                    })?;
            } else {
                std::fs::create_dir_all(parent).map_err(|err| {
                    format!(
                        "failed to create cluster dedupe directory {}: {err}",
                        parent.display()
                    )
                })?;
            }
        }

        if !path.exists() {
            if let Some(local_disk_budget) = local_disk_budget.as_ref() {
                local_disk_budget
                    .append_file_and_sync_parent(&path, &[], disk_category)
                    .map_err(|err| {
                        format!(
                            "failed to initialize managed cluster dedupe marker log {}: {err}",
                            path.display()
                        )
                    })?;
            } else {
                tsink::engine::fs_utils::write_file_atomically_and_sync_parent(&path, &[])
                    .map_err(|err| {
                        format!(
                            "failed to initialize cluster dedupe marker log {}: {err}",
                            path.display()
                        )
                    })?;
            }
        }

        let now = unix_timestamp_secs();
        let mut entries = HashMap::new();
        let mut entries_by_expiry = BTreeMap::new();
        load_existing_records(&path, now, &mut entries, &mut entries_by_expiry)?;

        let log_bytes = std::fs::metadata(&path)
            .map_err(|err| {
                format!(
                    "failed to inspect cluster dedupe marker log {}: {err}",
                    path.display()
                )
            })?
            .len();

        let state = DedupeState {
            entries,
            entries_by_expiry,
            in_flight: HashSet::new(),
            log_bytes,
            next_cleanup_unix_secs: now.saturating_add(config.cleanup_interval_secs),
            persistence_error: None,
            #[cfg(test)]
            append_fault: None,
        };

        let store = Self {
            path,
            config,
            local_disk_budget,
            disk_category,
            state: Arc::new(Mutex::new(state)),
        };
        store.run_cleanup_cycle(now, true);
        {
            let state = store
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            update_gauges(&state);
            if let Some(err) = state.persistence_error.clone() {
                return Err(format!("failed to initialize cluster dedupe store: {err}"));
            }
        }

        Ok(store)
    }

    pub fn begin(&self, key: &str) -> Result<DedupeBeginOutcome<'_>, DedupeBeginError> {
        validate_idempotency_key(key)
            .map_err(|message| DedupeBeginError::InvalidIdempotencyKey { message })?;
        CLUSTER_DEDUPE_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);

        let now = unix_timestamp_secs();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        self.maybe_cleanup_locked(&mut state, now, false);
        if let Some(entry) = state.entries.get(key).cloned() {
            if entry.expires_at_unix_secs > now {
                CLUSTER_DEDUPE_DUPLICATES_TOTAL.fetch_add(1, Ordering::Relaxed);
                update_gauges(&state);
                return Ok(DedupeBeginOutcome::Duplicate {
                    completion: entry.completion,
                });
            }
            remove_entry_locked(&mut state, key, entry.expires_at_unix_secs);
            CLUSTER_DEDUPE_EXPIRED_KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }

        // Exact results already held in memory remain replayable after a persistence failure, but
        // accepting a new key would create a result that cannot be made restart-safe.
        if let Some(err) = &state.persistence_error {
            update_gauges(&state);
            return Err(DedupeBeginError::Persistence(err.clone()));
        }

        if state.in_flight.contains(key) {
            CLUSTER_DEDUPE_INFLIGHT_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
            update_gauges(&state);
            return Ok(DedupeBeginOutcome::InFlight);
        }

        state.in_flight.insert(key.to_string());
        CLUSTER_DEDUPE_ACCEPTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        update_gauges(&state);
        drop(state);
        Ok(DedupeBeginOutcome::Accepted(DedupeReservation {
            store: self,
            key: key.to_string(),
            active: true,
        }))
    }

    fn commit_with_result(
        &self,
        key: &str,
        completion: DedupeCompletion,
    ) -> Result<(), DedupePersistenceError> {
        self.commit_locked(key, Some(completion))
    }

    fn commit_locked(
        &self,
        key: &str,
        completion: Option<DedupeCompletion>,
    ) -> Result<(), DedupePersistenceError> {
        let now = unix_timestamp_secs();
        let expires_at = now.saturating_add(self.config.window_secs);

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight.remove(key);

        // Make room before publishing the just-completed key so capacity eviction cannot
        // immediately discard the result required for a safe retry of this request.
        while state.entries.len() >= self.config.max_entries {
            if !evict_oldest_entry_locked(&mut state) {
                break;
            }
            CLUSTER_DEDUPE_EVICTED_KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        insert_entry_locked(&mut state, key.to_string(), expires_at, completion.clone());

        if let Some(err) = state.persistence_error.clone() {
            update_gauges(&state);
            return Err(err);
        }

        if let Err(err) = append_record_locked(
            &self.path,
            self.local_disk_budget.as_ref(),
            self.disk_category,
            &mut state,
            key,
            expires_at,
            completion,
        ) {
            CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "cluster dedupe append failed: {err}; detail: {}",
                err.detail
            );
            state.persistence_error = Some(err.clone());
            update_gauges(&state);
            return Err(err);
        }

        CLUSTER_DEDUPE_COMMITS_TOTAL.fetch_add(1, Ordering::Relaxed);
        self.maybe_cleanup_locked(&mut state, now, false);
        update_gauges(&state);
        if let Some(err) = state.persistence_error.clone() {
            return Err(err);
        }
        Ok(())
    }

    pub fn abort(&self, key: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.in_flight.remove(key) {
            CLUSTER_DEDUPE_ABORTS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        update_gauges(&state);
    }

    pub fn run_maintenance(&self) {
        self.run_cleanup_cycle(unix_timestamp_secs(), false);
    }

    pub fn start_cleanup_worker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(store.config.cleanup_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                store.run_maintenance();
            }
        })
    }

    pub fn marker_path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn fail_next_append_at(&self, stage: DedupePersistenceStage) {
        assert!(matches!(
            stage,
            DedupePersistenceStage::Append
                | DedupePersistenceStage::Flush
                | DedupePersistenceStage::Sync
        ));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.append_fault = Some(stage);
    }

    fn run_cleanup_cycle(&self, now: u64, force_compact: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.maybe_cleanup_locked(&mut state, now, force_compact);
        update_gauges(&state);
    }

    fn maybe_cleanup_locked(&self, state: &mut DedupeState, now: u64, force_compact: bool) {
        if !force_compact && state.persistence_error.is_none() && now < state.next_cleanup_unix_secs
        {
            return;
        }

        CLUSTER_DEDUPE_CLEANUP_RUNS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let expired = expire_entries_locked(state, now);
        if expired > 0 {
            CLUSTER_DEDUPE_EXPIRED_KEYS_TOTAL.fetch_add(expired as u64, Ordering::Relaxed);
        }
        while state.entries.len() > self.config.max_entries {
            if !evict_oldest_entry_locked(state) {
                break;
            }
            CLUSTER_DEDUPE_EVICTED_KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }

        if force_compact
            || state.persistence_error.is_some()
            || state.log_bytes > self.config.max_log_bytes
        {
            match compact_locked(
                &self.path,
                self.local_disk_budget.as_ref(),
                self.disk_category,
                state,
            ) {
                Ok(()) => state.persistence_error = None,
                Err(err) => {
                    CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "cluster dedupe compaction failed: {err}; detail: {}",
                        err.detail
                    );
                    state.persistence_error = Some(err);
                }
            }
        }

        state.next_cleanup_unix_secs = now.saturating_add(self.config.cleanup_interval_secs);
    }
}

pub fn validate_idempotency_key(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("idempotency key must not be empty".to_string());
    }
    if value.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(format!(
            "idempotency key exceeds max length {MAX_IDEMPOTENCY_KEY_LEN}"
        ));
    }
    if value.trim() != value {
        return Err("idempotency key must not contain leading or trailing whitespace".to_string());
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.'))
    {
        return Err(
            "idempotency key contains invalid characters; allowed: [A-Za-z0-9._:-]".to_string(),
        );
    }
    Ok(())
}

fn load_existing_records(
    path: &Path,
    now: u64,
    entries: &mut HashMap<String, DedupeEntry>,
    entries_by_expiry: &mut BTreeMap<u64, BTreeSet<String>>,
) -> Result<(), String> {
    let file = File::open(path).map_err(|err| {
        format!(
            "failed to open cluster dedupe marker log {}: {err}",
            path.display()
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut encoded_line = Vec::new();
    let mut line_number = 0usize;
    loop {
        encoded_line.clear();
        let bytes_read = reader.read_until(b'\n', &mut encoded_line).map_err(|err| {
            format!(
                "failed to read cluster dedupe marker log at line {}: {err}",
                line_number.saturating_add(1)
            )
        })?;
        if bytes_read == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        if encoded_line.last() != Some(&b'\n') {
            return Err(malformed_record_error(line_number, "incomplete record"));
        }
        encoded_line.pop();
        if encoded_line.iter().all(u8::is_ascii_whitespace) {
            return Err(malformed_record_error(line_number, "empty record"));
        }
        let parsed = serde_json::from_slice::<DedupeRecord>(&encoded_line)
            .map_err(|_| malformed_record_error(line_number, "invalid JSON record"))?;
        if parsed.expires_at_unix_secs <= now {
            continue;
        }
        if validate_idempotency_key(&parsed.key).is_err() {
            return Err(malformed_record_error(
                line_number,
                "invalid idempotency key",
            ));
        }
        insert_loaded_entry(
            entries,
            entries_by_expiry,
            parsed.key,
            parsed.expires_at_unix_secs,
            parsed.completion,
        );
    }
    Ok(())
}

fn malformed_record_error(line_number: usize, reason: &str) -> String {
    CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    format!("malformed cluster dedupe marker log record at line {line_number}: {reason}")
}

fn insert_loaded_entry(
    entries: &mut HashMap<String, DedupeEntry>,
    entries_by_expiry: &mut BTreeMap<u64, BTreeSet<String>>,
    key: String,
    expires_at: u64,
    completion: Option<DedupeCompletion>,
) {
    if let Some(previous) = entries.insert(
        key.clone(),
        DedupeEntry {
            expires_at_unix_secs: expires_at,
            completion,
        },
    ) {
        remove_key_from_expiry_index(entries_by_expiry, &key, previous.expires_at_unix_secs);
    }
    entries_by_expiry.entry(expires_at).or_default().insert(key);
}

fn insert_entry_locked(
    state: &mut DedupeState,
    key: String,
    expires_at: u64,
    completion: Option<DedupeCompletion>,
) {
    if let Some(previous) = state.entries.insert(
        key.clone(),
        DedupeEntry {
            expires_at_unix_secs: expires_at,
            completion,
        },
    ) {
        remove_key_from_expiry_index(
            &mut state.entries_by_expiry,
            &key,
            previous.expires_at_unix_secs,
        );
    }
    state
        .entries_by_expiry
        .entry(expires_at)
        .or_default()
        .insert(key);
}

fn remove_entry_locked(state: &mut DedupeState, key: &str, expires_at: u64) {
    state.entries.remove(key);
    remove_key_from_expiry_index(&mut state.entries_by_expiry, key, expires_at);
}

fn remove_key_from_expiry_index(
    entries_by_expiry: &mut BTreeMap<u64, BTreeSet<String>>,
    key: &str,
    expires_at: u64,
) {
    let mut should_remove_bucket = false;
    if let Some(keys) = entries_by_expiry.get_mut(&expires_at) {
        keys.remove(key);
        should_remove_bucket = keys.is_empty();
    }
    if should_remove_bucket {
        entries_by_expiry.remove(&expires_at);
    }
}

fn expire_entries_locked(state: &mut DedupeState, now: u64) -> usize {
    let expiries = state
        .entries_by_expiry
        .range(..=now)
        .map(|(expires_at, _)| *expires_at)
        .collect::<Vec<_>>();
    let mut removed = 0usize;
    for expires_at in expiries {
        if let Some(keys) = state.entries_by_expiry.remove(&expires_at) {
            for key in keys {
                if state.entries.remove(&key).is_some() {
                    removed += 1;
                }
            }
        }
    }
    removed
}

fn evict_oldest_entry_locked(state: &mut DedupeState) -> bool {
    let Some((&expires_at, keys)) = state.entries_by_expiry.iter_mut().next() else {
        return false;
    };
    let Some(key) = keys.iter().next().cloned() else {
        state.entries_by_expiry.remove(&expires_at);
        return false;
    };
    keys.remove(&key);
    if keys.is_empty() {
        state.entries_by_expiry.remove(&expires_at);
    }
    state.entries.remove(&key);
    true
}

fn append_record_locked(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    disk_category: DiskCategory,
    state: &mut DedupeState,
    key: &str,
    expires_at: u64,
    completion: Option<DedupeCompletion>,
) -> Result<(), DedupePersistenceError> {
    let record = DedupeRecord {
        key: key.to_string(),
        expires_at_unix_secs: expires_at,
        completion,
    };
    let mut encoded = serde_json::to_vec(&record)
        .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Encode, err))?;
    encoded.push(b'\n');

    #[cfg(test)]
    if state.append_fault == Some(DedupePersistenceStage::Append) {
        state.append_fault = None;
        return Err(DedupePersistenceError::new(
            DedupePersistenceStage::Append,
            "injected append failure",
        ));
    }

    if let Some(local_disk_budget) = local_disk_budget {
        local_disk_budget
            .append_file_and_sync_parent(path, &encoded, disk_category)
            .map_err(|err| {
                DedupePersistenceError::from_tsink(DedupePersistenceStage::Append, err)
            })?;
    } else {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Append, err))?;
        file.write_all(&encoded)
            .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Append, err))?;
        #[cfg(test)]
        if state.append_fault == Some(DedupePersistenceStage::Flush) {
            state.append_fault = None;
            return Err(DedupePersistenceError::new(
                DedupePersistenceStage::Flush,
                "injected flush failure",
            ));
        }
        file.flush()
            .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Flush, err))?;
        #[cfg(test)]
        if state.append_fault == Some(DedupePersistenceStage::Sync) {
            state.append_fault = None;
            return Err(DedupePersistenceError::new(
                DedupePersistenceStage::Sync,
                "injected fsync failure",
            ));
        }
        file.sync_data()
            .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Sync, err))?;
    }
    state.log_bytes = state.log_bytes.saturating_add(encoded.len() as u64);
    Ok(())
}

fn compact_locked(
    path: &Path,
    local_disk_budget: Option<&Arc<LocalDiskBudget>>,
    disk_category: DiskCategory,
    state: &mut DedupeState,
) -> Result<(), DedupePersistenceError> {
    let compacted_bytes = compacted_log_len(state)?;

    if let Some(local_disk_budget) = local_disk_budget {
        local_disk_budget
            .rewrite_file_atomically_and_sync_parent_for_cleanup_with(
                path,
                compacted_bytes,
                disk_category,
                |writer| write_compacted_log(state, writer).map_err(TsinkError::Other),
            )
            .map_err(|err| {
                DedupePersistenceError::from_tsink(DedupePersistenceStage::Compact, err)
            })?;
    } else {
        tsink::engine::fs_utils::write_file_atomically_and_sync_parent_with(
            path,
            compacted_bytes,
            |writer| write_compacted_log(state, writer).map_err(TsinkError::Other),
        )
        .map_err(|err| DedupePersistenceError::from_tsink(DedupePersistenceStage::Compact, err))?;
    }

    let actual_bytes = std::fs::metadata(path)
        .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Compact, err))?
        .len();
    if actual_bytes != compacted_bytes {
        return Err(DedupePersistenceError::new(
            DedupePersistenceStage::Compact,
            format!(
                "compacted dedupe marker log length mismatch: expected {compacted_bytes} bytes, found {actual_bytes} bytes"
            ),
        ));
    }
    state.log_bytes = actual_bytes;
    Ok(())
}

fn compacted_log_len(state: &DedupeState) -> Result<u64, DedupePersistenceError> {
    state
        .entries_by_expiry
        .iter()
        .try_fold(0u64, |total, (expires_at, keys)| {
            keys.iter().try_fold(total, |total, key| {
                let encoded = encode_compacted_record(state, key, *expires_at)?;
                let record_bytes = u64::try_from(encoded.len()).map_err(|_| {
                    DedupePersistenceError::new(
                        DedupePersistenceStage::Compact,
                        "encoded dedupe marker exceeds the supported byte range",
                    )
                })?;
                total.checked_add(record_bytes).ok_or_else(|| {
                    DedupePersistenceError::new(
                        DedupePersistenceStage::Compact,
                        "compacted dedupe marker log exceeds the supported byte range",
                    )
                })
            })
        })
}

fn write_compacted_log(state: &DedupeState, writer: &mut dyn Write) -> Result<(), String> {
    for (expires_at, keys) in &state.entries_by_expiry {
        for key in keys {
            let encoded =
                encode_compacted_record(state, key, *expires_at).map_err(|err| err.detail)?;
            writer
                .write_all(&encoded)
                .map_err(|err| format!("failed to write compacted dedupe marker log: {err}"))?;
        }
    }
    Ok(())
}

fn encode_compacted_record(
    state: &DedupeState,
    key: &str,
    expires_at: u64,
) -> Result<Vec<u8>, DedupePersistenceError> {
    let record = DedupeRecord {
        key: key.to_string(),
        expires_at_unix_secs: expires_at,
        completion: state
            .entries
            .get(key)
            .and_then(|entry| entry.completion.clone()),
    };
    let mut encoded = serde_json::to_vec(&record)
        .map_err(|err| DedupePersistenceError::new(DedupePersistenceStage::Compact, err))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn parse_env_u64(name: &str, default: u64, must_be_positive: bool) -> Result<u64, String> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{name} must be valid UTF-8 when set"))
        }
    };

    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer, got '{value}'"))?;
    if must_be_positive && parsed == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(parsed)
}

fn truncate_error_detail(detail: &str) -> String {
    if detail.len() <= MAX_PERSISTENCE_ERROR_DETAIL_BYTES {
        return detail.to_string();
    }
    let mut boundary = MAX_PERSISTENCE_ERROR_DETAIL_BYTES;
    while !detail.is_char_boundary(boundary) {
        boundary -= 1;
    }
    detail[..boundary].to_string()
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn update_gauges(state: &DedupeState) {
    CLUSTER_DEDUPE_ACTIVE_KEYS.store(state.entries.len() as u64, Ordering::Relaxed);
    CLUSTER_DEDUPE_INFLIGHT_KEYS.store(state.in_flight.len() as u64, Ordering::Relaxed);
    CLUSTER_DEDUPE_LOG_BYTES.store(state.log_bytes, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;
    use tsink::LocalDiskLimits;

    fn test_config() -> DedupeConfig {
        DedupeConfig {
            window_secs: 60,
            max_entries: 64,
            max_log_bytes: 64 * 1024,
            cleanup_interval_secs: 3_600,
        }
    }

    fn test_completion() -> DedupeCompletion {
        DedupeCompletion::IngestRows {
            inserted_rows: 1,
            write_result: None,
        }
    }

    fn category_bytes(snapshot: &tsink::LocalDiskBudgetSnapshot, category: DiskCategory) -> u64 {
        snapshot
            .categories
            .iter()
            .find(|usage| usage.category == category)
            .map_or(0, |usage| usage.bytes)
    }

    fn expect_accepted<'a>(store: &'a DedupeWindowStore, key: &str) -> DedupeReservation<'a> {
        match store.begin(key).expect("begin should succeed") {
            DedupeBeginOutcome::Accepted(reservation) => reservation,
            other => panic!("expected accepted reservation, got {other:?}"),
        }
    }

    #[test]
    fn validate_idempotency_key_enforces_format() {
        assert!(validate_idempotency_key("tsink:node-a:1234:abcd").is_ok());
        assert!(validate_idempotency_key("").is_err());
        assert!(validate_idempotency_key(" leading-space").is_err());
        assert!(validate_idempotency_key("bad/key").is_err());
    }

    #[test]
    fn legacy_ingest_rows_completion_remains_readable() {
        let completion: DedupeCompletion =
            serde_json::from_str(r#"{"response_type":"ingest_rows","inserted_rows":2}"#)
                .expect("legacy completion should decode");

        assert_eq!(
            completion,
            DedupeCompletion::IngestRows {
                inserted_rows: 2,
                write_result: None,
            }
        );
    }

    #[test]
    fn dedupe_store_marks_duplicates_and_aborts_inflight() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let store = DedupeWindowStore::open(
            dir.path().join("dedupe.log"),
            DedupeConfig {
                window_secs: 60,
                max_entries: 32,
                max_log_bytes: 8 * 1024,
                cleanup_interval_secs: 1,
            },
        )
        .expect("store should open");

        let first = expect_accepted(&store, "tsink:key:1");
        assert!(matches!(
            store.begin("tsink:key:1").expect("begin should succeed"),
            DedupeBeginOutcome::InFlight
        ));

        drop(first);
        let second = expect_accepted(&store, "tsink:key:1");
        let completion = DedupeCompletion::IngestWrite {
            inserted_rows: 3,
            accepted_metadata_updates: 2,
            accepted_exemplars: 1,
            dropped_exemplars: 4,
            acknowledgement: Some(tsink::WriteAcknowledgement::Durable),
        };
        second
            .commit(completion.clone())
            .expect("completion should persist");

        match store.begin("tsink:key:1").expect("begin should succeed") {
            DedupeBeginOutcome::Duplicate { completion: actual } => {
                assert_eq!(actual, Some(completion))
            }
            other => panic!("expected duplicate, got {other:?}"),
        };
    }

    #[test]
    fn dedupe_store_recovers_markers_after_restart() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("dedupe.log");
        let config = DedupeConfig {
            window_secs: 60,
            max_entries: 64,
            max_log_bytes: 64 * 1024,
            cleanup_interval_secs: 1,
        };

        let store = DedupeWindowStore::open(path.clone(), config).expect("store should open");
        let completion = DedupeCompletion::IngestRows {
            inserted_rows: 7,
            write_result: Some(tsink::BatchWriteResult::from_outcomes(
                Some(tsink::WriteAcknowledgement::Durable),
                (0..7).map(tsink::RowWriteOutcome::accepted).collect(),
            )),
        };
        expect_accepted(&store, "tsink:key:restart")
            .commit(completion.clone())
            .expect("completion should persist");
        drop(store);

        let reopened = DedupeWindowStore::open(path, config).expect("store should reopen");
        match reopened
            .begin("tsink:key:restart")
            .expect("begin should succeed")
        {
            DedupeBeginOutcome::Duplicate { completion: actual } => {
                assert_eq!(actual, Some(completion))
            }
            other => panic!("expected duplicate, got {other:?}"),
        };
    }

    #[test]
    fn append_failure_is_returned_and_same_key_remains_replayable() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let store = DedupeWindowStore::open(
            dir.path().join("dedupe.log"),
            DedupeConfig {
                window_secs: 60,
                max_entries: 32,
                max_log_bytes: 8 * 1024,
                cleanup_interval_secs: 30,
            },
        )
        .expect("store should open");
        let completion = DedupeCompletion::IngestRows {
            inserted_rows: 1,
            write_result: Some(tsink::BatchWriteResult::from_outcomes(
                Some(tsink::WriteAcknowledgement::Volatile),
                vec![tsink::RowWriteOutcome::accepted(0)],
            )),
        };

        store.fail_next_append_at(DedupePersistenceStage::Append);
        let err = expect_accepted(&store, "tsink:key:append-failure")
            .commit(completion.clone())
            .expect_err("injected append failure must be returned");
        assert_eq!(err.stage(), DedupePersistenceStage::Append);
        assert_eq!(
            err.to_string(),
            "cluster dedupe completion marker persistence failed during record append"
        );
        assert!(!err.to_string().contains("injected"));

        match store
            .begin("tsink:key:append-failure")
            .expect("the completed key should remain replayable")
        {
            DedupeBeginOutcome::Duplicate { completion: actual } => {
                assert_eq!(actual, Some(completion));
            }
            other => panic!("expected exact duplicate replay, got {other:?}"),
        }
        drop(expect_accepted(&store, "tsink:key:new-after-failure"));
    }

    #[test]
    fn fsync_failure_is_returned_with_a_typed_bounded_stage() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let store = DedupeWindowStore::open(
            dir.path().join("dedupe.log"),
            DedupeConfig {
                window_secs: 60,
                max_entries: 32,
                max_log_bytes: 8 * 1024,
                cleanup_interval_secs: 30,
            },
        )
        .expect("store should open");

        store.fail_next_append_at(DedupePersistenceStage::Sync);
        let err = expect_accepted(&store, "tsink:key:sync-failure")
            .commit(DedupeCompletion::IngestRows {
                inserted_rows: 0,
                write_result: None,
            })
            .expect_err("injected fsync failure must be returned");

        assert_eq!(err.stage(), DedupePersistenceStage::Sync);
        assert_eq!(
            err.to_string(),
            "cluster dedupe completion marker persistence failed during log fsync"
        );
        assert!(err.to_string().len() < 128);
    }

    #[test]
    fn malformed_marker_record_fails_open_with_line_number() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("dedupe.log");
        std::fs::write(
            &path,
            concat!(
                "{\"key\":\"tsink:key:valid\",\"expires_at_unix_secs\":4102444800}\n",
                "{not-json}\n"
            ),
        )
        .expect("marker fixture should be written");

        let err = DedupeWindowStore::open(path, DedupeConfig::default())
            .expect_err("malformed persisted records must fail open");
        assert_eq!(
            err,
            "malformed cluster dedupe marker log record at line 2: invalid JSON record"
        );
        assert!(!err.contains("not-json"));
    }

    #[test]
    fn unterminated_marker_record_is_rejected_as_incomplete() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("dedupe.log");
        std::fs::write(
            &path,
            b"{\"key\":\"tsink:key:torn\",\"expires_at_unix_secs\":4102444800}",
        )
        .expect("marker fixture should be written");

        let err = DedupeWindowStore::open(path, DedupeConfig::default())
            .expect_err("a torn final record must fail open");
        assert_eq!(
            err,
            "malformed cluster dedupe marker log record at line 1: incomplete record"
        );
    }

    #[test]
    fn legacy_marker_without_completion_remains_readable() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("dedupe.log");
        std::fs::write(
            &path,
            b"{\"key\":\"tsink:key:legacy\",\"expires_at_unix_secs\":4102444800}\n",
        )
        .expect("legacy marker should be written");
        let store = DedupeWindowStore::open(
            path,
            DedupeConfig {
                window_secs: 60,
                max_entries: 32,
                max_log_bytes: 8 * 1024,
                cleanup_interval_secs: 1,
            },
        )
        .expect("legacy marker store should open");

        match store
            .begin("tsink:key:legacy")
            .expect("legacy key lookup should succeed")
        {
            DedupeBeginOutcome::Duplicate { completion } => assert_eq!(completion, None),
            other => panic!("expected legacy marker to be a duplicate, got {other:?}"),
        };
    }

    #[test]
    fn physical_headroom_failure_preserves_typed_resource_detail() {
        let err = DedupePersistenceError::from_tsink(
            DedupePersistenceStage::Append,
            TsinkError::InsufficientDiskSpace {
                required: 4_096,
                available: 1_024,
            },
        );

        assert_eq!(err.stage(), DedupePersistenceStage::Append);
        assert_eq!(
            err.resource_limit(),
            Some(DedupeDiskResourceLimit::InsufficientDiskSpace {
                required: 4_096,
                available: 1_024,
            })
        );
    }

    #[test]
    fn budgeted_append_continues_after_nonempty_atomic_compaction() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("cluster/dedupe/node-a.markers.log");
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(8 * 1024 * 1024),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = DedupeWindowStore::open_with_disk_budget(
            path.clone(),
            test_config(),
            Some(Arc::clone(&budget)),
            DiskCategory::Cluster,
        )
        .expect("budgeted dedupe store should open");
        let completion = test_completion();
        expect_accepted(&store, "tsink:key:after-compaction:a")
            .commit(completion.clone())
            .expect("first marker should persist");

        store.run_cleanup_cycle(unix_timestamp_secs(), true);
        assert!(store
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .persistence_error
            .is_none());

        expect_accepted(&store, "tsink:key:after-compaction:b")
            .commit(completion.clone())
            .expect("append after atomic replacement should persist");
        let physical_bytes = std::fs::metadata(&path)
            .expect("marker metadata should load")
            .len();
        assert_eq!(budget.snapshot().accounted_bytes, physical_bytes);
        drop(store);

        let reopened = DedupeWindowStore::open_with_disk_budget(
            path,
            test_config(),
            Some(Arc::clone(&budget)),
            DiskCategory::Cluster,
        )
        .expect("dedupe store should reopen");
        for key in [
            "tsink:key:after-compaction:a",
            "tsink:key:after-compaction:b",
        ] {
            assert!(matches!(
                reopened.begin(key).expect("marker lookup should succeed"),
                DedupeBeginOutcome::Duplicate { .. }
            ));
        }
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, physical_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn budgeted_append_reports_typed_quota_and_preserves_exact_replay() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("cluster/dedupe/node-a.markers.log");
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(1),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = DedupeWindowStore::open_with_disk_budget(
            path.clone(),
            test_config(),
            Some(Arc::clone(&budget)),
            DiskCategory::Cluster,
        )
        .expect("budgeted dedupe store should open");
        let completion = test_completion();

        let err = expect_accepted(&store, "tsink:key:budget:failed")
            .commit(completion.clone())
            .expect_err("completion marker should exceed the disk quota");
        assert_eq!(err.stage(), DedupePersistenceStage::Append);
        assert!(matches!(
            err.resource_limit(),
            Some(DedupeDiskResourceLimit::DiskQuotaExceeded {
                limit: 1,
                used: 0,
                reserved: 0,
                requested,
            }) if requested > 1
        ));

        match store
            .begin("tsink:key:budget:failed")
            .expect("the failed key should remain exactly replayable")
        {
            DedupeBeginOutcome::Duplicate { completion: actual } => {
                assert_eq!(actual, Some(completion));
            }
            other => panic!("expected duplicate replay, got {other:?}"),
        }
        let new_key_err = store
            .begin("tsink:key:budget:new")
            .expect_err("a new key should retain the typed persistence fence");
        assert!(matches!(
            new_key_err,
            DedupeBeginError::Persistence(ref persistence)
                if persistence.stage() == DedupePersistenceStage::Compact
                    && persistence.resource_limit() == err.resource_limit()
        ));

        assert_eq!(std::fs::metadata(path).expect("marker metadata").len(), 0);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(category_bytes(&snapshot, DiskCategory::Cluster), 0);
    }

    #[test]
    fn budgeted_markers_have_exact_category_accounting_across_restart() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("edge_sync/dedupe/accepted.log");
        let limits = LocalDiskLimits {
            max_bytes: Some(8 * 1024 * 1024),
            ..LocalDiskLimits::default()
        };
        let completion = test_completion();

        let budget = LocalDiskBudget::open(dir.path(), limits).expect("disk budget should open");
        let store = DedupeWindowStore::open_with_disk_budget(
            path.clone(),
            test_config(),
            Some(Arc::clone(&budget)),
            DiskCategory::EdgeSync,
        )
        .expect("budgeted dedupe store should open");
        expect_accepted(&store, "tsink:edge:budget:restart")
            .commit(completion.clone())
            .expect("completion marker should persist");
        let physical_bytes = std::fs::metadata(&path)
            .expect("marker metadata should load")
            .len();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, physical_bytes);
        assert_eq!(
            category_bytes(&snapshot, DiskCategory::EdgeSync),
            physical_bytes
        );
        assert_eq!(snapshot.reserved_bytes, 0);
        drop(store);
        drop(budget);

        let restarted_budget =
            LocalDiskBudget::open(dir.path(), limits).expect("restarted budget should open");
        let restarted = DedupeWindowStore::open_with_disk_budget(
            path,
            test_config(),
            Some(Arc::clone(&restarted_budget)),
            DiskCategory::EdgeSync,
        )
        .expect("dedupe store should reopen");
        match restarted
            .begin("tsink:edge:budget:restart")
            .expect("persisted key lookup should succeed")
        {
            DedupeBeginOutcome::Duplicate { completion: actual } => {
                assert_eq!(actual, Some(completion));
            }
            other => panic!("expected duplicate after restart, got {other:?}"),
        }
        let restarted_snapshot = restarted_budget.snapshot();
        assert_eq!(restarted_snapshot.accounted_bytes, physical_bytes);
        assert_eq!(restarted_snapshot.reserved_bytes, 0);
        assert_eq!(restarted_snapshot.active_reservations, 0);
        assert_eq!(
            category_bytes(&restarted_snapshot, DiskCategory::EdgeSync),
            physical_bytes
        );
    }

    #[test]
    fn concurrent_budgeted_stores_cannot_share_the_final_record_bytes() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let key_a = "tsink:key:concurrent:a";
        let key_b = "tsink:key:concurrent:b";
        let expires_at = unix_timestamp_secs().saturating_add(test_config().window_secs);
        let encoded_a = serde_json::to_vec(&DedupeRecord {
            key: key_a.to_string(),
            expires_at_unix_secs: expires_at,
            completion: Some(test_completion()),
        })
        .expect("test marker should encode");
        let encoded_b = serde_json::to_vec(&DedupeRecord {
            key: key_b.to_string(),
            expires_at_unix_secs: expires_at,
            completion: Some(test_completion()),
        })
        .expect("test marker should encode");
        assert_eq!(encoded_a.len(), encoded_b.len());
        let record_bytes = encoded_a.len() as u64 + 1;
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(record_bytes),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store_a = Arc::new(
            DedupeWindowStore::open_with_disk_budget(
                dir.path().join("cluster/dedupe/a.log"),
                test_config(),
                Some(Arc::clone(&budget)),
                DiskCategory::Cluster,
            )
            .expect("first store should open"),
        );
        let store_b = Arc::new(
            DedupeWindowStore::open_with_disk_budget(
                dir.path().join("cluster/dedupe/b.log"),
                test_config(),
                Some(Arc::clone(&budget)),
                DiskCategory::Cluster,
            )
            .expect("second store should open"),
        );
        let barrier = Arc::new(Barrier::new(3));
        let handles = [(store_a, key_a), (store_b, key_b)]
            .into_iter()
            .map(|(store, key)| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    expect_accepted(&store, key).commit(test_completion())
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("commit thread should finish"))
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(err) if matches!(
                        err.resource_limit(),
                        Some(DedupeDiskResourceLimit::DiskQuotaExceeded { .. })
                    )
                ))
                .count(),
            1
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, record_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(
            category_bytes(&snapshot, DiskCategory::Cluster),
            record_bytes
        );
    }

    #[test]
    fn quota_full_maintenance_compacts_with_recovery_and_clears_the_fence() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("cluster/dedupe/node-a.markers.log");
        let mut config = test_config();
        config.max_entries = 1;
        let completion = test_completion();
        let first_key = "tsink:key:cleanup:a";
        let second_key = "tsink:key:cleanup:b";

        let initial = DedupeWindowStore::open(path.clone(), config)
            .expect("unbudgeted fixture store should open");
        expect_accepted(&initial, first_key)
            .commit(completion.clone())
            .expect("initial marker should persist");
        drop(initial);
        let record_bytes = std::fs::metadata(&path)
            .expect("initial marker metadata")
            .len();

        let limits = LocalDiskLimits {
            max_bytes: Some(record_bytes),
            ..LocalDiskLimits::default()
        };
        let budget = LocalDiskBudget::open(dir.path(), limits).expect("disk budget should open");
        let store = DedupeWindowStore::open_with_disk_budget(
            path.clone(),
            config,
            Some(Arc::clone(&budget)),
            DiskCategory::Cluster,
        )
        .expect("budgeted dedupe store should open at its quota");

        let err = expect_accepted(&store, second_key)
            .commit(completion.clone())
            .expect_err("growth append should fail at the exact quota");
        assert!(matches!(
            err.resource_limit(),
            Some(DedupeDiskResourceLimit::DiskQuotaExceeded { .. })
        ));
        assert!(store
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .persistence_error
            .is_some());

        store.run_maintenance();
        match store
            .begin(second_key)
            .expect("successful cleanup should preserve the replacement marker")
        {
            DedupeBeginOutcome::Duplicate { completion: actual } => {
                assert_eq!(actual, Some(completion.clone()));
            }
            other => panic!("expected replacement key replay, got {other:?}"),
        }
        drop(expect_accepted(&store, "tsink:key:cleanup:new"));
        assert_eq!(
            std::fs::metadata(&path)
                .expect("compacted marker metadata")
                .len(),
            record_bytes
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, record_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        drop(store);
        drop(budget);

        let restarted_budget =
            LocalDiskBudget::open(dir.path(), limits).expect("restarted budget should open");
        let restarted = DedupeWindowStore::open_with_disk_budget(
            path,
            config,
            Some(Arc::clone(&restarted_budget)),
            DiskCategory::Cluster,
        )
        .expect("compacted dedupe store should reopen");
        assert!(matches!(
            restarted
                .begin(second_key)
                .expect("replacement key lookup should succeed"),
            DedupeBeginOutcome::Duplicate { .. }
        ));
        assert_eq!(restarted_budget.snapshot().accounted_bytes, record_bytes);
    }

    #[test]
    fn budgeted_open_cleans_only_owned_atomic_and_legacy_temporaries() {
        let dir = tempfile::tempdir().expect("tempdir should build");
        let path = dir.path().join("cluster/dedupe/node-a.markers.log");
        let parent = path.parent().expect("marker path should have a parent");
        std::fs::create_dir_all(parent).expect("dedupe directory should build");
        let target_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("marker file name should be UTF-8");
        let generated_temp = parent.join(format!(".{target_name}.tmp-123-0000000000000000"));
        let generated_lookalike = parent.join(format!(".{target_name}.tmp-123-000000000000000G"));
        let legacy_temp = path.with_extension("tmp");
        let legacy_lookalike = parent.join(format!(
            "{}.keep",
            legacy_temp
                .file_name()
                .and_then(|name| name.to_str())
                .expect("legacy file name should be UTF-8")
        ));
        std::fs::write(&generated_temp, b"generated").expect("generated temporary should write");
        std::fs::write(&generated_lookalike, b"generated-lookalike")
            .expect("generated lookalike should write");
        std::fs::write(&legacy_temp, b"legacy").expect("legacy temporary should write");
        std::fs::write(&legacy_lookalike, b"legacy-lookalike")
            .expect("legacy lookalike should write");

        let budget = LocalDiskBudget::open(dir.path(), LocalDiskLimits::default())
            .expect("disk budget should open");
        let store = DedupeWindowStore::open_with_disk_budget(
            path.clone(),
            test_config(),
            Some(Arc::clone(&budget)),
            DiskCategory::Cluster,
        )
        .expect("budgeted dedupe store should open");

        assert!(!generated_temp.exists());
        assert!(!legacy_temp.exists());
        assert!(generated_lookalike.exists());
        assert!(legacy_lookalike.exists());
        assert_eq!(std::fs::metadata(path).expect("marker metadata").len(), 0);
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
        drop(store);
    }
}
