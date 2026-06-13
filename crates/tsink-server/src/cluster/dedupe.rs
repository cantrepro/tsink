use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupeBeginOutcome {
    Accepted,
    Duplicate,
    InFlight,
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
    state: Arc<Mutex<DedupeState>>,
}

#[derive(Debug)]
struct DedupeState {
    entries: HashMap<String, u64>,
    entries_by_expiry: BTreeMap<u64, BTreeSet<String>>,
    in_flight: HashSet<String>,
    file: File,
    log_bytes: u64,
    next_cleanup_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DedupeRecord {
    key: String,
    expires_at_unix_secs: u64,
}

impl DedupeWindowStore {
    pub fn open(path: PathBuf, config: DedupeConfig) -> Result<Self, String> {
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
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create cluster dedupe directory {}: {err}",
                    parent.display()
                )
            })?;
        }

        let now = unix_timestamp_secs();
        let mut entries = HashMap::new();
        let mut entries_by_expiry = BTreeMap::new();
        if path.exists() {
            load_existing_records(&path, now, &mut entries, &mut entries_by_expiry)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|err| {
                format!(
                    "failed to open cluster dedupe marker log {}: {err}",
                    path.display()
                )
            })?;
        let log_bytes = file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();

        let state = DedupeState {
            entries,
            entries_by_expiry,
            in_flight: HashSet::new(),
            file,
            log_bytes,
            next_cleanup_unix_secs: now.saturating_add(config.cleanup_interval_secs),
        };

        let store = Self {
            path,
            config,
            state: Arc::new(Mutex::new(state)),
        };
        store.run_cleanup_cycle(now, true);
        {
            let state = store
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            update_gauges(&state);
        }

        Ok(store)
    }

    pub fn begin(&self, key: &str) -> Result<DedupeBeginOutcome, String> {
        validate_idempotency_key(key)?;
        CLUSTER_DEDUPE_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);

        let now = unix_timestamp_secs();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        self.maybe_cleanup_locked(&mut state, now, false);
        if let Some(expires_at) = state.entries.get(key).copied() {
            if expires_at > now {
                CLUSTER_DEDUPE_DUPLICATES_TOTAL.fetch_add(1, Ordering::Relaxed);
                update_gauges(&state);
                return Ok(DedupeBeginOutcome::Duplicate);
            }
            remove_entry_locked(&mut state, key, expires_at);
            CLUSTER_DEDUPE_EXPIRED_KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }

        if state.in_flight.contains(key) {
            CLUSTER_DEDUPE_INFLIGHT_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
            update_gauges(&state);
            return Ok(DedupeBeginOutcome::InFlight);
        }

        state.in_flight.insert(key.to_string());
        CLUSTER_DEDUPE_ACCEPTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        update_gauges(&state);
        Ok(DedupeBeginOutcome::Accepted)
    }

    pub fn commit(&self, key: &str) {
        let now = unix_timestamp_secs();
        let expires_at = now.saturating_add(self.config.window_secs);

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight.remove(key);

        insert_entry_locked(&mut state, key.to_string(), expires_at);
        while state.entries.len() > self.config.max_entries {
            if !evict_oldest_entry_locked(&mut state) {
                break;
            }
            CLUSTER_DEDUPE_EVICTED_KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }

        if let Err(err) = append_record_locked(&mut state, key, expires_at) {
            CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            eprintln!("cluster dedupe append failed: {err}");
        }

        CLUSTER_DEDUPE_COMMITS_TOTAL.fetch_add(1, Ordering::Relaxed);
        self.maybe_cleanup_locked(&mut state, now, false);
        update_gauges(&state);
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

    fn run_cleanup_cycle(&self, now: u64, force_compact: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.maybe_cleanup_locked(&mut state, now, force_compact);
        update_gauges(&state);
    }

    fn maybe_cleanup_locked(&self, state: &mut DedupeState, now: u64, force_compact: bool) {
        if !force_compact && now < state.next_cleanup_unix_secs {
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

        if force_compact || state.log_bytes > self.config.max_log_bytes {
            if let Err(err) = compact_locked(&self.path, state) {
                CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                eprintln!("cluster dedupe compaction failed: {err}");
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
    entries: &mut HashMap<String, u64>,
    entries_by_expiry: &mut BTreeMap<u64, BTreeSet<String>>,
) -> Result<(), String> {
    let file = File::open(path).map_err(|err| {
        format!(
            "failed to open cluster dedupe marker log {}: {err}",
            path.display()
        )
    })?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line =
            line.map_err(|err| format!("failed to read cluster dedupe marker log line: {err}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let parsed = match serde_json::from_str::<DedupeRecord>(&line) {
            Ok(parsed) => parsed,
            Err(_) => {
                CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        if parsed.expires_at_unix_secs <= now {
            continue;
        }
        if validate_idempotency_key(&parsed.key).is_err() {
            CLUSTER_DEDUPE_PERSISTENCE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        insert_loaded_entry(
            entries,
            entries_by_expiry,
            parsed.key,
            parsed.expires_at_unix_secs,
        );
    }
    Ok(())
}

fn insert_loaded_entry(
    entries: &mut HashMap<String, u64>,
    entries_by_expiry: &mut BTreeMap<u64, BTreeSet<String>>,
    key: String,
    expires_at: u64,
) {
    if let Some(previous_expiry) = entries.insert(key.clone(), expires_at) {
        remove_key_from_expiry_index(entries_by_expiry, &key, previous_expiry);
    }
    entries_by_expiry.entry(expires_at).or_default().insert(key);
}

fn insert_entry_locked(state: &mut DedupeState, key: String, expires_at: u64) {
    if let Some(previous_expiry) = state.entries.insert(key.clone(), expires_at) {
        remove_key_from_expiry_index(&mut state.entries_by_expiry, &key, previous_expiry);
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

fn append_record_locked(state: &mut DedupeState, key: &str, expires_at: u64) -> Result<(), String> {
    let record = DedupeRecord {
        key: key.to_string(),
        expires_at_unix_secs: expires_at,
    };
    let mut encoded = serde_json::to_vec(&record)
        .map_err(|err| format!("failed to serialize dedupe marker record: {err}"))?;
    encoded.push(b'\n');

    state
        .file
        .write_all(&encoded)
        .map_err(|err| format!("failed to append dedupe marker record: {err}"))?;
    state
        .file
        .flush()
        .map_err(|err| format!("failed to flush dedupe marker log: {err}"))?;
    state
        .file
        .sync_data()
        .map_err(|err| format!("failed to fsync dedupe marker log: {err}"))?;
    state.log_bytes = state.log_bytes.saturating_add(encoded.len() as u64);
    Ok(())
}

fn compact_locked(path: &Path, state: &mut DedupeState) -> Result<(), String> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|err| {
                format!(
                    "failed to open temporary dedupe marker file {}: {err}",
                    tmp_path.display()
                )
            })?;
        for (expires_at, keys) in &state.entries_by_expiry {
            for key in keys {
                let record = DedupeRecord {
                    key: key.clone(),
                    expires_at_unix_secs: *expires_at,
                };
                let mut encoded = serde_json::to_vec(&record)
                    .map_err(|err| format!("failed to serialize dedupe marker record: {err}"))?;
                encoded.push(b'\n');
                tmp_file
                    .write_all(&encoded)
                    .map_err(|err| format!("failed to write compacted dedupe marker log: {err}"))?;
            }
        }
        tmp_file
            .flush()
            .map_err(|err| format!("failed to flush compacted dedupe marker log: {err}"))?;
        tmp_file
            .sync_all()
            .map_err(|err| format!("failed to fsync compacted dedupe marker log: {err}"))?;
    }

    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to replace dedupe marker log {} with {}: {err}",
            path.display(),
            tmp_path.display()
        )
    })?;

    state.file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .map_err(|err| {
            format!(
                "failed to reopen dedupe marker log {} after compaction: {err}",
                path.display()
            )
        })?;
    state.log_bytes = state
        .file
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Ok(())
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

    #[test]
    fn validate_idempotency_key_enforces_format() {
        assert!(validate_idempotency_key("tsink:node-a:1234:abcd").is_ok());
        assert!(validate_idempotency_key("").is_err());
        assert!(validate_idempotency_key(" leading-space").is_err());
        assert!(validate_idempotency_key("bad/key").is_err());
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

        assert_eq!(
            store.begin("tsink:key:1").expect("begin should succeed"),
            DedupeBeginOutcome::Accepted
        );
        assert_eq!(
            store.begin("tsink:key:1").expect("begin should succeed"),
            DedupeBeginOutcome::InFlight
        );

        store.abort("tsink:key:1");
        assert_eq!(
            store.begin("tsink:key:1").expect("begin should succeed"),
            DedupeBeginOutcome::Accepted
        );
        store.commit("tsink:key:1");

        assert_eq!(
            store.begin("tsink:key:1").expect("begin should succeed"),
            DedupeBeginOutcome::Duplicate
        );
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
        assert_eq!(
            store
                .begin("tsink:key:restart")
                .expect("begin should succeed"),
            DedupeBeginOutcome::Accepted
        );
        store.commit("tsink:key:restart");
        drop(store);

        let reopened = DedupeWindowStore::open(path, config).expect("store should reopen");
        assert_eq!(
            reopened
                .begin("tsink:key:restart")
                .expect("begin should succeed"),
            DedupeBeginOutcome::Duplicate
        );
    }
}
