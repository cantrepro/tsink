use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CLUSTER_AUDIT_RETENTION_SECS_ENV: &str = "TSINK_CLUSTER_AUDIT_RETENTION_SECS";
pub const CLUSTER_AUDIT_MAX_LOG_BYTES_ENV: &str = "TSINK_CLUSTER_AUDIT_MAX_LOG_BYTES";
pub const CLUSTER_AUDIT_MAX_QUERY_LIMIT_ENV: &str = "TSINK_CLUSTER_AUDIT_MAX_QUERY_LIMIT";

const DEFAULT_AUDIT_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_AUDIT_MAX_LOG_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_AUDIT_MAX_QUERY_LIMIT: usize = 1000;
const DEFAULT_AUDIT_QUERY_LIMIT: usize = 100;

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

#[derive(Debug, Clone)]
pub struct ClusterAuditLog {
    path: PathBuf,
    config: ClusterAuditConfig,
    state: Arc<Mutex<ClusterAuditState>>,
}

#[derive(Debug)]
struct ClusterAuditState {
    entries: VecDeque<ClusterAuditRecord>,
    file: File,
    log_bytes: u64,
    next_id: u64,
}

impl ClusterAuditLog {
    pub fn open(path: PathBuf, config: ClusterAuditConfig) -> Result<Self, String> {
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
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create cluster audit directory {}: {err}",
                    parent.display()
                )
            })?;
        }

        let now_ms = unix_timestamp_millis();
        let cutoff_ms = now_ms.saturating_sub(config.retention_secs.saturating_mul(1000));
        let mut entries = VecDeque::new();
        let mut next_id = 1_u64;
        if path.exists() {
            let reader = BufReader::new(File::open(&path).map_err(|err| {
                format!("failed to open cluster audit log {}: {err}", path.display())
            })?);
            for (line_number, line) in reader.lines().enumerate() {
                let line = line.map_err(|err| {
                    format!(
                        "failed to read cluster audit log {} line {}: {err}",
                        path.display(),
                        line_number + 1
                    )
                })?;
                if line.trim().is_empty() {
                    continue;
                }
                let record: ClusterAuditRecord = serde_json::from_str(&line).map_err(|err| {
                    format!(
                        "failed to parse cluster audit log {} line {}: {err}",
                        path.display(),
                        line_number + 1
                    )
                })?;
                if record.timestamp_unix_ms < cutoff_ms {
                    continue;
                }
                next_id = next_id.max(record.id.saturating_add(1));
                entries.push_back(record);
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|err| format!("failed to open cluster audit log {}: {err}", path.display()))?;
        let log_bytes = file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();

        let store = Self {
            path,
            config,
            state: Arc::new(Mutex::new(ClusterAuditState {
                entries,
                file,
                log_bytes,
                next_id,
            })),
        };

        {
            let mut state = store
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut changed = prune_retention_locked(
                &mut state.entries,
                now_ms.saturating_sub(store.config.retention_secs.saturating_mul(1000)),
            );
            changed |= enforce_size_bound_locked(&mut state.entries, store.config.max_log_bytes)?;
            if changed || state.log_bytes > store.config.max_log_bytes {
                compact_locked(&store.path, &mut state)?;
            }
        }

        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, input: ClusterAuditEntryInput) -> Result<ClusterAuditRecord, String> {
        let timestamp_unix_ms = input
            .timestamp_unix_ms
            .unwrap_or_else(unix_timestamp_millis);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = ClusterAuditRecord {
            id: state.next_id,
            timestamp_unix_ms,
            operation: input.operation,
            actor: input.actor,
            target: input.target,
            outcome: input.outcome,
        };
        let encoded = serialize_record_line(&record)?;
        state
            .file
            .write_all(&encoded)
            .map_err(|err| format!("failed to append cluster audit record: {err}"))?;
        state
            .file
            .flush()
            .map_err(|err| format!("failed to flush cluster audit record: {err}"))?;
        state
            .file
            .sync_data()
            .map_err(|err| format!("failed to fsync cluster audit log: {err}"))?;
        state.log_bytes = state.log_bytes.saturating_add(encoded.len() as u64);
        state.next_id = state.next_id.saturating_add(1);
        state.entries.push_back(record.clone());

        let now_ms = unix_timestamp_millis();
        let mut changed = prune_retention_locked(
            &mut state.entries,
            now_ms.saturating_sub(self.config.retention_secs.saturating_mul(1000)),
        );
        changed |= enforce_size_bound_locked(&mut state.entries, self.config.max_log_bytes)?;
        if changed || state.log_bytes > self.config.max_log_bytes {
            compact_locked(&self.path, &mut state)?;
        }
        Ok(record)
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

fn serialize_record_line(record: &ClusterAuditRecord) -> Result<Vec<u8>, String> {
    let mut encoded = serde_json::to_vec(record)
        .map_err(|err| format!("failed to serialize audit record: {err}"))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn prune_retention_locked(entries: &mut VecDeque<ClusterAuditRecord>, cutoff_ms: u64) -> bool {
    let mut changed = false;
    while entries
        .front()
        .is_some_and(|record| record.timestamp_unix_ms < cutoff_ms)
    {
        entries.pop_front();
        changed = true;
    }
    changed
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
        total = total.saturating_add(serialize_record_line(entry)?.len() as u64);
    }
    Ok(total)
}

fn compact_locked(path: &Path, state: &mut ClusterAuditState) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|err| {
            format!(
                "failed to rewrite cluster audit log {}: {err}",
                path.display()
            )
        })?;
    let mut written_bytes = 0_u64;
    for entry in &state.entries {
        let line = serialize_record_line(entry)?;
        file.write_all(&line)
            .map_err(|err| format!("failed to write compacted cluster audit log: {err}"))?;
        written_bytes = written_bytes.saturating_add(line.len() as u64);
    }
    file.flush()
        .map_err(|err| format!("failed to flush compacted cluster audit log: {err}"))?;
    file.sync_data()
        .map_err(|err| format!("failed to fsync compacted cluster audit log: {err}"))?;
    state.file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .map_err(|err| {
            format!(
                "failed to reopen cluster audit log {}: {err}",
                path.display()
            )
        })?;
    state.log_bytes = written_bytes;
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
    use tempfile::TempDir;

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
}
