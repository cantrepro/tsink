use crate::prom_remote::MetricType;
use crate::prom_write::NormalizedMetricMetadataUpdate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

const METADATA_STORE_FILE_NAME: &str = "metric-metadata-store.json";
const METADATA_STORE_MAGIC: &str = "tsink-metric-metadata-store";
const METADATA_STORE_SCHEMA_VERSION: u16 = 1;

type MetricMetadataKey = (String, String);
type MetricMetadataEntries = BTreeMap<MetricMetadataKey, MetricMetadataRecord>;
type MetricMetadataReadGuard<'a> = RwLockReadGuard<'a, MetricMetadataEntries>;
type MetricMetadataWriteGuard<'a> = RwLockWriteGuard<'a, MetricMetadataEntries>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricMetadataRecord {
    pub tenant_id: String,
    pub metric_family_name: String,
    pub metric_type: i32,
    pub help: String,
    pub unit: String,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedMetricMetadataStore {
    magic: String,
    schema_version: u16,
    entries: Vec<MetricMetadataRecord>,
}

#[derive(Debug)]
pub struct MetricMetadataStore {
    path: Option<PathBuf>,
    entries: RwLock<MetricMetadataEntries>,
}

impl MetricMetadataStore {
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self {
            path: None,
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn open(data_path: Option<&Path>) -> Result<Self, String> {
        let path = data_path.map(|path| path.join(METADATA_STORE_FILE_NAME));
        let entries = if let Some(path) = path.as_ref() {
            load_entries(path)?
        } else {
            BTreeMap::new()
        };

        Ok(Self {
            path,
            entries: RwLock::new(entries),
        })
    }

    pub fn apply_updates(
        &self,
        tenant_id: &str,
        updates: &[NormalizedMetricMetadataUpdate],
    ) -> Result<usize, String> {
        if updates.is_empty() {
            return Ok(0);
        }

        let mut entries = self.write_entries()?;
        let mut changed = 0usize;
        let mut updated_unix_ms = unix_timestamp_millis();
        for update in updates {
            let key = (tenant_id.to_string(), update.metric_family_name.clone());
            let candidate = MetricMetadataRecord {
                tenant_id: tenant_id.to_string(),
                metric_family_name: update.metric_family_name.clone(),
                metric_type: update.metric_type as i32,
                help: update.help.clone(),
                unit: update.unit.clone(),
                updated_unix_ms,
            };
            let existing_matches = entries.get(&key).is_some_and(|existing| {
                existing.metric_type == candidate.metric_type
                    && existing.help == candidate.help
                    && existing.unit == candidate.unit
            });
            if existing_matches {
                continue;
            }

            entries.insert(key, candidate);
            changed = changed.saturating_add(1);
            updated_unix_ms = updated_unix_ms.saturating_add(1);
        }

        if changed > 0 {
            self.persist_entries(&entries)?;
        }

        Ok(changed)
    }

    pub fn query(
        &self,
        tenant_id: &str,
        metric: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetricMetadataRecord>, String> {
        let entries = self.read_entries()?;
        let mut out = Vec::new();
        for ((entry_tenant, entry_metric), entry) in entries.iter() {
            if entry_tenant != tenant_id {
                continue;
            }
            if metric.is_some_and(|metric| metric != entry_metric) {
                continue;
            }
            out.push(entry.clone());
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), String> {
        let snapshot_file = snapshot_path.join(METADATA_STORE_FILE_NAME);
        let entries = self.read_entries()?;
        write_store_file(&snapshot_file, &entries)
    }

    #[cfg(test)]
    pub fn file_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn read_entries(&self) -> Result<MetricMetadataReadGuard<'_>, String> {
        self.entries
            .read()
            .map_err(|_| "metric metadata store read lock poisoned".to_string())
    }

    fn write_entries(&self) -> Result<MetricMetadataWriteGuard<'_>, String> {
        self.entries
            .write()
            .map_err(|_| "metric metadata store write lock poisoned".to_string())
    }

    fn persist_entries(&self, entries: &MetricMetadataEntries) -> Result<(), String> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        write_store_file(path, entries)
    }
}

pub fn metric_type_to_api_string(metric_type: i32) -> &'static str {
    match MetricType::try_from(metric_type) {
        Ok(MetricType::Unknown) => "unknown",
        Ok(MetricType::Counter) => "counter",
        Ok(MetricType::Gauge) => "gauge",
        Ok(MetricType::Histogram) => "histogram",
        Ok(MetricType::Gaugehistogram) => "gaugehistogram",
        Ok(MetricType::Summary) => "summary",
        Ok(MetricType::Info) => "info",
        Ok(MetricType::Stateset) => "stateset",
        Err(_) => "unknown",
    }
}

fn load_entries(path: &Path) -> Result<BTreeMap<(String, String), MetricMetadataRecord>, String> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = std::fs::read(path).map_err(|err| {
        format!(
            "failed to read metric metadata store {}: {err}",
            path.display()
        )
    })?;
    let persisted: PersistedMetricMetadataStore = serde_json::from_slice(&raw).map_err(|err| {
        format!(
            "failed to parse metric metadata store {}: {err}",
            path.display()
        )
    })?;
    if persisted.magic != METADATA_STORE_MAGIC {
        return Err(format!(
            "metric metadata store {} has unsupported magic '{}'",
            path.display(),
            persisted.magic
        ));
    }
    if persisted.schema_version != METADATA_STORE_SCHEMA_VERSION {
        return Err(format!(
            "metric metadata store {} has unsupported schema version {}",
            path.display(),
            persisted.schema_version
        ));
    }

    let mut entries = BTreeMap::new();
    for entry in persisted.entries {
        entries.insert(
            (entry.tenant_id.clone(), entry.metric_family_name.clone()),
            entry,
        );
    }
    Ok(entries)
}

fn write_store_file(
    path: &Path,
    entries: &BTreeMap<(String, String), MetricMetadataRecord>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create metric metadata directory {}: {err}",
                parent.display()
            )
        })?;
    }

    let persisted = PersistedMetricMetadataStore {
        magic: METADATA_STORE_MAGIC.to_string(),
        schema_version: METADATA_STORE_SCHEMA_VERSION,
        entries: entries.values().cloned().collect(),
    };
    let mut encoded = serde_json::to_vec_pretty(&persisted)
        .map_err(|err| format!("failed to serialize metric metadata store: {err}"))?;
    encoded.push(b'\n');

    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|err| {
            format!(
                "failed to open temporary metric metadata store {}: {err}",
                tmp_path.display()
            )
        })?;
    file.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write temporary metric metadata store {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to fsync temporary metric metadata store {}: {err}",
            tmp_path.display()
        )
    })?;
    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to replace metric metadata store {} with {}: {err}",
            path.display(),
            tmp_path.display()
        )
    })
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_write::NormalizedMetricMetadataUpdate;
    use tempfile::TempDir;

    #[test]
    fn metadata_store_persists_last_write_wins_updates() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let store = MetricMetadataStore::open(Some(temp_dir.path()))
            .expect("persistent metadata store should open");

        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "original".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect("first metadata update should succeed");
        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "updated".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect("second metadata update should succeed");

        let reopened = MetricMetadataStore::open(Some(temp_dir.path()))
            .expect("reopened metadata store should load");
        let records = reopened
            .query("tenant-a", Some("http_requests_total"), 10)
            .expect("metadata query should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].help, "updated");
        assert_eq!(
            reopened
                .file_path()
                .expect("persistent store should expose file path")
                .file_name()
                .and_then(|value| value.to_str()),
            Some(METADATA_STORE_FILE_NAME)
        );
    }

    #[test]
    fn metadata_store_queries_are_tenant_scoped_and_sorted() {
        let store = MetricMetadataStore::in_memory();
        store
            .apply_updates(
                "tenant-b",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "zeta_metric".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "z".to_string(),
                    unit: "".to_string(),
                }],
            )
            .expect("metadata update should succeed");
        store
            .apply_updates(
                "tenant-a",
                &[
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "zeta_metric".to_string(),
                        metric_type: MetricType::Gauge,
                        help: "z".to_string(),
                        unit: "".to_string(),
                    },
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "alpha_metric".to_string(),
                        metric_type: MetricType::Counter,
                        help: "a".to_string(),
                        unit: "requests".to_string(),
                    },
                ],
            )
            .expect("metadata updates should succeed");

        let records = store
            .query("tenant-a", None, 10)
            .expect("metadata query should succeed");
        assert_eq!(
            records
                .iter()
                .map(|record| record.metric_family_name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha_metric", "zeta_metric"]
        );
        assert_eq!(
            store
                .query("tenant-a", None, 1)
                .expect("limited metadata query should succeed")
                .len(),
            1
        );
        assert_eq!(
            store
                .query("tenant-b", None, 10)
                .expect("other tenant metadata query should succeed")
                .len(),
            1
        );
    }

    #[test]
    fn metadata_store_snapshot_writes_snapshot_copy() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let snapshot_dir = temp_dir.path().join("snapshot");
        std::fs::create_dir_all(&snapshot_dir).expect("snapshot dir should build");
        let store = MetricMetadataStore::in_memory();
        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "cpu_usage".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "CPU usage".to_string(),
                    unit: "percent".to_string(),
                }],
            )
            .expect("metadata update should succeed");

        store
            .snapshot_into(&snapshot_dir)
            .expect("metadata snapshot should succeed");

        let reopened =
            MetricMetadataStore::open(Some(&snapshot_dir)).expect("snapshot copy should reopen");
        let records = reopened
            .query("tenant-a", None, 10)
            .expect("metadata query should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].metric_family_name, "cpu_usage");
    }
}
