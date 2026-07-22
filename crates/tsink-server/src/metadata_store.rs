use crate::prom_remote::MetricType;
use crate::prom_write::NormalizedMetricMetadataUpdate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
    local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    entries: RwLock<MetricMetadataEntries>,
}

impl MetricMetadataStore {
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self {
            path: None,
            local_disk_budget: None,
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(data_path: Option<&Path>) -> Result<Self, String> {
        Self::open_with_disk_budget(data_path, None)
    }

    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, String> {
        let path = data_path.map(|path| path.join(METADATA_STORE_FILE_NAME));
        match (path.as_deref(), local_disk_budget.as_ref()) {
            (Some(path), Some(budget)) => {
                budget.cleanup_atomic_write_temps(path).map_err(|err| {
                    format!(
                        "failed to clean metric metadata temporary files for {}: {err}",
                        path.display()
                    )
                })?;
                budget.validate_managed_file_path(path).map_err(|err| {
                    format!(
                        "failed to validate metric metadata store {}: {err}",
                        path.display()
                    )
                })?;
            }
            (None, Some(_)) => {
                return Err(
                    "metric metadata cannot use a local disk budget without a data path"
                        .to_string(),
                )
            }
            _ => {}
        }
        let entries = if let Some(path) = path.as_ref() {
            load_entries(path)?
        } else {
            BTreeMap::new()
        };

        Ok(Self {
            path,
            local_disk_budget,
            entries: RwLock::new(entries),
        })
    }

    pub fn apply_updates(
        &self,
        tenant_id: &str,
        updates: &[NormalizedMetricMetadataUpdate],
    ) -> tsink::Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }

        let mut entries = self.write_entries().map_err(tsink::TsinkError::Other)?;
        let mut staged_entries = entries.clone();
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
            let existing_matches = staged_entries.get(&key).is_some_and(|existing| {
                existing.metric_type == candidate.metric_type
                    && existing.help == candidate.help
                    && existing.unit == candidate.unit
            });
            if existing_matches {
                continue;
            }

            staged_entries.insert(key, candidate);
            changed = changed.saturating_add(1);
            updated_unix_ms = updated_unix_ms.saturating_add(1);
        }

        if changed > 0 {
            self.persist_entries(&staged_entries)?;
            *entries = staged_entries;
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
        write_store_file(&snapshot_file, &entries, None).map_err(|err| err.to_string())
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

    fn persist_entries(&self, entries: &MetricMetadataEntries) -> tsink::Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        write_store_file(path, entries, self.local_disk_budget.as_ref())
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
    local_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
) -> tsink::Result<()> {
    let persisted = PersistedMetricMetadataStore {
        magic: METADATA_STORE_MAGIC.to_string(),
        schema_version: METADATA_STORE_SCHEMA_VERSION,
        entries: entries.values().cloned().collect(),
    };
    let mut encoded = serde_json::to_vec_pretty(&persisted)?;
    encoded.push(b'\n');

    if let Some(local_disk_budget) = local_disk_budget {
        return local_disk_budget.write_file_atomically_and_sync_parent(
            path,
            &encoded,
            tsink::DiskCategory::Metadata,
        );
    }

    tsink::engine::fs_utils::write_file_atomically_and_sync_parent(path, &encoded)
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

    #[test]
    fn metadata_store_persistence_failure_does_not_publish_updates() {
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
            .expect("initial metadata update should persist");

        let store_path = store
            .file_path()
            .expect("persistent store should expose file path");
        std::fs::remove_file(store_path).expect("persisted store should be removable");
        std::fs::create_dir(store_path).expect("blocking publication path should build");

        let error = store
            .apply_updates(
                "tenant-a",
                &[
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "http_requests_total".to_string(),
                        metric_type: MetricType::Counter,
                        help: "unpersisted replacement".to_string(),
                        unit: "requests".to_string(),
                    },
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "new_metric".to_string(),
                        metric_type: MetricType::Gauge,
                        help: "unpersisted insertion".to_string(),
                        unit: "widgets".to_string(),
                    },
                ],
            )
            .expect_err("publication-path collision should fail persistence");
        assert!(matches!(error, tsink::TsinkError::Io(_)), "{error}");

        let records = store
            .query("tenant-a", None, 10)
            .expect("metadata query should succeed after failed persistence");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].metric_family_name, "http_requests_total");
        assert_eq!(records[0].help, "original");

        assert!(
            std::fs::read_dir(temp_dir.path())
                .expect("metadata directory should remain readable")
                .all(|entry| !entry
                    .expect("metadata directory entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".metric-metadata-store.json.tmp-")),
            "a failed publication must clean its unique temporary file"
        );
    }

    #[test]
    fn metadata_store_disk_quota_rejection_does_not_publish_updates() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(64),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("persistent metadata store should open");

        let error = store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "a deliberately long help string that exceeds the tiny quota".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect_err("tiny disk quota should reject metadata persistence");
        assert!(matches!(error, tsink::TsinkError::DiskQuotaExceeded { .. }));
        assert!(
            store
                .query("tenant-a", None, 10)
                .expect("metadata query should succeed")
                .is_empty(),
            "a rejected update must not become visible"
        );
        assert!(
            !store
                .file_path()
                .expect("persistent store should expose file path")
                .exists(),
            "a rejected update must not publish a store file"
        );

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn metadata_store_observes_shared_root_usage_and_exact_category_accounting() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let shared_file = temp_dir.path().join("unrecognized-owner.bin");
        std::fs::write(&shared_file, vec![0_u8; 1_000]).expect("shared file should be written");
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(1_024),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("persistent metadata store should open");
        let update = NormalizedMetricMetadataUpdate {
            metric_family_name: "cpu_usage".to_string(),
            metric_type: MetricType::Gauge,
            help: "CPU usage".to_string(),
            unit: "percent".to_string(),
        };

        let error = store
            .apply_updates("tenant-a", std::slice::from_ref(&update))
            .expect_err("other usage beneath the shared root should reject the rewrite");
        assert!(matches!(error, tsink::TsinkError::DiskQuotaExceeded { .. }));
        std::fs::remove_file(shared_file).expect("shared file should be removed");
        budget
            .reconcile()
            .expect("shared budget should reconcile after removal");
        store
            .apply_updates("tenant-a", &[update])
            .expect("metadata should persist after releasing shared capacity");

        let file_bytes = std::fs::metadata(
            store
                .file_path()
                .expect("persistent store should expose file path"),
        )
        .expect("metadata file should exist")
        .len();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, file_bytes);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == tsink::DiskCategory::Metadata)
                .map(|usage| usage.bytes),
            Some(file_bytes)
        );
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn shared_budget_restart_recounts_sidecars_and_preserves_reads_at_limit() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let initial_budget =
            tsink::LocalDiskBudget::open(temp_dir.path(), tsink::LocalDiskLimits::default())
                .expect("initial disk budget should open");
        let metadata_store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&initial_budget)),
        )
        .expect("metadata store should open");
        let exemplar_store = crate::exemplar_store::ExemplarStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&initial_budget)),
        )
        .expect("exemplar store should open");
        metadata_store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "restart_metric".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "persisted before restart".to_string(),
                    unit: "widgets".to_string(),
                }],
            )
            .expect("metadata should persist");
        exemplar_store
            .apply_writes(&[crate::exemplar_store::ExemplarWrite {
                metric: "restart_metric".to_string(),
                series_labels: vec![tsink::Label::new("job", "restart")],
                exemplar_labels: vec![tsink::Label::new("trace_id", "persisted")],
                timestamp: 10,
                value: 1.0,
            }])
            .expect("exemplar should persist");
        let used_bytes = initial_budget.snapshot().accounted_bytes;
        assert!(used_bytes > 0);
        drop(metadata_store);
        drop(exemplar_store);
        drop(initial_budget);

        let restarted_budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(used_bytes),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("restarted disk budget should recount existing files");
        let metadata_store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&restarted_budget)),
        )
        .expect("metadata store should reopen");
        let exemplar_store = crate::exemplar_store::ExemplarStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&restarted_budget)),
        )
        .expect("exemplar store should reopen");

        let metadata = metadata_store
            .query("tenant-a", Some("restart_metric"), 10)
            .expect("metadata should remain readable");
        assert_eq!(metadata.len(), 1);
        let exemplars = exemplar_store
            .query(
                &[tsink::SeriesSelection::new()
                    .with_metric("restart_metric")
                    .with_matcher(tsink::SeriesMatcher::equal("job", "restart"))],
                0,
                20,
                10,
            )
            .expect("exemplars should remain readable");
        assert_eq!(exemplars.len(), 1);
        assert_eq!(exemplars[0].exemplars.len(), 1);

        let metadata_error = metadata_store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "restart_metric".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "must not replace persisted state at the limit".to_string(),
                    unit: "widgets".to_string(),
                }],
            )
            .expect_err("metadata growth at the recounted limit should fail");
        assert!(matches!(
            metadata_error,
            tsink::TsinkError::DiskQuotaExceeded { .. }
        ));
        let exemplar_error = exemplar_store
            .apply_writes(&[crate::exemplar_store::ExemplarWrite {
                metric: "restart_metric".to_string(),
                series_labels: vec![tsink::Label::new("job", "restart")],
                exemplar_labels: vec![tsink::Label::new("trace_id", "rejected")],
                timestamp: 20,
                value: 2.0,
            }])
            .expect_err("exemplar growth at the recounted limit should fail");
        assert!(matches!(
            exemplar_error,
            tsink::TsinkError::DiskQuotaExceeded { .. }
        ));

        let snapshot = restarted_budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, used_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 2);
    }

    #[cfg(unix)]
    #[test]
    fn budgeted_store_rejects_a_symlinked_owned_file() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let outside = temp_dir.path().join("outside.json");
        std::fs::write(&outside, b"outside-state").expect("outside file should write");
        std::os::unix::fs::symlink(&outside, temp_dir.path().join(METADATA_STORE_FILE_NAME))
            .expect("managed-file symlink should build");
        let budget =
            tsink::LocalDiskBudget::open(temp_dir.path(), tsink::LocalDiskLimits::default())
                .expect("disk budget should open");

        let error = MetricMetadataStore::open_with_disk_budget(Some(temp_dir.path()), Some(budget))
            .expect_err("the owned store path must not be followed through a symlink");
        assert!(error.contains("must be a regular file"), "{error}");
        assert_eq!(
            std::fs::read(&outside).expect("outside file should remain readable"),
            b"outside-state"
        );
    }
}
