use crate::tenant;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tsink::{DiskCategory, Label, LocalDiskBudget, MetricSeries, Storage};

const USAGE_LEDGER_DIR: &str = "usage-accounting";
const USAGE_LEDGER_FILE: &str = "ledger.ndjson";
const USAGE_LEDGER_BATCH_MAGIC: &str = "tsink-usage-ledger-batch";
const USAGE_LEDGER_BATCH_SCHEMA_VERSION: u16 = 1;
const ESTIMATED_SERIES_OVERHEAD_BYTES: u64 = 64;
const ESTIMATED_SAMPLE_BYTES: u64 = 16;
const STORAGE_RECONCILE_BATCH_SIZE: usize = 128;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UsageCategory {
    Ingest,
    Query,
    Retention,
    Background,
    Storage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UsageBucketWidth {
    None,
    Hour,
    Day,
}

impl UsageBucketWidth {
    fn bucket_size_ms(self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Hour => Some(60 * 60 * 1_000),
            Self::Day => Some(24 * 60 * 60 * 1_000),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageTotals {
    pub events_total: u64,
    pub request_units: u64,
    pub result_units: u64,
    pub rows: u64,
    pub metadata_updates: u64,
    pub exemplars_accepted: u64,
    pub exemplars_dropped: u64,
    pub histogram_series: u64,
    pub matched_series: u64,
    pub tombstones_applied: u64,
    pub duration_nanos: u64,
    pub request_bytes: u64,
    pub errors_total: u64,
}

impl UsageTotals {
    fn apply(&mut self, record: &UsageLedgerRecord) {
        self.events_total = self.events_total.saturating_add(1);
        self.request_units = self.request_units.saturating_add(record.request_units);
        self.result_units = self.result_units.saturating_add(record.result_units);
        self.rows = self.rows.saturating_add(record.rows);
        self.metadata_updates = self
            .metadata_updates
            .saturating_add(record.metadata_updates);
        self.exemplars_accepted = self
            .exemplars_accepted
            .saturating_add(record.exemplars_accepted);
        self.exemplars_dropped = self
            .exemplars_dropped
            .saturating_add(record.exemplars_dropped);
        self.histogram_series = self
            .histogram_series
            .saturating_add(record.histogram_series);
        self.matched_series = self.matched_series.saturating_add(record.matched_series);
        self.tombstones_applied = self
            .tombstones_applied
            .saturating_add(record.tombstones_applied);
        self.duration_nanos = self.duration_nanos.saturating_add(record.duration_nanos);
        self.request_bytes = self.request_bytes.saturating_add(record.request_bytes);
        if record.status != "success" {
            self.errors_total = self.errors_total.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageStorageSnapshot {
    pub tenant_id: String,
    pub reconciled_unix_ms: u64,
    pub series_total: u64,
    pub samples_total: u64,
    pub logical_storage_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageTenantSummary {
    pub tenant_id: String,
    pub ingest: UsageTotals,
    pub query: UsageTotals,
    pub retention: UsageTotals,
    pub background: UsageTotals,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_storage_snapshot: Option<UsageStorageSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageTenantBucketSummary {
    pub tenant_id: String,
    pub ingest: UsageTotals,
    pub query: UsageTotals,
    pub retention: UsageTotals,
    pub background: UsageTotals,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageBucketSummary {
    pub bucket_start_unix_ms: u64,
    pub bucket_end_unix_ms: u64,
    #[serde(default)]
    pub tenants: Vec<UsageTenantBucketSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UsageLedgerStatus {
    pub durable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_path: Option<String>,
    pub records_total: u64,
    pub tenant_count: u64,
    pub last_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_record_unix_ms: Option<u64>,
    pub storage_reconciliations_total: u64,
    pub record_failures_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_record_error_code: Option<String>,
}

#[derive(Debug)]
pub enum UsageAccountingError {
    Disk(tsink::TsinkError),
    Persistence(String),
    Other(String),
}

impl UsageAccountingError {
    pub fn disk_error(&self) -> Option<&tsink::TsinkError> {
        match self {
            Self::Disk(err) => Some(err),
            Self::Persistence(_) | Self::Other(_) => None,
        }
    }

    pub fn is_persistence_failure(&self) -> bool {
        matches!(self, Self::Disk(_) | Self::Persistence(_))
    }

    fn status_code(&self) -> &'static str {
        match self {
            Self::Disk(
                tsink::TsinkError::DiskQuotaExceeded { .. }
                | tsink::TsinkError::InsufficientDiskSpace { .. }
                | tsink::TsinkError::InsufficientCompactionHeadroom { .. },
            ) => "usage_ledger_disk_quota_exceeded",
            Self::Disk(_) => "usage_ledger_disk_error",
            Self::Persistence(_) => "usage_ledger_persistence_error",
            Self::Other(_) => "usage_accounting_error",
        }
    }
}

impl fmt::Display for UsageAccountingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disk(err) => write!(formatter, "{err}"),
            Self::Persistence(message) | Self::Other(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for UsageAccountingError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageReportFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_unix_ms: Option<u64>,
    pub bucket_width: UsageBucketWidth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageReport {
    pub filter: UsageReportFilter,
    pub journal: UsageLedgerStatus,
    #[serde(default)]
    pub tenants: Vec<UsageTenantSummary>,
    #[serde(default)]
    pub buckets: Vec<UsageBucketSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageLedgerRecord {
    pub seq: u64,
    pub unix_ms: u64,
    pub tenant_id: String,
    pub category: UsageCategory,
    pub operation: String,
    pub source: String,
    pub status: String,
    pub request_units: u64,
    pub result_units: u64,
    pub rows: u64,
    pub metadata_updates: u64,
    pub exemplars_accepted: u64,
    pub exemplars_dropped: u64,
    pub histogram_series: u64,
    pub matched_series: u64,
    pub tombstones_applied: u64,
    pub duration_nanos: u64,
    pub request_bytes: u64,
    pub logical_storage_series: u64,
    pub logical_storage_samples: u64,
    pub logical_storage_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PersistedUsageLedgerBatch {
    magic: String,
    schema_version: u16,
    records: Vec<UsageLedgerRecord>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedUsageLedgerBatchRef<'a> {
    magic: &'static str,
    schema_version: u16,
    records: &'a [UsageLedgerRecord],
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PersistedUsageLedgerLine {
    Record(UsageLedgerRecord),
    Batch(PersistedUsageLedgerBatch),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageRecordInput<'a> {
    pub tenant_id: &'a str,
    pub category: UsageCategory,
    pub operation: &'a str,
    pub source: &'a str,
    pub status: &'a str,
    pub request_units: u64,
    pub result_units: u64,
    pub rows: u64,
    pub metadata_updates: u64,
    pub exemplars_accepted: u64,
    pub exemplars_dropped: u64,
    pub histogram_series: u64,
    pub matched_series: u64,
    pub tombstones_applied: u64,
    pub duration_nanos: u64,
    pub request_bytes: u64,
    pub logical_storage_series: u64,
    pub logical_storage_samples: u64,
    pub logical_storage_bytes: u64,
}

impl<'a> UsageRecordInput<'a> {
    pub fn success(
        tenant_id: &'a str,
        category: UsageCategory,
        operation: &'a str,
        source: &'a str,
    ) -> Self {
        Self {
            tenant_id,
            category,
            operation,
            source,
            status: "success",
            request_units: 0,
            result_units: 0,
            rows: 0,
            metadata_updates: 0,
            exemplars_accepted: 0,
            exemplars_dropped: 0,
            histogram_series: 0,
            matched_series: 0,
            tombstones_applied: 0,
            duration_nanos: 0,
            request_bytes: 0,
            logical_storage_series: 0,
            logical_storage_samples: 0,
            logical_storage_bytes: 0,
        }
    }
}

#[derive(Debug)]
struct UsageLedgerState {
    next_seq: u64,
    records: Vec<UsageLedgerRecord>,
}

impl Default for UsageLedgerState {
    fn default() -> Self {
        Self {
            next_seq: 1,
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct UsageLedgerHealth {
    record_failures_total: u64,
    last_record_error_code: Option<String>,
}

#[derive(Debug)]
struct UsageAccountingInner {
    ledger_path: Option<PathBuf>,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    append_serialization: Mutex<()>,
    async_append_serialization: Arc<tokio::sync::Semaphore>,
    async_reconcile_serialization: Arc<tokio::sync::Semaphore>,
    writer: Mutex<Option<File>>,
    state: Mutex<UsageLedgerState>,
    health: Mutex<UsageLedgerHealth>,
}

#[derive(Debug, Clone)]
pub struct UsageAccounting {
    inner: Arc<UsageAccountingInner>,
}

impl UsageAccounting {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(data_path: Option<&Path>) -> Result<Arc<Self>, String> {
        Self::open_with_disk_budget(data_path, None)
    }

    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Arc<Self>, String> {
        if data_path.is_none() && local_disk_budget.is_some() {
            return Err(
                "usage accounting cannot use a local disk budget without a data path".to_string(),
            );
        }

        let (ledger_path, writer, state) = match data_path {
            Some(data_path) => {
                let dir = data_path.join(USAGE_LEDGER_DIR);
                let ledger_path = dir.join(USAGE_LEDGER_FILE);
                if let Some(budget) = local_disk_budget.as_ref() {
                    budget
                        .create_dir_all_and_sync_parents(&dir)
                        .map_err(|err| {
                            format!(
                                "failed to durably create usage ledger directory {}: {err}",
                                dir.display()
                            )
                        })?;
                    budget
                        .validate_managed_file_path(&ledger_path)
                        .map_err(|err| {
                            format!(
                                "failed to validate usage ledger path {}: {err}",
                                ledger_path.display()
                            )
                        })?;
                } else {
                    fs::create_dir_all(&dir).map_err(|err| {
                        format!(
                            "failed to create usage ledger directory {}: {err}",
                            dir.display()
                        )
                    })?;
                }
                let state = load_usage_ledger(&ledger_path)?;
                let writer = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&ledger_path)
                    .map_err(|err| {
                        format!(
                            "failed to open usage ledger {} for append: {err}",
                            ledger_path.display()
                        )
                    })?;
                (Some(ledger_path), Some(writer), state)
            }
            None => (None, None, UsageLedgerState::default()),
        };

        Ok(Arc::new(Self {
            inner: Arc::new(UsageAccountingInner {
                ledger_path,
                local_disk_budget,
                append_serialization: Mutex::new(()),
                async_append_serialization: Arc::new(tokio::sync::Semaphore::new(1)),
                async_reconcile_serialization: Arc::new(tokio::sync::Semaphore::new(1)),
                writer: Mutex::new(writer),
                state: Mutex::new(state),
                health: Mutex::new(UsageLedgerHealth::default()),
            }),
        }))
    }

    pub fn ledger_status(&self) -> UsageLedgerStatus {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let health = self
            .inner
            .health
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ledger_status_from_records(self.inner.ledger_path.as_deref(), &state.records, &health)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn record(
        &self,
        input: UsageRecordInput<'_>,
    ) -> Result<UsageLedgerRecord, UsageAccountingError> {
        self.append_records(vec![usage_record_from_input(input)])
            .map(|mut records| records.pop().expect("one usage record must be returned"))
    }

    pub async fn record_async(
        &self,
        input: UsageRecordInput<'_>,
    ) -> Result<UsageLedgerRecord, UsageAccountingError> {
        let record = usage_record_from_input(input);
        self.append_records_async(vec![record], "append")
            .await
            .map(|mut records| records.pop().expect("one usage record must be returned"))
    }

    async fn append_records_async(
        &self,
        records: Vec<UsageLedgerRecord>,
        task_name: &'static str,
    ) -> Result<Vec<UsageLedgerRecord>, UsageAccountingError> {
        let permit = match Arc::clone(&self.inner.async_append_serialization)
            .acquire_owned()
            .await
        {
            Ok(permit) => permit,
            Err(err) => {
                let err = UsageAccountingError::Other(format!(
                    "usage accounting append coordinator is closed: {err}"
                ));
                self.note_failure(&err);
                return Err(err);
            }
        };
        let accounting = self.clone();
        match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            accounting.append_records(records)
        })
        .await
        {
            Ok(result) => result,
            Err(join_err) => {
                // A blocking append task can panic after it has started mutating the ledger.
                // Treat the join boundary as indeterminate persistence, not as a safe internal
                // rejection.
                let err = append_task_join_failure(task_name, join_err);
                self.note_failure(&err);
                Err(err)
            }
        }
    }

    pub async fn record_best_effort(&self, input: UsageRecordInput<'_>) {
        if self.record_async(input).await.is_err() {
            // append_records records a fixed-cardinality health failure and emits a rate-limited
            // diagnostic. The caller awaits metering, but a ledger failure does not change the
            // classification of already-completed primary work or invite a duplicate data retry.
        }
    }

    fn note_failure(&self, err: &UsageAccountingError) {
        let (failure_count, error_code) = {
            let mut health = self
                .inner
                .health
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            health.record_failures_total = health.record_failures_total.saturating_add(1);
            let error_code = err.status_code();
            health.last_record_error_code = Some(error_code.to_string());
            (health.record_failures_total, error_code)
        };
        if failure_count.is_power_of_two() {
            eprintln!(
                "usage accounting append failed (code={error_code}, failures={failure_count}): {err}"
            );
        }
    }

    fn append_records(
        &self,
        mut records: Vec<UsageLedgerRecord>,
    ) -> Result<Vec<UsageLedgerRecord>, UsageAccountingError> {
        if records.is_empty() {
            return Ok(records);
        }

        let result = (|| {
            // Serialize sequence allocation, durable append, and publication without holding the
            // state lock across filesystem I/O. Status and metrics readers therefore remain
            // responsive while an append is waiting on a slow disk.
            let _append_guard = self
                .inner
                .append_serialization
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let initial_next_seq = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .next_seq;
            let mut next_seq = initial_next_seq;
            for record in &mut records {
                record.seq = next_seq;
                next_seq = next_seq.checked_add(1).ok_or_else(|| {
                    UsageAccountingError::Other(
                        "usage ledger sequence space is exhausted".to_string(),
                    )
                })?;
            }

            if self.inner.ledger_path.is_some() {
                let mut encoded = Vec::new();
                if records.len() == 1 {
                    serde_json::to_writer(&mut encoded, &records[0]).map_err(|err| {
                        UsageAccountingError::Other(format!("failed to encode usage record: {err}"))
                    })?;
                } else {
                    serde_json::to_writer(
                        &mut encoded,
                        &PersistedUsageLedgerBatchRef {
                            magic: USAGE_LEDGER_BATCH_MAGIC,
                            schema_version: USAGE_LEDGER_BATCH_SCHEMA_VERSION,
                            records: &records,
                        },
                    )
                    .map_err(|err| {
                        UsageAccountingError::Other(format!(
                            "failed to encode usage record batch: {err}"
                        ))
                    })?;
                }
                encoded.push(b'\n');
                let mut writer = self
                    .inner
                    .writer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let path = self
                    .inner
                    .ledger_path
                    .as_deref()
                    .expect("persistent usage accounting must have a ledger path");
                let file = writer.as_mut().ok_or_else(|| {
                    UsageAccountingError::Persistence(format!(
                        "usage ledger {} is unavailable after an earlier append failure",
                        path.display()
                    ))
                })?;
                let initial_len = file
                    .metadata()
                    .map_err(|err| {
                        UsageAccountingError::Persistence(format!(
                            "failed to inspect usage ledger {} before append: {err}",
                            path.display()
                        ))
                    })?
                    .len();
                let append_result = if let Some(budget) = self.inner.local_disk_budget.as_ref() {
                    budget
                        .append_file_and_sync_parent(path, &encoded, DiskCategory::ServerState)
                        .map_err(UsageAccountingError::Disk)
                } else {
                    append_usage_record_unbudgeted(path, file, &encoded, initial_len)
                        .map_err(UsageAccountingError::Persistence)
                };
                if let Err(err) = append_result {
                    let safely_rolled_back = writer
                        .as_ref()
                        .and_then(|file| file.metadata().ok())
                        .is_some_and(|metadata| metadata.len() == initial_len);
                    if !safely_rolled_back {
                        // Never append another record after bytes of a failed batch may have
                        // survived. Reopening will reject an unterminated/torn tail.
                        *writer = None;
                    }
                    return Err(err);
                }
            }

            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            debug_assert_eq!(state.next_seq, initial_next_seq);
            state.next_seq = next_seq;
            state.records.extend(records.iter().cloned());
            Ok(records)
        })();

        if let Err(err) = result.as_ref() {
            self.note_failure(err);
        }
        result
    }

    pub fn report(
        &self,
        tenant_id: Option<&str>,
        start_unix_ms: Option<u64>,
        end_unix_ms: Option<u64>,
        bucket_width: UsageBucketWidth,
    ) -> UsageReport {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut tenants = BTreeMap::<String, SummaryAccumulator>::new();
        let mut buckets = BTreeMap::<u64, BTreeMap<String, BucketAccumulator>>::new();
        let end_limit = end_unix_ms.unwrap_or(u64::MAX);
        let bucket_size_ms = bucket_width.bucket_size_ms();

        for record in &state.records {
            if !matches_tenant_filter(record, tenant_id) {
                continue;
            }
            if record.category == UsageCategory::Storage && record.unix_ms <= end_limit {
                tenants
                    .entry(record.tenant_id.clone())
                    .or_default()
                    .apply_storage(record);
                continue;
            }
            if !matches_time_filter(record, start_unix_ms, end_unix_ms) {
                continue;
            }
            tenants
                .entry(record.tenant_id.clone())
                .or_default()
                .apply_usage(record);

            if let Some(bucket_size_ms) = bucket_size_ms {
                let bucket_start = (record.unix_ms / bucket_size_ms) * bucket_size_ms;
                buckets
                    .entry(bucket_start)
                    .or_default()
                    .entry(record.tenant_id.clone())
                    .or_default()
                    .apply(record);
            }
        }

        let tenant_summaries = tenants
            .into_iter()
            .map(|(tenant_id, summary)| summary.into_summary(tenant_id))
            .collect::<Vec<_>>();

        let bucket_summaries = buckets
            .into_iter()
            .map(|(bucket_start, tenants)| UsageBucketSummary {
                bucket_start_unix_ms: bucket_start,
                bucket_end_unix_ms: bucket_start.saturating_add(bucket_size_ms.unwrap_or_default()),
                tenants: tenants
                    .into_iter()
                    .map(|(tenant_id, summary)| summary.into_summary(tenant_id))
                    .collect(),
            })
            .collect::<Vec<_>>();

        let health = self
            .inner
            .health
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        UsageReport {
            filter: UsageReportFilter {
                tenant_id: tenant_id.map(str::to_string),
                start_unix_ms,
                end_unix_ms,
                bucket_width,
            },
            journal: ledger_status_from_records(
                self.inner.ledger_path.as_deref(),
                &state.records,
                &health,
            ),
            tenants: tenant_summaries,
            buckets: bucket_summaries,
        }
    }

    pub fn export_records(
        &self,
        tenant_id: Option<&str>,
        start_unix_ms: Option<u64>,
        end_unix_ms: Option<u64>,
    ) -> Vec<UsageLedgerRecord> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .records
            .iter()
            .filter(|record| matches_tenant_filter(record, tenant_id))
            .filter(|record| matches_time_filter(record, start_unix_ms, end_unix_ms))
            .cloned()
            .collect()
    }

    pub fn tenant_summary(&self, tenant_id: &str) -> UsageTenantSummary {
        self.report(Some(tenant_id), None, None, UsageBucketWidth::None)
            .tenants
            .into_iter()
            .find(|summary| summary.tenant_id == tenant_id)
            .unwrap_or_else(|| UsageTenantSummary {
                tenant_id: tenant_id.to_string(),
                ..UsageTenantSummary::default()
            })
    }

    pub fn latest_storage_snapshot_for(&self, tenant_id: &str) -> Option<UsageStorageSnapshot> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.records.iter().rev().find_map(|record| {
            if record.category == UsageCategory::Storage && record.tenant_id == tenant_id {
                Some(UsageStorageSnapshot::from(record))
            } else {
                None
            }
        })
    }

    pub fn reconcile_storage(
        &self,
        storage: &Arc<dyn Storage>,
    ) -> Result<Vec<UsageStorageSnapshot>, UsageAccountingError> {
        let records = collect_storage_reconciliation_records(storage)?;
        Ok(storage_snapshots_from_records(
            self.append_records(records)?,
        ))
    }

    pub async fn reconcile_storage_async(
        &self,
        storage: Arc<dyn Storage>,
    ) -> Result<Vec<UsageStorageSnapshot>, UsageAccountingError> {
        // Bound full-database scans independently from ordinary ledger appends. Holding this
        // permit through publication also gives concurrent admin reconciliation requests a clear
        // whole-operation order without coupling request metering to scan latency.
        let reconciliation_permit = match Arc::clone(&self.inner.async_reconcile_serialization)
            .acquire_owned()
            .await
        {
            Ok(permit) => permit,
            Err(err) => {
                let err = UsageAccountingError::Other(format!(
                    "usage accounting reconciliation coordinator is closed: {err}"
                ));
                self.note_failure(&err);
                return Err(err);
            }
        };
        // The storage scan can be substantially slower than the final ledger append. Keep it off
        // runtime workers, but do not hold the one-permit append coordinator while it runs so
        // ordinary request metering remains independent of reconciliation latency.
        let records = match tokio::task::spawn_blocking(move || {
            collect_storage_reconciliation_records(&storage)
        })
        .await
        {
            Ok(result) => result?,
            Err(join_err) => {
                let err = UsageAccountingError::Other(format!(
                    "usage accounting reconciliation scan task failed: {join_err}"
                ));
                self.note_failure(&err);
                return Err(err);
            }
        };

        let records = self
            .append_records_async(records, "reconciliation append")
            .await?;
        drop(reconciliation_permit);
        Ok(storage_snapshots_from_records(records))
    }
}

fn append_task_join_failure(
    task_name: &str,
    join_err: tokio::task::JoinError,
) -> UsageAccountingError {
    UsageAccountingError::Persistence(format!(
        "usage accounting {task_name} task failed: {join_err}"
    ))
}

fn collect_storage_reconciliation_records(
    storage: &Arc<dyn Storage>,
) -> Result<Vec<UsageLedgerRecord>, UsageAccountingError> {
    let metrics = storage.list_metrics().map_err(|err| {
        UsageAccountingError::Other(format!(
            "usage storage reconciliation failed to list metrics: {err}"
        ))
    })?;
    let mut per_tenant = BTreeMap::<String, StorageAccumulator>::new();

    for chunk in metrics.chunks(STORAGE_RECONCILE_BATCH_SIZE) {
        let selected = storage
            .select_many(chunk, i64::MIN, i64::MAX)
            .map_err(|err| {
                UsageAccountingError::Other(format!(
                    "usage storage reconciliation failed to read points: {err}"
                ))
            })?;
        for series in selected {
            let tenant_id = tenant_id_for_metric_series(&series.series);
            let point_count = series.points.len() as u64;
            let acc = per_tenant.entry(tenant_id).or_default();
            acc.series_total = acc.series_total.saturating_add(1);
            acc.samples_total = acc.samples_total.saturating_add(point_count);
            acc.logical_storage_bytes = acc
                .logical_storage_bytes
                .saturating_add(estimated_series_bytes(&series.series, point_count));
        }
    }

    let reconciled_unix_ms = unix_timestamp_millis();
    let mut records = Vec::with_capacity(per_tenant.len());
    for (tenant_id, acc) in per_tenant {
        records.push(UsageLedgerRecord {
            seq: 0,
            unix_ms: reconciled_unix_ms,
            tenant_id,
            category: UsageCategory::Storage,
            operation: "reconcile_storage".to_string(),
            source: "admin".to_string(),
            status: "success".to_string(),
            request_units: 0,
            result_units: 0,
            rows: 0,
            metadata_updates: 0,
            exemplars_accepted: 0,
            exemplars_dropped: 0,
            histogram_series: 0,
            matched_series: 0,
            tombstones_applied: 0,
            duration_nanos: 0,
            request_bytes: 0,
            logical_storage_series: acc.series_total,
            logical_storage_samples: acc.samples_total,
            logical_storage_bytes: acc.logical_storage_bytes,
        });
    }
    Ok(records)
}

fn storage_snapshots_from_records(records: Vec<UsageLedgerRecord>) -> Vec<UsageStorageSnapshot> {
    let mut snapshots = records
        .into_iter()
        .map(|record| UsageStorageSnapshot {
            tenant_id: record.tenant_id,
            reconciled_unix_ms: record.unix_ms,
            series_total: record.logical_storage_series,
            samples_total: record.logical_storage_samples,
            logical_storage_bytes: record.logical_storage_bytes,
        })
        .collect::<Vec<_>>();
    snapshots.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
    snapshots
}

#[derive(Debug, Default)]
struct SummaryAccumulator {
    ingest: UsageTotals,
    query: UsageTotals,
    retention: UsageTotals,
    background: UsageTotals,
    latest_storage_snapshot: Option<UsageStorageSnapshot>,
}

impl SummaryAccumulator {
    fn apply_usage(&mut self, record: &UsageLedgerRecord) {
        match record.category {
            UsageCategory::Ingest => self.ingest.apply(record),
            UsageCategory::Query => self.query.apply(record),
            UsageCategory::Retention => self.retention.apply(record),
            UsageCategory::Background => self.background.apply(record),
            UsageCategory::Storage => self.apply_storage(record),
        }
    }

    fn apply_storage(&mut self, record: &UsageLedgerRecord) {
        let replace = self
            .latest_storage_snapshot
            .as_ref()
            .map(|snapshot| snapshot.reconciled_unix_ms <= record.unix_ms)
            .unwrap_or(true);
        if replace {
            self.latest_storage_snapshot = Some(UsageStorageSnapshot::from(record));
        }
    }

    fn into_summary(self, tenant_id: String) -> UsageTenantSummary {
        UsageTenantSummary {
            tenant_id,
            ingest: self.ingest,
            query: self.query,
            retention: self.retention,
            background: self.background,
            latest_storage_snapshot: self.latest_storage_snapshot,
        }
    }
}

#[derive(Debug, Default)]
struct BucketAccumulator {
    ingest: UsageTotals,
    query: UsageTotals,
    retention: UsageTotals,
    background: UsageTotals,
}

impl BucketAccumulator {
    fn apply(&mut self, record: &UsageLedgerRecord) {
        match record.category {
            UsageCategory::Ingest => self.ingest.apply(record),
            UsageCategory::Query => self.query.apply(record),
            UsageCategory::Retention => self.retention.apply(record),
            UsageCategory::Background => self.background.apply(record),
            UsageCategory::Storage => {}
        }
    }

    fn into_summary(self, tenant_id: String) -> UsageTenantBucketSummary {
        UsageTenantBucketSummary {
            tenant_id,
            ingest: self.ingest,
            query: self.query,
            retention: self.retention,
            background: self.background,
        }
    }
}

#[derive(Debug, Default)]
struct StorageAccumulator {
    series_total: u64,
    samples_total: u64,
    logical_storage_bytes: u64,
}

impl From<&UsageLedgerRecord> for UsageStorageSnapshot {
    fn from(record: &UsageLedgerRecord) -> Self {
        Self {
            tenant_id: record.tenant_id.clone(),
            reconciled_unix_ms: record.unix_ms,
            series_total: record.logical_storage_series,
            samples_total: record.logical_storage_samples,
            logical_storage_bytes: record.logical_storage_bytes,
        }
    }
}

fn usage_record_from_input(input: UsageRecordInput<'_>) -> UsageLedgerRecord {
    UsageLedgerRecord {
        seq: 0,
        unix_ms: unix_timestamp_millis(),
        tenant_id: input.tenant_id.to_string(),
        category: input.category,
        operation: input.operation.to_string(),
        source: input.source.to_string(),
        status: input.status.to_string(),
        request_units: input.request_units,
        result_units: input.result_units,
        rows: input.rows,
        metadata_updates: input.metadata_updates,
        exemplars_accepted: input.exemplars_accepted,
        exemplars_dropped: input.exemplars_dropped,
        histogram_series: input.histogram_series,
        matched_series: input.matched_series,
        tombstones_applied: input.tombstones_applied,
        duration_nanos: input.duration_nanos,
        request_bytes: input.request_bytes,
        logical_storage_series: input.logical_storage_series,
        logical_storage_samples: input.logical_storage_samples,
        logical_storage_bytes: input.logical_storage_bytes,
    }
}

fn load_usage_ledger(path: &Path) -> Result<UsageLedgerState, String> {
    if !path.exists() {
        return Ok(UsageLedgerState::default());
    }
    let mut file = File::open(path)
        .map_err(|err| format!("failed to open usage ledger {}: {err}", path.display()))?;
    let file_len = file
        .metadata()
        .map_err(|err| format!("failed to inspect usage ledger {}: {err}", path.display()))?
        .len();
    if file_len > 0 {
        file.seek(SeekFrom::End(-1)).map_err(|err| {
            format!(
                "failed to seek to the usage ledger tail {}: {err}",
                path.display()
            )
        })?;
        let mut final_byte = [0u8; 1];
        file.read_exact(&mut final_byte).map_err(|err| {
            format!(
                "failed to read the usage ledger tail {}: {err}",
                path.display()
            )
        })?;
        if final_byte[0] != b'\n' {
            return Err(format!(
                "usage ledger {} has an unterminated final record; refusing to append after a possibly torn write",
                path.display()
            ));
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|err| format!("failed to rewind usage ledger {}: {err}", path.display()))?;
    }

    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut max_seq = 0u64;
    let mut seen_sequences = BTreeSet::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|err| {
            format!(
                "failed to read usage ledger line {} from {}: {err}",
                index + 1,
                path.display()
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let persisted = serde_json::from_str::<PersistedUsageLedgerLine>(&line).map_err(|err| {
            format!(
                "failed to parse usage ledger line {} from {}: {err}",
                index + 1,
                path.display()
            )
        })?;
        let line_records = match persisted {
            PersistedUsageLedgerLine::Record(record) => vec![record],
            PersistedUsageLedgerLine::Batch(batch) => {
                if batch.magic != USAGE_LEDGER_BATCH_MAGIC {
                    return Err(format!(
                        "usage ledger {} has unsupported batch magic '{}' on line {}",
                        path.display(),
                        batch.magic,
                        index + 1
                    ));
                }
                if batch.schema_version != USAGE_LEDGER_BATCH_SCHEMA_VERSION {
                    return Err(format!(
                        "usage ledger {} has unsupported batch schema version {} on line {}",
                        path.display(),
                        batch.schema_version,
                        index + 1
                    ));
                }
                if batch.records.is_empty() {
                    return Err(format!(
                        "usage ledger {} has an empty record batch on line {}",
                        path.display(),
                        index + 1
                    ));
                }
                batch.records
            }
        };
        for record in line_records {
            if record.seq == 0 || !seen_sequences.insert(record.seq) {
                return Err(format!(
                    "usage ledger {} has an invalid or duplicate sequence {} on line {}",
                    path.display(),
                    record.seq,
                    index + 1
                ));
            }
            max_seq = max_seq.max(record.seq);
            records.push(record);
        }
    }
    let next_seq = max_seq.checked_add(1).ok_or_else(|| {
        format!(
            "usage ledger {} exhausted its sequence space",
            path.display()
        )
    })?;
    Ok(UsageLedgerState { next_seq, records })
}

fn append_usage_record_unbudgeted(
    path: &Path,
    file: &mut File,
    encoded: &[u8],
    initial_len: u64,
) -> Result<(), String> {
    let append_result = file.write_all(encoded).and_then(|()| file.flush());
    if let Err(append_err) = append_result {
        let rollback_result = file.set_len(initial_len).and_then(|()| file.flush());
        return match rollback_result {
            Ok(()) => Err(format!(
                "failed to append usage ledger {}: {append_err}",
                path.display()
            )),
            Err(rollback_err) => Err(format!(
                "failed to append usage ledger {}: {append_err}; failed to roll back partial record: {rollback_err}",
                path.display()
            )),
        };
    }
    Ok(())
}

fn ledger_status_from_records(
    ledger_path: Option<&Path>,
    records: &[UsageLedgerRecord],
    health: &UsageLedgerHealth,
) -> UsageLedgerStatus {
    let mut tenants = BTreeSet::new();
    let mut storage_reconciliations_total = 0u64;
    for record in records {
        tenants.insert(record.tenant_id.clone());
        if record.category == UsageCategory::Storage {
            storage_reconciliations_total = storage_reconciliations_total.saturating_add(1);
        }
    }
    UsageLedgerStatus {
        durable: ledger_path.is_some(),
        ledger_path: ledger_path.map(|path| path.display().to_string()),
        records_total: records.len() as u64,
        tenant_count: tenants.len() as u64,
        last_sequence: records.iter().map(|record| record.seq).max().unwrap_or(0),
        last_record_unix_ms: records.last().map(|record| record.unix_ms),
        storage_reconciliations_total,
        record_failures_total: health.record_failures_total,
        last_record_error_code: health.last_record_error_code.clone(),
    }
}

fn matches_tenant_filter(record: &UsageLedgerRecord, tenant_id: Option<&str>) -> bool {
    tenant_id
        .map(|tenant_id| record.tenant_id == tenant_id)
        .unwrap_or(true)
}

fn matches_time_filter(
    record: &UsageLedgerRecord,
    start_unix_ms: Option<u64>,
    end_unix_ms: Option<u64>,
) -> bool {
    if start_unix_ms.is_some_and(|start| record.unix_ms < start) {
        return false;
    }
    if end_unix_ms.is_some_and(|end| record.unix_ms > end) {
        return false;
    }
    true
}

fn tenant_id_for_metric_series(series: &MetricSeries) -> String {
    series
        .labels
        .iter()
        .find(|label| label.name == tenant::TENANT_LABEL)
        .map(|label| label.value.clone())
        .unwrap_or_else(|| tenant::DEFAULT_TENANT_ID.to_string())
}

fn estimated_series_bytes(series: &MetricSeries, point_count: u64) -> u64 {
    ESTIMATED_SERIES_OVERHEAD_BYTES
        .saturating_add(series.name.len() as u64)
        .saturating_add(labels_bytes(&series.labels))
        .saturating_add(point_count.saturating_mul(ESTIMATED_SAMPLE_BYTES))
}

fn labels_bytes(labels: &[Label]) -> u64 {
    labels
        .iter()
        .map(|label| label.name.len() as u64 + label.value.len() as u64)
        .sum()
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant;
    use std::sync::{Condvar, Mutex as StdMutex};
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;
    use tsink::{
        DataPoint, Label, LocalDiskLimits, QueryOptions, Row, SeriesPoints, StorageBuilder,
        TimestampPrecision,
    };

    #[derive(Debug, Default)]
    struct ReconciliationScanGate {
        released: StdMutex<bool>,
        wake: Condvar,
    }

    impl ReconciliationScanGate {
        fn wait(&self) {
            let mut released = self
                .released
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*released {
                released = self
                    .wake
                    .wait(released)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn release(&self) {
            *self
                .released
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            self.wake.notify_all();
        }
    }

    struct ObservedReconciliationStorage {
        inner: Arc<dyn Storage>,
        list_metrics_entered: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
        list_metrics_gate: Option<Arc<ReconciliationScanGate>>,
        select_many_completed: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl Storage for ObservedReconciliationStorage {
        fn insert_rows(&self, rows: &[Row]) -> tsink::Result<()> {
            self.inner.insert_rows(rows)
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

        fn select_many(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
        ) -> tsink::Result<Vec<SeriesPoints>> {
            let result = self.inner.select_many(series, start, end);
            if let Some(sender) = self
                .select_many_completed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
            result
        }

        fn select_with_options(
            &self,
            metric: &str,
            opts: QueryOptions,
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

        fn list_metrics(&self) -> tsink::Result<Vec<MetricSeries>> {
            if let Some(sender) = self
                .list_metrics_entered
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
            if let Some(gate) = self.list_metrics_gate.as_ref() {
                gate.wait();
            }
            self.inner.list_metrics()
        }

        fn close(&self) -> tsink::Result<()> {
            self.inner.close()
        }
    }

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
    }

    fn make_two_tenant_storage() -> Arc<dyn Storage> {
        let storage = make_storage();
        let team_a = tenant::scope_rows_for_tenant(
            vec![
                Row::with_labels(
                    "cpu_usage",
                    vec![Label::new("host", "a")],
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![Label::new("host", "b")],
                    DataPoint::new(1, 2.0),
                ),
            ],
            "team-a",
        )
        .expect("rows should scope");
        let team_b = tenant::scope_rows_for_tenant(
            vec![Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "c")],
                DataPoint::new(1, 3.0),
            )],
            "team-b",
        )
        .expect("rows should scope");
        storage
            .insert_rows(&team_a)
            .expect("team-a rows should insert");
        storage
            .insert_rows(&team_b)
            .expect("team-b rows should insert");
        storage
    }

    #[test]
    fn usage_ledger_persists_and_reports_records() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        let mut ingest = UsageRecordInput::success(
            "team-a",
            UsageCategory::Ingest,
            "remote_write",
            "/api/v1/write",
        );
        ingest.rows = 5;
        ingest.request_units = 5;
        ingest.request_bytes = 128;
        accounting
            .record(ingest)
            .expect("ingest record should write");

        let mut query = UsageRecordInput::success(
            "team-a",
            UsageCategory::Query,
            "instant_query",
            "/api/v1/query",
        );
        query.request_units = 1;
        query.result_units = 3;
        accounting.record(query).expect("query record should write");

        drop(accounting);

        let reopened = UsageAccounting::open(Some(dir.path())).expect("usage store should reopen");
        let report = reopened.report(Some("team-a"), None, None, UsageBucketWidth::Hour);
        assert_eq!(report.journal.records_total, 2);
        assert_eq!(report.tenants.len(), 1);
        assert_eq!(report.tenants[0].tenant_id, "team-a");
        assert_eq!(report.tenants[0].ingest.rows, 5);
        assert_eq!(report.tenants[0].query.result_units, 3);
        assert_eq!(report.buckets.len(), 1);
        assert_eq!(
            reopened
                .export_records(Some("team-a"), None, None)
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn budget_rejection_does_not_publish_or_consume_sequence() {
        let dir = tempdir().expect("temp dir should build");
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(1),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let accounting =
            UsageAccounting::open_with_disk_budget(Some(dir.path()), Some(Arc::clone(&budget)))
                .expect("usage store should open");

        let err = accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Ingest,
                "remote_write",
                "/api/v1/write",
            ))
            .expect_err("record should exceed the tiny disk quota");
        assert!(
            err.to_string().contains("Local disk quota exceeded"),
            "{err}"
        );
        assert!(accounting.export_records(None, None, None).is_empty());
        let status = accounting.ledger_status();
        assert_eq!(status.last_sequence, 0);
        assert_eq!(status.record_failures_total, 1);
        assert_eq!(
            status.last_record_error_code.as_deref(),
            Some("usage_ledger_disk_quota_exceeded")
        );

        let ledger_path = dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE);
        assert_eq!(
            fs::metadata(&ledger_path)
                .expect("ledger metadata should load")
                .len(),
            0
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 1);

        drop(accounting);
        drop(budget);
        let reopened = UsageAccounting::open(Some(dir.path())).expect("usage store should reopen");
        let first = reopened
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Ingest,
                "remote_write",
                "/api/v1/write",
            ))
            .expect("record should succeed without the tiny budget");
        assert_eq!(first.seq, 1);
    }

    #[test]
    fn budget_accounts_exact_usage_ledger_bytes_as_server_state() {
        let dir = tempdir().expect("temp dir should build");
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(64 * 1024),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let accounting =
            UsageAccounting::open_with_disk_budget(Some(dir.path()), Some(Arc::clone(&budget)))
                .expect("usage store should open");
        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Query,
                "instant_query",
                "/api/v1/query",
            ))
            .expect("record should fit the disk quota");

        let ledger_bytes = fs::metadata(dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE))
            .expect("ledger metadata should load")
            .len();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, ledger_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.reservation_overruns_total, 0);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == DiskCategory::ServerState)
                .map(|usage| usage.bytes),
            Some(ledger_bytes)
        );
    }

    #[test]
    fn concurrent_records_keep_file_and_sequence_order_across_restart() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");

        thread::scope(|scope| {
            let handles = (0..32)
                .map(|_| {
                    let accounting = Arc::clone(&accounting);
                    scope.spawn(move || {
                        accounting.record(UsageRecordInput::success(
                            "team-a",
                            UsageCategory::Background,
                            "concurrent_test",
                            "test",
                        ))
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle
                    .join()
                    .expect("recording thread should not panic")
                    .expect("record should append");
            }
        });

        let in_memory_sequences = accounting
            .export_records(None, None, None)
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>();
        assert_eq!(in_memory_sequences, (1..=32).collect::<Vec<_>>());
        drop(accounting);

        let reopened = UsageAccounting::open(Some(dir.path())).expect("usage store should reopen");
        let disk_sequences = reopened
            .export_records(None, None, None)
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>();
        assert_eq!(disk_sequences, in_memory_sequences);
        let next = reopened
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "after_restart",
                "test",
            ))
            .expect("post-restart record should append");
        assert_eq!(next.seq, 33);
    }

    #[test]
    fn reopen_accepts_legacy_out_of_order_unique_sequences() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        for operation in ["first", "second"] {
            accounting
                .record(UsageRecordInput::success(
                    "team-a",
                    UsageCategory::Background,
                    operation,
                    "test",
                ))
                .expect("record should append");
        }
        drop(accounting);

        let ledger_path = dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE);
        let contents = fs::read_to_string(&ledger_path).expect("ledger should read");
        let lines = contents.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        fs::write(&ledger_path, format!("{}\n{}\n", lines[1], lines[0]))
            .expect("legacy ordering should write");

        let reopened = UsageAccounting::open(Some(dir.path()))
            .expect("legacy out-of-order ledger should reopen");
        assert_eq!(reopened.ledger_status().last_sequence, 2);
        let next = reopened
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "after_upgrade",
                "test",
            ))
            .expect("new sequence should append after the legacy maximum");
        assert_eq!(next.seq, 3);
    }

    #[test]
    fn reopen_accepts_legacy_sequence_gaps_and_continues_after_maximum() {
        let dir = tempdir().expect("temp dir should build");
        let ledger_dir = dir.path().join(USAGE_LEDGER_DIR);
        fs::create_dir_all(&ledger_dir).expect("usage ledger directory should build");
        let ledger_path = ledger_dir.join(USAGE_LEDGER_FILE);
        let mut first = usage_record_from_input(UsageRecordInput::success(
            "team-a",
            UsageCategory::Background,
            "legacy_first",
            "test",
        ));
        first.seq = 1;
        let mut after_gap = usage_record_from_input(UsageRecordInput::success(
            "team-a",
            UsageCategory::Background,
            "legacy_after_gap",
            "test",
        ));
        after_gap.seq = 3;
        fs::write(
            &ledger_path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first).expect("first legacy record should encode"),
                serde_json::to_string(&after_gap)
                    .expect("legacy record after sequence gap should encode")
            ),
        )
        .expect("legacy sequence-gap fixture should write");

        let accounting = UsageAccounting::open(Some(dir.path()))
            .expect("legacy sequence-gap ledger should reopen");
        assert_eq!(
            accounting
                .export_records(None, None, None)
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        let next = accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "after_legacy_gap",
                "test",
            ))
            .expect("new record should continue after the legacy maximum");
        assert_eq!(next.seq, 4);
    }

    #[test]
    fn reopen_rejects_unterminated_final_record() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Ingest,
                "remote_write",
                "/api/v1/write",
            ))
            .expect("record should append");
        drop(accounting);

        let ledger_path = dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE);
        let ledger_len = fs::metadata(&ledger_path)
            .expect("ledger metadata should load")
            .len();
        OpenOptions::new()
            .write(true)
            .open(&ledger_path)
            .expect("ledger should open for truncation")
            .set_len(ledger_len - 1)
            .expect("final newline should truncate");

        let err = UsageAccounting::open(Some(dir.path()))
            .expect_err("unterminated ledger must not reopen for append");
        assert!(err.contains("unterminated final record"), "{err}");
    }

    #[test]
    fn storage_reconciliation_captures_per_tenant_snapshots() {
        let accounting = UsageAccounting::open(None).expect("usage store should open");
        let storage = make_two_tenant_storage();

        let snapshots = accounting
            .reconcile_storage(&storage)
            .expect("storage reconciliation should succeed");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].tenant_id, "team-a");
        assert_eq!(snapshots[0].series_total, 2);
        assert_eq!(snapshots[0].samples_total, 2);
        assert_eq!(snapshots[1].tenant_id, "team-b");
        assert_eq!(snapshots[1].series_total, 1);
        assert_eq!(snapshots[1].samples_total, 1);
        assert!(accounting
            .latest_storage_snapshot_for("team-a")
            .is_some_and(|snapshot| snapshot.samples_total == 2));
    }

    #[test]
    fn failed_multi_tenant_reconciliation_publishes_no_prefix_and_retries_once() {
        let dir = tempdir().expect("temp dir should build");
        let storage = make_two_tenant_storage();
        let budget = LocalDiskBudget::open(
            dir.path(),
            LocalDiskLimits {
                max_bytes: Some(1),
                ..LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let accounting =
            UsageAccounting::open_with_disk_budget(Some(dir.path()), Some(Arc::clone(&budget)))
                .expect("usage store should open");

        let err = accounting
            .reconcile_storage(&storage)
            .expect_err("the reconciliation batch should exceed the tiny quota");
        assert!(matches!(err, UsageAccountingError::Disk(_)), "{err}");
        assert!(accounting.export_records(None, None, None).is_empty());
        let status = accounting.ledger_status();
        assert_eq!(status.records_total, 0);
        assert_eq!(status.last_sequence, 0);
        assert_eq!(status.storage_reconciliations_total, 0);
        assert_eq!(status.record_failures_total, 1);

        let ledger_path = dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE);
        assert_eq!(
            fs::metadata(&ledger_path)
                .expect("ledger metadata should load")
                .len(),
            0
        );
        drop(accounting);
        drop(budget);

        let reopened = UsageAccounting::open(Some(dir.path())).expect("usage store should reopen");
        let snapshots = reopened
            .reconcile_storage(&storage)
            .expect("the reconciliation retry should succeed");
        assert_eq!(snapshots.len(), 2);
        let records = reopened.export_records(None, None, None);
        assert_eq!(records.len(), 2);
        assert_eq!(
            records.iter().map(|record| record.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(records
            .iter()
            .all(|record| record.category == UsageCategory::Storage));
        let contents = fs::read_to_string(&ledger_path).expect("ledger should read");
        assert_eq!(
            contents.lines().count(),
            1,
            "one reconciliation must use one ledger batch frame"
        );
        drop(reopened);
        let reopened = UsageAccounting::open(Some(dir.path())).expect("batch should reopen");
        assert_eq!(reopened.ledger_status().records_total, 2);
    }

    #[test]
    fn torn_multi_record_frame_cannot_reopen_as_a_committed_tenant_prefix() {
        let dir = tempdir().expect("temp dir should build");
        let storage = make_two_tenant_storage();
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        accounting
            .reconcile_storage(&storage)
            .expect("reconciliation should persist");
        drop(accounting);

        let ledger_path = dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE);
        let contents = fs::read_to_string(&ledger_path).expect("ledger should read");
        assert_eq!(contents.lines().count(), 1);
        let second_record_boundary = contents
            .find("},{\"seq\":2")
            .expect("the batch should contain its second record");
        let torn_prefix = format!("{}\n", &contents[..=second_record_boundary]);
        fs::write(&ledger_path, torn_prefix).expect("torn batch prefix should write");

        let err = UsageAccounting::open(Some(dir.path()))
            .expect_err("a complete first record without its batch commit must not reopen");
        assert!(err.contains("failed to parse usage ledger line 1"), "{err}");
    }

    #[tokio::test]
    async fn concurrent_async_records_keep_sequence_order() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        let mut tasks = Vec::new();
        for index in 0..16 {
            let accounting = Arc::clone(&accounting);
            tasks.push(tokio::spawn(async move {
                accounting
                    .record_async(UsageRecordInput::success(
                        "team-a",
                        UsageCategory::Background,
                        "async_test",
                        "test",
                    ))
                    .await
                    .map(|record| (index, record.seq))
            }));
        }
        let mut sequences = Vec::new();
        for task in tasks {
            sequences.push(
                task.await
                    .expect("record task should not panic")
                    .expect("record should persist")
                    .1,
            );
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=16).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn append_task_join_failure_is_indeterminate_persistence() {
        let join_err = tokio::task::spawn_blocking(|| panic!("injected append-task panic"))
            .await
            .expect_err("injected append task should panic");
        let err = append_task_join_failure("append", join_err);
        assert!(matches!(err, UsageAccountingError::Persistence(_)));
        assert!(err.is_persistence_failure());
        assert_eq!(err.status_code(), "usage_ledger_persistence_error");
    }

    #[tokio::test]
    async fn blocked_reconciliation_scan_queues_a_second_scan_but_not_async_usage() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        let gate = Arc::new(ReconciliationScanGate::default());
        let (scan_entered_tx, scan_entered_rx) = tokio::sync::oneshot::channel();
        let storage: Arc<dyn Storage> = Arc::new(ObservedReconciliationStorage {
            inner: make_two_tenant_storage(),
            list_metrics_entered: StdMutex::new(Some(scan_entered_tx)),
            list_metrics_gate: Some(Arc::clone(&gate)),
            select_many_completed: StdMutex::new(None),
        });

        let reconciliation = {
            let accounting = Arc::clone(&accounting);
            tokio::spawn(async move { accounting.reconcile_storage_async(storage).await })
        };
        tokio::time::timeout(Duration::from_secs(5), scan_entered_rx)
            .await
            .expect("reconciliation should enter its storage scan")
            .expect("scan notification sender should remain alive");

        let (second_scan_entered_tx, second_scan_entered_rx) = tokio::sync::oneshot::channel();
        let second_storage: Arc<dyn Storage> = Arc::new(ObservedReconciliationStorage {
            inner: make_two_tenant_storage(),
            list_metrics_entered: StdMutex::new(Some(second_scan_entered_tx)),
            list_metrics_gate: None,
            select_many_completed: StdMutex::new(None),
        });
        let second_reconciliation = {
            let accounting = Arc::clone(&accounting);
            tokio::spawn(async move { accounting.reconcile_storage_async(second_storage).await })
        };
        let second_scan_while_first_is_blocked =
            tokio::time::timeout(Duration::from_millis(500), second_scan_entered_rx).await;

        let ordinary_record = tokio::time::timeout(
            Duration::from_secs(5),
            accounting.record_async(UsageRecordInput::success(
                "team-a",
                UsageCategory::Query,
                "during_reconciliation_scan",
                "test",
            )),
        )
        .await;
        assert!(
            !reconciliation.is_finished(),
            "the reconciliation must still be blocked in its scan"
        );
        gate.release();
        let snapshots = tokio::time::timeout(Duration::from_secs(5), reconciliation)
            .await
            .expect("reconciliation should finish after releasing its scan")
            .expect("reconciliation task should not panic")
            .expect("reconciliation should append");
        let second_snapshots = tokio::time::timeout(Duration::from_secs(5), second_reconciliation)
            .await
            .expect("queued reconciliation should finish after the first")
            .expect("queued reconciliation task should not panic")
            .expect("queued reconciliation should append");
        let ordinary_record = ordinary_record
            .expect("ordinary usage append must not wait for the reconciliation scan")
            .expect("ordinary usage append should succeed");

        assert!(
            second_scan_while_first_is_blocked.is_err(),
            "a second reconciliation must not enter its scan while the first is blocked"
        );
        assert_eq!(ordinary_record.seq, 1);
        assert_eq!(snapshots.len(), 2);
        assert_eq!(second_snapshots.len(), 2);
        assert_eq!(accounting.ledger_status().records_total, 5);
    }

    #[tokio::test]
    async fn async_reconciliation_batch_remains_contiguous_under_concurrent_appends() {
        const CONCURRENT_RECORDS: usize = 16;

        let dir = tempdir().expect("temp dir should build");
        let accounting = UsageAccounting::open(Some(dir.path())).expect("usage store should open");
        let held_permit = Arc::clone(&accounting.inner.async_append_serialization)
            .acquire_owned()
            .await
            .expect("append coordinator should be open");
        let (scan_completed_tx, scan_completed_rx) = tokio::sync::oneshot::channel();
        let storage: Arc<dyn Storage> = Arc::new(ObservedReconciliationStorage {
            inner: make_two_tenant_storage(),
            list_metrics_entered: StdMutex::new(None),
            list_metrics_gate: None,
            select_many_completed: StdMutex::new(Some(scan_completed_tx)),
        });
        let reconciliation = {
            let accounting = Arc::clone(&accounting);
            tokio::spawn(async move { accounting.reconcile_storage_async(storage).await })
        };
        tokio::time::timeout(Duration::from_secs(5), scan_completed_rx)
            .await
            .expect("reconciliation scan should complete")
            .expect("scan completion sender should remain alive");

        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut appends = Vec::new();
        for index in 0..CONCURRENT_RECORDS {
            let accounting = Arc::clone(&accounting);
            let ready_tx = ready_tx.clone();
            appends.push(tokio::spawn(async move {
                ready_tx
                    .send(())
                    .expect("append readiness receiver should remain alive");
                accounting
                    .record_async(UsageRecordInput::success(
                        "team-a",
                        UsageCategory::Background,
                        "concurrent_with_reconciliation",
                        "test",
                    ))
                    .await
                    .map(|record| (index, record.seq))
            }));
        }
        drop(ready_tx);
        for _ in 0..CONCURRENT_RECORDS {
            ready_rx
                .recv()
                .await
                .expect("every concurrent append should report readiness");
        }
        tokio::task::yield_now().await;
        drop(held_permit);

        let snapshots = tokio::time::timeout(Duration::from_secs(5), reconciliation)
            .await
            .expect("reconciliation should finish after append admission opens")
            .expect("reconciliation task should not panic")
            .expect("reconciliation should append");
        assert_eq!(snapshots.len(), 2);
        for append in appends {
            append
                .await
                .expect("ordinary append task should not panic")
                .expect("ordinary append should succeed");
        }

        let records = accounting.export_records(None, None, None);
        assert_eq!(records.len(), CONCURRENT_RECORDS + 2);
        assert_eq!(
            records.iter().map(|record| record.seq).collect::<Vec<_>>(),
            (1..=(CONCURRENT_RECORDS as u64 + 2)).collect::<Vec<_>>()
        );
        let storage_sequences = records
            .iter()
            .filter(|record| record.category == UsageCategory::Storage)
            .map(|record| record.seq)
            .collect::<Vec<_>>();
        assert_eq!(storage_sequences.len(), 2);
        assert_eq!(storage_sequences[1], storage_sequences[0] + 1);

        let ledger_path = dir.path().join(USAGE_LEDGER_DIR).join(USAGE_LEDGER_FILE);
        let contents = fs::read_to_string(&ledger_path).expect("usage ledger should read");
        let batches = contents
            .lines()
            .filter_map(|line| {
                match serde_json::from_str::<PersistedUsageLedgerLine>(line)
                    .expect("every persisted ledger line should parse")
                {
                    PersistedUsageLedgerLine::Record(_) => None,
                    PersistedUsageLedgerLine::Batch(batch) => Some(batch),
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].records.len(), 2);
        assert_eq!(
            batches[0]
                .records
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            storage_sequences
        );

        drop(accounting);
        let reopened = UsageAccounting::open(Some(dir.path())).expect("usage store should reopen");
        assert_eq!(
            reopened
                .export_records(None, None, None)
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            (1..=(CONCURRENT_RECORDS as u64 + 2)).collect::<Vec<_>>()
        );
    }
}
