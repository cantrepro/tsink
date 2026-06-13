use crate::tenant;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tsink::{Label, MetricSeries, Storage};

const USAGE_LEDGER_DIR: &str = "usage-accounting";
const USAGE_LEDGER_FILE: &str = "ledger.ndjson";
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
}

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

#[derive(Debug)]
pub struct UsageAccounting {
    ledger_path: Option<PathBuf>,
    writer: Mutex<Option<File>>,
    state: Mutex<UsageLedgerState>,
}

impl UsageAccounting {
    pub fn open(data_path: Option<&Path>) -> Result<Arc<Self>, String> {
        let (ledger_path, writer, state) = match data_path {
            Some(data_path) => {
                let dir = data_path.join(USAGE_LEDGER_DIR);
                fs::create_dir_all(&dir).map_err(|err| {
                    format!(
                        "failed to create usage ledger directory {}: {err}",
                        dir.display()
                    )
                })?;
                let ledger_path = dir.join(USAGE_LEDGER_FILE);
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
            ledger_path,
            writer: Mutex::new(writer),
            state: Mutex::new(state),
        }))
    }

    pub fn ledger_status(&self) -> UsageLedgerStatus {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ledger_status_from_records(self.ledger_path.as_deref(), &state.records)
    }

    pub fn record(&self, input: UsageRecordInput<'_>) -> Result<UsageLedgerRecord, String> {
        let seq = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let seq = state.next_seq;
            state.next_seq = state.next_seq.saturating_add(1);
            seq
        };

        let record = UsageLedgerRecord {
            seq,
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
        };

        if self.ledger_path.is_some() {
            let encoded = serde_json::to_vec(&record)
                .map_err(|err| format!("failed to encode usage record: {err}"))?;
            let mut writer = self
                .writer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(file) = writer.as_mut() {
                file.write_all(&encoded).map_err(|err| {
                    format!(
                        "failed to append usage ledger {}: {err}",
                        self.ledger_path
                            .as_ref()
                            .map(|path| path.display().to_string())
                            .unwrap_or_default()
                    )
                })?;
                file.write_all(b"\n")
                    .map_err(|err| format!("failed to write usage ledger newline: {err}"))?;
                file.flush()
                    .map_err(|err| format!("failed to flush usage ledger: {err}"))?;
            }
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.records.push(record.clone());
        Ok(record)
    }

    pub fn report(
        &self,
        tenant_id: Option<&str>,
        start_unix_ms: Option<u64>,
        end_unix_ms: Option<u64>,
        bucket_width: UsageBucketWidth,
    ) -> UsageReport {
        let state = self
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

        UsageReport {
            filter: UsageReportFilter {
                tenant_id: tenant_id.map(str::to_string),
                start_unix_ms,
                end_unix_ms,
                bucket_width,
            },
            journal: ledger_status_from_records(self.ledger_path.as_deref(), &state.records),
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
    ) -> Result<Vec<UsageStorageSnapshot>, String> {
        let metrics = storage
            .list_metrics()
            .map_err(|err| format!("usage storage reconciliation failed to list metrics: {err}"))?;
        let mut per_tenant = BTreeMap::<String, StorageAccumulator>::new();

        for chunk in metrics.chunks(STORAGE_RECONCILE_BATCH_SIZE) {
            let selected = storage
                .select_many(chunk, i64::MIN, i64::MAX)
                .map_err(|err| {
                    format!("usage storage reconciliation failed to read points: {err}")
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
        let mut snapshots = Vec::with_capacity(per_tenant.len());
        for (tenant_id, acc) in per_tenant {
            let mut record = UsageRecordInput::success(
                &tenant_id,
                UsageCategory::Storage,
                "reconcile_storage",
                "admin",
            );
            record.logical_storage_series = acc.series_total;
            record.logical_storage_samples = acc.samples_total;
            record.logical_storage_bytes = acc.logical_storage_bytes;
            let recorded = self.record(record)?;
            snapshots.push(UsageStorageSnapshot {
                tenant_id,
                reconciled_unix_ms: recorded.unix_ms.max(reconciled_unix_ms),
                series_total: acc.series_total,
                samples_total: acc.samples_total,
                logical_storage_bytes: acc.logical_storage_bytes,
            });
        }
        snapshots.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        Ok(snapshots)
    }
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

fn load_usage_ledger(path: &Path) -> Result<UsageLedgerState, String> {
    if !path.exists() {
        return Ok(UsageLedgerState::default());
    }
    let file = File::open(path)
        .map_err(|err| format!("failed to open usage ledger {}: {err}", path.display()))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut next_seq = 1u64;
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
        let record = serde_json::from_str::<UsageLedgerRecord>(&line).map_err(|err| {
            format!(
                "failed to parse usage ledger line {} from {}: {err}",
                index + 1,
                path.display()
            )
        })?;
        next_seq = next_seq.max(record.seq.saturating_add(1));
        records.push(record);
    }
    Ok(UsageLedgerState { next_seq, records })
}

fn ledger_status_from_records(
    ledger_path: Option<&Path>,
    records: &[UsageLedgerRecord],
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
        last_sequence: records.last().map(|record| record.seq).unwrap_or(0),
        last_record_unix_ms: records.last().map(|record| record.unix_ms),
        storage_reconciliations_total,
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
    use tempfile::tempdir;
    use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision};

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
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
    }

    #[test]
    fn storage_reconciliation_captures_per_tenant_snapshots() {
        let accounting = UsageAccounting::open(None).expect("usage store should open");
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
}
