use crate::tenant;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::{
    modeled_write_batch_input_bytes, DiskCategory, HistogramBucketSpan, Label, LocalDiskBudget,
    MetricSeries, NativeHistogram, QueryCancellationToken, QueryExecution,
    QueryExecutionAccounting, QueryExecutionSnapshot, QueryMemoryReservation, QueryRowsScanOptions,
    QueryWorkLimits, Row, SeriesSelection, Storage, Value,
};
use xxhash_rust::xxh64::Xxh64;

const USAGE_LEDGER_DIR: &str = "usage-accounting";
const USAGE_LEDGER_FILE: &str = "ledger.ndjson";
const USAGE_LEDGER_BATCH_MAGIC: &str = "tsink-usage-ledger-batch";
const USAGE_LEDGER_BATCH_SCHEMA_VERSION: u16 = 1;
const ESTIMATED_SERIES_OVERHEAD_BYTES: u64 = 64;
const ESTIMATED_SAMPLE_BYTES: u64 = 16;
const STORAGE_RECONCILE_BATCH_SIZE: usize = 128;
const STORAGE_RECONCILE_PAGE_MAX_ROWS: usize = 128;
const STORAGE_RECONCILE_PAGE_MAX_BYTES: usize = 8 * 1024 * 1024;
const STORAGE_RECONCILE_MAX_MANIFEST_SERIES: usize = 1_000_000;
const STORAGE_RECONCILE_PAGE_MEMORY_MULTIPLIER: usize = 4;
const STORAGE_RECONCILE_OPERATION_MEMORY_MULTIPLIER: usize = 4;
const STORAGE_RECONCILE_TOTAL_BYTES_MULTIPLIER: usize = 8;
const STORAGE_RECONCILE_MAX_ROWS_PER_PASS: u64 = 1_000_000;
const STORAGE_RECONCILE_MAX_SAMPLES_SCANNED_PER_PAGE: u64 = 1_000_000;
const STORAGE_RECONCILE_MAX_SAMPLES_SCANNED_PER_PASS: u64 = 4_000_000;
const STORAGE_RECONCILE_MAX_PAGES_PER_PASS: u64 = 100_000;
const STORAGE_RECONCILE_OPERATION_WALL_TIME: Duration = Duration::from_secs(120);
const STORAGE_RECONCILE_MAX_SNAPSHOT_ATTEMPTS: usize = 2;
const STORAGE_RECONCILE_PASSES_PER_ATTEMPT: u64 = 2;
const STORAGE_RECONCILE_MANIFESTS_PER_ATTEMPT: u64 = 2;
const STORAGE_RECONCILE_FINGERPRINT_SEED_A: u64 = 0x9e37_79b1_85eb_ca87;
const STORAGE_RECONCILE_FINGERPRINT_SEED_B: u64 = 0xc2b2_ae3d_27d4_eb4f;
const STORAGE_RECONCILE_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const USAGE_STATUS_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const USAGE_LEDGER_STARTUP_READER_MAX_BYTES: usize = 8 * 1024;
const USAGE_LEDGER_STARTUP_INDEX_ALLOWANCE_BYTES: usize = 64;

pub const DEFAULT_USAGE_LEDGER_RECENT_RECORDS: usize = 8_192;
pub const DEFAULT_USAGE_LEDGER_MAX_TENANTS: usize = 4_096;
pub const DEFAULT_USAGE_LEDGER_MAX_RECORD_BYTES: usize = 64 * 1024;
pub const DEFAULT_USAGE_LEDGER_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_USAGE_LEDGER_MAX_LINE_BYTES: usize = DEFAULT_USAGE_LEDGER_MAX_FRAME_BYTES + 1;
pub const DEFAULT_USAGE_LEDGER_MAX_BATCH_RECORDS: usize = 4_096;
pub const DEFAULT_USAGE_LEDGER_STARTUP_SCRATCH_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_USAGE_LEDGER_MAX_SEQUENCE_RANGES: usize = 4_096;
pub const DEFAULT_USAGE_REPORT_RECORDS: usize = 1_000;
pub const DEFAULT_USAGE_REPORT_MAX_RECORDS: usize = 4_096;
pub const DEFAULT_USAGE_REPORT_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_USAGE_EXPORT_RECORDS: usize = 1_000;
pub const DEFAULT_USAGE_EXPORT_MAX_RECORDS: usize = 4_096;
pub const DEFAULT_USAGE_EXPORT_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageLedgerLimits {
    pub recent_records: usize,
    pub max_tenants: usize,
    pub max_record_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_line_bytes: usize,
    pub max_batch_records: usize,
    pub startup_scratch_bytes: usize,
    pub max_sequence_ranges: usize,
    pub report_default_records: usize,
    pub report_max_records: usize,
    pub report_max_response_bytes: usize,
    pub export_default_records: usize,
    pub export_max_records: usize,
    pub export_max_response_bytes: usize,
}

impl Default for UsageLedgerLimits {
    fn default() -> Self {
        Self {
            recent_records: DEFAULT_USAGE_LEDGER_RECENT_RECORDS,
            max_tenants: DEFAULT_USAGE_LEDGER_MAX_TENANTS,
            max_record_bytes: DEFAULT_USAGE_LEDGER_MAX_RECORD_BYTES,
            max_frame_bytes: DEFAULT_USAGE_LEDGER_MAX_FRAME_BYTES,
            max_line_bytes: DEFAULT_USAGE_LEDGER_MAX_LINE_BYTES,
            max_batch_records: DEFAULT_USAGE_LEDGER_MAX_BATCH_RECORDS,
            startup_scratch_bytes: DEFAULT_USAGE_LEDGER_STARTUP_SCRATCH_BYTES,
            max_sequence_ranges: DEFAULT_USAGE_LEDGER_MAX_SEQUENCE_RANGES,
            report_default_records: DEFAULT_USAGE_REPORT_RECORDS,
            report_max_records: DEFAULT_USAGE_REPORT_MAX_RECORDS,
            report_max_response_bytes: DEFAULT_USAGE_REPORT_MAX_RESPONSE_BYTES,
            export_default_records: DEFAULT_USAGE_EXPORT_RECORDS,
            export_max_records: DEFAULT_USAGE_EXPORT_MAX_RECORDS,
            export_max_response_bytes: DEFAULT_USAGE_EXPORT_MAX_RESPONSE_BYTES,
        }
    }
}

impl UsageLedgerLimits {
    pub fn validate(self) -> Result<Self, String> {
        for (name, value) in [
            ("recent records", self.recent_records),
            ("maximum tenants", self.max_tenants),
            ("maximum record bytes", self.max_record_bytes),
            ("maximum frame bytes", self.max_frame_bytes),
            ("maximum line bytes", self.max_line_bytes),
            ("maximum batch records", self.max_batch_records),
            ("startup scratch bytes", self.startup_scratch_bytes),
            ("maximum sequence ranges", self.max_sequence_ranges),
            ("report default records", self.report_default_records),
            ("report maximum records", self.report_max_records),
            (
                "report maximum response bytes",
                self.report_max_response_bytes,
            ),
            ("export default records", self.export_default_records),
            ("export maximum records", self.export_max_records),
            (
                "export maximum response bytes",
                self.export_max_response_bytes,
            ),
        ] {
            if value == 0 {
                return Err(format!("usage ledger {name} must be greater than zero"));
            }
        }
        if self.max_frame_bytes < self.max_record_bytes {
            return Err(
                "usage ledger maximum frame bytes must be at least maximum record bytes"
                    .to_string(),
            );
        }
        let frame_line_bytes = self.max_frame_bytes.checked_add(1).ok_or_else(|| {
            "usage ledger maximum frame bytes leaves no room for a newline".to_string()
        })?;
        if self.max_line_bytes < frame_line_bytes {
            return Err(
                "usage ledger maximum line bytes must fit the maximum frame plus its newline"
                    .to_string(),
            );
        }
        if self.max_batch_records < self.max_tenants {
            return Err(
                "usage ledger maximum batch records must be at least maximum tenants so storage reconciliation remains atomic"
                    .to_string(),
            );
        }
        if self.report_default_records > self.report_max_records {
            return Err(
                "usage report default records must not exceed its maximum records".to_string(),
            );
        }
        if self.export_default_records > self.export_max_records {
            return Err(
                "usage export default records must not exceed its maximum records".to_string(),
            );
        }
        let record_line_bytes = self.max_record_bytes.checked_add(1).ok_or_else(|| {
            "usage ledger maximum record bytes leaves no room for a newline".to_string()
        })?;
        if self.export_max_response_bytes < record_line_bytes {
            return Err(
                "usage export maximum response bytes must fit one maximum-size record plus its newline"
                    .to_string(),
            );
        }
        // JSON string storage is bounded by the frame bytes. Reserve two record-slot arrays for
        // the decoder's geometric Vec capacity, plus the fixed line/reader buffers and bounded
        // temporary sequence/recent indexes used while constructing the final state.
        let minimum_startup_scratch = self
            .max_line_bytes
            .checked_add(self.max_frame_bytes)
            .and_then(|bytes| {
                self.max_batch_records
                    .checked_mul(std::mem::size_of::<UsageLedgerRecord>().saturating_mul(2))
                    .and_then(|record_slots| bytes.checked_add(record_slots))
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.max_line_bytes
                        .min(USAGE_LEDGER_STARTUP_READER_MAX_BYTES),
                )
            })
            .and_then(|bytes| {
                self.max_sequence_ranges
                    .checked_mul(USAGE_LEDGER_STARTUP_INDEX_ALLOWANCE_BYTES)
                    .and_then(|range_index| bytes.checked_add(range_index))
            })
            .and_then(|bytes| {
                self.recent_records
                    .checked_mul(
                        std::mem::size_of::<UsageLedgerRecord>()
                            .saturating_add(USAGE_LEDGER_STARTUP_INDEX_ALLOWANCE_BYTES),
                    )
                    .and_then(|recent_index| bytes.checked_add(recent_index))
            })
            .ok_or_else(|| "usage ledger startup scratch limits overflow usize".to_string())?;
        if self.startup_scratch_bytes < minimum_startup_scratch {
            return Err(format!(
                "usage ledger startup scratch bytes must be at least {minimum_startup_scratch} for the configured line, frame, and batch limits"
            ));
        }
        Ok(self)
    }
}

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
    pub retained_records: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earliest_retained_sequence: Option<u64>,
    pub tenant_count: u64,
    pub last_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_record_unix_ms: Option<u64>,
    pub storage_reconciliations_total: u64,
    pub record_failures_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_record_error_code: Option<String>,
    pub limits: UsageLedgerLimits,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageAccountingMetricsSnapshot {
    pub durable: bool,
    pub records_total: u64,
    pub retained_records: u64,
    pub earliest_retained_sequence: Option<u64>,
    pub recent_record_limit: usize,
    pub tenant_count: u64,
    pub tenant_limit: usize,
    pub storage_reconciliations_total: u64,
    pub record_failures_total: u64,
}

#[derive(Debug)]
pub enum UsageAccountingError {
    Disk(tsink::TsinkError),
    Persistence(String),
    Limit(String),
    Other(String),
}

impl UsageAccountingError {
    pub fn disk_error(&self) -> Option<&tsink::TsinkError> {
        match self {
            Self::Disk(err) => Some(err),
            Self::Persistence(_) | Self::Limit(_) | Self::Other(_) => None,
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
            Self::Limit(_) => "usage_ledger_limit_exceeded",
            Self::Other(_) => "usage_accounting_error",
        }
    }
}

impl fmt::Display for UsageAccountingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disk(err) => write!(formatter, "{err}"),
            Self::Persistence(message) | Self::Limit(message) | Self::Other(message) => {
                formatter.write_str(message)
            }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UsageReadPage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_after_sequence: Option<u64>,
    pub snapshot_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earliest_available_sequence: Option<u64>,
    pub records_aggregated: u64,
    pub limit: usize,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after_sequence: Option<u64>,
    pub all_time_exact: bool,
    pub raw_history_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageReport {
    pub filter: UsageReportFilter,
    pub journal: UsageLedgerStatus,
    pub page: UsageReadPage,
    #[serde(default)]
    pub tenants: Vec<UsageTenantSummary>,
    #[serde(default)]
    pub buckets: Vec<UsageBucketSummary>,
}

/// Allocation-free reconciliation inputs consumed by direct TSDB status.
///
/// These values are exactly the aggregates produced by filtering an all-time, unbucketed usage
/// report to one tenant, without retaining a duplicate report/filter/vector tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct UsageStatusReconciliationSnapshot {
    pub ingest_rows_total: u64,
    pub query_result_units_total: u64,
    pub retention_tombstones_applied_total: u64,
    pub background_events_total: u64,
    pub latest_storage_logical_bytes: u64,
    pub latest_storage_reconciled_unix_ms: Option<u64>,
}

/// Schema-complete usage-accounting inputs consumed by direct TSDB status.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UsageStatusSnapshot {
    pub journal: UsageLedgerStatus,
    pub current_tenant: UsageTenantSummary,
    pub reconciliation: UsageStatusReconciliationSnapshot,
}

/// Usage status output whose dynamic allocations remain charged to the caller's query.
///
/// The inner snapshot is intentionally private and this wrapper has no extraction method, so a
/// caller cannot move the output away from its reservation.
#[derive(Debug)]
#[must_use = "dropping the status snapshot releases its query-memory reservation"]
pub(crate) struct AccountedUsageStatusSnapshot {
    snapshot: UsageStatusSnapshot,
    _reservation: QueryMemoryReservation,
}

impl AccountedUsageStatusSnapshot {
    #[cfg(test)]
    fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

impl std::ops::Deref for AccountedUsageStatusSnapshot {
    type Target = UsageStatusSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

fn modeled_usage_status_str_bytes(value: &str) -> u64 {
    if value.is_empty() {
        return 0;
    }
    u64::try_from(value.len())
        .unwrap_or(u64::MAX)
        .saturating_add(USAGE_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_usage_status_string_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        return 0;
    }
    u64::try_from(value.capacity())
        .unwrap_or(u64::MAX)
        .saturating_add(USAGE_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_usage_status_path_peak_bytes(path: &Path) -> u64 {
    if let Some(value) = path.to_str() {
        return modeled_usage_status_str_bytes(value);
    }

    // `Path::display` replaces invalid encoded units before writing the result. Reserve a
    // conservative simultaneous allowance for that temporary lossy buffer and the retained
    // output; ordinary UTF-8 paths use the exact branch above.
    u64::try_from(path.as_os_str().len())
        .unwrap_or(u64::MAX)
        .saturating_mul(8)
        .saturating_add(USAGE_STATUS_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(2))
}

fn modeled_usage_status_snapshot_retained_bytes(snapshot: &UsageStatusSnapshot) -> u64 {
    snapshot
        .journal
        .ledger_path
        .as_ref()
        .map(modeled_usage_status_string_bytes)
        .unwrap_or(0)
        .saturating_add(
            snapshot
                .journal
                .last_record_error_code
                .as_ref()
                .map(modeled_usage_status_string_bytes)
                .unwrap_or(0),
        )
        .saturating_add(modeled_usage_status_string_bytes(
            &snapshot.current_tenant.tenant_id,
        ))
        .saturating_add(
            snapshot
                .current_tenant
                .latest_storage_snapshot
                .as_ref()
                .map(|storage| modeled_usage_status_string_bytes(&storage.tenant_id))
                .unwrap_or(0),
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageReadOptions {
    pub after_sequence: Option<u64>,
    pub snapshot_sequence: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageExportPage {
    #[serde(default)]
    pub records: Vec<UsageLedgerRecord>,
    pub snapshot_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earliest_available_sequence: Option<u64>,
    pub records_returned: usize,
    pub response_bytes: usize,
    pub has_more: bool,
    pub raw_history_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageReadError {
    InvalidLimit {
        requested: usize,
        maximum: usize,
    },
    InvalidResponseBytes {
        requested: usize,
        maximum: usize,
    },
    InvalidSnapshot {
        requested: u64,
        latest: u64,
    },
    CursorExpired {
        requested_after: u64,
        earliest_available: u64,
    },
    RecordExceedsResponseLimit {
        sequence: u64,
        required_bytes: usize,
        maximum_bytes: usize,
    },
    Encoding(String),
}

impl UsageReadError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidLimit { .. } => "usage_page_limit_invalid",
            Self::InvalidResponseBytes { .. } => "usage_response_limit_invalid",
            Self::InvalidSnapshot { .. } => "usage_snapshot_invalid",
            Self::CursorExpired { .. } => "usage_cursor_expired",
            Self::RecordExceedsResponseLimit { .. } => "usage_record_exceeds_response_limit",
            Self::Encoding(_) => "usage_export_encoding_failed",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::CursorExpired { .. } => 410,
            Self::RecordExceedsResponseLimit { .. } => 413,
            Self::Encoding(_) => 500,
            Self::InvalidLimit { .. }
            | Self::InvalidResponseBytes { .. }
            | Self::InvalidSnapshot { .. } => 400,
        }
    }
}

impl fmt::Display for UsageReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit { requested, maximum } => write!(
                formatter,
                "usage page limit {requested} is outside the configured range 1..={maximum}"
            ),
            Self::InvalidResponseBytes { requested, maximum } => write!(
                formatter,
                "usage response byte limit {requested} is outside the configured range 1..={maximum}"
            ),
            Self::InvalidSnapshot { requested, latest } => write!(
                formatter,
                "usage snapshot sequence {requested} is newer than the latest sequence {latest}"
            ),
            Self::CursorExpired {
                requested_after,
                earliest_available,
            } => write!(
                formatter,
                "usage cursor after sequence {requested_after} is no longer retained; earliest available sequence is {earliest_available}"
            ),
            Self::RecordExceedsResponseLimit {
                sequence,
                required_bytes,
                maximum_bytes,
            } => write!(
                formatter,
                "usage record {sequence} requires {required_bytes} response bytes, exceeding the page limit {maximum_bytes}"
            ),
            Self::Encoding(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for UsageReadError {}

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
    records_total: u64,
    storage_reconciliations_total: u64,
    last_sequence: u64,
    last_record_unix_ms: Option<u64>,
    records: VecDeque<UsageLedgerRecord>,
    tenant_summaries: BTreeMap<String, SummaryAccumulator>,
}

impl Default for UsageLedgerState {
    fn default() -> Self {
        Self {
            next_seq: 1,
            records_total: 0,
            storage_reconciliations_total: 0,
            last_sequence: 0,
            last_record_unix_ms: None,
            records: VecDeque::new(),
            tenant_summaries: BTreeMap::new(),
        }
    }
}

impl UsageLedgerState {
    fn apply_record(
        &mut self,
        record: UsageLedgerRecord,
        limits: UsageLedgerLimits,
    ) -> Result<(), String> {
        if !self.tenant_summaries.contains_key(&record.tenant_id)
            && self.tenant_summaries.len() >= limits.max_tenants
        {
            return Err(format!(
                "usage ledger tenant limit {} exceeded by tenant '{}'",
                limits.max_tenants, record.tenant_id
            ));
        }
        self.records_total = self
            .records_total
            .checked_add(1)
            .ok_or_else(|| "usage ledger record count is exhausted".to_string())?;
        if record.category == UsageCategory::Storage {
            self.storage_reconciliations_total = self
                .storage_reconciliations_total
                .checked_add(1)
                .ok_or_else(|| {
                    "usage ledger storage reconciliation count is exhausted".to_string()
                })?;
        }
        if record.seq >= self.last_sequence {
            self.last_sequence = record.seq;
            self.last_record_unix_ms = Some(record.unix_ms);
        }
        self.tenant_summaries
            .entry(record.tenant_id.clone())
            .or_default()
            .apply_all_time(&record);
        self.records.push_back(record);
        while self.records.len() > limits.recent_records {
            self.records.pop_front();
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct UsageLedgerHealth {
    record_failures_total: u64,
    last_record_error_code: Option<String>,
}

#[derive(Debug)]
struct UsageAccountingInner {
    limits: UsageLedgerLimits,
    ledger_path: Option<PathBuf>,
    local_disk_budget: Option<Arc<LocalDiskBudget>>,
    append_serialization: Mutex<()>,
    async_append_serialization: Arc<tokio::sync::Semaphore>,
    async_reconcile_serialization: Arc<tokio::sync::Semaphore>,
    writer: Mutex<Option<File>>,
    state: Mutex<UsageLedgerState>,
    health: Mutex<UsageLedgerHealth>,
    #[cfg(test)]
    status_snapshot_string_clones: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
pub struct UsageAccounting {
    inner: Arc<UsageAccountingInner>,
}

impl UsageAccounting {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(data_path: Option<&Path>) -> Result<Arc<Self>, String> {
        Self::open_with_limits_and_disk_budget(data_path, UsageLedgerLimits::default(), None)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Arc<Self>, String> {
        Self::open_with_limits_and_disk_budget(
            data_path,
            UsageLedgerLimits::default(),
            local_disk_budget,
        )
    }

    pub fn open_with_limits_and_disk_budget(
        data_path: Option<&Path>,
        limits: UsageLedgerLimits,
        local_disk_budget: Option<Arc<LocalDiskBudget>>,
    ) -> Result<Arc<Self>, String> {
        let limits = limits.validate()?;
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
                let state = load_usage_ledger(&ledger_path, limits)?;
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
                limits,
                ledger_path,
                local_disk_budget,
                append_serialization: Mutex::new(()),
                async_append_serialization: Arc::new(tokio::sync::Semaphore::new(1)),
                async_reconcile_serialization: Arc::new(tokio::sync::Semaphore::new(1)),
                writer: Mutex::new(writer),
                state: Mutex::new(state),
                health: Mutex::new(UsageLedgerHealth::default()),
                #[cfg(test)]
                status_snapshot_string_clones: std::sync::atomic::AtomicU64::new(0),
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
        ledger_status_from_state(
            self.inner.ledger_path.as_deref(),
            &state,
            &health,
            self.inner.limits,
        )
    }

    /// Captures the journal, current tenant, and reconciliation inputs used by direct TSDB status.
    ///
    /// State and health are sampled under their established lock order exactly once. Every
    /// retained string is measured and reserved before the first output copy, and the returned
    /// private guard keeps the reconciled reservation live while any projected field is borrowed.
    pub(crate) fn status_snapshot_for_with_execution(
        &self,
        tenant_id: &str,
        execution: &QueryExecution,
    ) -> Result<AccountedUsageStatusSnapshot, tsink::QueryBudgetError> {
        execution.checkpoint()?;
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
        execution.checkpoint()?;

        let summary = state.tenant_summaries.get(tenant_id);
        let peak_bytes = self
            .inner
            .ledger_path
            .as_deref()
            .map(modeled_usage_status_path_peak_bytes)
            .unwrap_or(0)
            .saturating_add(
                health
                    .last_record_error_code
                    .as_deref()
                    .map(modeled_usage_status_str_bytes)
                    .unwrap_or(0),
            )
            .saturating_add(modeled_usage_status_str_bytes(tenant_id))
            .saturating_add(
                summary
                    .and_then(|summary| summary.latest_storage_snapshot.as_ref())
                    .map(|storage| modeled_usage_status_str_bytes(&storage.tenant_id))
                    .unwrap_or(0),
            );
        let mut reservation = execution.reserve_memory(peak_bytes)?;
        execution.checkpoint()?;

        let ledger_path = self
            .inner
            .ledger_path
            .as_deref()
            .map(|path| self.status_snapshot_path_string(path));
        execution.checkpoint()?;
        let last_record_error_code = health
            .last_record_error_code
            .as_deref()
            .map(|error| self.clone_status_snapshot_string(error));
        execution.checkpoint()?;

        let reconciliation = summary
            .map(|summary| UsageStatusReconciliationSnapshot {
                ingest_rows_total: summary.ingest.rows,
                query_result_units_total: summary.query.result_units,
                retention_tombstones_applied_total: summary.retention.tombstones_applied,
                background_events_total: summary.background.events_total,
                latest_storage_logical_bytes: summary
                    .latest_storage_snapshot
                    .as_ref()
                    .map(|storage| storage.logical_storage_bytes)
                    .unwrap_or(0),
                latest_storage_reconciled_unix_ms: summary
                    .latest_storage_snapshot
                    .as_ref()
                    .map(|storage| storage.reconciled_unix_ms),
            })
            .unwrap_or_default();
        let latest_storage_snapshot = summary
            .and_then(|summary| summary.latest_storage_snapshot.as_ref())
            .map(|storage| UsageStorageSnapshot {
                tenant_id: self.clone_status_snapshot_string(&storage.tenant_id),
                reconciled_unix_ms: storage.reconciled_unix_ms,
                series_total: storage.series_total,
                samples_total: storage.samples_total,
                logical_storage_bytes: storage.logical_storage_bytes,
            });
        execution.checkpoint()?;
        let current_tenant = UsageTenantSummary {
            tenant_id: self.clone_status_snapshot_string(tenant_id),
            ingest: summary
                .map(|summary| summary.ingest.clone())
                .unwrap_or_default(),
            query: summary
                .map(|summary| summary.query.clone())
                .unwrap_or_default(),
            retention: summary
                .map(|summary| summary.retention.clone())
                .unwrap_or_default(),
            background: summary
                .map(|summary| summary.background.clone())
                .unwrap_or_default(),
            latest_storage_snapshot,
        };
        let journal = UsageLedgerStatus {
            durable: self.inner.ledger_path.is_some(),
            ledger_path,
            records_total: state.records_total,
            retained_records: state.records.len() as u64,
            earliest_retained_sequence: state.records.front().map(|record| record.seq),
            tenant_count: state.tenant_summaries.len() as u64,
            last_sequence: state.last_sequence,
            last_record_unix_ms: state.last_record_unix_ms,
            storage_reconciliations_total: state.storage_reconciliations_total,
            record_failures_total: health.record_failures_total,
            last_record_error_code,
            limits: self.inner.limits,
        };
        execution.checkpoint()?;
        drop(health);
        drop(state);

        let snapshot = UsageStatusSnapshot {
            journal,
            current_tenant,
            reconciliation,
        };
        let retained_bytes = modeled_usage_status_snapshot_retained_bytes(&snapshot);
        assert!(
            retained_bytes <= peak_bytes,
            "usage status retained-memory model exceeded its pre-allocation reservation"
        );
        reservation.resize(retained_bytes)?;
        Ok(AccountedUsageStatusSnapshot {
            snapshot,
            _reservation: reservation,
        })
    }

    fn clone_status_snapshot_string(&self, value: &str) -> String {
        #[cfg(test)]
        self.inner
            .status_snapshot_string_clones
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut cloned = String::with_capacity(value.len());
        cloned.push_str(value);
        cloned
    }

    fn status_snapshot_path_string(&self, path: &Path) -> String {
        if let Some(value) = path.to_str() {
            return self.clone_status_snapshot_string(value);
        }
        #[cfg(test)]
        self.inner
            .status_snapshot_string_clones
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.display().to_string()
    }

    #[cfg(test)]
    fn reset_status_snapshot_string_clones(&self) {
        self.inner
            .status_snapshot_string_clones
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn status_snapshot_string_clones(&self) -> u64 {
        self.inner
            .status_snapshot_string_clones
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn metrics_snapshot(&self) -> UsageAccountingMetricsSnapshot {
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
        UsageAccountingMetricsSnapshot {
            durable: self.inner.ledger_path.is_some(),
            records_total: state.records_total,
            retained_records: state.records.len() as u64,
            earliest_retained_sequence: state.records.front().map(|record| record.seq),
            recent_record_limit: self.inner.limits.recent_records,
            tenant_count: state.tenant_summaries.len() as u64,
            tenant_limit: self.inner.limits.max_tenants,
            storage_reconciliations_total: state.storage_reconciliations_total,
            record_failures_total: health.record_failures_total,
        }
    }

    pub fn limits(&self) -> UsageLedgerLimits {
        self.inner.limits
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

    async fn append_prepared_storage_reconciliation_async(
        &self,
        prepared: PreparedStorageReconciliation,
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
            let PreparedStorageReconciliation {
                records,
                _operation_memory,
            } = prepared;
            let result = accounting.append_records(records);
            drop(_operation_memory);
            result
        })
        .await
        {
            Ok(result) => result,
            Err(join_err) => {
                let err = append_task_join_failure("reconciliation append", join_err);
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
        if records.len() > self.inner.limits.max_batch_records {
            let err = UsageAccountingError::Limit(format!(
                "usage ledger batch contains {} records, exceeding the configured maximum {}",
                records.len(),
                self.inner.limits.max_batch_records
            ));
            self.note_failure(&err);
            return Err(err);
        }
        for record in &records {
            if let Err(message) =
                validate_usage_record_size(record, self.inner.limits.max_record_bytes)
            {
                let err = UsageAccountingError::Limit(message);
                self.note_failure(&err);
                return Err(err);
            }
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
            let initial_next_seq = {
                let state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                validate_append_tenants(&state, &records, self.inner.limits)?;
                state.next_seq
            };
            let mut next_seq = initial_next_seq;
            for record in &mut records {
                record.seq = next_seq;
                next_seq = next_seq.checked_add(1).ok_or_else(|| {
                    UsageAccountingError::Other(
                        "usage ledger sequence space is exhausted".to_string(),
                    )
                })?;
            }

            for record in &records {
                validate_usage_record_size(record, self.inner.limits.max_record_bytes)
                    .map_err(UsageAccountingError::Limit)?;
            }

            if self.inner.ledger_path.is_some() {
                let encoded = encode_usage_frame(&records, self.inner.limits)?;
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
            for record in records.iter().cloned() {
                state
                    .apply_record(record, self.inner.limits)
                    .map_err(UsageAccountingError::Limit)?;
            }
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
        self.report_page(
            tenant_id,
            start_unix_ms,
            end_unix_ms,
            bucket_width,
            UsageReadOptions {
                after_sequence: None,
                snapshot_sequence: None,
                limit: self.inner.limits.report_max_records,
            },
        )
        .expect("configured usage report limits must be valid")
    }

    pub fn report_page(
        &self,
        tenant_id: Option<&str>,
        start_unix_ms: Option<u64>,
        end_unix_ms: Option<u64>,
        bucket_width: UsageBucketWidth,
        options: UsageReadOptions,
    ) -> Result<UsageReport, UsageReadError> {
        if options.limit == 0 || options.limit > self.inner.limits.report_max_records {
            return Err(UsageReadError::InvalidLimit {
                requested: options.limit,
                maximum: self.inner.limits.report_max_records,
            });
        }
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
        let journal = ledger_status_from_state(
            self.inner.ledger_path.as_deref(),
            &state,
            &health,
            self.inner.limits,
        );

        let all_time_exact = start_unix_ms.is_none()
            && end_unix_ms.is_none()
            && bucket_width == UsageBucketWidth::None
            && options.after_sequence.is_none()
            && options.snapshot_sequence.is_none();
        if all_time_exact {
            let mut records_aggregated = 0u64;
            let mut tenants = Vec::with_capacity(
                tenant_id
                    .map(|_| usize::from(!state.tenant_summaries.is_empty()))
                    .unwrap_or(state.tenant_summaries.len()),
            );
            for (candidate, summary) in &state.tenant_summaries {
                if tenant_id.is_some_and(|id| id != candidate.as_str()) {
                    continue;
                }
                records_aggregated = records_aggregated.saturating_add(summary.records_total());
                tenants.push(summary.to_summary(candidate.clone()));
            }
            return Ok(UsageReport {
                filter: UsageReportFilter {
                    tenant_id: tenant_id.map(str::to_string),
                    start_unix_ms,
                    end_unix_ms,
                    bucket_width,
                },
                journal,
                page: UsageReadPage {
                    requested_after_sequence: None,
                    snapshot_sequence: state.last_sequence,
                    earliest_available_sequence: state.records.front().map(|record| record.seq),
                    records_aggregated,
                    limit: options.limit,
                    has_more: false,
                    next_after_sequence: None,
                    all_time_exact: true,
                    raw_history_complete: state.records_total == state.records.len() as u64,
                },
                tenants,
                buckets: Vec::new(),
            });
        }

        let (snapshot_sequence, earliest_available, after_sequence) =
            resolve_read_window(&state, options)?;

        let mut tenants = BTreeMap::<String, SummaryAccumulator>::new();
        let mut buckets = BTreeMap::<u64, BTreeMap<String, BucketAccumulator>>::new();
        let end_limit = end_unix_ms.unwrap_or(u64::MAX);
        let bucket_size_ms = bucket_width.bucket_size_ms();
        let mut records_aggregated = 0usize;
        let mut last_aggregated_sequence = None;
        let mut has_more = false;

        for record in &state.records {
            if record.seq <= after_sequence || record.seq > snapshot_sequence {
                continue;
            }
            if !matches_tenant_filter(record, tenant_id) {
                continue;
            }
            let is_matching_storage =
                record.category == UsageCategory::Storage && record.unix_ms <= end_limit;
            if !is_matching_storage && !matches_time_filter(record, start_unix_ms, end_unix_ms) {
                continue;
            }
            if records_aggregated == options.limit {
                has_more = true;
                break;
            }
            records_aggregated += 1;
            last_aggregated_sequence = Some(record.seq);
            if record.category == UsageCategory::Storage && record.unix_ms <= end_limit {
                tenants
                    .entry(record.tenant_id.clone())
                    .or_default()
                    .apply_storage(record);
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

        Ok(UsageReport {
            filter: UsageReportFilter {
                tenant_id: tenant_id.map(str::to_string),
                start_unix_ms,
                end_unix_ms,
                bucket_width,
            },
            journal,
            page: UsageReadPage {
                requested_after_sequence: options.after_sequence,
                snapshot_sequence,
                earliest_available_sequence: earliest_available,
                records_aggregated: records_aggregated as u64,
                limit: options.limit,
                has_more,
                next_after_sequence: if has_more {
                    last_aggregated_sequence
                } else {
                    None
                },
                all_time_exact: false,
                raw_history_complete: state.records_total == state.records.len() as u64,
            },
            tenants: tenant_summaries,
            buckets: bucket_summaries,
        })
    }

    #[cfg(test)]
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

    pub fn export_page(
        &self,
        tenant_id: Option<&str>,
        start_unix_ms: Option<u64>,
        end_unix_ms: Option<u64>,
        options: UsageReadOptions,
        max_response_bytes: usize,
    ) -> Result<UsageExportPage, UsageReadError> {
        if options.limit == 0 || options.limit > self.inner.limits.export_max_records {
            return Err(UsageReadError::InvalidLimit {
                requested: options.limit,
                maximum: self.inner.limits.export_max_records,
            });
        }
        if max_response_bytes == 0
            || max_response_bytes > self.inner.limits.export_max_response_bytes
        {
            return Err(UsageReadError::InvalidResponseBytes {
                requested: max_response_bytes,
                maximum: self.inner.limits.export_max_response_bytes,
            });
        }
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (snapshot_sequence, earliest_available, after_sequence) =
            resolve_read_window(&state, options)?;
        let mut records = Vec::with_capacity(options.limit.min(state.records.len()));
        let mut response_bytes = 0usize;
        let mut has_more = false;
        for record in &state.records {
            if record.seq <= after_sequence || record.seq > snapshot_sequence {
                continue;
            }
            if !matches_tenant_filter(record, tenant_id)
                || !matches_time_filter(record, start_unix_ms, end_unix_ms)
            {
                continue;
            }
            if records.len() == options.limit {
                has_more = true;
                break;
            }
            let encoded = serde_json::to_vec(record).map_err(|err| {
                UsageReadError::Encoding(format!("failed to encode usage record: {err}"))
            })?;
            let line_bytes = encoded.len().checked_add(1).ok_or_else(|| {
                UsageReadError::Encoding("usage export line byte count overflowed".to_string())
            })?;
            let projected_response_bytes =
                response_bytes.checked_add(line_bytes).ok_or_else(|| {
                    UsageReadError::Encoding(
                        "usage export response byte count overflowed".to_string(),
                    )
                })?;
            if projected_response_bytes > max_response_bytes {
                if records.is_empty() {
                    return Err(UsageReadError::RecordExceedsResponseLimit {
                        sequence: record.seq,
                        required_bytes: line_bytes,
                        maximum_bytes: max_response_bytes,
                    });
                }
                has_more = true;
                break;
            }
            response_bytes = projected_response_bytes;
            records.push(record.clone());
        }
        let next_after_sequence = if has_more {
            records.last().map(|record| record.seq)
        } else {
            None
        };
        Ok(UsageExportPage {
            records_returned: records.len(),
            records,
            snapshot_sequence,
            earliest_available_sequence: earliest_available,
            response_bytes,
            has_more,
            raw_history_complete: state.records_total == state.records.len() as u64,
            next_after_sequence,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn tenant_summary(&self, tenant_id: &str) -> UsageTenantSummary {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .tenant_summaries
            .get(tenant_id)
            .map(|summary| summary.to_summary(tenant_id.to_string()))
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
        state
            .tenant_summaries
            .get(tenant_id)
            .and_then(|summary| summary.latest_storage_snapshot.clone())
    }

    pub fn reconcile_storage(
        &self,
        storage: &Arc<dyn Storage>,
    ) -> Result<Vec<UsageStorageSnapshot>, UsageAccountingError> {
        let prepared = collect_storage_reconciliation_records(storage, self.inner.limits)?;
        let PreparedStorageReconciliation {
            records,
            _operation_memory,
        } = prepared;
        let records = self.append_records(records)?;
        drop(_operation_memory);
        Ok(storage_snapshots_from_records(records))
    }

    pub async fn reconcile_storage_async(
        &self,
        storage: Arc<dyn Storage>,
    ) -> Result<Vec<UsageStorageSnapshot>, UsageAccountingError> {
        // Bound reconciliation independently from ordinary ledger appends. Holding this permit
        // through publication also gives concurrent admin requests a clear whole-operation order
        // without coupling request metering to scan latency.
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
        // Each metadata read and row page runs as a separate bounded blocking task. Cursor and
        // aggregate state remain in this future between awaits, so cancellation before append
        // drops all unpublished state after at most the currently running bounded task completes.
        let prepared =
            collect_storage_reconciliation_records_async(storage, self.inner.limits).await?;
        let records = self
            .append_prepared_storage_reconciliation_async(prepared)
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
    limits: UsageLedgerLimits,
) -> Result<PreparedStorageReconciliation, UsageAccountingError> {
    let scan_limits = StorageReconciliationScanLimits::from_ledger_limits(limits)?;
    let execution = admit_storage_reconciliation_execution(storage, scan_limits)?;
    for _attempt in 0..scan_limits.max_snapshot_attempts {
        let plan = load_storage_reconciliation_plan(storage, limits, scan_limits, &execution)?;
        let Some(first_pass) =
            run_storage_reconciliation_pass(storage, &plan, scan_limits, &execution)?
        else {
            continue;
        };
        let Some(second_pass) =
            run_storage_reconciliation_pass(storage, &plan, scan_limits, &execution)?
        else {
            continue;
        };
        let final_manifest =
            load_storage_reconciliation_manifest(storage, scan_limits, &execution)?;
        if first_pass == second_pass && plan.metrics() == final_manifest.metrics.as_slice() {
            return storage_reconciliation_records(first_pass.per_tenant, &plan, limits);
        }
    }
    Err(storage_reconciliation_snapshot_error(scan_limits))
}

fn admit_storage_reconciliation_execution(
    storage: &Arc<dyn Storage>,
    scan_limits: StorageReconciliationScanLimits,
) -> Result<QueryExecution, UsageAccountingError> {
    if storage.select_series_execution_accounting() != QueryExecutionAccounting::Complete {
        return Err(UsageAccountingError::Other(
            "usage storage reconciliation requires completely execution-accounted metadata selection"
                .to_string(),
        ));
    }
    if storage.scan_series_rows_execution_accounting() != QueryExecutionAccounting::Complete {
        return Err(UsageAccountingError::Other(
            "usage storage reconciliation requires completely execution-accounted guarded row-page scans"
                .to_string(),
        ));
    }
    storage
        .begin_query_execution(
            scan_limits.operation_query_limits()?,
            QueryCancellationToken::new(),
        )
        .map_err(|err| {
            storage_reconciliation_storage_error(
                "admit the finite whole-operation query execution",
                err,
            )
        })?
        .ok_or_else(|| {
            UsageAccountingError::Other(
                "usage storage reconciliation requires execution-accounted metadata and row scans; the storage backend exposes no query budget"
                    .to_string(),
            )
        })
}

#[derive(Debug, Clone, Copy)]
struct StorageReconciliationScanLimits {
    max_manifest_series: usize,
    max_manifest_bytes: usize,
    max_series_per_page: usize,
    max_rows_per_page: usize,
    max_page_bytes: usize,
    max_page_memory_bytes: usize,
    max_operation_memory_bytes: usize,
    max_rows_per_pass: u64,
    max_returned_bytes_per_pass: u64,
    max_samples_scanned_per_page: u64,
    max_samples_scanned_per_pass: u64,
    max_pages_per_pass: u64,
    max_snapshot_attempts: usize,
}

impl StorageReconciliationScanLimits {
    fn from_ledger_limits(limits: UsageLedgerLimits) -> Result<Self, UsageAccountingError> {
        let max_page_bytes = STORAGE_RECONCILE_PAGE_MAX_BYTES.min(limits.max_frame_bytes);
        let max_page_memory_bytes = max_page_bytes
            .checked_mul(STORAGE_RECONCILE_PAGE_MEMORY_MULTIPLIER)
            .ok_or_else(|| {
                UsageAccountingError::Limit(
                    "usage storage reconciliation page-memory limit overflowed usize".to_string(),
                )
            })?;
        let max_operation_memory_bytes = limits
            .startup_scratch_bytes
            .checked_mul(STORAGE_RECONCILE_OPERATION_MEMORY_MULTIPLIER)
            .ok_or_else(|| {
                UsageAccountingError::Limit(
                    "usage storage reconciliation operation-memory limit overflowed usize"
                        .to_string(),
                )
            })?;
        let max_returned_bytes_per_pass = limits
            .startup_scratch_bytes
            .checked_mul(STORAGE_RECONCILE_TOTAL_BYTES_MULTIPLIER)
            .ok_or_else(|| {
                UsageAccountingError::Limit(
                    "usage storage reconciliation total-byte limit overflowed usize".to_string(),
                )
            })?;
        for (name, value) in [
            ("manifest bytes", limits.startup_scratch_bytes),
            ("series per page", STORAGE_RECONCILE_BATCH_SIZE),
            ("rows per page", STORAGE_RECONCILE_PAGE_MAX_ROWS),
            ("page bytes", max_page_bytes),
            ("page memory bytes", max_page_memory_bytes),
            ("operation memory bytes", max_operation_memory_bytes),
            ("returned bytes per pass", max_returned_bytes_per_pass),
        ] {
            if value == 0 {
                return Err(UsageAccountingError::Limit(format!(
                    "usage storage reconciliation {name} must be greater than zero"
                )));
            }
        }
        Ok(Self {
            max_manifest_series: STORAGE_RECONCILE_MAX_MANIFEST_SERIES,
            max_manifest_bytes: limits.startup_scratch_bytes,
            max_series_per_page: STORAGE_RECONCILE_BATCH_SIZE,
            max_rows_per_page: STORAGE_RECONCILE_PAGE_MAX_ROWS,
            max_page_bytes,
            max_page_memory_bytes,
            max_operation_memory_bytes,
            max_rows_per_pass: STORAGE_RECONCILE_MAX_ROWS_PER_PASS,
            max_returned_bytes_per_pass: u64::try_from(max_returned_bytes_per_pass)
                .unwrap_or(u64::MAX),
            max_samples_scanned_per_page: STORAGE_RECONCILE_MAX_SAMPLES_SCANNED_PER_PAGE,
            max_samples_scanned_per_pass: STORAGE_RECONCILE_MAX_SAMPLES_SCANNED_PER_PASS,
            max_pages_per_pass: STORAGE_RECONCILE_MAX_PAGES_PER_PASS,
            max_snapshot_attempts: STORAGE_RECONCILE_MAX_SNAPSHOT_ATTEMPTS,
        })
    }

    fn operation_query_limits(self) -> Result<QueryWorkLimits, UsageAccountingError> {
        let attempts = u64::try_from(self.max_snapshot_attempts).map_err(|_| {
            UsageAccountingError::Limit(
                "usage storage reconciliation attempt limit overflowed u64".to_string(),
            )
        })?;
        let passes = storage_reconciliation_limit_mul(
            attempts,
            STORAGE_RECONCILE_PASSES_PER_ATTEMPT,
            "pass count",
        )?;
        let manifests = storage_reconciliation_limit_mul(
            attempts,
            STORAGE_RECONCILE_MANIFESTS_PER_ATTEMPT,
            "manifest count",
        )?;
        let page_calls =
            storage_reconciliation_limit_mul(passes, self.max_pages_per_pass, "page count")?;
        let manifest_series = storage_reconciliation_limit_mul(
            manifests,
            u64::try_from(self.max_manifest_series).unwrap_or(u64::MAX),
            "manifest series",
        )?;
        let page_series = storage_reconciliation_limit_mul(
            page_calls,
            u64::try_from(self.max_series_per_page).unwrap_or(u64::MAX),
            "page series",
        )?;
        let max_series_matched =
            storage_reconciliation_limit_add(manifest_series, page_series, "matched series")?;
        let max_samples_scanned = storage_reconciliation_limit_mul(
            passes,
            self.max_samples_scanned_per_pass,
            "scanned samples",
        )?;
        // One discarded page can be returned before a pass observes manifest drift. Include that
        // page in the whole-operation cap even though it is never applied to an aggregate.
        let returned_rows_per_pass = storage_reconciliation_limit_add(
            self.max_rows_per_pass,
            u64::try_from(self.max_rows_per_page).unwrap_or(u64::MAX),
            "returned rows per pass",
        )?;
        let max_samples_returned =
            storage_reconciliation_limit_mul(passes, returned_rows_per_pass, "returned samples")?;
        let manifest_returned_bytes = storage_reconciliation_limit_mul(
            manifests,
            u64::try_from(self.max_manifest_bytes).unwrap_or(u64::MAX),
            "manifest returned bytes",
        )?;
        let page_returned_bytes_per_pass = storage_reconciliation_limit_add(
            self.max_returned_bytes_per_pass,
            u64::try_from(self.max_page_bytes).unwrap_or(u64::MAX),
            "page returned bytes per pass",
        )?;
        let page_returned_bytes = storage_reconciliation_limit_mul(
            passes,
            page_returned_bytes_per_pass,
            "page returned bytes",
        )?;
        let max_returned_bytes = storage_reconciliation_limit_add(
            manifest_returned_bytes,
            page_returned_bytes,
            "whole-operation returned bytes",
        )?;
        let max_pattern_expansion = storage_reconciliation_limit_mul(
            manifests,
            u64::try_from(self.max_manifest_series).unwrap_or(u64::MAX),
            "pattern expansion",
        )?;
        // A plan reservation and a final-manifest reservation can coexist. Page/backend working
        // memory replaces, rather than nests with, the final manifest but may coexist with plan
        // state during either pass.
        let two_operation_reservations = storage_reconciliation_limit_mul(
            2,
            u64::try_from(self.max_operation_memory_bytes).unwrap_or(u64::MAX),
            "operation memory",
        )?;
        let max_memory_bytes = storage_reconciliation_limit_add(
            two_operation_reservations,
            u64::try_from(self.max_page_memory_bytes).unwrap_or(u64::MAX),
            "whole-operation memory",
        )?;
        let max_intermediate_vector_size = u64::try_from(self.max_manifest_series)
            .unwrap_or(u64::MAX)
            .max(self.max_samples_scanned_per_page)
            .max(u64::try_from(self.max_rows_per_page).unwrap_or(u64::MAX));

        Ok(QueryWorkLimits {
            max_series_matched: Some(max_series_matched),
            max_samples_scanned: Some(max_samples_scanned),
            max_samples_returned: Some(max_samples_returned),
            max_returned_bytes: Some(max_returned_bytes),
            max_pattern_expansion: Some(max_pattern_expansion),
            max_steps: None,
            max_intermediate_vector_size: Some(max_intermediate_vector_size),
            max_memory_bytes: Some(max_memory_bytes),
            max_wall_time: Some(STORAGE_RECONCILE_OPERATION_WALL_TIME),
        })
    }
}

fn storage_reconciliation_limit_mul(
    left: u64,
    right: u64,
    name: &str,
) -> Result<u64, UsageAccountingError> {
    left.checked_mul(right).ok_or_else(|| {
        UsageAccountingError::Limit(format!(
            "usage storage reconciliation {name} limit overflowed u64"
        ))
    })
}

fn storage_reconciliation_limit_add(
    left: u64,
    right: u64,
    name: &str,
) -> Result<u64, UsageAccountingError> {
    left.checked_add(right).ok_or_else(|| {
        UsageAccountingError::Limit(format!(
            "usage storage reconciliation {name} limit overflowed u64"
        ))
    })
}

fn storage_reconciliation_execution_delta(
    before: QueryExecutionSnapshot,
    after: QueryExecutionSnapshot,
) -> Result<QueryExecutionSnapshot, UsageAccountingError> {
    let counter_delta = |name: &str, before: u64, after: u64| {
        after.checked_sub(before).ok_or_else(|| {
            UsageAccountingError::Other(format!(
                "usage storage reconciliation query counter '{name}' moved backwards from {before} to {after}"
            ))
        })
    };
    Ok(QueryExecutionSnapshot {
        memory_reserved_bytes: after.memory_reserved_bytes,
        series_matched: counter_delta(
            "series_matched",
            before.series_matched,
            after.series_matched,
        )?,
        samples_scanned: counter_delta(
            "samples_scanned",
            before.samples_scanned,
            after.samples_scanned,
        )?,
        samples_returned: counter_delta(
            "samples_returned",
            before.samples_returned,
            after.samples_returned,
        )?,
        returned_bytes: counter_delta(
            "returned_bytes",
            before.returned_bytes,
            after.returned_bytes,
        )?,
        pattern_expansion: counter_delta(
            "pattern_expansion",
            before.pattern_expansion,
            after.pattern_expansion,
        )?,
        steps: counter_delta("steps", before.steps, after.steps)?,
        intermediate_vector_size: after.intermediate_vector_size,
    })
}

#[derive(Debug, Clone)]
struct StorageReconciliationPlan {
    manifest: Arc<StorageReconciliationManifest>,
    base_per_tenant: Arc<BTreeMap<String, StorageAccumulator>>,
    prepared_records_retained_upper_bound: u64,
}

impl StorageReconciliationPlan {
    fn metrics(&self) -> &[MetricSeries] {
        &self.manifest.metrics
    }
}

#[derive(Debug)]
struct StorageReconciliationManifest {
    metrics: Vec<MetricSeries>,
    operation_memory_reservation: QueryMemoryReservation,
    manifest_retained_bytes: u64,
    operation_retained_bytes: u64,
}

struct PreparedStorageReconciliation {
    records: Vec<UsageLedgerRecord>,
    _operation_memory: Arc<StorageReconciliationManifest>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StorageReconciliationCursor {
    series_offset: usize,
    row_offset: u64,
    expect_empty_probe: bool,
}

#[derive(Debug, Clone, Copy)]
struct StorageReconciliationPageRequest {
    max_rows: usize,
    max_returned_bytes: u64,
    max_samples_scanned: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StorageReconciliationPassResult {
    per_tenant: BTreeMap<String, StorageAccumulator>,
    rows_total: u64,
    returned_bytes_total: u64,
    pages_total: u64,
    fingerprint: [u64; 2],
}

struct StorageReconciliationPassState {
    per_tenant: BTreeMap<String, StorageAccumulator>,
    cursor: Option<StorageReconciliationCursor>,
    rows_total: u64,
    returned_bytes_total: u64,
    samples_scanned_total: u64,
    pages_total: u64,
    fingerprint_a: Xxh64,
    fingerprint_b: Xxh64,
}

impl StorageReconciliationPassState {
    fn new(plan: &StorageReconciliationPlan) -> Self {
        Self {
            per_tenant: plan.base_per_tenant.as_ref().clone(),
            cursor: (!plan.metrics().is_empty()).then_some(StorageReconciliationCursor::default()),
            rows_total: 0,
            returned_bytes_total: 0,
            samples_scanned_total: 0,
            pages_total: 0,
            fingerprint_a: Xxh64::new(STORAGE_RECONCILE_FINGERPRINT_SEED_A),
            fingerprint_b: Xxh64::new(STORAGE_RECONCILE_FINGERPRINT_SEED_B),
        }
    }

    fn page_request(
        &self,
        scan_limits: StorageReconciliationScanLimits,
    ) -> Result<StorageReconciliationPageRequest, UsageAccountingError> {
        if self.pages_total >= scan_limits.max_pages_per_pass {
            return Err(UsageAccountingError::Limit(format!(
                "usage storage reconciliation exceeded the per-pass page maximum {}",
                scan_limits.max_pages_per_pass
            )));
        }
        let remaining_scan = scan_limits
            .max_samples_scanned_per_pass
            .checked_sub(self.samples_scanned_total)
            .ok_or_else(|| {
                UsageAccountingError::Limit(format!(
                    "usage storage reconciliation exceeded the per-pass scanned-sample maximum {}",
                    scan_limits.max_samples_scanned_per_pass
                ))
            })?;
        if remaining_scan == 0 {
            return Err(UsageAccountingError::Limit(format!(
                "usage storage reconciliation needs another terminal page after reaching the per-pass scanned-sample maximum {}",
                scan_limits.max_samples_scanned_per_pass
            )));
        }

        let remaining_rows = scan_limits
            .max_rows_per_pass
            .checked_sub(self.rows_total)
            .ok_or_else(|| {
                UsageAccountingError::Limit(format!(
                    "usage storage reconciliation exceeded the per-pass row maximum {}",
                    scan_limits.max_rows_per_pass
                ))
            })?;
        let remaining_bytes = scan_limits
            .max_returned_bytes_per_pass
            .checked_sub(self.returned_bytes_total)
            .ok_or_else(|| {
                UsageAccountingError::Limit(format!(
                    "usage storage reconciliation exceeded the per-pass returned-byte maximum {}",
                    scan_limits.max_returned_bytes_per_pass
                ))
            })?;
        let cursor = self
            .cursor
            .expect("a page request is only valid while a cursor remains");
        let at_total_capacity = remaining_rows == 0 || remaining_bytes == 0;
        let max_rows = if at_total_capacity || cursor.expect_empty_probe {
            1
        } else {
            usize::try_from(remaining_rows)
                .unwrap_or(usize::MAX)
                .min(scan_limits.max_rows_per_page)
        };
        // A one-byte result budget lets an empty capacity probe succeed while rejecting any Row,
        // whose fixed modeled representation is necessarily larger than one byte.
        let max_returned_bytes = if at_total_capacity {
            1
        } else {
            remaining_bytes.min(u64::try_from(scan_limits.max_page_bytes).unwrap_or(u64::MAX))
        };
        Ok(StorageReconciliationPageRequest {
            max_rows,
            max_returned_bytes,
            max_samples_scanned: remaining_scan.min(scan_limits.max_samples_scanned_per_page),
        })
    }

    fn apply_page(
        &mut self,
        page: StorageReconciliationPage,
        scan_limits: StorageReconciliationScanLimits,
    ) -> Result<(), UsageAccountingError> {
        self.pages_total = self.pages_total.checked_add(1).ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation page count overflowed u64".to_string(),
            )
        })?;
        self.rows_total = self.rows_total.checked_add(page.rows).ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation row count overflowed u64".to_string(),
            )
        })?;
        self.returned_bytes_total = self
            .returned_bytes_total
            .checked_add(page.returned_bytes)
            .ok_or_else(|| {
                UsageAccountingError::Limit(
                    "usage storage reconciliation returned-byte count overflowed u64".to_string(),
                )
            })?;
        self.samples_scanned_total = self
            .samples_scanned_total
            .checked_add(page.samples_scanned)
            .ok_or_else(|| {
                UsageAccountingError::Limit(
                    "usage storage reconciliation scanned-sample count overflowed u64".to_string(),
                )
            })?;
        if self.rows_total > scan_limits.max_rows_per_pass {
            return Err(UsageAccountingError::Limit(format!(
                "usage storage reconciliation observed more than {} rows in one pass",
                scan_limits.max_rows_per_pass
            )));
        }
        if self.returned_bytes_total > scan_limits.max_returned_bytes_per_pass {
            return Err(UsageAccountingError::Limit(format!(
                "usage storage reconciliation observed more than {} returned bytes in one pass",
                scan_limits.max_returned_bytes_per_pass
            )));
        }
        if self.samples_scanned_total > scan_limits.max_samples_scanned_per_pass {
            return Err(UsageAccountingError::Limit(format!(
                "usage storage reconciliation scanned more than {} samples in one pass",
                scan_limits.max_samples_scanned_per_pass
            )));
        }
        for (tenant_id, samples) in page.samples_per_tenant {
            let acc = self.per_tenant.get_mut(tenant_id.as_str()).ok_or_else(|| {
                UsageAccountingError::Other(format!(
                    "usage storage reconciliation row referenced tenant '{tenant_id}' absent from the fenced manifest"
                ))
            })?;
            acc.samples_total = acc.samples_total.checked_add(samples).ok_or_else(|| {
                UsageAccountingError::Limit(format!(
                    "usage storage reconciliation sample total overflowed for tenant '{tenant_id}'"
                ))
            })?;
            let sample_bytes = samples.checked_mul(ESTIMATED_SAMPLE_BYTES).ok_or_else(|| {
                UsageAccountingError::Limit(format!(
                    "usage storage reconciliation byte total overflowed for tenant '{tenant_id}'"
                ))
            })?;
            acc.logical_storage_bytes = acc
                .logical_storage_bytes
                .checked_add(sample_bytes)
                .ok_or_else(|| {
                    UsageAccountingError::Limit(format!(
                        "usage storage reconciliation byte total overflowed for tenant '{tenant_id}'"
                    ))
                })?;
        }
        self.fingerprint_a.update(&page.rows.to_le_bytes());
        self.fingerprint_a
            .update(&page.fingerprint[0].to_le_bytes());
        self.fingerprint_a
            .update(&page.fingerprint[1].to_le_bytes());
        self.fingerprint_b.update(&page.rows.to_le_bytes());
        self.fingerprint_b
            .update(&page.fingerprint[0].to_le_bytes());
        self.fingerprint_b
            .update(&page.fingerprint[1].to_le_bytes());
        self.cursor = page.next_cursor;
        Ok(())
    }

    fn finish(self) -> StorageReconciliationPassResult {
        StorageReconciliationPassResult {
            per_tenant: self.per_tenant,
            rows_total: self.rows_total,
            returned_bytes_total: self.returned_bytes_total,
            pages_total: self.pages_total,
            fingerprint: [self.fingerprint_a.digest(), self.fingerprint_b.digest()],
        }
    }
}

struct StorageReconciliationPage {
    next_cursor: Option<StorageReconciliationCursor>,
    samples_per_tenant: BTreeMap<String, u64>,
    rows: u64,
    returned_bytes: u64,
    samples_scanned: u64,
    fingerprint: [u64; 2],
}

fn load_storage_reconciliation_plan(
    storage: &Arc<dyn Storage>,
    limits: UsageLedgerLimits,
    scan_limits: StorageReconciliationScanLimits,
    execution: &QueryExecution,
) -> Result<StorageReconciliationPlan, UsageAccountingError> {
    let mut manifest = load_storage_reconciliation_manifest(storage, scan_limits, execution)?;
    let mut base_per_tenant = BTreeMap::<String, StorageAccumulator>::new();
    for series in &manifest.metrics {
        let tenant_id = tenant_id_for_metric_series(series);
        if !base_per_tenant.contains_key(&tenant_id) && base_per_tenant.len() >= limits.max_tenants
        {
            return Err(UsageAccountingError::Limit(format!(
                "usage storage reconciliation observed more than {} tenants",
                limits.max_tenants
            )));
        }
        let acc = base_per_tenant.entry(tenant_id.clone()).or_default();
        acc.series_total = acc.series_total.checked_add(1).ok_or_else(|| {
            UsageAccountingError::Limit(format!(
                "usage storage reconciliation series total overflowed for tenant '{tenant_id}'"
            ))
        })?;
        acc.logical_storage_bytes = acc
            .logical_storage_bytes
            .checked_add(estimated_series_bytes(series, 0))
            .ok_or_else(|| {
                UsageAccountingError::Limit(format!(
                    "usage storage reconciliation byte total overflowed for tenant '{tenant_id}'"
                ))
            })?;
    }
    let accumulator_map_bytes =
        modeled_storage_reconciliation_accumulator_map_retained_bytes(&base_per_tenant)?;
    let concurrent_accumulator_bytes = accumulator_map_bytes.checked_mul(3).ok_or_else(|| {
        UsageAccountingError::Limit(
            "usage storage reconciliation accumulator-map memory overflowed u64".to_string(),
        )
    })?;
    let prepared_records_retained_upper_bound =
        modeled_storage_reconciliation_records_retained_upper_bound(&base_per_tenant, limits)?;
    let operation_retained_bytes = manifest
        .manifest_retained_bytes
        .checked_add(concurrent_accumulator_bytes)
        .and_then(|bytes| bytes.checked_add(prepared_records_retained_upper_bound))
        .ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation operation-memory model overflowed u64".to_string(),
            )
        })?;
    if operation_retained_bytes
        > u64::try_from(scan_limits.max_operation_memory_bytes).unwrap_or(u64::MAX)
    {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation operation requires {operation_retained_bytes} modeled retained bytes, exceeding the maximum {}",
            scan_limits.max_operation_memory_bytes
        )));
    }
    manifest
        .operation_memory_reservation
        .resize(operation_retained_bytes)
        .map_err(|err| {
            storage_reconciliation_storage_error(
                "retain the manifest, accumulator maps, and prepared records",
                tsink::TsinkError::from(err),
            )
        })?;
    manifest.operation_retained_bytes = operation_retained_bytes;
    if manifest.operation_memory_reservation.bytes() != operation_retained_bytes {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation operation reservation invariant failed: requested {operation_retained_bytes}, retained {}",
            manifest.operation_memory_reservation.bytes()
        )));
    }
    Ok(StorageReconciliationPlan {
        manifest: Arc::new(manifest),
        base_per_tenant: Arc::new(base_per_tenant),
        prepared_records_retained_upper_bound,
    })
}

fn load_storage_reconciliation_manifest(
    storage: &Arc<dyn Storage>,
    scan_limits: StorageReconciliationScanLimits,
    execution: &QueryExecution,
) -> Result<StorageReconciliationManifest, UsageAccountingError> {
    let before = execution.snapshot();
    let memory_before_reservation = before.memory_reserved_bytes;
    let mut selected = storage
        .select_series_with_execution_result(&SeriesSelection::new(), execution)
        .map_err(|err| {
            storage_reconciliation_storage_error("list the bounded metric manifest", err)
        })?;
    let mut memory_reservation = selected.take_memory_reservation().ok_or_else(|| {
        UsageAccountingError::Other(
            "usage storage reconciliation metric manifest omitted its retained-memory reservation"
                .to_string(),
        )
    })?;
    let mut metrics = selected.into_series();
    let execution_snapshot = execution.snapshot();
    let execution_delta = storage_reconciliation_execution_delta(before, execution_snapshot)?;
    if metrics.len() > scan_limits.max_manifest_series {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation manifest contains {} series, exceeding the maximum {}",
            metrics.len(),
            scan_limits.max_manifest_series
        )));
    }
    let modeled_bytes = modeled_storage_reconciliation_manifest_bytes(&metrics)?;
    if modeled_bytes > scan_limits.max_manifest_bytes {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation manifest requires {modeled_bytes} modeled bytes, exceeding the maximum {}",
            scan_limits.max_manifest_bytes
        )));
    }
    let metric_count = u64::try_from(metrics.len()).unwrap_or(u64::MAX);
    let modeled_bytes = u64::try_from(modeled_bytes).unwrap_or(u64::MAX);
    if execution_delta.series_matched < metric_count
        || execution_delta.series_matched
            > u64::try_from(scan_limits.max_manifest_series).unwrap_or(u64::MAX)
        || execution_delta.returned_bytes < modeled_bytes
        || execution_delta.returned_bytes
            > u64::try_from(scan_limits.max_manifest_bytes).unwrap_or(u64::MAX)
        || execution_delta.pattern_expansion
            > u64::try_from(scan_limits.max_manifest_series).unwrap_or(u64::MAX)
    {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation metric manifest violated its per-call accounting bounds: returned {metric_count} series/{modeled_bytes} modeled bytes, charged {}/{} with {} pattern candidates",
            execution_delta.series_matched,
            execution_delta.returned_bytes,
            execution_delta.pattern_expansion
        )));
    }

    metrics.sort();
    if metrics.windows(2).any(|window| window[0] == window[1]) {
        return Err(UsageAccountingError::Other(
            "usage storage reconciliation metric manifest contains duplicate series identities"
                .to_string(),
        ));
    }
    let retained_bytes = modeled_storage_reconciliation_manifest_retained_bytes(&metrics)?;
    if retained_bytes > u64::try_from(scan_limits.max_operation_memory_bytes).unwrap_or(u64::MAX) {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation manifest retains {retained_bytes} modeled bytes, exceeding the per-manifest memory maximum {}",
            scan_limits.max_operation_memory_bytes
        )));
    }
    memory_reservation.resize(retained_bytes).map_err(|err| {
        storage_reconciliation_storage_error(
            "adopt the bounded metric manifest reservation",
            tsink::TsinkError::from(err),
        )
    })?;
    let expected_reserved_memory = memory_before_reservation
        .checked_add(retained_bytes)
        .ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation manifest reservation overflowed u64".to_string(),
            )
        })?;
    if memory_reservation.bytes() != retained_bytes
        || execution.snapshot().memory_reserved_bytes < expected_reserved_memory
    {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation manifest reservation invariant failed after guarded adoption: requested {retained_bytes}, retained {}, expected execution total at least {expected_reserved_memory}, execution reports {}",
            memory_reservation.bytes(),
            execution.snapshot().memory_reserved_bytes
        )));
    }
    Ok(StorageReconciliationManifest {
        metrics,
        operation_memory_reservation: memory_reservation,
        manifest_retained_bytes: retained_bytes,
        operation_retained_bytes: retained_bytes,
    })
}

fn modeled_storage_reconciliation_manifest_bytes(
    metrics: &[MetricSeries],
) -> Result<usize, UsageAccountingError> {
    let mut bytes = metrics
        .len()
        .checked_mul(std::mem::size_of::<MetricSeries>())
        .ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation manifest byte model overflowed usize".to_string(),
            )
        })?;
    for series in metrics {
        bytes = bytes.checked_add(series.name.len()).ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation manifest byte model overflowed usize".to_string(),
            )
        })?;
        bytes = bytes
            .checked_add(
                series
                    .labels
                    .len()
                    .checked_mul(std::mem::size_of::<Label>())
                    .ok_or_else(|| {
                        UsageAccountingError::Limit(
                            "usage storage reconciliation manifest byte model overflowed usize"
                                .to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                UsageAccountingError::Limit(
                    "usage storage reconciliation manifest byte model overflowed usize".to_string(),
                )
            })?;
        for label in &series.labels {
            bytes = bytes
                .checked_add(label.name.len())
                .and_then(|value| value.checked_add(label.value.len()))
                .ok_or_else(|| {
                    UsageAccountingError::Limit(
                        "usage storage reconciliation manifest byte model overflowed usize"
                            .to_string(),
                    )
                })?;
        }
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_manifest_retained_bytes(
    metrics: &Vec<MetricSeries>,
) -> Result<u64, UsageAccountingError> {
    let mut bytes =
        modeled_storage_reconciliation_vec_retained_bytes::<MetricSeries>(metrics.capacity())?;
    for series in metrics {
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_string_retained_bytes(series.name.capacity())?,
        )?;
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_vec_retained_bytes::<Label>(series.labels.capacity())?,
        )?;
        for label in &series.labels {
            storage_reconciliation_checked_add(
                &mut bytes,
                modeled_storage_reconciliation_string_retained_bytes(label.name.capacity())?,
            )?;
            storage_reconciliation_checked_add(
                &mut bytes,
                modeled_storage_reconciliation_string_retained_bytes(label.value.capacity())?,
            )?;
        }
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_rows_retained_bytes(
    rows: &Vec<Row>,
) -> Result<u64, UsageAccountingError> {
    let mut bytes = modeled_storage_reconciliation_vec_retained_bytes::<Row>(rows.capacity())?;
    for row in rows {
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_string_retained_bytes(row.metric_capacity())?,
        )?;
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_vec_retained_bytes::<Label>(row.labels_capacity())?,
        )?;
        for label in row.labels() {
            storage_reconciliation_checked_add(
                &mut bytes,
                modeled_storage_reconciliation_string_retained_bytes(label.name.capacity())?,
            )?;
            storage_reconciliation_checked_add(
                &mut bytes,
                modeled_storage_reconciliation_string_retained_bytes(label.value.capacity())?,
            )?;
        }
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_value_retained_bytes(&row.data_point().value)?,
        )?;
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_accumulator_map_retained_bytes(
    per_tenant: &BTreeMap<String, StorageAccumulator>,
) -> Result<u64, UsageAccountingError> {
    let mut bytes = 0u64;
    for tenant_id in per_tenant.keys() {
        storage_reconciliation_checked_add(
            &mut bytes,
            storage_reconciliation_allocation_retained_bytes(
                u64::try_from(std::mem::size_of::<(String, StorageAccumulator)>()).map_err(
                    |_| {
                        UsageAccountingError::Limit(
                            "usage storage reconciliation accumulator-map model overflowed u64"
                                .to_string(),
                        )
                    },
                )?,
            )?,
        )?;
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_string_retained_bytes(tenant_id.capacity())?,
        )?;
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_records_retained_upper_bound(
    per_tenant: &BTreeMap<String, StorageAccumulator>,
    limits: UsageLedgerLimits,
) -> Result<u64, UsageAccountingError> {
    if per_tenant.len() > limits.max_batch_records {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation produced {} tenant records, exceeding the configured batch maximum {}",
            per_tenant.len(), limits.max_batch_records
        )));
    }
    let record_capacity = if per_tenant.is_empty() {
        0
    } else {
        per_tenant
            .len()
            .checked_next_power_of_two()
            .unwrap_or(limits.max_batch_records)
            .min(limits.max_batch_records)
            .max(per_tenant.len())
    };
    let mut bytes =
        modeled_storage_reconciliation_vec_retained_bytes::<UsageLedgerRecord>(record_capacity)?;
    for tenant_id in per_tenant.keys() {
        for capacity in [
            tenant_id.capacity(),
            "reconcile_storage".len(),
            "admin".len(),
            "success".len(),
        ] {
            storage_reconciliation_checked_add(
                &mut bytes,
                modeled_storage_reconciliation_string_retained_bytes(capacity)?,
            )?;
        }
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_records_retained_bytes(
    records: &Vec<UsageLedgerRecord>,
) -> Result<u64, UsageAccountingError> {
    let mut bytes =
        modeled_storage_reconciliation_vec_retained_bytes::<UsageLedgerRecord>(records.capacity())?;
    for record in records {
        for capacity in [
            record.tenant_id.capacity(),
            record.operation.capacity(),
            record.source.capacity(),
            record.status.capacity(),
        ] {
            storage_reconciliation_checked_add(
                &mut bytes,
                modeled_storage_reconciliation_string_retained_bytes(capacity)?,
            )?;
        }
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_page_accumulator_upper_bound(
    series: &[MetricSeries],
) -> Result<u64, UsageAccountingError> {
    let mut bytes = 0u64;
    for metric_series in series {
        storage_reconciliation_checked_add(
            &mut bytes,
            storage_reconciliation_allocation_retained_bytes(
                u64::try_from(std::mem::size_of::<(String, u64)>()).map_err(|_| {
                    UsageAccountingError::Limit(
                        "usage storage reconciliation page-accumulator model overflowed u64"
                            .to_string(),
                    )
                })?,
            )?,
        )?;
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_string_retained_bytes(
                tenant_id_for_labels(&metric_series.labels).len(),
            )?,
        )?;
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_page_accumulator_retained_bytes(
    per_tenant: &BTreeMap<String, u64>,
) -> Result<u64, UsageAccountingError> {
    let mut bytes = 0u64;
    for tenant_id in per_tenant.keys() {
        storage_reconciliation_checked_add(
            &mut bytes,
            storage_reconciliation_allocation_retained_bytes(
                u64::try_from(std::mem::size_of::<(String, u64)>()).map_err(|_| {
                    UsageAccountingError::Limit(
                        "usage storage reconciliation page-accumulator model overflowed u64"
                            .to_string(),
                    )
                })?,
            )?,
        )?;
        storage_reconciliation_checked_add(
            &mut bytes,
            modeled_storage_reconciliation_string_retained_bytes(tenant_id.capacity())?,
        )?;
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_value_retained_bytes(
    value: &Value,
) -> Result<u64, UsageAccountingError> {
    match value {
        Value::Bytes(bytes) => {
            modeled_storage_reconciliation_vec_retained_bytes::<u8>(bytes.capacity())
        }
        Value::String(text) => {
            modeled_storage_reconciliation_string_retained_bytes(text.capacity())
        }
        Value::Histogram(histogram) => {
            modeled_storage_reconciliation_histogram_retained_bytes(histogram)
        }
        Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => Ok(0),
    }
}

fn modeled_storage_reconciliation_histogram_retained_bytes(
    histogram: &NativeHistogram,
) -> Result<u64, UsageAccountingError> {
    let mut bytes = storage_reconciliation_allocation_retained_bytes(
        u64::try_from(std::mem::size_of::<NativeHistogram>()).map_err(|_| {
            UsageAccountingError::Limit(
                "usage storage reconciliation retained-memory model overflowed u64".to_string(),
            )
        })?,
    )?;
    for allocation in [
        modeled_storage_reconciliation_vec_retained_bytes::<HistogramBucketSpan>(
            histogram.negative_spans.capacity(),
        )?,
        modeled_storage_reconciliation_vec_retained_bytes::<i64>(
            histogram.negative_deltas.capacity(),
        )?,
        modeled_storage_reconciliation_vec_retained_bytes::<f64>(
            histogram.negative_counts.capacity(),
        )?,
        modeled_storage_reconciliation_vec_retained_bytes::<HistogramBucketSpan>(
            histogram.positive_spans.capacity(),
        )?,
        modeled_storage_reconciliation_vec_retained_bytes::<i64>(
            histogram.positive_deltas.capacity(),
        )?,
        modeled_storage_reconciliation_vec_retained_bytes::<f64>(
            histogram.positive_counts.capacity(),
        )?,
        modeled_storage_reconciliation_vec_retained_bytes::<f64>(
            histogram.custom_values.capacity(),
        )?,
    ] {
        storage_reconciliation_checked_add(&mut bytes, allocation)?;
    }
    Ok(bytes)
}

fn modeled_storage_reconciliation_vec_retained_bytes<T>(
    capacity: usize,
) -> Result<u64, UsageAccountingError> {
    if capacity == 0 {
        return Ok(0);
    }
    let capacity = u64::try_from(capacity).map_err(|_| {
        UsageAccountingError::Limit(
            "usage storage reconciliation retained-memory model overflowed u64".to_string(),
        )
    })?;
    let element_bytes = u64::try_from(std::mem::size_of::<T>()).map_err(|_| {
        UsageAccountingError::Limit(
            "usage storage reconciliation retained-memory model overflowed u64".to_string(),
        )
    })?;
    let bytes = capacity.checked_mul(element_bytes).ok_or_else(|| {
        UsageAccountingError::Limit(
            "usage storage reconciliation retained-memory model overflowed u64".to_string(),
        )
    })?;
    storage_reconciliation_allocation_retained_bytes(bytes)
}

fn modeled_storage_reconciliation_string_retained_bytes(
    capacity: usize,
) -> Result<u64, UsageAccountingError> {
    if capacity == 0 {
        return Ok(0);
    }
    storage_reconciliation_allocation_retained_bytes(u64::try_from(capacity).map_err(|_| {
        UsageAccountingError::Limit(
            "usage storage reconciliation retained-memory model overflowed u64".to_string(),
        )
    })?)
}

fn storage_reconciliation_allocation_retained_bytes(
    bytes: u64,
) -> Result<u64, UsageAccountingError> {
    bytes
        .checked_add(STORAGE_RECONCILE_ALLOCATION_ALLOWANCE_BYTES)
        .ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation retained-memory model overflowed u64".to_string(),
            )
        })
}

fn storage_reconciliation_checked_add(
    total: &mut u64,
    bytes: u64,
) -> Result<(), UsageAccountingError> {
    *total = total.checked_add(bytes).ok_or_else(|| {
        UsageAccountingError::Limit(
            "usage storage reconciliation retained-memory model overflowed u64".to_string(),
        )
    })?;
    Ok(())
}

fn run_storage_reconciliation_pass(
    storage: &Arc<dyn Storage>,
    plan: &StorageReconciliationPlan,
    scan_limits: StorageReconciliationScanLimits,
    execution: &QueryExecution,
) -> Result<Option<StorageReconciliationPassResult>, UsageAccountingError> {
    let mut state = StorageReconciliationPassState::new(plan);
    while let Some(cursor) = state.cursor {
        let request = state.page_request(scan_limits)?;
        let Some(page) = scan_storage_reconciliation_page(
            storage,
            plan,
            cursor,
            scan_limits,
            request,
            execution,
        )?
        else {
            return Ok(None);
        };
        state.apply_page(page, scan_limits)?;
    }
    Ok(Some(state.finish()))
}

fn scan_storage_reconciliation_page(
    storage: &Arc<dyn Storage>,
    plan: &StorageReconciliationPlan,
    cursor: StorageReconciliationCursor,
    scan_limits: StorageReconciliationScanLimits,
    request: StorageReconciliationPageRequest,
    execution: &QueryExecution,
) -> Result<Option<StorageReconciliationPage>, UsageAccountingError> {
    let chunk_end = cursor
        .series_offset
        .saturating_add(scan_limits.max_series_per_page)
        .min(plan.metrics().len());
    if cursor.series_offset >= chunk_end || request.max_rows == 0 || request.max_returned_bytes == 0
    {
        return Err(UsageAccountingError::Other(
            "usage storage reconciliation received an invalid page cursor or zero page limit"
                .to_string(),
        ));
    }
    let chunk = &plan.metrics()[cursor.series_offset..chunk_end];
    let chunk_series = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
    let execution_before = execution.snapshot();
    let mut detailed_page = storage
        .scan_series_rows_with_execution_result(
            chunk,
            i64::MIN,
            i64::MAX,
            QueryRowsScanOptions {
                max_rows: Some(request.max_rows),
                row_offset: (cursor.row_offset != 0).then_some(cursor.row_offset),
            },
            execution,
        )
        .map_err(|err| storage_reconciliation_storage_error("scan a bounded row page", err))?;
    let mut page_memory_reservation = detailed_page.take_memory_reservation().ok_or_else(|| {
        UsageAccountingError::Other(
            "usage storage reconciliation row page omitted its retained-memory reservation"
                .to_string(),
        )
    })?;
    let page = detailed_page.into_page();
    let retained_rows_bytes = modeled_storage_reconciliation_rows_retained_bytes(&page.rows)?;
    let page_accumulator_upper_bound =
        modeled_storage_reconciliation_page_accumulator_upper_bound(chunk)?;
    let retained_page_bytes = retained_rows_bytes
        .checked_add(page_accumulator_upper_bound)
        .ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation row-page memory model overflowed u64".to_string(),
            )
        })?;
    if retained_page_bytes > u64::try_from(scan_limits.max_page_memory_bytes).unwrap_or(u64::MAX) {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation row page retains {retained_page_bytes} modeled bytes, exceeding the per-page memory maximum {}",
            scan_limits.max_page_memory_bytes
        )));
    }
    page_memory_reservation
        .resize(retained_page_bytes)
        .map_err(|err| {
            storage_reconciliation_storage_error(
                "adopt the bounded row-page reservation",
                tsink::TsinkError::from(err),
            )
        })?;
    let execution_snapshot = execution.snapshot();
    let execution_delta =
        storage_reconciliation_execution_delta(execution_before, execution_snapshot)?;
    let expected_reserved_memory = execution_before
        .memory_reserved_bytes
        .checked_add(retained_page_bytes)
        .ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation row-page reservation overflowed u64".to_string(),
            )
        })?;
    if page_memory_reservation.bytes() != retained_page_bytes
        || execution_snapshot.memory_reserved_bytes < expected_reserved_memory
    {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation row-page reservation invariant failed after guarded adoption: requested {retained_page_bytes}, retained {}, expected execution total at least {expected_reserved_memory}, execution reports {}",
            page_memory_reservation.bytes(),
            execution_snapshot.memory_reserved_bytes
        )));
    }
    let row_count = u64::try_from(page.rows.len()).unwrap_or(u64::MAX);
    if page.rows.len() > request.max_rows || page.rows_scanned != row_count {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation backend violated the row-page contract: returned {} rows with rows_scanned={} under maximum {}",
            page.rows.len(),
            page.rows_scanned,
            request.max_rows
        )));
    }
    let modeled_bytes = modeled_write_batch_input_bytes(&page.rows).map_err(|err| {
        UsageAccountingError::Limit(format!(
            "usage storage reconciliation could not model row-page bytes: {err}"
        ))
    })?;
    if modeled_bytes > scan_limits.max_page_bytes
        || u64::try_from(modeled_bytes).unwrap_or(u64::MAX) > request.max_returned_bytes
    {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation page requires {modeled_bytes} modeled bytes, exceeding its byte limit {}",
            request.max_returned_bytes
        )));
    }
    let modeled_bytes_u64 = u64::try_from(modeled_bytes).unwrap_or(u64::MAX);
    if execution_delta.series_matched < chunk_series {
        // A series disappeared after the manifest was read. Discard this pass and retry from the
        // beginning; publishing its aggregate would expose a tenant prefix from mixed snapshots.
        return Ok(None);
    }
    if execution_delta.series_matched > chunk_series
        || execution_delta.samples_returned != row_count
        || execution_delta.samples_returned > u64::try_from(request.max_rows).unwrap_or(u64::MAX)
        || execution_delta.returned_bytes < modeled_bytes_u64
        || execution_delta.returned_bytes > request.max_returned_bytes
        || execution_delta.samples_scanned < page.rows_scanned
        || execution_delta.samples_scanned > request.max_samples_scanned
    {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation row page violated its per-page accounting bounds: returned {row_count} rows/{modeled_bytes_u64} modeled bytes, matched {}, charged {}/{}, scanned {}",
            execution_delta.series_matched,
            execution_delta.samples_returned,
            execution_delta.returned_bytes,
            execution_delta.samples_scanned
        )));
    }
    if cursor.expect_empty_probe
        && (page.truncated || page.next_row_offset.is_some() || !page.rows.is_empty())
    {
        // The preceding full page reported end-of-stream, but the exact terminal probe found
        // more work. Treat this as observed concurrent drift and restart the bounded attempt.
        return Ok(None);
    }

    let mut samples_per_tenant = BTreeMap::<String, u64>::new();
    let mut fingerprint_a = Xxh64::new(STORAGE_RECONCILE_FINGERPRINT_SEED_A);
    let mut fingerprint_b = Xxh64::new(STORAGE_RECONCILE_FINGERPRINT_SEED_B);
    for row in &page.rows {
        if !chunk
            .iter()
            .any(|series| series.name == row.metric() && series.labels.as_slice() == row.labels())
        {
            return Err(UsageAccountingError::Other(format!(
                "usage storage reconciliation backend returned unrequested series '{}'",
                row.metric()
            )));
        }
        let tenant_id = tenant_id_for_labels(row.labels()).to_string();
        let samples = samples_per_tenant.entry(tenant_id).or_default();
        *samples = samples.checked_add(1).ok_or_else(|| {
            UsageAccountingError::Limit(
                "usage storage reconciliation tenant page count overflowed u64".to_string(),
            )
        })?;
        update_storage_reconciliation_fingerprint(&mut fingerprint_a, &mut fingerprint_b, row)?;
    }
    let retained_page_accumulator =
        modeled_storage_reconciliation_page_accumulator_retained_bytes(&samples_per_tenant)?;
    if retained_page_accumulator > page_accumulator_upper_bound {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation page accumulator retained {retained_page_accumulator} bytes, exceeding its preflighted maximum {page_accumulator_upper_bound}"
        )));
    }

    let expected_next_offset = cursor.row_offset.checked_add(row_count).ok_or_else(|| {
        UsageAccountingError::Limit(
            "usage storage reconciliation row cursor overflowed u64".to_string(),
        )
    })?;
    let next_cursor = if page.truncated {
        if page.rows.is_empty() || page.next_row_offset != Some(expected_next_offset) {
            return Err(UsageAccountingError::Other(format!(
                "usage storage reconciliation backend returned a non-progressing page cursor: current={}, rows={}, next={:?}",
                cursor.row_offset,
                page.rows.len(),
                page.next_row_offset
            )));
        }
        Some(StorageReconciliationCursor {
            series_offset: cursor.series_offset,
            row_offset: expected_next_offset,
            expect_empty_probe: false,
        })
    } else if page.rows.len() == request.max_rows {
        if page.next_row_offset.is_some() {
            return Err(UsageAccountingError::Other(
                "usage storage reconciliation backend returned a terminal page with a continuation cursor"
                    .to_string(),
            ));
        }
        Some(StorageReconciliationCursor {
            series_offset: cursor.series_offset,
            row_offset: expected_next_offset,
            expect_empty_probe: true,
        })
    } else if chunk_end < plan.metrics().len() {
        if page.next_row_offset.is_some() {
            return Err(UsageAccountingError::Other(
                "usage storage reconciliation backend returned a partial terminal page with a continuation cursor"
                    .to_string(),
            ));
        }
        Some(StorageReconciliationCursor {
            series_offset: chunk_end,
            row_offset: 0,
            expect_empty_probe: false,
        })
    } else {
        if page.next_row_offset.is_some() {
            return Err(UsageAccountingError::Other(
                "usage storage reconciliation backend returned a final page with a continuation cursor"
                    .to_string(),
            ));
        }
        None
    };

    Ok(Some(StorageReconciliationPage {
        next_cursor,
        samples_per_tenant,
        rows: row_count,
        returned_bytes: execution_delta.returned_bytes,
        samples_scanned: execution_delta.samples_scanned,
        fingerprint: [fingerprint_a.digest(), fingerprint_b.digest()],
    }))
}

fn update_storage_reconciliation_fingerprint(
    fingerprint_a: &mut Xxh64,
    fingerprint_b: &mut Xxh64,
    row: &Row,
) -> Result<(), UsageAccountingError> {
    update_storage_reconciliation_len_prefixed(
        fingerprint_a,
        fingerprint_b,
        row.metric().as_bytes(),
    );
    update_storage_reconciliation_dual(
        fingerprint_a,
        fingerprint_b,
        &u64::try_from(row.labels().len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for label in row.labels() {
        update_storage_reconciliation_len_prefixed(
            fingerprint_a,
            fingerprint_b,
            label.name.as_bytes(),
        );
        update_storage_reconciliation_len_prefixed(
            fingerprint_a,
            fingerprint_b,
            label.value.as_bytes(),
        );
    }
    let mut writer = StorageReconciliationFingerprintWriter {
        fingerprint_a,
        fingerprint_b,
    };
    serde_json::to_writer(&mut writer, row.data_point()).map_err(|err| {
        UsageAccountingError::Other(format!(
            "usage storage reconciliation failed to fingerprint a row: {err}"
        ))
    })?;
    writer.write_all(&[0xff]).map_err(|err| {
        UsageAccountingError::Other(format!(
            "usage storage reconciliation failed to delimit a row fingerprint: {err}"
        ))
    })
}

fn update_storage_reconciliation_len_prefixed(
    fingerprint_a: &mut Xxh64,
    fingerprint_b: &mut Xxh64,
    bytes: &[u8],
) {
    update_storage_reconciliation_dual(
        fingerprint_a,
        fingerprint_b,
        &u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes(),
    );
    update_storage_reconciliation_dual(fingerprint_a, fingerprint_b, bytes);
}

fn update_storage_reconciliation_dual(
    fingerprint_a: &mut Xxh64,
    fingerprint_b: &mut Xxh64,
    bytes: &[u8],
) {
    fingerprint_a.update(bytes);
    fingerprint_b.update(bytes);
}

struct StorageReconciliationFingerprintWriter<'a> {
    fingerprint_a: &'a mut Xxh64,
    fingerprint_b: &'a mut Xxh64,
}

impl Write for StorageReconciliationFingerprintWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        update_storage_reconciliation_dual(self.fingerprint_a, self.fingerprint_b, bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn collect_storage_reconciliation_records_async(
    storage: Arc<dyn Storage>,
    limits: UsageLedgerLimits,
) -> Result<PreparedStorageReconciliation, UsageAccountingError> {
    let scan_limits = StorageReconciliationScanLimits::from_ledger_limits(limits)?;
    let execution = admit_storage_reconciliation_execution(&storage, scan_limits)?;
    for _attempt in 0..scan_limits.max_snapshot_attempts {
        let plan = {
            let storage = Arc::clone(&storage);
            let execution = execution.clone();
            reconciliation_blocking_task("manifest", move || {
                load_storage_reconciliation_plan(&storage, limits, scan_limits, &execution)
            })
            .await?
        };
        let Some(first_pass) = run_storage_reconciliation_pass_async(
            Arc::clone(&storage),
            plan.clone(),
            scan_limits,
            execution.clone(),
        )
        .await?
        else {
            continue;
        };
        let Some(second_pass) = run_storage_reconciliation_pass_async(
            Arc::clone(&storage),
            plan.clone(),
            scan_limits,
            execution.clone(),
        )
        .await?
        else {
            continue;
        };
        let final_manifest = {
            let storage = Arc::clone(&storage);
            let execution = execution.clone();
            reconciliation_blocking_task("final manifest", move || {
                load_storage_reconciliation_manifest(&storage, scan_limits, &execution)
            })
            .await?
        };
        if first_pass == second_pass && plan.metrics() == final_manifest.metrics.as_slice() {
            return storage_reconciliation_records(first_pass.per_tenant, &plan, limits);
        }
    }
    Err(storage_reconciliation_snapshot_error(scan_limits))
}

async fn run_storage_reconciliation_pass_async(
    storage: Arc<dyn Storage>,
    plan: StorageReconciliationPlan,
    scan_limits: StorageReconciliationScanLimits,
    execution: QueryExecution,
) -> Result<Option<StorageReconciliationPassResult>, UsageAccountingError> {
    let mut state = StorageReconciliationPassState::new(&plan);
    while let Some(cursor) = state.cursor {
        let request = state.page_request(scan_limits)?;
        let page = {
            let storage = Arc::clone(&storage);
            let plan = plan.clone();
            let execution = execution.clone();
            reconciliation_blocking_task("row page", move || {
                scan_storage_reconciliation_page(
                    &storage,
                    &plan,
                    cursor,
                    scan_limits,
                    request,
                    &execution,
                )
            })
            .await?
        };
        let Some(page) = page else {
            return Ok(None);
        };
        state.apply_page(page, scan_limits)?;
    }
    Ok(Some(state.finish()))
}

async fn reconciliation_blocking_task<T, F>(
    task_name: &'static str,
    task: F,
) -> Result<T, UsageAccountingError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, UsageAccountingError> + Send + 'static,
{
    match tokio::task::spawn_blocking(task).await {
        Ok(result) => result,
        Err(join_err) => Err(UsageAccountingError::Other(format!(
            "usage accounting reconciliation {task_name} task failed: {join_err}"
        ))),
    }
}

fn storage_reconciliation_records(
    per_tenant: BTreeMap<String, StorageAccumulator>,
    plan: &StorageReconciliationPlan,
    limits: UsageLedgerLimits,
) -> Result<PreparedStorageReconciliation, UsageAccountingError> {
    if per_tenant.len() > limits.max_batch_records {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation produced {} tenant records, exceeding the configured batch maximum {}",
            per_tenant.len(), limits.max_batch_records
        )));
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
    let retained_records = modeled_storage_reconciliation_records_retained_bytes(&records)?;
    if retained_records > plan.prepared_records_retained_upper_bound {
        return Err(UsageAccountingError::Limit(format!(
            "usage storage reconciliation prepared-record retained bytes {retained_records} exceeded the preflighted maximum {}",
            plan.prepared_records_retained_upper_bound
        )));
    }
    if plan.manifest.operation_memory_reservation.bytes() != plan.manifest.operation_retained_bytes
    {
        return Err(UsageAccountingError::Other(format!(
            "usage storage reconciliation operation reservation changed before append: expected {}, retained {}",
            plan.manifest.operation_retained_bytes,
            plan.manifest.operation_memory_reservation.bytes()
        )));
    }
    Ok(PreparedStorageReconciliation {
        records,
        _operation_memory: Arc::clone(&plan.manifest),
    })
}

fn storage_reconciliation_snapshot_error(
    scan_limits: StorageReconciliationScanLimits,
) -> UsageAccountingError {
    UsageAccountingError::Other(format!(
        "usage storage reconciliation could not observe two identical bounded passes and an unchanged metric manifest in {} attempts; no storage records were published",
        scan_limits.max_snapshot_attempts
    ))
}

fn storage_reconciliation_storage_error(
    operation: &str,
    err: tsink::TsinkError,
) -> UsageAccountingError {
    if matches!(&err, tsink::TsinkError::QueryBudget(_)) {
        UsageAccountingError::Limit(format!(
            "usage storage reconciliation could not {operation} within its finite work limits: {err}"
        ))
    } else {
        UsageAccountingError::Other(format!(
            "usage storage reconciliation failed to {operation}: {err}"
        ))
    }
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

#[derive(Debug, Clone, Default)]
struct SummaryAccumulator {
    ingest: UsageTotals,
    query: UsageTotals,
    retention: UsageTotals,
    background: UsageTotals,
    latest_storage_snapshot: Option<UsageStorageSnapshot>,
    latest_storage_sequence: u64,
    storage_events_total: u64,
}

impl SummaryAccumulator {
    fn apply_all_time(&mut self, record: &UsageLedgerRecord) {
        self.apply_usage(record);
    }

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
        self.storage_events_total = self.storage_events_total.saturating_add(1);
        let replace = self
            .latest_storage_snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot.reconciled_unix_ms < record.unix_ms
                    || (snapshot.reconciled_unix_ms == record.unix_ms
                        && self.latest_storage_sequence <= record.seq)
            })
            .unwrap_or(true);
        if replace {
            self.latest_storage_snapshot = Some(UsageStorageSnapshot::from(record));
            self.latest_storage_sequence = record.seq;
        }
    }

    fn to_summary(&self, tenant_id: String) -> UsageTenantSummary {
        self.clone().into_summary(tenant_id)
    }

    fn records_total(&self) -> u64 {
        self.ingest
            .events_total
            .saturating_add(self.query.events_total)
            .saturating_add(self.retention.events_total)
            .saturating_add(self.background.events_total)
            .saturating_add(self.storage_events_total)
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

fn load_usage_ledger(path: &Path, limits: UsageLedgerLimits) -> Result<UsageLedgerState, String> {
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
        file.seek(SeekFrom::Start(file_len - 1)).map_err(|err| {
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

    // Read exactly the length inspected above. This deliberately does not follow concurrent file
    // growth, and the bounded line buffer rejects an oversized frame before extending past the
    // configured allocation ceiling.
    let mut reader = BufReader::with_capacity(
        limits
            .max_line_bytes
            .min(USAGE_LEDGER_STARTUP_READER_MAX_BYTES),
        file.take(file_len),
    );
    let mut line = Vec::with_capacity(limits.max_line_bytes);
    let mut state = UsageLedgerState::default();
    let mut recent_by_sequence = BTreeMap::<u64, UsageLedgerRecord>::new();
    let mut seen_sequences = SequenceRanges::new(limits.max_sequence_ranges);
    let mut line_number = 0u64;
    loop {
        line.clear();
        let has_line = read_bounded_usage_line(&mut reader, &mut line, limits.max_line_bytes)
            .map_err(|err| {
                format!(
                    "failed to read usage ledger line {} from {}: {err}",
                    line_number.saturating_add(1),
                    path.display()
                )
            })?;
        if !has_line {
            break;
        }
        line_number = line_number
            .checked_add(1)
            .ok_or_else(|| format!("usage ledger {} has too many lines", path.display()))?;
        let frame = line.strip_suffix(b"\n").unwrap_or(&line);
        if frame.len() > limits.max_frame_bytes {
            return Err(format!(
                "usage ledger line {line_number} from {} is {} frame bytes, exceeding the configured maximum {}",
                path.display(),
                frame.len(),
                limits.max_frame_bytes
            ));
        }
        if frame.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let persisted =
            decode_persisted_usage_line(frame, limits.max_batch_records).map_err(|err| {
                format!(
                    "failed to parse usage ledger line {line_number} from {}: {err}",
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
                        line_number
                    ));
                }
                if batch.schema_version != USAGE_LEDGER_BATCH_SCHEMA_VERSION {
                    return Err(format!(
                        "usage ledger {} has unsupported batch schema version {} on line {}",
                        path.display(),
                        batch.schema_version,
                        line_number
                    ));
                }
                if batch.records.is_empty() {
                    return Err(format!(
                        "usage ledger {} has an empty record batch on line {}",
                        path.display(),
                        line_number
                    ));
                }
                if batch.records.len() > limits.max_batch_records {
                    return Err(format!(
                        "usage ledger {} has {} records on line {}, exceeding the configured batch maximum {}",
                        path.display(),
                        batch.records.len(),
                        line_number,
                        limits.max_batch_records
                    ));
                }
                batch.records
            }
        };
        for record in line_records {
            validate_usage_record_size(&record, limits.max_record_bytes).map_err(|message| {
                format!(
                    "usage ledger {} has an oversized record on line {}: {message}",
                    path.display(),
                    line_number
                )
            })?;
            let is_new_sequence = seen_sequences.insert(record.seq).map_err(|err| {
                format!(
                    "usage ledger {} cannot validate sequence {} on line {}: {err}",
                    path.display(),
                    record.seq,
                    line_number
                )
            })?;
            if record.seq == 0 || !is_new_sequence {
                return Err(format!(
                    "usage ledger {} has an invalid or duplicate sequence {} on line {}",
                    path.display(),
                    record.seq,
                    line_number
                ));
            }
            if !state.tenant_summaries.contains_key(&record.tenant_id)
                && state.tenant_summaries.len() >= limits.max_tenants
            {
                return Err(format!(
                    "usage ledger {} exceeds the configured tenant limit {} on line {}",
                    path.display(),
                    limits.max_tenants,
                    line_number
                ));
            }
            state.records_total = state.records_total.checked_add(1).ok_or_else(|| {
                format!("usage ledger {} record count overflowed", path.display())
            })?;
            if record.category == UsageCategory::Storage {
                state.storage_reconciliations_total = state
                    .storage_reconciliations_total
                    .checked_add(1)
                    .ok_or_else(|| {
                        format!(
                            "usage ledger {} storage reconciliation count overflowed",
                            path.display()
                        )
                    })?;
            }
            if record.seq >= state.last_sequence {
                state.last_sequence = record.seq;
                state.last_record_unix_ms = Some(record.unix_ms);
            }
            state
                .tenant_summaries
                .entry(record.tenant_id.clone())
                .or_default()
                .apply_all_time(&record);
            recent_by_sequence.insert(record.seq, record);
            while recent_by_sequence.len() > limits.recent_records {
                let Some(oldest) = recent_by_sequence.keys().next().copied() else {
                    break;
                };
                recent_by_sequence.remove(&oldest);
            }
        }
    }
    state.next_seq = state.last_sequence.checked_add(1).ok_or_else(|| {
        format!(
            "usage ledger {} exhausted its sequence space",
            path.display()
        )
    })?;
    state.records = recent_by_sequence.into_values().collect();
    Ok(state)
}

fn decode_persisted_usage_line(
    frame: &[u8],
    max_batch_records: usize,
) -> Result<PersistedUsageLedgerLine, String> {
    // Try the legacy record shape directly. Unlike serde's generic untagged representation, this
    // does not materialize an allocation-heavy intermediate tree for every startup line.
    if let Ok(record) = serde_json::from_slice::<UsageLedgerRecord>(frame) {
        return Ok(PersistedUsageLedgerLine::Record(record));
    }
    validate_records_array_count_before_decode(frame, max_batch_records)?;
    serde_json::from_slice::<PersistedUsageLedgerBatch>(frame)
        .map(PersistedUsageLedgerLine::Batch)
        .map_err(|err| err.to_string())
}

fn validate_records_array_count_before_decode(frame: &[u8], maximum: usize) -> Result<(), String> {
    let mut index = 0usize;
    let mut object_depth = 0usize;
    let mut array_depth = 0usize;
    while index < frame.len() {
        match frame[index] {
            b'"' => {
                let end = scan_json_string_end(frame, index)?;
                if object_depth == 1 && array_depth == 0 {
                    let mut after = end;
                    while after < frame.len() && frame[after].is_ascii_whitespace() {
                        after += 1;
                    }
                    if after < frame.len() && frame[after] == b':' {
                        let token = &frame[index..end];
                        let is_records = token == b"\"records\""
                            || (token.contains(&b'\\')
                                && serde_json::from_slice::<String>(token)
                                    .map(|key| key == "records")
                                    .unwrap_or(false));
                        if is_records {
                            after += 1;
                            while after < frame.len() && frame[after].is_ascii_whitespace() {
                                after += 1;
                            }
                            if after < frame.len() && frame[after] == b'[' {
                                let (_, end) = count_bounded_json_array(frame, after, maximum)?;
                                index = end;
                                continue;
                            }
                        }
                    }
                }
                index = end;
                continue;
            }
            b'{' => object_depth = object_depth.saturating_add(1),
            b'}' => object_depth = object_depth.saturating_sub(1),
            b'[' => array_depth = array_depth.saturating_add(1),
            b']' => array_depth = array_depth.saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
    Ok(())
}

fn scan_json_string_end(frame: &[u8], start: usize) -> Result<usize, String> {
    let mut index = start.saturating_add(1);
    while index < frame.len() {
        match frame[index] {
            b'"' => return Ok(index + 1),
            b'\\' => index = index.saturating_add(2),
            _ => index += 1,
        }
    }
    Err("unterminated JSON string".to_string())
}

fn count_bounded_json_array(
    frame: &[u8],
    start: usize,
    maximum: usize,
) -> Result<(usize, usize), String> {
    let mut index = start + 1;
    while index < frame.len() && frame[index].is_ascii_whitespace() {
        index += 1;
    }
    if index < frame.len() && frame[index] == b']' {
        return Ok((0, index + 1));
    }
    let mut count = 1usize;
    if count > maximum {
        return Err(format!(
            "usage ledger batch contains more than the configured {maximum} records"
        ));
    }
    let mut array_depth = 1usize;
    let mut object_depth = 0usize;
    while index < frame.len() {
        match frame[index] {
            b'"' => {
                index = scan_json_string_end(frame, index)?;
                continue;
            }
            b'[' => array_depth = array_depth.saturating_add(1),
            b']' => {
                array_depth = array_depth.saturating_sub(1);
                if array_depth == 0 {
                    return Ok((count, index + 1));
                }
            }
            b'{' => object_depth = object_depth.saturating_add(1),
            b'}' => object_depth = object_depth.saturating_sub(1),
            b',' if array_depth == 1 && object_depth == 0 => {
                count = count.checked_add(1).ok_or_else(|| {
                    "usage ledger batch record count overflowed usize".to_string()
                })?;
                if count > maximum {
                    return Err(format!(
                        "usage ledger batch contains more than the configured {maximum} records"
                    ));
                }
            }
            _ => {}
        }
        index += 1;
    }
    Ok((count, index))
}

fn read_bounded_usage_line<R: BufRead>(
    reader: &mut R,
    output: &mut Vec<u8>,
    max_line_bytes: usize,
) -> std::io::Result<bool> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(!output.is_empty());
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| position + 1)
            .unwrap_or(available.len());
        if take > max_line_bytes.saturating_sub(output.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line exceeds configured maximum {max_line_bytes} bytes"),
            ));
        }
        output.extend_from_slice(&available[..take]);
        let found_newline = available[take - 1] == b'\n';
        reader.consume(take);
        if found_newline {
            return Ok(true);
        }
    }
}

#[derive(Debug)]
struct SequenceRanges {
    ranges: BTreeMap<u64, u64>,
    maximum_ranges: usize,
}

impl SequenceRanges {
    fn new(maximum_ranges: usize) -> Self {
        Self {
            ranges: BTreeMap::new(),
            maximum_ranges,
        }
    }

    fn insert(&mut self, sequence: u64) -> Result<bool, String> {
        let previous = self
            .ranges
            .range(..=sequence)
            .next_back()
            .map(|(start, end)| (*start, *end));
        if previous.is_some_and(|(_, end)| sequence <= end) {
            return Ok(false);
        }
        let next = self
            .ranges
            .range(sequence..)
            .next()
            .map(|(start, end)| (*start, *end));
        let joins_previous = previous.is_some_and(|(_, end)| end.checked_add(1) == Some(sequence));
        let joins_next = next.is_some_and(|(start, _)| sequence.checked_add(1) == Some(start));

        match (joins_previous, joins_next) {
            (true, true) => {
                let (previous_start, _) = previous.expect("joined previous range must exist");
                let (next_start, next_end) = next.expect("joined next range must exist");
                self.ranges.insert(previous_start, next_end);
                self.ranges.remove(&next_start);
            }
            (true, false) => {
                let (previous_start, _) = previous.expect("joined previous range must exist");
                self.ranges.insert(previous_start, sequence);
            }
            (false, true) => {
                let (next_start, next_end) = next.expect("joined next range must exist");
                self.ranges.remove(&next_start);
                self.ranges.insert(sequence, next_end);
            }
            (false, false) => {
                if self.ranges.len() == self.maximum_ranges {
                    return Err(format!(
                        "usage ledger sequence validation requires more than the configured {} disjoint ranges",
                        self.maximum_ranges
                    ));
                }
                self.ranges.insert(sequence, sequence);
            }
        }
        Ok(true)
    }
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

fn ledger_status_from_state(
    ledger_path: Option<&Path>,
    state: &UsageLedgerState,
    health: &UsageLedgerHealth,
    limits: UsageLedgerLimits,
) -> UsageLedgerStatus {
    UsageLedgerStatus {
        durable: ledger_path.is_some(),
        ledger_path: ledger_path.map(|path| path.display().to_string()),
        records_total: state.records_total,
        retained_records: state.records.len() as u64,
        earliest_retained_sequence: state.records.front().map(|record| record.seq),
        tenant_count: state.tenant_summaries.len() as u64,
        last_sequence: state.last_sequence,
        last_record_unix_ms: state.last_record_unix_ms,
        storage_reconciliations_total: state.storage_reconciliations_total,
        record_failures_total: health.record_failures_total,
        last_record_error_code: health.last_record_error_code.clone(),
        limits,
    }
}

fn resolve_read_window(
    state: &UsageLedgerState,
    options: UsageReadOptions,
) -> Result<(u64, Option<u64>, u64), UsageReadError> {
    let snapshot_sequence = options.snapshot_sequence.unwrap_or(state.last_sequence);
    if snapshot_sequence > state.last_sequence {
        return Err(UsageReadError::InvalidSnapshot {
            requested: snapshot_sequence,
            latest: state.last_sequence,
        });
    }
    let earliest_available = state.records.front().map(|record| record.seq);
    let default_after = earliest_available
        .map(|sequence| {
            if snapshot_sequence < sequence {
                snapshot_sequence.saturating_sub(1)
            } else {
                sequence.saturating_sub(1)
            }
        })
        .unwrap_or(snapshot_sequence);
    let after_sequence = options.after_sequence.unwrap_or(default_after);
    if let Some(earliest) = earliest_available {
        if after_sequence < earliest.saturating_sub(1) && after_sequence < snapshot_sequence {
            return Err(UsageReadError::CursorExpired {
                requested_after: after_sequence,
                earliest_available: earliest,
            });
        }
    }
    Ok((snapshot_sequence, earliest_available, after_sequence))
}

fn validate_append_tenants(
    state: &UsageLedgerState,
    records: &[UsageLedgerRecord],
    limits: UsageLedgerLimits,
) -> Result<(), UsageAccountingError> {
    let record_count = u64::try_from(records.len()).map_err(|_| {
        UsageAccountingError::Other("usage ledger batch length exceeds u64".to_string())
    })?;
    state
        .records_total
        .checked_add(record_count)
        .ok_or_else(|| {
            UsageAccountingError::Other("usage ledger record count is exhausted".to_string())
        })?;
    let storage_count = records
        .iter()
        .filter(|record| record.category == UsageCategory::Storage)
        .count();
    let storage_count = u64::try_from(storage_count).map_err(|_| {
        UsageAccountingError::Other("usage ledger storage batch length exceeds u64".to_string())
    })?;
    state
        .storage_reconciliations_total
        .checked_add(storage_count)
        .ok_or_else(|| {
            UsageAccountingError::Other(
                "usage ledger storage reconciliation count is exhausted".to_string(),
            )
        })?;
    let mut new_tenants = BTreeMap::<&str, ()>::new();
    for record in records {
        if !state.tenant_summaries.contains_key(&record.tenant_id) {
            new_tenants.insert(record.tenant_id.as_str(), ());
        }
    }
    if state
        .tenant_summaries
        .len()
        .saturating_add(new_tenants.len())
        > limits.max_tenants
    {
        return Err(UsageAccountingError::Limit(format!(
            "usage ledger tenant limit {} would be exceeded by this batch",
            limits.max_tenants
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct CappedWriter {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl CappedWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(8 * 1024)),
            maximum,
            exceeded: false,
        }
    }
}

impl Write for CappedWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.maximum.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "encoded value exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct CountingWriter {
    bytes: usize,
    maximum: usize,
    exceeded: bool,
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.maximum.saturating_sub(self.bytes) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "encoded value exceeds configured limit",
            ));
        }
        self.bytes += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_usage_record_size(record: &UsageLedgerRecord, maximum: usize) -> Result<(), String> {
    let mut writer = CountingWriter {
        bytes: 0,
        maximum,
        exceeded: false,
    };
    if let Err(err) = serde_json::to_writer(&mut writer, record) {
        if writer.exceeded {
            return Err(format!(
                "usage record sequence {} exceeds the configured maximum {maximum} bytes",
                record.seq
            ));
        }
        return Err(format!("failed to encode usage record: {err}"));
    }
    Ok(())
}

fn encode_usage_frame(
    records: &[UsageLedgerRecord],
    limits: UsageLedgerLimits,
) -> Result<Vec<u8>, UsageAccountingError> {
    let mut writer = CappedWriter::new(limits.max_frame_bytes);
    let result = if records.len() == 1 {
        serde_json::to_writer(&mut writer, &records[0])
    } else {
        serde_json::to_writer(
            &mut writer,
            &PersistedUsageLedgerBatchRef {
                magic: USAGE_LEDGER_BATCH_MAGIC,
                schema_version: USAGE_LEDGER_BATCH_SCHEMA_VERSION,
                records,
            },
        )
    };
    if let Err(err) = result {
        if writer.exceeded {
            return Err(UsageAccountingError::Limit(format!(
                "usage ledger frame exceeds the configured maximum {} bytes",
                limits.max_frame_bytes
            )));
        }
        return Err(UsageAccountingError::Other(format!(
            "failed to encode usage ledger frame: {err}"
        )));
    }
    if writer.bytes.len().saturating_add(1) > limits.max_line_bytes {
        return Err(UsageAccountingError::Limit(format!(
            "usage ledger line exceeds the configured maximum {} bytes",
            limits.max_line_bytes
        )));
    }
    writer.bytes.push(b'\n');
    Ok(writer.bytes)
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
    tenant_id_for_labels(&series.labels).to_string()
}

fn tenant_id_for_labels(labels: &[Label]) -> &str {
    labels
        .iter()
        .find(|label| label.name == tenant::TENANT_LABEL)
        .map(|label| label.value.as_str())
        .unwrap_or(tenant::DEFAULT_TENANT_ID)
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex as StdMutex};
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;
    use tsink::{
        DataPoint, Label, LocalDiskLimits, QueryBudget, QueryBudgetError, QueryBudgetLimits,
        QueryLimitReason, QueryOptions, Row, SeriesPoints, StorageBuilder, TimestampPrecision,
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

    fn populated_usage_status_accounting(data_path: &Path) -> Arc<UsageAccounting> {
        let accounting =
            UsageAccounting::open(Some(data_path)).expect("usage status fixture should open");

        let mut ingest =
            UsageRecordInput::success("team-a", UsageCategory::Ingest, "write", "status-test");
        ingest.rows = 17;
        accounting
            .record(ingest)
            .expect("usage status ingest record should append");

        let mut query =
            UsageRecordInput::success("team-a", UsageCategory::Query, "query", "status-test");
        query.result_units = 23;
        accounting
            .record(query)
            .expect("usage status query record should append");

        let mut retention = UsageRecordInput::success(
            "team-a",
            UsageCategory::Retention,
            "retention",
            "status-test",
        );
        retention.tombstones_applied = 5;
        accounting
            .record(retention)
            .expect("usage status retention record should append");

        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "background",
                "status-test",
            ))
            .expect("usage status background record should append");

        let mut storage =
            UsageRecordInput::success("team-a", UsageCategory::Storage, "reconcile", "status-test");
        storage.logical_storage_series = 7;
        storage.logical_storage_samples = 31;
        storage.logical_storage_bytes = 8_192;
        accounting
            .record(storage)
            .expect("usage status storage record should append");

        let mut other_tenant =
            UsageRecordInput::success("team-b", UsageCategory::Ingest, "write", "status-test");
        other_tenant.rows = 101;
        accounting
            .record(other_tenant)
            .expect("usage status other-tenant record should append");

        {
            let mut health = accounting
                .inner
                .health
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            health.record_failures_total = 9;
            health.last_record_error_code =
                Some("usage_ledger_status_projection_fixture_error".to_string());
        }
        accounting
    }

    fn status_reconciliation_from_report(
        report: &UsageReport,
    ) -> UsageStatusReconciliationSnapshot {
        UsageStatusReconciliationSnapshot {
            ingest_rows_total: report.tenants.iter().map(|tenant| tenant.ingest.rows).sum(),
            query_result_units_total: report
                .tenants
                .iter()
                .map(|tenant| tenant.query.result_units)
                .sum(),
            retention_tombstones_applied_total: report
                .tenants
                .iter()
                .map(|tenant| tenant.retention.tombstones_applied)
                .sum(),
            background_events_total: report
                .tenants
                .iter()
                .map(|tenant| tenant.background.events_total)
                .sum(),
            latest_storage_logical_bytes: report
                .tenants
                .iter()
                .filter_map(|tenant| tenant.latest_storage_snapshot.as_ref())
                .map(|storage| storage.logical_storage_bytes)
                .sum(),
            latest_storage_reconciled_unix_ms: report
                .tenants
                .iter()
                .filter_map(|tenant| {
                    tenant
                        .latest_storage_snapshot
                        .as_ref()
                        .map(|storage| storage.reconciled_unix_ms)
                })
                .max(),
        }
    }

    struct ObservedReconciliationStorage {
        inner: Arc<dyn Storage>,
        list_metrics_entered: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
        list_metrics_gate: Option<Arc<ReconciliationScanGate>>,
        select_many_completed: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl Storage for ObservedReconciliationStorage {
        fn query_budget(&self) -> Option<tsink::QueryBudget> {
            self.inner.query_budget()
        }

        fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.select_series_execution_accounting()
        }

        fn scan_series_rows_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.scan_series_rows_execution_accounting()
        }

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

        fn list_metrics_with_execution(
            &self,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<Vec<MetricSeries>> {
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
            self.inner.list_metrics_with_execution(execution)
        }

        fn select_series_with_execution_result(
            &self,
            selection: &SeriesSelection,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::SelectSeriesExecutionResult> {
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
            self.inner
                .select_series_with_execution_result(selection, execution)
        }

        fn scan_series_rows_with_execution(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::QueryRowsPage> {
            let result = self
                .inner
                .scan_series_rows_with_execution(series, start, end, options, execution);
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

        fn scan_series_rows_with_execution_result(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::QueryRowsExecutionResult> {
            let result = self
                .inner
                .scan_series_rows_with_execution_result(series, start, end, options, execution);
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

        fn close(&self) -> tsink::Result<()> {
            self.inner.close()
        }
    }

    struct MutateAfterFirstReconciliationPageStorage {
        inner: Arc<dyn Storage>,
        mutation: Vec<Row>,
        mutated: AtomicBool,
    }

    impl Storage for MutateAfterFirstReconciliationPageStorage {
        fn query_budget(&self) -> Option<tsink::QueryBudget> {
            self.inner.query_budget()
        }

        fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.select_series_execution_accounting()
        }

        fn scan_series_rows_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.scan_series_rows_execution_accounting()
        }

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
            self.inner.select_many(series, start, end)
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
            self.inner.list_metrics()
        }

        fn list_metrics_with_execution(
            &self,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<Vec<MetricSeries>> {
            self.inner.list_metrics_with_execution(execution)
        }

        fn select_series_with_execution_result(
            &self,
            selection: &SeriesSelection,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::SelectSeriesExecutionResult> {
            self.inner
                .select_series_with_execution_result(selection, execution)
        }

        fn scan_series_rows_with_execution(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::QueryRowsPage> {
            let page = self
                .inner
                .scan_series_rows_with_execution(series, start, end, options, execution)?;
            if !self.mutated.swap(true, Ordering::SeqCst) {
                self.inner.insert_rows(&self.mutation)?;
            }
            Ok(page)
        }

        fn scan_series_rows_with_execution_result(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::QueryRowsExecutionResult> {
            let page = self
                .inner
                .scan_series_rows_with_execution_result(series, start, end, options, execution)?;
            if !self.mutated.swap(true, Ordering::SeqCst) {
                self.inner.insert_rows(&self.mutation)?;
            }
            Ok(page)
        }

        fn close(&self) -> tsink::Result<()> {
            self.inner.close()
        }
    }

    struct MissingReconciliationGuardStorage {
        inner: Arc<dyn Storage>,
        omit_manifest_guard: bool,
        omit_row_guard: bool,
    }

    impl Storage for MissingReconciliationGuardStorage {
        fn query_budget(&self) -> Option<tsink::QueryBudget> {
            self.inner.query_budget()
        }

        fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
            QueryExecutionAccounting::Complete
        }

        fn scan_series_rows_execution_accounting(&self) -> QueryExecutionAccounting {
            QueryExecutionAccounting::Complete
        }

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
            self.inner.select_many(series, start, end)
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

        fn select_series_with_execution_result(
            &self,
            selection: &SeriesSelection,
            execution: &QueryExecution,
        ) -> tsink::Result<tsink::SelectSeriesExecutionResult> {
            if self.omit_manifest_guard {
                return self
                    .inner
                    .select_series_with_execution(selection, execution)
                    .map(tsink::SelectSeriesExecutionResult::unaccounted);
            }
            self.inner
                .select_series_with_execution_result(selection, execution)
        }

        fn scan_series_rows_with_execution_result(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &QueryExecution,
        ) -> tsink::Result<tsink::QueryRowsExecutionResult> {
            if self.omit_row_guard {
                return self
                    .inner
                    .scan_series_rows_with_execution(series, start, end, options, execution)
                    .map(tsink::QueryRowsExecutionResult::unaccounted);
            }
            self.inner
                .scan_series_rows_with_execution_result(series, start, end, options, execution)
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

    fn make_scoped_single_series_rows(count: usize) -> Vec<Row> {
        tenant::scope_rows_for_tenant(
            (0..count)
                .map(|index| {
                    Row::with_labels(
                        "bounded_reconciliation",
                        vec![Label::new("host", "a")],
                        DataPoint::new(
                            i64::try_from(index).expect("test timestamp should fit"),
                            index as f64,
                        ),
                    )
                })
                .collect(),
            "team-a",
        )
        .expect("rows should scope")
    }

    fn make_storage_with_rows(rows: &[Row]) -> Arc<dyn Storage> {
        let storage = make_storage();
        storage.insert_rows(rows).expect("rows should insert");
        storage
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
    fn usage_accounting_metrics_snapshot_is_copy_and_default() {
        fn assert_copy_default<T: Copy + Default>() {}

        assert_copy_default::<UsageAccountingMetricsSnapshot>();
        assert!(!std::mem::needs_drop::<UsageAccountingMetricsSnapshot>());
        assert_eq!(
            UsageAccountingMetricsSnapshot::default(),
            UsageAccountingMetricsSnapshot {
                durable: false,
                records_total: 0,
                retained_records: 0,
                earliest_retained_sequence: None,
                recent_record_limit: 0,
                tenant_count: 0,
                tenant_limit: 0,
                storage_reconciliations_total: 0,
                record_failures_total: 0,
            }
        );
    }

    #[test]
    fn usage_accounting_metrics_snapshot_projects_scrape_scalars_without_status_strings() {
        let dir = tempdir().expect("temp dir should build");
        let limits = UsageLedgerLimits {
            recent_records: 2,
            max_tenants: 4,
            ..UsageLedgerLimits::default()
        };
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
                .expect("usage store should open");

        for tenant_id in ["team-a", "team-b", "team-a"] {
            accounting
                .record(UsageRecordInput::success(
                    tenant_id,
                    UsageCategory::Query,
                    "instant_query",
                    "/api/v1/query",
                ))
                .expect("usage record should append");
        }
        accounting
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .storage_reconciliations_total = 5;
        {
            let mut health = accounting
                .inner
                .health
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            health.record_failures_total = 7;
            health.last_record_error_code = Some("usage_ledger_test_error".to_string());
        }

        let snapshot = accounting.metrics_snapshot();
        assert_eq!(
            snapshot,
            UsageAccountingMetricsSnapshot {
                durable: true,
                records_total: 3,
                retained_records: 2,
                earliest_retained_sequence: Some(2),
                recent_record_limit: 2,
                tenant_count: 2,
                tenant_limit: 4,
                storage_reconciliations_total: 5,
                record_failures_total: 7,
            }
        );

        let legacy_status = accounting.ledger_status();
        assert!(legacy_status.ledger_path.is_some());
        assert_eq!(
            legacy_status.last_record_error_code.as_deref(),
            Some("usage_ledger_test_error")
        );
    }

    #[test]
    fn usage_status_projection_preserves_legacy_schema_values_and_absent_tenant_defaults() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = populated_usage_status_accounting(dir.path());
        let expected_journal = accounting.ledger_status();
        let expected_current = accounting.tenant_summary("team-a");
        let expected_report = accounting.report(Some("team-a"), None, None, UsageBucketWidth::None);
        accounting.reset_status_snapshot_string_clones();

        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("status budget should build");
        let execution = budget.begin_query().expect("status query should admit");
        let projected = accounting
            .status_snapshot_for_with_execution("team-a", &execution)
            .expect("accounted usage status should build");

        assert_eq!(projected.journal, expected_journal);
        assert_eq!(projected.current_tenant, expected_current);
        assert_eq!(
            projected.reconciliation,
            status_reconciliation_from_report(&expected_report),
            "the scalar projection must exactly reproduce the filtered all-time report inputs"
        );
        assert_eq!(projected.reconciliation.ingest_rows_total, 17);
        assert_eq!(projected.reconciliation.query_result_units_total, 23);
        assert_eq!(
            projected.reconciliation.retention_tombstones_applied_total,
            5
        );
        assert_eq!(projected.reconciliation.background_events_total, 1);
        assert_eq!(projected.reconciliation.latest_storage_logical_bytes, 8_192);
        assert_eq!(
            accounting.status_snapshot_string_clones(),
            4,
            "durable path, health error, tenant id, and storage tenant id should each copy once"
        );
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            projected.accounted_bytes()
        );
        drop(projected);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);

        let expected_absent_current = accounting.tenant_summary("missing-tenant");
        let expected_absent_report =
            accounting.report(Some("missing-tenant"), None, None, UsageBucketWidth::None);
        accounting.reset_status_snapshot_string_clones();
        let absent = accounting
            .status_snapshot_for_with_execution("missing-tenant", &execution)
            .expect("absent tenant usage status should still build");
        assert_eq!(absent.journal, expected_journal);
        assert_eq!(absent.current_tenant, expected_absent_current);
        assert_eq!(
            absent.reconciliation,
            status_reconciliation_from_report(&expected_absent_report)
        );
        assert_eq!(
            absent.reconciliation,
            UsageStatusReconciliationSnapshot::default()
        );
        assert_eq!(
            accounting.status_snapshot_string_clones(),
            3,
            "an absent tenant should copy only the path, error code, and requested tenant id"
        );
        drop(absent);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn usage_status_projection_enforces_exact_peak_before_output_clones() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = populated_usage_status_accounting(dir.path());
        accounting.reset_status_snapshot_string_clones();

        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = accounting
            .status_snapshot_for_with_execution("team-a", &calibration)
            .expect("calibration usage status should build");
        let retained_bytes = calibrated.accounted_bytes();
        let peak_bytes = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(retained_bytes > 0);
        assert_eq!(
            peak_bytes, retained_bytes,
            "UTF-8 ledger paths and exact-capacity strings need no transient over-reservation"
        );
        assert_eq!(accounting.status_snapshot_string_clones(), 4);
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibration_released = calibration_budget.snapshot();
        assert_eq!(calibration_released.active_queries, 0);
        assert_eq!(calibration_released.shared_reserved_memory_bytes, 0);
        assert_eq!(
            calibration_released.accounting_invariant_violations_total,
            0
        );

        accounting.reset_status_snapshot_string_clones();
        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(peak_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(peak_bytes),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact usage status budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let exact_snapshot = accounting
            .status_snapshot_for_with_execution("team-a", &exact)
            .expect("the exact modeled usage status peak should pass");
        assert_eq!(exact_snapshot.accounted_bytes(), retained_bytes);
        assert_eq!(accounting.status_snapshot_string_clones(), 4);
        drop(exact_snapshot);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        let exact_released = exact_budget.snapshot();
        assert_eq!(exact_released.active_queries, 0);
        assert_eq!(exact_released.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_released.accounting_invariant_violations_total, 0);

        accounting.reset_status_snapshot_string_clones();
        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(peak_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(peak_bytes.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under usage status budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = accounting
            .status_snapshot_for_with_execution("team-a", &one_under)
            .expect_err("one byte below the usage status peak must reject");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, peak_bytes);
            }
            other => panic!("unexpected usage status projection error: {other}"),
        }
        assert_eq!(
            accounting.status_snapshot_string_clones(),
            0,
            "failed admission must precede every retained output copy"
        );
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            one_under_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            0
        );
        drop(one_under);
        let one_under_released = one_under_budget.snapshot();
        assert_eq!(one_under_released.active_queries, 0);
        assert_eq!(one_under_released.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn usage_status_projection_honors_precancellation_without_residual_memory() {
        let dir = tempdir().expect("temp dir should build");
        let accounting = populated_usage_status_accounting(dir.path());
        accounting.reset_status_snapshot_string_clones();
        let budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("usage status cancellation budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("usage status cancellation query should admit");
        cancellation.cancel();

        let error = accounting
            .status_snapshot_for_with_execution("team-a", &execution)
            .expect_err("pre-cancelled usage status projection must stop");
        assert!(matches!(error, QueryBudgetError::Cancelled));
        assert_eq!(accounting.status_snapshot_string_clones(), 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.cancellations_total, 1);
        assert_eq!(released.accounting_invariant_violations_total, 0);
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
    fn storage_reconciliation_reuses_one_query_slot_and_releases_all_query_memory() {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(512 * 1024 * 1024),
                per_query: QueryWorkLimits::default(),
            })
            .build()
            .expect("single-slot storage should build");
        storage
            .insert_rows(&make_scoped_single_series_rows(3))
            .expect("rows should insert");
        let accounting = UsageAccounting::open(None).expect("usage store should open");

        let snapshots = accounting
            .reconcile_storage(&storage)
            .expect("reconciliation must not attempt a nested query admission");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].samples_total, 3);

        let budget = storage.query_budget_snapshot();
        assert_eq!(budget.queries_started_total, 1);
        assert_eq!(budget.queries_completed_total, 1);
        assert_eq!(budget.peak_active_queries, 1);
        assert_eq!(budget.active_queries, 0);
        assert_eq!(budget.shared_reserved_memory_bytes, 0);
        assert_eq!(budget.concurrency_rejections_total, 0);
    }

    #[test]
    fn reconciliation_rejects_complete_claims_that_omit_result_guards() {
        let ledger_limits = UsageLedgerLimits::default();
        let scan_limits = StorageReconciliationScanLimits::from_ledger_limits(ledger_limits)
            .expect("scan limits should derive");
        let rows = make_scoped_single_series_rows(1);

        let manifest_inner = make_storage_with_rows(&rows);
        let manifest_storage: Arc<dyn Storage> = Arc::new(MissingReconciliationGuardStorage {
            inner: Arc::clone(&manifest_inner),
            omit_manifest_guard: true,
            omit_row_guard: false,
        });
        let manifest_execution =
            admit_storage_reconciliation_execution(&manifest_storage, scan_limits)
                .expect("the declared operation accounting should admit");
        let manifest_err = load_storage_reconciliation_plan(
            &manifest_storage,
            ledger_limits,
            scan_limits,
            &manifest_execution,
        )
        .expect_err("a metadata result without its guard must fail closed");
        assert!(
            manifest_err
                .to_string()
                .contains("manifest omitted its retained-memory reservation"),
            "{manifest_err}"
        );
        drop(manifest_execution);
        assert_eq!(
            manifest_inner
                .query_budget_snapshot()
                .shared_reserved_memory_bytes,
            0
        );

        let row_inner = make_storage_with_rows(&rows);
        let row_storage: Arc<dyn Storage> = Arc::new(MissingReconciliationGuardStorage {
            inner: Arc::clone(&row_inner),
            omit_manifest_guard: false,
            omit_row_guard: true,
        });
        let row_execution = admit_storage_reconciliation_execution(&row_storage, scan_limits)
            .expect("the declared operation accounting should admit");
        let row_plan = load_storage_reconciliation_plan(
            &row_storage,
            ledger_limits,
            scan_limits,
            &row_execution,
        )
        .expect("the guarded manifest should load");
        let row_err =
            run_storage_reconciliation_pass(&row_storage, &row_plan, scan_limits, &row_execution)
                .expect_err("a row page without its guard must fail closed");
        assert!(
            row_err
                .to_string()
                .contains("row page omitted its retained-memory reservation"),
            "{row_err}"
        );
        drop(row_plan);
        drop(row_execution);
        assert_eq!(
            row_inner
                .query_budget_snapshot()
                .shared_reserved_memory_bytes,
            0
        );
    }

    #[test]
    fn bounded_reconciliation_accepts_exact_row_and_byte_limits_but_rejects_n_plus_one() {
        let ledger_limits = UsageLedgerLimits::default();

        let exact_rows = make_scoped_single_series_rows(2);
        let exact_storage = make_storage_with_rows(&exact_rows);
        let mut row_limits = StorageReconciliationScanLimits::from_ledger_limits(ledger_limits)
            .expect("scan limits should derive");
        row_limits.max_rows_per_page = 2;
        row_limits.max_rows_per_pass = 2;
        let memory_before = exact_storage
            .query_budget_snapshot()
            .shared_reserved_memory_bytes;
        let exact_execution = admit_storage_reconciliation_execution(&exact_storage, row_limits)
            .expect("exact reconciliation should admit");
        let exact_plan = load_storage_reconciliation_plan(
            &exact_storage,
            ledger_limits,
            row_limits,
            &exact_execution,
        )
        .expect("exact manifest should load");
        let cloned_plan = exact_plan.clone();
        assert!(
            Arc::ptr_eq(&exact_plan.base_per_tenant, &cloned_plan.base_per_tenant),
            "per-page plan clones must share the preflighted base map"
        );
        drop(cloned_plan);
        let manifest_reservation = exact_plan.manifest.operation_memory_reservation.bytes();
        assert!(manifest_reservation > 0);
        assert!(
            manifest_reservation > exact_plan.manifest.manifest_retained_bytes,
            "the live operation guard must also cover accumulator maps and prepared records"
        );
        assert!(
            exact_storage
                .query_budget_snapshot()
                .shared_reserved_memory_bytes
                >= memory_before.saturating_add(manifest_reservation)
        );
        let exact = run_storage_reconciliation_pass(
            &exact_storage,
            &exact_plan,
            row_limits,
            &exact_execution,
        )
        .expect("exact row limit should scan")
        .expect("exact row limit should remain stable");
        assert_eq!(exact.rows_total, 2);
        assert_eq!(
            exact.pages_total, 2,
            "a full terminal page must be followed by an empty exact probe"
        );
        drop(exact_plan);
        drop(exact_execution);
        assert_eq!(
            exact_storage
                .query_budget_snapshot()
                .shared_reserved_memory_bytes,
            memory_before,
            "dropping the plan must release its retained manifest reservation"
        );

        let n_plus_one_rows = make_scoped_single_series_rows(3);
        let n_plus_one_storage = make_storage_with_rows(&n_plus_one_rows);
        let n_plus_one_execution =
            admit_storage_reconciliation_execution(&n_plus_one_storage, row_limits)
                .expect("N+1 reconciliation should admit");
        let n_plus_one_plan = load_storage_reconciliation_plan(
            &n_plus_one_storage,
            ledger_limits,
            row_limits,
            &n_plus_one_execution,
        )
        .expect("N+1 manifest should load");
        let err = run_storage_reconciliation_pass(
            &n_plus_one_storage,
            &n_plus_one_plan,
            row_limits,
            &n_plus_one_execution,
        )
        .expect_err("N+1 rows must fail closed");
        assert!(matches!(err, UsageAccountingError::Limit(_)), "{err}");
        drop(n_plus_one_plan);
        drop(n_plus_one_execution);

        let exact_bytes = exact.returned_bytes_total;
        let mut byte_limits = StorageReconciliationScanLimits::from_ledger_limits(ledger_limits)
            .expect("scan limits should derive");
        byte_limits.max_returned_bytes_per_pass = exact_bytes;
        let exact_byte_execution =
            admit_storage_reconciliation_execution(&exact_storage, byte_limits)
                .expect("exact-byte reconciliation should admit");
        let exact_byte_plan = load_storage_reconciliation_plan(
            &exact_storage,
            ledger_limits,
            byte_limits,
            &exact_byte_execution,
        )
        .expect("exact-byte manifest should load");
        assert!(run_storage_reconciliation_pass(
            &exact_storage,
            &exact_byte_plan,
            byte_limits,
            &exact_byte_execution,
        )
        .expect("exact byte limit should scan")
        .is_some());
        drop(exact_byte_plan);
        drop(exact_byte_execution);
        let n_plus_one_byte_execution =
            admit_storage_reconciliation_execution(&n_plus_one_storage, byte_limits)
                .expect("N+1-byte reconciliation should admit");
        let n_plus_one_byte_plan = load_storage_reconciliation_plan(
            &n_plus_one_storage,
            ledger_limits,
            byte_limits,
            &n_plus_one_byte_execution,
        )
        .expect("N+1-byte manifest should load");
        let err = run_storage_reconciliation_pass(
            &n_plus_one_storage,
            &n_plus_one_byte_plan,
            byte_limits,
            &n_plus_one_byte_execution,
        )
        .expect_err("N+1 returned bytes must fail closed");
        assert!(matches!(err, UsageAccountingError::Limit(_)), "{err}");
    }

    #[test]
    fn reconciliation_retained_memory_model_charges_spare_capacity_until_guard_drop() {
        let mut metric = String::with_capacity(256);
        metric.push('m');
        let mut labels = Vec::with_capacity(16);
        let mut label_name = String::with_capacity(64);
        label_name.push_str("host");
        let mut label_value = String::with_capacity(128);
        label_value.push('a');
        labels.push(Label::new(label_name, label_value));
        let row = Row::with_labels(metric, labels, DataPoint::new(1, 1.0));
        let mut rows = Vec::with_capacity(32);
        rows.push(row);

        let retained =
            modeled_storage_reconciliation_rows_retained_bytes(&rows).expect("bytes should model");
        let logical = u64::try_from(
            modeled_write_batch_input_bytes(&rows).expect("logical bytes should model"),
        )
        .expect("logical byte model should fit");
        assert!(
            retained > logical,
            "capacity model must include spare capacity and allocation allowances"
        );

        let storage = make_storage();
        let execution = storage
            .begin_query_execution(
                QueryWorkLimits {
                    max_memory_bytes: Some(retained),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("query should admit")
            .expect("built-in storage should expose a query budget");
        let guard = execution
            .reserve_memory(retained)
            .expect("retained rows should reserve");
        assert_eq!(guard.bytes(), retained);
        assert_eq!(execution.snapshot().memory_reserved_bytes, retained);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            retained
        );
        drop(guard);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }

    #[test]
    fn prepared_reconciliation_keeps_operation_memory_charged_until_drop() {
        let storage = make_two_tenant_storage();
        let memory_before = storage.query_budget_snapshot().shared_reserved_memory_bytes;
        let prepared =
            collect_storage_reconciliation_records(&storage, UsageLedgerLimits::default())
                .expect("reconciliation should prepare");
        let retained = prepared
            ._operation_memory
            .operation_memory_reservation
            .bytes();
        assert!(retained > 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            memory_before.saturating_add(retained)
        );
        assert_eq!(prepared.records.len(), 2);

        drop(prepared);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            memory_before
        );
    }

    #[test]
    fn concurrent_storage_mutation_retries_without_publishing_a_failed_prefix() {
        let accounting = UsageAccounting::open(None).expect("usage store should open");
        let inner = make_two_tenant_storage();
        let mutation = tenant::scope_rows_for_tenant(
            vec![Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "a")],
                DataPoint::new(2, 4.0),
            )],
            "team-a",
        )
        .expect("mutation should scope");
        let storage: Arc<dyn Storage> = Arc::new(MutateAfterFirstReconciliationPageStorage {
            inner,
            mutation,
            mutated: AtomicBool::new(false),
        });

        let snapshots = accounting
            .reconcile_storage(&storage)
            .expect("one observed mutation should be discarded and retried");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots
                .iter()
                .find(|snapshot| snapshot.tenant_id == "team-a")
                .expect("team-a snapshot should exist")
                .samples_total,
            3
        );
        let records = accounting.export_records(None, None, None);
        assert_eq!(
            records.len(),
            2,
            "only the stable retry may publish its atomic tenant batch"
        );
        assert_eq!(
            records.iter().map(|record| record.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
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

    fn bounded_test_limits(recent_records: usize) -> UsageLedgerLimits {
        UsageLedgerLimits {
            recent_records,
            max_tenants: 8,
            report_default_records: 2,
            report_max_records: 16,
            export_default_records: 2,
            export_max_records: 16,
            ..UsageLedgerLimits::default()
        }
    }

    fn record_background_events(accounting: &UsageAccounting, count: usize) {
        for index in 0..count {
            let mut input = UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "bounded_test",
                "test",
            );
            input.result_units = index as u64;
            accounting.record(input).expect("record should append");
        }
    }

    #[test]
    fn retained_window_is_bounded_across_many_appends_and_reopen() {
        let dir = tempdir().expect("temp dir should build");
        let limits = bounded_test_limits(3);
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
                .expect("usage store should open");
        record_background_events(&accounting, 10);

        let status = accounting.ledger_status();
        assert_eq!(status.records_total, 10);
        assert_eq!(status.retained_records, 3);
        assert_eq!(status.earliest_retained_sequence, Some(8));
        assert_eq!(
            accounting
                .export_records(None, None, None)
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![8, 9, 10]
        );
        assert_eq!(
            accounting.tenant_summary("team-a").background.events_total,
            10
        );
        let exact_report = accounting.report(Some("team-a"), None, None, UsageBucketWidth::None);
        assert!(exact_report.page.all_time_exact);
        assert!(!exact_report.page.raw_history_complete);
        assert_eq!(exact_report.page.records_aggregated, 10);
        assert_eq!(exact_report.tenants[0].background.events_total, 10);
        drop(accounting);

        let reopened =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
                .expect("bounded ledger should reopen");
        assert_eq!(reopened.ledger_status().records_total, 10);
        assert_eq!(reopened.ledger_status().retained_records, 3);
        assert_eq!(
            reopened.tenant_summary("team-a").background.events_total,
            10
        );
        assert_eq!(
            reopened
                .export_records(None, None, None)
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![8, 9, 10]
        );
    }

    #[test]
    fn evicted_storage_snapshot_and_usage_totals_remain_exact_after_reopen() {
        let dir = tempdir().expect("temp dir should build");
        let limits = bounded_test_limits(2);
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
                .expect("usage store should open");
        let mut ingest =
            UsageRecordInput::success("team-a", UsageCategory::Ingest, "ingest", "test");
        ingest.rows = 7;
        accounting.record(ingest).expect("ingest should append");
        let mut storage = UsageRecordInput::success(
            "team-a",
            UsageCategory::Storage,
            "reconcile_storage",
            "test",
        );
        storage.logical_storage_series = 3;
        storage.logical_storage_samples = 11;
        storage.logical_storage_bytes = 1_024;
        accounting.record(storage).expect("snapshot should append");
        record_background_events(&accounting, 4);
        assert_eq!(
            accounting
                .export_records(None, None, None)
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
        let summary = accounting.tenant_summary("team-a");
        assert_eq!(summary.ingest.rows, 7);
        assert_eq!(summary.background.events_total, 4);
        assert_eq!(
            summary
                .latest_storage_snapshot
                .as_ref()
                .map(|snapshot| snapshot.logical_storage_bytes),
            Some(1_024)
        );
        drop(accounting);

        let reopened =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
                .expect("ledger should reopen");
        let summary = reopened.tenant_summary("team-a");
        assert_eq!(summary.ingest.rows, 7);
        assert_eq!(summary.background.events_total, 4);
        assert_eq!(
            summary
                .latest_storage_snapshot
                .map(|snapshot| snapshot.logical_storage_bytes),
            Some(1_024)
        );
        assert_eq!(reopened.ledger_status().storage_reconciliations_total, 1);
    }

    #[test]
    fn export_pages_have_exact_n_plus_one_boundaries_without_gaps_or_duplicates() {
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(None, bounded_test_limits(16), None)
                .expect("usage store should open");
        record_background_events(&accounting, 5);

        let first = accounting
            .export_page(
                None,
                None,
                None,
                UsageReadOptions {
                    after_sequence: None,
                    snapshot_sequence: None,
                    limit: 2,
                },
                accounting.limits().export_max_response_bytes,
            )
            .expect("first page should read");
        assert_eq!(first.records_returned, 2);
        assert!(first.has_more);
        assert_eq!(first.next_after_sequence, Some(2));
        assert_eq!(first.snapshot_sequence, 5);
        assert!(first.raw_history_complete);

        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "concurrent_after_snapshot",
                "test",
            ))
            .expect("concurrent record should append");

        let mut sequences = first
            .records
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>();
        let second = accounting
            .export_page(
                None,
                None,
                None,
                UsageReadOptions {
                    after_sequence: first.next_after_sequence,
                    snapshot_sequence: Some(first.snapshot_sequence),
                    limit: 2,
                },
                accounting.limits().export_max_response_bytes,
            )
            .expect("second page should read");
        assert_eq!(second.records_returned, 2);
        assert!(second.has_more);
        let third = accounting
            .export_page(
                None,
                None,
                None,
                UsageReadOptions {
                    after_sequence: second.next_after_sequence,
                    snapshot_sequence: Some(second.snapshot_sequence),
                    limit: 2,
                },
                accounting.limits().export_max_response_bytes,
            )
            .expect("third page should read");
        assert_eq!(third.records_returned, 1);
        assert!(!third.has_more);
        sequences.extend(second.records.iter().map(|record| record.seq));
        sequences.extend(third.records.iter().map(|record| record.seq));
        assert_eq!(sequences, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn concurrent_appends_do_not_move_a_pinned_export_snapshot() {
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(None, bounded_test_limits(128), None)
                .expect("usage store should open");
        record_background_events(&accounting, 32);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer = {
            let accounting = Arc::clone(&accounting);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                record_background_events(&accounting, 32);
            })
        };

        barrier.wait();
        let mut after_sequence = None;
        let mut snapshot_sequence = None;
        let mut exported = Vec::new();
        loop {
            let page = accounting
                .export_page(
                    None,
                    None,
                    None,
                    UsageReadOptions {
                        after_sequence,
                        snapshot_sequence,
                        limit: 3,
                    },
                    accounting.limits().export_max_response_bytes,
                )
                .expect("concurrent export page should remain available");
            snapshot_sequence = Some(page.snapshot_sequence);
            exported.extend(page.records.iter().map(|record| record.seq));
            if !page.has_more {
                break;
            }
            after_sequence = page.next_after_sequence;
        }
        writer.join().expect("writer should not panic");

        let snapshot = snapshot_sequence.expect("export must publish a snapshot");
        assert_eq!(
            exported,
            (1..=snapshot).collect::<Vec<_>>(),
            "the pinned traversal must contain every initial sequence exactly once"
        );
        assert!(accounting.ledger_status().last_sequence >= snapshot);
    }

    #[test]
    fn report_record_limit_uses_n_plus_one_continuation() {
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(None, bounded_test_limits(16), None)
                .expect("usage store should open");
        record_background_events(&accounting, 2);

        let exact = accounting
            .report_page(
                None,
                None,
                None,
                UsageBucketWidth::Hour,
                UsageReadOptions {
                    after_sequence: None,
                    snapshot_sequence: None,
                    limit: 2,
                },
            )
            .expect("exact-limit report page should build");
        assert_eq!(exact.page.records_aggregated, 2);
        assert!(!exact.page.has_more);
        assert_eq!(exact.page.next_after_sequence, None);
        assert_eq!(exact.tenants[0].background.events_total, 2);

        record_background_events(&accounting, 1);
        let n_plus_one = accounting
            .report_page(
                None,
                None,
                None,
                UsageBucketWidth::Hour,
                UsageReadOptions {
                    after_sequence: None,
                    snapshot_sequence: None,
                    limit: 2,
                },
            )
            .expect("N+1 report page should build");
        assert_eq!(n_plus_one.page.records_aggregated, 2);
        assert!(n_plus_one.page.has_more);
        assert_eq!(n_plus_one.page.next_after_sequence, Some(2));
        assert_eq!(n_plus_one.tenants[0].background.events_total, 2);
    }

    #[test]
    fn all_time_and_bucket_reports_have_stable_tenant_order() {
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(None, bounded_test_limits(16), None)
                .expect("usage store should open");
        let records = ["team-z", "team-a", "team-m"]
            .into_iter()
            .map(|tenant_id| {
                let mut record = usage_record_from_input(UsageRecordInput::success(
                    tenant_id,
                    UsageCategory::Query,
                    "stable_order",
                    "test",
                ));
                record.unix_ms = 1_700_000_000_000;
                record
            })
            .collect();
        accounting
            .append_records(records)
            .expect("usage records should append");

        let exact = accounting.report(None, None, None, UsageBucketWidth::None);
        assert_eq!(
            exact
                .tenants
                .iter()
                .map(|tenant| tenant.tenant_id.as_str())
                .collect::<Vec<_>>(),
            vec!["team-a", "team-m", "team-z"]
        );

        let bucketed = accounting
            .report_page(
                None,
                None,
                None,
                UsageBucketWidth::Hour,
                UsageReadOptions {
                    after_sequence: None,
                    snapshot_sequence: None,
                    limit: 3,
                },
            )
            .expect("bucketed report should build");
        assert_eq!(bucketed.buckets.len(), 1);
        assert_eq!(
            bucketed.buckets[0]
                .tenants
                .iter()
                .map(|tenant| tenant.tenant_id.as_str())
                .collect::<Vec<_>>(),
            vec!["team-a", "team-m", "team-z"]
        );
    }

    #[test]
    fn expired_cursor_is_explicit_after_retention_advances() {
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(None, bounded_test_limits(2), None)
                .expect("usage store should open");
        record_background_events(&accounting, 4);

        let err = accounting
            .export_page(
                None,
                None,
                None,
                UsageReadOptions {
                    after_sequence: Some(1),
                    snapshot_sequence: Some(4),
                    limit: 2,
                },
                accounting.limits().export_max_response_bytes,
            )
            .expect_err("evicted cursor must not silently skip records");
        assert_eq!(
            err,
            UsageReadError::CursorExpired {
                requested_after: 1,
                earliest_available: 3,
            }
        );
    }

    #[test]
    fn export_response_bytes_accept_exact_n_and_reject_n_minus_one() {
        let accounting =
            UsageAccounting::open_with_limits_and_disk_budget(None, bounded_test_limits(4), None)
                .expect("usage store should open");
        record_background_events(&accounting, 1);
        let record = accounting.export_records(None, None, None).remove(0);
        let exact_bytes = serde_json::to_vec(&record)
            .expect("record should encode")
            .len()
            + 1;

        let exact = accounting
            .export_page(
                None,
                None,
                None,
                UsageReadOptions {
                    after_sequence: None,
                    snapshot_sequence: None,
                    limit: 1,
                },
                exact_bytes,
            )
            .expect("exact byte limit should fit");
        assert_eq!(exact.response_bytes, exact_bytes);
        let err = accounting
            .export_page(
                None,
                None,
                None,
                UsageReadOptions {
                    after_sequence: None,
                    snapshot_sequence: None,
                    limit: 1,
                },
                exact_bytes - 1,
            )
            .expect_err("one byte below the record must fail explicitly");
        assert!(matches!(
            err,
            UsageReadError::RecordExceedsResponseLimit { .. }
        ));
    }

    #[test]
    fn startup_line_and_frame_limits_reject_n_plus_one() {
        let dir = tempdir().expect("temp dir should build");
        let ledger_dir = dir.path().join(USAGE_LEDGER_DIR);
        fs::create_dir_all(&ledger_dir).expect("ledger directory should build");
        let ledger_path = ledger_dir.join(USAGE_LEDGER_FILE);
        let mut record = usage_record_from_input(UsageRecordInput::success(
            "team-a",
            UsageCategory::Background,
            "startup_limit",
            "test",
        ));
        record.seq = 1;
        let encoded = serde_json::to_vec(&record).expect("record should encode");
        fs::write(&ledger_path, [&encoded[..], b"\n"].concat()).expect("ledger should write");

        let mut exact_limits = bounded_test_limits(2);
        exact_limits.max_record_bytes = encoded.len();
        exact_limits.max_frame_bytes = encoded.len();
        exact_limits.max_line_bytes = encoded.len() + 1;
        UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), exact_limits, None)
            .expect("exact frame and line limits should open");

        fs::write(&ledger_path, [&encoded[..], b" \n"].concat())
            .expect("oversized frame fixture should write");
        let mut frame_limits = exact_limits;
        frame_limits.max_line_bytes = encoded.len() + 2;
        let err =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), frame_limits, None)
                .expect_err("N+1 frame must fail");
        assert!(err.contains("frame bytes"), "{err}");

        fs::write(&ledger_path, [&encoded[..], b" \n"].concat())
            .expect("oversized line fixture should write");
        let err =
            UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), exact_limits, None)
                .expect_err("N+1 line must fail before extending the line buffer");
        assert!(err.contains("line exceeds configured maximum"), "{err}");
    }

    #[test]
    fn startup_sequence_range_limit_accepts_n_and_rejects_n_plus_one() {
        let dir = tempdir().expect("temp dir should build");
        let ledger_dir = dir.path().join(USAGE_LEDGER_DIR);
        fs::create_dir_all(&ledger_dir).expect("ledger directory should build");
        let ledger_path = ledger_dir.join(USAGE_LEDGER_FILE);
        let make_record = |seq| {
            let mut record = usage_record_from_input(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "legacy_ranges",
                "test",
            ));
            record.seq = seq;
            serde_json::to_string(&record).expect("record should encode")
        };
        let mut limits = bounded_test_limits(4);
        limits.max_sequence_ranges = 2;
        fs::write(
            &ledger_path,
            format!("{}\n{}\n", make_record(1), make_record(3)),
        )
        .expect("two-range ledger should write");
        UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
            .expect("exact sequence-range limit should open");

        fs::write(
            &ledger_path,
            format!(
                "{}\n{}\n{}\n",
                make_record(1),
                make_record(3),
                make_record(5)
            ),
        )
        .expect("three-range ledger should write");
        let err = UsageAccounting::open_with_limits_and_disk_budget(Some(dir.path()), limits, None)
            .expect_err("N+1 disjoint sequence range must fail");
        assert!(
            err.contains("more than the configured 2 disjoint ranges"),
            "{err}"
        );
    }

    #[test]
    fn tenant_cardinality_limit_rejects_n_plus_one_without_publication() {
        let mut limits = bounded_test_limits(8);
        limits.max_tenants = 2;
        let accounting = UsageAccounting::open_with_limits_and_disk_budget(None, limits, None)
            .expect("usage store should open");
        for tenant_id in ["team-a", "team-b"] {
            accounting
                .record(UsageRecordInput::success(
                    tenant_id,
                    UsageCategory::Query,
                    "query",
                    "test",
                ))
                .expect("tenant within limit should append");
        }
        let err = accounting
            .record(UsageRecordInput::success(
                "team-c",
                UsageCategory::Query,
                "query",
                "test",
            ))
            .expect_err("N+1 tenant must be rejected");
        assert!(matches!(err, UsageAccountingError::Limit(_)));
        assert_eq!(accounting.ledger_status().records_total, 2);
        assert_eq!(accounting.ledger_status().tenant_count, 2);
        assert_eq!(accounting.ledger_status().last_sequence, 2);
    }

    #[test]
    fn atomic_batch_record_limit_accepts_n_and_rejects_n_plus_one() {
        let mut limits = bounded_test_limits(8);
        limits.max_batch_records = 2;
        limits.max_tenants = 2;
        let accounting = UsageAccounting::open_with_limits_and_disk_budget(None, limits, None)
            .expect("usage store should open");
        let make_record = || {
            usage_record_from_input(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "batch_limit",
                "test",
            ))
        };
        accounting
            .append_records(vec![make_record(), make_record()])
            .expect("exact batch limit should append atomically");
        let err = accounting
            .append_records(vec![make_record(), make_record(), make_record()])
            .expect_err("N+1 batch should be rejected before publication");
        assert!(matches!(err, UsageAccountingError::Limit(_)));
        assert_eq!(accounting.ledger_status().records_total, 2);
        assert_eq!(accounting.ledger_status().last_sequence, 2);
    }

    #[test]
    fn startup_batch_scanner_rejects_n_plus_one_before_typed_decode() {
        let records = (1..=3)
            .map(|seq| {
                let mut record = usage_record_from_input(UsageRecordInput::success(
                    "team-a",
                    UsageCategory::Background,
                    "startup_batch_limit",
                    "test",
                ));
                record.seq = seq;
                record
            })
            .collect::<Vec<_>>();
        let frame = serde_json::to_vec(&PersistedUsageLedgerBatchRef {
            magic: USAGE_LEDGER_BATCH_MAGIC,
            schema_version: USAGE_LEDGER_BATCH_SCHEMA_VERSION,
            records: &records,
        })
        .expect("batch should encode");
        decode_persisted_usage_line(&frame, 3).expect("exact batch limit should decode");
        let err = decode_persisted_usage_line(&frame, 2)
            .expect_err("N+1 batch must fail in the lexical guard");
        assert!(err.contains("more than the configured 2 records"), "{err}");
    }
}
