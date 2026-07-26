use regex::{Regex, RegexBuilder};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(test)]
use std::sync::{Barrier, Mutex};
use tsink::{
    Label, QueryBudgetError, QueryExecution, QueryMemoryReservation, SeriesMatcher,
    SeriesMatcherOp, SeriesSelection,
};

const EXEMPLAR_STORE_FILE_NAME: &str = "exemplar-store.json";
const EXEMPLAR_STORE_MAGIC: &str = "tsink-exemplar-store";
const EXEMPLAR_STORE_SCHEMA_VERSION: u16 = 1;
const EXEMPLAR_QUERY_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const EXEMPLAR_QUERY_REGEX_SIZE_LIMIT_BYTES: usize = 256 * 1024;
const EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES: u64 = 128;
const EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES: u64 = 16 * 1024;

/// Finite ownership limits for retained exemplar state and its lifecycle operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExemplarStoreResourceLimits {
    pub max_total_series: usize,
    pub max_metric_name_bytes: usize,
    pub max_labels_per_series: usize,
    pub max_labels_per_exemplar: usize,
    pub max_label_name_bytes: usize,
    pub max_label_value_bytes: usize,
    pub max_series_identity_bytes: usize,
    pub max_exemplar_label_bytes: usize,
    pub max_total_retained_bytes: u64,
    pub max_update_batch_bytes: u64,
    pub max_write_transient_bytes: u64,
    pub max_replacement_peak_bytes: u64,
    pub max_persistence_serialization_bytes: u64,
    pub max_durable_file_bytes: u64,
    pub max_startup_transient_bytes: u64,
    pub max_snapshot_bytes: u64,
    pub max_snapshot_transient_bytes: u64,
    pub max_concurrent_transient_bytes: u64,
}

impl Default for ExemplarStoreResourceLimits {
    fn default() -> Self {
        const MIB: u64 = 1024 * 1024;
        Self {
            max_total_series: 50_000,
            max_metric_name_bytes: tsink::label::MAX_METRIC_NAME_LEN,
            max_labels_per_series: tsink::DEFAULT_MAX_LABELS_PER_SERIES,
            max_labels_per_exemplar: tsink::DEFAULT_MAX_LABELS_PER_SERIES,
            max_label_name_bytes: tsink::label::MAX_LABEL_NAME_LEN,
            max_label_value_bytes: tsink::label::MAX_LABEL_VALUE_LEN,
            max_series_identity_bytes: tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_exemplar_label_bytes: tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_total_retained_bytes: 64 * MIB,
            max_update_batch_bytes: 32 * MIB,
            max_write_transient_bytes: 64 * MIB,
            max_replacement_peak_bytes: 128 * MIB,
            max_persistence_serialization_bytes: 64 * MIB,
            max_durable_file_bytes: 64 * MIB,
            max_startup_transient_bytes: 160 * MIB,
            max_snapshot_bytes: 64 * MIB,
            max_snapshot_transient_bytes: 256 * 1024,
            max_concurrent_transient_bytes: 128 * MIB,
        }
    }
}

impl ExemplarStoreResourceLimits {
    pub fn validate(self) -> Result<Self, ExemplarStoreError> {
        let nonzero_usize = [
            ("max_total_series", self.max_total_series),
            ("max_metric_name_bytes", self.max_metric_name_bytes),
            ("max_labels_per_series", self.max_labels_per_series),
            ("max_labels_per_exemplar", self.max_labels_per_exemplar),
            ("max_label_name_bytes", self.max_label_name_bytes),
            ("max_label_value_bytes", self.max_label_value_bytes),
            ("max_series_identity_bytes", self.max_series_identity_bytes),
            ("max_exemplar_label_bytes", self.max_exemplar_label_bytes),
        ];
        if let Some((field, _)) = nonzero_usize
            .into_iter()
            .find(|(_, value)| *value == 0 || *value == usize::MAX)
        {
            return Err(ExemplarStoreError::configuration(format!(
                "exemplar store resource limit {field} must be finite and greater than zero"
            )));
        }
        let nonzero_u64 = [
            ("max_total_retained_bytes", self.max_total_retained_bytes),
            ("max_update_batch_bytes", self.max_update_batch_bytes),
            ("max_write_transient_bytes", self.max_write_transient_bytes),
            (
                "max_replacement_peak_bytes",
                self.max_replacement_peak_bytes,
            ),
            (
                "max_persistence_serialization_bytes",
                self.max_persistence_serialization_bytes,
            ),
            ("max_durable_file_bytes", self.max_durable_file_bytes),
            (
                "max_startup_transient_bytes",
                self.max_startup_transient_bytes,
            ),
            ("max_snapshot_bytes", self.max_snapshot_bytes),
            (
                "max_snapshot_transient_bytes",
                self.max_snapshot_transient_bytes,
            ),
            (
                "max_concurrent_transient_bytes",
                self.max_concurrent_transient_bytes,
            ),
        ];
        if let Some((field, _)) = nonzero_u64
            .into_iter()
            .find(|(_, value)| *value == 0 || *value == u64::MAX)
        {
            return Err(ExemplarStoreError::configuration(format!(
                "exemplar store resource limit {field} must be finite and greater than zero"
            )));
        }
        if self.max_metric_name_bytes > tsink::label::MAX_METRIC_NAME_LEN
            || self.max_label_name_bytes > tsink::label::MAX_LABEL_NAME_LEN
            || self.max_label_value_bytes > tsink::label::MAX_LABEL_VALUE_LEN
            || self.max_labels_per_series > tsink::label::MAX_SUPPORTED_LABELS_PER_SERIES
            || self.max_labels_per_exemplar > tsink::label::MAX_SUPPORTED_LABELS_PER_SERIES
        {
            return Err(ExemplarStoreError::configuration(
                "exemplar store shape limits exceed the storage-format hard limits",
            ));
        }
        if self.max_metric_name_bytes > self.max_series_identity_bytes {
            return Err(ExemplarStoreError::configuration(
                "exemplar store max_metric_name_bytes exceeds max_series_identity_bytes",
            ));
        }
        if self.max_replacement_peak_bytes < self.max_total_retained_bytes
            || self.max_replacement_peak_bytes < self.max_write_transient_bytes
        {
            return Err(ExemplarStoreError::configuration(
                "exemplar store max_replacement_peak_bytes must cover retained and write-transient limits",
            ));
        }
        if self.max_concurrent_transient_bytes < self.max_write_transient_bytes
            || self.max_concurrent_transient_bytes < self.max_snapshot_transient_bytes
        {
            return Err(ExemplarStoreError::configuration(
                "exemplar store max_concurrent_transient_bytes must cover each operation limit",
            ));
        }
        Ok(self)
    }
}

/// Stable machine-readable rejection code for exemplar-store operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ExemplarStoreErrorCode {
    InvalidConfiguration = 1,
    InvalidMetricShape = 2,
    InvalidSeriesLabelShape = 3,
    InvalidExemplarLabelShape = 4,
    UpdateBatchEntries = 5,
    UpdateBatchBytes = 6,
    TotalSeries = 7,
    RetainedBytes = 8,
    WriteTransientBytes = 9,
    ReplacementPeakBytes = 10,
    PersistenceSerializationBytes = 11,
    DurableFileBytes = 12,
    StartupFileBytes = 13,
    StartupTransientBytes = 14,
    StartupEntries = 15,
    StartupFormat = 16,
    SnapshotBytes = 17,
    SnapshotTransientBytes = 18,
    InvalidExemplarValue = 19,
}

impl ExemplarStoreErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "exemplar_invalid_configuration",
            Self::InvalidMetricShape => "exemplar_invalid_metric_shape",
            Self::InvalidSeriesLabelShape => "exemplar_invalid_series_label_shape",
            Self::InvalidExemplarLabelShape => "exemplar_invalid_exemplar_label_shape",
            Self::UpdateBatchEntries => "exemplar_update_batch_entries_limit",
            Self::UpdateBatchBytes => "exemplar_update_batch_bytes_limit",
            Self::TotalSeries => "exemplar_total_series_limit",
            Self::RetainedBytes => "exemplar_retained_bytes_limit",
            Self::WriteTransientBytes => "exemplar_write_transient_bytes_limit",
            Self::ReplacementPeakBytes => "exemplar_replacement_peak_bytes_limit",
            Self::PersistenceSerializationBytes => "exemplar_persistence_serialization_bytes_limit",
            Self::DurableFileBytes => "exemplar_durable_file_bytes_limit",
            Self::StartupFileBytes => "exemplar_startup_file_bytes_limit",
            Self::StartupTransientBytes => "exemplar_startup_transient_bytes_limit",
            Self::StartupEntries => "exemplar_startup_entries_limit",
            Self::StartupFormat => "exemplar_startup_format_invalid",
            Self::SnapshotBytes => "exemplar_snapshot_bytes_limit",
            Self::SnapshotTransientBytes => "exemplar_snapshot_transient_bytes_limit",
            Self::InvalidExemplarValue => "exemplar_invalid_value",
        }
    }

    fn from_u64(value: u64) -> Option<Self> {
        match value {
            1 => Some(Self::InvalidConfiguration),
            2 => Some(Self::InvalidMetricShape),
            3 => Some(Self::InvalidSeriesLabelShape),
            4 => Some(Self::InvalidExemplarLabelShape),
            5 => Some(Self::UpdateBatchEntries),
            6 => Some(Self::UpdateBatchBytes),
            7 => Some(Self::TotalSeries),
            8 => Some(Self::RetainedBytes),
            9 => Some(Self::WriteTransientBytes),
            10 => Some(Self::ReplacementPeakBytes),
            11 => Some(Self::PersistenceSerializationBytes),
            12 => Some(Self::DurableFileBytes),
            13 => Some(Self::StartupFileBytes),
            14 => Some(Self::StartupTransientBytes),
            15 => Some(Self::StartupEntries),
            16 => Some(Self::StartupFormat),
            17 => Some(Self::SnapshotBytes),
            18 => Some(Self::SnapshotTransientBytes),
            19 => Some(Self::InvalidExemplarValue),
            _ => None,
        }
    }
}

/// Typed exemplar-store failure retaining the native storage error as its source.
#[derive(Debug)]
pub struct ExemplarStoreError {
    code: Option<ExemplarStoreErrorCode>,
    source: tsink::TsinkError,
}

impl ExemplarStoreError {
    fn configuration(message: impl Into<String>) -> Self {
        Self {
            code: Some(ExemplarStoreErrorCode::InvalidConfiguration),
            source: tsink::TsinkError::InvalidConfiguration(message.into()),
        }
    }

    fn coded(code: ExemplarStoreErrorCode, source: tsink::TsinkError) -> Self {
        Self {
            code: Some(code),
            source,
        }
    }

    #[must_use]
    pub const fn code(&self) -> Option<ExemplarStoreErrorCode> {
        self.code
    }

    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn as_tsink_error(&self) -> &tsink::TsinkError {
        &self.source
    }
}

impl From<tsink::TsinkError> for ExemplarStoreError {
    fn from(source: tsink::TsinkError) -> Self {
        Self { code: None, source }
    }
}

impl Deref for ExemplarStoreError {
    type Target = tsink::TsinkError;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl fmt::Display for ExemplarStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code {
            write!(formatter, "{}: {}", code.as_str(), self.source)
        } else {
            self.source.fmt(formatter)
        }
    }
}

impl std::error::Error for ExemplarStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExemplarStoreConfig {
    pub max_total_exemplars: usize,
    pub max_exemplars_per_series: usize,
    pub max_exemplars_per_request: usize,
    pub max_query_results: usize,
    pub max_query_selectors: usize,
}

impl Default for ExemplarStoreConfig {
    fn default() -> Self {
        Self {
            max_total_exemplars: 50_000,
            max_exemplars_per_series: 128,
            max_exemplars_per_request: 512,
            max_query_results: 1_000,
            max_query_selectors: 32,
        }
    }
}

impl ExemplarStoreConfig {
    pub fn validate(self) -> Result<Self, String> {
        let limits = [
            ("max_total_exemplars", self.max_total_exemplars),
            ("max_exemplars_per_series", self.max_exemplars_per_series),
            ("max_exemplars_per_request", self.max_exemplars_per_request),
            ("max_query_results", self.max_query_results),
            ("max_query_selectors", self.max_query_selectors),
        ];
        if let Some((field, _)) = limits
            .into_iter()
            .find(|(_, value)| *value == 0 || *value == usize::MAX)
        {
            return Err(format!(
                "exemplar store {field} must be finite and greater than zero"
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExemplarStoreMetricsSnapshot {
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub dropped_total: u64,
    pub query_requests_total: u64,
    pub query_series_total: u64,
    pub query_exemplars_total: u64,
    pub stored_series: u64,
    pub stored_exemplars: u64,
    pub retained_bytes: u64,
    pub peak_retained_bytes: u64,
    pub retained_bytes_limit: u64,
    pub durable_file_bytes: u64,
    pub peak_durable_file_bytes: u64,
    pub durable_file_bytes_limit: u64,
    pub transient_bytes: u64,
    pub peak_transient_bytes: u64,
    pub concurrent_transient_bytes_limit: u64,
    pub resource_rejections_total: u64,
    pub shape_rejections_total: u64,
    pub batch_rejections_total: u64,
    pub retained_rejections_total: u64,
    pub transient_rejections_total: u64,
    pub durable_rejections_total: u64,
    pub startup_rejections_total: u64,
    pub snapshot_rejections_total: u64,
    pub last_rejection_code: Option<ExemplarStoreErrorCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExemplarApplyOutcome {
    pub accepted: usize,
    pub dropped: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarSample {
    #[serde(default)]
    pub labels: Vec<Label>,
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarSeries {
    pub metric: String,
    #[serde(default)]
    pub labels: Vec<Label>,
    #[serde(default)]
    pub exemplars: Vec<ExemplarSample>,
}

/// An exemplar query result whose retained heap remains charged to its query execution.
#[derive(Debug)]
#[must_use = "dropping the result releases its retained query-memory reservation"]
pub struct AccountedExemplarQueryResult {
    series: Vec<ExemplarSeries>,
    reservation: QueryMemoryReservation,
}

impl AccountedExemplarQueryResult {
    #[must_use]
    pub fn series(&self) -> &[ExemplarSeries] {
        &self.series
    }

    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.reservation.bytes()
    }

    pub fn into_parts(self) -> (Vec<ExemplarSeries>, QueryMemoryReservation) {
        (self.series, self.reservation)
    }
}

#[derive(Debug)]
pub enum ExemplarQueryError {
    Budget(QueryBudgetError),
    InvalidSelection,
    StoreUnavailable,
    Allocation,
}

impl fmt::Display for ExemplarQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(error) => write!(formatter, "exemplar query budget failed: {error}"),
            Self::InvalidSelection => formatter.write_str("invalid exemplar query selection"),
            Self::StoreUnavailable => formatter.write_str("exemplar store is unavailable"),
            Self::Allocation => formatter.write_str("exemplar query allocation failed"),
        }
    }
}

impl std::error::Error for ExemplarQueryError {}

impl From<QueryBudgetError> for ExemplarQueryError {
    fn from(error: QueryBudgetError) -> Self {
        Self::Budget(error)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarWrite {
    pub metric: String,
    #[serde(default)]
    pub series_labels: Vec<Label>,
    #[serde(default)]
    pub exemplar_labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    metric: String,
    labels: Vec<Label>,
}

#[derive(Debug, Clone, Default)]
struct ExemplarStoreState {
    series: BTreeMap<SeriesKey, BTreeMap<i64, ExemplarSample>>,
    retained_bytes: u64,
}

impl ExemplarStoreState {
    fn exemplar_count(&self) -> usize {
        self.series.values().map(BTreeMap::len).sum()
    }
}

#[derive(Debug, Default)]
struct ExemplarStoreMetrics {
    accepted_total: AtomicU64,
    rejected_total: AtomicU64,
    dropped_total: AtomicU64,
    query_requests_total: AtomicU64,
    query_series_total: AtomicU64,
    query_exemplars_total: AtomicU64,
    retained_bytes: AtomicU64,
    peak_retained_bytes: AtomicU64,
    durable_file_bytes: AtomicU64,
    peak_durable_file_bytes: AtomicU64,
    transient_bytes: AtomicU64,
    peak_transient_bytes: AtomicU64,
    resource_rejections_total: AtomicU64,
    shape_rejections_total: AtomicU64,
    batch_rejections_total: AtomicU64,
    retained_rejections_total: AtomicU64,
    transient_rejections_total: AtomicU64,
    durable_rejections_total: AtomicU64,
    startup_rejections_total: AtomicU64,
    snapshot_rejections_total: AtomicU64,
    last_rejection_code: AtomicU64,
}

impl ExemplarStoreMetrics {
    fn with_initial_state(
        retained_bytes: u64,
        durable_file_bytes: u64,
        startup_peak_bytes: u64,
    ) -> Self {
        Self {
            retained_bytes: AtomicU64::new(retained_bytes),
            peak_retained_bytes: AtomicU64::new(retained_bytes),
            durable_file_bytes: AtomicU64::new(durable_file_bytes),
            peak_durable_file_bytes: AtomicU64::new(durable_file_bytes),
            peak_transient_bytes: AtomicU64::new(startup_peak_bytes),
            ..Self::default()
        }
    }

    fn update_peak(counter: &AtomicU64, value: u64) {
        let mut observed = counter.load(Ordering::Relaxed);
        while value > observed {
            match counter.compare_exchange_weak(
                observed,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    fn set_retained_bytes(&self, bytes: u64) {
        self.retained_bytes.store(bytes, Ordering::Relaxed);
        Self::update_peak(&self.peak_retained_bytes, bytes);
    }

    fn set_durable_file_bytes(&self, bytes: u64) {
        self.durable_file_bytes.store(bytes, Ordering::Relaxed);
        Self::update_peak(&self.peak_durable_file_bytes, bytes);
    }

    fn record_rejection(&self, code: ExemplarStoreErrorCode, count: usize) {
        self.rejected_total
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.resource_rejections_total
            .fetch_add(1, Ordering::Relaxed);
        self.last_rejection_code
            .store(code as u64, Ordering::Relaxed);
        match code {
            ExemplarStoreErrorCode::InvalidMetricShape
            | ExemplarStoreErrorCode::InvalidSeriesLabelShape
            | ExemplarStoreErrorCode::InvalidExemplarLabelShape
            | ExemplarStoreErrorCode::InvalidExemplarValue => {
                self.shape_rejections_total.fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::UpdateBatchEntries
            | ExemplarStoreErrorCode::UpdateBatchBytes => {
                self.batch_rejections_total.fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::RetainedBytes | ExemplarStoreErrorCode::TotalSeries => {
                self.retained_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::WriteTransientBytes
            | ExemplarStoreErrorCode::ReplacementPeakBytes
            | ExemplarStoreErrorCode::PersistenceSerializationBytes => {
                self.transient_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::DurableFileBytes => {
                self.durable_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::StartupFileBytes
            | ExemplarStoreErrorCode::StartupTransientBytes
            | ExemplarStoreErrorCode::StartupEntries
            | ExemplarStoreErrorCode::StartupFormat => {
                self.startup_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::SnapshotBytes
            | ExemplarStoreErrorCode::SnapshotTransientBytes => {
                self.snapshot_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExemplarStoreErrorCode::InvalidConfiguration => {}
        }
    }
}

struct ExemplarTransientReservation<'a> {
    metrics: &'a ExemplarStoreMetrics,
    bytes: u64,
}

impl ExemplarTransientReservation<'_> {
    fn resize(
        &mut self,
        requested: u64,
        operation_limit: u64,
        global_limit: u64,
        code: ExemplarStoreErrorCode,
    ) -> Result<(), ExemplarStoreError> {
        if requested > operation_limit {
            return Err(ExemplarStoreError::coded(
                code,
                tsink::TsinkError::MemoryBudgetExceeded {
                    budget: usize::try_from(operation_limit).unwrap_or(usize::MAX),
                    required: usize::try_from(requested).unwrap_or(usize::MAX),
                },
            ));
        }
        if requested == self.bytes {
            return Ok(());
        }
        if requested < self.bytes {
            let released = self.bytes - requested;
            self.metrics
                .transient_bytes
                .fetch_sub(released, Ordering::AcqRel);
            self.bytes = requested;
            return Ok(());
        }
        let additional = requested - self.bytes;
        let mut current = self.metrics.transient_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(additional).ok_or_else(|| {
                ExemplarStoreError::coded(
                    code,
                    tsink::TsinkError::MemoryBudgetExceeded {
                        budget: usize::try_from(global_limit).unwrap_or(usize::MAX),
                        required: usize::MAX,
                    },
                )
            })?;
            if next > global_limit {
                return Err(ExemplarStoreError::coded(
                    code,
                    tsink::TsinkError::MemoryBudgetExceeded {
                        budget: usize::try_from(global_limit).unwrap_or(usize::MAX),
                        required: usize::try_from(next).unwrap_or(usize::MAX),
                    },
                ));
            }
            match self.metrics.transient_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.bytes = requested;
                    ExemplarStoreMetrics::update_peak(&self.metrics.peak_transient_bytes, next);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for ExemplarTransientReservation<'_> {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.metrics
                .transient_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

#[derive(Debug)]
pub struct ExemplarStore {
    path: Option<PathBuf>,
    local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    config: ExemplarStoreConfig,
    resource_limits: ExemplarStoreResourceLimits,
    state: RwLock<ExemplarStoreState>,
    metrics: ExemplarStoreMetrics,
    #[cfg(test)]
    query_test_gate: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

impl ExemplarStore {
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self::in_memory_with_config(ExemplarStoreConfig::default())
    }

    #[allow(dead_code)]
    pub fn in_memory_with_config(config: ExemplarStoreConfig) -> Self {
        Self::in_memory_with_config_and_resource_limits(
            config,
            ExemplarStoreResourceLimits::default(),
        )
        .expect("valid exemplar store resource limits")
    }

    #[allow(dead_code)]
    pub fn in_memory_with_config_and_resource_limits(
        config: ExemplarStoreConfig,
        resource_limits: ExemplarStoreResourceLimits,
    ) -> Result<Self, ExemplarStoreError> {
        let config = config
            .validate()
            .map_err(ExemplarStoreError::configuration)?;
        let resource_limits = resource_limits.validate()?;
        let state = ExemplarStoreState::default();
        let metrics = ExemplarStoreMetrics::with_initial_state(0, 0, 0);
        Ok(Self {
            path: None,
            local_disk_budget: None,
            config,
            resource_limits,
            state: RwLock::new(state),
            metrics,
            #[cfg(test)]
            query_test_gate: Mutex::new(None),
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(data_path: Option<&Path>) -> Result<Self, ExemplarStoreError> {
        Self::open_with_disk_budget(data_path, None)
    }

    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, ExemplarStoreError> {
        Self::open_with_config_and_disk_budget(
            data_path,
            ExemplarStoreConfig::default(),
            local_disk_budget,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open_with_config(
        data_path: Option<&Path>,
        config: ExemplarStoreConfig,
    ) -> Result<Self, ExemplarStoreError> {
        Self::open_with_config_and_disk_budget(data_path, config, None)
    }

    pub fn open_with_config_and_disk_budget(
        data_path: Option<&Path>,
        config: ExemplarStoreConfig,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, ExemplarStoreError> {
        Self::open_with_config_and_resource_limits_and_disk_budget(
            data_path,
            config,
            ExemplarStoreResourceLimits::default(),
            local_disk_budget,
        )
    }

    pub fn open_with_config_and_resource_limits_and_disk_budget(
        data_path: Option<&Path>,
        config: ExemplarStoreConfig,
        resource_limits: ExemplarStoreResourceLimits,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, ExemplarStoreError> {
        let config = config
            .validate()
            .map_err(ExemplarStoreError::configuration)?;
        let resource_limits = resource_limits.validate()?;
        let path = data_path.map(|path| path.join(EXEMPLAR_STORE_FILE_NAME));
        match (path.as_deref(), local_disk_budget.as_ref()) {
            (Some(path), Some(budget)) => {
                budget.cleanup_atomic_write_temps(path)?;
                budget.validate_managed_file_path(path)?;
            }
            (None, Some(_)) => {
                return Err(ExemplarStoreError::configuration(
                    "exemplars cannot use a local disk budget without a data path",
                ))
            }
            _ => {}
        }
        let (state, durable_file_bytes, startup_peak_bytes) = if let Some(path) = path.as_ref() {
            load_state(path, &config, &resource_limits)?
        } else {
            (ExemplarStoreState::default(), 0, 0)
        };
        let metrics = ExemplarStoreMetrics::with_initial_state(
            state.retained_bytes,
            durable_file_bytes,
            startup_peak_bytes,
        );

        Ok(Self {
            path,
            local_disk_budget,
            config,
            resource_limits,
            state: RwLock::new(state),
            metrics,
            #[cfg(test)]
            query_test_gate: Mutex::new(None),
        })
    }

    pub fn config(&self) -> ExemplarStoreConfig {
        self.config
    }

    pub fn resource_limits(&self) -> ExemplarStoreResourceLimits {
        self.resource_limits
    }

    pub fn apply_writes(
        &self,
        exemplars: &[ExemplarWrite],
    ) -> Result<ExemplarApplyOutcome, ExemplarStoreError> {
        if exemplars.is_empty() {
            return Ok(ExemplarApplyOutcome {
                accepted: 0,
                dropped: 0,
            });
        }

        if exemplars.len() > self.config.max_exemplars_per_request {
            return Err(self.reject_resource(
                ExemplarStoreErrorCode::UpdateBatchEntries,
                exemplars.len(),
                tsink::TsinkError::WriteBatchRowLimitExceeded {
                    limit: self.config.max_exemplars_per_request,
                    submitted: exemplars.len(),
                },
            ));
        }
        let batch_bytes = validate_and_model_exemplar_batch(exemplars, &self.resource_limits)
            .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
        if batch_bytes > self.resource_limits.max_update_batch_bytes {
            return Err(self.reject_resource(
                ExemplarStoreErrorCode::UpdateBatchBytes,
                exemplars.len(),
                tsink::TsinkError::WriteBatchInputLimitExceeded {
                    limit: usize::try_from(self.resource_limits.max_update_batch_bytes)
                        .unwrap_or(usize::MAX),
                    submitted: usize::try_from(batch_bytes).unwrap_or(usize::MAX),
                },
            ));
        }

        let mut state = self
            .write_state()
            .map_err(|message| ExemplarStoreError::from(tsink::TsinkError::Other(message)))?;
        let new_series = count_new_series_without_allocation(&state, exemplars);
        let prospective_series = state.series.len().saturating_add(new_series);
        if prospective_series > self.resource_limits.max_total_series {
            return Err(self.reject_resource(
                ExemplarStoreErrorCode::TotalSeries,
                exemplars.len(),
                tsink::TsinkError::CardinalityLimitExceeded {
                    limit: self.resource_limits.max_total_series,
                    current: state.series.len(),
                    requested: new_series,
                },
            ));
        }

        let mut transient = ExemplarTransientReservation {
            metrics: &self.metrics,
            bytes: 0,
        };
        let mut plan = ExemplarMutationPlan::default();
        self.resize_write_transient(
            &mut transient,
            state.retained_bytes,
            modeled_overlay_bytes(&plan),
        )
        .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;

        for exemplar in exemplars {
            stage_exemplar_write(
                &state,
                &mut plan,
                exemplar,
                self.config.max_exemplars_per_series,
                |requested| {
                    self.resize_write_transient(&mut transient, state.retained_bytes, requested)
                },
            )
            .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
        }

        while effective_exemplar_count(&state, &plan) > self.config.max_total_exemplars {
            let removed = stage_drop_oldest_exemplar(&state, &mut plan, |requested| {
                self.resize_write_transient(&mut transient, state.retained_bytes, requested)
            })
            .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
            if !removed {
                break;
            }
            plan.dropped = plan.dropped.saturating_add(1);
        }

        let candidate_retained_bytes = modeled_effective_state_retained_bytes(&state, &plan);
        if candidate_retained_bytes > self.resource_limits.max_total_retained_bytes {
            return Err(self.reject_resource(
                ExemplarStoreErrorCode::RetainedBytes,
                exemplars.len(),
                tsink::TsinkError::MemoryBudgetExceeded {
                    budget: usize::try_from(self.resource_limits.max_total_retained_bytes)
                        .unwrap_or(usize::MAX),
                    required: usize::try_from(candidate_retained_bytes).unwrap_or(usize::MAX),
                },
            ));
        }

        let mut durable_file_bytes = 0u64;
        if let Some(path) = self.path.as_ref() {
            durable_file_bytes = measure_effective_store_bytes(
                &state,
                &plan,
                self.resource_limits.max_durable_file_bytes,
            )
            .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
            if durable_file_bytes > self.resource_limits.max_persistence_serialization_bytes {
                return Err(self.reject_resource(
                    ExemplarStoreErrorCode::PersistenceSerializationBytes,
                    exemplars.len(),
                    tsink::TsinkError::MemoryBudgetExceeded {
                        budget: usize::try_from(
                            self.resource_limits.max_persistence_serialization_bytes,
                        )
                        .unwrap_or(usize::MAX),
                        required: usize::try_from(durable_file_bytes).unwrap_or(usize::MAX),
                    },
                ));
            }
            let overlay_bytes = modeled_overlay_bytes(&plan);
            let encoded_bytes = if self.local_disk_budget.is_some() {
                modeled_serialized_vec_bytes(durable_file_bytes)
            } else {
                EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES
            };
            let publication_scratch = saturating_u64(plan.series.len())
                .saturating_mul(EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES)
                .saturating_add(EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES);
            let persistence_peak = overlay_bytes
                .saturating_add(encoded_bytes)
                .saturating_add(publication_scratch);
            self.resize_write_transient(&mut transient, state.retained_bytes, persistence_peak)
                .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
            persist_effective_store(
                path,
                &state,
                &plan,
                durable_file_bytes,
                self.local_disk_budget.as_ref(),
                |actual_encoded_bytes| {
                    let actual_persistence_peak = overlay_bytes
                        .saturating_add(actual_encoded_bytes)
                        .saturating_add(publication_scratch);
                    self.resize_write_transient(
                        &mut transient,
                        state.retained_bytes,
                        actual_persistence_peak,
                    )
                },
            )
            .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
        } else {
            let publication_scratch = saturating_u64(plan.series.len())
                .saturating_mul(EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES);
            self.resize_write_transient(
                &mut transient,
                state.retained_bytes,
                modeled_overlay_bytes(&plan).saturating_add(publication_scratch),
            )
            .map_err(|error| self.record_typed_rejection(error, exemplars.len()))?;
        }

        let accepted = exemplars.len();
        let dropped = plan.dropped;
        apply_exemplar_mutation_plan(&mut state, plan);
        state.retained_bytes = candidate_retained_bytes;
        self.metrics.set_retained_bytes(candidate_retained_bytes);
        if self.path.is_some() {
            self.metrics.set_durable_file_bytes(durable_file_bytes);
        }

        self.metrics
            .accepted_total
            .fetch_add(accepted as u64, Ordering::Relaxed);
        self.metrics
            .dropped_total
            .fetch_add(dropped as u64, Ordering::Relaxed);

        Ok(ExemplarApplyOutcome { accepted, dropped })
    }

    pub fn query(
        &self,
        selections: &[SeriesSelection],
        start: i64,
        end: i64,
        limit: usize,
    ) -> Result<Vec<ExemplarSeries>, String> {
        let limit = limit.min(self.config.max_query_results).max(1);
        let compiled = selections
            .iter()
            .map(CompiledSelection::new)
            .collect::<Result<Vec<_>, _>>()?;
        let state = self.read_state()?;
        let mut out = Vec::new();
        let mut total_exemplars = 0usize;

        for (series_key, exemplars) in &state.series {
            if !compiled
                .iter()
                .any(|selection| selection.matches(&series_key.metric, &series_key.labels))
            {
                continue;
            }

            let remaining = limit.saturating_sub(total_exemplars);
            if remaining == 0 {
                break;
            }
            let matched = exemplars
                .range(start..=end)
                .take(remaining)
                .map(|(_, exemplar)| exemplar.clone())
                .collect::<Vec<_>>();
            if matched.is_empty() {
                continue;
            }

            total_exemplars = total_exemplars.saturating_add(matched.len());
            out.push(ExemplarSeries {
                metric: series_key.metric.clone(),
                labels: series_key.labels.clone(),
                exemplars: matched,
            });
            if total_exemplars >= limit {
                break;
            }
        }

        self.metrics
            .query_requests_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .query_series_total
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        self.metrics
            .query_exemplars_total
            .fetch_add(total_exemplars as u64, Ordering::Relaxed);

        Ok(out)
    }

    /// Queries exemplars while charging all selector compilation, scan work, and retained output
    /// memory to `execution`.
    pub fn query_with_execution_result(
        &self,
        selections: &[SeriesSelection],
        start: i64,
        end: i64,
        limit: usize,
        execution: &QueryExecution,
    ) -> Result<AccountedExemplarQueryResult, ExemplarQueryError> {
        execution.checkpoint()?;
        if selections.is_empty()
            || selections.len() > self.config.max_query_selectors
            || limit == 0
            || limit > self.config.max_query_results
            || end < start
        {
            return Err(ExemplarQueryError::InvalidSelection);
        }
        for selection in selections {
            selection
                .validate_shape()
                .map_err(|_| ExemplarQueryError::InvalidSelection)?;
        }
        let compilation_bytes = modeled_compiled_selections_bytes(selections);
        let _compilation_reservation = execution.reserve_memory(compilation_bytes)?;
        let mut compiled = Vec::new();
        compiled
            .try_reserve_exact(selections.len())
            .map_err(|_| ExemplarQueryError::Allocation)?;
        for selection in selections {
            execution.checkpoint()?;
            compiled.push(CompiledSelection::new_bounded(selection)?);
        }

        #[cfg(test)]
        {
            let gate = self
                .query_test_gate
                .lock()
                .expect("exemplar query test gate lock")
                .clone();
            if let Some((entered, release)) = gate {
                entered.wait();
                release.wait();
            }
        }
        execution.checkpoint()?;
        let state = self
            .state
            .read()
            .map_err(|_| ExemplarQueryError::StoreUnavailable)?;
        let mut result_bytes = modeled_vec_capacity_bytes::<ExemplarSeries>(limit);
        let mut result_reservation = execution.reserve_memory(result_bytes)?;
        let mut out = Vec::new();
        out.try_reserve_exact(limit)
            .map_err(|_| ExemplarQueryError::Allocation)?;
        let mut total_exemplars = 0usize;

        for (series_key, exemplars) in &state.series {
            execution.checkpoint()?;
            let mut matched_selection = false;
            for selection in &compiled {
                execution.charge_pattern_expansion(1)?;
                if selection.matches(&series_key.metric, &series_key.labels) {
                    matched_selection = true;
                    break;
                }
            }
            if !matched_selection {
                continue;
            }

            let remaining = limit.saturating_sub(total_exemplars);
            if remaining == 0 {
                break;
            }
            let matched_count = exemplars.range(start..=end).take(remaining).count();
            if matched_count == 0 {
                continue;
            }
            execution.ensure_samples_scanned(saturating_u64(matched_count))?;
            execution.ensure_samples_returned(saturating_u64(matched_count))?;
            execution.ensure_series_matched(1)?;
            execution
                .observe_intermediate_vector_size(saturating_u64(out.len().saturating_add(1)))?;

            let series_bytes = modeled_string_bytes(&series_key.metric)
                .saturating_add(modeled_labels_bytes(&series_key.labels))
                .saturating_add(modeled_vec_capacity_bytes::<ExemplarSample>(matched_count))
                .saturating_add(exemplars.range(start..=end).take(matched_count).fold(
                    0u64,
                    |bytes, (_, exemplar)| {
                        bytes.saturating_add(modeled_labels_bytes(&exemplar.labels))
                    },
                ));
            let logical_returned_bytes = modeled_exemplar_series_logical_bytes(
                series_key,
                exemplars,
                start,
                end,
                matched_count,
            );
            execution.ensure_returned_bytes(logical_returned_bytes)?;
            result_bytes = result_bytes.saturating_add(series_bytes);
            result_reservation.resize(result_bytes)?;

            let mut matched = Vec::new();
            matched
                .try_reserve_exact(matched_count)
                .map_err(|_| ExemplarQueryError::Allocation)?;
            for (_, exemplar) in exemplars.range(start..=end).take(matched_count) {
                execution.checkpoint()?;
                execution.charge_samples_scanned(1)?;
                execution.charge_samples_returned(1)?;
                matched.push(clone_exemplar_sample(exemplar)?);
            }
            execution.charge_series_matched(1)?;
            execution.charge_returned_bytes(logical_returned_bytes)?;
            total_exemplars = total_exemplars.saturating_add(matched.len());
            out.push(ExemplarSeries {
                metric: clone_string(&series_key.metric)?,
                labels: clone_labels(&series_key.labels)?,
                exemplars: matched,
            });
            if total_exemplars >= limit {
                break;
            }
        }
        execution.checkpoint()?;
        result_reservation.resize(modeled_owned_exemplar_series_bytes(&out))?;

        self.metrics
            .query_requests_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .query_series_total
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        self.metrics
            .query_exemplars_total
            .fetch_add(total_exemplars as u64, Ordering::Relaxed);

        Ok(AccountedExemplarQueryResult {
            series: out,
            reservation: result_reservation,
        })
    }

    pub fn record_rejected(&self, count: usize) {
        self.metrics
            .rejected_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn set_query_test_gate(&self, entered: Arc<Barrier>, release: Arc<Barrier>) {
        *self
            .query_test_gate
            .lock()
            .expect("exemplar query test gate lock") = Some((entered, release));
    }

    pub fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), ExemplarStoreError> {
        let snapshot_file = snapshot_path.join(EXEMPLAR_STORE_FILE_NAME);
        let state = self
            .read_state()
            .map_err(|message| ExemplarStoreError::from(tsink::TsinkError::Other(message)))?;
        let plan = ExemplarMutationPlan::default();
        let snapshot_bytes = measure_effective_store_bytes_for(
            &state,
            &plan,
            self.resource_limits.max_snapshot_bytes,
            ExemplarStoreErrorCode::SnapshotBytes,
        )
        .map_err(|error| self.record_typed_rejection(error, 1))?;
        let mut transient = ExemplarTransientReservation {
            metrics: &self.metrics,
            bytes: 0,
        };
        transient
            .resize(
                EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES,
                self.resource_limits.max_snapshot_transient_bytes,
                self.resource_limits.max_concurrent_transient_bytes,
                ExemplarStoreErrorCode::SnapshotTransientBytes,
            )
            .map_err(|error| self.record_typed_rejection(error, 1))?;
        stream_effective_store_file(&snapshot_file, &state, &plan, snapshot_bytes)
    }

    pub fn metrics_snapshot(&self) -> Result<ExemplarStoreMetricsSnapshot, String> {
        let state = self.read_state()?;
        Ok(ExemplarStoreMetricsSnapshot {
            accepted_total: self.metrics.accepted_total.load(Ordering::Relaxed),
            rejected_total: self.metrics.rejected_total.load(Ordering::Relaxed),
            dropped_total: self.metrics.dropped_total.load(Ordering::Relaxed),
            query_requests_total: self.metrics.query_requests_total.load(Ordering::Relaxed),
            query_series_total: self.metrics.query_series_total.load(Ordering::Relaxed),
            query_exemplars_total: self.metrics.query_exemplars_total.load(Ordering::Relaxed),
            stored_series: state.series.len() as u64,
            stored_exemplars: state.exemplar_count() as u64,
            retained_bytes: self.metrics.retained_bytes.load(Ordering::Relaxed),
            peak_retained_bytes: self.metrics.peak_retained_bytes.load(Ordering::Relaxed),
            retained_bytes_limit: self.resource_limits.max_total_retained_bytes,
            durable_file_bytes: self.metrics.durable_file_bytes.load(Ordering::Relaxed),
            peak_durable_file_bytes: self.metrics.peak_durable_file_bytes.load(Ordering::Relaxed),
            durable_file_bytes_limit: self.resource_limits.max_durable_file_bytes,
            transient_bytes: self.metrics.transient_bytes.load(Ordering::Relaxed),
            peak_transient_bytes: self.metrics.peak_transient_bytes.load(Ordering::Relaxed),
            concurrent_transient_bytes_limit: self.resource_limits.max_concurrent_transient_bytes,
            resource_rejections_total: self
                .metrics
                .resource_rejections_total
                .load(Ordering::Relaxed),
            shape_rejections_total: self.metrics.shape_rejections_total.load(Ordering::Relaxed),
            batch_rejections_total: self.metrics.batch_rejections_total.load(Ordering::Relaxed),
            retained_rejections_total: self
                .metrics
                .retained_rejections_total
                .load(Ordering::Relaxed),
            transient_rejections_total: self
                .metrics
                .transient_rejections_total
                .load(Ordering::Relaxed),
            durable_rejections_total: self
                .metrics
                .durable_rejections_total
                .load(Ordering::Relaxed),
            startup_rejections_total: self
                .metrics
                .startup_rejections_total
                .load(Ordering::Relaxed),
            snapshot_rejections_total: self
                .metrics
                .snapshot_rejections_total
                .load(Ordering::Relaxed),
            last_rejection_code: ExemplarStoreErrorCode::from_u64(
                self.metrics.last_rejection_code.load(Ordering::Relaxed),
            ),
        })
    }

    fn read_state(&self) -> Result<RwLockReadGuard<'_, ExemplarStoreState>, String> {
        self.state
            .read()
            .map_err(|_| "exemplar store read lock poisoned".to_string())
    }

    fn write_state(&self) -> Result<RwLockWriteGuard<'_, ExemplarStoreState>, String> {
        self.state
            .write()
            .map_err(|_| "exemplar store write lock poisoned".to_string())
    }

    fn reject_resource(
        &self,
        code: ExemplarStoreErrorCode,
        count: usize,
        source: tsink::TsinkError,
    ) -> ExemplarStoreError {
        self.metrics.record_rejection(code, count);
        ExemplarStoreError::coded(code, source)
    }

    fn record_typed_rejection(
        &self,
        error: ExemplarStoreError,
        count: usize,
    ) -> ExemplarStoreError {
        if let Some(code) = error.code() {
            self.metrics.record_rejection(code, count);
        }
        error
    }

    fn resize_write_transient(
        &self,
        reservation: &mut ExemplarTransientReservation<'_>,
        retained_bytes: u64,
        requested: u64,
    ) -> Result<(), ExemplarStoreError> {
        let replacement_peak = retained_bytes.saturating_add(requested);
        if replacement_peak > self.resource_limits.max_replacement_peak_bytes {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::ReplacementPeakBytes,
                tsink::TsinkError::MemoryBudgetExceeded {
                    budget: usize::try_from(self.resource_limits.max_replacement_peak_bytes)
                        .unwrap_or(usize::MAX),
                    required: usize::try_from(replacement_peak).unwrap_or(usize::MAX),
                },
            ));
        }
        reservation.resize(
            requested,
            self.resource_limits.max_write_transient_bytes,
            self.resource_limits.max_concurrent_transient_bytes,
            ExemplarStoreErrorCode::WriteTransientBytes,
        )
    }
}

#[derive(Debug, Default)]
struct ExemplarMutationPlan {
    /// An empty value removes the corresponding live series.
    series: BTreeMap<SeriesKey, BTreeMap<i64, ExemplarSample>>,
    dropped: usize,
}

fn same_series_identity(key: &SeriesKey, metric: &str, labels: &[Label]) -> bool {
    key.metric == metric && key.labels == labels
}

fn count_new_series_without_allocation(
    state: &ExemplarStoreState,
    writes: &[ExemplarWrite],
) -> usize {
    writes
        .iter()
        .enumerate()
        .filter(|(index, write)| {
            !state
                .series
                .keys()
                .any(|key| same_series_identity(key, &write.metric, &write.series_labels))
                && !writes[..*index].iter().any(|previous| {
                    previous.metric == write.metric && previous.series_labels == write.series_labels
                })
        })
        .count()
}

fn validate_and_model_exemplar_batch(
    writes: &[ExemplarWrite],
    limits: &ExemplarStoreResourceLimits,
) -> Result<u64, ExemplarStoreError> {
    let mut bytes = saturating_u64(writes.len())
        .saturating_mul(saturating_u64(std::mem::size_of::<ExemplarWrite>()))
        .saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES);
    for write in writes {
        if write.metric.is_empty() || write.metric.len() > limits.max_metric_name_bytes {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::InvalidMetricShape,
                tsink::TsinkError::InvalidMetricName(
                    "exemplar metric is empty or exceeds its configured byte limit".to_string(),
                ),
            ));
        }
        if !write.value.is_finite() {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::InvalidExemplarValue,
                tsink::TsinkError::Other("exemplar value must be finite".to_string()),
            ));
        }
        validate_exemplar_labels(
            &write.series_labels,
            limits.max_labels_per_series,
            limits,
            true,
        )
        .map_err(|source| {
            ExemplarStoreError::coded(ExemplarStoreErrorCode::InvalidSeriesLabelShape, source)
        })?;
        validate_exemplar_labels(
            &write.exemplar_labels,
            limits.max_labels_per_exemplar,
            limits,
            false,
        )
        .map_err(|source| {
            ExemplarStoreError::coded(ExemplarStoreErrorCode::InvalidExemplarLabelShape, source)
        })?;
        let identity_bytes = write
            .series_labels
            .iter()
            .fold(write.metric.len(), |total, label| {
                total
                    .saturating_add(label.name.len())
                    .saturating_add(label.value.len())
            });
        if identity_bytes > limits.max_series_identity_bytes {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::InvalidSeriesLabelShape,
                tsink::TsinkError::InvalidLabel(
                    "exemplar series identity exceeds its configured byte limit".to_string(),
                ),
            ));
        }
        let exemplar_label_bytes = write.exemplar_labels.iter().fold(0usize, |total, label| {
            total
                .saturating_add(label.name.len())
                .saturating_add(label.value.len())
        });
        if exemplar_label_bytes > limits.max_exemplar_label_bytes {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::InvalidExemplarLabelShape,
                tsink::TsinkError::InvalidLabel(
                    "exemplar labels exceed their configured cumulative byte limit".to_string(),
                ),
            ));
        }
        bytes = bytes
            .saturating_add(modeled_owned_string_bytes(&write.metric))
            .saturating_add(modeled_owned_labels_bytes(&write.series_labels))
            .saturating_add(modeled_owned_labels_bytes(&write.exemplar_labels));
    }
    Ok(bytes)
}

fn validate_exemplar_labels(
    labels: &[Label],
    maximum_labels: usize,
    limits: &ExemplarStoreResourceLimits,
    allow_tenant_label: bool,
) -> Result<(), tsink::TsinkError> {
    if labels.len() > maximum_labels {
        return Err(tsink::TsinkError::InvalidLabel(
            "exemplar label count exceeds its configured limit".to_string(),
        ));
    }
    for (index, label) in labels.iter().enumerate() {
        if label.name.is_empty()
            || label.name.len() > limits.max_label_name_bytes
            || label.value.len() > limits.max_label_value_bytes
            || label.name == "__name__"
            || (!allow_tenant_label && label.name == "__tsink_tenant__")
            || labels[..index]
                .iter()
                .any(|previous| previous.name == label.name)
        {
            return Err(tsink::TsinkError::InvalidLabel(
                "exemplar labels contain an invalid, reserved, duplicate, or oversized name/value"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn store_allocation_error() -> ExemplarStoreError {
    ExemplarStoreError::coded(
        ExemplarStoreErrorCode::WriteTransientBytes,
        tsink::TsinkError::Other("exemplar store allocation failed".to_string()),
    )
}

fn clone_store_string(value: &str) -> Result<String, ExemplarStoreError> {
    let mut cloned = String::new();
    cloned
        .try_reserve_exact(value.len())
        .map_err(|_| store_allocation_error())?;
    cloned.push_str(value);
    Ok(cloned)
}

fn clone_store_labels(labels: &[Label]) -> Result<Vec<Label>, ExemplarStoreError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(labels.len())
        .map_err(|_| store_allocation_error())?;
    for label in labels {
        cloned.push(Label {
            name: clone_store_string(&label.name)?,
            value: clone_store_string(&label.value)?,
        });
    }
    Ok(cloned)
}

fn clone_store_sample(sample: &ExemplarSample) -> Result<ExemplarSample, ExemplarStoreError> {
    Ok(ExemplarSample {
        labels: clone_store_labels(&sample.labels)?,
        value: sample.value,
        timestamp: sample.timestamp,
    })
}

fn clone_store_sample_map(
    samples: &BTreeMap<i64, ExemplarSample>,
) -> Result<BTreeMap<i64, ExemplarSample>, ExemplarStoreError> {
    let mut cloned = BTreeMap::new();
    for (timestamp, sample) in samples {
        cloned.insert(*timestamp, clone_store_sample(sample)?);
    }
    Ok(cloned)
}

fn modeled_series_key_bytes(key: &SeriesKey) -> u64 {
    modeled_owned_string_bytes(&key.metric).saturating_add(modeled_owned_labels_bytes(&key.labels))
}

fn modeled_sample_bytes(sample: &ExemplarSample) -> u64 {
    EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES
        .saturating_add(modeled_owned_labels_bytes(&sample.labels))
}

fn modeled_sample_map_bytes(samples: &BTreeMap<i64, ExemplarSample>) -> u64 {
    samples.values().fold(
        if samples.is_empty() {
            0
        } else {
            EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES
        },
        |bytes, sample| bytes.saturating_add(modeled_sample_bytes(sample)),
    )
}

fn modeled_series_entry_bytes(key: &SeriesKey, samples: &BTreeMap<i64, ExemplarSample>) -> u64 {
    EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES
        .saturating_add(modeled_series_key_bytes(key))
        .saturating_add(modeled_sample_map_bytes(samples))
}

fn modeled_overlay_bytes(plan: &ExemplarMutationPlan) -> u64 {
    plan.series.iter().fold(
        if plan.series.is_empty() {
            0
        } else {
            EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES
        },
        |bytes, (key, samples)| bytes.saturating_add(modeled_series_entry_bytes(key, samples)),
    )
}

fn modeled_input_key_bytes(metric: &str, labels: &[Label]) -> u64 {
    modeled_string_bytes(metric)
        .saturating_add(modeled_labels_bytes(labels))
        .saturating_add(EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES)
}

fn modeled_input_sample_bytes(labels: &[Label]) -> u64 {
    modeled_labels_bytes(labels).saturating_add(EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES)
}

fn stage_exemplar_write(
    state: &ExemplarStoreState,
    plan: &mut ExemplarMutationPlan,
    write: &ExemplarWrite,
    max_exemplars_per_series: usize,
    mut reserve: impl FnMut(u64) -> Result<(), ExemplarStoreError>,
) -> Result<(), ExemplarStoreError> {
    let sample_upper = modeled_input_sample_bytes(&write.exemplar_labels);
    if plan
        .series
        .keys()
        .any(|key| same_series_identity(key, &write.metric, &write.series_labels))
    {
        reserve(modeled_overlay_bytes(plan).saturating_add(sample_upper))?;
        {
            let samples = plan
                .series
                .iter_mut()
                .find(|(key, _)| same_series_identity(key, &write.metric, &write.series_labels))
                .map(|(_, samples)| samples)
                .expect("checked overlay series exists");
            let replaced = samples.insert(
                write.timestamp,
                ExemplarSample {
                    labels: clone_store_labels(&write.exemplar_labels)?,
                    value: write.value,
                    timestamp: write.timestamp,
                },
            );
            if replaced.is_none() {
                while samples.len() > max_exemplars_per_series {
                    if samples.pop_first().is_some() {
                        plan.dropped = plan.dropped.saturating_add(1);
                    } else {
                        break;
                    }
                }
            }
        }
        reserve(modeled_overlay_bytes(plan))?;
        return Ok(());
    }

    let base = state
        .series
        .iter()
        .find(|(key, _)| same_series_identity(key, &write.metric, &write.series_labels))
        .map(|(_, samples)| samples);
    let base_clone_upper = base.map_or(0, |samples| {
        samples.values().fold(
            EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES,
            |bytes, sample| {
                bytes.saturating_add(
                    EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES
                        .saturating_add(modeled_labels_bytes(&sample.labels)),
                )
            },
        )
    });
    let additional = modeled_input_key_bytes(&write.metric, &write.series_labels)
        .saturating_add(base_clone_upper)
        .saturating_add(sample_upper);
    reserve(modeled_overlay_bytes(plan).saturating_add(additional))?;
    let key = SeriesKey {
        metric: clone_store_string(&write.metric)?,
        labels: clone_store_labels(&write.series_labels)?,
    };
    let mut samples = match base {
        Some(samples) => clone_store_sample_map(samples)?,
        None => BTreeMap::new(),
    };
    let replaced = samples.insert(
        write.timestamp,
        ExemplarSample {
            labels: clone_store_labels(&write.exemplar_labels)?,
            value: write.value,
            timestamp: write.timestamp,
        },
    );
    if replaced.is_none() {
        while samples.len() > max_exemplars_per_series {
            if samples.pop_first().is_some() {
                plan.dropped = plan.dropped.saturating_add(1);
            } else {
                break;
            }
        }
    }
    plan.series.insert(key, samples);
    reserve(modeled_overlay_bytes(plan))
}

enum EffectiveOldest<'a> {
    Base(&'a SeriesKey, &'a BTreeMap<i64, ExemplarSample>),
    Overlay(&'a SeriesKey),
}

type EffectiveOldestCandidate<'a> = (
    i64,
    &'a SeriesKey,
    bool,
    Option<&'a BTreeMap<i64, ExemplarSample>>,
);

fn effective_oldest<'a>(
    state: &'a ExemplarStoreState,
    plan: &'a ExemplarMutationPlan,
) -> Option<EffectiveOldest<'a>> {
    let mut oldest: Option<EffectiveOldestCandidate<'_>> = None;
    for (key, samples) in &state.series {
        if plan.series.contains_key(key) {
            continue;
        }
        let Some((timestamp, _)) = samples.first_key_value() else {
            continue;
        };
        let candidate = (*timestamp, key, false, Some(samples));
        if oldest.as_ref().is_none_or(|(old_timestamp, old_key, ..)| {
            timestamp < old_timestamp || (timestamp == old_timestamp && key < *old_key)
        }) {
            oldest = Some(candidate);
        }
    }
    for (key, samples) in &plan.series {
        let Some((timestamp, _)) = samples.first_key_value() else {
            continue;
        };
        let candidate = (*timestamp, key, true, None);
        if oldest.as_ref().is_none_or(|(old_timestamp, old_key, ..)| {
            timestamp < old_timestamp || (timestamp == old_timestamp && key < *old_key)
        }) {
            oldest = Some(candidate);
        }
    }
    oldest.map(|(_, key, overlay, samples)| {
        if overlay {
            EffectiveOldest::Overlay(key)
        } else {
            EffectiveOldest::Base(key, samples.expect("base oldest has samples"))
        }
    })
}

fn stage_drop_oldest_exemplar(
    state: &ExemplarStoreState,
    plan: &mut ExemplarMutationPlan,
    mut reserve: impl FnMut(u64) -> Result<(), ExemplarStoreError>,
) -> Result<bool, ExemplarStoreError> {
    let Some(oldest) = effective_oldest(state, plan) else {
        return Ok(false);
    };
    match oldest {
        EffectiveOldest::Base(key, samples) => {
            let clone_upper = modeled_input_key_bytes(&key.metric, &key.labels).saturating_add(
                samples.values().fold(
                    EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES,
                    |bytes, sample| {
                        bytes.saturating_add(
                            EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES
                                .saturating_add(modeled_labels_bytes(&sample.labels)),
                        )
                    },
                ),
            );
            reserve(modeled_overlay_bytes(plan).saturating_add(clone_upper))?;
            let staged_key = SeriesKey {
                metric: clone_store_string(&key.metric)?,
                labels: clone_store_labels(&key.labels)?,
            };
            let mut staged_samples = clone_store_sample_map(samples)?;
            let removed = staged_samples.pop_first().is_some();
            plan.series.insert(staged_key, staged_samples);
            reserve(modeled_overlay_bytes(plan))?;
            Ok(removed)
        }
        EffectiveOldest::Overlay(key) => {
            let key_upper = modeled_input_key_bytes(&key.metric, &key.labels);
            reserve(modeled_overlay_bytes(plan).saturating_add(key_upper))?;
            let lookup_key = SeriesKey {
                metric: clone_store_string(&key.metric)?,
                labels: clone_store_labels(&key.labels)?,
            };
            let removed = plan
                .series
                .get_mut(&lookup_key)
                .is_some_and(|samples| samples.pop_first().is_some());
            drop(lookup_key);
            reserve(modeled_overlay_bytes(plan))?;
            Ok(removed)
        }
    }
}

fn effective_exemplar_count(state: &ExemplarStoreState, plan: &ExemplarMutationPlan) -> usize {
    let base = state
        .series
        .iter()
        .filter(|(key, _)| !plan.series.contains_key(*key))
        .fold(0usize, |count, (_, samples)| {
            count.saturating_add(samples.len())
        });
    plan.series
        .values()
        .fold(base, |count, samples| count.saturating_add(samples.len()))
}

fn modeled_effective_state_retained_bytes(
    state: &ExemplarStoreState,
    plan: &ExemplarMutationPlan,
) -> u64 {
    let mut series_count = 0usize;
    let mut bytes = 0u64;
    for (key, samples) in &state.series {
        if plan.series.contains_key(key) {
            continue;
        }
        series_count = series_count.saturating_add(1);
        bytes = bytes.saturating_add(modeled_series_entry_bytes(key, samples));
    }
    for (key, samples) in &plan.series {
        if samples.is_empty() {
            continue;
        }
        series_count = series_count.saturating_add(1);
        bytes = bytes.saturating_add(modeled_series_entry_bytes(key, samples));
    }
    if series_count == 0 {
        0
    } else {
        bytes.saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn apply_exemplar_mutation_plan(state: &mut ExemplarStoreState, plan: ExemplarMutationPlan) {
    for (key, samples) in plan.series {
        if samples.is_empty() {
            state.series.remove(&key);
        } else {
            state.series.insert(key, samples);
        }
    }
}

struct PersistedStoreRef<'a> {
    state: &'a ExemplarStoreState,
    plan: &'a ExemplarMutationPlan,
}

impl Serialize for PersistedStoreRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut store = serializer.serialize_struct("PersistedExemplarStore", 3)?;
        store.serialize_field("magic", EXEMPLAR_STORE_MAGIC)?;
        store.serialize_field("schema_version", &EXEMPLAR_STORE_SCHEMA_VERSION)?;
        store.serialize_field(
            "entries",
            &PersistedEntriesRef {
                state: self.state,
                plan: self.plan,
            },
        )?;
        store.end()
    }
}

struct PersistedEntriesRef<'a> {
    state: &'a ExemplarStoreState,
    plan: &'a ExemplarMutationPlan,
}

impl Serialize for PersistedEntriesRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entries =
            serializer.serialize_seq(Some(effective_exemplar_count(self.state, self.plan)))?;
        for (key, samples) in &self.state.series {
            if self.plan.series.contains_key(key) {
                continue;
            }
            for sample in samples.values() {
                entries.serialize_element(&PersistedWriteRef { key, sample })?;
            }
        }
        for (key, samples) in &self.plan.series {
            for sample in samples.values() {
                entries.serialize_element(&PersistedWriteRef { key, sample })?;
            }
        }
        entries.end()
    }
}

struct PersistedWriteRef<'a> {
    key: &'a SeriesKey,
    sample: &'a ExemplarSample,
}

impl Serialize for PersistedWriteRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut write = serializer.serialize_struct("ExemplarWrite", 5)?;
        write.serialize_field("metric", &self.key.metric)?;
        write.serialize_field("series_labels", &self.key.labels)?;
        write.serialize_field("exemplar_labels", &self.sample.labels)?;
        write.serialize_field("timestamp", &self.sample.timestamp)?;
        write.serialize_field("value", &self.sample.value)?;
        write.end()
    }
}

struct LimitedLengthWriter {
    bytes: u64,
    limit: u64,
    exceeded: bool,
}

impl Write for LimitedLengthWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let requested = self
            .bytes
            .saturating_add(u64::try_from(buffer.len()).unwrap_or(u64::MAX));
        if requested > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bounded exemplar serialization exceeded its byte limit",
            ));
        }
        self.bytes = requested;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn measure_effective_store_bytes(
    state: &ExemplarStoreState,
    plan: &ExemplarMutationPlan,
    limit: u64,
) -> Result<u64, ExemplarStoreError> {
    measure_effective_store_bytes_for(state, plan, limit, ExemplarStoreErrorCode::DurableFileBytes)
}

fn measure_effective_store_bytes_for(
    state: &ExemplarStoreState,
    plan: &ExemplarMutationPlan,
    limit: u64,
    code: ExemplarStoreErrorCode,
) -> Result<u64, ExemplarStoreError> {
    let mut writer = LimitedLengthWriter {
        bytes: 0,
        limit,
        exceeded: false,
    };
    let payload = PersistedStoreRef { state, plan };
    if let Err(error) = serde_json::to_writer_pretty(&mut writer, &payload) {
        if writer.exceeded {
            return Err(ExemplarStoreError::coded(
                code,
                tsink::TsinkError::DiskQuotaExceeded {
                    limit,
                    used: 0,
                    reserved: 0,
                    requested: limit.saturating_add(1),
                },
            ));
        }
        return Err(ExemplarStoreError::from(tsink::TsinkError::Json(error)));
    }
    writer.write_all(b"\n").map_err(|error| {
        if writer.exceeded {
            ExemplarStoreError::coded(
                code,
                tsink::TsinkError::DiskQuotaExceeded {
                    limit,
                    used: 0,
                    reserved: 0,
                    requested: limit.saturating_add(1),
                },
            )
        } else {
            ExemplarStoreError::from(tsink::TsinkError::Io(error))
        }
    })?;
    Ok(writer.bytes)
}

fn modeled_serialized_vec_bytes(bytes: u64) -> u64 {
    bytes.saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES)
}

/// Conservative upper bound for the one decoded entry owned by the streaming visitor.
///
/// JSON input bytes are charged separately. The doubled label-vector capacities cover geometric
/// growth, while every possible owned string and vector receives the portable allocation
/// allowance used by the reconciled capacity model.
fn modeled_max_startup_entry_bytes(limits: &ExemplarStoreResourceLimits) -> u64 {
    let series_label_capacity = limits.max_labels_per_series.saturating_mul(2).max(4);
    let exemplar_label_capacity = limits.max_labels_per_exemplar.saturating_mul(2).max(4);
    let label_vec_bytes = |capacity: usize| {
        saturating_u64(capacity)
            .saturating_mul(saturating_u64(std::mem::size_of::<Label>()))
            .saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES)
    };
    let owned_string_count = 1usize
        .saturating_add(limits.max_labels_per_series.saturating_mul(2))
        .saturating_add(limits.max_labels_per_exemplar.saturating_mul(2));
    saturating_u64(std::mem::size_of::<ExemplarWrite>())
        .saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(saturating_u64(limits.max_series_identity_bytes))
        .saturating_add(saturating_u64(limits.max_exemplar_label_bytes))
        .saturating_add(label_vec_bytes(series_label_capacity))
        .saturating_add(label_vec_bytes(exemplar_label_capacity))
        .saturating_add(
            saturating_u64(owned_string_count)
                .saturating_mul(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES),
        )
}

fn modeled_startup_predecode_peak(file_bytes: u64, limits: &ExemplarStoreResourceLimits) -> u64 {
    // The reader never owns the complete raw file, but charging its full logical size covers
    // decoder string slack before shape validation. Normalized state, one decoded entry, and the
    // parser/file buffer are then independently bounded.
    file_bytes
        .saturating_add(limits.max_total_retained_bytes)
        .saturating_add(modeled_max_startup_entry_bytes(limits))
        .saturating_add(EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES)
}

fn write_effective_store_payload(
    writer: &mut dyn Write,
    state: &ExemplarStoreState,
    plan: &ExemplarMutationPlan,
) -> Result<(), ExemplarStoreError> {
    serde_json::to_writer_pretty(&mut *writer, &PersistedStoreRef { state, plan })
        .map_err(|error| ExemplarStoreError::from(tsink::TsinkError::Json(error)))?;
    writer
        .write_all(b"\n")
        .map_err(|error| ExemplarStoreError::from(tsink::TsinkError::Io(error)))
}

fn stream_effective_store_file(
    path: &Path,
    state: &ExemplarStoreState,
    plan: &ExemplarMutationPlan,
    expected_bytes: u64,
) -> Result<(), ExemplarStoreError> {
    tsink::engine::fs_utils::write_file_atomically_and_sync_parent_with(
        path,
        expected_bytes,
        |writer| write_effective_store_payload(writer, state, plan).map_err(|error| error.source),
    )
    .map_err(ExemplarStoreError::from)
}

fn persist_effective_store(
    path: &Path,
    state: &ExemplarStoreState,
    plan: &ExemplarMutationPlan,
    expected_bytes: u64,
    local_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
    mut admit_encoded: impl FnMut(u64) -> Result<(), ExemplarStoreError>,
) -> Result<(), ExemplarStoreError> {
    let Some(local_disk_budget) = local_disk_budget else {
        return stream_effective_store_file(path, state, plan, expected_bytes);
    };
    let expected = usize::try_from(expected_bytes).map_err(|_| {
        ExemplarStoreError::coded(
            ExemplarStoreErrorCode::PersistenceSerializationBytes,
            tsink::TsinkError::MemoryBudgetExceeded {
                budget: usize::MAX,
                required: usize::MAX,
            },
        )
    })?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(expected)
        .map_err(|_| store_allocation_error())?;
    write_effective_store_payload(&mut encoded, state, plan)?;
    if encoded.len() != expected {
        return Err(ExemplarStoreError::from(tsink::TsinkError::Other(
            "exemplar serialization length changed after preflight".to_string(),
        )));
    }
    admit_encoded(modeled_serialized_vec_bytes(saturating_u64(
        encoded.capacity(),
    )))?;
    local_disk_budget
        .write_file_atomically_and_sync_parent(path, &encoded, tsink::DiskCategory::Exemplars)
        .map_err(ExemplarStoreError::from)
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum PersistedStoreField {
    Magic,
    SchemaVersion,
    Entries,
    #[serde(other)]
    Other,
}

struct StartupDecodeContext<'a> {
    config: &'a ExemplarStoreConfig,
    limits: &'a ExemplarStoreResourceLimits,
    state: ExemplarStoreState,
    entries: usize,
    peak_bytes: u64,
    failure: Option<ExemplarStoreError>,
}

struct PersistedStoreSeed<'context, 'limits> {
    context: &'context mut StartupDecodeContext<'limits>,
}

impl<'de> DeserializeSeed<'de> for PersistedStoreSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "PersistedExemplarStore",
            &["magic", "schema_version", "entries"],
            PersistedStoreVisitor {
                context: self.context,
            },
        )
    }
}

struct PersistedStoreVisitor<'context, 'limits> {
    context: &'context mut StartupDecodeContext<'limits>,
}

impl<'de> Visitor<'de> for PersistedStoreVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a persisted exemplar store object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut magic = None;
        let mut schema_version = None;
        let mut saw_entries = false;
        while let Some(field) = map.next_key::<PersistedStoreField>()? {
            match field {
                PersistedStoreField::Magic => {
                    if magic.is_some() {
                        return Err(<A::Error as serde::de::Error>::duplicate_field("magic"));
                    }
                    magic = Some(map.next_value::<String>()?);
                }
                PersistedStoreField::SchemaVersion => {
                    if schema_version.is_some() {
                        return Err(<A::Error as serde::de::Error>::duplicate_field(
                            "schema_version",
                        ));
                    }
                    schema_version = Some(map.next_value::<u16>()?);
                }
                PersistedStoreField::Entries => {
                    if saw_entries {
                        return Err(<A::Error as serde::de::Error>::duplicate_field("entries"));
                    }
                    saw_entries = true;
                    map.next_value_seed(PersistedEntriesSeed {
                        context: self.context,
                    })?;
                }
                PersistedStoreField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let Some(magic) = magic else {
            return Err(<A::Error as serde::de::Error>::missing_field("magic"));
        };
        let Some(schema_version) = schema_version else {
            return Err(<A::Error as serde::de::Error>::missing_field(
                "schema_version",
            ));
        };
        if !saw_entries {
            return Err(<A::Error as serde::de::Error>::missing_field("entries"));
        }
        if magic != EXEMPLAR_STORE_MAGIC || schema_version != EXEMPLAR_STORE_SCHEMA_VERSION {
            self.context.failure = Some(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::StartupFormat,
                tsink::TsinkError::DataCorruption(
                    "exemplar store magic or schema version is unsupported".to_string(),
                ),
            ));
            return Err(<A::Error as serde::de::Error>::custom(
                "unsupported exemplar store format",
            ));
        }
        Ok(())
    }
}

struct PersistedEntriesSeed<'context, 'limits> {
    context: &'context mut StartupDecodeContext<'limits>,
}

impl<'de> DeserializeSeed<'de> for PersistedEntriesSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(PersistedEntriesVisitor {
            context: self.context,
        })
    }
}

struct PersistedEntriesVisitor<'context, 'limits> {
    context: &'context mut StartupDecodeContext<'limits>,
}

impl<'de> Visitor<'de> for PersistedEntriesVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sequence of persisted exemplars")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(entry) = sequence.next_element::<ExemplarWrite>()? {
            if let Err(error) = stage_startup_entry(self.context, entry) {
                self.context.failure = Some(error);
                return Err(<A::Error as serde::de::Error>::custom(
                    "persisted exemplar exceeded its startup envelope",
                ));
            }
        }
        Ok(())
    }
}

fn admit_startup_candidate(
    context: &StartupDecodeContext<'_>,
    candidate_retained_bytes: u64,
    entry_bytes: u64,
    decode_peak: u64,
) -> Result<u64, ExemplarStoreError> {
    if candidate_retained_bytes > context.limits.max_total_retained_bytes {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::RetainedBytes,
            tsink::TsinkError::MemoryBudgetExceeded {
                budget: usize::try_from(context.limits.max_total_retained_bytes)
                    .unwrap_or(usize::MAX),
                required: usize::try_from(candidate_retained_bytes).unwrap_or(usize::MAX),
            },
        ));
    }
    let startup_peak = decode_peak.max(
        candidate_retained_bytes
            .saturating_add(entry_bytes)
            .saturating_add(EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES),
    );
    if startup_peak > context.limits.max_startup_transient_bytes {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupTransientBytes,
            tsink::TsinkError::MemoryBudgetExceeded {
                budget: usize::try_from(context.limits.max_startup_transient_bytes)
                    .unwrap_or(usize::MAX),
                required: usize::try_from(startup_peak).unwrap_or(usize::MAX),
            },
        ));
    }
    Ok(startup_peak)
}

fn stage_startup_entry(
    context: &mut StartupDecodeContext<'_>,
    entry: ExemplarWrite,
) -> Result<(), ExemplarStoreError> {
    if context.entries >= context.config.max_total_exemplars {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupEntries,
            tsink::TsinkError::CardinalityLimitExceeded {
                limit: context.config.max_total_exemplars,
                current: context.entries,
                requested: 1,
            },
        ));
    }
    let entry_bytes =
        validate_and_model_exemplar_batch(std::slice::from_ref(&entry), context.limits)?;
    let decode_peak = context
        .state
        .retained_bytes
        .saturating_add(entry_bytes)
        .saturating_add(EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES);

    let existing = context
        .state
        .series
        .iter()
        .find(|(key, _)| same_series_identity(key, &entry.metric, &entry.series_labels))
        .map(|(_, samples)| samples);
    if existing.is_none() && context.state.series.len() >= context.limits.max_total_series {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::TotalSeries,
            tsink::TsinkError::CardinalityLimitExceeded {
                limit: context.limits.max_total_series,
                current: context.state.series.len(),
                requested: 1,
            },
        ));
    }
    if let Some(samples) = existing {
        if samples.contains_key(&entry.timestamp) {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::StartupFormat,
                tsink::TsinkError::DataCorruption(
                    "exemplar store contains duplicate series timestamps".to_string(),
                ),
            ));
        }
        if samples.len() >= context.config.max_exemplars_per_series {
            return Err(ExemplarStoreError::coded(
                ExemplarStoreErrorCode::StartupEntries,
                tsink::TsinkError::CardinalityLimitExceeded {
                    limit: context.config.max_exemplars_per_series,
                    current: samples.len(),
                    requested: 1,
                },
            ));
        }
    }
    let has_existing = existing.is_some();

    let sample_bytes = modeled_input_sample_bytes(&entry.exemplar_labels);
    let preflight_retained_bytes = if has_existing {
        context.state.retained_bytes.saturating_add(sample_bytes)
    } else {
        context
            .state
            .retained_bytes
            .saturating_add(if context.state.series.is_empty() {
                EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES
            } else {
                0
            })
            .saturating_add(modeled_input_key_bytes(&entry.metric, &entry.series_labels))
            .saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES)
            .saturating_add(sample_bytes)
    };
    admit_startup_candidate(context, preflight_retained_bytes, entry_bytes, decode_peak)?;

    let sample = ExemplarSample {
        labels: clone_store_labels(&entry.exemplar_labels).map_err(|error| {
            ExemplarStoreError::coded(ExemplarStoreErrorCode::StartupTransientBytes, error.source)
        })?,
        value: entry.value,
        timestamp: entry.timestamp,
    };
    let (candidate_retained_bytes, startup_peak) = if has_existing {
        let candidate_retained_bytes = context
            .state
            .retained_bytes
            .saturating_add(modeled_sample_bytes(&sample));
        let startup_peak =
            admit_startup_candidate(context, candidate_retained_bytes, entry_bytes, decode_peak)?;
        context
            .state
            .series
            .iter_mut()
            .find(|(key, _)| same_series_identity(key, &entry.metric, &entry.series_labels))
            .map(|(_, samples)| samples)
            .expect("checked startup series exists")
            .insert(entry.timestamp, sample);
        (candidate_retained_bytes, startup_peak)
    } else {
        let key = SeriesKey {
            metric: clone_store_string(&entry.metric).map_err(|error| {
                ExemplarStoreError::coded(
                    ExemplarStoreErrorCode::StartupTransientBytes,
                    error.source,
                )
            })?,
            labels: clone_store_labels(&entry.series_labels).map_err(|error| {
                ExemplarStoreError::coded(
                    ExemplarStoreErrorCode::StartupTransientBytes,
                    error.source,
                )
            })?,
        };
        let candidate_retained_bytes = context
            .state
            .retained_bytes
            .saturating_add(if context.state.series.is_empty() {
                EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES
            } else {
                0
            })
            .saturating_add(EXEMPLAR_STORE_BTREE_NODE_ALLOWANCE_BYTES)
            .saturating_add(modeled_series_key_bytes(&key))
            .saturating_add(EXEMPLAR_STORE_ALLOCATION_ALLOWANCE_BYTES)
            .saturating_add(modeled_sample_bytes(&sample));
        let startup_peak =
            admit_startup_candidate(context, candidate_retained_bytes, entry_bytes, decode_peak)?;
        context
            .state
            .series
            .entry(key)
            .or_default()
            .insert(entry.timestamp, sample);
        (candidate_retained_bytes, startup_peak)
    };
    context.entries = context.entries.saturating_add(1);
    context.state.retained_bytes = candidate_retained_bytes;
    context.peak_bytes = context.peak_bytes.max(startup_peak);
    Ok(())
}

fn load_state(
    path: &Path,
    config: &ExemplarStoreConfig,
    limits: &ExemplarStoreResourceLimits,
) -> Result<(ExemplarStoreState, u64, u64), ExemplarStoreError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((ExemplarStoreState::default(), 0, 0));
        }
        Err(source) => {
            return Err(ExemplarStoreError::from(tsink::TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            }));
        }
    };
    let file_bytes = metadata.len();
    if file_bytes > limits.max_durable_file_bytes {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupFileBytes,
            tsink::TsinkError::DiskQuotaExceeded {
                limit: limits.max_durable_file_bytes,
                used: 0,
                reserved: 0,
                requested: file_bytes,
            },
        ));
    }
    let preparse_transient = modeled_startup_predecode_peak(file_bytes, limits);
    if preparse_transient > limits.max_startup_transient_bytes {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupTransientBytes,
            tsink::TsinkError::MemoryBudgetExceeded {
                budget: usize::try_from(limits.max_startup_transient_bytes).unwrap_or(usize::MAX),
                required: usize::try_from(preparse_transient).unwrap_or(usize::MAX),
            },
        ));
    }
    let file = std::fs::File::open(path).map_err(|source| {
        ExemplarStoreError::from(tsink::TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })
    })?;
    let reader = std::io::BufReader::with_capacity(
        usize::try_from(EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES).unwrap_or(16 * 1024),
        file.take(file_bytes.saturating_add(1)),
    );
    let mut context = StartupDecodeContext {
        config,
        limits,
        state: ExemplarStoreState::default(),
        entries: 0,
        peak_bytes: preparse_transient,
        failure: None,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let decoded = PersistedStoreSeed {
        context: &mut context,
    }
    .deserialize(&mut deserializer)
    .and_then(|()| deserializer.end());
    if let Err(error) = decoded {
        if let Some(failure) = context.failure {
            return Err(failure);
        }
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupFormat,
            tsink::TsinkError::Json(error),
        ));
    }
    let final_file_bytes = std::fs::metadata(path)
        .map_err(|source| {
            ExemplarStoreError::from(tsink::TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        })?
        .len();
    if final_file_bytes > limits.max_durable_file_bytes {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupFileBytes,
            tsink::TsinkError::DiskQuotaExceeded {
                limit: limits.max_durable_file_bytes,
                used: 0,
                reserved: 0,
                requested: final_file_bytes,
            },
        ));
    }
    if final_file_bytes != file_bytes {
        return Err(ExemplarStoreError::coded(
            ExemplarStoreErrorCode::StartupFormat,
            tsink::TsinkError::DataCorruption(
                "exemplar store file changed while startup was decoding it".to_string(),
            ),
        ));
    }
    Ok((context.state, file_bytes, context.peak_bytes))
}

#[derive(Debug)]
struct CompiledSelection {
    metric: Option<String>,
    matchers: Vec<CompiledMatcher>,
}

impl CompiledSelection {
    fn new(selection: &SeriesSelection) -> Result<Self, String> {
        let matchers = selection
            .matchers
            .iter()
            .map(CompiledMatcher::new)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            metric: selection.metric.clone(),
            matchers,
        })
    }

    fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        if self
            .metric
            .as_deref()
            .is_some_and(|expected| expected != metric)
        {
            return false;
        }
        self.matchers
            .iter()
            .all(|matcher| matcher.matches(metric, labels))
    }

    fn new_bounded(selection: &SeriesSelection) -> Result<Self, ExemplarQueryError> {
        let mut matchers = Vec::new();
        matchers
            .try_reserve_exact(selection.matchers.len())
            .map_err(|_| ExemplarQueryError::Allocation)?;
        for matcher in &selection.matchers {
            matchers.push(CompiledMatcher::new_bounded(matcher)?);
        }
        Ok(Self {
            metric: selection.metric.as_deref().map(clone_string).transpose()?,
            matchers,
        })
    }
}

#[derive(Debug)]
struct CompiledMatcher {
    name: String,
    op: SeriesMatcherOp,
    value: String,
    regex: Option<Regex>,
}

impl CompiledMatcher {
    fn new(matcher: &SeriesMatcher) -> Result<Self, String> {
        if matcher.name.trim().is_empty() {
            return Err("series matcher label name cannot be empty".to_string());
        }
        let regex =
            match matcher.op {
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                    let anchored = format!("^(?:{})$", matcher.value);
                    Some(Regex::new(&anchored).map_err(|err| {
                        format!("invalid matcher regex '{}': {err}", matcher.value)
                    })?)
                }
                SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => None,
            };
        Ok(Self {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        })
    }

    fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        let actual = if self.name == "__name__" {
            Some(metric)
        } else {
            labels
                .iter()
                .find(|label| label.name == self.name)
                .map(|label| label.value.as_str())
        };

        match self.op {
            SeriesMatcherOp::Equal => actual.is_some_and(|value| value == self.value),
            SeriesMatcherOp::NotEqual => actual.is_none_or(|value| value != self.value),
            SeriesMatcherOp::RegexMatch => actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
            SeriesMatcherOp::RegexNoMatch => !actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
        }
    }

    fn new_bounded(matcher: &SeriesMatcher) -> Result<Self, ExemplarQueryError> {
        if matcher.name.trim().is_empty() {
            return Err(ExemplarQueryError::InvalidSelection);
        }
        let regex = match matcher.op {
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                let anchored_capacity = matcher.value.len().saturating_add(6);
                let mut anchored = String::new();
                anchored
                    .try_reserve_exact(anchored_capacity)
                    .map_err(|_| ExemplarQueryError::Allocation)?;
                anchored.push_str("^(?:");
                anchored.push_str(&matcher.value);
                anchored.push_str(")$");
                Some(
                    RegexBuilder::new(&anchored)
                        .size_limit(EXEMPLAR_QUERY_REGEX_SIZE_LIMIT_BYTES)
                        .dfa_size_limit(EXEMPLAR_QUERY_REGEX_SIZE_LIMIT_BYTES)
                        .build()
                        .map_err(|_| ExemplarQueryError::InvalidSelection)?,
                )
            }
            SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => None,
        };
        Ok(Self {
            name: clone_string(&matcher.name)?,
            op: matcher.op,
            value: clone_string(&matcher.value)?,
            regex,
        })
    }
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    saturating_u64(capacity)
        .saturating_mul(saturating_u64(std::mem::size_of::<T>()))
        .saturating_add(EXEMPLAR_QUERY_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_string_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        saturating_u64(value.len()).saturating_add(EXEMPLAR_QUERY_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_labels_bytes(labels: &[Label]) -> u64 {
    modeled_vec_capacity_bytes::<Label>(labels.len()).saturating_add(labels.iter().fold(
        0u64,
        |bytes, label| {
            bytes
                .saturating_add(modeled_string_bytes(&label.name))
                .saturating_add(modeled_string_bytes(&label.value))
        },
    ))
}

fn modeled_owned_string_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        saturating_u64(value.capacity()).saturating_add(EXEMPLAR_QUERY_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_owned_labels_bytes(labels: &Vec<Label>) -> u64 {
    modeled_vec_capacity_bytes::<Label>(labels.capacity()).saturating_add(labels.iter().fold(
        0u64,
        |bytes, label| {
            bytes
                .saturating_add(modeled_owned_string_bytes(&label.name))
                .saturating_add(modeled_owned_string_bytes(&label.value))
        },
    ))
}

fn modeled_owned_exemplar_series_bytes(series: &Vec<ExemplarSeries>) -> u64 {
    modeled_vec_capacity_bytes::<ExemplarSeries>(series.capacity()).saturating_add(
        series.iter().fold(0u64, |bytes, item| {
            bytes
                .saturating_add(modeled_owned_string_bytes(&item.metric))
                .saturating_add(modeled_owned_labels_bytes(&item.labels))
                .saturating_add(modeled_vec_capacity_bytes::<ExemplarSample>(
                    item.exemplars.capacity(),
                ))
                .saturating_add(
                    item.exemplars
                        .iter()
                        .fold(0u64, |exemplar_bytes, exemplar| {
                            exemplar_bytes
                                .saturating_add(modeled_owned_labels_bytes(&exemplar.labels))
                        }),
                )
        }),
    )
}

fn modeled_compiled_selections_bytes(selections: &[SeriesSelection]) -> u64 {
    selections.iter().fold(
        modeled_vec_capacity_bytes::<CompiledSelection>(selections.len()),
        |bytes, selection| {
            bytes
                .saturating_add(selection.metric.as_deref().map_or(0, modeled_string_bytes))
                .saturating_add(modeled_vec_capacity_bytes::<CompiledMatcher>(
                    selection.matchers.len(),
                ))
                .saturating_add(
                    selection
                        .matchers
                        .iter()
                        .fold(0u64, |matcher_bytes, matcher| {
                            matcher_bytes
                                .saturating_add(modeled_string_bytes(&matcher.name))
                                .saturating_add(modeled_string_bytes(&matcher.value))
                                .saturating_add(
                                    if matches!(
                                        matcher.op,
                                        SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
                                    ) {
                                        saturating_u64(EXEMPLAR_QUERY_REGEX_SIZE_LIMIT_BYTES)
                                            .saturating_add(
                                                modeled_string_bytes(&matcher.value)
                                                    .saturating_add(6),
                                            )
                                    } else {
                                        0
                                    },
                                )
                        }),
                )
        },
    )
}

fn modeled_exemplar_series_logical_bytes(
    series_key: &SeriesKey,
    exemplars: &BTreeMap<i64, ExemplarSample>,
    start: i64,
    end: i64,
    matched_count: usize,
) -> u64 {
    let series_identity = saturating_u64(series_key.metric.len()).saturating_add(
        series_key.labels.iter().fold(0u64, |bytes, label| {
            bytes
                .saturating_add(saturating_u64(label.name.len()))
                .saturating_add(saturating_u64(label.value.len()))
        }),
    );
    exemplars.range(start..=end).take(matched_count).fold(
        series_identity,
        |bytes, (_, exemplar)| {
            bytes
                .saturating_add(16)
                .saturating_add(exemplar.labels.iter().fold(0u64, |label_bytes, label| {
                    label_bytes
                        .saturating_add(saturating_u64(label.name.len()))
                        .saturating_add(saturating_u64(label.value.len()))
                }))
        },
    )
}

fn clone_string(value: &str) -> Result<String, ExemplarQueryError> {
    let mut cloned = String::new();
    cloned
        .try_reserve_exact(value.len())
        .map_err(|_| ExemplarQueryError::Allocation)?;
    cloned.push_str(value);
    Ok(cloned)
}

fn clone_labels(labels: &[Label]) -> Result<Vec<Label>, ExemplarQueryError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(labels.len())
        .map_err(|_| ExemplarQueryError::Allocation)?;
    for label in labels {
        cloned.push(Label {
            name: clone_string(&label.name)?,
            value: clone_string(&label.value)?,
        });
    }
    Ok(cloned)
}

fn clone_exemplar_sample(exemplar: &ExemplarSample) -> Result<ExemplarSample, ExemplarQueryError> {
    Ok(ExemplarSample {
        labels: clone_labels(&exemplar.labels)?,
        value: exemplar.value,
        timestamp: exemplar.timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn resource_test_config() -> ExemplarStoreConfig {
        ExemplarStoreConfig {
            max_total_exemplars: 32,
            max_exemplars_per_series: 16,
            max_exemplars_per_request: 16,
            max_query_results: 32,
            max_query_selectors: 4,
        }
    }

    fn resource_test_write(
        metric: &str,
        series: &str,
        trace: &str,
        timestamp: i64,
    ) -> ExemplarWrite {
        ExemplarWrite {
            metric: metric.to_string(),
            series_labels: vec![Label::new("job", series)],
            exemplar_labels: vec![Label::new("trace_id", trace)],
            timestamp,
            value: timestamp as f64,
        }
    }

    fn in_memory_resource_store(limits: ExemplarStoreResourceLimits) -> ExemplarStore {
        ExemplarStore::in_memory_with_config_and_resource_limits(resource_test_config(), limits)
            .expect("resource-test limits should be valid")
    }

    fn persistent_resource_store(
        path: &Path,
        limits: ExemplarStoreResourceLimits,
    ) -> Result<ExemplarStore, ExemplarStoreError> {
        ExemplarStore::open_with_config_and_resource_limits_and_disk_budget(
            Some(path),
            resource_test_config(),
            limits,
            None,
        )
    }

    fn assert_store_rejection(
        store: &ExemplarStore,
        error: &ExemplarStoreError,
        code: ExemplarStoreErrorCode,
        submitted: usize,
    ) {
        assert_eq!(error.code(), Some(code), "{error}");
        let metrics = store.metrics_snapshot().expect("metrics should load");
        assert_eq!(metrics.last_rejection_code, Some(code));
        assert_eq!(metrics.rejected_total, submitted as u64);
        assert_eq!(metrics.resource_rejections_total, 1);
        assert_eq!(metrics.transient_bytes, 0);
    }

    fn query_budget_with_memory_limit(
        memory_limit: u64,
    ) -> (tsink::QueryBudget, tsink::QueryExecution) {
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("valid exemplar query budget");
        let execution = budget.begin_query().expect("admitted exemplar query");
        (budget, execution)
    }

    fn one_exemplar_store() -> ExemplarStore {
        let store = ExemplarStore::in_memory_with_config(ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 2,
        });
        store
            .apply_writes(&[ExemplarWrite {
                metric: "latency_seconds".to_string(),
                series_labels: vec![Label::new("job", "api")],
                exemplar_labels: vec![Label::new("trace_id", "abc")],
                timestamp: 10,
                value: 1.5,
            }])
            .expect("seed exemplar");
        store
    }

    #[test]
    fn exemplar_store_persists_and_queries_series() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = ExemplarStore::open(Some(temp_dir.path())).expect("store should open");
        store
            .apply_writes(&[ExemplarWrite {
                metric: "http_request_duration_seconds".to_string(),
                series_labels: vec![
                    Label::new("job", "api"),
                    Label::new("__tsink_tenant__", "a"),
                ],
                exemplar_labels: vec![Label::new("trace_id", "abc")],
                timestamp: 10,
                value: 0.42,
            }])
            .expect("write should persist");

        let reopened = ExemplarStore::open(Some(temp_dir.path())).expect("store should reopen");
        let queried = reopened
            .query(
                &[SeriesSelection::new()
                    .with_metric("http_request_duration_seconds")
                    .with_matcher(SeriesMatcher::equal("job", "api"))
                    .with_matcher(SeriesMatcher::equal("__tsink_tenant__", "a"))],
                0,
                20,
                10,
            )
            .expect("query should succeed");
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].exemplars.len(), 1);
        assert_eq!(queried[0].exemplars[0].labels[0].name, "trace_id");
    }

    #[test]
    fn exemplar_store_enforces_global_and_series_bounds() {
        let store = ExemplarStore::in_memory_with_config(ExemplarStoreConfig {
            max_total_exemplars: 2,
            max_exemplars_per_series: 1,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 4,
        });
        store
            .apply_writes(&[
                ExemplarWrite {
                    metric: "metric_a".to_string(),
                    series_labels: vec![Label::new("job", "a")],
                    exemplar_labels: vec![Label::new("trace_id", "1")],
                    timestamp: 10,
                    value: 1.0,
                },
                ExemplarWrite {
                    metric: "metric_a".to_string(),
                    series_labels: vec![Label::new("job", "a")],
                    exemplar_labels: vec![Label::new("trace_id", "2")],
                    timestamp: 20,
                    value: 2.0,
                },
                ExemplarWrite {
                    metric: "metric_b".to_string(),
                    series_labels: vec![Label::new("job", "b")],
                    exemplar_labels: vec![Label::new("trace_id", "3")],
                    timestamp: 30,
                    value: 3.0,
                },
            ])
            .expect("writes should succeed");

        let metrics = store.metrics_snapshot().expect("metrics should load");
        assert_eq!(metrics.stored_exemplars, 2);
        assert_eq!(metrics.dropped_total, 1);
    }

    #[test]
    fn exemplar_store_query_supports_regex_matchers() {
        let store = ExemplarStore::in_memory();
        store
            .apply_writes(&[
                ExemplarWrite {
                    metric: "metric_a".to_string(),
                    series_labels: vec![Label::new("job", "api-a")],
                    exemplar_labels: vec![Label::new("trace_id", "1")],
                    timestamp: 10,
                    value: 1.0,
                },
                ExemplarWrite {
                    metric: "metric_b".to_string(),
                    series_labels: vec![Label::new("job", "worker-b")],
                    exemplar_labels: vec![Label::new("trace_id", "2")],
                    timestamp: 20,
                    value: 2.0,
                },
            ])
            .expect("writes should succeed");

        let queried =
            store
                .query(
                    &[SeriesSelection::new()
                        .with_matcher(SeriesMatcher::regex_match("job", "api-.*"))],
                    0,
                    30,
                    10,
                )
                .expect("query should succeed");
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].metric, "metric_a");
    }

    #[test]
    fn accounted_query_reconciles_actual_capacity_and_has_exact_memory_boundary() {
        let store = one_exemplar_store();
        let selections = vec![SeriesSelection::new()
            .with_metric("latency_seconds")
            .with_matcher(SeriesMatcher::equal("job", "api"))];
        let compilation_bytes = modeled_compiled_selections_bytes(&selections);

        let calibration_budget =
            tsink::QueryBudget::new(tsink::QueryBudgetLimits::default()).expect("budget");
        let calibration_execution = calibration_budget.begin_query().expect("execution");
        let calibration = store
            .query_with_execution_result(&selections, 0, 20, 1, &calibration_execution)
            .expect("calibration query");
        let retained = modeled_owned_exemplar_series_bytes(&calibration.series);
        assert_eq!(calibration.reserved_memory_bytes(), retained);
        assert_eq!(
            calibration_execution.snapshot().memory_reserved_bytes,
            retained
        );
        drop(calibration);
        assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);

        let exact_limit = compilation_bytes.saturating_add(retained);
        let (exact_budget, exact_execution) = query_budget_with_memory_limit(exact_limit);
        let exact = store
            .query_with_execution_result(&selections, 0, 20, 1, &exact_execution)
            .expect("exact modeled memory must succeed");
        assert_eq!(exact.reserved_memory_bytes(), retained);
        drop(exact);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let (under_budget, under_execution) =
            query_budget_with_memory_limit(exact_limit.saturating_sub(1));
        let error = store
            .query_with_execution_result(&selections, 0, 20, 1, &under_execution)
            .expect_err("one byte under the modeled envelope must fail");
        assert!(matches!(
            error,
            ExemplarQueryError::Budget(QueryBudgetError::LimitExceeded(ref exceeded))
                if exceeded.reason == tsink::QueryLimitReason::PerQueryMemoryBytes
        ));
        drop(under_execution);
        assert_eq!(under_budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn accounted_query_rejects_invalid_direct_request_shapes_without_allocating() {
        let store = one_exemplar_store();
        let valid = SeriesSelection::new().with_metric("latency_seconds");
        let too_many = vec![valid.clone(), valid.clone(), valid.clone()];
        let invalid_requests = [
            store.query_with_execution_result(
                &[],
                0,
                20,
                1,
                &tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
                    .expect("budget")
                    .begin_query()
                    .expect("execution"),
            ),
            store.query_with_execution_result(
                &too_many,
                0,
                20,
                1,
                &tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
                    .expect("budget")
                    .begin_query()
                    .expect("execution"),
            ),
            store.query_with_execution_result(
                std::slice::from_ref(&valid),
                0,
                20,
                0,
                &tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
                    .expect("budget")
                    .begin_query()
                    .expect("execution"),
            ),
            store.query_with_execution_result(
                std::slice::from_ref(&valid),
                0,
                20,
                store.config().max_query_results + 1,
                &tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
                    .expect("budget")
                    .begin_query()
                    .expect("execution"),
            ),
            store.query_with_execution_result(
                std::slice::from_ref(&valid),
                20,
                10,
                1,
                &tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
                    .expect("budget")
                    .begin_query()
                    .expect("execution"),
            ),
        ];
        assert!(invalid_requests
            .into_iter()
            .all(|result| matches!(result, Err(ExemplarQueryError::InvalidSelection))));
    }

    #[test]
    fn accounted_query_checks_intermediate_limit_before_second_series_push() {
        let store = ExemplarStore::in_memory_with_config(ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 2,
        });
        store
            .apply_writes(&[
                ExemplarWrite {
                    metric: "a".to_string(),
                    series_labels: Vec::new(),
                    exemplar_labels: Vec::new(),
                    timestamp: 10,
                    value: 1.0,
                },
                ExemplarWrite {
                    metric: "b".to_string(),
                    series_labels: Vec::new(),
                    exemplar_labels: Vec::new(),
                    timestamp: 10,
                    value: 2.0,
                },
            ])
            .expect("seed exemplars");
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            per_query: tsink::QueryWorkLimits {
                max_intermediate_vector_size: Some(1),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("budget");
        let execution = budget.begin_query().expect("execution");
        let error = store
            .query_with_execution_result(
                &[SeriesSelection::new()
                    .with_matcher(SeriesMatcher::regex_match("__name__", ".*"))],
                0,
                20,
                2,
                &execution,
            )
            .expect_err("second series must be rejected before push");
        assert!(matches!(
            error,
            ExemplarQueryError::Budget(QueryBudgetError::LimitExceeded(ref exceeded))
                if exceeded.reason == tsink::QueryLimitReason::IntermediateVectorSize
                    && exceeded.current == 1
                    && exceeded.requested == 2
        ));
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    }

    #[test]
    fn exemplar_store_persistence_failure_does_not_publish_writes_or_counters() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = ExemplarStore::open_with_config(
            Some(temp_dir.path()),
            ExemplarStoreConfig {
                max_total_exemplars: 8,
                max_exemplars_per_series: 1,
                max_exemplars_per_request: 8,
                max_query_results: 8,
                max_query_selectors: 4,
            },
        )
        .expect("store should open");
        let selection = SeriesSelection::new()
            .with_metric("metric_a")
            .with_matcher(SeriesMatcher::equal("job", "api"));
        store
            .apply_writes(&[ExemplarWrite {
                metric: "metric_a".to_string(),
                series_labels: vec![Label::new("job", "api")],
                exemplar_labels: vec![Label::new("trace_id", "persisted")],
                timestamp: 10,
                value: 1.0,
            }])
            .expect("initial exemplar should persist");
        let metrics_before = store.metrics_snapshot().expect("metrics should load");

        let store_path = store
            .path
            .as_deref()
            .expect("persistent store should expose file path");
        std::fs::remove_file(store_path).expect("persisted store should be removable");
        std::fs::create_dir(store_path).expect("blocking publication path should build");

        let error = store
            .apply_writes(&[ExemplarWrite {
                metric: "metric_a".to_string(),
                series_labels: vec![Label::new("job", "api")],
                exemplar_labels: vec![Label::new("trace_id", "unpersisted")],
                timestamp: 20,
                value: 2.0,
            }])
            .expect_err("publication-path collision should fail persistence");
        assert!(
            matches!(error.as_tsink_error(), tsink::TsinkError::Io(_)),
            "{error}"
        );
        let metrics_after_failure = store.metrics_snapshot().expect("metrics should load");
        assert_eq!(
            (
                metrics_after_failure.accepted_total,
                metrics_after_failure.rejected_total,
                metrics_after_failure.dropped_total,
                metrics_after_failure.stored_series,
                metrics_after_failure.stored_exemplars,
                metrics_after_failure.retained_bytes,
                metrics_after_failure.durable_file_bytes,
                metrics_after_failure.transient_bytes,
            ),
            (
                metrics_before.accepted_total,
                metrics_before.rejected_total,
                metrics_before.dropped_total,
                metrics_before.stored_series,
                metrics_before.stored_exemplars,
                metrics_before.retained_bytes,
                metrics_before.durable_file_bytes,
                0,
            )
        );
        assert!(metrics_after_failure.peak_transient_bytes >= metrics_before.peak_transient_bytes);

        let queried = store
            .query(std::slice::from_ref(&selection), 0, 30, 8)
            .expect("query should succeed after failed persistence");
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].exemplars.len(), 1);
        assert_eq!(queried[0].exemplars[0].timestamp, 10);
        assert_eq!(queried[0].exemplars[0].labels[0].value, "persisted");

        assert!(
            std::fs::read_dir(temp_dir.path())
                .expect("exemplar directory should remain readable")
                .all(|entry| !entry
                    .expect("exemplar directory entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".exemplar-store.json.tmp-")),
            "a failed publication must clean its unique temporary file"
        );
    }

    #[test]
    fn exemplar_store_disk_quota_rejection_does_not_publish_writes_or_counters() {
        let temp_dir = TempDir::new().expect("tempdir");
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(64),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store =
            ExemplarStore::open_with_disk_budget(Some(temp_dir.path()), Some(Arc::clone(&budget)))
                .expect("store should open");
        let metrics_before = store.metrics_snapshot().expect("metrics should load");
        let selection = SeriesSelection::new()
            .with_metric("metric_a")
            .with_matcher(SeriesMatcher::equal("job", "api"));

        let error = store
            .apply_writes(&[ExemplarWrite {
                metric: "metric_a".to_string(),
                series_labels: vec![Label::new("job", "api")],
                exemplar_labels: vec![Label::new(
                    "trace_id",
                    "a-deliberately-long-value-that-exceeds-the-tiny-quota",
                )],
                timestamp: 10,
                value: 1.0,
            }])
            .expect_err("tiny disk quota should reject exemplar persistence");
        assert!(matches!(
            error.as_tsink_error(),
            tsink::TsinkError::DiskQuotaExceeded { .. }
        ));
        let metrics_after_failure = store.metrics_snapshot().expect("metrics should load");
        assert_eq!(
            (
                metrics_after_failure.accepted_total,
                metrics_after_failure.rejected_total,
                metrics_after_failure.dropped_total,
                metrics_after_failure.stored_series,
                metrics_after_failure.stored_exemplars,
                metrics_after_failure.retained_bytes,
                metrics_after_failure.durable_file_bytes,
                metrics_after_failure.transient_bytes,
            ),
            (
                metrics_before.accepted_total,
                metrics_before.rejected_total,
                metrics_before.dropped_total,
                metrics_before.stored_series,
                metrics_before.stored_exemplars,
                metrics_before.retained_bytes,
                metrics_before.durable_file_bytes,
                0,
            ),
            "a rejected write must not publish counters or state"
        );
        assert!(metrics_after_failure.peak_transient_bytes >= metrics_before.peak_transient_bytes);
        assert!(
            store
                .query(&[selection], 0, 20, 10)
                .expect("query should succeed")
                .is_empty(),
            "a rejected write must not become visible"
        );
        assert!(
            !store
                .path
                .as_deref()
                .expect("persistent store should expose file path")
                .exists(),
            "a rejected write must not publish a store file"
        );

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn exemplar_resource_limits_are_finite_inspectable_and_validated() {
        let defaults = ExemplarStoreResourceLimits::default();
        assert!(defaults.max_total_series < usize::MAX);
        assert!(defaults.max_total_retained_bytes < u64::MAX);
        assert!(defaults.max_update_batch_bytes < u64::MAX);
        assert!(defaults.max_write_transient_bytes < u64::MAX);
        assert!(defaults.max_replacement_peak_bytes < u64::MAX);
        assert!(defaults.max_persistence_serialization_bytes < u64::MAX);
        assert!(defaults.max_durable_file_bytes < u64::MAX);
        assert!(defaults.max_startup_transient_bytes < u64::MAX);
        assert!(defaults.max_snapshot_bytes < u64::MAX);
        assert!(defaults.max_snapshot_transient_bytes < u64::MAX);
        assert!(defaults.max_concurrent_transient_bytes < u64::MAX);

        let store = in_memory_resource_store(defaults);
        assert_eq!(store.resource_limits(), defaults);

        let invalid_limits = [
            ExemplarStoreResourceLimits {
                max_total_series: 0,
                ..defaults
            },
            ExemplarStoreResourceLimits {
                max_total_series: usize::MAX,
                ..defaults
            },
            ExemplarStoreResourceLimits {
                max_total_retained_bytes: u64::MAX,
                ..defaults
            },
            ExemplarStoreResourceLimits {
                max_metric_name_bytes: tsink::label::MAX_METRIC_NAME_LEN.saturating_add(1),
                ..defaults
            },
            ExemplarStoreResourceLimits {
                max_series_identity_bytes: defaults.max_metric_name_bytes.saturating_sub(1),
                ..defaults
            },
            ExemplarStoreResourceLimits {
                max_replacement_peak_bytes: defaults.max_total_retained_bytes.saturating_sub(1),
                ..defaults
            },
            ExemplarStoreResourceLimits {
                max_concurrent_transient_bytes: defaults
                    .max_write_transient_bytes
                    .saturating_sub(1),
                ..defaults
            },
        ];
        for limits in invalid_limits {
            let error = ExemplarStore::in_memory_with_config_and_resource_limits(
                resource_test_config(),
                limits,
            )
            .expect_err("invalid resource-limit combinations must fail at construction");
            assert_eq!(
                error.code(),
                Some(ExemplarStoreErrorCode::InvalidConfiguration)
            );
            assert!(matches!(
                error.as_tsink_error(),
                tsink::TsinkError::InvalidConfiguration(_)
            ));
        }

        let mut unlimited_legacy = resource_test_config();
        unlimited_legacy.max_total_exemplars = usize::MAX;
        let error =
            ExemplarStore::in_memory_with_config_and_resource_limits(unlimited_legacy, defaults)
                .expect_err("legacy entry limits must not use an accidental unlimited sentinel");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::InvalidConfiguration)
        );

        ExemplarStoreResourceLimits {
            max_persistence_serialization_bytes: 1,
            ..defaults
        }
        .validate()
        .expect("serialization and durable limits are independent finite envelopes");
    }

    #[test]
    fn exemplar_shapes_and_cumulative_identity_have_exact_boundaries() {
        let limits = ExemplarStoreResourceLimits {
            max_metric_name_bytes: 4,
            max_labels_per_series: 1,
            max_labels_per_exemplar: 1,
            max_label_name_bytes: 8,
            max_label_value_bytes: 8,
            max_series_identity_bytes: 11,
            max_exemplar_label_bytes: 12,
            ..ExemplarStoreResourceLimits::default()
        };
        let store = in_memory_resource_store(limits);
        let exact = resource_test_write("metr", "api1", "abcd", 1);
        store
            .apply_writes(std::slice::from_ref(&exact))
            .expect("exact shape and cumulative-byte limits must succeed");

        let mut metric_too_long = exact.clone();
        metric_too_long.metric.push('x');
        let error = store
            .apply_writes(&[metric_too_long])
            .expect_err("metric N+1 must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::InvalidMetricShape)
        );

        let mut too_many_series_labels = exact.clone();
        too_many_series_labels
            .series_labels
            .push(Label::new("env", "x"));
        let error = store
            .apply_writes(&[too_many_series_labels])
            .expect_err("series-label count N+1 must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::InvalidSeriesLabelShape)
        );

        let mut series_identity_too_large = exact.clone();
        series_identity_too_large.series_labels[0].value.push('x');
        let error = store
            .apply_writes(&[series_identity_too_large])
            .expect_err("series identity N+1 must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::InvalidSeriesLabelShape)
        );

        let mut exemplar_labels_too_large = exact;
        exemplar_labels_too_large.exemplar_labels[0].value.push('x');
        let error = store
            .apply_writes(&[exemplar_labels_too_large])
            .expect_err("exemplar cumulative label bytes N+1 must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::InvalidExemplarLabelShape)
        );

        let mut non_finite = resource_test_write("metr", "api1", "abcd", 2);
        non_finite.value = f64::NAN;
        let error = store
            .apply_writes(&[non_finite])
            .expect_err("non-finite values must fail before in-memory or durable publication");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::InvalidExemplarValue)
        );

        let metrics = store.metrics_snapshot().expect("metrics should load");
        assert_eq!(metrics.accepted_total, 1);
        assert_eq!(metrics.rejected_total, 5);
        assert_eq!(metrics.stored_series, 1);
        assert_eq!(metrics.stored_exemplars, 1);
        assert_eq!(metrics.shape_rejections_total, 5);
        assert_eq!(
            metrics.last_rejection_code,
            Some(ExemplarStoreErrorCode::InvalidExemplarValue)
        );
        assert_eq!(metrics.transient_bytes, 0);
    }

    #[test]
    fn exemplar_update_entry_and_actual_capacity_byte_limits_are_exact() {
        let mut one_entry_config = resource_test_config();
        one_entry_config.max_exemplars_per_request = 1;
        let entry_store = ExemplarStore::in_memory_with_config_and_resource_limits(
            one_entry_config,
            ExemplarStoreResourceLimits::default(),
        )
        .expect("store");
        let first = resource_test_write("metric", "a", "one", 1);
        entry_store
            .apply_writes(std::slice::from_ref(&first))
            .expect("exact entry count must succeed");
        let error = entry_store
            .apply_writes(&[
                resource_test_write("metric", "a", "two", 2),
                resource_test_write("metric", "a", "three", 3),
            ])
            .expect_err("entry count N+1 must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::UpdateBatchEntries)
        );
        let metrics = entry_store.metrics_snapshot().expect("metrics");
        assert_eq!(metrics.accepted_total, 1);
        assert_eq!(metrics.rejected_total, 2);
        assert_eq!(metrics.stored_exemplars, 1);
        assert_eq!(metrics.batch_rejections_total, 1);
        assert_eq!(metrics.transient_bytes, 0);

        let measured = validate_and_model_exemplar_batch(
            std::slice::from_ref(&first),
            &ExemplarStoreResourceLimits::default(),
        )
        .expect("valid calibration write");
        let exact_limits = ExemplarStoreResourceLimits {
            max_update_batch_bytes: measured,
            ..ExemplarStoreResourceLimits::default()
        };
        in_memory_resource_store(exact_limits)
            .apply_writes(std::slice::from_ref(&first))
            .expect("exact actual-capacity batch bytes must succeed");

        let under_limits = ExemplarStoreResourceLimits {
            max_update_batch_bytes: measured.saturating_sub(1),
            ..ExemplarStoreResourceLimits::default()
        };
        let under = in_memory_resource_store(under_limits);
        let error = under
            .apply_writes(&[first])
            .expect_err("one byte below actual-capacity batch bytes must fail");
        assert_store_rejection(&under, &error, ExemplarStoreErrorCode::UpdateBatchBytes, 1);
        assert_eq!(
            under.metrics_snapshot().expect("metrics").stored_exemplars,
            0
        );

        let one_series = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_total_series: 1,
            ..ExemplarStoreResourceLimits::default()
        });
        one_series
            .apply_writes(&[resource_test_write("metric", "a", "one", 1)])
            .expect("exact total-series limit must succeed");
        one_series
            .apply_writes(&[resource_test_write("metric", "a", "two", 2)])
            .expect("another exemplar in the admitted series must succeed");
        let error = one_series
            .apply_writes(&[resource_test_write("metric", "b", "three", 3)])
            .expect_err("total-series N+1 must fail before allocation");
        assert_eq!(error.code(), Some(ExemplarStoreErrorCode::TotalSeries));
        let metrics = one_series.metrics_snapshot().expect("metrics");
        assert_eq!(metrics.accepted_total, 2);
        assert_eq!(metrics.rejected_total, 1);
        assert_eq!(metrics.stored_series, 1);
        assert_eq!(metrics.stored_exemplars, 2);
        assert_eq!(metrics.retained_rejections_total, 1);
        assert_eq!(metrics.transient_bytes, 0);
    }

    #[test]
    fn exemplar_retained_bytes_and_same_timestamp_replacement_are_atomic_and_exact() {
        let write = resource_test_write("metric", "api", "trace", 1);
        let calibration = in_memory_resource_store(ExemplarStoreResourceLimits::default());
        calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("calibration write");
        let retained = calibration
            .metrics_snapshot()
            .expect("metrics")
            .retained_bytes;
        assert!(retained > 1);

        let exact = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_total_retained_bytes: retained,
            ..ExemplarStoreResourceLimits::default()
        });
        exact
            .apply_writes(std::slice::from_ref(&write))
            .expect("exact retained-byte limit must succeed");
        let exact_metrics = exact.metrics_snapshot().expect("metrics");
        assert_eq!(exact_metrics.retained_bytes, retained);
        assert_eq!(exact_metrics.peak_retained_bytes, retained);

        let under = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_total_retained_bytes: retained.saturating_sub(1),
            ..ExemplarStoreResourceLimits::default()
        });
        let error = under
            .apply_writes(std::slice::from_ref(&write))
            .expect_err("one byte below retained size must fail");
        assert_store_rejection(&under, &error, ExemplarStoreErrorCode::RetainedBytes, 1);
        let under_metrics = under.metrics_snapshot().expect("metrics");
        assert_eq!(under_metrics.retained_bytes, 0);
        assert_eq!(under_metrics.peak_retained_bytes, 0);
        assert_eq!(under_metrics.stored_exemplars, 0);

        let replacement =
            resource_test_write("metric", "api", &"replacement".repeat(64), write.timestamp);
        let replacement_calibration =
            in_memory_resource_store(ExemplarStoreResourceLimits::default());
        replacement_calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        replacement_calibration
            .apply_writes(std::slice::from_ref(&replacement))
            .expect("replacement calibration");
        let replacement_retained = replacement_calibration
            .metrics_snapshot()
            .expect("metrics")
            .retained_bytes;
        assert!(replacement_retained > retained);

        let exact_replacement = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_total_retained_bytes: replacement_retained,
            ..ExemplarStoreResourceLimits::default()
        });
        exact_replacement
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        exact_replacement
            .apply_writes(std::slice::from_ref(&replacement))
            .expect("exact final replacement size must succeed");
        let metrics = exact_replacement.metrics_snapshot().expect("metrics");
        assert_eq!(metrics.stored_exemplars, 1);
        assert_eq!(metrics.retained_bytes, replacement_retained);

        let failed_replacement = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_total_retained_bytes: replacement_retained.saturating_sub(1),
            ..ExemplarStoreResourceLimits::default()
        });
        failed_replacement
            .apply_writes(std::slice::from_ref(&write))
            .expect("the old sample fits");
        let before = failed_replacement.metrics_snapshot().expect("metrics");
        let error = failed_replacement
            .apply_writes(&[replacement])
            .expect_err("oversized same-timestamp replacement must fail atomically");
        assert_eq!(error.code(), Some(ExemplarStoreErrorCode::RetainedBytes));
        let after = failed_replacement.metrics_snapshot().expect("metrics");
        assert_eq!(after.accepted_total, before.accepted_total);
        assert_eq!(after.stored_exemplars, 1);
        assert_eq!(after.retained_bytes, before.retained_bytes);
        assert_eq!(after.transient_bytes, 0);
        let queried = failed_replacement
            .query(
                &[SeriesSelection::new()
                    .with_metric("metric")
                    .with_matcher(SeriesMatcher::equal("job", "api"))],
                0,
                2,
                2,
            )
            .expect("query");
        assert_eq!(queried[0].exemplars[0].labels[0].value, "trace");
    }

    #[test]
    fn exemplar_write_transient_and_replacement_peak_limits_are_exact() {
        let write = resource_test_write("metric", "api", &"trace".repeat(32), 1);
        let calibration = in_memory_resource_store(ExemplarStoreResourceLimits::default());
        calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("calibration write");
        let transient_peak = calibration
            .metrics_snapshot()
            .expect("metrics")
            .peak_transient_bytes;
        assert!(transient_peak > 1);

        in_memory_resource_store(ExemplarStoreResourceLimits {
            max_write_transient_bytes: transient_peak,
            ..ExemplarStoreResourceLimits::default()
        })
        .apply_writes(std::slice::from_ref(&write))
        .expect("exact write-transient limit must succeed");

        let transient_under = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_write_transient_bytes: transient_peak.saturating_sub(1),
            ..ExemplarStoreResourceLimits::default()
        });
        let error = transient_under
            .apply_writes(std::slice::from_ref(&write))
            .expect_err("one byte below write-transient peak must fail");
        assert_store_rejection(
            &transient_under,
            &error,
            ExemplarStoreErrorCode::WriteTransientBytes,
            1,
        );
        assert_eq!(
            transient_under
                .metrics_snapshot()
                .expect("metrics")
                .stored_exemplars,
            0
        );

        let replacement = resource_test_write("metric", "api", &"larger-replacement".repeat(64), 1);
        let peak_calibration = in_memory_resource_store(ExemplarStoreResourceLimits::default());
        peak_calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        let old_retained = peak_calibration
            .metrics_snapshot()
            .expect("metrics")
            .retained_bytes;
        peak_calibration
            .apply_writes(std::slice::from_ref(&replacement))
            .expect("replacement calibration");
        let calibrated_metrics = peak_calibration.metrics_snapshot().expect("metrics");
        let replacement_peak = old_retained.saturating_add(calibrated_metrics.peak_transient_bytes);
        let final_retained = calibrated_metrics.retained_bytes;
        assert!(replacement_peak > final_retained);

        let replacement_limits = ExemplarStoreResourceLimits {
            max_total_retained_bytes: final_retained,
            max_write_transient_bytes: calibrated_metrics.peak_transient_bytes,
            max_replacement_peak_bytes: replacement_peak,
            ..ExemplarStoreResourceLimits::default()
        };
        let exact_replacement = in_memory_resource_store(replacement_limits);
        exact_replacement
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        exact_replacement
            .apply_writes(std::slice::from_ref(&replacement))
            .expect("exact replacement-peak limit must succeed");

        let under_replacement = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_replacement_peak_bytes: replacement_peak.saturating_sub(1),
            ..replacement_limits
        });
        under_replacement
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed must fit under the replacement limit");
        let error = under_replacement
            .apply_writes(&[replacement])
            .expect_err("one byte below replacement peak must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::ReplacementPeakBytes)
        );
        let metrics = under_replacement.metrics_snapshot().expect("metrics");
        assert_eq!(metrics.accepted_total, 1);
        assert_eq!(metrics.stored_exemplars, 1);
        assert_eq!(metrics.retained_bytes, old_retained);
        assert_eq!(metrics.transient_bytes, 0);
        assert_eq!(metrics.transient_rejections_total, 1);
    }

    #[test]
    fn exemplar_durable_and_serialization_file_limits_are_exact() {
        let write = resource_test_write("metric", "api", &"trace".repeat(32), 1);
        let calibration_dir = TempDir::new().expect("tempdir");
        let calibration = persistent_resource_store(
            calibration_dir.path(),
            ExemplarStoreResourceLimits::default(),
        )
        .expect("store");
        calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("calibration write");
        let file_bytes = calibration
            .metrics_snapshot()
            .expect("metrics")
            .durable_file_bytes;
        assert_eq!(
            file_bytes,
            std::fs::metadata(calibration_dir.path().join(EXEMPLAR_STORE_FILE_NAME))
                .expect("file metadata")
                .len()
        );

        let exact_dir = TempDir::new().expect("tempdir");
        persistent_resource_store(
            exact_dir.path(),
            ExemplarStoreResourceLimits {
                max_durable_file_bytes: file_bytes,
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect("store")
        .apply_writes(std::slice::from_ref(&write))
        .expect("exact durable-file bytes must succeed");

        let durable_under_dir = TempDir::new().expect("tempdir");
        let durable_under = persistent_resource_store(
            durable_under_dir.path(),
            ExemplarStoreResourceLimits {
                max_durable_file_bytes: file_bytes.saturating_sub(1),
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect("store");
        let error = durable_under
            .apply_writes(std::slice::from_ref(&write))
            .expect_err("one byte below durable file size must fail");
        assert_store_rejection(
            &durable_under,
            &error,
            ExemplarStoreErrorCode::DurableFileBytes,
            1,
        );
        assert!(!durable_under_dir
            .path()
            .join(EXEMPLAR_STORE_FILE_NAME)
            .exists());

        let serialization_exact_dir = TempDir::new().expect("tempdir");
        persistent_resource_store(
            serialization_exact_dir.path(),
            ExemplarStoreResourceLimits {
                max_persistence_serialization_bytes: file_bytes,
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect("store")
        .apply_writes(std::slice::from_ref(&write))
        .expect("exact serialization bytes must succeed");

        let serialization_under_dir = TempDir::new().expect("tempdir");
        let serialization_under = persistent_resource_store(
            serialization_under_dir.path(),
            ExemplarStoreResourceLimits {
                max_persistence_serialization_bytes: file_bytes.saturating_sub(1),
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect("store");
        let error = serialization_under
            .apply_writes(&[write])
            .expect_err("one byte below serialization size must fail");
        assert_store_rejection(
            &serialization_under,
            &error,
            ExemplarStoreErrorCode::PersistenceSerializationBytes,
            1,
        );
        assert!(!serialization_under_dir
            .path()
            .join(EXEMPLAR_STORE_FILE_NAME)
            .exists());
    }

    #[test]
    fn exemplar_budgeted_persistence_reconciles_actual_buffer_capacity_before_publication() {
        let write = resource_test_write("metric", "api", &"trace".repeat(32), 1);
        let open_budgeted = |path: &Path,
                             limits: ExemplarStoreResourceLimits|
         -> (ExemplarStore, Arc<tsink::LocalDiskBudget>) {
            let budget = tsink::LocalDiskBudget::open(
                path,
                tsink::LocalDiskLimits {
                    max_bytes: Some(1024 * 1024),
                    ..tsink::LocalDiskLimits::default()
                },
            )
            .expect("disk budget");
            let store = ExemplarStore::open_with_config_and_resource_limits_and_disk_budget(
                Some(path),
                resource_test_config(),
                limits,
                Some(Arc::clone(&budget)),
            )
            .expect("budgeted exemplar store");
            (store, budget)
        };

        let calibration_dir = TempDir::new().expect("tempdir");
        let (calibration, calibration_budget) = open_budgeted(
            calibration_dir.path(),
            ExemplarStoreResourceLimits::default(),
        );
        calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("calibration write");
        let calibration_metrics = calibration.metrics_snapshot().expect("metrics");
        let transient_peak = calibration_metrics.peak_transient_bytes;
        assert!(transient_peak > 1);
        assert_eq!(
            calibration_budget.snapshot().accounted_bytes,
            calibration_metrics.durable_file_bytes
        );

        let exact_dir = TempDir::new().expect("tempdir");
        let (exact, exact_budget) = open_budgeted(
            exact_dir.path(),
            ExemplarStoreResourceLimits {
                max_write_transient_bytes: transient_peak,
                ..ExemplarStoreResourceLimits::default()
            },
        );
        exact
            .apply_writes(std::slice::from_ref(&write))
            .expect("exact actual encoded-buffer capacity must succeed");
        assert_eq!(
            exact.metrics_snapshot().expect("metrics").transient_bytes,
            0
        );
        assert_eq!(exact_budget.snapshot().active_reservations, 0);

        let under_dir = TempDir::new().expect("tempdir");
        let (under, under_budget) = open_budgeted(
            under_dir.path(),
            ExemplarStoreResourceLimits {
                max_write_transient_bytes: transient_peak.saturating_sub(1),
                ..ExemplarStoreResourceLimits::default()
            },
        );
        let error = under
            .apply_writes(&[write])
            .expect_err("one byte below actual encoded-buffer peak must fail");
        assert_store_rejection(
            &under,
            &error,
            ExemplarStoreErrorCode::WriteTransientBytes,
            1,
        );
        assert!(
            !under_dir.path().join(EXEMPLAR_STORE_FILE_NAME).exists(),
            "capacity reconciliation must happen before durable publication"
        );
        let budget = under_budget.snapshot();
        assert_eq!(budget.accounted_bytes, 0);
        assert_eq!(budget.reserved_bytes, 0);
        assert_eq!(budget.active_reservations, 0);
    }

    #[test]
    fn exemplar_startup_file_transient_and_entry_envelopes_are_exact() {
        let data_dir = TempDir::new().expect("tempdir");
        let write = resource_test_write("metric", "api", &"trace".repeat(16), 1);
        let seeded =
            persistent_resource_store(data_dir.path(), ExemplarStoreResourceLimits::default())
                .expect("store");
        seeded.apply_writes(&[write]).expect("persist seed");
        let retained_bytes = seeded.metrics_snapshot().expect("metrics").retained_bytes;
        drop(seeded);
        let store_file = data_dir.path().join(EXEMPLAR_STORE_FILE_NAME);
        let file_bytes = std::fs::metadata(&store_file).expect("metadata").len();

        let calibration =
            persistent_resource_store(data_dir.path(), ExemplarStoreResourceLimits::default())
                .expect("calibration reopen");
        let startup_peak = calibration
            .metrics_snapshot()
            .expect("metrics")
            .peak_transient_bytes;
        assert!(startup_peak > 1);

        persistent_resource_store(
            data_dir.path(),
            ExemplarStoreResourceLimits {
                max_durable_file_bytes: file_bytes,
                max_total_retained_bytes: retained_bytes,
                max_startup_transient_bytes: startup_peak,
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect("exact startup file and transient limits must succeed");

        let error = persistent_resource_store(
            data_dir.path(),
            ExemplarStoreResourceLimits {
                max_total_retained_bytes: retained_bytes.saturating_sub(1),
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect_err("one byte below normalized startup retained bytes must fail");
        assert_eq!(error.code(), Some(ExemplarStoreErrorCode::RetainedBytes));

        let error = persistent_resource_store(
            data_dir.path(),
            ExemplarStoreResourceLimits {
                max_startup_transient_bytes: startup_peak.saturating_sub(1),
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect_err("one byte below startup peak must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::StartupTransientBytes)
        );

        let error = persistent_resource_store(
            data_dir.path(),
            ExemplarStoreResourceLimits {
                max_durable_file_bytes: file_bytes.saturating_sub(1),
                ..ExemplarStoreResourceLimits::default()
            },
        )
        .expect_err("an N+1 durable file must fail before decode");
        assert_eq!(error.code(), Some(ExemplarStoreErrorCode::StartupFileBytes));

        let oversized_entries_dir = TempDir::new().expect("tempdir");
        let store = persistent_resource_store(
            oversized_entries_dir.path(),
            ExemplarStoreResourceLimits::default(),
        )
        .expect("store");
        store
            .apply_writes(&[
                resource_test_write("metric", "api", "one", 1),
                resource_test_write("metric", "api", "two", 2),
            ])
            .expect("persist two entries");
        let mut one_total = resource_test_config();
        one_total.max_total_exemplars = 1;
        let error = ExemplarStore::open_with_config_and_resource_limits_and_disk_budget(
            Some(oversized_entries_dir.path()),
            one_total,
            ExemplarStoreResourceLimits::default(),
            None,
        )
        .expect_err("startup entry count N+1 must fail");
        assert_eq!(error.code(), Some(ExemplarStoreErrorCode::StartupEntries));

        let mut one_per_series = resource_test_config();
        one_per_series.max_exemplars_per_series = 1;
        let error = ExemplarStore::open_with_config_and_resource_limits_and_disk_budget(
            Some(oversized_entries_dir.path()),
            one_per_series,
            ExemplarStoreResourceLimits::default(),
            None,
        )
        .expect_err("startup per-series entry count N+1 must fail");
        assert_eq!(error.code(), Some(ExemplarStoreErrorCode::StartupEntries));
    }

    #[test]
    fn exemplar_default_startup_envelope_admits_the_maximum_durable_file_exactly() {
        let defaults = ExemplarStoreResourceLimits::default();
        let data_dir = TempDir::new().expect("tempdir");
        let seeded = persistent_resource_store(data_dir.path(), defaults).expect("store");
        seeded
            .apply_writes(&[resource_test_write("metric", "api", "trace", 1)])
            .expect("seed");
        drop(seeded);

        let store_file = data_dir.path().join(EXEMPLAR_STORE_FILE_NAME);
        let current_bytes = std::fs::metadata(&store_file).expect("metadata").len();
        assert!(current_bytes < defaults.max_durable_file_bytes);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&store_file)
            .expect("open exemplar file for valid JSON whitespace padding");
        let padding = vec![b' '; 1024 * 1024];
        let mut remaining = defaults.max_durable_file_bytes - current_bytes;
        while remaining != 0 {
            let chunk = usize::try_from(remaining.min(padding.len() as u64))
                .expect("padding chunk fits usize");
            file.write_all(&padding[..chunk]).expect("append padding");
            remaining -= chunk as u64;
        }
        file.sync_all().expect("sync padded exemplar file");
        drop(file);
        assert_eq!(
            std::fs::metadata(&store_file).expect("metadata").len(),
            defaults.max_durable_file_bytes
        );

        let exact_startup_peak =
            modeled_startup_predecode_peak(defaults.max_durable_file_bytes, &defaults);
        assert!(
            exact_startup_peak <= defaults.max_startup_transient_bytes,
            "the default startup envelope must cover the maximum admissible durable file"
        );
        let reopened = persistent_resource_store(
            data_dir.path(),
            ExemplarStoreResourceLimits {
                max_startup_transient_bytes: exact_startup_peak,
                ..defaults
            },
        )
        .expect("the exact maximum-file startup peak must reopen");
        let metrics = reopened.metrics_snapshot().expect("metrics");
        assert_eq!(metrics.durable_file_bytes, defaults.max_durable_file_bytes);
        assert_eq!(metrics.stored_exemplars, 1);
        assert_eq!(metrics.peak_transient_bytes, exact_startup_peak);
        drop(reopened);

        let error = persistent_resource_store(
            data_dir.path(),
            ExemplarStoreResourceLimits {
                max_startup_transient_bytes: exact_startup_peak.saturating_sub(1),
                ..defaults
            },
        )
        .expect_err("one byte below the maximum-file startup formula must fail");
        assert_eq!(
            error.code(),
            Some(ExemplarStoreErrorCode::StartupTransientBytes)
        );
    }

    #[test]
    fn exemplar_snapshot_size_and_transient_envelopes_are_exact_and_atomic() {
        let write = resource_test_write("metric", "api", &"trace".repeat(16), 1);
        let calibration = in_memory_resource_store(ExemplarStoreResourceLimits::default());
        calibration
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        let calibration_snapshot = TempDir::new().expect("tempdir");
        calibration
            .snapshot_into(calibration_snapshot.path())
            .expect("snapshot");
        let snapshot_bytes =
            std::fs::metadata(calibration_snapshot.path().join(EXEMPLAR_STORE_FILE_NAME))
                .expect("snapshot metadata")
                .len();

        let exact = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_snapshot_bytes: snapshot_bytes,
            max_snapshot_transient_bytes: EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES,
            ..ExemplarStoreResourceLimits::default()
        });
        exact
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        let exact_dir = TempDir::new().expect("tempdir");
        exact
            .snapshot_into(exact_dir.path())
            .expect("exact snapshot size and transient limit must succeed");
        assert_eq!(
            std::fs::metadata(exact_dir.path().join(EXEMPLAR_STORE_FILE_NAME))
                .expect("snapshot metadata")
                .len(),
            snapshot_bytes
        );
        assert_eq!(
            exact.metrics_snapshot().expect("metrics").transient_bytes,
            0
        );

        let size_under = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_snapshot_bytes: snapshot_bytes.saturating_sub(1),
            ..ExemplarStoreResourceLimits::default()
        });
        size_under
            .apply_writes(std::slice::from_ref(&write))
            .expect("seed");
        let size_under_dir = TempDir::new().expect("tempdir");
        let error = size_under
            .snapshot_into(size_under_dir.path())
            .expect_err("one byte below snapshot size must fail");
        assert_store_rejection(
            &size_under,
            &error,
            ExemplarStoreErrorCode::SnapshotBytes,
            1,
        );
        assert!(!size_under_dir
            .path()
            .join(EXEMPLAR_STORE_FILE_NAME)
            .exists());

        let transient_under = in_memory_resource_store(ExemplarStoreResourceLimits {
            max_snapshot_transient_bytes: EXEMPLAR_STORE_SERIALIZATION_SCRATCH_BYTES - 1,
            ..ExemplarStoreResourceLimits::default()
        });
        transient_under.apply_writes(&[write]).expect("seed");
        let transient_under_dir = TempDir::new().expect("tempdir");
        let error = transient_under
            .snapshot_into(transient_under_dir.path())
            .expect_err("one byte below snapshot scratch must fail");
        assert_store_rejection(
            &transient_under,
            &error,
            ExemplarStoreErrorCode::SnapshotTransientBytes,
            1,
        );
        assert!(!transient_under_dir
            .path()
            .join(EXEMPLAR_STORE_FILE_NAME)
            .exists());
    }

    #[test]
    fn concurrent_exemplar_writes_leave_exact_durable_accounting_and_no_transient_residue() {
        let data_dir = TempDir::new().expect("tempdir");
        let store = Arc::new(
            persistent_resource_store(data_dir.path(), ExemplarStoreResourceLimits::default())
                .expect("store"),
        );
        let mut writers = Vec::new();
        for index in 0..8 {
            let store = Arc::clone(&store);
            writers.push(std::thread::spawn(move || {
                store.apply_writes(&[resource_test_write(
                    "metric",
                    &format!("worker-{index}"),
                    &format!("trace-{index}"),
                    index,
                )])
            }));
        }
        for writer in writers {
            writer
                .join()
                .expect("writer must not panic")
                .expect("writer must succeed");
        }

        let metrics = store.metrics_snapshot().expect("metrics");
        let actual_file_bytes = std::fs::metadata(data_dir.path().join(EXEMPLAR_STORE_FILE_NAME))
            .expect("file metadata")
            .len();
        assert_eq!(metrics.accepted_total, 8);
        assert_eq!(metrics.stored_series, 8);
        assert_eq!(metrics.stored_exemplars, 8);
        assert_eq!(metrics.durable_file_bytes, actual_file_bytes);
        assert_eq!(metrics.transient_bytes, 0);

        let reopened =
            persistent_resource_store(data_dir.path(), ExemplarStoreResourceLimits::default())
                .expect("reopen");
        let reopened_metrics = reopened.metrics_snapshot().expect("metrics");
        assert_eq!(reopened_metrics.stored_series, 8);
        assert_eq!(reopened_metrics.stored_exemplars, 8);
        assert_eq!(reopened_metrics.retained_bytes, metrics.retained_bytes);
        assert_eq!(reopened_metrics.durable_file_bytes, actual_file_bytes);
        assert_eq!(reopened_metrics.transient_bytes, 0);
    }
}
