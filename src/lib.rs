//! Public tsink data model and entrypoints.
//!
//! The stable API is the set of types and builders re-exported from this crate
//! root, plus the documented `label`, `promql`, `storage`, `value`, and `wal`
//! modules. Internal engine modules are hidden from generated documentation and
//! are not part of the 1.0 compatibility contract.

pub mod r#async;
#[allow(dead_code)]
pub(crate) mod cgroup;
#[allow(dead_code)]
pub(crate) mod concurrency;
pub mod disk_budget;
#[doc(hidden)]
pub mod engine;
pub mod error;
pub mod inspection;
pub mod label;
#[allow(dead_code)]
pub(crate) mod mmap;
pub mod promql;
pub(crate) mod query_aggregation;
pub mod query_budget;
pub(crate) mod query_matcher;
pub(crate) mod query_selection;
pub mod storage;
pub(crate) mod validation;
pub mod value;
pub mod wal;

pub use disk_budget::{
    with_staged_file_replacements, DiskCategory, DiskCategoryUsage, LocalDiskBudget,
    LocalDiskBudgetSnapshot, LocalDiskLimits, LocalDiskMetricsSnapshot, ManagedFileReplacement,
    StagedManagedFileReplacements,
};
pub(crate) use disk_budget::{DiskReservation, DiskReservationKind};
#[doc(hidden)]
pub use engine::engine::modeled_query_rows_retained_bytes;
pub use error::{Result, TsinkError};
pub use label::{
    Label, DEFAULT_MAX_LABELS_PER_SERIES, DEFAULT_MAX_SERIES_IDENTITY_BYTES,
    MAX_SUPPORTED_LABELS_PER_SERIES,
};
pub use query_budget::{
    QueryBudget, QueryBudgetConfigError, QueryBudgetError, QueryBudgetLimits, QueryBudgetSnapshot,
    QueryCancellationToken, QueryExecution, QueryExecutionSnapshot, QueryLimitExceeded,
    QueryLimitReason, QueryMemoryReservation, QueryWorkLimits,
};
pub use query_matcher::{
    MAX_QUERY_REGEX_DIAGNOSTIC_BYTES, MAX_SERIES_MATCHER_NAME_BYTES,
    MAX_SERIES_MATCHER_VALUE_BYTES, MAX_SERIES_SELECTION_MATCHERS,
    MAX_SERIES_SELECTION_MATCHER_BYTES, QUERY_REGEX_COMPILED_SIZE_LIMIT_BYTES,
    QUERY_REGEX_DFA_SIZE_LIMIT_BYTES, QUERY_REGEX_NEST_LIMIT,
};
pub use r#async::{AsyncRuntimeOptions, AsyncRuntimeSnapshot, AsyncStorage, AsyncStorageBuilder};
pub use storage::modeled_write_batch_input_bytes;
pub use storage::{
    Aggregation, AsyncResourceLimits, BackgroundResourceLimits,
    BackgroundWorkObservabilitySnapshot, BackgroundWorkerObservabilitySnapshot, BatchWriteResult,
    CardinalityObservabilitySnapshot, CompactionObservabilitySnapshot, DeleteSeriesResult,
    DownsampleOptions, EffectiveStorageLimits, FlushObservabilitySnapshot,
    MemoryMetricsObservabilitySnapshot, MemoryObservabilitySnapshot, MemoryPressureLevel,
    MemoryPressureMetricsSnapshot, MemoryPressureSnapshot, MetadataShardScope, MetricSeries,
    QueryExecutionAccounting, QueryObservabilitySnapshot, QueryOptions, QueryRowsExecutionResult,
    QueryRowsPage, QueryRowsScanOptions, RemoteSegmentCachePolicy,
    RemoteStorageMetricsObservabilitySnapshot, RemoteStorageObservabilitySnapshot,
    ResolvedResourceLimits, ResourceConfigurationSnapshot, ResourceLimitOverride, ResourceLimits,
    ResourceProfile, ResourceProfileName, RetentionObservabilitySnapshot,
    RollupMetricsObservabilitySnapshot, RollupMetricsPolicyStatus, RollupObservabilitySnapshot,
    RollupPolicy, RollupPolicyStatus, RowWriteOutcome, RowWriteStatus, SelectManyExecutionResult,
    SelectSeriesExecutionResult, SeriesMatcher, SeriesMatcherOp, SeriesPoints, SeriesSelection,
    SeriesSelectionPreparation, SeriesSelectionPreparationError, SeriesSelectionValidationError,
    ShardWindowDigest, ShardWindowRowsExecutionResult, ShardWindowRowsPage, ShardWindowScanOptions,
    Storage, StorageBuilder, StorageMetricsObservabilitySnapshot, StorageObservabilitySnapshot,
    StorageRuntimeMode, StorageStatusObservabilitySnapshot, TimestampPrecision,
    WalMetricsObservabilitySnapshot, WalObservabilitySnapshot, WriteAcknowledgement,
    WriteBatchLimits, WriteMode, WriteRejection, WriteRejectionCategory, WriteResult,
    DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES, MAX_SNAPSHOT_RESTORE_DEPTH,
    MAX_SNAPSHOT_RESTORE_ENTRIES, MAX_WRITE_REJECTION_MESSAGE_BYTES,
    RESOURCE_CONFIGURATION_SCHEMA_VERSION, SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES,
};
pub use value::{
    Aggregator, BytesAggregation, Codec, CodecAggregator, HistogramBucketSpan, HistogramCount,
    HistogramResetHint, NativeHistogram, Value,
};
pub use wal::{WalReplayMode, WalSyncMode};

use serde::{Deserialize, Serialize};
use std::fmt;

/// One timestamped sample stored by tsink.
///
/// The unit of [`DataPoint::timestamp`] is selected with
/// [`StorageBuilder::with_timestamp_precision`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataPoint {
    /// The sample payload.
    pub value: Value,
    /// The sample timestamp in the storage instance's configured precision.
    pub timestamp: i64,
}

impl DataPoint {
    /// Creates a sample at `timestamp`, converting `value` into a [`Value`].
    pub fn new(timestamp: i64, value: impl Into<Value>) -> Self {
        Self {
            timestamp,
            value: value.into(),
        }
    }

    /// Returns the numeric payload as an exactly representable `f64`.
    ///
    /// Returns `None` for non-numeric values and integers that cannot be represented exactly.
    pub fn value_as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }

    /// Returns the sample as bytes, or `None` when it has another value kind.
    pub fn value_as_bytes(&self) -> Option<&[u8]> {
        self.value.as_bytes()
    }

    /// Returns the sample as a native histogram, or `None` for another value kind.
    pub fn value_as_histogram(&self) -> Option<&NativeHistogram> {
        self.value.as_histogram()
    }
}

/// A metric identity and one sample, used as the unit of ingestion.
///
/// A row without labels identifies the unlabeled series for its metric. Label order is
/// preserved by this type; the built-in storage backend canonicalizes the series identity when
/// processing the row.
#[derive(Debug, Clone)]
pub struct Row {
    metric: String,
    labels: Vec<Label>,
    data_point: DataPoint,
}

impl Row {
    /// Creates an unlabeled metric row.
    pub fn new(metric: impl Into<String>, data_point: DataPoint) -> Self {
        Self {
            metric: metric.into(),
            labels: Vec::new(),
            data_point,
        }
    }

    /// Creates a metric row with the supplied labels.
    pub fn with_labels(
        metric: impl Into<String>,
        labels: Vec<Label>,
        data_point: DataPoint,
    ) -> Self {
        Self {
            metric: metric.into(),
            labels,
            data_point,
        }
    }

    /// Returns the metric name.
    pub fn metric(&self) -> &str {
        &self.metric
    }

    /// Returns the allocation capacity of the owned metric name.
    ///
    /// Resource-accounting adapters use this to retain an exact capacity-based memory charge
    /// while a returned row remains live.
    pub fn metric_capacity(&self) -> usize {
        self.metric.capacity()
    }

    /// Returns the labels attached to the metric series.
    pub fn labels(&self) -> &[Label] {
        &self.labels
    }

    /// Returns the allocation capacity of the owned label vector.
    ///
    /// Resource-accounting adapters use this to retain an exact capacity-based memory charge
    /// while a returned row remains live.
    pub fn labels_capacity(&self) -> usize {
        self.labels.capacity()
    }

    /// Returns the row's sample.
    pub fn data_point(&self) -> &DataPoint {
        &self.data_point
    }

    /// Replaces the metric name.
    pub fn set_metric(&mut self, metric: impl Into<String>) {
        self.metric = metric.into();
    }

    /// Replaces all labels on the row.
    pub fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

    /// Replaces the row's sample.
    pub fn set_data_point(&mut self, data_point: DataPoint) {
        self.data_point = data_point;
    }
}

impl fmt::Display for DataPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataPoint(ts: {}, val: {})", self.timestamp, self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::{DataPoint, Label, Row};

    #[test]
    fn datapoint_equality_treats_nan_values_as_equal() {
        assert_eq!(DataPoint::new(1, f64::NAN), DataPoint::new(1, f64::NAN));
    }

    #[test]
    fn datapoint_equality_keeps_standard_f64_behavior_for_non_nan_values() {
        assert_eq!(DataPoint::new(1, 0.0_f64), DataPoint::new(1, -0.0_f64));
        assert_ne!(DataPoint::new(1, 1.0_f64), DataPoint::new(1, 2.0_f64));
    }

    #[test]
    fn row_capacity_accessors_report_spare_owned_capacity() {
        let mut metric = String::with_capacity(64);
        metric.push('m');
        let mut labels = Vec::with_capacity(8);
        labels.push(Label::new("host", "a"));
        let row = Row::with_labels(metric, labels, DataPoint::new(1, 1.0));

        assert_eq!(row.metric().len(), 1);
        assert_eq!(row.metric_capacity(), 64);
        assert_eq!(row.labels().len(), 1);
        assert_eq!(row.labels_capacity(), 8);
    }
}
