//! Public tsink data model and entrypoints.

pub mod r#async;
pub mod cgroup;
pub mod concurrency;
pub mod engine;
pub mod error;
pub mod label;
pub mod mmap;
pub mod promql;
pub(crate) mod query_aggregation;
pub(crate) mod query_matcher;
pub(crate) mod query_selection;
pub mod storage;
pub(crate) mod validation;
pub mod value;
pub mod wal;

pub use error::{Result, TsinkError};
pub use label::Label;
pub use r#async::{AsyncRuntimeOptions, AsyncStorage, AsyncStorageBuilder};
pub use storage::{
    Aggregation, CompactionObservabilitySnapshot, DeleteSeriesResult, DownsampleOptions,
    FlushObservabilitySnapshot, MemoryObservabilitySnapshot, MetadataShardScope, MetricSeries,
    QueryObservabilitySnapshot, QueryOptions, QueryRowsPage, QueryRowsScanOptions,
    RemoteSegmentCachePolicy, RemoteStorageObservabilitySnapshot, RetentionObservabilitySnapshot,
    RollupObservabilitySnapshot, RollupPolicy, RollupPolicyStatus, SeriesMatcher, SeriesMatcherOp,
    SeriesPoints, SeriesSelection, ShardWindowDigest, ShardWindowRowsPage, ShardWindowScanOptions,
    Storage, StorageBuilder, StorageObservabilitySnapshot, StorageRuntimeMode, TimestampPrecision,
    WalObservabilitySnapshot, WriteAcknowledgement, WriteResult,
    DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
};
pub use value::{
    Aggregator, BytesAggregation, Codec, CodecAggregator, HistogramBucketSpan, HistogramCount,
    HistogramResetHint, NativeHistogram, Value,
};
pub use wal::{WalReplayMode, WalSyncMode};

use serde::{Deserialize, Serialize};
use std::fmt;

/// One timestamped sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataPoint {
    pub value: Value,
    pub timestamp: i64,
}

impl DataPoint {
    pub fn new(timestamp: i64, value: impl Into<Value>) -> Self {
        Self {
            timestamp,
            value: value.into(),
        }
    }

    pub fn value_as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }

    pub fn value_as_bytes(&self) -> Option<&[u8]> {
        self.value.as_bytes()
    }

    pub fn value_as_histogram(&self) -> Option<&NativeHistogram> {
        self.value.as_histogram()
    }
}

/// Metric identity plus sample payload.
#[derive(Debug, Clone)]
pub struct Row {
    metric: String,
    labels: Vec<Label>,
    data_point: DataPoint,
}

impl Row {
    pub fn new(metric: impl Into<String>, data_point: DataPoint) -> Self {
        Self {
            metric: metric.into(),
            labels: Vec::new(),
            data_point,
        }
    }

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

    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn labels(&self) -> &[Label] {
        &self.labels
    }

    pub fn data_point(&self) -> &DataPoint {
        &self.data_point
    }

    pub fn set_metric(&mut self, metric: impl Into<String>) {
        self.metric = metric.into();
    }

    pub fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

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
    use super::DataPoint;

    #[test]
    fn datapoint_equality_treats_nan_values_as_equal() {
        assert_eq!(DataPoint::new(1, f64::NAN), DataPoint::new(1, f64::NAN));
    }

    #[test]
    fn datapoint_equality_keeps_standard_f64_behavior_for_non_nan_values() {
        assert_eq!(DataPoint::new(1, 0.0_f64), DataPoint::new(1, -0.0_f64));
        assert_ne!(DataPoint::new(1, 1.0_f64), DataPoint::new(1, 2.0_f64));
    }
}
