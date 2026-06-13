//! tsink - A lightweight embedded time-series database
//!
//! tsink is a Rust implementation of a time-series storage engine with a straightforward API.

pub mod bstream;
pub mod cgroup;
pub mod concurrency;
pub mod disk;
pub mod encoding;
pub mod error;
pub mod label;
pub mod list;
pub mod memory;
pub mod mmap;
pub mod partition;
pub mod storage;
pub mod wal;

pub use error::{Result, TsinkError};
pub use label::Label;
pub use storage::{
    Aggregation, DownsampleOptions, QueryOptions, Storage, StorageBuilder, TimestampPrecision,
};

use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents a data point, the smallest unit of time series data.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    /// The actual value.
    pub value: f64,
    /// Unix timestamp.
    pub timestamp: i64,
}

impl DataPoint {
    /// Creates a new DataPoint.
    pub fn new(timestamp: i64, value: f64) -> Self {
        Self { timestamp, value }
    }
}

/// A row includes a data point along with properties to identify a kind of metric.
#[derive(Debug, Clone)]
pub struct Row {
    /// The unique name of the metric.
    metric: String,
    /// Optional key-value properties for detailed identification.
    labels: Vec<Label>,
    /// The data point.
    data_point: DataPoint,
}

impl Row {
    /// Creates a new Row.
    pub fn new(metric: impl Into<String>, data_point: DataPoint) -> Self {
        Self {
            metric: metric.into(),
            labels: Vec::new(),
            data_point,
        }
    }

    /// Creates a new Row with labels.
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

    /// Gets the metric name.
    pub fn metric(&self) -> &str {
        &self.metric
    }

    /// Gets the labels.
    pub fn labels(&self) -> &[Label] {
        &self.labels
    }

    /// Gets the data point.
    pub fn data_point(&self) -> DataPoint {
        self.data_point
    }

    /// Sets the metric name.
    pub fn set_metric(&mut self, metric: impl Into<String>) {
        self.metric = metric.into();
    }

    /// Sets the labels.
    pub fn set_labels(&mut self, labels: Vec<Label>) {
        self.labels = labels;
    }

    /// Sets the data point.
    pub fn set_data_point(&mut self, data_point: DataPoint) {
        self.data_point = data_point;
    }
}

impl fmt::Display for DataPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataPoint(ts: {}, val: {})", self.timestamp, self.value)
    }
}
