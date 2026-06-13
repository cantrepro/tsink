//! Main storage API and query helpers for tsink.

use crate::wal::WalSyncMode;
use crate::{
    Aggregator as TypedAggregator, BytesAggregation, Codec, CodecAggregator, DataPoint, Label,
    Result, Row, TsinkError, Value,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Timestamp precision for data points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

/// A unique metric series identified by name and label set.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricSeries {
    pub name: String,
    pub labels: Vec<Label>,
}

/// Storage provides thread-safe capabilities for insertion and retrieval from time-series storage.
pub trait Storage: Send + Sync {
    /// Inserts rows into the storage.
    fn insert_rows(&self, rows: &[Row]) -> Result<()>;

    /// Selects data points for a metric within the given time range.
    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>>;

    /// Selects data points into a caller-provided buffer, allowing allocation reuse.
    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        *out = self.select(metric, labels, start, end)?;
        Ok(())
    }

    /// Selects with additional options like downsampling, aggregation, and pagination.
    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>>;

    /// Selects data points for a metric regardless of labels.
    /// Returns a map of label sets to their corresponding data points.
    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>>;

    /// Lists all known metric series currently present in storage.
    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        Err(TsinkError::Other(
            "list_metrics is not implemented for this storage backend".to_string(),
        ))
    }

    /// Lists known metric series and may include additional WAL-scanned series.
    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        self.list_metrics()
    }

    /// Closes the storage gracefully.
    fn close(&self) -> Result<()>;
}

/// Builder for creating a Storage instance.
pub struct StorageBuilder {
    data_path: Option<PathBuf>,
    retention: Duration,
    retention_enforced: bool,
    timestamp_precision: TimestampPrecision,
    chunk_points: usize,
    max_writers: usize,
    write_timeout: Duration,
    partition_duration: Duration,
    wal_enabled: bool,
    wal_buffer_size: usize,
    wal_sync_mode: WalSyncMode,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self {
            data_path: None,
            retention: Duration::from_secs(14 * 24 * 3600), // 14 days
            retention_enforced: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            chunk_points: crate::engine::DEFAULT_CHUNK_POINTS,
            max_writers: crate::cgroup::default_workers_limit(),
            write_timeout: Duration::from_secs(30),
            partition_duration: Duration::from_secs(3600),
            wal_enabled: true,
            wal_buffer_size: 4096,
            wal_sync_mode: WalSyncMode::default(),
        }
    }
}

impl StorageBuilder {
    /// Creates a new StorageBuilder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the data path for persistent storage.
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.data_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the retention period.
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// Enables or disables retention enforcement.
    ///
    /// When disabled, points are never rejected or filtered due to retention.
    pub fn with_retention_enforced(mut self, enforced: bool) -> Self {
        self.retention_enforced = enforced;
        self
    }

    /// Sets the timestamp precision.
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    /// Sets the target points-per-chunk for the storage engine.
    pub fn with_chunk_points(mut self, points: usize) -> Self {
        self.chunk_points = points.clamp(1, u16::MAX as usize);
        self
    }

    /// Sets the maximum number of concurrent writers.
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.max_writers = if max_writers == 0 {
            crate::cgroup::default_workers_limit().max(1)
        } else {
            max_writers
        };
        self
    }

    /// Sets the write timeout.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Sets the partition duration.
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.partition_duration = duration;
        self
    }

    /// Enables or disables WAL.
    pub fn with_wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Sets the WAL buffer size.
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.wal_buffer_size = size;
        self
    }

    /// Sets WAL fsync policy.
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.wal_sync_mode = mode;
        self
    }

    /// Builds the Storage instance.
    pub fn build(self) -> Result<Arc<dyn Storage>> {
        crate::engine::build_storage(self)
    }

    pub(crate) fn chunk_points(&self) -> usize {
        self.chunk_points
    }

    pub(crate) fn data_path(&self) -> Option<&Path> {
        self.data_path.as_deref()
    }

    pub(crate) fn retention(&self) -> Duration {
        self.retention
    }

    pub(crate) fn retention_enforced(&self) -> bool {
        self.retention_enforced
    }

    pub(crate) fn timestamp_precision(&self) -> TimestampPrecision {
        self.timestamp_precision
    }

    pub(crate) fn max_writers(&self) -> usize {
        self.max_writers
    }

    pub(crate) fn write_timeout(&self) -> Duration {
        self.write_timeout
    }

    pub(crate) fn partition_duration(&self) -> Duration {
        self.partition_duration
    }

    pub(crate) fn wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    pub(crate) fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    pub(crate) fn wal_sync_mode(&self) -> WalSyncMode {
        self.wal_sync_mode
    }
}

/// Aggregation applied to query results or buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    None,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
    Count,
    Median,
    Range,
    Variance,
    StdDev,
}

/// Downsampling configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownsampleOptions {
    pub interval: i64,
}

/// Options to customize queries (aggregation, downsampling, pagination).
#[derive(Clone)]
pub struct QueryOptions {
    pub labels: Vec<Label>,
    pub start: i64,
    pub end: i64,
    pub aggregation: Aggregation,
    pub downsample: Option<DownsampleOptions>,
    pub custom_aggregation: Option<Arc<dyn BytesAggregation>>,
    pub limit: Option<usize>,
    pub offset: usize,
}

impl QueryOptions {
    /// Create query options for a time range.
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            labels: Vec::new(),
            start,
            end,
            aggregation: Aggregation::None,
            downsample: None,
            custom_aggregation: None,
            limit: None,
            offset: 0,
        }
    }

    /// Attach labels to filter the series.
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }

    /// Apply pagination.
    pub fn with_pagination(mut self, offset: usize, limit: Option<usize>) -> Self {
        self.offset = offset;
        self.limit = limit;
        self
    }

    /// Apply downsampling using the given interval and aggregation.
    pub fn with_downsample(mut self, interval: i64, aggregation: Aggregation) -> Self {
        self.downsample = Some(DownsampleOptions { interval });
        self.aggregation = aggregation;
        self
    }

    /// Apply aggregation without downsampling (reduces the whole series to one point).
    pub fn with_aggregation(mut self, aggregation: Aggregation) -> Self {
        self.aggregation = aggregation;
        self
    }

    /// Apply a custom bytes aggregation by providing a codec and typed aggregator.
    pub fn with_custom_bytes_aggregation<C, A>(mut self, codec: C, aggregator: A) -> Self
    where
        C: Codec + 'static,
        A: TypedAggregator<C::Item> + 'static,
    {
        self.custom_aggregation = Some(Arc::new(CodecAggregator::new(codec, aggregator)));
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericDomain {
    F64,
    I64,
    U64,
    Mixed,
}

fn aggregation_name(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::None => "none",
        Aggregation::Sum => "sum",
        Aggregation::Min => "min",
        Aggregation::Max => "max",
        Aggregation::Avg => "avg",
        Aggregation::First => "first",
        Aggregation::Last => "last",
        Aggregation::Count => "count",
        Aggregation::Median => "median",
        Aggregation::Range => "range",
        Aggregation::Variance => "variance",
        Aggregation::StdDev => "stddev",
    }
}

fn is_nan_f64(value: &Value) -> bool {
    matches!(value, Value::F64(v) if v.is_nan())
}

fn numeric_domain(points: &[DataPoint], aggregation: Aggregation) -> Result<Option<NumericDomain>> {
    let mut has_f64 = false;
    let mut has_i64 = false;
    let mut has_u64 = false;

    for point in points {
        match &point.value {
            Value::F64(v) => {
                if !v.is_nan() {
                    has_f64 = true;
                }
            }
            Value::I64(_) => has_i64 = true,
            Value::U64(_) => has_u64 = true,
            _ => {
                return Err(TsinkError::UnsupportedAggregation {
                    aggregation: aggregation_name(aggregation).to_string(),
                    value_type: point.value.kind().to_string(),
                });
            }
        }
    }

    let count = has_f64 as u8 + has_i64 as u8 + has_u64 as u8;
    if count == 0 {
        Ok(None)
    } else if count > 1 {
        Ok(Some(NumericDomain::Mixed))
    } else if has_f64 {
        Ok(Some(NumericDomain::F64))
    } else if has_i64 {
        Ok(Some(NumericDomain::I64))
    } else {
        Ok(Some(NumericDomain::U64))
    }
}

fn numeric_values_f64(points: &[DataPoint], aggregation: Aggregation) -> Result<Vec<f64>> {
    let mut values = Vec::with_capacity(points.len());
    for point in points {
        match &point.value {
            Value::F64(v) if v.is_nan() => {}
            Value::F64(v) => values.push(*v),
            Value::I64(v) => values.push(*v as f64),
            Value::U64(v) => values.push(*v as f64),
            _ => {
                return Err(TsinkError::UnsupportedAggregation {
                    aggregation: aggregation_name(aggregation).to_string(),
                    value_type: point.value.kind().to_string(),
                });
            }
        }
    }
    Ok(values)
}

fn value_cmp(lhs: &Value, rhs: &Value) -> Result<std::cmp::Ordering> {
    match (lhs, rhs) {
        (Value::F64(a), Value::F64(b)) => Ok(a.total_cmp(b)),
        (Value::F64(a), Value::I64(b)) => Ok(a.total_cmp(&(*b as f64))),
        (Value::F64(a), Value::U64(b)) => Ok(a.total_cmp(&(*b as f64))),
        (Value::I64(a), Value::F64(b)) => Ok((*a as f64).total_cmp(b)),
        (Value::I64(a), Value::I64(b)) => Ok(a.cmp(b)),
        (Value::I64(a), Value::U64(b)) => Ok((*a as i128).cmp(&(*b as i128))),
        (Value::U64(a), Value::F64(b)) => Ok((*a as f64).total_cmp(b)),
        (Value::U64(a), Value::I64(b)) => Ok((*a as i128).cmp(&(*b as i128))),
        (Value::U64(a), Value::U64(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Ok(a.cmp(b)),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        _ => Err(TsinkError::ValueTypeMismatch {
            expected: lhs.kind().to_string(),
            actual: rhs.kind().to_string(),
        }),
    }
}

fn min_point(points: &[DataPoint]) -> Result<Option<DataPoint>> {
    let mut best: Option<DataPoint> = None;
    for point in points {
        if is_nan_f64(&point.value) {
            continue;
        }

        match &best {
            Some(current) if value_cmp(&point.value, &current.value)?.is_lt() => {
                best = Some(point.clone())
            }
            None => best = Some(point.clone()),
            _ => {}
        }
    }
    Ok(best)
}

fn max_point(points: &[DataPoint]) -> Result<Option<DataPoint>> {
    let mut best: Option<DataPoint> = None;
    for point in points {
        if is_nan_f64(&point.value) {
            continue;
        }

        match &best {
            Some(current) if value_cmp(&point.value, &current.value)?.is_gt() => {
                best = Some(point.clone())
            }
            None => best = Some(point.clone()),
            _ => {}
        }
    }
    Ok(best)
}

fn sum_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let Some(domain) = numeric_domain(points, Aggregation::Sum)? else {
        return Ok(None);
    };

    let value = match domain {
        NumericDomain::F64 => {
            let sum: f64 = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::F64(v) if !v.is_nan() => Some(*v),
                    _ => None,
                })
                .sum();
            Value::F64(sum)
        }
        NumericDomain::I64 => {
            let sum = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::I64(v) => Some(*v as i128),
                    _ => None,
                })
                .sum::<i128>()
                .clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            Value::I64(sum)
        }
        NumericDomain::U64 => {
            let sum = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::U64(v) => Some(*v as u128),
                    _ => None,
                })
                .sum::<u128>()
                .min(u64::MAX as u128) as u64;
            Value::U64(sum)
        }
        NumericDomain::Mixed => {
            let sum: f64 = numeric_values_f64(points, Aggregation::Sum)?
                .into_iter()
                .sum();
            Value::F64(sum)
        }
    };

    Ok(Some(value))
}

fn average_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let values = numeric_values_f64(points, Aggregation::Avg)?;
    if values.is_empty() {
        return Ok(None);
    }
    let sum: f64 = values.iter().sum();
    Ok(Some(Value::F64(sum / values.len() as f64)))
}

fn median_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let mut values = numeric_values_f64(points, Aggregation::Median)?;
    if values.is_empty() {
        return Ok(None);
    }

    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    let median = if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    };

    Ok(Some(Value::F64(median)))
}

fn range_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let Some(domain) = numeric_domain(points, Aggregation::Range)? else {
        return Ok(None);
    };

    let value = match domain {
        NumericDomain::F64 => {
            let values: Vec<f64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::F64(v) if !v.is_nan() => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap();
            Value::F64(max - min)
        }
        NumericDomain::I64 => {
            let values: Vec<i64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::I64(v) => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values.iter().min().copied().unwrap() as i128;
            let max = values.iter().max().copied().unwrap() as i128;
            Value::I64((max - min).clamp(i64::MIN as i128, i64::MAX as i128) as i64)
        }
        NumericDomain::U64 => {
            let values: Vec<u64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::U64(v) => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values.iter().min().copied().unwrap();
            let max = values.iter().max().copied().unwrap();
            Value::U64(max.saturating_sub(min))
        }
        NumericDomain::Mixed => {
            // Preserve integer precision when the mixed set is i64/u64 only.
            let mut int_min: Option<i128> = None;
            let mut int_max: Option<i128> = None;
            let mut has_f64 = false;

            for point in points {
                match &point.value {
                    Value::I64(v) => {
                        let value = *v as i128;
                        int_min = Some(int_min.map_or(value, |min| min.min(value)));
                        int_max = Some(int_max.map_or(value, |max| max.max(value)));
                    }
                    Value::U64(v) => {
                        let value = *v as i128;
                        int_min = Some(int_min.map_or(value, |min| min.min(value)));
                        int_max = Some(int_max.map_or(value, |max| max.max(value)));
                    }
                    Value::F64(v) if v.is_nan() => {}
                    Value::F64(_) => {
                        has_f64 = true;
                        break;
                    }
                    _ => {
                        return Err(TsinkError::UnsupportedAggregation {
                            aggregation: aggregation_name(Aggregation::Range).to_string(),
                            value_type: point.value.kind().to_string(),
                        });
                    }
                }
            }

            if !has_f64 {
                let (Some(min), Some(max)) = (int_min, int_max) else {
                    return Ok(None);
                };
                return Ok(Some(Value::F64((max - min) as f64)));
            }

            let values = numeric_values_f64(points, Aggregation::Range)?;
            if values.is_empty() {
                return Ok(None);
            }
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap();
            Value::F64(max - min)
        }
    };

    Ok(Some(value))
}

fn population_variance_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let values = numeric_values_f64(points, Aggregation::Variance)?;
    if values.is_empty() {
        return Ok(None);
    }

    let mut count = 0.0f64;
    let mut mean = 0.0f64;
    let mut m2 = 0.0f64;

    for value in values {
        count += 1.0;
        let delta = value - mean;
        mean += delta / count;
        let delta2 = value - mean;
        m2 += delta * delta2;
    }

    Ok(Some(Value::F64(m2 / count)))
}

pub(crate) fn aggregate_series(
    points: &[DataPoint],
    aggregation: Aggregation,
) -> Result<Option<DataPoint>> {
    let Some(last) = points.last() else {
        return Ok(None);
    };

    let aggregated = match aggregation {
        Aggregation::None => None,
        Aggregation::First => return Ok(points.first().cloned()),
        Aggregation::Last => return Ok(points.last().cloned()),
        Aggregation::Count => Some(DataPoint::new(
            last.timestamp,
            Value::U64(
                points
                    .iter()
                    .filter(|point| !is_nan_f64(&point.value))
                    .count() as u64,
            ),
        )),
        Aggregation::Sum => sum_numeric(points)?
            .map(|value| DataPoint::new(last.timestamp, value))
            .or_else(|| Some(last.clone())),
        Aggregation::Avg => average_numeric(points)?
            .map(|value| DataPoint::new(last.timestamp, value))
            .or_else(|| Some(last.clone())),
        Aggregation::Min => min_point(points)?.or_else(|| Some(last.clone())),
        Aggregation::Max => max_point(points)?.or_else(|| Some(last.clone())),
        Aggregation::Median => median_numeric(points)?
            .map(|value| DataPoint::new(last.timestamp, value))
            .or_else(|| Some(last.clone())),
        Aggregation::Range => range_numeric(points)?
            .map(|value| DataPoint::new(last.timestamp, value))
            .or_else(|| Some(last.clone())),
        Aggregation::Variance => population_variance_numeric(points)?
            .map(|value| DataPoint::new(last.timestamp, value))
            .or_else(|| Some(last.clone())),
        Aggregation::StdDev => population_variance_numeric(points)?
            .and_then(|value| match value {
                Value::F64(v) => Some(Value::F64(v.sqrt())),
                _ => None,
            })
            .map(|value| DataPoint::new(last.timestamp, value))
            .or_else(|| Some(last.clone())),
    };

    Ok(aggregated)
}

fn aggregate_bucket(
    points: &[DataPoint],
    aggregation: Aggregation,
    bucket_start: i64,
) -> Result<Option<DataPoint>> {
    let Some(last) = points.last() else {
        return Ok(None);
    };

    let aggregated = match aggregation {
        Aggregation::None => None,
        Aggregation::First => return Ok(points.first().cloned()),
        Aggregation::Last => return Ok(points.last().cloned()),
        Aggregation::Count => Some(DataPoint::new(
            bucket_start,
            Value::U64(
                points
                    .iter()
                    .filter(|point| !is_nan_f64(&point.value))
                    .count() as u64,
            ),
        )),
        Aggregation::Sum => sum_numeric(points)?
            .map(|value| DataPoint::new(bucket_start, value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::Avg => average_numeric(points)?
            .map(|value| DataPoint::new(bucket_start, value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::Min => min_point(points)?
            .map(|point| DataPoint::new(bucket_start, point.value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::Max => max_point(points)?
            .map(|point| DataPoint::new(bucket_start, point.value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::Median => median_numeric(points)?
            .map(|value| DataPoint::new(bucket_start, value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::Range => range_numeric(points)?
            .map(|value| DataPoint::new(bucket_start, value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::Variance => population_variance_numeric(points)?
            .map(|value| DataPoint::new(bucket_start, value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
        Aggregation::StdDev => population_variance_numeric(points)?
            .and_then(|value| match value {
                Value::F64(v) => Some(Value::F64(v.sqrt())),
                _ => None,
            })
            .map(|value| DataPoint::new(bucket_start, value))
            .or_else(|| Some(DataPoint::new(bucket_start, last.value.clone()))),
    };

    Ok(aggregated)
}

pub(crate) fn downsample_points(
    points: &[DataPoint],
    interval: i64,
    aggregation: Aggregation,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    if points.is_empty() || interval <= 0 || start >= end {
        return Ok(Vec::new());
    }

    fn bucket_start_for(ts: i64, start: i64, interval: i64) -> i64 {
        let rel = ts as i128 - start as i128;
        let bucket = start as i128 + rel.div_euclid(interval as i128) * interval as i128;
        bucket.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }

    let mut result = Vec::new();
    let mut idx = 0;

    while idx < points.len() && points[idx].timestamp < start {
        idx += 1;
    }

    while idx < points.len() {
        if points[idx].timestamp >= end {
            break;
        }

        let bucket_start = bucket_start_for(points[idx].timestamp, start, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        let bucket_begin = idx;

        while idx < points.len() {
            let ts = points[idx].timestamp;
            if ts >= end || ts >= bucket_end {
                break;
            }
            idx += 1;
        }

        if let Some(dp) = aggregate_bucket(&points[bucket_begin..idx], aggregation, bucket_start)? {
            result.push(dp);
        }
    }

    Ok(result)
}

pub(crate) fn downsample_points_with_custom(
    points: &[DataPoint],
    interval: i64,
    aggregation: &dyn BytesAggregation,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    if points.is_empty() || interval <= 0 || start >= end {
        return Ok(Vec::new());
    }

    fn bucket_start_for(ts: i64, start: i64, interval: i64) -> i64 {
        let rel = ts as i128 - start as i128;
        let bucket = start as i128 + rel.div_euclid(interval as i128) * interval as i128;
        bucket.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }

    let mut result = Vec::new();
    let mut idx = 0;

    while idx < points.len() && points[idx].timestamp < start {
        idx += 1;
    }

    while idx < points.len() {
        if points[idx].timestamp >= end {
            break;
        }

        let bucket_start = bucket_start_for(points[idx].timestamp, start, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        let bucket_begin = idx;

        while idx < points.len() {
            let ts = points[idx].timestamp;
            if ts >= end || ts >= bucket_end {
                break;
            }
            idx += 1;
        }

        if let Some(dp) = aggregation.aggregate_bucket(&points[bucket_begin..idx], bucket_start)? {
            result.push(dp);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::aggregate_series;
    use crate::{Aggregation, DataPoint, Value};

    #[test]
    fn mixed_i64_u64_min_max_use_integer_order_for_large_values() {
        let larger = i64::MAX;
        let smaller = (i64::MAX as u64).saturating_sub(1);

        let min_points = vec![
            DataPoint::new(1, Value::I64(larger)),
            DataPoint::new(2, Value::U64(smaller)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::U64(smaller));

        let max_points = vec![
            DataPoint::new(1, Value::U64(smaller)),
            DataPoint::new(2, Value::I64(larger)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::I64(larger));
    }

    #[test]
    fn mixed_i64_u64_range_preserves_unit_difference_at_high_values() {
        let points = vec![
            DataPoint::new(1, Value::I64(i64::MAX - 1)),
            DataPoint::new(2, Value::U64(i64::MAX as u64)),
        ];

        let range = aggregate_series(&points, Aggregation::Range)
            .unwrap()
            .unwrap();
        assert_eq!(range.value, Value::F64(1.0));
    }
}
