use std::collections::BTreeMap;
use std::fmt;
use tsink::{DataPoint, MetricSeries, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadMergeLimits {
    pub max_series: usize,
    pub max_points_per_series: usize,
    pub max_total_points: usize,
}

impl ReadMergeLimits {
    pub const DEFAULT_MAX_SERIES: usize = 250_000;
    pub const DEFAULT_MAX_POINTS_PER_SERIES: usize = 1_000_000;
    pub const DEFAULT_MAX_TOTAL_POINTS: usize = 5_000_000;

    pub fn validate(self) -> Result<(), String> {
        if self.max_series == 0 {
            return Err("read merge max series must be greater than zero".to_string());
        }
        if self.max_points_per_series == 0 {
            return Err("read merge max points per series must be greater than zero".to_string());
        }
        if self.max_total_points == 0 {
            return Err("read merge max total points must be greater than zero".to_string());
        }
        Ok(())
    }
}

impl Default for ReadMergeLimits {
    fn default() -> Self {
        Self {
            max_series: Self::DEFAULT_MAX_SERIES,
            max_points_per_series: Self::DEFAULT_MAX_POINTS_PER_SERIES,
            max_total_points: Self::DEFAULT_MAX_TOTAL_POINTS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeLimitError {
    Series {
        limit: usize,
        attempted: usize,
    },
    PointsPerSeries {
        series: String,
        limit: usize,
        attempted: usize,
    },
    PointsTotal {
        limit: usize,
        attempted: usize,
    },
}

impl fmt::Display for MergeLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Series { limit, attempted } => {
                write!(
                    f,
                    "read merge series limit exceeded: attempted {attempted}, max {limit}"
                )
            }
            Self::PointsPerSeries {
                series,
                limit,
                attempted,
            } => {
                write!(
                    f,
                    "read merge point limit exceeded for series '{series}': attempted {attempted}, max {limit}"
                )
            }
            Self::PointsTotal { limit, attempted } => {
                write!(
                    f,
                    "read merge total points limit exceeded: attempted {attempted}, max {limit}"
                )
            }
        }
    }
}

impl std::error::Error for MergeLimitError {}

#[derive(Debug, Clone)]
pub struct SeriesMetadataMerger {
    limits: ReadMergeLimits,
    merged: BTreeMap<SeriesIdentity, MetricSeries>,
}

impl SeriesMetadataMerger {
    pub fn new(limits: ReadMergeLimits) -> Self {
        Self {
            limits,
            merged: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, series: MetricSeries) -> Result<(), MergeLimitError> {
        let identity = SeriesIdentity::from_series(&series);
        if self.merged.contains_key(&identity) {
            return Ok(());
        }

        let next = self.merged.len().saturating_add(1);
        if next > self.limits.max_series {
            return Err(MergeLimitError::Series {
                limit: self.limits.max_series,
                attempted: next,
            });
        }
        self.merged.insert(identity, series);
        Ok(())
    }

    pub fn extend(
        &mut self,
        series: impl IntoIterator<Item = MetricSeries>,
    ) -> Result<(), MergeLimitError> {
        for item in series {
            self.insert(item)?;
        }
        Ok(())
    }

    pub fn into_series(self) -> Vec<MetricSeries> {
        self.merged.into_values().collect()
    }
}

#[derive(Debug, Clone)]
pub struct SeriesPointsMerger {
    limits: ReadMergeLimits,
    points_by_series: BTreeMap<SeriesIdentity, BTreeMap<PointIdentity, DataPoint>>,
    unique_points_total: usize,
}

impl SeriesPointsMerger {
    pub fn new(limits: ReadMergeLimits) -> Self {
        Self {
            limits,
            points_by_series: BTreeMap::new(),
            unique_points_total: 0,
        }
    }

    pub fn merge_series_points(
        &mut self,
        series: &MetricSeries,
        points: impl IntoIterator<Item = DataPoint>,
    ) -> Result<(), MergeLimitError> {
        let identity = self.ensure_series(series)?;
        let bucket = self
            .points_by_series
            .get_mut(&identity)
            .expect("series bucket should exist");

        for point in points {
            let point_identity = PointIdentity::from_point(&point);
            if bucket.contains_key(&point_identity) {
                continue;
            }

            let next_series_points = bucket.len().saturating_add(1);
            if next_series_points > self.limits.max_points_per_series {
                return Err(MergeLimitError::PointsPerSeries {
                    series: identity.display(),
                    limit: self.limits.max_points_per_series,
                    attempted: next_series_points,
                });
            }

            let next_total_points = self.unique_points_total.saturating_add(1);
            if next_total_points > self.limits.max_total_points {
                return Err(MergeLimitError::PointsTotal {
                    limit: self.limits.max_total_points,
                    attempted: next_total_points,
                });
            }

            bucket.insert(point_identity, point);
            self.unique_points_total = next_total_points;
        }

        Ok(())
    }

    pub fn into_points(self) -> BTreeMap<SeriesIdentity, Vec<DataPoint>> {
        self.points_by_series
            .into_iter()
            .map(|(identity, points)| (identity, points.into_values().collect()))
            .collect()
    }

    #[cfg(test)]
    fn unique_points_total(&self) -> usize {
        self.unique_points_total
    }

    #[cfg(test)]
    fn point_count_for_series(&self, series: &MetricSeries) -> usize {
        let identity = SeriesIdentity::from_series(series);
        self.points_by_series
            .get(&identity)
            .map_or(0, BTreeMap::len)
    }

    fn ensure_series(&mut self, series: &MetricSeries) -> Result<SeriesIdentity, MergeLimitError> {
        let identity = SeriesIdentity::from_series(series);
        if self.points_by_series.contains_key(&identity) {
            return Ok(identity);
        }

        let next = self.points_by_series.len().saturating_add(1);
        if next > self.limits.max_series {
            return Err(MergeLimitError::Series {
                limit: self.limits.max_series,
                attempted: next,
            });
        }

        self.points_by_series
            .insert(identity.clone(), BTreeMap::new());
        Ok(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SeriesIdentity {
    metric: String,
    labels: Vec<(String, String)>,
}

impl SeriesIdentity {
    pub fn from_series(series: &MetricSeries) -> Self {
        let mut labels = series
            .labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect::<Vec<_>>();
        labels.sort();
        Self {
            metric: series.name.clone(),
            labels,
        }
    }

    pub fn display(&self) -> String {
        let labels = self
            .labels
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        if labels.is_empty() {
            self.metric.clone()
        } else {
            format!("{}{{{labels}}}", self.metric)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PointIdentity {
    timestamp: i64,
    value: ValueIdentity,
}

impl PointIdentity {
    fn from_point(point: &DataPoint) -> Self {
        Self {
            timestamp: point.timestamp,
            value: ValueIdentity::from_value(&point.value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ValueIdentity {
    F64(u64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
    Histogram(Vec<u8>),
}

impl ValueIdentity {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::F64(number) => {
                let bits = if number.is_nan() {
                    f64::NAN.to_bits()
                } else {
                    number.to_bits()
                };
                Self::F64(bits)
            }
            Value::I64(number) => Self::I64(*number),
            Value::U64(number) => Self::U64(*number),
            Value::Bool(flag) => Self::Bool(*flag),
            Value::Bytes(bytes) => Self::Bytes(bytes.clone()),
            Value::String(text) => Self::String(text.clone()),
            Value::Histogram(histogram) => {
                let bytes = serde_json::to_vec(histogram).unwrap_or_default();
                Self::Histogram(bytes)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsink::Label;

    fn series(metric: &str, labels: &[(&str, &str)]) -> MetricSeries {
        MetricSeries {
            name: metric.to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect(),
        }
    }

    #[test]
    fn metadata_merger_dedupes_and_orders_series() {
        let mut merger = SeriesMetadataMerger::new(ReadMergeLimits {
            max_series: 8,
            max_points_per_series: 16,
            max_total_points: 32,
        });

        merger
            .extend([
                series("cpu", &[("instance", "b"), ("job", "api")]),
                series("cpu", &[("job", "api"), ("instance", "b")]),
                series("cpu", &[("instance", "a"), ("job", "api")]),
            ])
            .expect("merge should succeed");

        let merged = merger.into_series();
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[0],
            series("cpu", &[("instance", "a"), ("job", "api")])
        );
        assert_eq!(
            merged[1],
            series("cpu", &[("instance", "b"), ("job", "api")])
        );
    }

    #[test]
    fn points_merger_dedupes_without_replica_amplification() {
        let mut merger = SeriesPointsMerger::new(ReadMergeLimits {
            max_series: 8,
            max_points_per_series: 16,
            max_total_points: 32,
        });
        let metric = series("cpu", &[("instance", "a")]);

        merger
            .merge_series_points(
                &metric,
                vec![
                    DataPoint::new(10, 1.0),
                    DataPoint::new(10, 1.0),
                    DataPoint::new(11, 2.0),
                ],
            )
            .expect("first replica points should merge");
        merger
            .merge_series_points(
                &metric,
                vec![
                    DataPoint::new(10, 1.0),
                    DataPoint::new(11, 2.0),
                    DataPoint::new(12, 3.0),
                ],
            )
            .expect("second replica points should merge");

        assert_eq!(merger.unique_points_total(), 3);
        assert_eq!(merger.point_count_for_series(&metric), 3);

        let points = merger.into_points();
        let identity = SeriesIdentity::from_series(&metric);
        let merged = points.get(&identity).expect("series should exist");
        let timestamps = merged
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![10, 11, 12]);
    }

    #[test]
    fn points_merger_enforces_total_point_limit() {
        let mut merger = SeriesPointsMerger::new(ReadMergeLimits {
            max_series: 8,
            max_points_per_series: 8,
            max_total_points: 2,
        });
        let metric = series("cpu", &[("instance", "a")]);

        merger
            .merge_series_points(
                &metric,
                vec![DataPoint::new(10, 1.0), DataPoint::new(11, 2.0)],
            )
            .expect("initial merge should succeed");
        let err = merger
            .merge_series_points(&metric, vec![DataPoint::new(12, 3.0)])
            .expect_err("limit should be enforced");

        assert_eq!(
            err,
            MergeLimitError::PointsTotal {
                limit: 2,
                attempted: 3,
            }
        );
    }
}
