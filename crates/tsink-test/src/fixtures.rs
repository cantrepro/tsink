//! Focused metric-fixture constructors used by [`crate::TsinkTestDb`].

use tsink::{DataPoint, Label, MetricSeries, NativeHistogram, Row, TsinkError, Value};

/// Creates one metric label.
#[must_use]
pub fn label(name: impl Into<String>, value: impl Into<String>) -> Label {
    Label::new(name, value)
}

/// Creates labels from `(name, value)` pairs while preserving their supplied order.
///
/// The core engine performs canonical identity sorting and validation when rows are written.
#[must_use]
pub fn labels<I, N, V>(pairs: I) -> Vec<Label>
where
    I: IntoIterator<Item = (N, V)>,
    N: Into<String>,
    V: Into<String>,
{
    pairs
        .into_iter()
        .map(|(name, value)| Label::new(name, value))
        .collect()
}

/// Creates one metric-series identity.
#[must_use]
pub fn metric_series(name: impl Into<String>, labels: Vec<Label>) -> MetricSeries {
    MetricSeries {
        name: name.into(),
        labels,
    }
}

/// Creates one row with an arbitrary core value.
#[must_use]
pub fn sample(
    metric: impl Into<String>,
    labels: Vec<Label>,
    timestamp: i64,
    value: impl Into<Value>,
) -> Row {
    Row::with_labels(metric, labels, DataPoint::new(timestamp, value))
}

/// Creates one numeric gauge row.
///
/// This is a fixture-intent alias; it does not register a separate metric-type metadata model.
#[must_use]
pub fn gauge(metric: impl Into<String>, labels: Vec<Label>, timestamp: i64, value: f64) -> Row {
    sample(metric, labels, timestamp, value)
}

/// Creates one numeric counter row.
///
/// This is a fixture-intent alias; monotonicity across multiple calls remains the test's explicit
/// responsibility.
#[must_use]
pub fn counter(metric: impl Into<String>, labels: Vec<Label>, timestamp: i64, value: f64) -> Row {
    sample(metric, labels, timestamp, value)
}

/// Creates numeric rows at `start`, `start + step`, and so on.
///
/// `step` must be positive. Timestamp arithmetic is checked before any rows are returned, so an
/// overflow never produces a partial fixture.
pub fn evenly_spaced_samples<I>(
    metric: impl Into<String>,
    labels: Vec<Label>,
    start: i64,
    step: i64,
    values: I,
) -> tsink::Result<Vec<Row>>
where
    I: IntoIterator<Item = f64>,
{
    if step <= 0 {
        return Err(TsinkError::InvalidConfiguration(
            "tsink-test evenly spaced sample step must be positive".to_string(),
        ));
    }

    let metric = metric.into();
    let values = values.into_iter().collect::<Vec<_>>();
    let mut timestamps = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        let index = i64::try_from(index).map_err(|_| {
            TsinkError::InvalidConfiguration(
                "tsink-test evenly spaced sample count exceeds i64".to_string(),
            )
        })?;
        let offset = step.checked_mul(index).ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "tsink-test evenly spaced sample timestamp overflow".to_string(),
            )
        })?;
        timestamps.push(start.checked_add(offset).ok_or_else(|| {
            TsinkError::InvalidConfiguration(
                "tsink-test evenly spaced sample timestamp overflow".to_string(),
            )
        })?);
    }

    Ok(timestamps
        .into_iter()
        .zip(values)
        .map(|(timestamp, value)| gauge(metric.clone(), labels.clone(), timestamp, value))
        .collect())
}

/// Creates a monotonic counter sequence at evenly spaced timestamps.
///
/// `initial` and `increment` must be finite and non-negative, and every generated value must
/// remain finite. Timestamp arithmetic follows [`evenly_spaced_samples`].
pub fn counter_sequence(
    metric: impl Into<String>,
    labels: Vec<Label>,
    start: i64,
    step: i64,
    initial: f64,
    increment: f64,
    count: usize,
) -> tsink::Result<Vec<Row>> {
    if !initial.is_finite() || initial < 0.0 || !increment.is_finite() || increment < 0.0 {
        return Err(TsinkError::InvalidConfiguration(
            "tsink-test counter initial value and increment must be finite and non-negative"
                .to_string(),
        ));
    }

    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let value = initial + increment * index as f64;
        if !value.is_finite() {
            return Err(TsinkError::InvalidConfiguration(
                "tsink-test counter sequence value overflow".to_string(),
            ));
        }
        values.push(value);
    }
    evenly_spaced_samples(metric, labels, start, step, values)
}

/// Expands one classic Prometheus histogram snapshot into canonical `_bucket`, `_sum`, and
/// `_count` rows.
///
/// `finite_buckets` contains `(upper_bound, cumulative_count)` pairs. Bounds must be finite and
/// strictly increasing; counts must be finite, non-negative, nondecreasing, and no greater than
/// `count`. The helper always appends the required `le="+Inf"` bucket with cumulative `count`.
/// Caller labels may not already contain `le`.
pub fn classic_histogram(
    metric: impl AsRef<str>,
    labels: &[Label],
    timestamp: i64,
    finite_buckets: &[(f64, f64)],
    count: f64,
    sum: f64,
) -> tsink::Result<Vec<Row>> {
    let metric = metric.as_ref();
    if labels.iter().any(|label| label.name == "le") {
        return Err(TsinkError::InvalidConfiguration(
            "tsink-test classic histogram base labels may not contain `le`".to_string(),
        ));
    }
    if !count.is_finite() || count < 0.0 {
        return Err(TsinkError::InvalidConfiguration(
            "tsink-test classic histogram count must be finite and non-negative".to_string(),
        ));
    }
    if !sum.is_finite() {
        return Err(TsinkError::InvalidConfiguration(
            "tsink-test classic histogram sum must be finite".to_string(),
        ));
    }

    let mut previous_bound = None;
    let mut previous_count = 0.0;
    for (bound, bucket_count) in finite_buckets {
        if !bound.is_finite()
            || previous_bound.is_some_and(|previous| *bound <= previous)
            || !bucket_count.is_finite()
            || *bucket_count < previous_count
            || *bucket_count < 0.0
            || *bucket_count > count
        {
            return Err(TsinkError::InvalidConfiguration(
                "tsink-test classic histogram buckets require strictly increasing finite bounds \
                 and nondecreasing finite counts within the total count"
                    .to_string(),
            ));
        }
        previous_bound = Some(*bound);
        previous_count = *bucket_count;
    }

    let bucket_metric = format!("{metric}_bucket");
    let mut rows = Vec::with_capacity(finite_buckets.len().saturating_add(3));
    for (bound, bucket_count) in finite_buckets {
        let mut bucket_labels = labels.to_vec();
        bucket_labels.push(Label::new("le", format_histogram_bound(*bound)));
        rows.push(gauge(
            bucket_metric.clone(),
            bucket_labels,
            timestamp,
            *bucket_count,
        ));
    }
    let mut infinite_labels = labels.to_vec();
    infinite_labels.push(Label::new("le", "+Inf"));
    rows.push(gauge(bucket_metric, infinite_labels, timestamp, count));
    rows.push(gauge(
        format!("{metric}_sum"),
        labels.to_vec(),
        timestamp,
        sum,
    ));
    rows.push(counter(
        format!("{metric}_count"),
        labels.to_vec(),
        timestamp,
        count,
    ));
    Ok(rows)
}

/// Creates one first-class native-histogram row.
#[must_use]
pub fn native_histogram(
    metric: impl Into<String>,
    labels: Vec<Label>,
    timestamp: i64,
    histogram: NativeHistogram,
) -> Row {
    sample(metric, labels, timestamp, histogram)
}

fn format_histogram_bound(bound: f64) -> String {
    if bound == 0.0 {
        "0".to_string()
    } else {
        bound.to_string()
    }
}
