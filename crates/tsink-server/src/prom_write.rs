use crate::prom_remote::{histogram, HistogramResetHint, MetricMetadata, MetricType, WriteRequest};
use crate::tenant;
use std::collections::BTreeSet;
use tsink::label::{MAX_LABEL_NAME_LEN, MAX_LABEL_VALUE_LEN, MAX_METRIC_NAME_LEN};
use tsink::{
    DataPoint, HistogramBucketSpan, HistogramCount, HistogramResetHint as TsinkHistogramResetHint,
    Label, NativeHistogram, Row, TimestampPrecision, Value,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedWriteEnvelope {
    pub scalar_samples: Vec<NormalizedScalarSample>,
    pub histogram_samples: Vec<NormalizedHistogramSample>,
    pub exemplars: Vec<NormalizedExemplar>,
    pub metadata_updates: Vec<NormalizedMetricMetadataUpdate>,
}

impl NormalizedWriteEnvelope {
    pub fn into_rows(self) -> Vec<Row> {
        let mut rows = self
            .scalar_samples
            .into_iter()
            .map(|sample| {
                Row::with_labels(
                    sample.series.metric,
                    sample.series.labels,
                    sample.data_point,
                )
            })
            .collect::<Vec<_>>();
        rows.extend(self.histogram_samples.into_iter().map(|sample| {
            let histogram = NativeHistogram {
                count: sample.count.map(normalized_histogram_count_to_tsink),
                sum: sample.sum,
                schema: sample.schema,
                zero_threshold: sample.zero_threshold,
                zero_count: sample.zero_count.map(normalized_histogram_count_to_tsink),
                negative_spans: sample
                    .negative_spans
                    .into_iter()
                    .map(normalized_bucket_span_to_tsink)
                    .collect(),
                negative_deltas: sample.negative_deltas,
                negative_counts: sample.negative_counts,
                positive_spans: sample
                    .positive_spans
                    .into_iter()
                    .map(normalized_bucket_span_to_tsink)
                    .collect(),
                positive_deltas: sample.positive_deltas,
                positive_counts: sample.positive_counts,
                reset_hint: normalized_reset_hint_to_tsink(sample.reset_hint),
                custom_values: sample.custom_values,
            };
            Row::with_labels(
                sample.series.metric,
                sample.series.labels,
                DataPoint::new(sample.timestamp, Value::from(histogram)),
            )
        }));
        rows
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedSeriesIdentity {
    pub metric: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedScalarSample {
    pub series: NormalizedSeriesIdentity,
    pub data_point: DataPoint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedExemplar {
    pub series: NormalizedSeriesIdentity,
    pub labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedHistogramCount {
    Int(u64),
    Float(f64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedBucketSpan {
    pub offset: i32,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedHistogramSample {
    pub series: NormalizedSeriesIdentity,
    pub count: Option<NormalizedHistogramCount>,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: Option<NormalizedHistogramCount>,
    pub negative_spans: Vec<NormalizedBucketSpan>,
    pub negative_deltas: Vec<i64>,
    pub negative_counts: Vec<f64>,
    pub positive_spans: Vec<NormalizedBucketSpan>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
    pub reset_hint: HistogramResetHint,
    pub timestamp: i64,
    pub custom_values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedMetricMetadataUpdate {
    pub metric_family_name: String,
    pub metric_type: MetricType,
    pub help: String,
    pub unit: String,
}

pub fn normalize_remote_write_request(
    write_req: WriteRequest,
    tenant_id: &str,
    precision: TimestampPrecision,
) -> Result<NormalizedWriteEnvelope, String> {
    let mut envelope = NormalizedWriteEnvelope::default();

    for (metadata_idx, metadata) in write_req.metadata.into_iter().enumerate() {
        envelope
            .metadata_updates
            .push(normalize_metadata_update(metadata, metadata_idx)?);
    }

    for (series_idx, series) in write_req.timeseries.into_iter().enumerate() {
        let identity = normalize_series_identity(series.labels, tenant_id, series_idx)?;

        for (sample_idx, sample) in series.samples.into_iter().enumerate() {
            let timestamp = convert_remote_write_timestamp(
                sample.timestamp,
                precision,
                &format!("remote write timeseries index {series_idx} sample index {sample_idx}"),
            )?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series: identity.clone(),
                data_point: DataPoint::new(timestamp, sample.value),
            });
        }

        for (exemplar_idx, exemplar) in series.exemplars.into_iter().enumerate() {
            envelope.exemplars.push(normalize_exemplar(
                identity.clone(),
                exemplar,
                precision,
                series_idx,
                exemplar_idx,
            )?);
        }

        for (histogram_idx, histogram) in series.histograms.into_iter().enumerate() {
            envelope.histogram_samples.push(normalize_histogram(
                identity.clone(),
                histogram,
                precision,
                series_idx,
                histogram_idx,
            )?);
        }
    }

    Ok(envelope)
}

fn normalize_series_identity(
    labels: Vec<crate::prom_remote::Label>,
    tenant_id: &str,
    series_idx: usize,
) -> Result<NormalizedSeriesIdentity, String> {
    let mut metric_name = None::<String>;
    let mut seen_names = BTreeSet::new();
    let mut normalized_labels = Vec::with_capacity(labels.len() + 1);

    for label in labels {
        if !seen_names.insert(label.name.clone()) {
            return Err(format!(
                "remote write timeseries index {series_idx} contains duplicate label '{}'",
                label.name
            ));
        }

        if label.name == "__name__" {
            validate_metric_name(
                &label.value,
                &format!("remote write timeseries index {series_idx} metric name"),
            )?;
            metric_name = Some(label.value);
            continue;
        }

        validate_label(
            &label.name,
            &label.value,
            &format!("remote write timeseries index {series_idx}"),
        )?;
        if label.name == tenant::TENANT_LABEL {
            return Err(format!(
                "remote write timeseries index {series_idx} label '{}' is reserved for server-managed tenant isolation",
                tenant::TENANT_LABEL
            ));
        }
        normalized_labels.push(Label::new(label.name, label.value));
    }

    let Some(metric) = metric_name else {
        return Err(format!(
            "remote write timeseries index {series_idx} is missing the __name__ label"
        ));
    };

    build_series_identity(
        metric,
        normalized_labels
            .into_iter()
            .map(|label| (label.name, label.value))
            .collect(),
        tenant_id,
        &format!("remote write timeseries index {series_idx}"),
    )
}

fn normalize_metadata_update(
    metadata: MetricMetadata,
    metadata_idx: usize,
) -> Result<NormalizedMetricMetadataUpdate, String> {
    let metric_type = MetricType::try_from(metadata.r#type).map_err(|_| {
        format!(
            "remote write metadata index {metadata_idx} has unsupported metric type {}",
            metadata.r#type
        )
    })?;
    build_metadata_update(
        metadata.metric_family_name,
        metric_type,
        metadata.help,
        metadata.unit,
        &format!("remote write metadata index {metadata_idx}"),
    )
}

fn normalize_exemplar(
    series: NormalizedSeriesIdentity,
    exemplar: crate::prom_remote::Exemplar,
    precision: TimestampPrecision,
    series_idx: usize,
    exemplar_idx: usize,
) -> Result<NormalizedExemplar, String> {
    let timestamp = convert_remote_write_timestamp(
        exemplar.timestamp,
        precision,
        &format!("remote write timeseries index {series_idx} exemplar index {exemplar_idx}"),
    )?;
    let labels = normalize_auxiliary_labels(
        exemplar.labels,
        &format!("remote write timeseries index {series_idx} exemplar index {exemplar_idx}"),
    )?;
    Ok(NormalizedExemplar {
        series,
        labels,
        timestamp,
        value: exemplar.value,
    })
}

fn normalize_histogram(
    series: NormalizedSeriesIdentity,
    histogram: crate::prom_remote::Histogram,
    precision: TimestampPrecision,
    series_idx: usize,
    histogram_idx: usize,
) -> Result<NormalizedHistogramSample, String> {
    let timestamp = convert_remote_write_timestamp(
        histogram.timestamp,
        precision,
        &format!("remote write timeseries index {series_idx} histogram index {histogram_idx}"),
    )?;
    let reset_hint = HistogramResetHint::try_from(histogram.reset_hint).map_err(|_| {
        format!(
            "remote write timeseries index {series_idx} histogram index {histogram_idx} has unsupported reset hint {}",
            histogram.reset_hint
        )
    })?;
    Ok(NormalizedHistogramSample {
        series,
        count: normalize_histogram_count(histogram.count),
        sum: histogram.sum,
        schema: histogram.schema,
        zero_threshold: histogram.zero_threshold,
        zero_count: normalize_histogram_zero_count(histogram.zero_count),
        negative_spans: histogram
            .negative_spans
            .into_iter()
            .map(normalize_bucket_span)
            .collect(),
        negative_deltas: histogram.negative_deltas,
        negative_counts: histogram.negative_counts,
        positive_spans: histogram
            .positive_spans
            .into_iter()
            .map(normalize_bucket_span)
            .collect(),
        positive_deltas: histogram.positive_deltas,
        positive_counts: histogram.positive_counts,
        reset_hint,
        timestamp,
        custom_values: histogram.custom_values,
    })
}

fn normalize_auxiliary_labels(
    labels: Vec<crate::prom_remote::Label>,
    context: &str,
) -> Result<Vec<Label>, String> {
    let mut seen_names = BTreeSet::new();
    let mut normalized = Vec::with_capacity(labels.len());
    for label in labels {
        if !seen_names.insert(label.name.clone()) {
            return Err(format!(
                "{context} contains duplicate label '{}'",
                label.name
            ));
        }
        validate_label(&label.name, &label.value, context)?;
        if label.name == tenant::TENANT_LABEL {
            return Err(format!(
                "{context} label '{}' is reserved for server-managed tenant isolation",
                tenant::TENANT_LABEL
            ));
        }
        normalized.push(Label::new(label.name, label.value));
    }
    normalized.sort();
    Ok(normalized)
}

pub(crate) fn build_series_identity(
    metric: impl Into<String>,
    labels: Vec<(String, String)>,
    tenant_id: &str,
    context: &str,
) -> Result<NormalizedSeriesIdentity, String> {
    let metric = metric.into();
    validate_metric_name(&metric, &format!("{context} metric name"))?;

    let mut seen_names = BTreeSet::new();
    let mut normalized_labels = Vec::with_capacity(labels.len() + 1);
    for (name, value) in labels {
        if !seen_names.insert(name.clone()) {
            return Err(format!("{context} contains duplicate label '{name}'"));
        }
        validate_label(&name, &value, context)?;
        if name == tenant::TENANT_LABEL {
            return Err(format!(
                "{context} label '{}' is reserved for server-managed tenant isolation",
                tenant::TENANT_LABEL
            ));
        }
        normalized_labels.push(Label::new(name, value));
    }

    normalized_labels.push(Label::new(tenant::TENANT_LABEL, tenant_id));
    normalized_labels.sort();
    Ok(NormalizedSeriesIdentity {
        metric,
        labels: normalized_labels,
    })
}

pub(crate) fn build_metadata_update(
    metric_family_name: impl Into<String>,
    metric_type: MetricType,
    help: impl Into<String>,
    unit: impl Into<String>,
    context: &str,
) -> Result<NormalizedMetricMetadataUpdate, String> {
    let metric_family_name = metric_family_name.into();
    validate_metric_name(
        &metric_family_name,
        &format!("{context} metric family name"),
    )?;
    Ok(NormalizedMetricMetadataUpdate {
        metric_family_name,
        metric_type,
        help: help.into(),
        unit: unit.into(),
    })
}

fn validate_metric_name(metric: &str, context: &str) -> Result<(), String> {
    if metric.is_empty() {
        return Err(format!("{context} must not be empty"));
    }
    if !is_prometheus_metric_name(metric) {
        return Err(format!(
            "{context} must match Prometheus metric name syntax [a-zA-Z_:][a-zA-Z0-9_:]*"
        ));
    }
    if metric.len() > MAX_METRIC_NAME_LEN {
        return Err(format!("{context} must be <= {MAX_METRIC_NAME_LEN} bytes"));
    }
    Ok(())
}

fn validate_label(name: &str, value: &str, context: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{context} label name must not be empty"));
    }
    if !is_prometheus_label_name(name) {
        return Err(format!(
            "{context} label '{name}' must match Prometheus label name syntax [a-zA-Z_][a-zA-Z0-9_]*"
        ));
    }
    if name.len() > MAX_LABEL_NAME_LEN {
        return Err(format!(
            "{context} label '{name}' exceeds the {MAX_LABEL_NAME_LEN}-byte name limit"
        ));
    }
    if value.len() > MAX_LABEL_VALUE_LEN {
        return Err(format!(
            "{context} label '{name}' exceeds the {MAX_LABEL_VALUE_LEN}-byte value limit"
        ));
    }
    Ok(())
}

fn is_prometheus_metric_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (is_ascii_alpha(first) || first == b'_' || first == b':')
        && bytes.all(|byte| is_ascii_alphanumeric(byte) || byte == b'_' || byte == b':')
}

fn is_prometheus_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (is_ascii_alpha(first) || first == b'_')
        && bytes.all(|byte| is_ascii_alphanumeric(byte) || byte == b'_')
}

fn is_ascii_alpha(byte: u8) -> bool {
    byte.is_ascii_alphabetic()
}

fn is_ascii_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn convert_remote_write_timestamp(
    timestamp_millis: i64,
    precision: TimestampPrecision,
    context: &str,
) -> Result<i64, String> {
    validate_timestamp_sanity(timestamp_millis, context)?;
    let millis = i128::from(timestamp_millis);
    let scaled = match precision {
        TimestampPrecision::Seconds => millis / 1_000,
        TimestampPrecision::Milliseconds => millis,
        TimestampPrecision::Microseconds => millis.saturating_mul(1_000),
        TimestampPrecision::Nanoseconds => millis.saturating_mul(1_000_000),
    };
    i64::try_from(scaled).map_err(|_| format!("{context} timestamp is out of range"))
}

fn validate_timestamp_sanity(timestamp: i64, context: &str) -> Result<(), String> {
    if timestamp == i64::MAX {
        return Err(format!(
            "{context} timestamp {timestamp} is out of supported range"
        ));
    }
    Ok(())
}

fn normalize_histogram_count(count: Option<histogram::Count>) -> Option<NormalizedHistogramCount> {
    match count {
        Some(histogram::Count::CountInt(value)) => Some(NormalizedHistogramCount::Int(value)),
        Some(histogram::Count::CountFloat(value)) => Some(NormalizedHistogramCount::Float(value)),
        None => None,
    }
}

fn normalize_histogram_zero_count(
    count: Option<histogram::ZeroCount>,
) -> Option<NormalizedHistogramCount> {
    match count {
        Some(histogram::ZeroCount::ZeroCountInt(value)) => {
            Some(NormalizedHistogramCount::Int(value))
        }
        Some(histogram::ZeroCount::ZeroCountFloat(value)) => {
            Some(NormalizedHistogramCount::Float(value))
        }
        None => None,
    }
}

fn normalize_bucket_span(span: crate::prom_remote::BucketSpan) -> NormalizedBucketSpan {
    NormalizedBucketSpan {
        offset: span.offset,
        length: span.length,
    }
}

fn normalized_histogram_count_to_tsink(count: NormalizedHistogramCount) -> HistogramCount {
    match count {
        NormalizedHistogramCount::Int(value) => HistogramCount::Int(value),
        NormalizedHistogramCount::Float(value) => HistogramCount::Float(value),
    }
}

fn normalized_bucket_span_to_tsink(span: NormalizedBucketSpan) -> HistogramBucketSpan {
    HistogramBucketSpan {
        offset: span.offset,
        length: span.length,
    }
}

fn normalized_reset_hint_to_tsink(reset_hint: HistogramResetHint) -> TsinkHistogramResetHint {
    match reset_hint {
        HistogramResetHint::Unknown => TsinkHistogramResetHint::Unknown,
        HistogramResetHint::Yes => TsinkHistogramResetHint::Yes,
        HistogramResetHint::No => TsinkHistogramResetHint::No,
        HistogramResetHint::Gauge => TsinkHistogramResetHint::Gauge,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_remote::{
        BucketSpan, Exemplar, Histogram, HistogramResetHint, Label as PromLabel, MetricMetadata,
        MetricType, Sample, TimeSeries,
    };
    use tsink::engine::series::SeriesRegistry;

    #[test]
    fn normalize_remote_write_request_scopes_and_sorts_labels() {
        let envelope = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![
                        PromLabel {
                            name: "region".to_string(),
                            value: "us-west".to_string(),
                        },
                        PromLabel {
                            name: "__name__".to_string(),
                            value: "cpu_usage".to_string(),
                        },
                        PromLabel {
                            name: "host".to_string(),
                            value: "server-a".to_string(),
                        },
                    ],
                    samples: vec![crate::prom_remote::Sample {
                        value: 11.5,
                        timestamp: 1_700_000_000_000,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect("normalization should succeed");

        assert_eq!(envelope.scalar_samples.len(), 1);
        assert_eq!(envelope.scalar_samples[0].series.metric, "cpu_usage");
        assert_eq!(
            envelope.scalar_samples[0].series.labels,
            vec![
                Label::new("__tsink_tenant__", "tenant-a"),
                Label::new("host", "server-a"),
                Label::new("region", "us-west"),
            ]
        );
    }

    #[test]
    fn normalize_remote_write_request_rejects_duplicate_labels() {
        let err = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![
                        PromLabel {
                            name: "__name__".to_string(),
                            value: "cpu_usage".to_string(),
                        },
                        PromLabel {
                            name: "host".to_string(),
                            value: "server-a".to_string(),
                        },
                        PromLabel {
                            name: "host".to_string(),
                            value: "server-b".to_string(),
                        },
                    ],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect_err("duplicate labels must be rejected");

        assert!(err.contains("duplicate label 'host'"));
    }

    #[test]
    fn normalize_remote_write_request_converts_timestamps_to_server_precision() {
        let envelope = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![PromLabel {
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    }],
                    samples: vec![Sample {
                        value: 11.5,
                        timestamp: 1_700_000_000_123,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Seconds,
        )
        .expect("normalization should succeed");

        assert_eq!(
            envelope.scalar_samples[0].data_point.timestamp,
            1_700_000_000
        );

        let envelope = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![PromLabel {
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    }],
                    samples: vec![Sample {
                        value: 11.5,
                        timestamp: 1_700_000_000_123,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Nanoseconds,
        )
        .expect("normalization should succeed");

        assert_eq!(
            envelope.scalar_samples[0].data_point.timestamp,
            1_700_000_000_123_000_000
        );
    }

    #[test]
    fn normalize_remote_write_request_accepts_empty_label_values() {
        let envelope = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![
                        PromLabel {
                            name: "__name__".to_string(),
                            value: "cpu_usage".to_string(),
                        },
                        PromLabel {
                            name: "optional".to_string(),
                            value: String::new(),
                        },
                    ],
                    samples: vec![Sample {
                        value: 11.5,
                        timestamp: 1_700_000_000_000,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect("normalization should succeed");

        assert!(envelope.scalar_samples[0]
            .series
            .labels
            .contains(&Label::new("optional", "")));
    }

    #[test]
    fn normalize_remote_write_request_rejects_invalid_prometheus_names() {
        let invalid_metric = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![PromLabel {
                        name: "__name__".to_string(),
                        value: "9cpu".to_string(),
                    }],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1_700_000_000_000,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect_err("invalid metric name should be rejected");
        assert!(invalid_metric.contains("Prometheus metric name syntax"));

        let invalid_label = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![
                        PromLabel {
                            name: "__name__".to_string(),
                            value: "cpu_usage".to_string(),
                        },
                        PromLabel {
                            name: "bad-label".to_string(),
                            value: "server-a".to_string(),
                        },
                    ],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1_700_000_000_000,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect_err("invalid label name should be rejected");
        assert!(invalid_label.contains("Prometheus label name syntax"));

        build_metadata_update("valid:metric_name", MetricType::Counter, "", "", "metadata")
            .expect("colon is valid in metric names");
    }

    #[test]
    fn build_series_identity_matches_embedded_registry_canonicalization() {
        let identity = build_series_identity(
            "cpu_usage",
            vec![
                ("region".to_string(), "us-west".to_string()),
                ("host".to_string(), "server-a".to_string()),
            ],
            "tenant-a",
            "test identity",
        )
        .expect("series identity should normalize");

        let registry = SeriesRegistry::new();
        let resolution = registry
            .resolve_or_insert(
                &identity.metric,
                &[
                    Label::new("region", "us-west"),
                    Label::new("__tsink_tenant__", "tenant-a"),
                    Label::new("host", "server-a"),
                ],
            )
            .expect("embedded registry should accept the same label set");
        let decoded = registry
            .decode_series_key(resolution.series_id)
            .expect("registered series should decode");

        assert_eq!(decoded.metric, identity.metric);
        assert_eq!(decoded.labels, identity.labels);
    }

    #[test]
    fn build_series_identity_duplicate_labels_match_embedded_validation() {
        let err = build_series_identity(
            "cpu_usage",
            vec![
                ("host".to_string(), "server-a".to_string()),
                ("host".to_string(), "server-b".to_string()),
            ],
            "tenant-a",
            "test identity",
        )
        .expect_err("duplicate labels must be rejected");
        assert!(err.contains("duplicate label 'host'"));

        let registry = SeriesRegistry::new();
        let err = registry
            .resolve_or_insert(
                "cpu_usage",
                &[
                    Label::new("__tsink_tenant__", "tenant-a"),
                    Label::new("host", "server-a"),
                    Label::new("host", "server-b"),
                ],
            )
            .expect_err("embedded registry must reject duplicate labels too");
        assert!(matches!(
            err,
            tsink::TsinkError::InvalidLabel(message)
                if message.contains("duplicate label 'host'")
        ));
    }

    #[test]
    fn normalize_remote_write_request_keeps_explicit_payloads() {
        let envelope = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![
                    TimeSeries {
                        labels: vec![PromLabel {
                            name: "__name__".to_string(),
                            value: "http_request_duration_seconds".to_string(),
                        }],
                        exemplars: vec![Exemplar {
                            labels: vec![PromLabel {
                                name: "trace_id".to_string(),
                                value: "abc123".to_string(),
                            }],
                            value: 0.42,
                            timestamp: 1_700_000_000_000,
                        }],
                        ..Default::default()
                    },
                    TimeSeries {
                        labels: vec![PromLabel {
                            name: "__name__".to_string(),
                            value: "rpc_duration_seconds".to_string(),
                        }],
                        histograms: vec![Histogram {
                            count: Some(histogram::Count::CountInt(5)),
                            sum: 1.5,
                            schema: 0,
                            zero_threshold: 0.0,
                            zero_count: Some(histogram::ZeroCount::ZeroCountInt(1)),
                            negative_spans: Vec::new(),
                            negative_deltas: Vec::new(),
                            negative_counts: Vec::new(),
                            positive_spans: vec![BucketSpan {
                                offset: 0,
                                length: 1,
                            }],
                            positive_deltas: vec![5],
                            positive_counts: Vec::new(),
                            reset_hint: HistogramResetHint::No as i32,
                            timestamp: 1_700_000_000_000,
                            custom_values: Vec::new(),
                        }],
                        ..Default::default()
                    },
                ],
                metadata: vec![MetricMetadata {
                    r#type: MetricType::Counter as i32,
                    metric_family_name: "http_requests_total".to_string(),
                    help: "Total requests.".to_string(),
                    unit: "requests".to_string(),
                }],
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect("normalization should succeed");

        assert_eq!(envelope.metadata_updates.len(), 1);
        assert_eq!(envelope.exemplars.len(), 1);
        assert_eq!(envelope.histogram_samples.len(), 1);
        let rows = envelope.into_rows();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].data_point().value_as_histogram().is_some());
    }

    #[test]
    fn normalize_remote_write_request_rejects_max_timestamp() {
        let err = normalize_remote_write_request(
            WriteRequest {
                timeseries: vec![TimeSeries {
                    labels: vec![PromLabel {
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    }],
                    samples: vec![Sample {
                        value: 11.5,
                        timestamp: i64::MAX,
                    }],
                    ..Default::default()
                }],
                metadata: Vec::new(),
            },
            "tenant-a",
            TimestampPrecision::Milliseconds,
        )
        .expect_err("max timestamp must be rejected");

        assert!(err.contains("out of supported range"));
    }
}
