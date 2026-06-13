//! OTLP HTTP/protobuf metric ingestion types and normalization.

use crate::prom_remote::MetricType;
use crate::prom_write::{
    NormalizedExemplar, NormalizedMetricMetadataUpdate, NormalizedScalarSample,
    NormalizedSeriesIdentity, NormalizedWriteEnvelope,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use tsink::{DataPoint, Label, TimestampPrecision};

#[allow(clippy::all, dead_code)]
pub mod generated {
    pub mod opentelemetry {
        pub mod proto {
            pub mod collector {
                pub mod metrics {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/opentelemetry.proto.collector.metrics.v1.rs"
                        ));
                    }
                }
            }
            pub mod common {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.common.v1.rs"
                    ));
                }
            }
            pub mod metrics {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.metrics.v1.rs"
                    ));
                }
            }
            pub mod resource {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.resource.v1.rs"
                    ));
                }
            }
        }
    }
}

pub use generated::opentelemetry::proto::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
pub use generated::opentelemetry::proto::common::v1::{
    any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
pub use generated::opentelemetry::proto::metrics::v1::{
    exemplar, metric, number_data_point, AggregationTemporality, Exemplar, Gauge, Histogram,
    Metric, NumberDataPoint, Sum, Summary,
};
pub use generated::opentelemetry::proto::resource::v1::Resource;

const OTLP_NO_RECORDED_VALUE_MASK: u32 = 0x1;
const TENANT_LABEL: &str = "__tenant_id";
const HISTOGRAM_BUCKET_LABEL: &str = "le";
const SUMMARY_QUANTILE_LABEL: &str = "quantile";
const OTEL_SCOPE_NAME_LABEL: &str = "otel_scope_name";
const OTEL_SCOPE_VERSION_LABEL: &str = "otel_scope_version";
const OTEL_TEMPORALITY_LABEL: &str = "otel_temporality";
const OTEL_METRIC_KIND_LABEL: &str = "otel_metric_kind";
const OTEL_MONOTONIC_LABEL: &str = "otel_monotonic";
const TRACE_ID_LABEL: &str = "trace_id";
const SPAN_ID_LABEL: &str = "span_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpMetricKind {
    Gauge,
    Sum,
    Histogram,
    Summary,
    ExponentialHistogram,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtlpNormalizationStats {
    pub gauges: usize,
    pub sums: usize,
    pub histograms: usize,
    pub summaries: usize,
    pub rejected_kind: Option<OtlpMetricKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpNormalizationError {
    pub message: String,
    pub stats: OtlpNormalizationStats,
}

impl fmt::Display for OtlpNormalizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for OtlpNormalizationError {}

impl From<String> for OtlpNormalizationError {
    fn from(message: String) -> Self {
        Self {
            message,
            stats: OtlpNormalizationStats::default(),
        }
    }
}

pub fn normalize_metrics_export_request(
    request: ExportMetricsServiceRequest,
    tenant_id: &str,
    precision: TimestampPrecision,
) -> Result<(NormalizedWriteEnvelope, OtlpNormalizationStats), OtlpNormalizationError> {
    let mut envelope = NormalizedWriteEnvelope::default();
    let mut metadata_updates = BTreeMap::<String, NormalizedMetricMetadataUpdate>::new();
    let mut stats = OtlpNormalizationStats::default();

    for (resource_idx, resource_metrics) in request.resource_metrics.into_iter().enumerate() {
        let resource_labels = normalize_resource_labels(resource_metrics.resource, resource_idx)?;
        for (scope_idx, scope_metrics) in resource_metrics.scope_metrics.into_iter().enumerate() {
            let scope_labels =
                normalize_scope_labels(scope_metrics.scope, resource_idx, scope_idx)?;
            let mut shared_labels =
                Vec::with_capacity(resource_labels.len().saturating_add(scope_labels.len()));
            shared_labels.extend(resource_labels.iter().cloned());
            shared_labels.extend(scope_labels.iter().cloned());
            shared_labels.sort();

            for (metric_idx, metric) in scope_metrics.metrics.into_iter().enumerate() {
                if let Err(err) = normalize_metric(
                    metric,
                    &shared_labels,
                    tenant_id,
                    precision,
                    resource_idx,
                    scope_idx,
                    metric_idx,
                    &mut envelope,
                    &mut metadata_updates,
                    &mut stats,
                ) {
                    return Err(OtlpNormalizationError {
                        message: err,
                        stats,
                    });
                }
            }
        }
    }

    envelope.metadata_updates = metadata_updates.into_values().collect();
    Ok((envelope, stats))
}

#[allow(clippy::too_many_arguments)]
fn normalize_metric(
    metric: Metric,
    shared_labels: &[Label],
    tenant_id: &str,
    precision: TimestampPrecision,
    resource_idx: usize,
    scope_idx: usize,
    metric_idx: usize,
    envelope: &mut NormalizedWriteEnvelope,
    metadata_updates: &mut BTreeMap<String, NormalizedMetricMetadataUpdate>,
    stats: &mut OtlpNormalizationStats,
) -> Result<(), String> {
    let context =
        format!("resource index {resource_idx} scope index {scope_idx} metric index {metric_idx}");
    let metric_name = normalize_metric_name(&metric.name, &context)?;
    match metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            let count = gauge.data_points.len();
            record_metadata(
                metadata_updates,
                &metric_name,
                MetricType::Gauge,
                metric.description,
                metric.unit,
            )?;
            if let Err(err) = normalize_gauge(
                &metric_name,
                gauge,
                shared_labels,
                tenant_id,
                precision,
                &context,
                envelope,
            ) {
                stats.rejected_kind = Some(OtlpMetricKind::Gauge);
                return Err(err);
            }
            stats.gauges = stats.gauges.saturating_add(count);
        }
        Some(metric::Data::Sum(sum)) => {
            let metric_type = if sum.is_monotonic {
                MetricType::Counter
            } else {
                MetricType::Gauge
            };
            record_metadata(
                metadata_updates,
                &metric_name,
                metric_type,
                metric.description,
                metric.unit,
            )?;
            let count = sum.data_points.len();
            if let Err(err) = normalize_sum(
                &metric_name,
                sum,
                shared_labels,
                tenant_id,
                precision,
                &context,
                envelope,
            ) {
                stats.rejected_kind = Some(OtlpMetricKind::Sum);
                return Err(err);
            }
            stats.sums = stats.sums.saturating_add(count);
        }
        Some(metric::Data::Histogram(histogram)) => {
            record_metadata(
                metadata_updates,
                &metric_name,
                MetricType::Histogram,
                metric.description,
                metric.unit,
            )?;
            let count = histogram.data_points.len();
            if let Err(err) = normalize_histogram(
                &metric_name,
                histogram,
                shared_labels,
                tenant_id,
                precision,
                &context,
                envelope,
            ) {
                stats.rejected_kind = Some(OtlpMetricKind::Histogram);
                return Err(err);
            }
            stats.histograms = stats.histograms.saturating_add(count);
        }
        Some(metric::Data::Summary(summary)) => {
            record_metadata(
                metadata_updates,
                &metric_name,
                MetricType::Summary,
                metric.description,
                metric.unit,
            )?;
            let count = summary.data_points.len();
            if let Err(err) = normalize_summary(
                &metric_name,
                summary,
                shared_labels,
                tenant_id,
                precision,
                &context,
                envelope,
            ) {
                stats.rejected_kind = Some(OtlpMetricKind::Summary);
                return Err(err);
            }
            stats.summaries = stats.summaries.saturating_add(count);
        }
        Some(metric::Data::ExponentialHistogram(_)) => {
            stats.rejected_kind = Some(OtlpMetricKind::ExponentialHistogram);
            return Err(format!(
                "{context} metric '{}' uses exponential histograms, which are not supported by /v1/metrics yet",
                metric.name
            ));
        }
        None => {
            return Err(format!(
                "{context} metric '{}' is missing a supported OTLP data payload",
                metric.name
            ));
        }
    }

    Ok(())
}

fn normalize_gauge(
    metric_name: &str,
    gauge: Gauge,
    shared_labels: &[Label],
    tenant_id: &str,
    precision: TimestampPrecision,
    context: &str,
    envelope: &mut NormalizedWriteEnvelope,
) -> Result<(), String> {
    for (point_idx, point) in gauge.data_points.into_iter().enumerate() {
        normalize_number_data_point(
            metric_name,
            point,
            shared_labels,
            tenant_id,
            precision,
            context,
            point_idx,
            &[],
            envelope,
        )?;
    }
    Ok(())
}

fn normalize_sum(
    metric_name: &str,
    sum: Sum,
    shared_labels: &[Label],
    tenant_id: &str,
    precision: TimestampPrecision,
    context: &str,
    envelope: &mut NormalizedWriteEnvelope,
) -> Result<(), String> {
    let temporality =
        AggregationTemporality::try_from(sum.aggregation_temporality).map_err(|_| {
            format!(
                "{context} metric '{metric_name}' has unsupported aggregation temporality {}",
                sum.aggregation_temporality
            )
        })?;
    if temporality != AggregationTemporality::Cumulative {
        return Err(format!(
            "{context} metric '{metric_name}' uses {:?} temporality, but /v1/metrics currently accepts OTLP sums only with cumulative temporality",
            temporality
        ));
    }

    let mut extra_labels = vec![
        Label::new(OTEL_METRIC_KIND_LABEL, "sum"),
        Label::new(OTEL_TEMPORALITY_LABEL, "cumulative"),
    ];
    if !sum.is_monotonic {
        extra_labels.push(Label::new(OTEL_MONOTONIC_LABEL, "false"));
    }
    extra_labels.sort();

    for (point_idx, point) in sum.data_points.into_iter().enumerate() {
        normalize_number_data_point(
            metric_name,
            point,
            shared_labels,
            tenant_id,
            precision,
            context,
            point_idx,
            &extra_labels,
            envelope,
        )?;
    }
    Ok(())
}

fn normalize_histogram(
    metric_name: &str,
    histogram: Histogram,
    shared_labels: &[Label],
    tenant_id: &str,
    precision: TimestampPrecision,
    context: &str,
    envelope: &mut NormalizedWriteEnvelope,
) -> Result<(), String> {
    let temporality =
        AggregationTemporality::try_from(histogram.aggregation_temporality).map_err(|_| {
            format!(
                "{context} metric '{metric_name}' has unsupported aggregation temporality {}",
                histogram.aggregation_temporality
            )
        })?;
    if temporality != AggregationTemporality::Cumulative {
        return Err(format!(
            "{context} metric '{metric_name}' uses {:?} temporality, but /v1/metrics currently accepts OTLP histograms only with cumulative temporality",
            temporality
        ));
    }

    let mut base_extra_labels = vec![
        Label::new(OTEL_METRIC_KIND_LABEL, "histogram"),
        Label::new(OTEL_TEMPORALITY_LABEL, "cumulative"),
    ];
    base_extra_labels.sort();

    for (point_idx, point) in histogram.data_points.into_iter().enumerate() {
        let point_context = format!("{context} histogram data point index {point_idx}");
        validate_flags(point.flags, &point_context)?;
        let timestamp = normalize_timestamp(point.time_unix_nano, precision, &point_context)?;
        if point.explicit_bounds.len().saturating_add(1) != point.bucket_counts.len() {
            return Err(format!(
                "{point_context} has {} explicit bounds but {} bucket counts; expected bucket_counts = explicit_bounds + 1",
                point.explicit_bounds.len(),
                point.bucket_counts.len()
            ));
        }
        let point_labels =
            normalize_point_labels(point.attributes, shared_labels, tenant_id, &point_context)?;
        let mut cumulative = 0f64;
        for (bucket_idx, bucket_count) in point.bucket_counts.iter().enumerate() {
            cumulative += *bucket_count as f64;
            let boundary = if bucket_idx < point.explicit_bounds.len() {
                let bound = point.explicit_bounds[bucket_idx];
                if !bound.is_finite() {
                    return Err(format!(
                        "{point_context} bucket bound index {bucket_idx} must be finite"
                    ));
                }
                bound.to_string()
            } else {
                "+Inf".to_string()
            };
            let identity = series_identity(
                &format!("{metric_name}_bucket"),
                &point_labels,
                &base_extra_labels,
                Some(Label::new(HISTOGRAM_BUCKET_LABEL, boundary)),
                tenant_id,
                &point_context,
            )?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series: identity,
                data_point: DataPoint::new(timestamp, cumulative),
            });
        }

        let count_identity = series_identity(
            &format!("{metric_name}_count"),
            &point_labels,
            &base_extra_labels,
            None,
            tenant_id,
            &point_context,
        )?;
        envelope.scalar_samples.push(NormalizedScalarSample {
            series: count_identity.clone(),
            data_point: DataPoint::new(timestamp, point.count as f64),
        });

        if let Some(sum) = point.sum {
            let sum_identity = series_identity(
                &format!("{metric_name}_sum"),
                &point_labels,
                &base_extra_labels,
                None,
                tenant_id,
                &point_context,
            )?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series: sum_identity,
                data_point: DataPoint::new(timestamp, sum),
            });
        }
        if let Some(min) = point.min {
            let min_identity = series_identity(
                &format!("{metric_name}_min"),
                &point_labels,
                &base_extra_labels,
                None,
                tenant_id,
                &point_context,
            )?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series: min_identity,
                data_point: DataPoint::new(timestamp, min),
            });
        }
        if let Some(max) = point.max {
            let max_identity = series_identity(
                &format!("{metric_name}_max"),
                &point_labels,
                &base_extra_labels,
                None,
                tenant_id,
                &point_context,
            )?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series: max_identity,
                data_point: DataPoint::new(timestamp, max),
            });
        }

        let exemplar_identity = series_identity(
            metric_name,
            &point_labels,
            &base_extra_labels,
            None,
            tenant_id,
            &point_context,
        )?;
        normalize_exemplars(
            exemplar_identity,
            point.exemplars,
            precision,
            &point_context,
            envelope,
        )?;
    }

    Ok(())
}

fn normalize_summary(
    metric_name: &str,
    summary: Summary,
    shared_labels: &[Label],
    tenant_id: &str,
    precision: TimestampPrecision,
    context: &str,
    envelope: &mut NormalizedWriteEnvelope,
) -> Result<(), String> {
    let mut base_extra_labels = vec![Label::new(OTEL_METRIC_KIND_LABEL, "summary")];
    base_extra_labels.sort();

    for (point_idx, point) in summary.data_points.into_iter().enumerate() {
        let point_context = format!("{context} summary data point index {point_idx}");
        validate_flags(point.flags, &point_context)?;
        let timestamp = normalize_timestamp(point.time_unix_nano, precision, &point_context)?;
        let point_labels =
            normalize_point_labels(point.attributes, shared_labels, tenant_id, &point_context)?;

        for (quantile_idx, quantile) in point.quantile_values.into_iter().enumerate() {
            if !(0.0..=1.0).contains(&quantile.quantile) {
                return Err(format!(
                    "{point_context} quantile index {quantile_idx} must be between 0 and 1 inclusive"
                ));
            }
            let identity = series_identity(
                metric_name,
                &point_labels,
                &base_extra_labels,
                Some(Label::new(
                    SUMMARY_QUANTILE_LABEL,
                    quantile.quantile.to_string(),
                )),
                tenant_id,
                &point_context,
            )?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series: identity,
                data_point: DataPoint::new(timestamp, quantile.value),
            });
        }

        let count_identity = series_identity(
            &format!("{metric_name}_count"),
            &point_labels,
            &base_extra_labels,
            None,
            tenant_id,
            &point_context,
        )?;
        envelope.scalar_samples.push(NormalizedScalarSample {
            series: count_identity,
            data_point: DataPoint::new(timestamp, point.count as f64),
        });

        let sum_identity = series_identity(
            &format!("{metric_name}_sum"),
            &point_labels,
            &base_extra_labels,
            None,
            tenant_id,
            &point_context,
        )?;
        envelope.scalar_samples.push(NormalizedScalarSample {
            series: sum_identity,
            data_point: DataPoint::new(timestamp, point.sum),
        });
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_number_data_point(
    metric_name: &str,
    point: NumberDataPoint,
    shared_labels: &[Label],
    tenant_id: &str,
    precision: TimestampPrecision,
    context: &str,
    point_idx: usize,
    extra_labels: &[Label],
    envelope: &mut NormalizedWriteEnvelope,
) -> Result<(), String> {
    let point_context = format!("{context} data point index {point_idx}");
    validate_flags(point.flags, &point_context)?;
    let timestamp = normalize_timestamp(point.time_unix_nano, precision, &point_context)?;
    let value = match point.value {
        Some(number_data_point::Value::AsDouble(value)) => value,
        Some(number_data_point::Value::AsInt(value)) => value as f64,
        None => {
            return Err(format!(
                "{point_context} is missing an integer or double value"
            ))
        }
    };
    let point_labels =
        normalize_point_labels(point.attributes, shared_labels, tenant_id, &point_context)?;
    let identity = series_identity(
        metric_name,
        &point_labels,
        extra_labels,
        None,
        tenant_id,
        &point_context,
    )?;
    envelope.scalar_samples.push(NormalizedScalarSample {
        series: identity.clone(),
        data_point: DataPoint::new(timestamp, value),
    });
    normalize_exemplars(
        identity,
        point.exemplars,
        precision,
        &point_context,
        envelope,
    )?;
    Ok(())
}

fn normalize_exemplars(
    series: NormalizedSeriesIdentity,
    exemplars: Vec<Exemplar>,
    precision: TimestampPrecision,
    context: &str,
    envelope: &mut NormalizedWriteEnvelope,
) -> Result<(), String> {
    for (exemplar_idx, exemplar) in exemplars.into_iter().enumerate() {
        let exemplar_context = format!("{context} exemplar index {exemplar_idx}");
        let timestamp = normalize_timestamp(exemplar.time_unix_nano, precision, &exemplar_context)?;
        let value = match exemplar.value {
            Some(exemplar::Value::AsDouble(value)) => value,
            Some(exemplar::Value::AsInt(value)) => value as f64,
            None => {
                return Err(format!(
                    "{exemplar_context} is missing an integer or double value"
                ))
            }
        };
        let mut labels =
            normalize_attribute_labels(exemplar.filtered_attributes, None, &exemplar_context)?;
        if !exemplar.trace_id.is_empty() {
            labels.push(Label::new(TRACE_ID_LABEL, hex_encode(&exemplar.trace_id)));
        }
        if !exemplar.span_id.is_empty() {
            labels.push(Label::new(SPAN_ID_LABEL, hex_encode(&exemplar.span_id)));
        }
        sort_and_validate_labels(&mut labels, &exemplar_context, true)?;
        envelope.exemplars.push(NormalizedExemplar {
            series: series.clone(),
            labels,
            timestamp,
            value,
        });
    }
    Ok(())
}

fn normalize_resource_labels(
    resource: Option<Resource>,
    resource_idx: usize,
) -> Result<Vec<Label>, String> {
    let Some(resource) = resource else {
        return Ok(Vec::new());
    };
    let context = format!("resource index {resource_idx}");
    let mut labels = normalize_attribute_labels(resource.attributes, Some("resource_"), &context)?;
    if resource.dropped_attributes_count > 0 {
        labels.push(Label::new(
            "otel_resource_dropped_attributes_count",
            resource.dropped_attributes_count.to_string(),
        ));
    }
    sort_and_validate_labels(&mut labels, &context, false)?;
    Ok(labels)
}

fn normalize_scope_labels(
    scope: Option<InstrumentationScope>,
    resource_idx: usize,
    scope_idx: usize,
) -> Result<Vec<Label>, String> {
    let Some(scope) = scope else {
        return Ok(Vec::new());
    };
    let context = format!("resource index {resource_idx} scope index {scope_idx}");
    let mut labels = normalize_attribute_labels(scope.attributes, Some("scope_"), &context)?;
    if !scope.name.is_empty() {
        labels.push(Label::new(OTEL_SCOPE_NAME_LABEL, scope.name));
    }
    if !scope.version.is_empty() {
        labels.push(Label::new(OTEL_SCOPE_VERSION_LABEL, scope.version));
    }
    if scope.dropped_attributes_count > 0 {
        labels.push(Label::new(
            "otel_scope_dropped_attributes_count",
            scope.dropped_attributes_count.to_string(),
        ));
    }
    sort_and_validate_labels(&mut labels, &context, false)?;
    Ok(labels)
}

fn normalize_point_labels(
    attributes: Vec<KeyValue>,
    shared_labels: &[Label],
    tenant_id: &str,
    context: &str,
) -> Result<Vec<Label>, String> {
    let mut labels = Vec::with_capacity(
        shared_labels
            .len()
            .saturating_add(attributes.len())
            .saturating_add(1),
    );
    labels.extend(shared_labels.iter().cloned());
    labels.extend(normalize_attribute_labels(attributes, None, context)?);
    labels.push(Label::new(TENANT_LABEL, tenant_id));
    sort_and_validate_labels(&mut labels, context, false)?;
    Ok(labels)
}

fn normalize_attribute_labels(
    attributes: Vec<KeyValue>,
    prefix: Option<&str>,
    context: &str,
) -> Result<Vec<Label>, String> {
    let mut labels = Vec::with_capacity(attributes.len());
    for attribute in attributes {
        if attribute.key.is_empty() {
            return Err(format!("{context} contains an attribute with an empty key"));
        }
        let value = any_value_to_string(attribute.value, context)?;
        let mut name = prefix.unwrap_or_default().to_string();
        name.push_str(&escape_prom_ident(&attribute.key));
        labels.push(Label::new(name, value));
    }
    Ok(labels)
}

fn series_identity(
    metric: &str,
    point_labels: &[Label],
    extra_labels: &[Label],
    extra_label: Option<Label>,
    tenant_id: &str,
    context: &str,
) -> Result<NormalizedSeriesIdentity, String> {
    let mut labels = Vec::with_capacity(
        point_labels
            .len()
            .saturating_add(extra_labels.len())
            .saturating_add(usize::from(extra_label.is_some())),
    );
    labels.extend(point_labels.iter().cloned());
    labels.extend(extra_labels.iter().cloned());
    if let Some(label) = extra_label {
        labels.push(label);
    }
    sort_and_validate_labels(&mut labels, context, false)?;
    if !labels
        .iter()
        .any(|label| label.name == TENANT_LABEL && label.value == tenant_id)
    {
        return Err(format!(
            "{context} is missing the server-managed tenant label after normalization"
        ));
    }
    Ok(NormalizedSeriesIdentity {
        metric: metric.to_string(),
        labels,
    })
}

fn record_metadata(
    metadata_updates: &mut BTreeMap<String, NormalizedMetricMetadataUpdate>,
    metric_family_name: &str,
    metric_type: MetricType,
    help: String,
    unit: String,
) -> Result<(), String> {
    let candidate = NormalizedMetricMetadataUpdate {
        metric_family_name: metric_family_name.to_string(),
        metric_type,
        help,
        unit,
    };
    match metadata_updates.get(metric_family_name) {
        Some(existing)
            if existing.metric_type != candidate.metric_type
                || existing.help != candidate.help
                || existing.unit != candidate.unit =>
        {
            Err(format!(
                "OTLP payload defines conflicting metadata for metric family '{metric_family_name}'"
            ))
        }
        Some(_) => Ok(()),
        None => {
            metadata_updates.insert(metric_family_name.to_string(), candidate);
            Ok(())
        }
    }
}

fn any_value_to_string(value: Option<AnyValue>, context: &str) -> Result<String, String> {
    let Some(value) = value.and_then(|inner| inner.value) else {
        return Err(format!("{context} contains an attribute without a value"));
    };
    match value {
        any_value::Value::StringValue(value) => Ok(value),
        any_value::Value::BoolValue(value) => Ok(value.to_string()),
        any_value::Value::IntValue(value) => Ok(value.to_string()),
        any_value::Value::DoubleValue(value) => Ok(format_float(value)),
        any_value::Value::ArrayValue(value) => serde_json::to_string(&array_value_to_json(value))
            .map_err(|err| format!("{context} array attribute JSON encode failed: {err}")),
        any_value::Value::KvlistValue(value) => serde_json::to_string(&kvlist_value_to_json(value))
            .map_err(|err| format!("{context} map attribute JSON encode failed: {err}")),
        any_value::Value::BytesValue(value) => {
            Ok(format!("base64:{}", BASE64_STANDARD.encode(value)))
        }
    }
}

fn array_value_to_json(value: ArrayValue) -> JsonValue {
    JsonValue::Array(
        value
            .values
            .into_iter()
            .map(any_value_to_json)
            .collect::<Vec<_>>(),
    )
}

fn kvlist_value_to_json(value: KeyValueList) -> JsonValue {
    let mut map = JsonMap::new();
    for entry in value.values {
        map.insert(
            entry.key,
            entry
                .value
                .map(any_value_to_json)
                .unwrap_or(JsonValue::Null),
        );
    }
    JsonValue::Object(map)
}

fn any_value_to_json(value: AnyValue) -> JsonValue {
    match value.value {
        Some(any_value::Value::StringValue(value)) => JsonValue::String(value),
        Some(any_value::Value::BoolValue(value)) => JsonValue::Bool(value),
        Some(any_value::Value::IntValue(value)) => JsonValue::Number(value.into()),
        Some(any_value::Value::DoubleValue(value)) => json_number(value),
        Some(any_value::Value::ArrayValue(value)) => array_value_to_json(value),
        Some(any_value::Value::KvlistValue(value)) => kvlist_value_to_json(value),
        Some(any_value::Value::BytesValue(value)) => {
            JsonValue::String(format!("base64:{}", BASE64_STANDARD.encode(value)))
        }
        None => JsonValue::Null,
    }
}

fn json_number(value: f64) -> JsonValue {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or_else(|| JsonValue::String(format_float(value)))
}

fn normalize_metric_name(metric: &str, context: &str) -> Result<String, String> {
    if metric.is_empty() {
        return Err(format!("{context} metric name must not be empty"));
    }
    let escaped = escape_prom_ident(metric);
    if escaped.is_empty() {
        return Err(format!(
            "{context} metric name '{metric}' could not be normalized"
        ));
    }
    Ok(escaped)
}

fn normalize_timestamp(
    time_unix_nano: u64,
    precision: TimestampPrecision,
    context: &str,
) -> Result<i64, String> {
    if time_unix_nano == 0 {
        return Err(format!(
            "{context} timestamp must be a non-zero Unix nanosecond value"
        ));
    }
    let reduced = match precision {
        TimestampPrecision::Seconds => time_unix_nano / 1_000_000_000,
        TimestampPrecision::Milliseconds => time_unix_nano / 1_000_000,
        TimestampPrecision::Microseconds => time_unix_nano / 1_000,
        TimestampPrecision::Nanoseconds => time_unix_nano,
    };
    i64::try_from(reduced).map_err(|_| format!("{context} timestamp is out of range"))
}

fn validate_flags(flags: u32, context: &str) -> Result<(), String> {
    if flags & OTLP_NO_RECORDED_VALUE_MASK != 0 {
        return Err(format!(
            "{context} uses DataPointFlags.NO_RECORDED_VALUE, which /v1/metrics does not support"
        ));
    }
    if flags & !OTLP_NO_RECORDED_VALUE_MASK != 0 {
        return Err(format!(
            "{context} contains unsupported OTLP data point flags value {flags}"
        ));
    }
    Ok(())
}

fn sort_and_validate_labels(
    labels: &mut [Label],
    context: &str,
    allow_trace_labels: bool,
) -> Result<(), String> {
    let mut seen_names = BTreeSet::new();
    for label in labels.iter() {
        if label.name == TENANT_LABEL && !allow_trace_labels {
            // Tenant label is injected by the server and valid here.
        } else if label.name == TENANT_LABEL {
            return Err(format!(
                "{context} label '{TENANT_LABEL}' is reserved for server-managed tenant isolation"
            ));
        }
        validate_label(&label.name, &label.value, context)?;
        if !seen_names.insert(label.name.clone()) {
            return Err(format!(
                "{context} contains duplicate label '{}'",
                label.name
            ));
        }
    }
    labels.sort();
    Ok(())
}

fn validate_label(name: &str, value: &str, context: &str) -> Result<(), String> {
    let max_label_name_len = tsink::label::MAX_LABEL_NAME_LEN;
    let max_label_value_len = tsink::label::MAX_LABEL_VALUE_LEN;
    if name.is_empty() {
        return Err(format!("{context} label name must not be empty"));
    }
    if name.len() > max_label_name_len {
        return Err(format!(
            "{context} label '{name}' exceeds the {max_label_name_len}-byte name limit"
        ));
    }
    if value.len() > max_label_value_len {
        return Err(format!(
            "{context} label '{name}' exceeds the {max_label_value_len}-byte value limit"
        ));
    }
    Ok(())
}

fn escape_prom_ident(raw: &str) -> String {
    let mut out = String::new();
    for byte in raw.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => out.push(byte as char),
            b'_' => out.push_str("__"),
            _ => {
                out.push_str("_x");
                out.push_str(&format!("{byte:02x}"));
                out.push('_');
            }
        }
    }
    if out.is_empty() {
        return out;
    }
    match out.as_bytes()[0] {
        b'a'..=b'z' | b'A'..=b'Z' | b'_' => out,
        _ => format!("_x{:02x}_{}", raw.as_bytes()[0], &out[1..]),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::generated::opentelemetry::proto::metrics::v1::{
        HistogramDataPoint, ResourceMetrics, ScopeMetrics, SummaryDataPoint, ValueAtQuantile,
    };

    fn string_attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn int_attr(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(value)),
            }),
        }
    }

    #[test]
    fn normalizes_gauge_sum_histogram_summary_and_exemplars() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "checkout")],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "otel.scope".to_string(),
                        version: "1.2.3".to_string(),
                        attributes: vec![string_attr("scope.attr", "yes")],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![
                        Metric {
                            name: "system.cpu.time".to_string(),
                            description: "CPU time".to_string(),
                            unit: "s".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![string_attr("cpu", "0")],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_123_000_000,
                                    value: Some(number_data_point::Value::AsDouble(12.5)),
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                            })),
                        },
                        Metric {
                            name: "http.server.active_requests".to_string(),
                            description: "Active".to_string(),
                            unit: "{request}".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![string_attr("method", "GET")],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_124_000_000,
                                    value: Some(number_data_point::Value::AsInt(5)),
                                    exemplars: vec![Exemplar {
                                        filtered_attributes: vec![string_attr(
                                            "trace.role",
                                            "frontend",
                                        )],
                                        time_unix_nano: 1_700_000_000_124_000_000,
                                        value: Some(exemplar::Value::AsInt(5)),
                                        span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                                        trace_id: vec![
                                            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                        ],
                                    }],
                                    flags: 0,
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                                is_monotonic: false,
                            })),
                        },
                        Metric {
                            name: "http.server.duration".to_string(),
                            description: "Duration".to_string(),
                            unit: "ms".to_string(),
                            data: Some(metric::Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: vec![string_attr("route", "/")],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_125_000_000,
                                    count: 6,
                                    sum: Some(48.0),
                                    bucket_counts: vec![2, 3, 1],
                                    explicit_bounds: vec![10.0, 20.0],
                                    exemplars: vec![],
                                    flags: 0,
                                    min: Some(2.0),
                                    max: Some(22.0),
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            })),
                        },
                        Metric {
                            name: "rpc.latency".to_string(),
                            description: "Latency summary".to_string(),
                            unit: "ms".to_string(),
                            data: Some(metric::Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint {
                                    attributes: vec![int_attr("status", 200)],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_126_000_000,
                                    count: 3,
                                    sum: 44.0,
                                    quantile_values: vec![
                                        ValueAtQuantile {
                                            quantile: 0.5,
                                            value: 12.0,
                                        },
                                        ValueAtQuantile {
                                            quantile: 0.9,
                                            value: 20.0,
                                        },
                                    ],
                                    flags: 0,
                                }],
                            })),
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (envelope, stats) =
            normalize_metrics_export_request(request, "tenant-a", TimestampPrecision::Milliseconds)
                .expect("normalization should succeed");

        assert_eq!(stats.gauges, 1);
        assert_eq!(stats.sums, 1);
        assert_eq!(stats.histograms, 1);
        assert_eq!(stats.summaries, 1);
        assert_eq!(envelope.exemplars.len(), 1);
        assert_eq!(envelope.metadata_updates.len(), 4);
        assert!(envelope
            .scalar_samples
            .iter()
            .any(|sample| sample.series.metric == "system_x2e_cpu_x2e_time"));
        assert!(envelope
            .scalar_samples
            .iter()
            .any(|sample| sample.series.metric == "http_x2e_server_x2e_duration_bucket"));
        assert!(envelope
            .scalar_samples
            .iter()
            .any(|sample| sample.series.metric == "http_x2e_server_x2e_duration_count"));
        assert!(envelope
            .scalar_samples
            .iter()
            .any(|sample| sample.series.metric == "rpc_x2e_latency_sum"));
        let non_monotonic_sum = envelope
            .scalar_samples
            .iter()
            .find(|sample| sample.series.metric == "http_x2e_server_x2e_active__requests")
            .expect("sum sample should be present");
        assert!(non_monotonic_sum
            .series
            .labels
            .iter()
            .any(|label| label.name == OTEL_MONOTONIC_LABEL && label.value == "false"));
        assert!(non_monotonic_sum
            .series
            .labels
            .iter()
            .any(|label| label.name == TENANT_LABEL && label.value == "tenant-a"));
    }

    #[test]
    fn rejects_delta_histograms() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "delta.hist".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1,
                                time_unix_nano: 5,
                                count: 1,
                                sum: Some(1.0),
                                bucket_counts: vec![1],
                                explicit_bounds: vec![],
                                exemplars: vec![],
                                flags: 0,
                                min: None,
                                max: None,
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let err =
            normalize_metrics_export_request(request, "tenant-a", TimestampPrecision::Milliseconds)
                .expect_err("delta histograms must be rejected");
        assert!(err.to_string().contains("cumulative temporality"));
    }
}
