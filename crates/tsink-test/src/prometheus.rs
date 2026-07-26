//! Optional Prometheus protocol fixture helpers.
//!
//! Enable the `prometheus` feature to build real protobuf/Snappy remote-write request bodies
//! without depending on the `tsink-server` binary or an HTTP runtime.

use std::collections::BTreeSet;

use prost::Message;
use snap::raw::Encoder as SnappyEncoder;
use tsink::{Label, TsinkError};

pub use tsink_protocol::prometheus::WriteRequest;
use tsink_protocol::prometheus::{Label as ProtocolLabel, Sample, TimeSeries};

/// One Prometheus remote-write scalar sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PrometheusRemoteWriteSample {
    /// Sample value.
    pub value: f64,
    /// Unix timestamp in milliseconds, as required by Prometheus remote write.
    pub timestamp_millis: i64,
}

impl PrometheusRemoteWriteSample {
    /// Creates one explicit-time scalar sample.
    #[must_use]
    pub const fn new(timestamp_millis: i64, value: f64) -> Self {
        Self {
            value,
            timestamp_millis,
        }
    }
}

/// One Prometheus remote-write series.
#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusRemoteWriteSeries {
    /// Metric name encoded as the required `__name__` label.
    pub metric: String,
    /// Non-`__name__` metric labels.
    pub labels: Vec<Label>,
    /// Scalar samples for this series.
    pub samples: Vec<PrometheusRemoteWriteSample>,
}

impl PrometheusRemoteWriteSeries {
    /// Creates one series description.
    #[must_use]
    pub fn new(
        metric: impl Into<String>,
        labels: Vec<Label>,
        samples: Vec<PrometheusRemoteWriteSample>,
    ) -> Self {
        Self {
            metric: metric.into(),
            labels,
            samples,
        }
    }
}

/// Endpoint-ready Prometheus remote-write request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrometheusRemoteWritePayload {
    body: Vec<u8>,
    protobuf_bytes: usize,
    series: usize,
    samples: usize,
}

impl PrometheusRemoteWritePayload {
    /// Snappy-compressed protobuf body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Consumes the payload and returns its Snappy-compressed protobuf body.
    #[must_use]
    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    /// Required HTTP `Content-Type` value.
    #[must_use]
    pub const fn content_type(&self) -> &'static str {
        "application/x-protobuf"
    }

    /// Required HTTP `Content-Encoding` value.
    #[must_use]
    pub const fn content_encoding(&self) -> &'static str {
        "snappy"
    }

    /// Size of the protobuf message before Snappy compression.
    #[must_use]
    pub const fn protobuf_bytes(&self) -> usize {
        self.protobuf_bytes
    }

    /// Number of encoded time series.
    #[must_use]
    pub const fn series(&self) -> usize {
        self.series
    }

    /// Number of encoded scalar samples.
    #[must_use]
    pub const fn samples(&self) -> usize {
        self.samples
    }
}

/// Builds the real Prometheus protobuf request model from focused fixture series.
///
/// The helper adds `__name__`, rejects a caller-supplied duplicate, rejects duplicate label
/// names, and sorts remaining labels by name/value for deterministic payloads. To construct
/// intentionally malformed protocol fixtures, instantiate the re-exported [`WriteRequest`]
/// directly and pass it to [`encode_prometheus_remote_write_request`].
pub fn prometheus_remote_write_request(
    series: &[PrometheusRemoteWriteSeries],
) -> tsink::Result<WriteRequest> {
    let mut timeseries = Vec::with_capacity(series.len());
    for input in series {
        if input.metric.is_empty() {
            return Err(TsinkError::InvalidConfiguration(
                "tsink-test remote-write metric name may not be empty".to_string(),
            ));
        }

        let mut seen_names = BTreeSet::new();
        let mut labels = Vec::with_capacity(input.labels.len().saturating_add(1));
        labels.push(ProtocolLabel {
            name: "__name__".to_string(),
            value: input.metric.clone(),
        });
        for label in &input.labels {
            if label.name == "__name__" {
                return Err(TsinkError::InvalidConfiguration(
                    "tsink-test remote-write labels may not contain `__name__`".to_string(),
                ));
            }
            if !seen_names.insert(label.name.as_str()) {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "tsink-test remote-write label name {:?} is duplicated",
                    label.name
                )));
            }
            labels.push(ProtocolLabel {
                name: label.name.clone(),
                value: label.value.clone(),
            });
        }
        labels[1..].sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.value.cmp(&right.value))
        });

        timeseries.push(TimeSeries {
            labels,
            samples: input
                .samples
                .iter()
                .map(|sample| Sample {
                    value: sample.value,
                    timestamp: sample.timestamp_millis,
                })
                .collect(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
        });
    }

    Ok(WriteRequest {
        timeseries,
        metadata: Vec::new(),
    })
}

/// Builds and encodes an endpoint-ready Prometheus remote-write payload.
pub fn prometheus_remote_write_payload(
    series: &[PrometheusRemoteWriteSeries],
) -> tsink::Result<PrometheusRemoteWritePayload> {
    let request = prometheus_remote_write_request(series)?;
    encode_prometheus_remote_write_request(&request)
}

/// Encodes any real Prometheus [`WriteRequest`] as an endpoint-ready Snappy protobuf body.
///
/// This lower-level entry point intentionally permits malformed semantic content so negative
/// protocol tests can exercise the server's actual validation path.
pub fn encode_prometheus_remote_write_request(
    request: &WriteRequest,
) -> tsink::Result<PrometheusRemoteWritePayload> {
    let protobuf = request.encode_to_vec();
    let body = SnappyEncoder::new()
        .compress_vec(&protobuf)
        .map_err(|error| TsinkError::Other(format!("encode remote-write Snappy body: {error}")))?;
    Ok(PrometheusRemoteWritePayload {
        body,
        protobuf_bytes: protobuf.len(),
        series: request.timeseries.len(),
        samples: request
            .timeseries
            .iter()
            .map(|series| series.samples.len())
            .sum(),
    })
}
