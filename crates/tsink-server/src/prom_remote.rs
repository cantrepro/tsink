//! Generated Prometheus remote read/write protobuf types.
//!
//! Vendored schema sources:
//! - `proto/prometheus/remote.proto`
//! - `proto/prometheus/types.proto`
//! - `proto/gogoproto/gogo.proto`

#[allow(clippy::all, dead_code)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
}

#[allow(unused_imports)]
pub use generated::chunk;
#[allow(unused_imports)]
pub use generated::chunk::Encoding as ChunkEncoding;
#[allow(unused_imports)]
pub use generated::histogram;
#[allow(unused_imports)]
pub use generated::histogram::ResetHint as HistogramResetHint;
#[allow(unused_imports)]
pub use generated::label_matcher;
pub use generated::label_matcher::Type as MatcherType;
#[allow(unused_imports)]
pub use generated::metric_metadata;
pub use generated::metric_metadata::MetricType;
#[allow(unused_imports)]
pub use generated::read_request;
pub use generated::read_request::ResponseType as ReadResponseType;
#[allow(unused_imports)]
pub use generated::{
    BucketSpan, Chunk, ChunkedReadResponse, ChunkedSeries, Exemplar, Histogram, Label,
    LabelMatcher, Labels, MetricMetadata, Query, QueryResult, ReadHints, ReadRequest, ReadResponse,
    Sample, TimeSeries, WriteRequest,
};

#[cfg(test)]
mod tests {
    use super::{
        histogram, BucketSpan, Exemplar, Histogram, HistogramResetHint, Label, LabelMatcher,
        MatcherType, MetricMetadata, MetricType, Query, QueryResult, ReadHints, ReadRequest,
        ReadResponse, ReadResponseType, Sample, TimeSeries, WriteRequest,
    };
    use prost::Message;
    use serde_json::{json, Value};
    use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
    use std::fs;
    use std::path::{Path, PathBuf};

    enum FixtureMessage {
        WriteRequest(WriteRequest),
        ReadRequest(ReadRequest),
        ReadResponse(ReadResponse),
    }

    struct FixtureCase {
        name: &'static str,
        message: FixtureMessage,
    }

    impl FixtureCase {
        fn expectation_path(&self) -> PathBuf {
            fixture_dir().join(format!("{}.json", self.name))
        }
    }

    fn fixture_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/prom_remote")
    }

    fn fixture_cases() -> Vec<FixtureCase> {
        vec![
            FixtureCase {
                name: "write_scalar",
                message: FixtureMessage::WriteRequest(WriteRequest {
                    timeseries: vec![TimeSeries {
                        labels: vec![
                            Label {
                                name: "__name__".to_string(),
                                value: "cpu_usage_seconds_total".to_string(),
                            },
                            Label {
                                name: "job".to_string(),
                                value: "api".to_string(),
                            },
                            Label {
                                name: "instance".to_string(),
                                value: "node-a".to_string(),
                            },
                        ],
                        samples: vec![
                            Sample {
                                value: 123.5,
                                timestamp: 1_700_000_000_000,
                            },
                            Sample {
                                value: 124.0,
                                timestamp: 1_700_000_060_000,
                            },
                        ],
                        ..Default::default()
                    }],
                    metadata: Vec::new(),
                }),
            },
            FixtureCase {
                name: "write_metadata",
                message: FixtureMessage::WriteRequest(WriteRequest {
                    timeseries: Vec::new(),
                    metadata: vec![MetricMetadata {
                        r#type: MetricType::Counter as i32,
                        metric_family_name: "http_requests_total".to_string(),
                        help: "Total HTTP requests processed.".to_string(),
                        unit: "requests".to_string(),
                    }],
                }),
            },
            FixtureCase {
                name: "write_exemplar",
                message: FixtureMessage::WriteRequest(WriteRequest {
                    timeseries: vec![TimeSeries {
                        labels: vec![
                            Label {
                                name: "__name__".to_string(),
                                value: "http_request_duration_seconds".to_string(),
                            },
                            Label {
                                name: "job".to_string(),
                                value: "frontend".to_string(),
                            },
                        ],
                        samples: vec![Sample {
                            value: 0.423,
                            timestamp: 1_700_000_120_000,
                        }],
                        exemplars: vec![Exemplar {
                            labels: vec![
                                Label {
                                    name: "trace_id".to_string(),
                                    value: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
                                },
                                Label {
                                    name: "span_id".to_string(),
                                    value: "00f067aa0ba902b7".to_string(),
                                },
                            ],
                            value: 0.423,
                            timestamp: 1_700_000_120_000,
                        }],
                        ..Default::default()
                    }],
                    metadata: Vec::new(),
                }),
            },
            FixtureCase {
                name: "write_histogram",
                message: FixtureMessage::WriteRequest(WriteRequest {
                    timeseries: vec![TimeSeries {
                        labels: vec![
                            Label {
                                name: "__name__".to_string(),
                                value: "rpc_duration_seconds".to_string(),
                            },
                            Label {
                                name: "job".to_string(),
                                value: "backend".to_string(),
                            },
                        ],
                        histograms: vec![Histogram {
                            count: Some(histogram::Count::CountInt(42)),
                            sum: 17.5,
                            schema: 1,
                            zero_threshold: 0.001,
                            zero_count: Some(histogram::ZeroCount::ZeroCountInt(7)),
                            negative_spans: Vec::new(),
                            negative_deltas: Vec::new(),
                            negative_counts: Vec::new(),
                            positive_spans: vec![BucketSpan {
                                offset: 0,
                                length: 2,
                            }],
                            positive_deltas: vec![3, 1],
                            positive_counts: Vec::new(),
                            reset_hint: HistogramResetHint::No as i32,
                            timestamp: 1_700_000_180_000,
                            custom_values: Vec::new(),
                        }],
                        ..Default::default()
                    }],
                    metadata: Vec::new(),
                }),
            },
            FixtureCase {
                name: "write_mixed",
                message: FixtureMessage::WriteRequest(WriteRequest {
                    timeseries: vec![
                        TimeSeries {
                            labels: vec![
                                Label {
                                    name: "__name__".to_string(),
                                    value: "http_request_duration_seconds".to_string(),
                                },
                                Label {
                                    name: "job".to_string(),
                                    value: "frontend".to_string(),
                                },
                                Label {
                                    name: "route".to_string(),
                                    value: "/checkout".to_string(),
                                },
                            ],
                            samples: vec![Sample {
                                value: 0.851,
                                timestamp: 1_700_000_240_000,
                            }],
                            exemplars: vec![Exemplar {
                                labels: vec![Label {
                                    name: "trace_id".to_string(),
                                    value: "7d3ce929d0e0e47364bf92f3577b34da".to_string(),
                                }],
                                value: 0.851,
                                timestamp: 1_700_000_240_000,
                            }],
                            ..Default::default()
                        },
                        TimeSeries {
                            labels: vec![
                                Label {
                                    name: "__name__".to_string(),
                                    value: "rpc_duration_seconds".to_string(),
                                },
                                Label {
                                    name: "job".to_string(),
                                    value: "backend".to_string(),
                                },
                                Label {
                                    name: "method".to_string(),
                                    value: "POST".to_string(),
                                },
                            ],
                            histograms: vec![Histogram {
                                count: Some(histogram::Count::CountFloat(14.5)),
                                sum: 6.75,
                                schema: 2,
                                zero_threshold: 0.0,
                                zero_count: Some(histogram::ZeroCount::ZeroCountFloat(2.5)),
                                negative_spans: Vec::new(),
                                negative_deltas: Vec::new(),
                                negative_counts: Vec::new(),
                                positive_spans: vec![BucketSpan {
                                    offset: 1,
                                    length: 3,
                                }],
                                positive_deltas: Vec::new(),
                                positive_counts: vec![1.0, 4.5, 6.5],
                                reset_hint: HistogramResetHint::Gauge as i32,
                                timestamp: 1_700_000_240_000,
                                custom_values: vec![0.25, 0.5],
                            }],
                            ..Default::default()
                        },
                    ],
                    metadata: vec![MetricMetadata {
                        r#type: MetricType::Histogram as i32,
                        metric_family_name: "rpc_duration_seconds".to_string(),
                        help: "RPC latency represented as a native histogram.".to_string(),
                        unit: "seconds".to_string(),
                    }],
                }),
            },
            FixtureCase {
                name: "read_request_samples",
                message: FixtureMessage::ReadRequest(ReadRequest {
                    queries: vec![Query {
                        start_timestamp_ms: 1_700_000_000_000,
                        end_timestamp_ms: 1_700_000_300_000,
                        matchers: vec![
                            LabelMatcher {
                                r#type: MatcherType::Eq as i32,
                                name: "__name__".to_string(),
                                value: "http_requests_total".to_string(),
                            },
                            LabelMatcher {
                                r#type: MatcherType::Re as i32,
                                name: "job".to_string(),
                                value: "api|worker".to_string(),
                            },
                        ],
                        hints: Some(ReadHints {
                            step_ms: 15_000,
                            func: "rate".to_string(),
                            start_ms: 1_700_000_000_000,
                            end_ms: 1_700_000_300_000,
                            grouping: vec!["job".to_string()],
                            by: true,
                            range_ms: 60_000,
                        }),
                    }],
                    accepted_response_types: vec![
                        ReadResponseType::Samples as i32,
                        ReadResponseType::StreamedXorChunks as i32,
                    ],
                }),
            },
            FixtureCase {
                name: "read_response_samples",
                message: FixtureMessage::ReadResponse(ReadResponse {
                    results: vec![QueryResult {
                        timeseries: vec![
                            TimeSeries {
                                labels: vec![
                                    Label {
                                        name: "__name__".to_string(),
                                        value: "http_requests_total".to_string(),
                                    },
                                    Label {
                                        name: "job".to_string(),
                                        value: "api".to_string(),
                                    },
                                ],
                                samples: vec![
                                    Sample {
                                        value: 10.0,
                                        timestamp: 1_700_000_000_000,
                                    },
                                    Sample {
                                        value: 11.0,
                                        timestamp: 1_700_000_060_000,
                                    },
                                ],
                                ..Default::default()
                            },
                            TimeSeries {
                                labels: vec![
                                    Label {
                                        name: "__name__".to_string(),
                                        value: "rpc_duration_seconds".to_string(),
                                    },
                                    Label {
                                        name: "job".to_string(),
                                        value: "backend".to_string(),
                                    },
                                ],
                                histograms: vec![Histogram {
                                    count: Some(histogram::Count::CountInt(9)),
                                    sum: 4.0,
                                    schema: 0,
                                    zero_threshold: 0.0,
                                    zero_count: Some(histogram::ZeroCount::ZeroCountInt(1)),
                                    negative_spans: Vec::new(),
                                    negative_deltas: Vec::new(),
                                    negative_counts: Vec::new(),
                                    positive_spans: vec![BucketSpan {
                                        offset: 0,
                                        length: 2,
                                    }],
                                    positive_deltas: vec![1, 2],
                                    positive_counts: Vec::new(),
                                    reset_hint: HistogramResetHint::No as i32,
                                    timestamp: 1_700_000_060_000,
                                    custom_values: Vec::new(),
                                }],
                                ..Default::default()
                            },
                        ],
                    }],
                }),
            },
        ]
    }

    fn canonical_fixture_json(message: &FixtureMessage) -> Value {
        match message {
            FixtureMessage::WriteRequest(write) => write_request_json(write),
            FixtureMessage::ReadRequest(read) => read_request_json(read),
            FixtureMessage::ReadResponse(read) => read_response_json(read),
        }
    }

    fn encode_snappy(message: &FixtureMessage) -> Vec<u8> {
        let mut encoded = Vec::new();
        match message {
            FixtureMessage::WriteRequest(write) => write.encode(&mut encoded),
            FixtureMessage::ReadRequest(read) => read.encode(&mut encoded),
            FixtureMessage::ReadResponse(read) => read.encode(&mut encoded),
        }
        .expect("protobuf encode should succeed");
        SnappyEncoder::new()
            .compress_vec(&encoded)
            .expect("snappy encode should succeed")
    }

    fn decode_fixture_json(case: &FixtureCase, body: &[u8]) -> Value {
        let decoded = SnappyDecoder::new()
            .decompress_vec(body)
            .expect("snappy decode should succeed");
        match &case.message {
            FixtureMessage::WriteRequest(_) => write_request_json(
                &WriteRequest::decode(decoded.as_slice()).expect("write request should decode"),
            ),
            FixtureMessage::ReadRequest(_) => read_request_json(
                &ReadRequest::decode(decoded.as_slice()).expect("read request should decode"),
            ),
            FixtureMessage::ReadResponse(_) => read_response_json(
                &ReadResponse::decode(decoded.as_slice()).expect("read response should decode"),
            ),
        }
    }

    fn rewrite_fixture_expectations() {
        fs::create_dir_all(fixture_dir()).expect("fixture directory should exist");
        for case in fixture_cases() {
            fs::write(
                case.expectation_path(),
                serde_json::to_vec_pretty(&canonical_fixture_json(&case.message))
                    .expect("fixture JSON should encode"),
            )
            .expect("fixture expectation should write");
        }
    }

    fn load_json(path: &Path) -> Value {
        serde_json::from_slice(&fs::read(path).expect("fixture JSON should exist"))
            .expect("fixture JSON should decode")
    }

    fn enum_name<E>(result: Result<E, prost::UnknownEnumValue>) -> String
    where
        E: std::fmt::Debug,
    {
        result
            .ok()
            .map(|value| format!("{value:?}"))
            .unwrap_or_else(|| "UNKNOWN".to_string())
    }

    fn write_request_json(write: &WriteRequest) -> Value {
        json!({
            "timeseries": write.timeseries.iter().map(time_series_json).collect::<Vec<_>>(),
            "metadata": write.metadata.iter().map(metric_metadata_json).collect::<Vec<_>>(),
        })
    }

    fn read_request_json(read: &ReadRequest) -> Value {
        json!({
            "queries": read.queries.iter().map(query_json).collect::<Vec<_>>(),
            "acceptedResponseTypes": read
                .accepted_response_types
                .iter()
                .map(|value| enum_name(ReadResponseType::try_from(*value)))
                .collect::<Vec<_>>(),
        })
    }

    fn read_response_json(read: &ReadResponse) -> Value {
        json!({
            "results": read.results.iter().map(query_result_json).collect::<Vec<_>>(),
        })
    }

    fn query_json(query: &Query) -> Value {
        json!({
            "startTimestampMs": query.start_timestamp_ms,
            "endTimestampMs": query.end_timestamp_ms,
            "matchers": query.matchers.iter().map(label_matcher_json).collect::<Vec<_>>(),
            "hints": query.hints.as_ref().map(read_hints_json),
        })
    }

    fn query_result_json(query_result: &QueryResult) -> Value {
        json!({
            "timeseries": query_result.timeseries.iter().map(time_series_json).collect::<Vec<_>>(),
        })
    }

    fn time_series_json(series: &TimeSeries) -> Value {
        json!({
            "labels": series.labels.iter().map(label_json).collect::<Vec<_>>(),
            "samples": series.samples.iter().map(sample_json).collect::<Vec<_>>(),
            "exemplars": series.exemplars.iter().map(exemplar_json).collect::<Vec<_>>(),
            "histograms": series.histograms.iter().map(histogram_json).collect::<Vec<_>>(),
        })
    }

    fn label_json(label: &Label) -> Value {
        json!({
            "name": label.name,
            "value": label.value,
        })
    }

    fn sample_json(sample: &Sample) -> Value {
        json!({
            "value": sample.value,
            "timestamp": sample.timestamp,
        })
    }

    fn exemplar_json(exemplar: &Exemplar) -> Value {
        json!({
            "labels": exemplar.labels.iter().map(label_json).collect::<Vec<_>>(),
            "value": exemplar.value,
            "timestamp": exemplar.timestamp,
        })
    }

    fn histogram_json(histogram: &Histogram) -> Value {
        let count = match &histogram.count {
            Some(histogram::Count::CountInt(value)) => json!({
                "kind": "countInt",
                "value": value,
            }),
            Some(histogram::Count::CountFloat(value)) => json!({
                "kind": "countFloat",
                "value": value,
            }),
            None => Value::Null,
        };
        let zero_count = match &histogram.zero_count {
            Some(histogram::ZeroCount::ZeroCountInt(value)) => json!({
                "kind": "zeroCountInt",
                "value": value,
            }),
            Some(histogram::ZeroCount::ZeroCountFloat(value)) => json!({
                "kind": "zeroCountFloat",
                "value": value,
            }),
            None => Value::Null,
        };

        json!({
            "count": count,
            "sum": histogram.sum,
            "schema": histogram.schema,
            "zeroThreshold": histogram.zero_threshold,
            "zeroCount": zero_count,
            "negativeSpans": histogram.negative_spans.iter().map(bucket_span_json).collect::<Vec<_>>(),
            "negativeDeltas": histogram.negative_deltas,
            "negativeCounts": histogram.negative_counts,
            "positiveSpans": histogram.positive_spans.iter().map(bucket_span_json).collect::<Vec<_>>(),
            "positiveDeltas": histogram.positive_deltas,
            "positiveCounts": histogram.positive_counts,
            "resetHint": enum_name(HistogramResetHint::try_from(histogram.reset_hint)),
            "timestamp": histogram.timestamp,
            "customValues": histogram.custom_values,
        })
    }

    fn bucket_span_json(span: &BucketSpan) -> Value {
        json!({
            "offset": span.offset,
            "length": span.length,
        })
    }

    fn metric_metadata_json(metadata: &MetricMetadata) -> Value {
        json!({
            "type": enum_name(MetricType::try_from(metadata.r#type)),
            "metricFamilyName": metadata.metric_family_name,
            "help": metadata.help,
            "unit": metadata.unit,
        })
    }

    fn label_matcher_json(matcher: &LabelMatcher) -> Value {
        json!({
            "type": enum_name(MatcherType::try_from(matcher.r#type)),
            "name": matcher.name,
            "value": matcher.value,
        })
    }

    fn read_hints_json(hints: &ReadHints) -> Value {
        json!({
            "stepMs": hints.step_ms,
            "func": hints.func,
            "startMs": hints.start_ms,
            "endMs": hints.end_ms,
            "grouping": hints.grouping,
            "by": hints.by,
            "rangeMs": hints.range_ms,
        })
    }

    #[test]
    fn protocol_fixture_json_matches_current_schema() {
        for case in fixture_cases() {
            let expected_json = canonical_fixture_json(&case.message);
            let actual_json = load_json(&case.expectation_path());
            assert_eq!(
                actual_json, expected_json,
                "fixture JSON mismatch for {}",
                case.name
            );
        }
    }

    #[test]
    fn protocol_fixtures_decode_and_roundtrip() {
        for case in fixture_cases() {
            let fixture_body = encode_snappy(&case.message);
            let expected_json = load_json(&case.expectation_path());

            let decoded_json = decode_fixture_json(&case, &fixture_body);
            assert_eq!(
                decoded_json, expected_json,
                "decode mismatch for {}",
                case.name
            );

            let decoded = SnappyDecoder::new()
                .decompress_vec(&fixture_body)
                .expect("snappy decode should succeed");
            let reencoded_body = match &case.message {
                FixtureMessage::WriteRequest(_) => encode_snappy(&FixtureMessage::WriteRequest(
                    WriteRequest::decode(decoded.as_slice())
                        .expect("write request should roundtrip decode"),
                )),
                FixtureMessage::ReadRequest(_) => encode_snappy(&FixtureMessage::ReadRequest(
                    ReadRequest::decode(decoded.as_slice())
                        .expect("read request should roundtrip decode"),
                )),
                FixtureMessage::ReadResponse(_) => encode_snappy(&FixtureMessage::ReadResponse(
                    ReadResponse::decode(decoded.as_slice())
                        .expect("read response should roundtrip decode"),
                )),
            };
            let roundtrip_json = decode_fixture_json(&case, &reencoded_body);
            assert_eq!(
                roundtrip_json, expected_json,
                "roundtrip mismatch for {}",
                case.name
            );
        }
    }

    #[test]
    #[ignore = "run explicitly to regenerate checked-in Prometheus protocol JSON expectations"]
    fn regenerate_protocol_fixture_expectations() {
        rewrite_fixture_expectations();
    }
}
