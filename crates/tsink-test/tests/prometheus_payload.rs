#![cfg(feature = "prometheus")]

use prost::Message;
use snap::raw::Decoder as SnappyDecoder;
use tsink::TsinkError;
use tsink_protocol::prometheus::WriteRequest;
use tsink_test::{
    encode_prometheus_remote_write_request, label, prometheus_remote_write_payload,
    prometheus_remote_write_request, PrometheusRemoteWriteSample, PrometheusRemoteWriteSeries,
    PrometheusWriteRequest,
};

#[test]
fn remote_write_payload_uses_real_protobuf_and_snappy_models() {
    let input = [PrometheusRemoteWriteSeries::new(
        "requests_total",
        vec![label("status", "200"), label("method", "GET")],
        vec![
            PrometheusRemoteWriteSample::new(1_000, 3.0),
            PrometheusRemoteWriteSample::new(2_000, 5.0),
        ],
    )];
    let request = prometheus_remote_write_request(&input).unwrap();
    assert_eq!(
        request.timeseries[0]
            .labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("__name__", "requests_total"),
            ("method", "GET"),
            ("status", "200"),
        ]
    );

    let payload = prometheus_remote_write_payload(&input).unwrap();
    assert_eq!(payload.content_type(), "application/x-protobuf");
    assert_eq!(payload.content_encoding(), "snappy");
    assert_eq!(payload.series(), 1);
    assert_eq!(payload.samples(), 2);

    let protobuf = SnappyDecoder::new().decompress_vec(payload.body()).unwrap();
    assert_eq!(protobuf.len(), payload.protobuf_bytes());
    let decoded = WriteRequest::decode(protobuf.as_slice()).unwrap();
    assert_eq!(decoded, request);
}

#[test]
fn focused_builder_rejects_ambiguous_series_labels() {
    let invalid = [
        PrometheusRemoteWriteSeries::new(
            "",
            vec![],
            vec![PrometheusRemoteWriteSample::new(1, 1.0)],
        ),
        PrometheusRemoteWriteSeries::new(
            "m",
            vec![label("__name__", "other")],
            vec![PrometheusRemoteWriteSample::new(1, 1.0)],
        ),
        PrometheusRemoteWriteSeries::new(
            "m",
            vec![label("job", "a"), label("job", "b")],
            vec![PrometheusRemoteWriteSample::new(1, 1.0)],
        ),
    ];
    assert!(invalid.iter().all(|series| matches!(
        prometheus_remote_write_request(std::slice::from_ref(series)),
        Err(TsinkError::InvalidConfiguration(_))
    )));
}

#[test]
fn low_level_encoder_preserves_intentionally_malformed_requests() {
    let request = PrometheusWriteRequest::default();
    let payload = encode_prometheus_remote_write_request(&request).unwrap();
    let protobuf = SnappyDecoder::new().decompress_vec(payload.body()).unwrap();
    assert_eq!(
        PrometheusWriteRequest::decode(protobuf.as_slice()).unwrap(),
        request
    );
    assert_eq!(payload.series(), 0);
    assert_eq!(payload.samples(), 0);
}
