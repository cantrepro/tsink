use std::sync::Arc;

use chrono::TimeZone;
use tsink::{
    promql::{types::PROMETHEUS_STALE_NAN_BITS, Engine, PromqlError, PromqlValue, Sample, Series},
    DataPoint, HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram,
    Row, Storage, StorageBuilder, TimestampPrecision, Value,
};

fn setup_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let mut rows = Vec::new();
    let get_status_200 = vec![Label::new("method", "GET"), Label::new("status", "200")];
    let post_status_200 = vec![Label::new("method", "POST"), Label::new("status", "200")];

    for minute in 0..=10 {
        let ts = minute * 60;
        rows.push(Row::with_labels(
            "http_requests_total",
            get_status_200.clone(),
            DataPoint::new(ts, (minute * 60) as f64),
        ));
        rows.push(Row::with_labels(
            "http_requests_total",
            post_status_200.clone(),
            DataPoint::new(ts, (minute * 30) as f64),
        ));
    }

    storage.insert_rows(&rows).unwrap();
    storage
}

fn setup_group_matching_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "left_metric",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("region", "west"),
                ],
                DataPoint::new(60, 5.0),
            ),
            Row::with_labels(
                "left_metric",
                vec![
                    Label::new("instance", "b"),
                    Label::new("job", "api"),
                    Label::new("region", "west"),
                ],
                DataPoint::new(60, 10.0),
            ),
            Row::with_labels(
                "right_metric",
                vec![Label::new("job", "api"), Label::new("team", "core")],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "compare_left",
                vec![Label::new("job", "api"), Label::new("region", "west")],
                DataPoint::new(60, 3.0),
            ),
            Row::with_labels(
                "compare_right",
                vec![Label::new("instance", "a"), Label::new("job", "api")],
                DataPoint::new(60, 3.0),
            ),
            Row::with_labels(
                "compare_right",
                vec![Label::new("instance", "b"), Label::new("job", "api")],
                DataPoint::new(60, 3.0),
            ),
        ])
        .unwrap();

    storage
}

fn setup_collision_identity_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let promql_delimiter = "\u{1f}";
    let job_with_group_delimiter = format!("api{promql_delimiter}zone=west");

    storage
        .insert_rows(&[
            Row::with_labels(
                "collision_sum_total",
                vec![Label::new("job", job_with_group_delimiter.clone())],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "collision_sum_total",
                vec![Label::new("zone", "west"), Label::new("job", "api")],
                DataPoint::new(60, 3.0),
            ),
            Row::with_labels(
                "collision_match_left",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", job_with_group_delimiter.clone()),
                ],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "collision_match_left",
                vec![
                    Label::new("instance", "b"),
                    Label::new("zone", "west"),
                    Label::new("job", "api"),
                ],
                DataPoint::new(60, 4.0),
            ),
            Row::with_labels(
                "collision_match_right",
                vec![Label::new("job", job_with_group_delimiter.clone())],
                DataPoint::new(60, 10.0),
            ),
            Row::with_labels(
                "collision_match_right",
                vec![Label::new("zone", "west"), Label::new("job", "api")],
                DataPoint::new(60, 20.0),
            ),
            Row::with_labels(
                "collision_duration_seconds_bucket",
                vec![
                    Label::new("job", job_with_group_delimiter.clone()),
                    Label::new("le", "0.5"),
                ],
                DataPoint::new(60, 3.0),
            ),
            Row::with_labels(
                "collision_duration_seconds_bucket",
                vec![
                    Label::new("job", job_with_group_delimiter.clone()),
                    Label::new("le", "1"),
                ],
                DataPoint::new(60, 4.0),
            ),
            Row::with_labels(
                "collision_duration_seconds_bucket",
                vec![
                    Label::new("job", job_with_group_delimiter),
                    Label::new("le", "+Inf"),
                ],
                DataPoint::new(60, 5.0),
            ),
            Row::with_labels(
                "collision_duration_seconds_bucket",
                vec![
                    Label::new("zone", "west"),
                    Label::new("job", "api"),
                    Label::new("le", "0.5"),
                ],
                DataPoint::new(60, 1.0),
            ),
            Row::with_labels(
                "collision_duration_seconds_bucket",
                vec![
                    Label::new("zone", "west"),
                    Label::new("job", "api"),
                    Label::new("le", "1"),
                ],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "collision_duration_seconds_bucket",
                vec![
                    Label::new("zone", "west"),
                    Label::new("job", "api"),
                    Label::new("le", "+Inf"),
                ],
                DataPoint::new(60, 4.0),
            ),
        ])
        .unwrap();

    storage
}

fn setup_range_function_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "reset_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(0, 1.0),
            ),
            Row::with_labels(
                "reset_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(60, 4.0),
            ),
            Row::with_labels(
                "reset_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(120, 2.0),
            ),
            Row::with_labels(
                "reset_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(180, 7.0),
            ),
            Row::with_labels(
                "gauge_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(0, 0.0),
            ),
            Row::with_labels(
                "gauge_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(60, 120.0),
            ),
            Row::with_labels(
                "gauge_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(120, 240.0),
            ),
            Row::with_labels(
                "gauge_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(180, 360.0),
            ),
        ])
        .unwrap();

    storage
}

fn setup_histogram_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("le", "0.1"),
                ],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("le", "0.5"),
                ],
                DataPoint::new(60, 5.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("le", "1"),
                ],
                DataPoint::new(60, 9.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("le", "+Inf"),
                ],
                DataPoint::new(60, 10.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "b"),
                    Label::new("job", "api"),
                    Label::new("le", "0.1"),
                ],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "b"),
                    Label::new("job", "api"),
                    Label::new("le", "0.5"),
                ],
                DataPoint::new(60, 5.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "b"),
                    Label::new("job", "api"),
                    Label::new("le", "1"),
                ],
                DataPoint::new(60, 9.0),
            ),
            Row::with_labels(
                "request_duration_seconds_bucket",
                vec![
                    Label::new("instance", "b"),
                    Label::new("job", "api"),
                    Label::new("le", "+Inf"),
                ],
                DataPoint::new(60, 10.0),
            ),
            Row::with_labels(
                "incomplete_duration_seconds_bucket",
                vec![Label::new("job", "api"), Label::new("le", "0.5")],
                DataPoint::new(60, 5.0),
            ),
            Row::with_labels(
                "incomplete_duration_seconds_bucket",
                vec![Label::new("job", "api"), Label::new("le", "1")],
                DataPoint::new(60, 9.0),
            ),
        ])
        .unwrap();

    storage
}

fn native_histogram(
    count: f64,
    sum: f64,
    zero_count: f64,
    first_bucket: f64,
    second_bucket: f64,
) -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Float(count)),
        sum,
        schema: 0,
        zero_threshold: 0.5,
        zero_count: Some(HistogramCount::Float(zero_count)),
        negative_spans: Vec::new(),
        negative_deltas: Vec::new(),
        negative_counts: Vec::new(),
        positive_spans: vec![HistogramBucketSpan {
            offset: -1,
            length: 2,
        }],
        positive_deltas: Vec::new(),
        positive_counts: vec![first_bucket, second_bucket],
        reset_hint: HistogramResetHint::No,
        custom_values: Vec::new(),
    }
}

fn setup_native_histogram_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "request_duration_native_seconds",
                vec![Label::new("instance", "a"), Label::new("job", "api")],
                DataPoint::new(60, Value::from(native_histogram(10.0, 7.0, 2.0, 3.0, 5.0))),
            ),
            Row::with_labels(
                "request_duration_native_seconds",
                vec![Label::new("instance", "a"), Label::new("job", "api")],
                DataPoint::new(
                    120,
                    Value::from(native_histogram(20.0, 15.0, 4.0, 6.0, 10.0)),
                ),
            ),
            Row::with_labels(
                "request_duration_native_seconds",
                vec![Label::new("instance", "b"), Label::new("job", "api")],
                DataPoint::new(60, Value::from(native_histogram(10.0, 7.0, 2.0, 3.0, 5.0))),
            ),
            Row::with_labels(
                "request_duration_native_seconds",
                vec![Label::new("instance", "b"), Label::new("job", "api")],
                DataPoint::new(
                    120,
                    Value::from(native_histogram(20.0, 15.0, 4.0, 6.0, 10.0)),
                ),
            ),
            Row::with_labels(
                "mixed_histogram_metric",
                vec![Label::new("kind", "float"), Label::new("job", "api")],
                DataPoint::new(120, 3.0),
            ),
            Row::with_labels(
                "mixed_histogram_metric",
                vec![Label::new("kind", "hist"), Label::new("job", "api")],
                DataPoint::new(120, Value::from(native_histogram(5.0, 3.5, 1.0, 1.5, 2.5))),
            ),
        ])
        .unwrap();

    storage
}

fn setup_extrapolation_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "sparse_counter_total",
                vec![Label::new("job", "api")],
                DataPoint::new(390, 390.0),
            ),
            Row::with_labels(
                "sparse_counter_total",
                vec![Label::new("job", "api")],
                DataPoint::new(450, 450.0),
            ),
            Row::with_labels(
                "sparse_counter_total",
                vec![Label::new("job", "api")],
                DataPoint::new(510, 510.0),
            ),
            Row::with_labels(
                "sparse_counter_total",
                vec![Label::new("job", "api")],
                DataPoint::new(570, 570.0),
            ),
            Row::with_labels(
                "sparse_gauge",
                vec![Label::new("job", "api")],
                DataPoint::new(390, 390.0),
            ),
            Row::with_labels(
                "sparse_gauge",
                vec![Label::new("job", "api")],
                DataPoint::new(450, 450.0),
            ),
            Row::with_labels(
                "sparse_gauge",
                vec![Label::new("job", "api")],
                DataPoint::new(510, 510.0),
            ),
            Row::with_labels(
                "sparse_gauge",
                vec![Label::new("job", "api")],
                DataPoint::new(570, 570.0),
            ),
        ])
        .unwrap();

    storage
}

fn setup_staleness_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    let stale_nan = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    storage
        .insert_rows(&[
            Row::with_labels(
                "stale_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(0, 1.0),
            ),
            Row::with_labels(
                "stale_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(60, 2.0),
            ),
            Row::with_labels(
                "stale_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(120, stale_nan),
            ),
            Row::with_labels(
                "revived_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(0, 1.0),
            ),
            Row::with_labels(
                "revived_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(60, stale_nan),
            ),
            Row::with_labels(
                "revived_metric",
                vec![Label::new("job", "api")],
                DataPoint::new(120, 7.0),
            ),
        ])
        .unwrap();

    storage
}

fn setup_nan_aggregation_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "nan_metric",
                vec![Label::new("instance", "a")],
                DataPoint::new(60, f64::NAN),
            ),
            Row::with_labels(
                "nan_metric",
                vec![Label::new("instance", "b")],
                DataPoint::new(60, 1.0),
            ),
            Row::with_labels(
                "nan_metric",
                vec![Label::new("instance", "c")],
                DataPoint::new(60, 2.0),
            ),
        ])
        .unwrap();

    storage
}

fn setup_info_storage() -> Arc<dyn Storage> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "up",
                vec![Label::new("instance", "a"), Label::new("job", "api")],
                DataPoint::new(600, 1.0),
            ),
            Row::with_labels(
                "target_info",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("k8s_cluster_name", "old"),
                    Label::new("team", "core"),
                ],
                DataPoint::new(540, 1.0),
            ),
            Row::with_labels(
                "target_info",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("k8s_cluster_name", "prod"),
                    Label::new("team", "platform"),
                ],
                DataPoint::new(600, 1.0),
            ),
            Row::with_labels(
                "build_info",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("build_version", "1.2.3"),
                ],
                DataPoint::new(600, 1.0),
            ),
        ])
        .unwrap();

    storage
}

fn as_instant_vector(value: PromqlValue) -> Vec<Sample> {
    match value {
        PromqlValue::InstantVector(v) => v,
        other => panic!("expected instant vector, got {other:?}"),
    }
}

fn as_range_vector(value: PromqlValue) -> Vec<Series> {
    match value {
        PromqlValue::RangeVector(v) => v,
        other => panic!("expected range vector, got {other:?}"),
    }
}

fn as_scalar(value: PromqlValue) -> (f64, i64) {
    match value {
        PromqlValue::Scalar(v, ts) => (v, ts),
        other => panic!("expected scalar, got {other:?}"),
    }
}

#[test]
fn selector_exact_and_regex_matchers_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let value = engine
        .instant_query("http_requests_total{method=\"GET\",status=~\"2..\"}", 600)
        .unwrap();

    let samples = as_instant_vector(value);
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].metric, "http_requests_total");
    assert!((samples[0].value - 600.0).abs() < 1e-9);
}

#[test]
fn rate_irate_and_increase_work_for_counter_data() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let rate = as_instant_vector(
        engine
            .instant_query("rate(http_requests_total[5m])", 600)
            .unwrap(),
    );
    assert_eq!(rate.len(), 2);

    let get_rate = rate
        .iter()
        .find(|s| {
            s.labels
                .iter()
                .any(|l| l.name == "method" && l.value == "GET")
        })
        .unwrap()
        .value;
    let post_rate = rate
        .iter()
        .find(|s| {
            s.labels
                .iter()
                .any(|l| l.name == "method" && l.value == "POST")
        })
        .unwrap()
        .value;
    assert!((get_rate - 1.0).abs() < 1e-9);
    assert!((post_rate - 0.5).abs() < 1e-9);

    let irate = as_instant_vector(
        engine
            .instant_query("irate(http_requests_total{method=\"GET\"}[5m])", 600)
            .unwrap(),
    );
    assert_eq!(irate.len(), 1);
    assert!((irate[0].value - 1.0).abs() < 1e-9);

    let increase = as_instant_vector(
        engine
            .instant_query("increase(http_requests_total{method=\"GET\"}[5m])", 600)
            .unwrap(),
    );
    assert_eq!(increase.len(), 1);
    assert!((increase[0].value - 300.0).abs() < 1e-9);
}

#[test]
fn aggregation_by_and_without_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let by_method = as_instant_vector(
        engine
            .instant_query("sum by (method) (rate(http_requests_total[5m]))", 600)
            .unwrap(),
    );
    assert_eq!(by_method.len(), 2);

    let without_status = as_instant_vector(
        engine
            .instant_query("sum without (status) (rate(http_requests_total[5m]))", 600)
            .unwrap(),
    );
    assert_eq!(without_status.len(), 2);

    let mut values = without_status
        .iter()
        .map(|s| {
            (
                s.labels
                    .iter()
                    .find(|l| l.name == "method")
                    .unwrap()
                    .value
                    .clone(),
                s.value,
            )
        })
        .collect::<Vec<_>>();
    values.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(values[0].0, "GET");
    assert!((values[0].1 - 1.0).abs() < 1e-9);
    assert_eq!(values[1].0, "POST");
    assert!((values[1].1 - 0.5).abs() < 1e-9);
}

#[test]
fn additional_aggregations_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let quantile = as_instant_vector(
        engine
            .instant_query("quantile(0.5, http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(quantile.len(), 1);
    assert!((quantile[0].value - 450.0).abs() < 1e-9);

    let stdvar = as_instant_vector(
        engine
            .instant_query("stdvar(http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(stdvar.len(), 1);
    assert!((stdvar[0].value - 22_500.0).abs() < 1e-9);

    let stddev = as_instant_vector(
        engine
            .instant_query("stddev(http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(stddev.len(), 1);
    assert!((stddev[0].value - 150.0).abs() < 1e-9);

    let grouped = as_instant_vector(
        engine
            .instant_query("group by (method) (http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(grouped.len(), 2);
    assert!(grouped.iter().all(|sample| sample.value == 1.0));

    let mut count_values = as_instant_vector(
        engine
            .instant_query(r#"count_values("current", http_requests_total)"#, 600)
            .unwrap(),
    );
    count_values.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(count_values.len(), 2);
    assert_eq!(count_values[0].value, 1.0);
    assert_eq!(count_values[1].value, 1.0);
    assert!(count_values[0]
        .labels
        .iter()
        .any(|label| label.name == "current" && label.value == "300"));
    assert!(count_values[1]
        .labels
        .iter()
        .any(|label| label.name == "current" && label.value == "600"));

    let topk = as_instant_vector(
        engine
            .instant_query("topk(1, http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(topk.len(), 1);
    assert_eq!(topk[0].metric, "http_requests_total");
    assert!(topk[0]
        .labels
        .iter()
        .any(|label| label.name == "method" && label.value == "GET"));

    let limitk = as_instant_vector(
        engine
            .instant_query("limitk(1, http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(limitk.len(), 1);
    assert_eq!(limitk[0].metric, "http_requests_total");

    let limit_ratio_pos = as_instant_vector(
        engine
            .instant_query("limit_ratio(0.5, http_requests_total)", 600)
            .unwrap(),
    );
    let limit_ratio_neg = as_instant_vector(
        engine
            .instant_query("limit_ratio(-0.5, http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(limit_ratio_pos.len(), 1);
    assert_eq!(limit_ratio_neg.len(), 1);
    assert_ne!(limit_ratio_pos[0].labels, limit_ratio_neg[0].labels);
}

#[test]
fn binary_vector_scalar_ops_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let filtered = as_instant_vector(
        engine
            .instant_query("http_requests_total > 400", 600)
            .unwrap(),
    );
    assert_eq!(filtered.len(), 1);
    assert!(filtered[0]
        .labels
        .iter()
        .any(|l| l.name == "method" && l.value == "GET"));

    let divided = as_instant_vector(
        engine
            .instant_query("http_requests_total{method=\"POST\"} / 60", 600)
            .unwrap(),
    );
    assert_eq!(divided.len(), 1);
    assert!((divided[0].value - 5.0).abs() < 1e-9);
}

#[test]
fn over_time_functions_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let avg = as_instant_vector(
        engine
            .instant_query(
                "avg_over_time(http_requests_total{method=\"GET\"}[5m])",
                600,
            )
            .unwrap(),
    );
    assert_eq!(avg.len(), 1);
    assert!((avg[0].value - 450.0).abs() < 1e-9);

    let count = as_instant_vector(
        engine
            .instant_query(
                "count_over_time(http_requests_total{method=\"GET\"}[5m])",
                600,
            )
            .unwrap(),
    );
    assert_eq!(count.len(), 1);
    assert!((count[0].value - 6.0).abs() < 1e-9);
}

#[test]
fn at_modifier_supports_absolute_and_range_bounds() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let instant = as_instant_vector(
        engine
            .instant_query("http_requests_total{method=\"GET\"} @ 300", 600)
            .unwrap(),
    );
    assert_eq!(instant.len(), 1);
    assert_eq!(instant[0].timestamp, 300);
    assert!((instant[0].value - 300.0).abs() < 1e-9);

    let range = as_range_vector(
        engine
            .range_query(
                "http_requests_total{method=\"GET\"} @ start()",
                300,
                600,
                60,
            )
            .unwrap(),
    );
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].samples.len(), 6);
    assert!(range[0]
        .samples
        .iter()
        .all(|(ts, value)| *ts >= 300 && *value == 300.0));
}

#[test]
fn subquery_returns_range_vectors_and_uses_default_or_explicit_resolution() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let explicit = as_range_vector(
        engine
            .instant_query("http_requests_total{method=\"GET\"}[5m:2m]", 600)
            .unwrap(),
    );
    assert_eq!(explicit.len(), 1);
    assert_eq!(
        explicit[0].samples,
        vec![(300, 300.0), (420, 420.0), (540, 540.0)]
    );

    let defaulted = as_range_vector(
        engine
            .instant_query("http_requests_total{method=\"GET\"}[5m:]", 600)
            .unwrap(),
    );
    assert_eq!(defaulted.len(), 1);
    assert_eq!(defaulted[0].samples.len(), 6);
    assert_eq!(defaulted[0].samples[0], (300, 300.0));
    assert_eq!(defaulted[0].samples[5], (600, 600.0));
}

#[test]
fn subquery_composes_with_range_functions() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(
        engine
            .instant_query(
                "last_over_time(rate(http_requests_total{method=\"GET\"}[2m])[4m:1m])",
                600,
            )
            .unwrap(),
    );
    assert_eq!(samples.len(), 1);
    assert!((samples[0].value - 1.0).abs() < 1e-9);
}

#[test]
fn range_query_step_iteration_works() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let value = engine
        .range_query(
            "rate(http_requests_total{method=\"GET\"}[5m])",
            300,
            600,
            60,
        )
        .unwrap();

    let series = as_range_vector(value);
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].samples.len(), 6);
    for (_, v) in &series[0].samples {
        assert!((*v - 1.0).abs() < 1e-9);
    }
}

#[test]
fn metric_name_matcher_with_empty_matching_label_regex_keeps_series() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(
        engine
            .instant_query(r#"{__name__="http_requests_total",foo=~".*"}"#, 600)
            .unwrap(),
    );

    assert_eq!(samples.len(), 2);
}

#[test]
fn metricless_selector_uses_missing_label_empty_string_semantics() {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    storage
        .insert_rows(&[
            Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "a"), Label::new("job", "api")],
                DataPoint::new(600, 1.0),
            ),
            Row::with_labels(
                "memory_usage",
                vec![
                    Label::new("host", "b"),
                    Label::new("job", "api"),
                    Label::new("zone", "use1"),
                ],
                DataPoint::new(600, 2.0),
            ),
        ])
        .unwrap();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let missing_zone =
        as_instant_vector(engine.instant_query(r#"{job="api",zone=""}"#, 600).unwrap());
    assert_eq!(missing_zone.len(), 1);
    assert_eq!(missing_zone[0].metric, "cpu_usage");
    assert_eq!(
        missing_zone[0].labels,
        vec![Label::new("host", "a"), Label::new("job", "api")]
    );

    let present_zone = as_instant_vector(
        engine
            .instant_query(r#"{job="api",zone!=""}"#, 600)
            .unwrap(),
    );
    assert_eq!(present_zone.len(), 1);
    assert_eq!(present_zone[0].metric, "memory_usage");
    assert_eq!(
        present_zone[0].labels,
        vec![
            Label::new("host", "b"),
            Label::new("job", "api"),
            Label::new("zone", "use1"),
        ]
    );
}

#[test]
fn arithmetic_vector_scalar_drops_metric_name() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(
        engine
            .instant_query("http_requests_total / 60", 600)
            .unwrap(),
    );
    assert_eq!(samples.len(), 2);
    assert!(samples.iter().all(|sample| sample.metric.is_empty()));
}

#[test]
fn comparison_bool_vector_scalar_drops_metric_name() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(
        engine
            .instant_query("http_requests_total > bool 0", 600)
            .unwrap(),
    );
    assert_eq!(samples.len(), 2);
    assert!(samples.iter().all(|sample| sample.metric.is_empty()));
}

#[test]
fn comparison_on_modifier_drops_metric_name_without_bool() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(
        engine
            .instant_query(
                "http_requests_total{method=\"GET\"} == on(status) http_requests_total{method=\"GET\"}",
                600,
            )
            .unwrap(),
    );

    assert_eq!(samples.len(), 1);
    assert!(samples[0].metric.is_empty());
}

#[test]
fn vector_vector_many_to_many_matching_is_rejected_without_group_modifiers() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let err = engine
        .instant_query("http_requests_total == on(status) http_requests_total", 600)
        .unwrap_err();
    assert!(matches!(err, PromqlError::Eval(msg) if msg.contains("many-to-many")));
}

#[test]
fn group_left_arithmetic_supports_many_to_one_matching_and_label_inclusion() {
    let storage = setup_group_matching_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let mut samples = as_instant_vector(
        engine
            .instant_query("left_metric / on(job) group_left(team) right_metric", 60)
            .unwrap(),
    );

    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(samples.len(), 2);
    assert!(samples.iter().all(|sample| sample.metric.is_empty()));
    assert_eq!(samples[0].value, 2.5);
    assert_eq!(samples[1].value, 5.0);
    assert!(samples.iter().all(|sample| sample
        .labels
        .iter()
        .any(|label| label.name == "team" && label.value == "core")));
}

#[test]
fn group_right_comparison_uses_rhs_cardinality_and_metric_name() {
    let storage = setup_group_matching_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let mut samples = as_instant_vector(
        engine
            .instant_query(
                "compare_left == on(job) group_right(region) compare_right",
                60,
            )
            .unwrap(),
    );

    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(samples.len(), 2);
    assert!(samples
        .iter()
        .all(|sample| sample.metric == "compare_right" && sample.value == 3.0));
    assert!(samples.iter().all(|sample| {
        sample
            .labels
            .iter()
            .any(|label| label.name == "region" && label.value == "west")
    }));
}

#[test]
fn aggregation_distinguishes_delimiter_collision_group_labels() {
    let storage = setup_collision_identity_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);
    let delimiter = "\u{1f}";

    let mut samples = as_instant_vector(
        engine
            .instant_query("sum by (job, zone) (collision_sum_total)", 60)
            .unwrap(),
    );

    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(samples.len(), 2);
    assert_eq!(
        samples[0].labels,
        vec![Label::new("job", "api"), Label::new("zone", "west")]
    );
    assert_eq!(samples[0].value, 3.0);
    assert_eq!(
        samples[1].labels,
        vec![Label::new("job", format!("api{delimiter}zone=west"))]
    );
    assert_eq!(samples[1].value, 2.0);
}

#[test]
fn vector_matching_distinguishes_delimiter_collision_group_labels() {
    let storage = setup_collision_identity_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);
    let delimiter = "\u{1f}";

    let mut samples = as_instant_vector(
        engine
            .instant_query(
                "collision_match_left + on(job, zone) collision_match_right",
                60,
            )
            .unwrap(),
    );

    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].value, 12.0);
    assert_eq!(
        samples[0].labels,
        vec![
            Label::new("instance", "a"),
            Label::new("job", format!("api{delimiter}zone=west")),
        ]
    );
    assert_eq!(samples[1].value, 24.0);
    assert_eq!(
        samples[1].labels,
        vec![
            Label::new("instance", "b"),
            Label::new("job", "api"),
            Label::new("zone", "west"),
        ]
    );
}

#[test]
fn additional_range_functions_work() {
    let storage = setup_range_function_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let delta = as_instant_vector(
        engine
            .instant_query("delta(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(delta[0].value, 6.0);

    let idelta = as_instant_vector(
        engine
            .instant_query("idelta(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(idelta[0].value, 5.0);

    let changes = as_instant_vector(
        engine
            .instant_query("changes(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(changes[0].value, 3.0);

    let resets = as_instant_vector(
        engine
            .instant_query("resets(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(resets[0].value, 1.0);

    let last = as_instant_vector(
        engine
            .instant_query("last_over_time(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(last[0].value, 7.0);

    let present = as_instant_vector(
        engine
            .instant_query("present_over_time(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(present[0].value, 1.0);

    let stdvar = as_instant_vector(
        engine
            .instant_query("stdvar_over_time(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert!((stdvar[0].value - 5.25).abs() < 1e-9);

    let stddev = as_instant_vector(
        engine
            .instant_query("stddev_over_time(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert!((stddev[0].value - 5.25_f64.sqrt()).abs() < 1e-9);
}

#[test]
fn round_supports_optional_precision_argument() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(
        engine
            .instant_query("round(vector(1.24), 0.1)", 600)
            .unwrap(),
    );
    assert_eq!(samples.len(), 1);
    assert!((samples[0].value - 1.2).abs() < 1e-9);
}

#[test]
fn absent_and_additional_range_functions_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let absent = as_instant_vector(
        engine
            .instant_query(
                r#"absent(nonexistent_metric{job="api",instance=~".*"})"#,
                600,
            )
            .unwrap(),
    );
    assert_eq!(absent.len(), 1);
    assert_eq!(absent[0].value, 1.0);
    assert!(absent[0]
        .labels
        .iter()
        .any(|label| label.name == "job" && label.value == "api"));
    assert!(!absent[0]
        .labels
        .iter()
        .any(|label| label.name == "instance"));

    let present = as_instant_vector(
        engine
            .instant_query(r#"absent(http_requests_total{method="GET"})"#, 600)
            .unwrap(),
    );
    assert!(present.is_empty());

    let absent_over_time = as_instant_vector(
        engine
            .instant_query(
                r#"absent_over_time(nonexistent_metric{job="api",instance=~".*"}[5m])"#,
                600,
            )
            .unwrap(),
    );
    assert_eq!(absent_over_time.len(), 1);
    assert_eq!(absent_over_time[0].value, 1.0);
    assert!(absent_over_time[0]
        .labels
        .iter()
        .any(|label| label.name == "job" && label.value == "api"));

    let storage = setup_range_function_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let quantile = as_instant_vector(
        engine
            .instant_query("quantile_over_time(0.5, reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(quantile.len(), 1);
    assert!((quantile[0].value - 3.0).abs() < 1e-9);

    let deriv = as_instant_vector(
        engine
            .instant_query("deriv(gauge_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(deriv.len(), 1);
    assert!((deriv[0].value - 2.0).abs() < 1e-9);
}

#[test]
fn histogram_quantile_works_for_classic_buckets() {
    let storage = setup_histogram_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let direct = as_instant_vector(
        engine
            .instant_query(
                "histogram_quantile(0.9, request_duration_seconds_bucket{instance=\"a\"})",
                60,
            )
            .unwrap(),
    );
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0].metric, "request_duration_seconds");
    assert!((direct[0].value - 1.0).abs() < 1e-9);

    let aggregated = as_instant_vector(
        engine
            .instant_query(
                "histogram_quantile(0.5, sum by (job, le) (request_duration_seconds_bucket))",
                60,
            )
            .unwrap(),
    );
    assert_eq!(aggregated.len(), 1);
    assert!(aggregated[0].metric.is_empty());
    assert!(aggregated[0]
        .labels
        .iter()
        .any(|label| label.name == "job" && label.value == "api"));
    assert!((aggregated[0].value - 0.5).abs() < 1e-9);

    let incomplete = as_instant_vector(
        engine
            .instant_query(
                "histogram_quantile(0.5, incomplete_duration_seconds_bucket)",
                60,
            )
            .unwrap(),
    );
    assert_eq!(incomplete.len(), 1);
    assert!(incomplete[0].value.is_nan());
}

#[test]
fn histogram_quantile_distinguishes_delimiter_collision_groups() {
    let storage = setup_collision_identity_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);
    let delimiter = "\u{1f}";

    let mut samples = as_instant_vector(
        engine
            .instant_query(
                "histogram_quantile(0.5, collision_duration_seconds_bucket)",
                60,
            )
            .unwrap(),
    );

    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].metric, "collision_duration_seconds");
    assert_eq!(
        samples[0].labels,
        vec![Label::new("job", "api"), Label::new("zone", "west")]
    );
    assert!((samples[0].value - 1.0).abs() < 1e-9);
    assert_eq!(samples[1].metric, "collision_duration_seconds");
    assert_eq!(
        samples[1].labels,
        vec![Label::new("job", format!("api{delimiter}zone=west"))]
    );
    assert!((samples[1].value - (5.0 / 12.0)).abs() < 1e-9);
}

#[test]
fn rate_increase_and_delta_extrapolate_to_query_boundaries() {
    let storage = setup_extrapolation_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let rate = as_instant_vector(
        engine
            .instant_query("rate(sparse_counter_total[5m])", 600)
            .unwrap(),
    );
    assert_eq!(rate.len(), 1);
    assert!((rate[0].value - 0.8).abs() < 1e-9);

    let increase = as_instant_vector(
        engine
            .instant_query("increase(sparse_counter_total[5m])", 600)
            .unwrap(),
    );
    assert_eq!(increase.len(), 1);
    assert!((increase[0].value - 240.0).abs() < 1e-9);

    let delta = as_instant_vector(
        engine
            .instant_query("delta(sparse_gauge[5m])", 600)
            .unwrap(),
    );
    assert_eq!(delta.len(), 1);
    assert!((delta[0].value - 240.0).abs() < 1e-9);

    let irate = as_instant_vector(
        engine
            .instant_query("irate(sparse_counter_total[5m])", 600)
            .unwrap(),
    );
    assert_eq!(irate.len(), 1);
    assert!((irate[0].value - 1.0).abs() < 1e-9);

    let idelta = as_instant_vector(
        engine
            .instant_query("idelta(sparse_gauge[5m])", 600)
            .unwrap(),
    );
    assert_eq!(idelta.len(), 1);
    assert!((idelta[0].value - 60.0).abs() < 1e-9);
}

#[test]
fn stale_markers_hide_series_from_instant_queries_but_not_range_history() {
    let storage = setup_staleness_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let stale = as_instant_vector(
        engine
            .instant_query("stale_metric{job=\"api\"}", 150)
            .unwrap(),
    );
    assert!(stale.is_empty());

    let stale_range = as_range_vector(
        engine
            .instant_query("stale_metric{job=\"api\"}[5m]", 150)
            .unwrap(),
    );
    assert_eq!(stale_range.len(), 1);
    assert_eq!(stale_range[0].samples, vec![(0, 1.0), (60, 2.0)]);

    let revived = as_instant_vector(
        engine
            .instant_query("revived_metric{job=\"api\"}", 150)
            .unwrap(),
    );
    assert_eq!(revived.len(), 1);
    assert_eq!(revived[0].timestamp, 120);
    assert!((revived[0].value - 7.0).abs() < 1e-9);
}

#[test]
fn scalar_and_calendar_functions_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let (count, ts) = as_scalar(
        engine
            .instant_query("count_scalar(http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(count, 2.0);
    assert_eq!(ts, 600);

    let (pi, _) = as_scalar(engine.instant_query("pi()", 600).unwrap());
    assert!((pi - std::f64::consts::PI).abs() < 1e-12);

    let sgn = as_instant_vector(engine.instant_query("sgn(vector(-2))", 600).unwrap());
    assert_eq!(sgn.len(), 1);
    assert_eq!(sgn[0].value, -1.0);

    let ts = chrono::Utc
        .with_ymd_and_hms(2024, 2, 29, 23, 58, 0)
        .single()
        .unwrap()
        .timestamp();

    let year_default = as_instant_vector(engine.instant_query("year()", ts).unwrap());
    assert_eq!(year_default.len(), 1);
    assert_eq!(year_default[0].value, 2024.0);

    let month = as_instant_vector(engine.instant_query(&format!("month({ts})"), 0).unwrap());
    assert_eq!(month[0].value, 2.0);

    let day_of_month = as_instant_vector(
        engine
            .instant_query(&format!("day_of_month({ts})"), 0)
            .unwrap(),
    );
    assert_eq!(day_of_month[0].value, 29.0);

    let day_of_week = as_instant_vector(
        engine
            .instant_query(&format!("day_of_week({ts})"), 0)
            .unwrap(),
    );
    assert_eq!(day_of_week[0].value, 4.0);

    let day_of_year = as_instant_vector(
        engine
            .instant_query(&format!("day_of_year({ts})"), 0)
            .unwrap(),
    );
    assert_eq!(day_of_year[0].value, 60.0);

    let days_in_month = as_instant_vector(
        engine
            .instant_query(&format!("days_in_month({ts})"), 0)
            .unwrap(),
    );
    assert_eq!(days_in_month[0].value, 29.0);

    let hour = as_instant_vector(engine.instant_query(&format!("hour({ts})"), 0).unwrap());
    assert_eq!(hour[0].value, 23.0);

    let minute = as_instant_vector(engine.instant_query(&format!("minute({ts})"), 0).unwrap());
    assert_eq!(minute[0].value, 58.0);
}

#[test]
fn label_sort_and_drop_common_labels_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let sorted = as_instant_vector(
        engine
            .instant_query(
                r#"sort_by_label(label_replace(http_requests_total, "instance", "srv$1", "method", "(.*)"), "instance")"#,
                600,
            )
            .unwrap(),
    );
    assert_eq!(sorted.len(), 2);
    assert!(sorted[0]
        .labels
        .iter()
        .any(|label| label.name == "instance" && label.value == "srvGET"));
    assert!(sorted[1]
        .labels
        .iter()
        .any(|label| label.name == "instance" && label.value == "srvPOST"));

    let sorted_desc = as_instant_vector(
        engine
            .instant_query(
                r#"sort_by_label_desc(label_replace(http_requests_total, "shard", "node$1", "method", "(.*)"), "shard")"#,
                600,
            )
            .unwrap(),
    );
    assert_eq!(sorted_desc.len(), 2);
    assert!(sorted_desc[0]
        .labels
        .iter()
        .any(|label| label.name == "shard" && label.value == "nodePOST"));

    let dropped = as_instant_vector(
        engine
            .instant_query("drop_common_labels(http_requests_total)", 600)
            .unwrap(),
    );
    assert_eq!(dropped.len(), 2);
    assert!(dropped.iter().all(|sample| {
        !sample
            .labels
            .iter()
            .any(|label| label.name == "status" && label.value == "200")
    }));
    assert!(dropped
        .iter()
        .all(|sample| { sample.labels.iter().any(|label| label.name == "method") }));
}

#[test]
fn info_function_enriches_series_with_info_labels() {
    let storage = setup_info_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let enriched = as_instant_vector(engine.instant_query("info(up)", 600).unwrap());
    assert_eq!(enriched.len(), 1);
    assert!(enriched[0]
        .labels
        .iter()
        .any(|label| label.name == "k8s_cluster_name" && label.value == "prod"));
    assert!(enriched[0]
        .labels
        .iter()
        .any(|label| label.name == "team" && label.value == "platform"));

    let restricted = as_instant_vector(
        engine
            .instant_query(r#"info(up, {k8s_cluster_name=~".+"})"#, 600)
            .unwrap(),
    );
    assert_eq!(restricted.len(), 1);
    assert!(restricted[0]
        .labels
        .iter()
        .any(|label| label.name == "k8s_cluster_name" && label.value == "prod"));
    assert!(!restricted[0]
        .labels
        .iter()
        .any(|label| label.name == "team"));

    let multi_metric = as_instant_vector(
        engine
            .instant_query(
                r#"info(up, {__name__=~"(target|build)_info",build_version=~".+"})"#,
                600,
            )
            .unwrap(),
    );
    assert_eq!(multi_metric.len(), 1);
    assert!(multi_metric[0]
        .labels
        .iter()
        .any(|label| label.name == "build_version" && label.value == "1.2.3"));
}

#[test]
fn native_histogram_only_functions_ignore_float_samples() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    for query in [
        "histogram_avg(http_requests_total)",
        "histogram_count(http_requests_total)",
        "histogram_sum(http_requests_total)",
        "histogram_stddev(http_requests_total)",
        "histogram_stdvar(http_requests_total)",
        "histogram_fraction(0, 1, http_requests_total)",
    ] {
        let samples = as_instant_vector(engine.instant_query(query, 600).unwrap());
        assert!(samples.is_empty(), "{query} should ignore float samples");
    }
}

#[test]
fn native_histogram_selectors_and_supported_functions_work() {
    let storage = setup_native_histogram_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let selected = as_instant_vector(
        engine
            .instant_query(r#"request_duration_native_seconds{instance="a"}"#, 120)
            .unwrap(),
    );
    assert_eq!(selected.len(), 1);
    let histogram = selected[0]
        .histogram
        .as_deref()
        .expect("selector should return a histogram sample");
    assert_eq!(histogram.count, Some(HistogramCount::Float(20.0)));
    assert_eq!(histogram.sum, 15.0);

    let range = as_range_vector(
        engine
            .range_query(
                r#"request_duration_native_seconds{instance="a"}"#,
                60,
                120,
                60,
            )
            .unwrap(),
    );
    assert_eq!(range.len(), 1);
    assert!(range[0].samples.is_empty());
    assert_eq!(range[0].histograms.len(), 2);

    let summed = as_instant_vector(
        engine
            .instant_query("sum by (job) (request_duration_native_seconds)", 120)
            .unwrap(),
    );
    assert_eq!(summed.len(), 1);
    let summed_hist = summed[0]
        .histogram
        .as_deref()
        .expect("sum should return a histogram sample");
    assert_eq!(summed_hist.count, Some(HistogramCount::Float(40.0)));
    assert_eq!(summed_hist.sum, 30.0);
    assert_eq!(summed_hist.zero_count, Some(HistogramCount::Float(8.0)));
    assert_eq!(summed_hist.positive_counts, vec![12.0, 20.0]);

    let averaged = as_instant_vector(
        engine
            .instant_query("avg by (job) (request_duration_native_seconds)", 120)
            .unwrap(),
    );
    assert_eq!(averaged.len(), 1);
    let averaged_hist = averaged[0]
        .histogram
        .as_deref()
        .expect("avg should return a histogram sample");
    assert_eq!(averaged_hist.count, Some(HistogramCount::Float(20.0)));
    assert_eq!(averaged_hist.sum, 15.0);
    assert_eq!(averaged_hist.zero_count, Some(HistogramCount::Float(4.0)));
    assert_eq!(averaged_hist.positive_counts, vec![6.0, 10.0]);

    let increased = as_instant_vector(
        engine
            .instant_query(
                r#"increase(request_duration_native_seconds{instance="a"}[1m])"#,
                120,
            )
            .unwrap(),
    );
    let increased_hist = increased[0]
        .histogram
        .as_deref()
        .expect("increase should return a histogram sample");
    assert_eq!(increased_hist.count, Some(HistogramCount::Float(10.0)));
    assert_eq!(increased_hist.sum, 8.0);
    assert_eq!(increased_hist.zero_count, Some(HistogramCount::Float(2.0)));
    assert_eq!(increased_hist.positive_counts, vec![3.0, 5.0]);

    let rated = as_instant_vector(
        engine
            .instant_query(
                r#"rate(request_duration_native_seconds{instance="a"}[1m])"#,
                120,
            )
            .unwrap(),
    );
    let rated_hist = rated[0]
        .histogram
        .as_deref()
        .expect("rate should return a histogram sample");
    assert_eq!(rated_hist.count, Some(HistogramCount::Float(10.0 / 60.0)));
    assert!((rated_hist.sum - (8.0 / 60.0)).abs() < 1e-12);
    assert_eq!(
        rated_hist.zero_count,
        Some(HistogramCount::Float(2.0 / 60.0))
    );
    assert!((rated_hist.positive_counts[0] - (3.0 / 60.0)).abs() < 1e-12);
    assert!((rated_hist.positive_counts[1] - (5.0 / 60.0)).abs() < 1e-12);

    let quantile = as_instant_vector(
        engine
            .instant_query(
                r#"histogram_quantile(0.5, request_duration_native_seconds{instance="a"})"#,
                120,
            )
            .unwrap(),
    );
    assert_eq!(quantile.len(), 1);
    assert_eq!(quantile[0].value, 1.0);
    assert!(quantile[0].histogram.is_none());
}

#[test]
fn unsupported_native_histogram_ops_fail_explicitly() {
    let storage = setup_native_histogram_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let arithmetic = engine
        .instant_query(
            r#"request_duration_native_seconds{instance="a"} + vector(1)"#,
            120,
        )
        .unwrap_err();
    assert!(matches!(arithmetic, PromqlError::Eval(msg) if msg.contains("histogram")));

    let mixed_sum = engine
        .instant_query("sum without (kind) (mixed_histogram_metric)", 120)
        .unwrap_err();
    assert!(
        matches!(mixed_sum, PromqlError::Eval(msg) if msg.contains("mixed float and histogram"))
    );
}

#[test]
fn aggregation_nan_ordering_matches_prometheus_semantics() {
    let storage = setup_nan_aggregation_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let quantile = as_instant_vector(
        engine
            .instant_query("quantile(0.5, nan_metric)", 60)
            .unwrap(),
    );
    assert_eq!(quantile.len(), 1);
    assert_eq!(quantile[0].value, 1.0);

    let top = as_instant_vector(engine.instant_query("topk(1, nan_metric)", 60).unwrap());
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].value, 2.0);

    let bottom = as_instant_vector(engine.instant_query("bottomk(1, nan_metric)", 60).unwrap());
    assert_eq!(bottom.len(), 1);
    assert_eq!(bottom[0].value, 1.0);

    let min = as_instant_vector(engine.instant_query("min(nan_metric)", 60).unwrap());
    assert_eq!(min.len(), 1);
    assert_eq!(min[0].value, 1.0);

    let max = as_instant_vector(engine.instant_query("max(nan_metric)", 60).unwrap());
    assert_eq!(max.len(), 1);
    assert_eq!(max[0].value, 2.0);
}

#[test]
fn math_and_predict_linear_functions_work() {
    let storage = setup_range_function_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let cos = as_instant_vector(engine.instant_query("cos(vector(0))", 0).unwrap());
    assert_eq!(cos.len(), 1);
    assert!((cos[0].value - 1.0).abs() < 1e-12);

    let sin = as_instant_vector(
        engine
            .instant_query("sin(vector(1.5707963267948966))", 0)
            .unwrap(),
    );
    assert_eq!(sin.len(), 1);
    assert!((sin[0].value - 1.0).abs() < 1e-12);

    let deg = as_instant_vector(
        engine
            .instant_query("deg(vector(3.141592653589793))", 0)
            .unwrap(),
    );
    assert_eq!(deg.len(), 1);
    assert!((deg[0].value - 180.0).abs() < 1e-9);

    let rad = as_instant_vector(engine.instant_query("rad(vector(180))", 0).unwrap());
    assert_eq!(rad.len(), 1);
    assert!((rad[0].value - std::f64::consts::PI).abs() < 1e-12);

    let predict = as_instant_vector(
        engine
            .instant_query("predict_linear(gauge_metric[3m], 60)", 180)
            .unwrap(),
    );
    assert_eq!(predict.len(), 1);
    assert!((predict[0].value - 480.0).abs() < 1e-9);

    let atan2 = as_instant_vector(
        engine
            .instant_query("vector(1) atan2 vector(1)", 0)
            .unwrap(),
    );
    assert_eq!(atan2.len(), 1);
    assert!((atan2[0].value - std::f64::consts::FRAC_PI_4).abs() < 1e-12);

    let mad = as_instant_vector(
        engine
            .instant_query("mad_over_time(reset_metric[3m])", 180)
            .unwrap(),
    );
    assert_eq!(mad.len(), 1);
    assert!((mad[0].value - 1.5).abs() < 1e-9);

    let des = as_instant_vector(
        engine
            .instant_query(
                "double_exponential_smoothing(gauge_metric[3m], 0.5, 0.5)",
                180,
            )
            .unwrap(),
    );
    assert_eq!(des.len(), 1);
    assert!((des[0].value - 480.0).abs() < 1e-9);

    let holt = as_instant_vector(
        engine
            .instant_query("holt_winters(gauge_metric[3m], 0.5, 0.5)", 180)
            .unwrap(),
    );
    assert_eq!(holt.len(), 1);
    assert!((holt[0].value - 480.0).abs() < 1e-9);
}

#[test]
fn scalar_scalar_comparison_requires_bool() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let err = engine.instant_query("1 < 2", 0).unwrap_err();
    assert!(matches!(err, PromqlError::Type(msg) if msg.contains("requires the bool modifier")));

    let (value, _) = as_scalar(engine.instant_query("1 < bool 2", 0).unwrap());
    assert_eq!(value, 1.0);
}

#[test]
fn unary_plus_and_clamp_invalid_bounds_work() {
    let storage = setup_storage();
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let samples = as_instant_vector(engine.instant_query("+vector(2)", 0).unwrap());
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].value, 2.0);

    let clamped = as_instant_vector(
        engine
            .instant_query("clamp(http_requests_total, 5, 1)", 600)
            .unwrap(),
    );
    assert!(clamped.is_empty());
}
