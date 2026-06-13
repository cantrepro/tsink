#![cfg(feature = "promql")]

use std::sync::Arc;

use tsink::{
    promql::{Engine, PromqlError, PromqlValue, Sample, Series},
    DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision,
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
