use tsink::{
    HistogramBucketSpan, HistogramCount, HistogramResetHint, NativeHistogram, TimestampPrecision,
    TsinkError,
};
use tsink_test::{
    classic_histogram, counter, counter_sequence, evenly_spaced_samples, gauge, label, labels,
    metric_series, native_histogram, sample, TsinkTestDb,
};

#[test]
fn basic_metric_helpers_feed_the_canonical_atomic_write_path() {
    let method_labels = labels([("method", "GET"), ("status", "200")]);
    let series = metric_series("requests_total", method_labels.clone());
    assert_eq!(series.name, "requests_total");
    assert_eq!(series.labels, method_labels);

    let rows = vec![
        counter("requests_total", series.labels.clone(), 10, 1.0),
        gauge("inflight", vec![label("worker", "a")], 10, 3.0),
        sample("build_info", vec![], 10, b"ready".as_slice()),
    ];
    let mut db = TsinkTestDb::builder()
        .in_memory()
        .timestamp_precision(TimestampPrecision::Seconds)
        .start()
        .unwrap();
    let write = db.write_atomic(&rows).unwrap();
    assert_eq!(write.accepted, rows.len());
    assert_eq!(
        db.select("requests_total", &series.labels, 0, 11).unwrap()[0].value_as_f64(),
        Some(1.0)
    );
    assert_eq!(
        db.select("build_info", &[], 0, 11).unwrap()[0].value_as_bytes(),
        Some("ready".as_bytes())
    );
    db.close().unwrap();
}

#[test]
fn sequence_helpers_are_checked_and_evenly_spaced() {
    let rows = evenly_spaced_samples("temperature", vec![], 100, 10, [1.0, 2.5, 4.0]).unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.data_point().timestamp, row.data_point().value_as_f64()))
            .collect::<Vec<_>>(),
        vec![(100, Some(1.0)), (110, Some(2.5)), (120, Some(4.0))]
    );

    let counters = counter_sequence("requests_total", vec![], 5, 5, 2.0, 3.0, 3).unwrap();
    assert_eq!(
        counters
            .iter()
            .map(|row| (row.data_point().timestamp, row.data_point().value_as_f64()))
            .collect::<Vec<_>>(),
        vec![(5, Some(2.0)), (10, Some(5.0)), (15, Some(8.0))]
    );

    assert!(matches!(
        evenly_spaced_samples("m", vec![], 0, 0, [1.0]),
        Err(TsinkError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        evenly_spaced_samples("m", vec![], i64::MAX, 1, [1.0, 2.0]),
        Err(TsinkError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        counter_sequence("m", vec![], 0, 1, -1.0, 1.0, 2),
        Err(TsinkError::InvalidConfiguration(_))
    ));
}

#[test]
fn classic_histogram_expands_to_prometheus_series_and_is_queryable() {
    let base_labels = labels([("route", "/v1")]);
    let rows = classic_histogram(
        "request_duration_seconds",
        &base_labels,
        30,
        &[(0.1, 2.0), (0.5, 4.0), (1.0, 5.0)],
        6.0,
        2.75,
    )
    .unwrap();
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0].metric(), "request_duration_seconds_bucket");
    assert_eq!(rows[0].labels().last(), Some(&label("le", "0.1")));
    assert_eq!(rows[3].labels().last(), Some(&label("le", "+Inf")));
    assert_eq!(rows[4].metric(), "request_duration_seconds_sum");
    assert_eq!(rows[5].metric(), "request_duration_seconds_count");

    let mut db = TsinkTestDb::builder()
        .in_memory()
        .timestamp_precision(TimestampPrecision::Seconds)
        .start()
        .unwrap();
    assert_eq!(db.write_atomic(&rows).unwrap().accepted, rows.len());
    assert_eq!(
        db.select(
            "request_duration_seconds_bucket",
            &labels([("route", "/v1"), ("le", "+Inf")]),
            0,
            31,
        )
        .unwrap()[0]
            .value_as_f64(),
        Some(6.0)
    );
    db.assert_promql_scalar(
        "scalar(request_duration_seconds_count{route=\"/v1\"})",
        30,
        6.0,
        0.0,
    )
    .unwrap();
    db.close().unwrap();
}

#[test]
fn classic_histogram_rejects_ambiguous_or_inconsistent_snapshots() {
    let invalid_cases = [
        classic_histogram("h", &[label("le", "1")], 1, &[], 0.0, 0.0),
        classic_histogram("h", &[], 1, &[(1.0, 2.0), (0.5, 2.0)], 2.0, 1.0),
        classic_histogram("h", &[], 1, &[(1.0, 2.0), (2.0, 1.0)], 2.0, 1.0),
        classic_histogram("h", &[], 1, &[(1.0, 3.0)], 2.0, 1.0),
        classic_histogram("h", &[], 1, &[(f64::INFINITY, 1.0)], 1.0, 1.0),
        classic_histogram("h", &[], 1, &[], f64::NAN, 0.0),
        classic_histogram("h", &[], 1, &[], 0.0, f64::INFINITY),
    ];
    assert!(invalid_cases
        .into_iter()
        .all(|result| matches!(result, Err(TsinkError::InvalidConfiguration(_)))));
}

#[test]
fn native_histogram_helper_preserves_the_first_class_payload() {
    let histogram = NativeHistogram {
        count: Some(HistogramCount::Int(4)),
        sum: 9.0,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(1)),
        negative_spans: vec![],
        negative_deltas: vec![],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![2, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![],
    };
    let row = native_histogram(
        "request_size_bytes",
        vec![label("route", "/upload")],
        42,
        histogram.clone(),
    );
    assert_eq!(row.data_point().value_as_histogram(), Some(&histogram));

    let mut db = TsinkTestDb::builder().in_memory().start().unwrap();
    assert_eq!(db.write_atomic(&[row]).unwrap().accepted, 1);
    let points = db
        .select("request_size_bytes", &[label("route", "/upload")], 0, 43)
        .unwrap();
    assert_eq!(points[0].value_as_histogram(), Some(&histogram));
    db.close().unwrap();
}
