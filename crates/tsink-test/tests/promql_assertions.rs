use tsink::promql::types::{PromqlValue, Sample, Series};
use tsink::promql::MAX_PARSE_INPUT_BYTES;
use tsink::{DataPoint, Label, QueryLimitReason, Row, TimestampPrecision};
use tsink_test::{
    PromqlErrorExpectation, TsinkTestDb, MAX_DIAGNOSTIC_MESSAGE_BYTES,
    MAX_PROMQL_ASSERTION_DIAGNOSTIC_BYTES,
};

fn ordered_fixture() -> TsinkTestDb {
    let db = TsinkTestDb::builder()
        .in_memory()
        .timestamp_precision(TimestampPrecision::Seconds)
        .start()
        .unwrap();
    db.write_atomic(&[
        Row::with_labels(
            "ordered_metric",
            vec![Label::new("zone", "west"), Label::new("host", "b")],
            DataPoint::new(10, 1.0),
        ),
        Row::with_labels(
            "ordered_metric",
            vec![Label::new("host", "a"), Label::new("zone", "east")],
            DataPoint::new(10, 3.0),
        ),
        Row::with_labels(
            "ordered_metric",
            vec![Label::new("host", "b"), Label::new("zone", "west")],
            DataPoint::new(20, 2.0),
        ),
        Row::with_labels(
            "ordered_metric",
            vec![Label::new("zone", "east"), Label::new("host", "a")],
            DataPoint::new(20, 4.0),
        ),
    ])
    .unwrap();
    db
}

#[test]
fn instant_and_range_equality_normalize_series_labels_and_samples() {
    let mut db = ordered_fixture();

    let instant = db
        .assert_promql_instant_eq(
            "ordered_metric",
            20,
            PromqlValue::InstantVector(vec![
                Sample {
                    metric: "ordered_metric".to_string(),
                    labels: vec![Label::new("zone", "west"), Label::new("host", "b")],
                    timestamp: 20,
                    value: 2.0,
                    histogram: None,
                },
                Sample {
                    metric: "ordered_metric".to_string(),
                    labels: vec![Label::new("zone", "east"), Label::new("host", "a")],
                    timestamp: 20,
                    value: 4.0,
                    histogram: None,
                },
            ]),
        )
        .unwrap();
    assert_eq!(instant.as_instant_vector().unwrap().len(), 2);

    let range = db
        .assert_promql_range_eq(
            "ordered_metric",
            10,
            20,
            10,
            PromqlValue::RangeVector(vec![
                Series {
                    metric: "ordered_metric".to_string(),
                    labels: vec![Label::new("zone", "west"), Label::new("host", "b")],
                    samples: vec![(20, 2.0), (10, 1.0)],
                    histograms: vec![],
                },
                Series {
                    metric: "ordered_metric".to_string(),
                    labels: vec![Label::new("zone", "east"), Label::new("host", "a")],
                    samples: vec![(20, 4.0), (10, 3.0)],
                    histograms: vec![],
                },
            ]),
        )
        .unwrap();
    assert_eq!(range.as_range_vector().unwrap().len(), 2);

    db.close().unwrap();
}

#[test]
fn duplicate_series_expectations_are_rejected_independent_of_input_order() {
    let mut db = ordered_fixture();
    let first = Sample {
        metric: "ordered_metric".to_string(),
        labels: vec![Label::new("host", "a"), Label::new("zone", "east")],
        timestamp: 20,
        value: 4.0,
        histogram: None,
    };
    let mut second = first.clone();
    second.value = 5.0;

    for expected in [
        PromqlValue::InstantVector(vec![first.clone(), second.clone()]),
        PromqlValue::InstantVector(vec![second, first]),
    ] {
        let error = db
            .assert_promql_instant_eq("ordered_metric", 20, expected)
            .unwrap_err();
        assert!(error.diagnostic().contains(
            "invalid expected value: instant vector contains a duplicate series identity"
        ));
    }

    let first_series = Series {
        metric: "ordered_metric".to_string(),
        labels: vec![Label::new("host", "a"), Label::new("zone", "east")],
        samples: vec![(10, 3.0), (20, 4.0)],
        histograms: vec![],
    };
    let mut second_series = first_series.clone();
    second_series.samples = vec![(10, 30.0), (20, 40.0)];
    for expected in [
        PromqlValue::RangeVector(vec![first_series.clone(), second_series.clone()]),
        PromqlValue::RangeVector(vec![second_series, first_series]),
    ] {
        let error = db
            .assert_promql_range_eq("ordered_metric", 10, 20, 10, expected)
            .unwrap_err();
        assert!(error
            .diagnostic()
            .contains("invalid expected value: range vector contains a duplicate series identity"));
    }

    let duplicate_timestamps = PromqlValue::RangeVector(vec![Series {
        metric: "ordered_metric".to_string(),
        labels: vec![Label::new("host", "a"), Label::new("zone", "east")],
        samples: vec![(10, 3.0), (10, 4.0)],
        histograms: vec![],
    }]);
    let error = db
        .assert_promql_range_eq("ordered_metric", 10, 20, 10, duplicate_timestamps)
        .unwrap_err();
    assert!(error
        .diagnostic()
        .contains("range series contains duplicate float-sample timestamps"));

    db.close().unwrap();
}

#[test]
fn scalar_tolerance_has_an_exact_boundary_and_returns_bounded_nearby_diagnostics() {
    let mut db = ordered_fixture();

    let actual = db
        .assert_promql_scalar("scalar(sum(ordered_metric))", 20, 6.5, 0.5)
        .unwrap();
    assert_eq!(actual.as_scalar(), Some((6.0, 20)));

    let error = db
        .assert_promql_scalar("scalar(sum(ordered_metric))", 20, 6.5, 0.499)
        .unwrap_err();
    assert!(error.diagnostic().len() <= MAX_PROMQL_ASSERTION_DIAGNOSTIC_BYTES);
    assert!(error.diagnostic().contains("query:"));
    assert!(error.diagnostic().contains("instant at 20"));
    assert!(error.diagnostic().contains("expected:"));
    assert!(error.diagnostic().contains("actual:"));
    assert!(error.diagnostic().contains("nearby stored series:"));
    assert!(error.diagnostic().contains("ordered_metric"));

    let invalid_tolerance = db
        .assert_promql_scalar("scalar(sum(ordered_metric))", 20, 6.0, -1.0)
        .unwrap_err();
    assert!(invalid_tolerance
        .diagnostic()
        .contains("tolerance must be finite and non-negative"));

    db.close().unwrap();
}

#[test]
fn empty_and_nonempty_helpers_accept_only_vector_or_matrix_results() {
    let mut db = ordered_fixture();

    db.assert_promql_instant_empty("missing_metric", 20)
        .unwrap();
    db.assert_promql_instant_nonempty("ordered_metric", 20)
        .unwrap();
    db.assert_promql_range_empty("missing_metric", 10, 20, 10)
        .unwrap();
    db.assert_promql_range_nonempty("ordered_metric", 10, 20, 10)
        .unwrap();

    let scalar_error = db.assert_promql_instant_nonempty("1", 20).unwrap_err();
    assert!(scalar_error
        .diagnostic()
        .contains("non-empty vector or matrix"));

    db.close().unwrap();
}

#[test]
fn expected_errors_match_structured_parse_function_and_query_limit_variants() {
    let mut db = ordered_fixture();

    db.assert_promql_instant_error("{}", 20, PromqlErrorExpectation::Parse)
        .unwrap();
    db.assert_promql_instant_error(
        "not_a_real_promql_function()",
        20,
        PromqlErrorExpectation::UnknownFunction,
    )
    .unwrap();
    db.assert_promql_range_error(
        "ordered_metric",
        0,
        1_000_000,
        1,
        PromqlErrorExpectation::QueryLimit(QueryLimitReason::Steps),
    )
    .unwrap();

    db.close().unwrap();
}

#[test]
fn oversized_multibyte_query_and_ring_messages_remain_within_public_caps() {
    let mut db = ordered_fixture();
    let oversized_query = "é".repeat(MAX_PARSE_INPUT_BYTES / 2 + 1);
    let error = db
        .assert_promql_instant_error(
            &oversized_query,
            20,
            PromqlErrorExpectation::UnknownFunction,
        )
        .unwrap_err();

    assert!(error.diagnostic().len() <= MAX_PROMQL_ASSERTION_DIAGNOSTIC_BYTES);
    assert!(error
        .diagnostic()
        .is_char_boundary(error.diagnostic().len()));
    for required in [
        "query:",
        "evaluation: instant at 20",
        "expected: unknown-function error",
        "actual: parse error:",
        "nearby stored series:",
        "ordered_metric",
    ] {
        assert!(
            error.diagnostic().contains(required),
            "missing {required:?} in {}",
            error.diagnostic()
        );
    }
    assert!(db
        .diagnostics()
        .iter()
        .all(|entry| entry.message.len() <= MAX_DIAGNOSTIC_MESSAGE_BYTES));

    db.close().unwrap();
}
