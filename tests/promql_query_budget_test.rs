use std::sync::Arc;

use tsink::{
    promql::{Engine, PromqlError, PromqlValue},
    DataPoint, Label, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken,
    QueryLimitReason, QueryWorkLimits, Row, Storage, StorageBuilder, TimestampPrecision,
    TsinkError,
};

fn storage_with_limits(limits: QueryBudgetLimits) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_query_budget_limits(limits)
        .build()
        .unwrap()
}

fn selector_storage_with_limits(limits: QueryBudgetLimits) -> Arc<dyn Storage> {
    let storage = storage_with_limits(limits);
    storage
        .insert_rows(&[
            Row::with_labels(
                "http_requests_total",
                vec![Label::new("method", "GET")],
                DataPoint::new(0, 1.0),
            ),
            Row::with_labels(
                "http_requests_total",
                vec![Label::new("method", "GET")],
                DataPoint::new(60, 2.0),
            ),
        ])
        .unwrap();
    storage
}

fn assert_limit(error: PromqlError, reason: QueryLimitReason) {
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(
            exceeded
        ))) if exceeded.reason == reason
    ));
}

fn assert_cancelled(error: PromqlError) {
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::QueryBudget(QueryBudgetError::Cancelled))
    ));
}

#[test]
fn range_steps_accept_exact_bound_and_request_tightening_rejects_one_less() {
    let exact_storage = storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        per_query: QueryWorkLimits {
            max_steps: Some(3),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    let exact_engine = Engine::with_precision(exact_storage.clone(), TimestampPrecision::Seconds);

    let value = exact_engine.range_query("vector(1)", 0, 120, 60).unwrap();
    let PromqlValue::RangeVector(series) = value else {
        panic!("expected range vector");
    };
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].samples.len(), 3);
    let snapshot = exact_storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);

    let tightened_storage = storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        per_query: QueryWorkLimits {
            max_steps: Some(10),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    let tightened_engine =
        Engine::with_precision(tightened_storage.clone(), TimestampPrecision::Seconds);
    let error = tightened_engine
        .range_query_with_control(
            "vector(1)",
            0,
            120,
            60,
            QueryWorkLimits {
                max_steps: Some(2),
                ..QueryWorkLimits::default()
            },
            QueryCancellationToken::new(),
        )
        .unwrap_err();
    assert_limit(error, QueryLimitReason::Steps);
    let snapshot = tightened_storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
}

#[test]
fn subquery_steps_share_the_top_level_execution_and_have_an_exact_bound() {
    for (limit, succeeds) in [(4, true), (3, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            per_query: QueryWorkLimits {
                max_steps: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
        let result = engine.instant_query("vector(1)[2m:1m]", 120);
        if succeeds {
            let PromqlValue::RangeVector(series) = result.unwrap() else {
                panic!("expected range vector");
            };
            assert_eq!(series.len(), 1);
            assert_eq!(series[0].samples.len(), 3);
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::Steps);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
    }
}

#[test]
fn range_prefetch_and_selector_reads_do_not_acquire_nested_permits() {
    let storage = selector_storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        ..QueryBudgetLimits::default()
    });
    let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);

    let value = engine
        .range_query(r#"http_requests_total{method="GET"}"#, 0, 60, 60)
        .unwrap();
    let PromqlValue::RangeVector(series) = value else {
        panic!("expected range vector");
    };
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].samples.len(), 2);

    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.peak_active_queries, 1);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.concurrency_rejections_total, 0);
}

#[test]
fn selector_errors_release_the_owned_permit_and_query_memory() {
    let storage = selector_storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        max_shared_memory_bytes: Some(1_000_000),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1_000_000),
            ..QueryWorkLimits::default()
        },
    });
    let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);

    engine
        .instant_query(r#"http_requests_total{method=~"["}"#, 60)
        .unwrap_err();

    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
}

#[test]
fn caller_owned_execution_cancels_without_nested_admission_and_releases_on_drop() {
    let storage = storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        max_shared_memory_bytes: Some(1_000_000),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1_000_000),
            ..QueryWorkLimits::default()
        },
    });
    let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
    let cancellation = QueryCancellationToken::new();
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), cancellation.clone())
        .unwrap()
        .expect("built-in storage exposes a query execution");
    assert_eq!(storage.query_budget_snapshot().active_queries, 1);

    cancellation.cancel();
    let error = engine
        .range_query_with_execution("vector(1)", 0, 120, 60, &execution)
        .unwrap_err();
    assert_cancelled(error);
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 1);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.cancellations_total, 1);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    drop(execution);
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
}

#[test]
fn promql_result_bytes_accept_exact_modeled_bound_and_reject_one_less() {
    let calibration_storage = storage_with_limits(QueryBudgetLimits::default());
    let calibration_engine =
        Engine::with_precision(calibration_storage.clone(), TimestampPrecision::Seconds);
    let execution = calibration_storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    calibration_engine
        .instant_query_with_execution("vector(1)", 0, &execution)
        .unwrap();
    let modeled_bytes = execution.snapshot().returned_bytes;
    assert!(modeled_bytes > 1);
    drop(execution);

    for (limit, succeeds) in [(modeled_bytes, true), (modeled_bytes - 1, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
        let result = engine.instant_query("vector(1)", 0);
        if succeeds {
            result.unwrap();
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::ReturnedBytes);
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    }
}

#[test]
fn promql_memory_accepts_exact_modeled_peak_and_rejects_one_less() {
    let calibration_storage = storage_with_limits(QueryBudgetLimits::default());
    let calibration_engine =
        Engine::with_precision(calibration_storage.clone(), TimestampPrecision::Seconds);
    calibration_engine.instant_query("vector(1)", 0).unwrap();
    let modeled_peak = calibration_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(modeled_peak > 1);

    for (limit, succeeds) in [(modeled_peak, true), (modeled_peak - 1, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
        let result = engine.instant_query("vector(1)", 0);
        if succeeds {
            result.unwrap();
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.queries_completed_total, 1);
    }
}

#[test]
fn binary_transform_pre_admits_the_combined_intermediate_vector() {
    for (limit, succeeds) in [(2, true), (1, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
        let result = engine.instant_query("vector(1) + vector(2)", 0);
        if succeeds {
            result.unwrap();
        } else {
            assert_limit(
                result.unwrap_err(),
                QueryLimitReason::IntermediateVectorSize,
            );
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }
}

#[test]
fn aggregation_transform_accepts_its_exact_modeled_peak_and_rejects_one_less() {
    let calibration_storage = storage_with_limits(QueryBudgetLimits::default());
    let calibration_engine =
        Engine::with_precision(calibration_storage.clone(), TimestampPrecision::Seconds);
    calibration_engine
        .instant_query("sum(vector(1))", 0)
        .unwrap();
    let modeled_peak = calibration_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;

    let baseline_storage = storage_with_limits(QueryBudgetLimits::default());
    let baseline_engine =
        Engine::with_precision(baseline_storage.clone(), TimestampPrecision::Seconds);
    baseline_engine.instant_query("vector(1)", 0).unwrap();
    let baseline_peak = baseline_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(modeled_peak > baseline_peak);

    for (limit, succeeds) in [(modeled_peak, true), (modeled_peak - 1, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            max_concurrent_queries: None,
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
        });
        let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
        let result = engine.instant_query("sum(vector(1))", 0);
        if succeeds {
            result.unwrap();
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.queries_completed_total, 1);
    }
}
