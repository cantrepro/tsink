use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tsink::{
    promql::{Engine, PromqlError, PromqlValue},
    DataPoint, Label, MetricSeries, QueryBudget, QueryBudgetError, QueryBudgetLimits,
    QueryCancellationToken, QueryExecution, QueryExecutionAccounting, QueryLimitReason,
    QueryOptions, QueryWorkLimits, Row, SelectManyExecutionResult, SelectSeriesExecutionResult,
    SeriesPoints, SeriesSelection, Storage, StorageBuilder, TimestampPrecision, TsinkError,
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

struct MetadataAccountingStorage {
    inner: Arc<dyn Storage>,
    accounting: QueryExecutionAccounting,
}

#[derive(Default)]
struct BatchReservationProbe {
    calls: AtomicUsize,
    first_call_reserved_bytes: AtomicU64,
    second_call_reserved_bytes: AtomicU64,
}

struct PointAccountingStorage {
    inner: Arc<dyn Storage>,
    accounting: QueryExecutionAccounting,
    forward_detailed_result: bool,
    probe: Option<Arc<BatchReservationProbe>>,
}

impl Storage for PointAccountingStorage {
    fn query_budget(&self) -> Option<QueryBudget> {
        self.inner.query_budget()
    }

    fn insert_rows(&self, rows: &[Row]) -> tsink::Result<()> {
        self.inner.insert_rows(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> tsink::Result<Vec<DataPoint>> {
        self.inner.select(metric, labels, start, end)
    }

    fn select_many_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> tsink::Result<Vec<SeriesPoints>> {
        self.inner
            .select_many_with_execution(series, start, end, execution)
    }

    fn select_many_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> tsink::Result<SelectManyExecutionResult> {
        if let Some(probe) = self.probe.as_ref() {
            match probe.calls.fetch_add(1, Ordering::SeqCst) {
                0 => probe
                    .first_call_reserved_bytes
                    .store(execution.snapshot().memory_reserved_bytes, Ordering::SeqCst),
                1 => probe
                    .second_call_reserved_bytes
                    .store(execution.snapshot().memory_reserved_bytes, Ordering::SeqCst),
                _ => {}
            }
        }
        if self.forward_detailed_result {
            self.inner
                .select_many_with_execution_result(series, start, end, execution)
        } else {
            self.inner
                .select_many_with_execution(series, start, end, execution)
                .map(SelectManyExecutionResult::unaccounted)
        }
    }

    fn select_many_execution_accounting(&self) -> QueryExecutionAccounting {
        self.accounting
    }

    fn select_with_options(
        &self,
        metric: &str,
        options: QueryOptions,
    ) -> tsink::Result<Vec<DataPoint>> {
        self.inner.select_with_options(metric, options)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.inner.select_all(metric, start, end)
    }

    fn select_series(&self, selection: &SeriesSelection) -> tsink::Result<Vec<MetricSeries>> {
        self.inner.select_series(selection)
    }

    fn select_series_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> tsink::Result<Vec<MetricSeries>> {
        self.inner
            .select_series_with_execution(selection, execution)
    }

    fn select_series_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> tsink::Result<SelectSeriesExecutionResult> {
        self.inner
            .select_series_with_execution_result(selection, execution)
    }

    fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Complete
    }

    fn close(&self) -> tsink::Result<()> {
        Ok(())
    }
}

impl Storage for MetadataAccountingStorage {
    fn query_budget(&self) -> Option<QueryBudget> {
        self.inner.query_budget()
    }

    fn insert_rows(&self, rows: &[Row]) -> tsink::Result<()> {
        self.inner.insert_rows(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> tsink::Result<Vec<DataPoint>> {
        self.inner.select(metric, labels, start, end)
    }

    fn select_with_options(
        &self,
        metric: &str,
        options: QueryOptions,
    ) -> tsink::Result<Vec<DataPoint>> {
        self.inner.select_with_options(metric, options)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.inner.select_all(metric, start, end)
    }

    fn list_metrics(&self) -> tsink::Result<Vec<MetricSeries>> {
        self.inner.list_metrics()
    }

    fn select_series(&self, selection: &SeriesSelection) -> tsink::Result<Vec<MetricSeries>> {
        self.inner.select_series(selection)
    }

    fn select_series_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> tsink::Result<Vec<MetricSeries>> {
        self.inner
            .select_series_with_execution(selection, execution)
    }

    fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
        self.accounting
    }

    fn close(&self) -> tsink::Result<()> {
        Ok(())
    }
}

fn info_storage_with_limits(limits: QueryBudgetLimits) -> Arc<dyn Storage> {
    let storage = storage_with_limits(limits);
    storage
        .insert_rows(&[
            Row::with_labels(
                "up",
                vec![Label::new("instance", "a"), Label::new("job", "api")],
                DataPoint::new(60, 1.0),
            ),
            Row::with_labels(
                "target_info",
                vec![
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                    Label::new("team", "platform"),
                ],
                DataPoint::new(60, 1.0),
            ),
            Row::with_labels(
                "build_info",
                vec![
                    Label::new("build_version", "1.2.3"),
                    Label::new("instance", "a"),
                    Label::new("job", "api"),
                ],
                DataPoint::new(60, 1.0),
            ),
        ])
        .unwrap();
    storage
}

fn two_metric_storage_with_limits(limits: QueryBudgetLimits) -> Arc<dyn Storage> {
    let storage = storage_with_limits(limits);
    storage
        .insert_rows(&[
            Row::new("metric_a", DataPoint::new(0, 1.0)),
            Row::new("metric_a", DataPoint::new(60, 2.0)),
            Row::new("metric_b", DataPoint::new(0, 3.0)),
            Row::new("metric_b", DataPoint::new(60, 4.0)),
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
fn bounded_promql_rejects_unaccounted_metadata_backends() {
    let inner = selector_storage_with_limits(QueryBudgetLimits::default());
    let storage: Arc<dyn Storage> = Arc::new(MetadataAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Unaccounted,
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let error = engine
        .instant_query(r#"{method="GET"}"#, 60)
        .expect_err("bounded metadata selection must reject an unaccounted backend");
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL metadata selection",
            ..
        })
    ));
}

#[test]
fn bounded_promql_rejects_complete_metadata_claim_without_a_reservation() {
    let inner = selector_storage_with_limits(QueryBudgetLimits::default());
    let storage: Arc<dyn Storage> = Arc::new(MetadataAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Complete,
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let error = engine
        .instant_query(r#"{method="GET"}"#, 60)
        .expect_err("a false Complete claim must not release an unreserved result");
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::Other(message))
            if message.contains("omitted its result reservation")
    ));
}

#[test]
fn bounded_promql_rejects_unaccounted_point_backends() {
    let inner = selector_storage_with_limits(QueryBudgetLimits::default());
    let probe = Arc::new(BatchReservationProbe::default());
    let storage: Arc<dyn Storage> = Arc::new(PointAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Unaccounted,
        forward_detailed_result: false,
        probe: Some(Arc::clone(&probe)),
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let error = engine
        .instant_query(r#"http_requests_total{method=~"GET"}"#, 60)
        .expect_err("bounded point selection must reject an unaccounted backend");
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL point selection",
            ..
        })
    ));
    assert_eq!(
        probe.calls.load(Ordering::SeqCst),
        0,
        "fail-closed accounting must reject before invoking the point backend"
    );
}

#[test]
fn bounded_promql_exact_selector_rejects_unaccounted_point_backends() {
    let inner = selector_storage_with_limits(QueryBudgetLimits::default());
    let probe = Arc::new(BatchReservationProbe::default());
    let storage: Arc<dyn Storage> = Arc::new(PointAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Unaccounted,
        forward_detailed_result: false,
        probe: Some(Arc::clone(&probe)),
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let error = engine
        .instant_query(r#"http_requests_total{method="GET"}"#, 60)
        .expect_err("bounded exact-series selection must reject an unaccounted point backend");
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL point selection",
            ..
        })
    ));
    assert_eq!(
        probe.calls.load(Ordering::SeqCst),
        0,
        "the exact-series fast path must fail closed before invoking the point backend"
    );
}

#[test]
fn bounded_promql_rejects_complete_point_claim_without_a_reservation() {
    let inner = selector_storage_with_limits(QueryBudgetLimits::default());
    let storage: Arc<dyn Storage> = Arc::new(PointAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Complete,
        forward_detailed_result: false,
        probe: None,
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let error = engine
        .instant_query(r#"http_requests_total{method=~"GET"}"#, 60)
        .expect_err("a false Complete point claim must not release an unreserved result");
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::Other(message))
            if message.contains("omitted selector-existence bits")
                || message.contains("omitted its result reservation")
    ));
}

#[test]
fn bounded_promql_exact_selector_rejects_complete_claim_without_a_reservation() {
    let inner = selector_storage_with_limits(QueryBudgetLimits::default());
    let storage: Arc<dyn Storage> = Arc::new(PointAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Complete,
        forward_detailed_result: false,
        probe: None,
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let error = engine
        .instant_query(r#"http_requests_total{method="GET"}"#, 60)
        .expect_err("a false Complete exact-series claim must not release an unguarded result");
    assert!(matches!(
        error,
        PromqlError::Storage(TsinkError::Other(message))
            if message.contains("omitted selector-existence bits")
                || message.contains("omitted its result reservation")
    ));
}

#[test]
fn exact_selector_retains_the_first_point_guard_while_loading_the_second() {
    let inner = selector_storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        max_shared_memory_bytes: Some(1_000_000),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1_000_000),
            ..QueryWorkLimits::default()
        },
    });
    let probe = Arc::new(BatchReservationProbe::default());
    let storage: Arc<dyn Storage> = Arc::new(PointAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Complete,
        forward_detailed_result: true,
        probe: Some(Arc::clone(&probe)),
    });
    let engine = Engine::with_precision(storage, TimestampPrecision::Seconds);

    let value = engine
        .instant_query(
            r#"http_requests_total{method="GET"} + http_requests_total{method="GET"}"#,
            60,
        )
        .unwrap();
    assert!(matches!(
        value,
        PromqlValue::InstantVector(samples) if samples.len() == 1
    ));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert!(
        probe.second_call_reserved_bytes.load(Ordering::SeqCst)
            > probe.first_call_reserved_bytes.load(Ordering::SeqCst),
        "the first exact-series point reservation must remain live through the second read"
    );
}

#[test]
fn range_prefetch_retains_the_first_batch_result_while_loading_the_second() {
    let inner = two_metric_storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        max_shared_memory_bytes: Some(1_000_000),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1_000_000),
            ..QueryWorkLimits::default()
        },
    });
    let probe = Arc::new(BatchReservationProbe::default());
    let storage: Arc<dyn Storage> = Arc::new(PointAccountingStorage {
        inner,
        accounting: QueryExecutionAccounting::Complete,
        forward_detailed_result: true,
        probe: Some(Arc::clone(&probe)),
    });
    let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);

    engine
        .range_query("metric_a + metric_b", 0, 60, 60)
        .unwrap();

    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    let first = probe.first_call_reserved_bytes.load(Ordering::SeqCst);
    let second = probe.second_call_reserved_bytes.load(Ordering::SeqCst);
    assert!(
        second > first,
        "the first prefetch result reservation must remain live while the second batch starts: first={first}, second={second}"
    );
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

#[test]
fn range_prefetch_adoption_accepts_exact_modeled_peak_and_rejects_one_less() {
    let calibration = two_metric_storage_with_limits(QueryBudgetLimits::default());
    let engine = Engine::with_precision(Arc::clone(&calibration), TimestampPrecision::Seconds);
    engine
        .range_query("metric_a + metric_b", 0, 60, 60)
        .unwrap();
    let exact_peak = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_peak > 1);

    for (limit, succeeds) in [(exact_peak, true), (exact_peak - 1, false)] {
        let storage = two_metric_storage_with_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
        let result = engine.range_query("metric_a + metric_b", 0, 60, 60);
        if succeeds {
            result.unwrap();
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
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
fn info_reads_reuse_the_top_level_execution_and_release_all_resources() {
    let storage = info_storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        max_shared_memory_bytes: Some(1_000_000),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1_000_000),
            ..QueryWorkLimits::default()
        },
    });
    let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);

    let PromqlValue::InstantVector(vector) = engine.instant_query("info(up)", 60).unwrap() else {
        panic!("expected instant vector");
    };
    assert_eq!(vector.len(), 1);
    assert!(vector[0]
        .labels
        .iter()
        .any(|label| label.name == "team" && label.value == "platform"));

    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.peak_active_queries, 1);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.concurrency_rejections_total, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

#[test]
fn info_map_and_label_merge_accept_the_exact_modeled_peak_and_reject_one_less() {
    let calibration = info_storage_with_limits(QueryBudgetLimits::default());
    let engine = Engine::with_precision(Arc::clone(&calibration), TimestampPrecision::Seconds);
    let PromqlValue::InstantVector(vector) = engine.instant_query("info(up)", 60).unwrap() else {
        panic!("expected instant vector");
    };
    assert_eq!(vector.len(), 1);
    assert!(vector[0]
        .labels
        .iter()
        .any(|label| label.name == "team" && label.value == "platform"));
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 0);

    for (limit, succeeds) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = info_storage_with_limits(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
        });
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
        let result = engine.instant_query("info(up)", 60);
        if succeeds {
            let PromqlValue::InstantVector(vector) = result.unwrap() else {
                panic!("expected instant vector");
            };
            assert_eq!(vector.len(), 1);
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn info_metric_listing_shares_request_tightened_work_limits() {
    const QUERY: &str = r#"info(up, {__name__=~"(target|build)_info"})"#;

    let calibration_storage = info_storage_with_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        ..QueryBudgetLimits::default()
    });
    let calibration_engine =
        Engine::with_precision(calibration_storage.clone(), TimestampPrecision::Seconds);
    let execution = calibration_storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .expect("built-in storage exposes a query execution");
    calibration_engine
        .instant_query_with_execution(QUERY, 60, &execution)
        .unwrap();
    let exact_series_matched = execution.snapshot().series_matched;
    assert!(exact_series_matched > 1);
    drop(execution);

    let snapshot = calibration_storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.concurrency_rejections_total, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    for (limit, succeeds) in [
        (exact_series_matched, true),
        (exact_series_matched - 1, false),
    ] {
        let storage = info_storage_with_limits(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            per_query: QueryWorkLimits {
                max_series_matched: Some(exact_series_matched + 10),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(storage.clone(), TimestampPrecision::Seconds);
        let result = engine.instant_query_with_control(
            QUERY,
            60,
            QueryWorkLimits {
                max_series_matched: Some(limit),
                ..QueryWorkLimits::default()
            },
            QueryCancellationToken::new(),
        );
        if succeeds {
            result.unwrap();
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::SeriesMatched);
        }

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.peak_active_queries, 1);
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.concurrency_rejections_total, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
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
fn string_literal_clone_accepts_its_exact_pre_reserved_peak_and_rejects_one_less() {
    let literal = "s".repeat(4_096);
    let query = format!(r#""{literal}""#);
    let calibration_storage = storage_with_limits(QueryBudgetLimits::default());
    let calibration_engine = Engine::with_precision(
        Arc::clone(&calibration_storage),
        TimestampPrecision::Seconds,
    );
    assert!(matches!(
        calibration_engine.instant_query(&query, 0).unwrap(),
        PromqlValue::String(value, 0) if value == literal
    ));
    let exact_peak = calibration_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_peak > u64::try_from(literal.len()).unwrap());

    for (limit, succeeds) in [(exact_peak, true), (exact_peak - 1, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
        let result = engine.instant_query(&query, 0);
        if succeeds {
            assert!(matches!(
                result.unwrap(),
                PromqlValue::String(value, 0) if value == literal
            ));
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn detailed_promql_result_retains_memory_after_intermediate_tracker_drops() {
    let storage = storage_with_limits(QueryBudgetLimits::default());
    let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();

    let result = engine
        .instant_query_with_execution_result("vector(1)", 0, &execution)
        .unwrap();
    assert!(matches!(
        result.value(),
        PromqlValue::InstantVector(vector) if vector.len() == 1
    ));
    assert!(result.reserved_memory_bytes() > 0);
    assert_eq!(
        execution.snapshot().memory_reserved_bytes,
        result.reserved_memory_bytes(),
        "only the retained result guard must remain after evaluator intermediates drop"
    );

    let (value, result_reservation) = result.into_parts();
    assert!(matches!(
        value,
        PromqlValue::InstantVector(vector) if vector.len() == 1
    ));
    assert_eq!(
        execution.snapshot().memory_reserved_bytes,
        result_reservation.bytes()
    );
    drop(result_reservation);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    drop(execution);
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

#[test]
fn detailed_promql_result_handoff_has_an_exact_memory_boundary() {
    let calibration_storage = storage_with_limits(QueryBudgetLimits::default());
    let calibration_engine = Engine::with_precision(
        Arc::clone(&calibration_storage),
        TimestampPrecision::Seconds,
    );
    let calibration_execution = calibration_storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let calibration_result = calibration_engine
        .instant_query_with_execution_result("vector(1)", 0, &calibration_execution)
        .unwrap();
    let exact_peak = calibration_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_peak > calibration_result.reserved_memory_bytes());
    drop(calibration_result);
    drop(calibration_execution);

    for (limit, succeeds) in [(exact_peak, true), (exact_peak - 1, false)] {
        let storage = storage_with_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = engine.instant_query_with_execution_result("vector(1)", 0, &execution);
        if succeeds {
            let retained = result.unwrap();
            assert_eq!(
                execution.snapshot().memory_reserved_bytes,
                retained.reserved_memory_bytes()
            );
            drop(retained);
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
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

fn label_replace_storage_with_limits(
    limits: QueryBudgetLimits,
    source_value: &str,
) -> Arc<dyn Storage> {
    let storage = storage_with_limits(limits);
    storage
        .insert_rows(&[Row::with_labels(
            "replace_source",
            vec![Label::new("src", source_value)],
            DataPoint::new(0, 1.0),
        )])
        .unwrap();
    storage
}

#[test]
fn label_replace_many_capture_amplification_is_preflighted_and_has_an_exact_query_peak() {
    let source_value = "a".repeat(4_096);
    let pattern = "()".repeat(64);
    let replacement = "x".repeat(512);
    let amplified_query =
        format!(r#"label_replace(replace_source, "dst", "{replacement}", "src", "{pattern}")"#);
    let baseline_query =
        format!(r#"label_replace(replace_source, "dst", "x", "src", "{pattern}")"#);

    let baseline_storage =
        label_replace_storage_with_limits(QueryBudgetLimits::default(), &source_value);
    let baseline_engine =
        Engine::with_precision(Arc::clone(&baseline_storage), TimestampPrecision::Seconds);
    baseline_engine
        .instant_query(&baseline_query, 0)
        .expect("the many-capture regex must compile and execute");
    let baseline_peak = baseline_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;

    let calibration_storage =
        label_replace_storage_with_limits(QueryBudgetLimits::default(), &source_value);
    let calibration_engine = Engine::with_precision(
        Arc::clone(&calibration_storage),
        TimestampPrecision::Seconds,
    );
    let PromqlValue::InstantVector(calibration_vector) = calibration_engine
        .instant_query(&amplified_query, 0)
        .expect("unbounded calibration must succeed")
    else {
        panic!("expected an instant vector");
    };
    let expected_value_len = source_value
        .len()
        .saturating_add((source_value.len() + 1).saturating_mul(replacement.len()));
    assert_eq!(
        calibration_vector[0]
            .labels
            .iter()
            .find(|label| label.name == "dst")
            .unwrap()
            .value
            .len(),
        expected_value_len
    );
    let exact_peak = calibration_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(
        exact_peak > baseline_peak,
        "replacement amplification must dominate the regex-compilation peak"
    );

    // The transform reservation contains at least three times this exact output length to cover
    // String growth. A limit one byte below that component alone must therefore reject at the
    // preflight reserve, before replace_all can construct the amplified value.
    let preflight_limit = u64::try_from(expected_value_len)
        .unwrap()
        .saturating_mul(3)
        .saturating_sub(1);
    assert!(
        preflight_limit > baseline_peak,
        "the constrained run must pass regex compilation before reaching transform preflight"
    );
    let preflight_storage = label_replace_storage_with_limits(
        QueryBudgetLimits {
            max_shared_memory_bytes: Some(preflight_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(preflight_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        },
        &source_value,
    );
    let preflight_engine =
        Engine::with_precision(Arc::clone(&preflight_storage), TimestampPrecision::Seconds);
    assert_limit(
        preflight_engine
            .instant_query(&amplified_query, 0)
            .unwrap_err(),
        QueryLimitReason::PerQueryMemoryBytes,
    );
    let snapshot = preflight_storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);

    for (limit, succeeds) in [(exact_peak, true), (exact_peak - 1, false)] {
        let storage = label_replace_storage_with_limits(
            QueryBudgetLimits {
                max_shared_memory_bytes: Some(limit),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(limit),
                    ..QueryWorkLimits::default()
                },
                ..QueryBudgetLimits::default()
            },
            &source_value,
        );
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
        let result = engine.instant_query(&amplified_query, 0);
        if succeeds {
            let PromqlValue::InstantVector(vector) = result.unwrap() else {
                panic!("expected an instant vector");
            };
            assert_eq!(
                vector[0]
                    .labels
                    .iter()
                    .find(|label| label.name == "dst")
                    .unwrap()
                    .value
                    .len(),
                expected_value_len
            );
        } else {
            assert_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

fn sort_storage_with_limits(limits: QueryBudgetLimits, series_count: usize) -> Arc<dyn Storage> {
    let storage = storage_with_limits(limits);
    let rows = (0..series_count)
        .map(|index| {
            Row::with_labels(
                "sort_source",
                vec![Label::new("rank", index.to_string())],
                DataPoint::new(0, (series_count - index) as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
    storage
}

#[test]
fn stable_sort_scratch_is_admitted_before_sorting() {
    let series_count = 128;
    let calibration_storage = sort_storage_with_limits(QueryBudgetLimits::default(), series_count);
    let calibration_engine = Engine::with_precision(
        Arc::clone(&calibration_storage),
        TimestampPrecision::Seconds,
    );
    calibration_engine
        .instant_query("sort_source", 0)
        .expect("baseline selector must succeed");
    let exact_input_peak = calibration_storage
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_input_peak > 0);

    let limits = QueryBudgetLimits {
        max_shared_memory_bytes: Some(exact_input_peak),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(exact_input_peak),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    };
    let baseline_storage = sort_storage_with_limits(limits, series_count);
    let baseline_engine =
        Engine::with_precision(Arc::clone(&baseline_storage), TimestampPrecision::Seconds);
    baseline_engine
        .instant_query("sort_source", 0)
        .expect("the exact input peak must still admit the selector");
    assert_eq!(
        baseline_storage
            .query_budget_snapshot()
            .shared_reserved_memory_bytes,
        0
    );

    let sort_storage = sort_storage_with_limits(limits, series_count);
    let sort_engine =
        Engine::with_precision(Arc::clone(&sort_storage), TimestampPrecision::Seconds);
    assert_limit(
        sort_engine
            .instant_query("sort(sort_source)", 0)
            .unwrap_err(),
        QueryLimitReason::PerQueryMemoryBytes,
    );
    let snapshot = sort_storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

#[test]
fn label_join_amplification_is_rejected_by_transform_preflight() {
    let source_value = "j".repeat(16 * 1_024);
    let source_count = 256usize;
    let source_args = std::iter::repeat_n(r#""src""#, source_count)
        .collect::<Vec<_>>()
        .join(", ");
    let missing_args = std::iter::repeat_n(r#""missing""#, source_count)
        .collect::<Vec<_>>()
        .join(", ");
    let amplified_query = format!(r#"label_join(join_source, "dst", ":", {source_args})"#);
    let control_query = format!(r#"label_join(join_source, "dst", ":", {missing_args})"#);
    let joined_len = source_value
        .len()
        .saturating_mul(source_count)
        .saturating_add(source_count - 1);
    let preflight_limit = u64::try_from(joined_len).unwrap().saturating_sub(1);

    let storage = label_replace_storage_with_limits(
        QueryBudgetLimits {
            max_shared_memory_bytes: Some(preflight_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(preflight_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        },
        &source_value,
    );
    // Use the same data shape under the metric expected by the query.
    storage
        .insert_rows(&[Row::with_labels(
            "join_source",
            vec![Label::new("src", source_value)],
            DataPoint::new(0, 1.0),
        )])
        .unwrap();
    let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);
    let PromqlValue::InstantVector(control) = engine
        .instant_query(&control_query, 0)
        .expect("the same argument preparation with a small joined value must fit")
    else {
        panic!("expected an instant vector");
    };
    assert_eq!(
        control[0]
            .labels
            .iter()
            .find(|label| label.name == "dst")
            .unwrap()
            .value
            .len(),
        source_count - 1
    );

    assert_limit(
        engine.instant_query(&amplified_query, 0).unwrap_err(),
        QueryLimitReason::PerQueryMemoryBytes,
    );
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}
