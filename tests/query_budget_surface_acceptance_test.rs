use std::sync::Arc;
use std::time::Instant;

use tsink::{
    AsyncStorageBuilder, DataPoint, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken,
    QueryLimitExceeded, QueryLimitReason, QueryWorkLimits, ResourceLimits, ResourceProfile,
    ResourceProfileName, Row, Storage, StorageBuilder, TimestampPrecision, TsinkError,
};

const RETURNED_SAMPLE_LIMIT: u64 = 2;

fn test_profile_query_limits() -> QueryBudgetLimits {
    let mut limits = ResourceLimits::test().query;
    limits.per_query.max_samples_returned = Some(RETURNED_SAMPLE_LIMIT);
    limits
}

fn assert_finite_test_profile(storage: &Arc<dyn Storage>) {
    let configuration = storage.resource_configuration_snapshot();
    assert_eq!(configuration.selected_profile, ResourceProfileName::Test);
    assert_eq!(
        configuration.resolved_limits.query,
        test_profile_query_limits()
    );
    configuration
        .resolved_limits
        .query
        .validate()
        .expect("the resolved Test query limits must be valid");

    let limits = configuration.resolved_limits.query;
    assert!(limits.max_concurrent_queries.is_some());
    assert!(limits.max_shared_memory_bytes.is_some());
    assert!(limits.per_query.max_series_matched.is_some());
    assert!(limits.per_query.max_samples_scanned.is_some());
    assert!(limits.per_query.max_samples_returned.is_some());
    assert!(limits.per_query.max_returned_bytes.is_some());
    assert!(limits.per_query.max_pattern_expansion.is_some());
    assert!(limits.per_query.max_steps.is_some());
    assert!(limits.per_query.max_intermediate_vector_size.is_some());
    assert!(limits.per_query.max_memory_bytes.is_some());
    assert!(limits.per_query.max_wall_time.is_some());
}

fn assert_samples_returned_limit(error: TsinkError, requested: u64) {
    assert!(matches!(
        error,
        TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(QueryLimitExceeded {
            reason: QueryLimitReason::SamplesReturned,
            limit: RETURNED_SAMPLE_LIMIT,
            current: 0,
            requested: actual,
        })) if actual == requested
    ));
}

fn assert_released(storage: &Arc<dyn Storage>) {
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(
        snapshot.queries_started_total,
        snapshot.queries_completed_total
    );
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

fn profile_storage() -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Test)
        .with_wal_enabled(false)
        .with_query_budget_limits(test_profile_query_limits())
        .build()
        .expect("finite Test-profile storage should build")
}

#[test]
fn direct_test_profile_query_accepts_exact_n_rejects_n_plus_one_and_releases() {
    let storage = profile_storage();
    assert_finite_test_profile(&storage);
    storage
        .insert_rows(&[
            Row::new("surface_direct", DataPoint::new(1, 1.0)),
            Row::new("surface_direct", DataPoint::new(2, 2.0)),
        ])
        .expect("seed exact-boundary rows");

    let exact = storage
        .select("surface_direct", &[], 0, 3)
        .expect("the exact returned-sample boundary should pass");
    assert_eq!(exact.len(), RETURNED_SAMPLE_LIMIT as usize);
    assert_released(&storage);

    storage
        .insert_rows(&[Row::new("surface_direct", DataPoint::new(3, 3.0))])
        .expect("seed the one-over row");
    let error = storage
        .select("surface_direct", &[], 0, 4)
        .expect_err("N+1 returned samples must be rejected, not truncated");
    assert_samples_returned_limit(error, RETURNED_SAMPLE_LIMIT + 1);
    assert_released(&storage);
    assert_eq!(
        storage
            .query_budget_snapshot()
            .samples_returned_rejections_total,
        1
    );

    let expired = QueryCancellationToken::new().with_deadline(Instant::now());
    let error = storage
        .begin_query_execution(QueryWorkLimits::default(), expired)
        .expect_err("an expired direct-query deadline must reject before admission");
    assert!(matches!(
        error,
        TsinkError::QueryBudget(QueryBudgetError::DeadlineExceeded)
    ));
    assert_released(&storage);
    assert_eq!(storage.query_budget_snapshot().deadline_exceeded_total, 1);

    storage.close().expect("close direct storage");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_test_profile_query_accepts_exact_n_rejects_n_plus_one_and_releases() {
    let storage = AsyncStorageBuilder::new()
        .with_resource_profile(ResourceProfile::Test)
        .with_wal_enabled(false)
        .with_query_budget_limits(test_profile_query_limits())
        .build()
        .expect("finite async Test-profile storage should build");
    let inner = storage.inner();
    assert_finite_test_profile(&inner);
    storage
        .insert_rows(vec![
            Row::new("surface_async", DataPoint::new(1, 1.0)),
            Row::new("surface_async", DataPoint::new(2, 2.0)),
        ])
        .await
        .expect("seed exact-boundary rows");

    let exact = storage
        .select("surface_async", Vec::new(), 0, 3)
        .await
        .expect("the exact async returned-sample boundary should pass");
    assert_eq!(exact.len(), RETURNED_SAMPLE_LIMIT as usize);
    assert_released(&inner);

    storage
        .insert_rows(vec![Row::new("surface_async", DataPoint::new(3, 3.0))])
        .await
        .expect("seed the async one-over row");
    let error = storage
        .select("surface_async", Vec::new(), 0, 4)
        .await
        .expect_err("async N+1 returned samples must be rejected, not truncated");
    assert_samples_returned_limit(error, RETURNED_SAMPLE_LIMIT + 1);
    assert_released(&inner);
    assert_eq!(
        storage
            .inner()
            .query_budget_snapshot()
            .samples_returned_rejections_total,
        1
    );

    storage.close().await.expect("close async storage");
}

#[test]
fn promql_test_profile_range_accepts_exact_n_rejects_n_plus_one_and_releases() {
    let storage = profile_storage();
    assert_finite_test_profile(&storage);
    let engine =
        tsink::promql::Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Seconds);

    let exact = engine
        .range_query("vector(1)", 0, 1, 1)
        .expect("the exact PromQL returned-sample boundary should pass");
    assert_eq!(
        exact,
        tsink::promql::PromqlValue::RangeVector(vec![tsink::promql::types::Series {
            metric: String::new(),
            labels: Vec::new(),
            samples: vec![(0, 1.0), (1, 1.0)],
            histograms: Vec::new(),
        }])
    );
    assert_released(&storage);

    let error = engine
        .range_query("vector(1)", 0, 2, 1)
        .expect_err("PromQL N+1 returned samples must be rejected, not truncated");
    assert!(matches!(
        error,
        tsink::promql::PromqlError::Storage(TsinkError::QueryBudget(
            QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::SamplesReturned,
                limit: RETURNED_SAMPLE_LIMIT,
                current: 0,
                requested: 3,
            })
        ))
    ));
    assert_released(&storage);
    assert_eq!(
        storage
            .query_budget_snapshot()
            .samples_returned_rejections_total,
        1
    );

    let expired = QueryCancellationToken::new().with_deadline(Instant::now());
    let error = engine
        .range_query_with_control("vector(1)", 0, 1, 1, QueryWorkLimits::default(), expired)
        .expect_err("an expired PromQL deadline must reject before admission");
    assert!(matches!(
        error,
        tsink::promql::PromqlError::Storage(TsinkError::QueryBudget(
            QueryBudgetError::DeadlineExceeded
        ))
    ));
    assert_released(&storage);
    assert_eq!(storage.query_budget_snapshot().deadline_exceeded_total, 1);

    storage.close().expect("close PromQL storage");
}
