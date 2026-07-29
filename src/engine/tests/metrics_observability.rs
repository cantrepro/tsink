use super::*;
use crate::{
    Aggregation, QueryBudget, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken,
    QueryLimitReason, QueryOptions, QueryWorkLimits,
};

struct DefaultProjectionStorage;

impl Storage for DefaultProjectionStorage {
    fn insert_rows(&self, _rows: &[Row]) -> crate::Result<()> {
        Ok(())
    }

    fn select(
        &self,
        _metric: &str,
        _labels: &[Label],
        _start: i64,
        _end: i64,
    ) -> crate::Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_with_options(
        &self,
        _metric: &str,
        _opts: QueryOptions,
    ) -> crate::Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_all(
        &self,
        _metric: &str,
        _start: i64,
        _end: i64,
    ) -> crate::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        Ok(Vec::new())
    }

    fn close(&self) -> crate::Result<()> {
        Ok(())
    }
}

fn rollup_policy(id: &str, metric: &str) -> crate::RollupPolicy {
    crate::RollupPolicy {
        id: id.to_string(),
        metric: metric.to_string(),
        match_labels: vec![Label::new("unused", "in-metrics-projection")],
        interval: 1_000,
        aggregation: Aggregation::Avg,
        bucket_origin: 0,
    }
}

fn projection_budget(memory_bytes: Option<u64>) -> QueryBudget {
    QueryBudget::new(QueryBudgetLimits {
        max_shared_memory_bytes: memory_bytes,
        per_query: QueryWorkLimits {
            max_memory_bytes: memory_bytes,
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    })
    .unwrap()
}

#[test]
fn default_storage_refuses_unaccounted_metrics_projection() {
    let budget = projection_budget(None);
    let execution = budget.begin_query().unwrap();
    let err = DefaultProjectionStorage
        .metrics_observability_snapshot_with_execution(&execution)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::UnsupportedOperation {
            operation: "metrics_observability_snapshot_with_execution",
            ..
        }
    ));
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    drop(execution);
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
}

#[test]
fn metrics_observability_projection_has_exact_memory_boundary_and_retains_guard() {
    let data_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(data_dir.path());
    storage
        .apply_rollup_policies(vec![
            rollup_policy("metrics-alpha", "cpu_usage"),
            rollup_policy("metrics-beta", "request_latency"),
        ])
        .unwrap();
    storage.reset_rollup_metrics_snapshot_policy_copies();

    let calibration_budget = projection_budget(None);
    let calibration_execution = calibration_budget.begin_query().unwrap();
    let calibration = storage
        .metrics_observability_snapshot_with_execution(&calibration_execution)
        .unwrap();
    let required = calibration.reserved_memory_bytes();
    assert!(required > 0);
    assert_eq!(
        calibration_execution.snapshot().memory_reserved_bytes,
        required
    );
    assert_eq!(calibration.rollups.policies.len(), 2);
    let first = &calibration.rollups.policies[0];
    assert_eq!(calibration.rollups.policy_id(first), "metrics-alpha");
    assert_eq!(calibration.rollups.metric(first), "cpu_usage");
    assert_eq!(storage.rollup_metrics_snapshot_policy_copies(), 2);
    drop(calibration);
    assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);
    drop(calibration_execution);
    assert_eq!(
        calibration_budget.snapshot().shared_reserved_memory_bytes,
        0
    );

    storage.reset_rollup_metrics_snapshot_policy_copies();
    let exact_budget = projection_budget(Some(required));
    let exact_execution = exact_budget.begin_query().unwrap();
    let exact = storage
        .metrics_observability_snapshot_with_execution(&exact_execution)
        .unwrap();
    assert_eq!(exact.reserved_memory_bytes(), required);
    assert_eq!(exact_execution.snapshot().memory_reserved_bytes, required);
    assert_eq!(storage.rollup_metrics_snapshot_policy_copies(), 2);
    drop(exact);
    assert_eq!(exact_execution.snapshot().memory_reserved_bytes, 0);
    drop(exact_execution);
    let exact_snapshot = exact_budget.snapshot();
    assert_eq!(exact_snapshot.active_queries, 0);
    assert_eq!(exact_snapshot.shared_reserved_memory_bytes, 0);

    storage.reset_rollup_metrics_snapshot_policy_copies();
    let rejected_budget = projection_budget(Some(required - 1));
    let rejected_execution = rejected_budget.begin_query().unwrap();
    let err = storage
        .metrics_observability_snapshot_with_execution(&rejected_execution)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
            if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == required - 1
                && exceeded.requested == required
    ));
    assert_eq!(
        storage.rollup_metrics_snapshot_policy_copies(),
        0,
        "policy labels must not be copied before reservation admission"
    );
    assert_eq!(rejected_execution.snapshot().memory_reserved_bytes, 0);
    drop(rejected_execution);
    let rejected_snapshot = rejected_budget.snapshot();
    assert_eq!(rejected_snapshot.active_queries, 0);
    assert_eq!(rejected_snapshot.shared_reserved_memory_bytes, 0);
    storage.close().unwrap();
}

#[test]
fn cancelled_metrics_observability_projection_stops_before_policy_copy() {
    let data_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(data_dir.path());
    storage
        .apply_rollup_policies(vec![rollup_policy("cancelled", "cpu_usage")])
        .unwrap();
    storage.reset_rollup_metrics_snapshot_policy_copies();

    let budget = projection_budget(None);
    let cancellation = QueryCancellationToken::new();
    let execution = budget.begin_query_with_token(cancellation.clone()).unwrap();
    cancellation.cancel();
    let err = storage
        .metrics_observability_snapshot_with_execution(&execution)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::QueryBudget(QueryBudgetError::Cancelled)
    ));
    assert_eq!(storage.rollup_metrics_snapshot_policy_copies(), 0);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    drop(execution);
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.cancellations_total, 1);
    storage.close().unwrap();
}
